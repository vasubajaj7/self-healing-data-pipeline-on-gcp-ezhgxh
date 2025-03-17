# Self-Healing Data Pipeline - Monitoring Configuration
# Terraform configuration for monitoring and alerting resources

# Define local variables for use in this module
locals {
  # Combine all notification channel IDs (email, Teams, and any externally provided)
  monitoring_notification_channel_ids = concat(
    [for channel in google_monitoring_notification_channel.email_channels : channel.id],
    [for channel in google_monitoring_notification_channel.teams_webhook : channel.id],
    var.monitoring_notification_channels
  )
  
  # Standard format for resource names
  resource_name_format = "${var.resource_prefix}-${var.environment}"
  
  # Common variables used in dashboard templates
  dashboard_variables = {
    project_id = var.project_id
    environment = var.environment
    resource_prefix = var.resource_prefix
    composer_environment_name = "${local.resource_name_format}-composer"
    bigquery_slot_threshold = 80
    pipeline_failure_threshold = 5
    data_quality_threshold = 5
    self_healing_threshold = 75
  }
}

#################################################
# Notification Channels
#################################################

# Create email notification channels for each email address in the list
resource "google_monitoring_notification_channel" "email_channels" {
  for_each     = toset(var.alert_email_addresses)
  project      = var.project_id
  display_name = "${local.resource_name_format}-email-${each.value}"
  type         = "email"
  labels = {
    email_address = each.value
  }
  user_labels = var.labels
}

# Create Microsoft Teams webhook notification channel
resource "google_monitoring_notification_channel" "teams_webhook" {
  count        = var.teams_webhook_url != "" ? 1 : 0
  project      = var.project_id
  display_name = "${local.resource_name_format}-teams-webhook"
  type         = "webhook_tokenauth"
  labels = {
    url = var.teams_webhook_url
  }
  user_labels = var.labels
}

#################################################
# Alert Policies
#################################################

# Alert policy for pipeline execution failures
resource "google_monitoring_alert_policy" "pipeline_failure_alert" {
  project      = var.project_id
  display_name = "${local.resource_name_format}-pipeline-failure"
  combiner     = "OR"
  
  conditions {
    display_name = "Pipeline execution failure rate > 5%"
    condition_threshold {
      filter          = "resource.type = \"cloud_composer_environment\" AND metric.type = \"composer.googleapis.com/workflow/failed_dag_run_count\" AND resource.label.\"environment_name\" = \"${local.resource_name_format}-composer\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields    = ["resource.label.environment_name"]
      }
    }
  }
  
  notification_channels = local.monitoring_notification_channel_ids
  user_labels = var.labels
  
  documentation {
    content   = "## Pipeline Failure Alert\n\nThis alert indicates that one or more pipeline executions have failed in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the Airflow UI for failed DAGs\n- Review task logs for error details\n- Verify data source availability\n- Check for recent changes"
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"  # Auto-close after 30 minutes
    notification_rate_limit {
      period = "300s"     # Limit notifications to once per 5 minutes
    }
  }
}

# Alert policy for data quality validation failures
resource "google_monitoring_alert_policy" "data_quality_alert" {
  project      = var.project_id
  display_name = "${local.resource_name_format}-data-quality"
  combiner     = "OR"
  
  conditions {
    display_name = "Data quality validation failure rate > 5%"
    condition_monitoring_query_language {
      query    = "fetch custom.googleapis.com/pipeline/data_quality_score\n| filter (resource.project_id == '${var.project_id}')\n| filter (metric.environment == '${var.environment}')\n| align mean_aligner(5m)\n| every 5m\n| group_by [metric.dataset], [value_quality_score_mean: mean(value.quality_score)]\n| condition value_quality_score_mean < 95"
      duration = "300s"
    }
  }
  
  notification_channels = local.monitoring_notification_channel_ids
  user_labels = var.labels
  
  documentation {
    content   = "## Data Quality Alert\n\nThis alert indicates that data quality validations are failing at a rate higher than the threshold in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the quality validation logs\n- Review the failing validation rules\n- Examine the source data for issues\n- Verify if self-healing is attempting to resolve the issues"
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"  # Auto-close after 30 minutes
    notification_rate_limit {
      period = "300s"     # Limit notifications to once per 5 minutes
    }
  }
}

# Alert policy for self-healing system failures
resource "google_monitoring_alert_policy" "self_healing_alert" {
  project      = var.project_id
  display_name = "${local.resource_name_format}-self-healing-failure"
  combiner     = "OR"
  
  conditions {
    display_name = "Self-healing success rate < 75%"
    condition_monitoring_query_language {
      query    = "fetch custom.googleapis.com/pipeline/self_healing_success_rate\n| filter (resource.project_id == '${var.project_id}')\n| filter (metric.environment == '${var.environment}')\n| align mean_aligner(5m)\n| every 5m\n| group_by [metric.issue_type], [value_success_rate_mean: mean(value.success_rate)]\n| condition value_success_rate_mean < 75"
      duration = "300s"
    }
  }
  
  notification_channels = local.monitoring_notification_channel_ids
  user_labels = var.labels
  
  documentation {
    content   = "## Self-Healing Failure Alert\n\nThis alert indicates that the self-healing system is failing to resolve issues at an acceptable rate in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the self-healing logs\n- Review the AI model performance\n- Examine the types of issues that are failing to be resolved\n- Consider updating the healing rules or retraining models"
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"  # Auto-close after 30 minutes
    notification_rate_limit {
      period = "300s"     # Limit notifications to once per 5 minutes
    }
  }
}

# Alert policy for BigQuery slot utilization
resource "google_monitoring_alert_policy" "bigquery_slots_alert" {
  project      = var.project_id
  display_name = "${local.resource_name_format}-bigquery-slots"
  combiner     = "OR"
  
  conditions {
    display_name = "BigQuery slot utilization > 80%"
    condition_threshold {
      filter          = "resource.type = \"bigquery_project\" AND metric.type = \"bigquery.googleapis.com/slots/allocated_for_project\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 80
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }
  
  notification_channels = local.monitoring_notification_channel_ids
  user_labels = var.labels
  
  documentation {
    content   = "## BigQuery Slot Utilization Alert\n\nThis alert indicates that BigQuery slot utilization is exceeding the threshold in the ${var.environment} environment.\n\n### Troubleshooting\n- Review active queries and their resource consumption\n- Consider optimizing heavy queries\n- Evaluate if additional slots are needed\n- Implement query scheduling for better distribution"
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"  # Auto-close after 30 minutes
    notification_rate_limit {
      period = "300s"     # Limit notifications to once per 5 minutes
    }
  }
}

#################################################
# Dashboards
#################################################

# Dashboard for pipeline overview metrics
resource "google_monitoring_dashboard" "pipeline_overview_dashboard" {
  project      = var.project_id
  dashboard_json = templatefile("${path.module}/templates/pipeline_overview_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for data quality metrics
resource "google_monitoring_dashboard" "data_quality_dashboard" {
  project      = var.project_id
  dashboard_json = templatefile("${path.module}/templates/data_quality_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for self-healing metrics
resource "google_monitoring_dashboard" "self_healing_dashboard" {
  project      = var.project_id
  dashboard_json = templatefile("${path.module}/templates/self_healing_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for BigQuery performance metrics
resource "google_monitoring_dashboard" "bigquery_performance_dashboard" {
  project      = var.project_id
  dashboard_json = templatefile("${path.module}/templates/bigquery_performance_dashboard.json.tpl", local.dashboard_variables)
}

#################################################
# Custom Metrics
#################################################

# Custom metric for data quality score
resource "google_monitoring_metric_descriptor" "data_quality_score_metric" {
  project      = var.project_id
  description  = "Data quality score by dataset"
  display_name = "Data Quality Score"
  type         = "custom.googleapis.com/pipeline/data_quality_score"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "%"
  
  labels {
    key         = "dataset"
    value_type  = "STRING"
    description = "Dataset name"
  }
  
  labels {
    key         = "environment"
    value_type  = "STRING"
    description = "Deployment environment"
  }
}

# Custom metric for self-healing success rate
resource "google_monitoring_metric_descriptor" "self_healing_success_metric" {
  project      = var.project_id
  description  = "Self-healing success rate"
  display_name = "Self-Healing Success Rate"
  type         = "custom.googleapis.com/pipeline/self_healing_success_rate"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "%"
  
  labels {
    key         = "issue_type"
    value_type  = "STRING"
    description = "Type of issue being healed"
  }
  
  labels {
    key         = "environment"
    value_type  = "STRING"
    description = "Deployment environment"
  }
}

# Custom metric for pipeline execution time
resource "google_monitoring_metric_descriptor" "pipeline_execution_time_metric" {
  project      = var.project_id
  description  = "Pipeline execution duration"
  display_name = "Pipeline Execution Time"
  type         = "custom.googleapis.com/pipeline/execution_time"
  metric_kind  = "GAUGE"
  value_type   = "DOUBLE"
  unit         = "s"
  
  labels {
    key         = "pipeline_id"
    value_type  = "STRING"
    description = "Pipeline identifier"
  }
  
  labels {
    key         = "environment"
    value_type  = "STRING"
    description = "Deployment environment"
  }
}

#################################################
# Uptime Checks
#################################################

# Uptime check for pipeline health endpoint
resource "google_monitoring_uptime_check_config" "pipeline_health_check" {
  count        = var.environment == "prod" ? 1 : 0
  project      = var.project_id
  display_name = "${local.resource_name_format}-pipeline-health"
  timeout      = "10s"
  period       = "300s"
  
  selected_regions = [
    "us-central1",
    "us-east1",
    "us-west1"
  ]
  
  http_check {
    path         = "/api/v1/health"
    port         = 443
    use_ssl      = true
    validate_ssl = true
  }
  
  monitored_resource {
    type = "uptime_url"
    labels = {
      host       = "${local.resource_name_format}-api.example.com"
      project_id = var.project_id
    }
  }
  
  content_matchers {
    content = "healthy"
    matcher = "CONTAINS_STRING"
  }
}

#################################################
# Outputs
#################################################

output "notification_channels" {
  description = "Map of notification channel IDs by type"
  value = {
    email = [for channel in google_monitoring_notification_channel.email_channels : channel.id]
    teams = [for channel in google_monitoring_notification_channel.teams_webhook : channel.id]
  }
}

output "alert_policies" {
  description = "Map of alert policy names by type"
  value = {
    pipeline_failure = google_monitoring_alert_policy.pipeline_failure_alert.name
    data_quality = google_monitoring_alert_policy.data_quality_alert.name
    self_healing = google_monitoring_alert_policy.self_healing_alert.name
    bigquery_slots = google_monitoring_alert_policy.bigquery_slots_alert.name
  }
}

output "dashboards" {
  description = "Map of dashboard names by type"
  value = {
    pipeline_overview = google_monitoring_dashboard.pipeline_overview_dashboard.name
    data_quality = google_monitoring_dashboard.data_quality_dashboard.name
    self_healing = google_monitoring_dashboard.self_healing_dashboard.name
    bigquery_performance = google_monitoring_dashboard.bigquery_performance_dashboard.name
  }
}

output "custom_metrics" {
  description = "Map of custom metric names by type"
  value = {
    data_quality_score = google_monitoring_metric_descriptor.data_quality_score_metric.name
    self_healing_success = google_monitoring_metric_descriptor.self_healing_success_metric.name
    pipeline_execution_time = google_monitoring_metric_descriptor.pipeline_execution_time_metric.name
  }
}