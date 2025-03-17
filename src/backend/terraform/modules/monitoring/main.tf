# Provider Configuration
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.80.0"
    }
  }
}

# Local values for common variables and expressions
locals {
  resource_name_prefix = "${var.resource_prefix}-${var.environment}"
  
  # Combine all notification channel IDs for use in alert policies
  notification_channel_ids = concat(
    try([google_monitoring_notification_channel.teams_webhook[0].id], []),
    try([google_monitoring_notification_channel.email[0].id], []),
    [for k, v in google_monitoring_notification_channel.custom_channels : v.id]
  )
  
  # Common variables used in dashboard templates
  dashboard_variables = {
    project_id = var.project_id
    environment = var.environment
    resource_prefix = var.resource_prefix
    composer_environment_name = "${local.resource_name_prefix}-composer"
    pipeline_failure_threshold = var.pipeline_failure_threshold
    data_quality_threshold = var.data_quality_threshold
    self_healing_threshold = var.self_healing_threshold
    bigquery_slot_threshold = var.bigquery_slot_threshold
  }
}

#################################################
# Notification Channels
#################################################

# Microsoft Teams webhook notification channel for alerts
resource "google_monitoring_notification_channel" "teams_webhook" {
  count = var.teams_webhook_url != "" ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-teams-webhook"
  type         = "webhook_tokenauth"
  
  labels = {
    url = var.teams_webhook_url
    auth_token = var.teams_webhook_auth_token != "" ? var.teams_webhook_auth_token : null
  }
  
  user_labels = var.labels
}

# Email notification channel for alerts
resource "google_monitoring_notification_channel" "email" {
  count = var.alert_email_address != "" ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-email-alert"
  type         = "email"
  
  labels = {
    email_address = var.alert_email_address
  }
  
  user_labels = var.labels
}

# Custom notification channels defined in variables
resource "google_monitoring_notification_channel" "custom_channels" {
  for_each = var.notification_channels
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-${each.key}"
  type         = each.value.type
  labels       = each.value.labels
  user_labels  = var.labels
}

#################################################
# Alert Policies
#################################################

# Alert policy for pipeline execution failures
resource "google_monitoring_alert_policy" "pipeline_failure_alert" {
  count = var.create_default_alerts ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-pipeline-failure"
  combiner     = "OR"
  
  conditions {
    display_name = "Pipeline execution failure rate > ${var.pipeline_failure_threshold}%"
    
    condition_threshold {
      filter     = "resource.type = \"cloud_composer_environment\" AND metric.type = \"composer.googleapis.com/workflow/failed_dag_run_count\" AND resource.label.\"environment_name\" = \"${var.composer_environment_name}\""
      duration   = "300s"
      comparison = "COMPARISON_GT"
      threshold_value = var.pipeline_failure_threshold
      
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.environment_name"]
      }
    }
  }
  
  notification_channels = local.notification_channel_ids
  user_labels           = var.labels
  
  documentation {
    content    = "## Pipeline Failure Alert\n\nThis alert indicates that one or more pipeline executions have failed in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the Airflow UI for failed DAGs\n- Review task logs for error details\n- Verify data source availability\n- Check for recent changes"
    mime_type  = "text/markdown"
  }
  
  alert_strategy {
    auto_close = var.alert_auto_close
    notification_rate_limit {
      period = var.alert_notification_rate_limit
    }
  }
}

# Alert policy for data quality validation failures
resource "google_monitoring_alert_policy" "data_quality_alert" {
  count = var.create_default_alerts ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-data-quality"
  combiner     = "OR"
  
  conditions {
    display_name = "Data quality validation failure rate > ${var.data_quality_threshold}%"
    
    condition_monitoring_query_language {
      query = "fetch custom.googleapis.com/pipeline/data_quality_score\n| filter (resource.project_id == '${var.project_id}')\n| filter (metric.environment == '${var.environment}')\n| align mean_aligner(5m)\n| every 5m\n| group_by [metric.dataset], [value_quality_score_mean: mean(value.quality_score)]\n| condition value_quality_score_mean < ${100 - var.data_quality_threshold}"
      duration = "300s"
    }
  }
  
  notification_channels = local.notification_channel_ids
  user_labels           = var.labels
  
  documentation {
    content    = "## Data Quality Alert\n\nThis alert indicates that data quality validations are failing at a rate higher than the threshold in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the quality validation logs\n- Review the failing validation rules\n- Examine the source data for issues\n- Verify if self-healing is attempting to resolve the issues"
    mime_type  = "text/markdown"
  }
  
  alert_strategy {
    auto_close = var.alert_auto_close
    notification_rate_limit {
      period = var.alert_notification_rate_limit
    }
  }
}

# Alert policy for self-healing system failures
resource "google_monitoring_alert_policy" "self_healing_alert" {
  count = var.create_default_alerts ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-self-healing-failure"
  combiner     = "OR"
  
  conditions {
    display_name = "Self-healing success rate < ${var.self_healing_threshold}%"
    
    condition_monitoring_query_language {
      query = "fetch custom.googleapis.com/pipeline/self_healing_success_rate\n| filter (resource.project_id == '${var.project_id}')\n| filter (metric.environment == '${var.environment}')\n| align mean_aligner(5m)\n| every 5m\n| group_by [metric.issue_type], [value_success_rate_mean: mean(value.success_rate)]\n| condition value_success_rate_mean < ${var.self_healing_threshold}"
      duration = "300s"
    }
  }
  
  notification_channels = local.notification_channel_ids
  user_labels           = var.labels
  
  documentation {
    content    = "## Self-Healing Failure Alert\n\nThis alert indicates that the self-healing system is failing to resolve issues at an acceptable rate in the ${var.environment} environment.\n\n### Troubleshooting\n- Check the self-healing logs\n- Review the AI model performance\n- Examine the types of issues that are failing to be resolved\n- Consider updating the healing rules or retraining models"
    mime_type  = "text/markdown"
  }
  
  alert_strategy {
    auto_close = var.alert_auto_close
    notification_rate_limit {
      period = var.alert_notification_rate_limit
    }
  }
}

# Alert policy for BigQuery slot utilization
resource "google_monitoring_alert_policy" "bigquery_slots_alert" {
  count = var.create_default_alerts ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-bigquery-slots"
  combiner     = "OR"
  
  conditions {
    display_name = "BigQuery slot utilization > ${var.bigquery_slot_threshold}%"
    
    condition_threshold {
      filter     = "resource.type = \"bigquery_project\" AND metric.type = \"bigquery.googleapis.com/slots/allocated_for_project\""
      duration   = "300s"
      comparison = "COMPARISON_GT"
      threshold_value = var.bigquery_slot_threshold
      
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = []
      }
    }
  }
  
  notification_channels = local.notification_channel_ids
  user_labels           = var.labels
  
  documentation {
    content    = "## BigQuery Slot Utilization Alert\n\nThis alert indicates that BigQuery slot utilization is exceeding the threshold in the ${var.environment} environment.\n\n### Troubleshooting\n- Review active queries and their resource consumption\n- Consider optimizing heavy queries\n- Evaluate if additional slots are needed\n- Implement query scheduling for better distribution"
    mime_type  = "text/markdown"
  }
  
  alert_strategy {
    auto_close = var.alert_auto_close
    notification_rate_limit {
      period = var.alert_notification_rate_limit
    }
  }
}

# Custom alert policies defined in variables
resource "google_monitoring_alert_policy" "custom_alerts" {
  for_each = var.alert_policies
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-${each.key}"
  combiner     = each.value.combiner
  conditions   = each.value.conditions
  
  notification_channels = local.notification_channel_ids
  user_labels           = var.labels
  
  alert_strategy {
    auto_close = var.alert_auto_close
    notification_rate_limit {
      period = var.alert_notification_rate_limit
    }
  }
}

#################################################
# Dashboards
#################################################

# Dashboard for pipeline overview metrics
resource "google_monitoring_dashboard" "pipeline_overview_dashboard" {
  count = var.create_default_dashboards ? 1 : 0
  
  project        = var.project_id
  dashboard_json = templatefile("${path.module}/templates/pipeline_overview_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for data quality metrics
resource "google_monitoring_dashboard" "data_quality_dashboard" {
  count = var.create_default_dashboards ? 1 : 0
  
  project        = var.project_id
  dashboard_json = templatefile("${path.module}/templates/data_quality_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for self-healing metrics
resource "google_monitoring_dashboard" "self_healing_dashboard" {
  count = var.create_default_dashboards ? 1 : 0
  
  project        = var.project_id
  dashboard_json = templatefile("${path.module}/templates/self_healing_dashboard.json.tpl", local.dashboard_variables)
}

# Dashboard for BigQuery performance metrics
resource "google_monitoring_dashboard" "bigquery_performance_dashboard" {
  count = var.create_default_dashboards ? 1 : 0
  
  project        = var.project_id
  dashboard_json = templatefile("${path.module}/templates/bigquery_performance_dashboard.json.tpl", local.dashboard_variables)
}

# Custom dashboards defined in variables
resource "google_monitoring_dashboard" "custom_dashboards" {
  for_each = var.dashboards
  
  project        = var.project_id
  dashboard_json = each.value.dashboard_json
}

#################################################
# Custom Metrics
#################################################

# Custom metric for data quality score
resource "google_monitoring_metric_descriptor" "data_quality_score_metric" {
  count = var.enable_custom_metrics ? 1 : 0
  
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
  count = var.enable_custom_metrics ? 1 : 0
  
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
  count = var.enable_custom_metrics ? 1 : 0
  
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
  count = var.enable_uptime_checks && var.api_endpoint != "" ? 1 : 0
  
  project      = var.project_id
  display_name = "${local.resource_name_prefix}-pipeline-health"
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
    
    auth_info {
      username = var.health_check_username
      password = var.health_check_password
    }
  }
  
  monitored_resource {
    type = "uptime_url"
    labels = {
      host       = var.api_endpoint
      project_id = var.project_id
    }
  }
  
  content_matchers {
    content = "healthy"
    matcher = "CONTAINS_STRING"
  }
}