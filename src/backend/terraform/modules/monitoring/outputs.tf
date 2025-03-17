# Output definitions for the monitoring module

# Notification channels output
output "notification_channels" {
  description = "Map of notification channel IDs by type"
  value = {
    teams_webhook = try(google_monitoring_notification_channel.teams_webhook[0].id, "")
    email = try(google_monitoring_notification_channel.email[0].id, "")
    custom = { for k, v in google_monitoring_notification_channel.custom_channels : k => v.id }
  }
}

# All notification channel IDs combined
output "notification_channel_ids" {
  description = "List of all notification channel IDs used in alert policies"
  value = local.notification_channel_ids
}

# Alert policies output
output "alert_policies" {
  description = "Map of alert policy names by type"
  value = {
    pipeline_failure = try(google_monitoring_alert_policy.pipeline_failure_alert[0].name, "")
    data_quality = try(google_monitoring_alert_policy.data_quality_alert[0].name, "")
    self_healing = try(google_monitoring_alert_policy.self_healing_alert[0].name, "")
    bigquery_slots = try(google_monitoring_alert_policy.bigquery_slots_alert[0].name, "")
    custom = { for k, v in google_monitoring_alert_policy.custom_alerts : k => v.name }
  }
}

# Dashboards output
output "dashboards" {
  description = "Map of dashboard names by type"
  value = {
    pipeline_overview = try(google_monitoring_dashboard.pipeline_overview_dashboard[0].name, "")
    data_quality = try(google_monitoring_dashboard.data_quality_dashboard[0].name, "")
    self_healing = try(google_monitoring_dashboard.self_healing_dashboard[0].name, "")
    bigquery_performance = try(google_monitoring_dashboard.bigquery_performance_dashboard[0].name, "")
    custom = { for k, v in google_monitoring_dashboard.custom_dashboards : k => v.name }
  }
}

# Custom metrics output
output "custom_metrics" {
  description = "Map of custom metric types by name"
  value = {
    data_quality_score = try(google_monitoring_metric_descriptor.data_quality_score_metric[0].type, "")
    self_healing_success = try(google_monitoring_metric_descriptor.self_healing_success_metric[0].type, "")
    pipeline_execution_time = try(google_monitoring_metric_descriptor.pipeline_execution_time_metric[0].type, "")
  }
}

# Uptime check output
output "uptime_check" {
  description = "Name of the pipeline health uptime check if enabled"
  value = try(google_monitoring_uptime_check_config.pipeline_health_check[0].name, "")
}

# Project info output
output "monitoring_project_id" {
  description = "The Google Cloud Project ID where monitoring resources are deployed"
  value = var.project_id
}

# Environment output
output "monitoring_environment" {
  description = "The deployment environment for the monitoring resources"
  value = var.environment
}