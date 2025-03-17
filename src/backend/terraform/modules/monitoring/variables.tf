################################################################################
# General Configuration Variables
################################################################################

variable "project_id" {
  description = "The Google Cloud Project ID where monitoring resources will be deployed"
  type        = string

  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be set."
  }
}

variable "environment" {
  description = "The deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "The environment variable must be one of: dev, staging, prod."
  }
}

variable "resource_prefix" {
  description = "Prefix to be applied to all resource names for identification"
  type        = string
  default     = "shp"

  validation {
    condition     = length(var.resource_prefix) > 0
    error_message = "The resource_prefix variable must be set."
  }
}

variable "labels" {
  description = "Labels to apply to all monitoring resources"
  type        = map(string)
  default = {
    application = "self-healing-pipeline"
    component   = "monitoring"
    managed-by  = "terraform"
  }
}

################################################################################
# Notification Channel Variables
################################################################################

variable "teams_webhook_url" {
  description = "Microsoft Teams webhook URL for sending alert notifications"
  type        = string
  default     = ""
  sensitive   = true
}

variable "teams_webhook_auth_token" {
  description = "Authentication token for Microsoft Teams webhook (if required)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "alert_email_address" {
  description = "Email address to receive alert notifications"
  type        = string
  default     = ""
}

variable "notification_channels" {
  description = "Map of additional notification channels to create"
  type        = map(object({
    type   = string
    labels = map(string)
  }))
  default = {}
}

################################################################################
# Alert Policy Variables
################################################################################

variable "create_default_alerts" {
  description = "Whether to create default alert policies for common pipeline metrics"
  type        = bool
  default     = true
}

variable "alert_policies" {
  description = "Map of custom alert policies to create"
  type = map(object({
    display_name = string
    combiner     = string
    conditions = list(object({
      display_name = string
      condition_threshold = optional(object({
        filter          = string
        duration        = string
        comparison      = string
        threshold_value = number
        aggregations = list(object({
          alignment_period     = string
          per_series_aligner   = string
          cross_series_reducer = string
          group_by_fields      = list(string)
        }))
      }))
      condition_monitoring_query_language = optional(object({
        query    = string
        duration = string
      }))
    }))
  }))
  default = {}
}

variable "pipeline_failure_threshold" {
  description = "Threshold percentage for pipeline failure rate alerts"
  type        = number
  default     = 5

  validation {
    condition     = var.pipeline_failure_threshold >= 0 && var.pipeline_failure_threshold <= 100
    error_message = "The pipeline_failure_threshold must be between 0 and 100."
  }
}

variable "data_quality_threshold" {
  description = "Threshold percentage for data quality validation failure alerts"
  type        = number
  default     = 5

  validation {
    condition     = var.data_quality_threshold >= 0 && var.data_quality_threshold <= 100
    error_message = "The data_quality_threshold must be between 0 and 100."
  }
}

variable "self_healing_threshold" {
  description = "Threshold percentage for self-healing success rate alerts (alert when below this value)"
  type        = number
  default     = 80

  validation {
    condition     = var.self_healing_threshold >= 0 && var.self_healing_threshold <= 100
    error_message = "The self_healing_threshold must be between 0 and 100."
  }
}

variable "bigquery_slot_threshold" {
  description = "Threshold percentage for BigQuery slot utilization alerts"
  type        = number
  default     = 80

  validation {
    condition     = var.bigquery_slot_threshold >= 0 && var.bigquery_slot_threshold <= 100
    error_message = "The bigquery_slot_threshold must be between 0 and 100."
  }
}

variable "alert_auto_close" {
  description = "Duration after which alerts auto-close if the condition is no longer met"
  type        = string
  default     = "86400s" # 24 hours
}

variable "alert_notification_rate_limit" {
  description = "Minimum interval between alert notifications for the same policy"
  type        = string
  default     = "300s" # 5 minutes
}

################################################################################
# Dashboard Variables
################################################################################

variable "create_default_dashboards" {
  description = "Whether to create default monitoring dashboards"
  type        = bool
  default     = true
}

variable "dashboards" {
  description = "Map of custom dashboards to create"
  type        = map(object({
    dashboard_json = string
  }))
  default = {}
}

################################################################################
# Uptime Check Variables
################################################################################

variable "enable_uptime_checks" {
  description = "Whether to enable uptime checks for the pipeline API"
  type        = bool
  default     = true
}

variable "api_endpoint" {
  description = "API endpoint URL for uptime checks (without protocol)"
  type        = string
  default     = ""
}

variable "health_check_username" {
  description = "Username for authenticated health check endpoint"
  type        = string
  default     = ""
  sensitive   = true
}

variable "health_check_password" {
  description = "Password for authenticated health check endpoint"
  type        = string
  default     = ""
  sensitive   = true
}

################################################################################
# Custom Metrics Variables
################################################################################

variable "enable_custom_metrics" {
  description = "Whether to create custom metrics for pipeline monitoring"
  type        = bool
  default     = true
}

variable "composer_environment_name" {
  description = "Name of the Cloud Composer environment to monitor"
  type        = string
  default     = "pipeline-composer"
}