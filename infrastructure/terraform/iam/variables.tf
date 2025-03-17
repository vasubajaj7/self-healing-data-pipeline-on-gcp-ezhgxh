# Basic project and environment variables

variable "project_id" {
  description = "The Google Cloud Project ID where the IAM resources will be deployed"
  type        = string
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be provided and cannot be empty."
  }
}

variable "region" {
  description = "The Google Cloud region where the IAM resources will be deployed"
  type        = string
  default     = "us-central1"
  validation {
    condition     = length(var.region) > 0
    error_message = "The region variable must be provided and cannot be empty."
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
  description = "Prefix to be used for all resource names"
  type        = string
  default     = "shp"
  validation {
    condition     = length(var.resource_prefix) > 0
    error_message = "The resource_prefix variable must be provided and cannot be empty."
  }
}

variable "labels" {
  description = "A map of labels to apply to all resources"
  type        = map(string)
  default     = {}
}

# Feature flag variables

variable "enable_cmek" {
  description = "Whether to enable Customer Managed Encryption Keys"
  type        = bool
  default     = false
}

variable "enable_iap" {
  description = "Whether to enable Identity-Aware Proxy for secure access"
  type        = bool
  default     = false
}

variable "create_keys" {
  description = "Whether to create service account keys (not recommended for production)"
  type        = bool
  default     = false
}

# Service account role variables

variable "orchestrator_roles" {
  description = "List of IAM roles to assign to the pipeline orchestrator service account"
  type        = list(string)
  default = [
    "roles/composer.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/secretmanager.secretAccessor"
  ]
}

variable "ingestion_roles" {
  description = "List of IAM roles to assign to the data ingestion service account"
  type        = list(string)
  default = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/cloudsql.client",
    "roles/secretmanager.secretAccessor"
  ]
}

variable "validator_roles" {
  description = "List of IAM roles to assign to the quality validator service account"
  type        = list(string)
  default = [
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser",
    "roles/storage.objectViewer"
  ]
}

variable "healing_roles" {
  description = "List of IAM roles to assign to the self-healing service account"
  type        = list(string)
  default = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/aiplatform.user"
  ]
}

variable "monitoring_roles" {
  description = "List of IAM roles to assign to the monitoring service account"
  type        = list(string)
  default = [
    "roles/monitoring.admin",
    "roles/logging.viewer",
    "roles/errorreporting.user",
    "roles/cloudnotifications.publisher"
  ]
}

# Custom role permission variables

variable "operator_permissions" {
  description = "List of permissions for the custom pipeline operator role"
  type        = list(string)
  default = [
    "composer.environments.get",
    "composer.environments.list",
    "bigquery.jobs.create",
    "bigquery.jobs.list",
    "bigquery.jobs.get",
    "bigquery.jobs.update",
    "bigquery.jobs.cancel",
    "bigquery.datasets.get",
    "bigquery.datasets.getIamPolicy",
    "bigquery.tables.get",
    "bigquery.tables.list",
    "bigquery.tables.getData",
    "bigquery.tables.export",
    "monitoring.timeSeries.list",
    "monitoring.alertPolicies.get",
    "monitoring.alertPolicies.list",
    "logging.logEntries.list",
    "logging.logs.list"
  ]
}

variable "viewer_permissions" {
  description = "List of permissions for the custom pipeline viewer role"
  type        = list(string)
  default = [
    "composer.environments.get",
    "composer.environments.list",
    "bigquery.jobs.list",
    "bigquery.jobs.get",
    "bigquery.datasets.get",
    "bigquery.tables.get",
    "bigquery.tables.list",
    "bigquery.tables.getData",
    "monitoring.timeSeries.list",
    "monitoring.alertPolicies.get",
    "monitoring.alertPolicies.list",
    "logging.logEntries.list",
    "logging.logs.list"
  ]
}

# IAP-related variables

variable "support_email" {
  description = "Support email for IAP brand"
  type        = string
}

variable "backend_service_id" {
  description = "Backend service ID for IAP binding"
  type        = string
  default     = ""
}