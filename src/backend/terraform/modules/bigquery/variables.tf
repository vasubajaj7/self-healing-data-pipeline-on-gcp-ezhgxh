variable "project_id" {
  description = "The Google Cloud Project ID where resources will be deployed"
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

variable "dataset_id" {
  description = "Base ID for the main BigQuery dataset"
  type        = string
  default     = "pipeline_data"
  validation {
    condition     = length(var.dataset_id) > 0
    error_message = "The dataset_id variable must be set."
  }
}

variable "dataset_location" {
  description = "Location for BigQuery datasets"
  type        = string
  default     = "US"
  validation {
    condition     = length(var.dataset_location) > 0
    error_message = "The dataset_location variable must be set."
  }
}

variable "labels" {
  description = "Labels to apply to all BigQuery resources"
  type        = map(string)
  default     = {}
}

variable "service_account_email" {
  description = "Email of the service account that will access BigQuery resources"
  type        = string
  validation {
    condition     = var.service_account_email != null
    error_message = "The service_account_email variable must be set."
  }
}

variable "default_table_expiration_ms" {
  description = "Default expiration time for tables in milliseconds"
  type        = number
  default     = null
}

variable "delete_contents_on_destroy" {
  description = "Whether to delete contents when datasets are destroyed"
  type        = bool
  default     = false
}

variable "enable_cmek" {
  description = "Whether to enable Customer Managed Encryption Keys"
  type        = bool
  default     = false
}

variable "kms_key_ring" {
  description = "Name of the KMS key ring for CMEK encryption"
  type        = string
  default     = "pipeline-keyring"
}

variable "create_metadata_dataset" {
  description = "Whether to create a separate dataset for pipeline metadata"
  type        = bool
  default     = true
}

variable "create_quality_dataset" {
  description = "Whether to create a separate dataset for data quality metrics"
  type        = bool
  default     = true
}

variable "create_healing_dataset" {
  description = "Whether to create a separate dataset for self-healing data"
  type        = bool
  default     = true
}

variable "create_monitoring_dataset" {
  description = "Whether to create a separate dataset for monitoring metrics"
  type        = bool
  default     = true
}

variable "create_default_tables" {
  description = "Whether to create default tables for pipeline operations"
  type        = bool
  default     = true
}

variable "tables" {
  description = "Map of custom tables to create with their schemas and configuration"
  type = map(object({
    description      = string
    schema           = string
    partition_field  = optional(string)
    clustering_fields = optional(list(string))
  }))
  default = {}
}