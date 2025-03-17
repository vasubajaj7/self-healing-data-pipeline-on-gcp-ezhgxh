##############################################
# Self-Healing Data Pipeline - Variables
##############################################

# Core project configuration
variable "project_id" {
  description = "The Google Cloud Project ID where resources will be deployed"
  type        = string
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be set."
  }
}

variable "region" {
  description = "The primary Google Cloud region for resource deployment"
  type        = string
  default     = "us-central1"
  validation {
    condition     = length(var.region) > 0
    error_message = "The region variable must be set."
  }
}

variable "secondary_region" {
  description = "The secondary Google Cloud region for disaster recovery"
  type        = string
  default     = "us-east1"
  validation {
    condition     = length(var.secondary_region) > 0
    error_message = "The secondary_region variable must be set."
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
  description = "Labels to apply to all resources"
  type        = map(string)
  default = {
    application = "self-healing-pipeline"
    managed-by  = "terraform"
  }
}

# API and service enablement
variable "enable_apis" {
  description = "Whether to enable required Google Cloud APIs"
  type        = bool
  default     = true
}

variable "required_apis" {
  description = "List of Google Cloud APIs required for the pipeline"
  type        = list(string)
  default = [
    "compute.googleapis.com",
    "composer.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "aiplatform.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudkms.googleapis.com",
    "servicenetworking.googleapis.com",
    "vpcaccess.googleapis.com",
    "dns.googleapis.com"
  ]
}

# Security configuration
variable "enable_cmek" {
  description = "Whether to enable Customer Managed Encryption Keys"
  type        = bool
  default     = false
}

variable "service_account_name" {
  description = "Name of the service account for pipeline components"
  type        = string
  default     = "pipeline-sa"
}

# Networking configuration
variable "network_name" {
  description = "Name of the VPC network to use or create"
  type        = string
  default     = "pipeline-network"
}

variable "subnet_name" {
  description = "Name of the subnet to use or create"
  type        = string
  default     = "pipeline-subnet"
}

variable "subnet_cidr" {
  description = "CIDR range for the subnet"
  type        = string
  default     = "10.0.0.0/20"
}

variable "enable_private_services" {
  description = "Whether to enable private Google access for services"
  type        = bool
  default     = true
}

variable "create_network" {
  description = "Whether to create a new network or use an existing one"
  type        = bool
  default     = true
}

# BigQuery configuration
variable "bigquery_dataset_name" {
  description = "Name of the main BigQuery dataset"
  type        = string
  default     = "pipeline_data"
}

variable "bigquery_location" {
  description = "Location for BigQuery datasets"
  type        = string
  default     = "US"
}

variable "bigquery_tables" {
  description = "Map of BigQuery tables to create with their schemas and configuration"
  type = map(object({
    description       = string
    schema            = string
    partition_field   = optional(string)
    clustering_fields = optional(list(string))
  }))
  default = {}
}

# Storage configuration
variable "storage_buckets" {
  description = "Map of storage buckets to create with their configuration"
  type = map(object({
    location        = string
    storage_class   = string
    versioning      = bool
    lifecycle_rules = list(object({
      condition = object({
        age                   = optional(number)
        created_before        = optional(string)
        with_state            = optional(string)
        matches_storage_class = optional(list(string))
      })
      action = object({
        type          = string
        storage_class = optional(string)
      })
    }))
  }))
  default = {}
}

# Cloud Composer configuration
variable "composer_environment_name" {
  description = "Name of the Cloud Composer environment"
  type        = string
  default     = "pipeline-composer"
}

variable "composer_node_count" {
  description = "Number of nodes for the Cloud Composer environment"
  type        = number
  default     = 3
}

variable "composer_machine_type" {
  description = "Machine type for Cloud Composer nodes"
  type        = string
  default     = "n1-standard-2"
}

variable "composer_disk_size_gb" {
  description = "Disk size in GB for Cloud Composer nodes"
  type        = number
  default     = 100
}

variable "composer_python_version" {
  description = "Python version for Cloud Composer"
  type        = string
  default     = "3.9"
}

variable "composer_airflow_version" {
  description = "Airflow version for Cloud Composer"
  type        = string
  default     = "2.5.1"
}

variable "composer_env_variables" {
  description = "Environment variables for Cloud Composer"
  type        = map(string)
  default = {
    ENVIRONMENT = "var.environment"
    PROJECT_ID  = "var.project_id"
    REGION      = "var.region"
  }
}

variable "composer_admin_user" {
  description = "Email of the user to grant Composer admin access"
  type        = string
  default     = ""
}

# Vertex AI configuration
variable "vertex_ai_region" {
  description = "Region for Vertex AI resources"
  type        = string
  default     = "us-central1"
}

variable "enable_vertex_ai_pipelines" {
  description = "Whether to enable Vertex AI Pipelines"
  type        = bool
  default     = true
}

# Monitoring and alerting configuration
variable "monitoring_notification_channels" {
  description = "List of notification channel IDs for monitoring alerts"
  type        = list(string)
  default     = []
}

variable "alert_email_addresses" {
  description = "List of email addresses to receive alerts"
  type        = list(string)
  default     = []
}

variable "teams_webhook_url" {
  description = "Microsoft Teams webhook URL for alerts"
  type        = string
  default     = ""
  sensitive   = true
}

# High Availability configuration
variable "enable_high_availability" {
  description = "Whether to enable high availability configurations"
  type        = bool
  default     = false
}