# Variables for the Vertex AI module to support the self-healing data pipeline
# These variables define configuration options for Google Cloud Vertex AI resources
# including model registry, feature store, endpoints, and metadata store

variable "project_id" {
  description = "The Google Cloud Project ID where Vertex AI resources will be created"
  type        = string
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be set."
  }
}

variable "region" {
  description = "The Google Cloud region where Vertex AI resources will be created"
  type        = string
  default     = "us-central1"
  validation {
    condition     = length(var.region) > 0
    error_message = "The region variable must be set."
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
  description = "Labels to apply to all Vertex AI resources"
  type        = map(string)
  default     = {}
}

variable "enable_vertex_ai_pipelines" {
  description = "Whether to enable Vertex AI Pipelines for model training and deployment"
  type        = bool
  default     = true
}

variable "model_registry_name" {
  description = "Name for the Vertex AI Model Registry"
  type        = string
  default     = "self-healing-model-registry"
}

variable "featurestore_name" {
  description = "Name for the Vertex AI Featurestore"
  type        = string
  default     = "self-healing-featurestore"
}

variable "featurestore_online_serving_config" {
  description = "Configuration for online serving of features from the Featurestore"
  type = object({
    fixed_node_count = number
  })
  default = {
    fixed_node_count = 1
  }
}

variable "metadata_store_name" {
  description = "Name for the Vertex AI Metadata Store"
  type        = string
  default     = "self-healing-metadata-store"
}

variable "tensorboard_name" {
  description = "Name for the Vertex AI Tensorboard"
  type        = string
  default     = "self-healing-tensorboard"
}

variable "endpoints" {
  description = "Configuration for Vertex AI endpoints for model serving"
  type = map(object({
    display_name      = string
    machine_type      = string
    min_replica_count = number
    max_replica_count = number
  }))
  default = {
    "anomaly-detection" = {
      display_name      = "Anomaly Detection Endpoint"
      machine_type      = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    "data-correction" = {
      display_name      = "Data Correction Endpoint"
      machine_type      = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    "root-cause-analysis" = {
      display_name      = "Root Cause Analysis Endpoint"
      machine_type      = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    "predictive-failure" = {
      display_name      = "Predictive Failure Endpoint"
      machine_type      = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    }
  }
}

variable "network_name" {
  description = "Name of the VPC network for private Vertex AI resources"
  type        = string
  default     = null
}

variable "subnet_name" {
  description = "Name of the subnet for private Vertex AI resources"
  type        = string
  default     = null
}

variable "enable_private_endpoints" {
  description = "Whether to enable private endpoints for Vertex AI resources"
  type        = bool
  default     = false
}

variable "service_account_email" {
  description = "Email address of the service account to use for Vertex AI resources"
  type        = string
  default     = null
}

variable "encryption_key_name" {
  description = "Name of the CMEK encryption key for Vertex AI resources"
  type        = string
  default     = null
}

variable "enable_cmek" {
  description = "Whether to enable Customer Managed Encryption Keys for Vertex AI resources"
  type        = bool
  default     = false
}

variable "model_monitoring_config" {
  description = "Configuration for Vertex AI model monitoring"
  type = object({
    enable                  = bool
    monitoring_interval_days = number
    alert_email_addresses   = list(string)
  })
  default = {
    enable                  = true
    monitoring_interval_days = 1
    alert_email_addresses   = []
  }
}