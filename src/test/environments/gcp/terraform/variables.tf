variable "project_id" {
  description = "The Google Cloud Project ID where test resources will be deployed"
  type        = string
  
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be provided."
  }
}

variable "region" {
  description = "The Google Cloud region where test resources will be deployed"
  type        = string
  default     = "us-central1"
  
  validation {
    condition     = length(var.region) > 0
    error_message = "The region variable must be provided."
  }
}

variable "test_environment_id" {
  description = "A unique identifier for the test environment, used to prevent resource name collisions"
  type        = string
}

variable "resource_prefix" {
  description = "Prefix to be applied to all test resource names for identification"
  type        = string
  default     = "shp-test"
}

variable "labels" {
  description = "Labels to apply to all test resources for organization and cost tracking"
  type        = map(string)
  default     = {
    environment = "test"
    project     = "self-healing-pipeline"
    managed-by  = "terraform"
  }
}

variable "auto_destroy_test_environment" {
  description = "Whether to automatically destroy the test environment after tests are complete"
  type        = bool
  default     = true
}

variable "test_environment_ttl" {
  description = "Time to live in hours for the test environment before automatic destruction"
  type        = number
  default     = 24
}

variable "test_network_name" {
  description = "Name of the VPC network for test resources"
  type        = string
  default     = "shp-test-network"
}

variable "test_subnet_name" {
  description = "Name of the subnet for test resources"
  type        = string
  default     = "shp-test-subnet"
}

variable "test_subnet_cidr" {
  description = "CIDR range for the test subnet"
  type        = string
  default     = "10.0.0.0/20"
}

variable "test_service_account_name" {
  description = "Name of the service account for test resources"
  type        = string
  default     = "shp-test-sa"
}

variable "test_service_account_roles" {
  description = "IAM roles to assign to the test service account"
  type        = list(string)
  default     = [
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/composer.worker",
    "roles/aiplatform.user",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ]
}

variable "test_bigquery_dataset_name" {
  description = "Name of the BigQuery dataset for test data"
  type        = string
  default     = "shp_test_data"
}

variable "test_bigquery_location" {
  description = "Location for test BigQuery datasets"
  type        = string
  default     = "US"
}

variable "test_data_bucket_name" {
  description = "Name of the GCS bucket to store test data files"
  type        = string
}

variable "enable_composer_test_environment" {
  description = "Whether to create a Cloud Composer environment for testing"
  type        = bool
  default     = false
}

variable "composer_test_environment_name" {
  description = "Name of the Cloud Composer environment for testing"
  type        = string
  default     = "shp-test-composer"
}

variable "composer_test_node_count" {
  description = "Number of nodes for the test Cloud Composer environment"
  type        = number
  default     = 3
}

variable "composer_test_machine_type" {
  description = "Machine type for test Cloud Composer nodes"
  type        = string
  default     = "n1-standard-2"
}

variable "enable_vertex_ai_test_environment" {
  description = "Whether to create Vertex AI resources for testing"
  type        = bool
  default     = false
}

variable "vertex_ai_region" {
  description = "Region for Vertex AI resources"
  type        = string
  default     = "us-central1"
}

variable "test_model_display_name" {
  description = "Display name for test ML models"
  type        = string
  default     = "shp-test-model"
}

variable "test_endpoint_display_name" {
  description = "Display name for test ML model endpoints"
  type        = string
  default     = "shp-test-endpoint"
}

variable "enable_monitoring_test_resources" {
  description = "Whether to create Cloud Monitoring resources for testing"
  type        = bool
  default     = true
}

variable "alert_notification_channels" {
  description = "List of notification channel IDs for test alerts"
  type        = list(string)
  default     = []
}