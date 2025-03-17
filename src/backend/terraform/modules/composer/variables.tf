variable "project_id" {
  description = "The Google Cloud Project ID where the Composer environment will be deployed"
  type        = string
}

variable "region" {
  description = "The Google Cloud region where the Composer environment will be deployed"
  type        = string
}

variable "environment" {
  description = "The deployment environment (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "resource_prefix" {
  description = "Prefix to be used for resource names to ensure uniqueness"
  type        = string
  default     = "shp"
}

variable "composer_environment_name" {
  description = "Name of the Cloud Composer environment"
  type        = string
  default     = "pipeline"
}

variable "composer_node_count" {
  description = "Number of GKE nodes to provision for the Composer environment"
  type        = number
  default     = 3
}

variable "composer_machine_type" {
  description = "Machine type for the GKE nodes in the Composer environment"
  type        = string
  default     = "n1-standard-2"
}

variable "composer_disk_size_gb" {
  description = "Disk size in GB for the GKE nodes in the Composer environment"
  type        = number
  default     = 100
}

variable "composer_python_version" {
  description = "Python version for the Composer environment"
  type        = string
  default     = "3.9"
}

variable "composer_airflow_version" {
  description = "Airflow version for the Composer environment"
  type        = string
  default     = "2.5.1"
}

variable "composer_env_variables" {
  description = "Environment variables to set in the Composer environment"
  type        = map(string)
  default     = {}
}

variable "network_name" {
  description = "Name of the VPC network to use for the Composer environment"
  type        = string
  default     = ""
}

variable "subnet_name" {
  description = "Name of the subnet to use for the Composer environment"
  type        = string
  default     = ""
}

variable "create_network" {
  description = "Whether to create a new network and subnet for the Composer environment"
  type        = bool
  default     = true
}

variable "subnet_ip_range" {
  description = "IP range for the subnet if creating a new network"
  type        = string
  default     = "10.0.0.0/24"
}

variable "service_account_name" {
  description = "Name of the service account to create for the Composer environment"
  type        = string
  default     = "composer-sa"
}

variable "service_account_roles" {
  description = "List of IAM roles to assign to the Composer service account"
  type        = list(string)
  default = [
    "roles/composer.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/aiplatform.user",
    "roles/monitoring.viewer",
    "roles/logging.logWriter"
  ]
}

variable "labels" {
  description = "Labels to apply to the Composer environment"
  type        = map(string)
  default     = {}
}

variable "enable_private_environment" {
  description = "Whether to enable private IP for the Composer environment"
  type        = bool
  default     = false
}

variable "enable_private_builds" {
  description = "Whether to enable private builds for the Composer environment"
  type        = bool
  default     = true
}

variable "scheduler_count" {
  description = "Number of schedulers for Airflow"
  type        = number
  default     = 1
}

variable "worker_min_count" {
  description = "Minimum number of workers for Airflow"
  type        = number
  default     = 2
}

variable "worker_max_count" {
  description = "Maximum number of workers for Airflow"
  type        = number
  default     = 6
}

variable "maintenance_window_start_time" {
  description = "Start time for the maintenance window in RFC3339 format"
  type        = string
  default     = "2023-01-01T00:00:00Z"
}

variable "maintenance_window_end_time" {
  description = "End time for the maintenance window in RFC3339 format"
  type        = string
  default     = "2023-01-01T04:00:00Z"
}

variable "maintenance_window_recurrence" {
  description = "Recurrence pattern for the maintenance window in RRULE format"
  type        = string
  default     = "FREQ=WEEKLY;BYDAY=TU,TH"
}

variable "resilience_mode" {
  description = "Resilience mode for the Composer environment (STANDARD_RESILIENCE or HIGH_RESILIENCE)"
  type        = string
  default     = "STANDARD_RESILIENCE"
  validation {
    condition     = contains(["STANDARD_RESILIENCE", "HIGH_RESILIENCE"], var.resilience_mode)
    error_message = "Resilience mode must be one of: STANDARD_RESILIENCE, HIGH_RESILIENCE."
  }
}

variable "airflow_config_overrides" {
  description = "Airflow configuration overrides as key-value pairs"
  type        = map(string)
  default = {
    "core-dags_are_paused_at_creation" = "True"
    "core-parallelism"                 = "50"
    "scheduler-max_threads"            = "8"
    "webserver-dag_orientation"        = "TB"
    "webserver-dag_run_display_number" = "50"
  }
}

variable "pypi_packages" {
  description = "PyPI packages to install in the Composer environment as package-version pairs"
  type        = map(string)
  default = {
    "great-expectations"      = "0.15.50"
    "google-cloud-bigquery"   = "3.11.4"
    "google-cloud-storage"    = "2.9.0"
    "google-cloud-aiplatform"  = "1.25.0"
    "pandas"                  = "2.0.3"
    "numpy"                   = "1.24.3"
    "scikit-learn"            = "1.2.2"
    "pymsteams"               = "0.2.2"
  }
}

variable "scheduler_cpu" {
  description = "CPU allocation for Airflow scheduler"
  type        = number
  default     = 2
}

variable "scheduler_memory_gb" {
  description = "Memory allocation in GB for Airflow scheduler"
  type        = number
  default     = 7.5
}

variable "scheduler_storage_gb" {
  description = "Storage allocation in GB for Airflow scheduler"
  type        = number
  default     = 5
}

variable "web_server_cpu" {
  description = "CPU allocation for Airflow web server"
  type        = number
  default     = 2
}

variable "web_server_memory_gb" {
  description = "Memory allocation in GB for Airflow web server"
  type        = number
  default     = 4
}

variable "web_server_storage_gb" {
  description = "Storage allocation in GB for Airflow web server"
  type        = number
  default     = 5
}

variable "web_server_machine_type" {
  description = "Machine type for Airflow web server"
  type        = string
  default     = "composer-n1-webserver-2"
}

variable "worker_cpu" {
  description = "CPU allocation for Airflow workers"
  type        = number
  default     = 2
}

variable "worker_memory_gb" {
  description = "Memory allocation in GB for Airflow workers"
  type        = number
  default     = 7.5
}

variable "worker_storage_gb" {
  description = "Storage allocation in GB for Airflow workers"
  type        = number
  default     = 10
}

variable "enable_cmek" {
  description = "Whether to enable Customer-Managed Encryption Keys for the Composer environment"
  type        = bool
  default     = false
}

variable "kms_key_id" {
  description = "KMS key ID for CMEK encryption if enabled"
  type        = string
  default     = ""
}