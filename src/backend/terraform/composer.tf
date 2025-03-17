# Provider configurations are already defined in networking.tf

# Local variables for Cloud Composer configuration
locals {
  # Formatted name for the Cloud Composer environment
  composer_env_name = "${var.resource_prefix}-${var.composer_environment_name}-${var.environment}"
  
  # Number of schedulers based on high availability setting
  composer_scheduler_count = var.enable_high_availability ? 2 : 1
  
  # Minimum number of workers based on high availability setting
  composer_worker_min_count = var.enable_high_availability ? 3 : 2
  
  # Maximum number of workers based on high availability setting
  composer_worker_max_count = var.enable_high_availability ? 8 : 6
  
  # Resilience mode based on high availability setting
  composer_resilience_mode = var.enable_high_availability ? "HIGH_RESILIENCE" : "STANDARD_RESILIENCE"
  
  # Environment variables with default values merged with user-provided variables
  composer_env_variables_with_defaults = merge({
    ENVIRONMENT: var.environment,
    PROJECT_ID: var.project_id,
    REGION: var.region,
    ENABLE_SELF_HEALING: "true",
    LOG_LEVEL: var.environment == "prod" ? "WARNING" : "INFO"
  }, var.composer_env_variables)
  
  # Airflow configuration overrides with values based on high availability setting
  airflow_config_overrides = {
    "core-dags_are_paused_at_creation": "True",
    "core-parallelism": var.enable_high_availability ? "100" : "50",
    "scheduler-max_threads": var.enable_high_availability ? "16" : "8",
    "webserver-dag_orientation": "TB",
    "webserver-dag_run_display_number": "50"
  }
  
  # Python packages to install in the Composer environment
  pypi_packages = {
    "great-expectations": "0.15.50",
    "google-cloud-bigquery": "3.11.4",
    "google-cloud-storage": "2.9.0",
    "google-cloud-aiplatform": "1.25.0",
    "pandas": "2.0.3",
    "numpy": "1.24.3",
    "scikit-learn": "1.2.2",
    "pymsteams": "0.2.2"
  }
}

# Data source to get project information
data "google_project" "project" {
  project_id = var.project_id
}

# Create a service account for the Cloud Composer environment
resource "google_service_account" "composer_service_account" {
  account_id   = "${var.resource_prefix}-composer-sa-${var.environment}"
  display_name = "Cloud Composer Service Account"
  description  = "Service account for Cloud Composer environment in the self-healing data pipeline"
  project      = var.project_id
}

# Assign IAM roles to the Composer service account
resource "google_project_iam_member" "composer_service_account_roles" {
  for_each = toset([
    "roles/composer.worker", 
    "roles/bigquery.dataEditor", 
    "roles/bigquery.jobUser", 
    "roles/storage.objectAdmin", 
    "roles/aiplatform.user", 
    "roles/monitoring.viewer", 
    "roles/logging.logWriter"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
}

# Create the Cloud Composer environment
resource "google_composer_environment" "composer_environment" {
  name    = local.composer_env_name
  project = var.project_id
  region  = var.region
  labels  = var.labels

  config {
    node_count = var.composer_node_count
    
    node_config {
      zone         = "${var.region}-a"
      machine_type = var.composer_machine_type
      disk_size_gb = var.composer_disk_size_gb
      service_account = google_service_account.composer_service_account.email
      oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
      
      # Network configuration
      network    = google_compute_network.vpc_network.self_link
      subnetwork = google_compute_subnetwork.vpc_subnet.self_link
      
      # IP allocation for GKE cluster
      ip_allocation_policy {
        cluster_secondary_range_name  = "pods"
        services_secondary_range_name = "services"
      }
    }

    # Software configuration
    software_config {
      image_version = "composer-2.5.1-airflow-${var.composer_airflow_version}"
      python_version = var.composer_python_version
      
      # Environment variables
      env_variables = local.composer_env_variables_with_defaults
      
      # Airflow configuration overrides
      airflow_config_overrides = local.airflow_config_overrides
      
      # Python packages to install
      pypi_packages = local.pypi_packages
    }

    # Private environment configuration
    private_environment_config {
      enable_private_endpoint = var.environment == "prod" ? true : false
      enable_private_builds   = true
    }

    # Maintenance window configuration
    maintenance_window {
      start_time = "2023-01-01T${var.environment == "prod" ? "02" : "00"}:00:00Z"
      end_time   = "2023-01-01T${var.environment == "prod" ? "06" : "04"}:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=TU,TH"
    }

    # Workload configuration for various components
    workloads_config {
      scheduler {
        cpu         = 2
        memory_gb   = 7.5
        storage_gb  = 5
        count       = local.composer_scheduler_count
      }

      web_server {
        cpu         = 2
        memory_gb   = 4
        storage_gb  = 5
      }

      worker {
        cpu         = 2
        memory_gb   = 7.5
        storage_gb  = 10
        min_count   = local.composer_worker_min_count
        max_count   = local.composer_worker_max_count
      }
    }

    # Resilience mode configuration
    resilience_mode = local.composer_resilience_mode

    # Environment size
    environment_size = var.environment == "prod" ? "ENVIRONMENT_SIZE_LARGE" : "ENVIRONMENT_SIZE_MEDIUM"

    # Database configuration
    database_config {
      machine_type = var.environment == "prod" ? "db-n1-standard-4" : "db-n1-standard-2"
    }

    # Web server configuration
    web_server_config {
      machine_type = var.environment == "prod" ? "composer-n1-webserver-4" : "composer-n1-webserver-2"
    }

    # Encryption configuration
    encryption_config {
      kms_key_name = var.enable_cmek ? google_kms_crypto_key.pipeline_crypto_key[0].id : null
    }
  }

  depends_on = [
    google_service_account.pipeline_service_account,
    google_project_iam_member.pipeline_service_account_roles,
    google_compute_network.vpc_network,
    google_compute_subnetwork.vpc_subnet
  ]
}

# Upload DAG files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_dags" {
  for_each     = fileset("${path.module}/../../airflow/dags", "*.py")
  name         = "dags/${each.value}"
  bucket       = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source       = "${path.module}/../../airflow/dags/${each.value}"
  content_type = "application/octet-stream"
}

# Upload plugin files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_plugins" {
  for_each     = fileset("${path.module}/../../airflow/plugins", "**/*.py")
  name         = "plugins/${each.value}"
  bucket       = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source       = "${path.module}/../../airflow/plugins/${each.value}"
  content_type = "application/octet-stream"
}

# Upload configuration files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_configs" {
  for_each     = fileset("${path.module}/../../airflow/config", "*.json")
  name         = "config/${each.value}"
  bucket       = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source       = "${path.module}/../../airflow/config/${each.value}"
  content_type = "application/json"
}

# Create an alert policy for Composer environment health
resource "google_monitoring_alert_policy" "composer_health_alert" {
  display_name = "Composer Environment Health - ${local.composer_env_name}"
  project      = var.project_id
  combiner     = "OR"
  
  conditions {
    display_name = "Composer Environment Unhealthy"
    condition_threshold {
      filter = "resource.type = \"cloud_composer_environment\" AND resource.labels.environment_name = \"${local.composer_env_name}\" AND metric.type = \"composer.googleapis.com/environment/healthy\""
      duration = "300s"
      comparison = "COMPARISON_LT"
      threshold_value = 1
      
      aggregations {
        alignment_period = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = var.monitoring_notification_channels
  
  alert_strategy {
    auto_close = "86400s"
  }
  
  documentation {
    content = "The Cloud Composer environment ${local.composer_env_name} is reporting an unhealthy state. This may indicate issues with the environment that could affect pipeline execution. Please investigate the environment health in the Google Cloud Console."
    mime_type = "text/markdown"
  }
  
  depends_on = [google_composer_environment.composer_environment]
}

# Output values
output "composer_environment_id" {
  value       = google_composer_environment.composer_environment.id
  description = "The ID of the created Cloud Composer environment"
}

output "composer_environment_name" {
  value       = google_composer_environment.composer_environment.name
  description = "The name of the created Cloud Composer environment"
}

output "composer_gcs_bucket" {
  value       = regex("gs://([^/]+)/", google_composer_environment.composer_environment.config.0.dag_gcs_prefix)[0]
  description = "The GCS bucket associated with the Composer environment"
}

output "composer_airflow_uri" {
  value       = google_composer_environment.composer_environment.config.0.airflow_uri
  description = "The URI of the Airflow web UI"
}

output "composer_dag_gcs_prefix" {
  value       = google_composer_environment.composer_environment.config.0.dag_gcs_prefix
  description = "The GCS prefix where DAGs should be uploaded"
}

output "composer_service_account" {
  value       = google_service_account.composer_service_account.email
  description = "The service account email used by the Composer environment"
}