# This module provisions and configures a Cloud Composer (managed Apache Airflow) environment
# for the self-healing data pipeline. It handles the creation of the Composer environment,
# service account, IAM permissions, network configuration, and uploads DAGs, plugins, and 
# configuration files.

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

# Local variables for configuration
locals {
  composer_env_name = "${var.resource_prefix}-${var.composer_environment_name}-${var.environment}"
  network_self_link = var.create_network ? google_compute_network.composer_network[0].self_link : data.google_compute_network.existing_network[0].self_link
  subnet_self_link = var.create_network ? google_compute_subnetwork.composer_subnet[0].self_link : data.google_compute_subnetwork.existing_subnet[0].self_link
  composer_image_version = "composer-${var.composer_python_version}-airflow-${var.composer_airflow_version}"
  composer_env_variables_with_defaults = merge({
    ENVIRONMENT: var.environment,
    PROJECT_ID: var.project_id,
    REGION: var.region,
    ENABLE_SELF_HEALING: "true",
    LOG_LEVEL: var.environment == "prod" ? "WARNING" : "INFO"
  }, var.composer_env_variables)
}

# Data sources for existing network resources if not creating new ones
data "google_compute_network" "existing_network" {
  count   = var.create_network ? 0 : 1
  name    = var.network_name
  project = var.project_id
}

data "google_compute_subnetwork" "existing_subnet" {
  count   = var.create_network ? 0 : 1
  name    = var.subnet_name
  project = var.project_id
  region  = var.region
}

# Service account for the Composer environment
resource "google_service_account" "composer_service_account" {
  account_id   = "${var.resource_prefix}-${var.service_account_name}-${var.environment}"
  display_name = "Cloud Composer Service Account"
  description  = "Service account for Cloud Composer environment in the self-healing data pipeline"
  project      = var.project_id
}

# IAM role assignments for the Composer service account
resource "google_project_iam_member" "composer_service_account_roles" {
  for_each = toset(var.service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.composer_service_account.email}"
}

# VPC network for the Composer environment (if requested)
resource "google_compute_network" "composer_network" {
  count                   = var.create_network ? 1 : 0
  name                    = "${var.resource_prefix}-${var.network_name}-${var.environment}"
  project                 = var.project_id
  auto_create_subnetworks = false
  description             = "VPC network for Cloud Composer environment in the self-healing data pipeline"
}

# Subnet for the Composer environment (if creating network)
resource "google_compute_subnetwork" "composer_subnet" {
  count                    = var.create_network ? 1 : 0
  name                     = "${var.resource_prefix}-${var.subnet_name}-${var.environment}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.composer_network[0].self_link
  ip_cidr_range            = var.subnet_ip_range
  private_ip_google_access = true
  
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.10.0.0/16"
  }
  
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.20.0.0/16"
  }
}

# Cloud Composer environment
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
      
      network = local.network_self_link
      subnetwork = local.subnet_self_link
      
      ip_allocation_policy {
        use_ip_aliases               = true
        cluster_secondary_range_name = "pods"
        services_secondary_range_name = "services"
      }
    }

    software_config {
      image_version = local.composer_image_version
      python_version = var.composer_python_version
      env_variables = local.composer_env_variables_with_defaults
      airflow_config_overrides = var.airflow_config_overrides
      pypi_packages = var.pypi_packages
    }

    private_environment_config {
      enable_private_endpoint = var.enable_private_environment
      enable_private_builds = var.enable_private_builds
    }

    maintenance_window {
      start_time = var.maintenance_window_start_time
      end_time = var.maintenance_window_end_time
      recurrence = var.maintenance_window_recurrence
    }

    workloads_config {
      scheduler {
        cpu = var.scheduler_cpu
        memory_gb = var.scheduler_memory_gb
        storage_gb = var.scheduler_storage_gb
        count = var.scheduler_count
      }
      
      web_server {
        cpu = var.web_server_cpu
        memory_gb = var.web_server_memory_gb
        storage_gb = var.web_server_storage_gb
        machine_type = var.web_server_machine_type
      }
      
      worker {
        cpu = var.worker_cpu
        memory_gb = var.worker_memory_gb
        storage_gb = var.worker_storage_gb
        min_count = var.worker_min_count
        max_count = var.worker_max_count
      }
    }

    resilience_mode = var.resilience_mode
    
    encryption_config {
      kms_key_name = var.enable_cmek ? var.kms_key_id : null
    }
  }

  depends_on = [
    google_service_account.composer_service_account,
    google_project_iam_member.composer_service_account_roles
  ]
}

# Upload DAG files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_dags" {
  for_each = fileset("${path.module}/../../../airflow/dags", "*.py")
  
  name        = "dags/${each.value}"
  bucket      = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source      = "${path.module}/../../../airflow/dags/${each.value}"
  content_type = "application/octet-stream"
}

# Upload plugin files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_plugins" {
  for_each = fileset("${path.module}/../../../airflow/plugins", "**/*.py")
  
  name        = "plugins/${each.value}"
  bucket      = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source      = "${path.module}/../../../airflow/plugins/${each.value}"
  content_type = "application/octet-stream"
}

# Upload configuration files to the Composer environment's bucket
resource "google_storage_bucket_object" "composer_configs" {
  for_each = fileset("${path.module}/../../../airflow/config", "*.json")
  
  name        = "config/${each.value}"
  bucket      = trimsuffix(google_composer_environment.composer_environment.config.0.dag_gcs_prefix, "/dags")
  source      = "${path.module}/../../../airflow/config/${each.value}"
  content_type = "application/json"
}

# Alert policy for monitoring Composer environment health
resource "google_monitoring_alert_policy" "composer_health_alert" {
  display_name = "Composer Environment Health - ${local.composer_env_name}"
  project      = var.project_id
  combiner     = "OR"
  
  conditions {
    display_name = "Composer Environment Unhealthy"
    
    condition_threshold {
      filter      = "resource.type = \"cloud_composer_environment\" AND resource.labels.environment_name = \"${local.composer_env_name}\" AND metric.type = \"composer.googleapis.com/environment/healthy\""
      duration    = "300s"
      comparison  = "COMPARISON_LT"
      threshold_value = 1
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  documentation {
    content   = "The Cloud Composer environment ${local.composer_env_name} is reporting an unhealthy state. This may indicate issues with the environment that could affect pipeline execution. Please investigate the environment health in the Google Cloud Console."
    mime_type = "text/markdown"
  }
  
  depends_on = [google_composer_environment.composer_environment]
}