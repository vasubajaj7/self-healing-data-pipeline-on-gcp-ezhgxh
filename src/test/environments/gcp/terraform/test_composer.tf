# Terraform configuration for the test Cloud Composer environment
# This creates a smaller, cost-effective Composer environment for testing
# the self-healing data pipeline with appropriate isolation from production.

# Local variables for the test Composer environment
locals {
  # Create a unique name for the test Composer environment
  test_composer_env_name = "${var.resource_prefix}-${var.composer_test_environment_name}-${random_id.test_environment_suffix.hex}"
  
  # Environment variables for the test Composer environment
  composer_env_variables = {
    ENVIRONMENT       = "test"
    PROJECT_ID        = "${var.project_id}"
    REGION            = "${var.region}"
    ENABLE_SELF_HEALING = "true"
    LOG_LEVEL         = "DEBUG"
    TEST_MODE         = "true"
    TEST_ENVIRONMENT_ID = "${random_id.test_environment_suffix.hex}"
  }
  
  # Airflow configuration overrides for test environment
  airflow_config_overrides = {
    "core-dags_are_paused_at_creation" = "False"
    "core-parallelism"                = "20"
    "scheduler-max_threads"           = "4"
    "webserver-dag_orientation"       = "TB"
    "webserver-dag_run_display_number" = "20"
  }
  
  # Python packages needed for testing
  test_pypi_packages = {
    "great-expectations" = "0.15.50"
    "google-cloud-bigquery" = "3.11.4"
    "google-cloud-storage" = "2.9.0"
    "google-cloud-aiplatform" = "1.25.0"
    "pandas" = "2.0.3"
    "numpy" = "1.24.3"
    "scikit-learn" = "1.2.2"
    "pymsteams" = "0.2.2"
    "pytest" = "7.3.1"
    "pytest-airflow" = "0.1.0"
  }
}

# Create the test Cloud Composer environment
resource "google_composer_environment" "test_composer" {
  count    = var.enable_composer_test_environment ? 1 : 0
  name     = local.test_composer_env_name
  project  = var.project_id
  region   = var.region
  labels   = merge(var.labels, { "test-environment-id": random_id.test_environment_suffix.hex })
  
  config {
    node_count = var.composer_test_node_count
    
    node_config {
      zone         = "${var.region}-a"
      machine_type = var.composer_test_machine_type
      disk_size_gb = 50
      service_account = google_service_account.test_service_account.email
      oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
      
      network    = google_compute_network.test_network.id
      subnetwork = google_compute_subnetwork.test_subnet.id
      
      ip_allocation_policy {
        use_ip_aliases              = true
        cluster_secondary_range_name = "pods"
        services_secondary_range_name = "services"
      }
    }
    
    software_config {
      image_version = "composer-2.5.1-airflow-2.5.1"
      python_version = "3.9"
      env_variables = local.composer_env_variables
      airflow_config_overrides = local.airflow_config_overrides
      pypi_packages = local.test_pypi_packages
    }
    
    private_environment_config {
      enable_private_endpoint = false
      enable_private_builds = true
    }
    
    maintenance_window {
      start_time = "2023-01-01T00:00:00Z"
      end_time   = "2023-01-01T04:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA"
    }
    
    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      
      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }
      
      worker {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 2
        min_count  = 1
        max_count  = 3
      }
    }
    
    environment_size = "ENVIRONMENT_SIZE_SMALL"
    resilience_mode = "STANDARD_RESILIENCE"
  }
  
  depends_on = [
    google_compute_network.test_network,
    google_compute_subnetwork.test_subnet,
    google_service_account.test_service_account,
    google_project_iam_member.test_service_account_roles,
    time_sleep.wait_for_iam_propagation
  ]
}

# Upload test DAG files to the Composer environment's bucket
resource "google_storage_bucket_object" "test_dags" {
  for_each = var.enable_composer_test_environment ? fileset("${path.module}/../../../test/mock_data/airflow/dags", "*.py") : {}
  
  name        = "dags/${each.value}"
  bucket      = var.enable_composer_test_environment ? trimsuffix(google_composer_environment.test_composer[0].config[0].dag_gcs_prefix, "/dags") : ""
  source      = "${path.module}/../../../test/mock_data/airflow/dags/${each.value}"
  content_type = "application/octet-stream"
}

# Upload test plugin files to the Composer environment's bucket
resource "google_storage_bucket_object" "test_plugins" {
  for_each = var.enable_composer_test_environment ? fileset("${path.module}/../../../test/mock_data/airflow/plugins", "**/*.py") : {}
  
  name        = "plugins/${each.value}"
  bucket      = var.enable_composer_test_environment ? trimsuffix(google_composer_environment.test_composer[0].config[0].dag_gcs_prefix, "/dags") : ""
  source      = "${path.module}/../../../test/mock_data/airflow/plugins/${each.value}"
  content_type = "application/octet-stream"
}

# Upload test configuration files to the Composer environment's bucket
resource "google_storage_bucket_object" "test_configs" {
  for_each = var.enable_composer_test_environment ? fileset("${path.module}/../../../test/mock_data/airflow/config", "*.json") : {}
  
  name        = "config/${each.value}"
  bucket      = var.enable_composer_test_environment ? trimsuffix(google_composer_environment.test_composer[0].config[0].dag_gcs_prefix, "/dags") : ""
  source      = "${path.module}/../../../test/mock_data/airflow/config/${each.value}"
  content_type = "application/json"
}

# Perform additional setup for the test Composer environment
resource "null_resource" "composer_test_setup" {
  count = var.enable_composer_test_environment ? 1 : 0
  
  triggers = {
    composer_id = var.enable_composer_test_environment ? google_composer_environment.test_composer[0].id : ""
    test_environment_id = random_id.test_environment_suffix.hex
  }
  
  provisioner "local-exec" {
    command = "echo 'Test Composer environment ${local.test_composer_env_name} has been created with Airflow UI at ${var.enable_composer_test_environment ? google_composer_environment.test_composer[0].config[0].airflow_uri : "not enabled"}'"
  }
  
  depends_on = [
    google_composer_environment.test_composer,
    google_storage_bucket_object.test_dags,
    google_storage_bucket_object.test_plugins,
    google_storage_bucket_object.test_configs
  ]
}

# Output the test Composer environment name
output "test_composer_environment_name" {
  description = "The name of the test Composer environment"
  value       = var.enable_composer_test_environment ? google_composer_environment.test_composer[0].name : ""
}

# Output the Airflow web UI URI for the test environment
output "test_composer_airflow_uri" {
  description = "The URI of the Airflow web UI for the test environment"
  value       = var.enable_composer_test_environment ? google_composer_environment.test_composer[0].config[0].airflow_uri : ""
}

# Output the GCS DAG prefix for the test environment
output "test_composer_dag_gcs_prefix" {
  description = "The GCS prefix where DAGs should be uploaded for the test environment"
  value       = var.enable_composer_test_environment ? google_composer_environment.test_composer[0].config[0].dag_gcs_prefix : ""
}

# Output the GCS bucket for the test Composer environment
output "test_composer_gcs_bucket" {
  description = "The GCS bucket associated with the test Composer environment"
  value       = var.enable_composer_test_environment ? regex("gs://([^/]+)/", google_composer_environment.test_composer[0].config[0].dag_gcs_prefix)[0] : ""
}