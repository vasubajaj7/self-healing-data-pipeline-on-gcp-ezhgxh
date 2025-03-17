project_id        = "shp-dev-project"
region            = "us-central1"
secondary_region  = "us-east1"
environment       = "dev"
resource_prefix   = "shp"
enable_apis       = true

# Network configuration
enable_private_services = false
network_name            = "shp-dev-network"
subnet_name             = "shp-dev-subnet"
subnet_cidr             = "10.0.0.0/20"
create_network          = true

# Cloud Composer configuration
composer_node_count      = 3
composer_machine_type    = "n1-standard-2"
composer_disk_size_gb    = 50
composer_python_version  = "3.9"
composer_airflow_version = "2.5.1"
composer_env_variables   = {
  ENVIRONMENT = "dev"
  DEBUG_MODE  = "true"
  LOG_LEVEL   = "DEBUG"
}

# Security configuration
enable_cmek = false
enable_high_availability = false

# Service account configuration
service_account_name  = "shp-service-account"
service_account_roles = [
  "roles/bigquery.dataEditor",
  "roles/bigquery.jobUser",
  "roles/storage.objectAdmin",
  "roles/cloudfunctions.invoker",
  "roles/aiplatform.user",
  "roles/monitoring.viewer",
  "roles/logging.logWriter"
]

# BigQuery configuration
bigquery_dataset_name = "self_healing_pipeline_dev"
bigquery_tables = {
  pipeline_metrics = {
    description      = "Pipeline performance metrics"
    schema           = "src/backend/terraform/schemas/pipeline_metrics.json"
    partition_field  = "collection_time"
    clustering_fields = ["metric_category", "metric_name"]
  }
  data_quality_metrics = {
    description      = "Data quality metrics"
    schema           = "src/backend/terraform/schemas/data_quality_metrics.json"
    partition_field  = "validation_time"
    clustering_fields = ["dataset_name", "table_name"]
  }
}

# Storage configuration
storage_buckets = {
  raw_data = {
    location      = "us-central1"
    storage_class = "STANDARD"
    versioning    = true
    lifecycle_rules = [
      {
        action = {
          type = "SetStorageClass"
          storage_class = "NEARLINE"
        }
        condition = {
          age = 30
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "Delete"
        }
        condition = {
          age = 60
          with_state = "ANY"
        }
      }
    ]
  }
  processed_data = {
    location      = "us-central1"
    storage_class = "STANDARD"
    versioning    = true
    lifecycle_rules = [
      {
        action = {
          type = "SetStorageClass"
          storage_class = "NEARLINE"
        }
        condition = {
          age = 30
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "Delete"
        }
        condition = {
          age = 60
          with_state = "ANY"
        }
      }
    ]
  }
  temp = {
    location      = "us-central1"
    storage_class = "STANDARD"
    versioning    = false
    lifecycle_rules = [
      {
        action = {
          type = "Delete"
        }
        condition = {
          age = 3
          with_state = "ANY"
        }
      }
    ]
  }
}

# Resource labeling
labels = {
  environment = "dev"
  application = "self-healing-pipeline"
  managed-by  = "terraform"
  team        = "data-engineering"
}

# Monitoring configuration
monitoring_notification_channels = {
  email = {
    type = "email"
    labels = {
      email_address = "dev-alerts@example.com"
    }
  }
  teams = {
    type = "webhook_tokenauth"
    labels = {
      url = "https://teams-webhook-dev.example.com/incoming"
      auth_token = "dev-token"
    }
  }
}

# Alert policies with relaxed thresholds for development
monitoring_alert_policies = {
  pipeline_failure = {
    display_name = "Pipeline Failure - Development"
    combiner     = "OR"
    conditions = [
      {
        display_name = "Pipeline Execution Failure"
        condition_threshold = {
          filter          = "resource.type = \"cloud_composer_environment\" AND metric.type = \"composer.googleapis.com/workflow/failed_dag_run_count\""
          duration        = "300s"
          comparison      = "COMPARISON_GT"
          threshold_value = 0
        }
      }
    ]
    notification_channels = ["email", "teams"]
  }
  data_quality_alert = {
    display_name = "Data Quality Alert - Development"
    combiner     = "OR"
    conditions = [
      {
        display_name = "Data Quality Validation Failure"
        condition_threshold = {
          filter          = "resource.type = \"custom.googleapis.com\" AND metric.type = \"custom.googleapis.com/data_quality/validation_failure_count\""
          duration        = "300s"
          comparison      = "COMPARISON_GT"
          threshold_value = 10
        }
      }
    ]
    notification_channels = ["email", "teams"]
  }
}