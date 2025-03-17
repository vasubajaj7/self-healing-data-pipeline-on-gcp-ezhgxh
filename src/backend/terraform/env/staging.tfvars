# Core project configuration
project_id = "shp-staging-project"
region = "us-central1"
secondary_region = "us-east1"
environment = "staging"
resource_prefix = "shp"

# API and service enablement
enable_apis = true
enable_private_services = true

# Security configuration
enable_cmek = true  # Enable for security testing in staging

# Cloud Composer configuration
composer_node_count = 3
composer_machine_type = "n1-standard-4"  # More powerful than dev, less than prod
composer_disk_size_gb = 100
composer_env_variables = {
  ENVIRONMENT = "staging"
  DEBUG_MODE = "false"
  LOG_LEVEL = "INFO"
  ENABLE_SELF_HEALING = "true"
}

# BigQuery configuration
bigquery_dataset_name = "self_healing_pipeline_staging"

# Storage configuration for staging environment (intermediate retention)
storage_buckets = {
  raw_data = {
    location = "us-central1"
    storage_class = "STANDARD"
    versioning = true
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
          age = 90
          with_state = "ANY"
        }
      }
    ]
  },
  processed_data = {
    location = "us-central1"
    storage_class = "STANDARD"
    versioning = true
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
          age = 90
          with_state = "ANY"
        }
      }
    ]
  },
  backup = {
    location = "us-central1"
    storage_class = "STANDARD"
    versioning = true
    lifecycle_rules = [
      {
        action = {
          type = "SetStorageClass"
          storage_class = "NEARLINE"
        }
        condition = {
          age = 60
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "Delete"
        }
        condition = {
          age = 180
          with_state = "ANY"
        }
      }
    ]
  },
  temp = {
    location = "us-central1"
    storage_class = "STANDARD"
    versioning = false
    lifecycle_rules = [
      {
        action = {
          type = "Delete"
        }
        condition = {
          age = 7
          with_state = "ANY"
        }
      }
    ]
  }
}

# Labels for staging resources
labels = {
  environment = "staging"
  application = "self-healing-pipeline"
  managed-by = "terraform"
  team = "data-engineering"
  cost-center = "data-platform"
}

# Monitoring and alerting for staging
monitoring_notification_channels = {
  email = {
    type = "email"
    labels = {
      email_address = "staging-alerts@example.com"
    }
  },
  teams = {
    type = "webhook_tokenauth"
    labels = {
      url = "https://teams-webhook-staging.example.com/incoming"
      auth_token = "staging-token"
    }
  }
}

# Alert policies for the staging environment
monitoring_alert_policies = {
  pipeline_failure = {
    display_name = "Pipeline Failure - Staging"
    combiner = "OR"
    conditions = [
      {
        display_name = "Pipeline Execution Failure"
        condition_threshold = {
          filter = "resource.type = \"cloud_composer_environment\" AND metric.type = \"composer.googleapis.com/workflow/failed_dag_run_count\""
          duration = "300s"
          comparison = "COMPARISON_GT"
          threshold_value = 0
        }
      }
    ]
    notification_channels = ["email", "teams"]
  },
  data_quality_alert = {
    display_name = "Data Quality Alert - Staging"
    combiner = "OR"
    conditions = [
      {
        display_name = "Data Quality Validation Failure"
        condition_threshold = {
          filter = "resource.type = \"custom.googleapis.com\" AND metric.type = \"custom.googleapis.com/data_quality/validation_failure_count\""
          duration = "300s"
          comparison = "COMPARISON_GT"
          threshold_value = 5
        }
      }
    ]
    notification_channels = ["email", "teams"]
  },
  self_healing_failure = {
    display_name = "Self-Healing Failure - Staging"
    combiner = "OR"
    conditions = [
      {
        display_name = "Self-Healing Action Failure"
        condition_threshold = {
          filter = "resource.type = \"custom.googleapis.com\" AND metric.type = \"custom.googleapis.com/self_healing/action_failure_count\""
          duration = "300s"
          comparison = "COMPARISON_GT"
          threshold_value = 3
        }
      }
    ]
    notification_channels = ["email", "teams"]
  }
}

# BigQuery tables configuration
bigquery_tables = {
  pipeline_metrics = {
    description = "Pipeline performance metrics"
    schema = "src/backend/terraform/schemas/pipeline_metrics.json"
    partition_field = "collection_time"
    clustering_fields = ["metric_category", "metric_name"]
  },
  data_quality_metrics = {
    description = "Data quality metrics"
    schema = "src/backend/terraform/schemas/data_quality_metrics.json"
    partition_field = "validation_time"
    clustering_fields = ["dataset_name", "table_name"]
  },
  healing_actions = {
    description = "Self-healing action records"
    schema = "src/backend/terraform/schemas/healing_actions.json"
    partition_field = "execution_time"
    clustering_fields = ["issue_type", "action_type"]
  },
  alerts_history = {
    description = "Historical alert records"
    schema = "src/backend/terraform/schemas/alerts_history.json"
    partition_field = "created_at"
    clustering_fields = ["alert_type", "severity"]
  }
}

# Environment version configurations
composer_python_version = "3.9"
composer_airflow_version = "2.5.1"

# Network configuration
network_name = "shp-staging-network"
subnet_name = "shp-staging-subnet"
subnet_cidr = "10.0.0.0/20"
create_network = true

# Service account configuration
service_account_name = "shp-service-account"
service_account_roles = [
  "roles/bigquery.dataEditor",
  "roles/bigquery.jobUser",
  "roles/storage.objectAdmin",
  "roles/cloudfunctions.invoker",
  "roles/aiplatform.user",
  "roles/monitoring.viewer",
  "roles/logging.logWriter",
  "roles/secretmanager.secretAccessor"
]

# Cloud Functions configuration
functions_to_deploy = {
  data_quality_validator = {
    runtime = "python39"
    entry_point = "validate_data"
    source_dir = "src/backend/functions/data_quality"
    memory = 2048
    timeout = 540
    environment_variables = {
      ENVIRONMENT = "staging"
      LOG_LEVEL = "INFO"
    }
  },
  self_healing_processor = {
    runtime = "python39"
    entry_point = "process_healing"
    source_dir = "src/backend/functions/self_healing"
    memory = 2048
    timeout = 540
    environment_variables = {
      ENVIRONMENT = "staging"
      LOG_LEVEL = "INFO"
      MAX_RETRY_ATTEMPTS = "3"
    }
  },
  alert_notifier = {
    runtime = "python39"
    entry_point = "send_notification"
    source_dir = "src/backend/functions/alerting"
    memory = 1024
    timeout = 300
    environment_variables = {
      ENVIRONMENT = "staging"
      LOG_LEVEL = "INFO"
    }
  }
}

# High Availability configuration
enable_high_availability = true

# Vertex AI configuration
vertex_ai_region = "us-central1"
enable_vertex_ai_pipelines = true