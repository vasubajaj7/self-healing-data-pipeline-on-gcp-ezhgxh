project_id          = "shp-prod-project"
region              = "us-central1"
secondary_region    = "us-east1"
environment         = "prod"
resource_prefix     = "shp"

# API and service enablement
enable_apis             = true
enable_private_services = true

# Cloud Composer configuration
composer_node_count     = 5
composer_machine_type   = "n1-standard-8"
composer_disk_size_gb   = 200
composer_python_version = "3.9"
composer_airflow_version = "2.5.1"
composer_env_variables  = {
  ENVIRONMENT        = "prod"
  DEBUG_MODE         = "false"
  LOG_LEVEL          = "WARNING"
  ENABLE_SELF_HEALING = "true"
  MAX_RETRY_ATTEMPTS = "5"
  ALERT_THRESHOLD    = "high"
}

# Security configuration
enable_cmek = true

# BigQuery configuration
bigquery_dataset_name = "self_healing_pipeline_prod"
bigquery_location = "US"
bigquery_tables = {
  pipeline_metrics = {
    description = "Pipeline performance metrics"
    schema = "src/backend/terraform/schemas/pipeline_metrics.json"
    partition_field = "collection_time"
    clustering_fields = ["metric_category", "metric_name"]
  }
  data_quality_metrics = {
    description = "Data quality metrics"
    schema = "src/backend/terraform/schemas/data_quality_metrics.json"
    partition_field = "validation_time"
    clustering_fields = ["dataset_name", "table_name"]
  }
  healing_actions = {
    description = "Self-healing action records"
    schema = "src/backend/terraform/schemas/healing_actions.json"
    partition_field = "execution_time"
    clustering_fields = ["issue_type", "action_type"]
  }
  alerts_history = {
    description = "Historical alert records"
    schema = "src/backend/terraform/schemas/alerts_history.json"
    partition_field = "created_at"
    clustering_fields = ["alert_type", "severity"]
  }
  performance_optimization = {
    description = "Performance optimization metrics and recommendations"
    schema = "src/backend/terraform/schemas/performance_optimization.json"
    partition_field = "timestamp"
    clustering_fields = ["resource_type", "optimization_type"]
  }
  audit_logs = {
    description = "Audit logs for compliance and security"
    schema = "src/backend/terraform/schemas/audit_logs.json"
    partition_field = "timestamp"
    clustering_fields = ["service", "method", "resource_type"]
  }
}

# Storage buckets configuration
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
          age = 60
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "COLDLINE"
        }
        condition = {
          age = 180
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "ARCHIVE"
        }
        condition = {
          age = 365
          with_state = "ANY"
        }
      }
    ]
  }
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
          age = 60
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "COLDLINE"
        }
        condition = {
          age = 180
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "ARCHIVE"
        }
        condition = {
          age = 365
          with_state = "ANY"
        }
      }
    ]
  }
  backup = {
    location = "us-east1"
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
          type = "SetStorageClass"
          storage_class = "COLDLINE"
        }
        condition = {
          age = 90
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "ARCHIVE"
        }
        condition = {
          age = 365
          with_state = "ANY"
        }
      }
    ]
  }
  disaster_recovery = {
    location = "us-east1"
    storage_class = "STANDARD"
    versioning = true
    lifecycle_rules = [
      {
        action = {
          type = "SetStorageClass"
          storage_class = "NEARLINE"
        }
        condition = {
          age = 90
          with_state = "ANY"
        }
      },
      {
        action = {
          type = "SetStorageClass"
          storage_class = "COLDLINE"
        }
        condition = {
          age = 180
          with_state = "ANY"
        }
      }
    ]
  }
  archive = {
    location = "us-central1"
    storage_class = "COLDLINE"
    versioning = true
    lifecycle_rules = [
      {
        action = {
          type = "SetStorageClass"
          storage_class = "ARCHIVE"
        }
        condition = {
          age = 180
          with_state = "ANY"
        }
      }
    ]
  }
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
          age = 14
          with_state = "ANY"
        }
      }
    ]
  }
}

# Labels
labels = {
  environment   = "prod"
  application   = "self-healing-pipeline"
  managed-by    = "terraform"
  team          = "data-engineering"
  cost-center   = "data-platform"
  business-unit = "analytics"
  compliance    = "data-governance"
}

# Monitoring and alerting configuration
monitoring_notification_channels = {
  email = {
    type = "email"
    labels = {
      email_address = "prod-alerts@example.com"
    }
  }
  teams = {
    type = "webhook_tokenauth"
    labels = {
      url = "https://teams-webhook-prod.example.com/incoming"
      auth_token = "prod-token"
    }
  }
  sms = {
    type = "sms"
    labels = {
      number = "+15551234567"
    }
  }
}

# Alert policies
monitoring_alert_policies = {
  pipeline_failure = {
    display_name = "Pipeline Failure - Production"
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
    notification_channels = ["email", "teams", "sms"]
    severity = "CRITICAL"
  }
  data_quality_alert = {
    display_name = "Data Quality Alert - Production"
    combiner = "OR"
    conditions = [
      {
        display_name = "Data Quality Validation Failure"
        condition_threshold = {
          filter = "resource.type = \"custom.googleapis.com\" AND metric.type = \"custom.googleapis.com/data_quality/validation_failure_count\""
          duration = "300s"
          comparison = "COMPARISON_GT"
          threshold_value = 3
        }
      }
    ]
    notification_channels = ["email", "teams"]
    severity = "HIGH"
  }
  self_healing_failure = {
    display_name = "Self-Healing Failure - Production"
    combiner = "OR"
    conditions = [
      {
        display_name = "Self-Healing Action Failure"
        condition_threshold = {
          filter = "resource.type = \"custom.googleapis.com\" AND metric.type = \"custom.googleapis.com/self_healing/action_failure_count\""
          duration = "300s"
          comparison = "COMPARISON_GT"
          threshold_value = 2
        }
      }
    ]
    notification_channels = ["email", "teams"]
    severity = "HIGH"
  }
  resource_utilization = {
    display_name = "Resource Utilization - Production"
    combiner = "OR"
    conditions = [
      {
        display_name = "High CPU Utilization"
        condition_threshold = {
          filter = "resource.type = \"gce_instance\" AND metric.type = \"compute.googleapis.com/instance/cpu/utilization\""
          duration = "900s"
          comparison = "COMPARISON_GT"
          threshold_value = 0.8
        }
      },
      {
        display_name = "High Memory Utilization"
        condition_threshold = {
          filter = "resource.type = \"gce_instance\" AND metric.type = \"compute.googleapis.com/instance/memory/percent_used\""
          duration = "900s"
          comparison = "COMPARISON_GT"
          threshold_value = 0.85
        }
      }
    ]
    notification_channels = ["email", "teams"]
    severity = "WARNING"
  }
  bigquery_slot_utilization = {
    display_name = "BigQuery Slot Utilization - Production"
    combiner = "OR"
    conditions = [
      {
        display_name = "High Slot Utilization"
        condition_threshold = {
          filter = "resource.type = \"bigquery_project\" AND metric.type = \"bigquery.googleapis.com/reservation/slot_utilization\""
          duration = "1800s"
          comparison = "COMPARISON_GT"
          threshold_value = 0.9
        }
      }
    ]
    notification_channels = ["email", "teams"]
    severity = "WARNING"
  }
}

# Cloud Functions configuration
functions_to_deploy = {
  data_quality_validator = {
    runtime = "python39"
    entry_point = "validate_data"
    source_dir = "src/backend/functions/data_quality"
    memory = 4096
    timeout = 540
    min_instances = 1
    max_instances = 10
    environment_variables = {
      ENVIRONMENT = "prod"
      LOG_LEVEL = "WARNING"
    }
  }
  self_healing_processor = {
    runtime = "python39"
    entry_point = "process_healing"
    source_dir = "src/backend/functions/self_healing"
    memory = 4096
    timeout = 540
    min_instances = 1
    max_instances = 10
    environment_variables = {
      ENVIRONMENT = "prod"
      LOG_LEVEL = "WARNING"
      MAX_RETRY_ATTEMPTS = "5"
    }
  }
  alert_notifier = {
    runtime = "python39"
    entry_point = "send_notification"
    source_dir = "src/backend/functions/alerting"
    memory = 2048
    timeout = 300
    min_instances = 1
    max_instances = 5
    environment_variables = {
      ENVIRONMENT = "prod"
      LOG_LEVEL = "WARNING"
    }
  }
  optimization_analyzer = {
    runtime = "python39"
    entry_point = "analyze_optimization"
    source_dir = "src/backend/functions/optimization"
    memory = 4096
    timeout = 540
    min_instances = 0
    max_instances = 5
    environment_variables = {
      ENVIRONMENT = "prod"
      LOG_LEVEL = "WARNING"
    }
  }
}

# Network configuration
network_name = "shp-prod-network"
subnet_name = "shp-prod-subnet"
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
  "roles/secretmanager.secretAccessor",
  "roles/cloudkms.cryptoKeyEncrypterDecrypter"
]

# High availability & performance configuration
enable_high_availability = true
vertex_ai_region = "us-central1"
enable_vertex_ai_pipelines = true
bigquery_reservation = {
  slot_count = 100
  edition = "ENTERPRISE"
  concurrency = 50
}

# Maintenance configuration
backup_retention_days = 90
log_retention_days = 365
maintenance_window = {
  day = "SUNDAY"
  hour = 2
  duration_hours = 4
}