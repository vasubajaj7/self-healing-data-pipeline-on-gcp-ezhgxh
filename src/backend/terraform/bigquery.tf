# BigQuery resources for Self-Healing Data Pipeline

locals {
  # Dataset names for different components
  bigquery_dataset_names = {
    main      = "${var.resource_prefix}_${var.bigquery_dataset_name}_${var.environment}"
    metadata  = "${var.resource_prefix}_metadata_${var.environment}"
    quality   = "${var.resource_prefix}_quality_${var.environment}"
    healing   = "${var.resource_prefix}_healing_${var.environment}"
    monitoring = "${var.resource_prefix}_monitoring_${var.environment}"
  }

  # KMS key for BigQuery CMEK encryption if enabled
  bigquery_kms_key = var.enable_cmek ? google_kms_crypto_key.bigquery_key.id : null

  # Default table schemas loaded from JSON files
  default_table_schemas = {
    source_systems = file("${path.module}/schemas/source_systems.json")
    pipeline_executions = file("${path.module}/schemas/pipeline_executions.json")
    task_executions = file("${path.module}/schemas/task_executions.json")
    quality_validations = file("${path.module}/schemas/quality_validations.json")
    healing_executions = file("${path.module}/schemas/healing_executions.json")
    pipeline_metrics = file("${path.module}/schemas/pipeline_metrics.json")
    alerts = file("${path.module}/schemas/alerts.json")
  }

  # Common labels to apply to all resources
  common_labels = merge(var.labels, {
    environment = var.environment
    project     = var.project_id
    terraform   = "true"
    component   = "bigquery"
  })

  # Flag for production environment
  is_production = var.environment == "prod"
}

# Create main BigQuery dataset for pipeline data
resource "google_bigquery_dataset" "main_dataset" {
  dataset_id                  = local.bigquery_dataset_names.main
  project                     = var.project_id
  location                    = var.bigquery_location
  description                 = "Main dataset for self-healing data pipeline analytics"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"
  labels                      = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create metadata dataset for pipeline operations
resource "google_bigquery_dataset" "metadata_dataset" {
  dataset_id                  = local.bigquery_dataset_names.metadata
  project                     = var.project_id
  location                    = var.bigquery_location
  description                 = "Dataset for pipeline metadata, execution history, and operational data"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"
  labels                      = local.common_labels
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create quality dataset for data quality monitoring
resource "google_bigquery_dataset" "quality_dataset" {
  dataset_id                  = local.bigquery_dataset_names.quality
  project                     = var.project_id
  location                    = var.bigquery_location
  description                 = "Dataset for data quality metrics, validation results, and quality trends"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"
  labels                      = local.common_labels
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create healing dataset for AI model data
resource "google_bigquery_dataset" "healing_dataset" {
  dataset_id                  = local.bigquery_dataset_names.healing
  project                     = var.project_id
  location                    = var.bigquery_location
  description                 = "Dataset for self-healing AI models, training data, and healing metrics"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"
  labels                      = local.common_labels
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create monitoring dataset for pipeline monitoring
resource "google_bigquery_dataset" "monitoring_dataset" {
  dataset_id                  = local.bigquery_dataset_names.monitoring
  project                     = var.project_id
  location                    = var.bigquery_location
  description                 = "Dataset for pipeline monitoring metrics, alerts, and performance data"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.environment != "prod"
  labels                      = local.common_labels
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create source systems table for tracking data sources
resource "google_bigquery_table" "source_systems_table" {
  dataset_id          = google_bigquery_dataset.metadata_dataset.dataset_id
  table_id            = "source_systems"
  project             = var.project_id
  description         = "Metadata about data source systems"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.source_systems
  labels              = local.common_labels
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create pipeline executions table for tracking pipeline runs
resource "google_bigquery_table" "pipeline_executions_table" {
  dataset_id          = google_bigquery_dataset.metadata_dataset.dataset_id
  table_id            = "pipeline_executions"
  project             = var.project_id
  description         = "Metadata about pipeline executions"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.pipeline_executions
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "start_time"
    expiration_ms = null
  }
  
  clustering {
    fields = ["pipeline_id", "status"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create task executions table for tracking individual task runs
resource "google_bigquery_table" "task_executions_table" {
  dataset_id          = google_bigquery_dataset.metadata_dataset.dataset_id
  table_id            = "task_executions"
  project             = var.project_id
  description         = "Metadata about task executions within pipelines"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.task_executions
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "start_time"
    expiration_ms = null
  }
  
  clustering {
    fields = ["execution_id", "task_id"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create quality validations table for tracking data quality checks
resource "google_bigquery_table" "quality_validations_table" {
  dataset_id          = google_bigquery_dataset.quality_dataset.dataset_id
  table_id            = "quality_validations"
  project             = var.project_id
  description         = "Results of data quality validations"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.quality_validations
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "validation_time"
    expiration_ms = null
  }
  
  clustering {
    fields = ["execution_id", "passed"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create healing executions table for tracking self-healing actions
resource "google_bigquery_table" "healing_executions_table" {
  dataset_id          = google_bigquery_dataset.healing_dataset.dataset_id
  table_id            = "healing_executions"
  project             = var.project_id
  description         = "Record of self-healing actions taken"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.healing_executions
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "execution_time"
    expiration_ms = null
  }
  
  clustering {
    fields = ["pattern_id", "successful"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create pipeline metrics table for tracking performance metrics
resource "google_bigquery_table" "pipeline_metrics_table" {
  dataset_id          = google_bigquery_dataset.monitoring_dataset.dataset_id
  table_id            = "pipeline_metrics"
  project             = var.project_id
  description         = "Performance and operational metrics for pipelines"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.pipeline_metrics
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "collection_time"
    expiration_ms = null
  }
  
  clustering {
    fields = ["metric_category", "metric_name"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create alerts table for tracking system alerts
resource "google_bigquery_table" "alerts_table" {
  dataset_id          = google_bigquery_dataset.monitoring_dataset.dataset_id
  table_id            = "alerts"
  project             = var.project_id
  description         = "Record of alerts generated by the monitoring system"
  deletion_protection = local.is_production
  schema              = local.default_table_schemas.alerts
  labels              = local.common_labels
  
  time_partitioning {
    type          = "DAY"
    field         = "created_at"
    expiration_ms = null
  }
  
  clustering {
    fields = ["severity", "acknowledged"]
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Create custom tables defined in the bigquery_tables variable
resource "google_bigquery_table" "custom_tables" {
  for_each            = var.bigquery_tables
  dataset_id          = google_bigquery_dataset.main_dataset.dataset_id
  table_id            = each.key
  project             = var.project_id
  description         = each.value.description
  deletion_protection = local.is_production
  schema              = each.value.schema
  labels              = local.common_labels
  
  dynamic "time_partitioning" {
    for_each = each.value.partition_field != null ? [1] : []
    content {
      type          = "DAY"
      field         = each.value.partition_field
      expiration_ms = null
    }
  }
  
  dynamic "clustering" {
    for_each = each.value.clustering_fields != null ? [1] : []
    content {
      fields = each.value.clustering_fields
    }
  }
  
  dynamic "encryption_configuration" {
    for_each = local.bigquery_kms_key != null ? [1] : []
    content {
      kms_key_name = local.bigquery_kms_key
    }
  }
}

# Grant service account access to the main dataset
resource "google_bigquery_dataset_iam_member" "main_dataset_access" {
  dataset_id = google_bigquery_dataset.main_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to the metadata dataset
resource "google_bigquery_dataset_iam_member" "metadata_dataset_access" {
  dataset_id = google_bigquery_dataset.metadata_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to the quality dataset
resource "google_bigquery_dataset_iam_member" "quality_dataset_access" {
  dataset_id = google_bigquery_dataset.quality_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to the healing dataset
resource "google_bigquery_dataset_iam_member" "healing_dataset_access" {
  dataset_id = google_bigquery_dataset.healing_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to the monitoring dataset
resource "google_bigquery_dataset_iam_member" "monitoring_dataset_access" {
  dataset_id = google_bigquery_dataset.monitoring_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Outputs
output "bigquery_dataset_ids" {
  description = "Map of BigQuery dataset IDs by purpose"
  value = {
    main = google_bigquery_dataset.main_dataset.dataset_id
    metadata = google_bigquery_dataset.metadata_dataset.dataset_id
    quality = google_bigquery_dataset.quality_dataset.dataset_id
    healing = google_bigquery_dataset.healing_dataset.dataset_id
    monitoring = google_bigquery_dataset.monitoring_dataset.dataset_id
  }
}

output "bigquery_table_ids" {
  description = "Map of BigQuery table IDs by table name"
  value = merge(
    {
      source_systems = google_bigquery_table.source_systems_table.table_id
      pipeline_executions = google_bigquery_table.pipeline_executions_table.table_id
      task_executions = google_bigquery_table.task_executions_table.table_id
      quality_validations = google_bigquery_table.quality_validations_table.table_id
      healing_executions = google_bigquery_table.healing_executions_table.table_id
      pipeline_metrics = google_bigquery_table.pipeline_metrics_table.table_id
      alerts = google_bigquery_table.alerts_table.table_id
    },
    { for k, v in google_bigquery_table.custom_tables : k => v.table_id }
  )
}

output "bigquery_location" {
  description = "Location of the BigQuery datasets"
  value       = var.bigquery_location
}