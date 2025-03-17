/**
 * # BigQuery Module
 *
 * This module creates BigQuery resources for the self-healing data pipeline, including:
 *  - Main dataset for analytics data
 *  - Metadata dataset for pipeline operations
 *  - Quality dataset for data quality validation results
 *  - Healing dataset for self-healing AI data
 *  - Monitoring dataset for operational metrics
 *  - Default tables with appropriate partitioning and clustering
 *  - IAM permissions for service accounts
 *
 * The module supports customer-managed encryption keys (CMEK) and configurable
 * retention policies.
 */

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

locals {
  dataset_names = {
    main      = "${var.resource_prefix}_${var.dataset_id}_${var.environment}"
    metadata  = "${var.resource_prefix}_metadata_${var.environment}"
    quality   = "${var.resource_prefix}_quality_${var.environment}"
    healing   = "${var.resource_prefix}_healing_${var.environment}"
    monitoring = "${var.resource_prefix}_monitoring_${var.environment}"
  }

  kms_key = var.enable_cmek ? "projects/${var.project_id}/locations/${var.dataset_location}/keyRings/${var.kms_key_ring}/cryptoKeys/bigquery-key" : null

  default_table_schemas = {
    source_systems = file("${path.module}/schemas/source_systems.json")
    pipeline_executions = file("${path.module}/schemas/pipeline_executions.json")
    task_executions = file("${path.module}/schemas/task_executions.json")
    quality_validations = file("${path.module}/schemas/quality_validations.json")
    healing_executions = file("${path.module}/schemas/healing_executions.json")
    pipeline_metrics = file("${path.module}/schemas/pipeline_metrics.json")
    alerts = file("${path.module}/schemas/alerts.json")
  }

  common_labels = merge(var.labels, {
    environment = var.environment
    project     = var.project_id
    terraform   = "true"
    component   = "bigquery"
  })

  is_production = var.environment == "prod"
}

# Main dataset for analytics data
resource "google_bigquery_dataset" "main_dataset" {
  dataset_id                 = local.dataset_names.main
  project                    = var.project_id
  location                   = var.dataset_location
  description                = "Main dataset for self-healing data pipeline analytics"
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Metadata dataset for pipeline operations
resource "google_bigquery_dataset" "metadata_dataset" {
  count                      = var.create_metadata_dataset ? 1 : 0
  dataset_id                 = local.dataset_names.metadata
  project                    = var.project_id
  location                   = var.dataset_location
  description                = "Dataset for pipeline metadata, execution history, and operational data"
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Quality dataset for data quality validation
resource "google_bigquery_dataset" "quality_dataset" {
  count                      = var.create_quality_dataset ? 1 : 0
  dataset_id                 = local.dataset_names.quality
  project                    = var.project_id
  location                   = var.dataset_location
  description                = "Dataset for data quality metrics, validation results, and quality trends"
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Self-healing dataset for AI operations
resource "google_bigquery_dataset" "healing_dataset" {
  count                      = var.create_healing_dataset ? 1 : 0
  dataset_id                 = local.dataset_names.healing
  project                    = var.project_id
  location                   = var.dataset_location
  description                = "Dataset for self-healing AI models, training data, and healing metrics"
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Monitoring dataset for pipeline metrics
resource "google_bigquery_dataset" "monitoring_dataset" {
  count                      = var.create_monitoring_dataset ? 1 : 0
  dataset_id                 = local.dataset_names.monitoring
  project                    = var.project_id
  location                   = var.dataset_location
  description                = "Dataset for pipeline monitoring metrics, alerts, and performance data"
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Default tables for pipeline operations
resource "google_bigquery_table" "default_tables" {
  for_each = var.create_default_tables ? local.default_table_schemas : {}

  # Determine which dataset to place the table in based on table type
  dataset_id = each.key == "source_systems" || each.key == "pipeline_executions" || each.key == "task_executions" ? (
    var.create_metadata_dataset ? google_bigquery_dataset.metadata_dataset[0].dataset_id : google_bigquery_dataset.main_dataset.dataset_id
  ) : each.key == "quality_validations" ? (
    var.create_quality_dataset ? google_bigquery_dataset.quality_dataset[0].dataset_id : google_bigquery_dataset.main_dataset.dataset_id
  ) : each.key == "healing_executions" ? (
    var.create_healing_dataset ? google_bigquery_dataset.healing_dataset[0].dataset_id : google_bigquery_dataset.main_dataset.dataset_id
  ) : (
    var.create_monitoring_dataset ? google_bigquery_dataset.monitoring_dataset[0].dataset_id : google_bigquery_dataset.main_dataset.dataset_id
  )
  
  table_id            = each.key
  project             = var.project_id
  description         = contains(keys(local.default_table_schemas), each.key) ? "Table for ${replace(each.key, "_", " ")}" : "Custom table"
  deletion_protection = local.is_production
  schema              = each.value

  # Time partitioning for appropriate tables
  dynamic "time_partitioning" {
    for_each = contains(["pipeline_executions", "task_executions", "quality_validations", "healing_executions", "pipeline_metrics", "alerts"], each.key) ? [1] : []
    content {
      type = "DAY"
      field = contains(["pipeline_executions", "task_executions"], each.key) ? "start_time" : contains(["quality_validations"], each.key) ? "validation_time" : contains(["healing_executions"], each.key) ? "execution_time" : contains(["pipeline_metrics"], each.key) ? "collection_time" : "created_at"
      expiration_ms = null
    }
  }

  # Clustering for appropriate tables
  dynamic "clustering" {
    for_each = contains(["pipeline_executions", "task_executions", "quality_validations", "healing_executions", "pipeline_metrics", "alerts"], each.key) ? [1] : []
    content {
      fields = contains(["pipeline_executions"], each.key) ? ["pipeline_id", "status"] : contains(["task_executions"], each.key) ? ["execution_id", "task_id"] : contains(["quality_validations"], each.key) ? ["execution_id", "passed"] : contains(["healing_executions"], each.key) ? ["pattern_id", "successful"] : contains(["pipeline_metrics"], each.key) ? ["metric_category", "metric_name"] : ["severity", "acknowledged"]
    }
  }

  labels = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# Custom tables
resource "google_bigquery_table" "custom_tables" {
  for_each = var.tables

  dataset_id          = google_bigquery_dataset.main_dataset.dataset_id
  table_id            = each.key
  project             = var.project_id
  description         = each.value.description
  deletion_protection = local.is_production
  schema              = each.value.schema

  dynamic "time_partitioning" {
    for_each = each.value.partition_field != null ? [1] : []
    content {
      type         = "DAY"
      field        = each.value.partition_field
      expiration_ms = null
    }
  }

  dynamic "clustering" {
    for_each = each.value.clustering_fields != null ? [1] : []
    content {
      fields = each.value.clustering_fields
    }
  }

  labels = local.common_labels

  dynamic "encryption_configuration" {
    for_each = local.kms_key != null ? [1] : []
    content {
      kms_key_name = local.kms_key
    }
  }
}

# IAM for main dataset
resource "google_bigquery_dataset_iam_member" "main_dataset_access" {
  dataset_id = google_bigquery_dataset.main_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}

# IAM for metadata dataset
resource "google_bigquery_dataset_iam_member" "metadata_dataset_access" {
  count      = var.create_metadata_dataset ? 1 : 0
  dataset_id = google_bigquery_dataset.metadata_dataset[0].dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}

# IAM for quality dataset
resource "google_bigquery_dataset_iam_member" "quality_dataset_access" {
  count      = var.create_quality_dataset ? 1 : 0
  dataset_id = google_bigquery_dataset.quality_dataset[0].dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}

# IAM for healing dataset
resource "google_bigquery_dataset_iam_member" "healing_dataset_access" {
  count      = var.create_healing_dataset ? 1 : 0
  dataset_id = google_bigquery_dataset.healing_dataset[0].dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}

# IAM for monitoring dataset
resource "google_bigquery_dataset_iam_member" "monitoring_dataset_access" {
  count      = var.create_monitoring_dataset ? 1 : 0
  dataset_id = google_bigquery_dataset.monitoring_dataset[0].dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.service_account_email}"
}