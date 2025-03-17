# BigQuery resources for the test environment
# These resources include datasets and tables needed for testing the self-healing pipeline
# All resources have deletion protection disabled and force_destroy enabled for test cleanup

resource "google_bigquery_dataset" "test_dataset" {
  dataset_id                  = "${var.test_bigquery_dataset_name}_${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.test_bigquery_location
  description                 = "Main dataset for self-healing pipeline testing"
  delete_contents_on_destroy  = true
  labels                      = local.common_labels
  default_table_expiration_ms = 2592000000
  default_partition_expiration_ms = 2592000000
  access {
    role          = "OWNER"
    user_by_email = google_service_account.test_service_account.email
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_dataset" "quality_dataset" {
  dataset_id                  = "${var.resource_prefix}_quality_${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.test_bigquery_location
  description                 = "Dataset for testing data quality validation features"
  delete_contents_on_destroy  = true
  labels                      = local.common_labels
  default_table_expiration_ms = 2592000000
  access {
    role          = "OWNER"
    user_by_email = google_service_account.test_service_account.email
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_dataset" "monitoring_dataset" {
  dataset_id                  = "${var.resource_prefix}_monitoring_${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.test_bigquery_location
  description                 = "Dataset for testing monitoring and metrics features"
  delete_contents_on_destroy  = true
  labels                      = local.common_labels
  default_table_expiration_ms = 7776000000
  access {
    role          = "OWNER"
    user_by_email = google_service_account.test_service_account.email
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

resource "google_bigquery_table" "source_data" {
  dataset_id          = google_bigquery_dataset.test_dataset.dataset_id
  table_id            = "source_data"
  project             = var.project_id
  description         = "Sample source data for pipeline testing"
  deletion_protection = false
  labels              = local.common_labels
  schema              = "[{\"name\":\"id\",\"type\":\"INTEGER\",\"mode\":\"REQUIRED\",\"description\":\"Unique identifier\"},{\"name\":\"name\",\"type\":\"STRING\",\"mode\":\"NULLABLE\",\"description\":\"Name field\"},{\"name\":\"value\",\"type\":\"FLOAT\",\"mode\":\"NULLABLE\",\"description\":\"Numeric value\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\",\"mode\":\"NULLABLE\",\"description\":\"Event timestamp\"},{\"name\":\"valid\",\"type\":\"BOOLEAN\",\"mode\":\"NULLABLE\",\"description\":\"Validity flag\"}]"
  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris   = ["gs://${google_storage_bucket.test_data_bucket.name}/test_data/sample_data.csv"]
    csv_options {
      quote                = "\""
      skip_leading_rows    = 1
      allow_quoted_newlines = true
    }
  }
}

resource "google_bigquery_table" "quality_rules" {
  dataset_id          = google_bigquery_dataset.quality_dataset.dataset_id
  table_id            = "quality_rules"
  project             = var.project_id
  description         = "Data quality validation rules for testing"
  deletion_protection = false
  labels              = local.common_labels
  schema              = "[{\"name\":\"rule_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Unique rule identifier\"},{\"name\":\"dataset_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Target dataset ID\"},{\"name\":\"table_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Target table ID\"},{\"name\":\"column_id\",\"type\":\"STRING\",\"mode\":\"NULLABLE\",\"description\":\"Target column ID if applicable\"},{\"name\":\"rule_type\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Type of validation rule\"},{\"name\":\"rule_definition\",\"type\":\"JSON\",\"mode\":\"REQUIRED\",\"description\":\"JSON definition of the validation rule\"},{\"name\":\"severity\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Rule violation severity (CRITICAL, HIGH, MEDIUM, LOW)\"},{\"name\":\"created_at\",\"type\":\"TIMESTAMP\",\"mode\":\"REQUIRED\",\"description\":\"Rule creation timestamp\"},{\"name\":\"updated_at\",\"type\":\"TIMESTAMP\",\"mode\":\"REQUIRED\",\"description\":\"Rule last update timestamp\"},{\"name\":\"active\",\"type\":\"BOOLEAN\",\"mode\":\"REQUIRED\",\"description\":\"Whether the rule is active\"}]"
}

resource "google_bigquery_table" "quality_results" {
  dataset_id          = google_bigquery_dataset.quality_dataset.dataset_id
  table_id            = "quality_results"
  project             = var.project_id
  description         = "Data quality validation results for testing"
  deletion_protection = false
  labels              = local.common_labels
  time_partitioning {
    type          = "DAY"
    field         = "validation_time"
    expiration_ms = 2592000000
  }
  schema              = "[{\"name\":\"validation_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Unique validation identifier\"},{\"name\":\"rule_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Reference to the applied rule\"},{\"name\":\"dataset_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Validated dataset ID\"},{\"name\":\"table_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Validated table ID\"},{\"name\":\"column_id\",\"type\":\"STRING\",\"mode\":\"NULLABLE\",\"description\":\"Validated column ID if applicable\"},{\"name\":\"validation_time\",\"type\":\"TIMESTAMP\",\"mode\":\"REQUIRED\",\"description\":\"When validation was performed\"},{\"name\":\"success\",\"type\":\"BOOLEAN\",\"mode\":\"REQUIRED\",\"description\":\"Whether validation passed\"},{\"name\":\"failure_count\",\"type\":\"INTEGER\",\"mode\":\"NULLABLE\",\"description\":\"Number of validation failures if applicable\"},{\"name\":\"details\",\"type\":\"JSON\",\"mode\":\"NULLABLE\",\"description\":\"Detailed validation results\"},{\"name\":\"execution_id\",\"type\":\"STRING\",\"mode\":\"NULLABLE\",\"description\":\"Reference to pipeline execution ID\"}]"
}

resource "google_bigquery_table" "pipeline_metrics" {
  dataset_id          = google_bigquery_dataset.monitoring_dataset.dataset_id
  table_id            = "pipeline_metrics"
  project             = var.project_id
  description         = "Pipeline execution metrics for monitoring and analysis"
  deletion_protection = false
  labels              = local.common_labels
  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }
  clustering {
    fields = ["pipeline_id", "metric_category"]
  }
  schema              = "[{\"name\":\"metric_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Unique metric identifier\"},{\"name\":\"pipeline_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Pipeline identifier\"},{\"name\":\"execution_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Execution identifier\"},{\"name\":\"metric_category\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Category of metric (PERFORMANCE, QUALITY, RESOURCE, etc.)\"},{\"name\":\"metric_name\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Name of the metric\"},{\"name\":\"metric_value\",\"type\":\"FLOAT\",\"mode\":\"REQUIRED\",\"description\":\"Value of the metric\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\",\"mode\":\"REQUIRED\",\"description\":\"When the metric was collected\"},{\"name\":\"dimensions\",\"type\":\"JSON\",\"mode\":\"NULLABLE\",\"description\":\"Additional dimensional data for the metric\"}]"
}

resource "google_bigquery_table" "healing_actions" {
  dataset_id          = google_bigquery_dataset.monitoring_dataset.dataset_id
  table_id            = "healing_actions"
  project             = var.project_id
  description         = "Self-healing actions for monitoring and analysis"
  deletion_protection = false
  labels              = local.common_labels
  time_partitioning {
    type          = "DAY"
    field         = "timestamp"
    expiration_ms = 7776000000
  }
  schema              = "[{\"name\":\"action_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Unique action identifier\"},{\"name\":\"issue_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Issue identifier that triggered the action\"},{\"name\":\"pipeline_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Pipeline identifier\"},{\"name\":\"execution_id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Execution identifier\"},{\"name\":\"issue_type\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Type of issue (DATA_QUALITY, PERFORMANCE, RESOURCE, etc.)\"},{\"name\":\"action_type\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Type of healing action taken\"},{\"name\":\"action_details\",\"type\":\"JSON\",\"mode\":\"REQUIRED\",\"description\":\"Details of the healing action\"},{\"name\":\"confidence_score\",\"type\":\"FLOAT\",\"mode\":\"REQUIRED\",\"description\":\"Confidence score for the healing action\"},{\"name\":\"success\",\"type\":\"BOOLEAN\",\"mode\":\"REQUIRED\",\"description\":\"Whether the healing action was successful\"},{\"name\":\"timestamp\",\"type\":\"TIMESTAMP\",\"mode\":\"REQUIRED\",\"description\":\"When the healing action was performed\"},{\"name\":\"duration_ms\",\"type\":\"INTEGER\",\"mode\":\"NULLABLE\",\"description\":\"Duration of the healing action in milliseconds\"}]"
}

resource "google_bigquery_table" "partitioned_test_table" {
  dataset_id          = google_bigquery_dataset.test_dataset.dataset_id
  table_id            = "partitioned_test_table"
  project             = var.project_id
  description         = "Partitioned and clustered table for testing optimization features"
  deletion_protection = false
  labels              = local.common_labels
  time_partitioning {
    type          = "DAY"
    field         = "event_date"
    expiration_ms = 2592000000
  }
  clustering {
    fields = ["category", "region"]
  }
  schema              = "[{\"name\":\"id\",\"type\":\"INTEGER\",\"mode\":\"REQUIRED\",\"description\":\"Unique identifier\"},{\"name\":\"event_date\",\"type\":\"DATE\",\"mode\":\"REQUIRED\",\"description\":\"Date of the event for partitioning\"},{\"name\":\"category\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Category for clustering\"},{\"name\":\"region\",\"type\":\"STRING\",\"mode\":\"REQUIRED\",\"description\":\"Region for clustering\"},{\"name\":\"value\",\"type\":\"FLOAT\",\"mode\":\"NULLABLE\",\"description\":\"Numeric value\"},{\"name\":\"metadata\",\"type\":\"JSON\",\"mode\":\"NULLABLE\",\"description\":\"Additional metadata\"}]"
}

locals {
  test_bigquery_tables = {
    source_data = google_bigquery_table.source_data.table_id
    quality_rules = google_bigquery_table.quality_rules.table_id
    quality_results = google_bigquery_table.quality_results.table_id
    pipeline_metrics = google_bigquery_table.pipeline_metrics.table_id
    healing_actions = google_bigquery_table.healing_actions.table_id
    partitioned_test_table = google_bigquery_table.partitioned_test_table.table_id
  }
}

output "test_bigquery_dataset_id" {
  description = "The ID of the main BigQuery test dataset"
  value       = google_bigquery_dataset.test_dataset.dataset_id
}

output "quality_dataset_id" {
  description = "The ID of the quality validation test dataset"
  value       = google_bigquery_dataset.quality_dataset.dataset_id
}

output "monitoring_dataset_id" {
  description = "The ID of the monitoring test dataset"
  value       = google_bigquery_dataset.monitoring_dataset.dataset_id
}

output "source_data_table_id" {
  description = "The ID of the source data test table"
  value       = google_bigquery_table.source_data.table_id
}

output "quality_rules_table_id" {
  description = "The ID of the quality rules table"
  value       = google_bigquery_table.quality_rules.table_id
}

output "quality_results_table_id" {
  description = "The ID of the quality results table"
  value       = google_bigquery_table.quality_results.table_id
}

output "pipeline_metrics_table_id" {
  description = "The ID of the pipeline metrics table"
  value       = google_bigquery_table.pipeline_metrics.table_id
}

output "healing_actions_table_id" {
  description = "The ID of the healing actions table"
  value       = google_bigquery_table.healing_actions.table_id
}

output "partitioned_test_table_id" {
  description = "The ID of the partitioned test table"
  value       = google_bigquery_table.partitioned_test_table.table_id
}