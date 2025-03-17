# Outputs for the BigQuery terraform module
# These outputs expose dataset IDs, table IDs, and other resources created by the module
# for integration with other components of the self-healing data pipeline

output "dataset_ids" {
  description = "Map of BigQuery dataset IDs by purpose (main, metadata, quality, healing, monitoring)"
  value = {
    main = google_bigquery_dataset.main_dataset.dataset_id
    metadata = var.create_metadata_dataset ? google_bigquery_dataset.metadata_dataset[0].dataset_id : null
    quality = var.create_quality_dataset ? google_bigquery_dataset.quality_dataset[0].dataset_id : null
    healing = var.create_healing_dataset ? google_bigquery_dataset.healing_dataset[0].dataset_id : null
    monitoring = var.create_monitoring_dataset ? google_bigquery_dataset.monitoring_dataset[0].dataset_id : null
  }
}

output "dataset_names" {
  description = "Map of dataset names used for the different pipeline components"
  value       = local.dataset_names
}

output "main_dataset_id" {
  description = "ID of the main BigQuery dataset"
  value       = google_bigquery_dataset.main_dataset.dataset_id
}

output "metadata_dataset_id" {
  description = "ID of the metadata BigQuery dataset if created"
  value       = var.create_metadata_dataset ? google_bigquery_dataset.metadata_dataset[0].dataset_id : null
}

output "quality_dataset_id" {
  description = "ID of the quality BigQuery dataset if created"
  value       = var.create_quality_dataset ? google_bigquery_dataset.quality_dataset[0].dataset_id : null
}

output "healing_dataset_id" {
  description = "ID of the healing BigQuery dataset if created"
  value       = var.create_healing_dataset ? google_bigquery_dataset.healing_dataset[0].dataset_id : null
}

output "monitoring_dataset_id" {
  description = "ID of the monitoring BigQuery dataset if created"
  value       = var.create_monitoring_dataset ? google_bigquery_dataset.monitoring_dataset[0].dataset_id : null
}

output "default_table_ids" {
  description = "Map of default table IDs by table name if default tables were created"
  value       = var.create_default_tables ? { for k, v in google_bigquery_table.default_tables : k => v.table_id } : {}
}

output "custom_table_ids" {
  description = "Map of custom table IDs by table name"
  value       = { for k, v in google_bigquery_table.custom_tables : k => v.table_id }
}

output "all_table_ids" {
  description = "Combined map of all table IDs (default and custom) by table name"
  value = merge(
    var.create_default_tables ? { for k, v in google_bigquery_table.default_tables : k => v.table_id } : {},
    { for k, v in google_bigquery_table.custom_tables : k => v.table_id }
  )
}

output "dataset_location" {
  description = "Location of the BigQuery datasets"
  value       = var.dataset_location
}

output "main_dataset_self_link" {
  description = "Self link for the main BigQuery dataset"
  value       = google_bigquery_dataset.main_dataset.self_link
}

output "main_dataset_project" {
  description = "Project ID where the main dataset is located"
  value       = google_bigquery_dataset.main_dataset.project
}