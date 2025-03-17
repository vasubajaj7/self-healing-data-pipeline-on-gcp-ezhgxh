# Outputs for Self-Healing Data Pipeline Infrastructure

# Core project information
output "project_id" {
  value       = var.project_id
  description = "The Google Cloud Project ID where resources are deployed"
}

output "region" {
  value       = var.region
  description = "The primary Google Cloud region for resource deployment"
}

output "environment" {
  value       = var.environment
  description = "The deployment environment (dev, staging, prod)"
}

output "random_suffix" {
  value       = random_id.suffix.hex
  description = "Random suffix for globally unique resource names"
}

# Service accounts
output "service_accounts" {
  value = {
    pipeline = google_service_account.pipeline_service_account.email
    composer = google_service_account.composer_service_account.email
  }
  description = "Map of service account emails used by the pipeline"
}

# Storage buckets
output "storage_buckets" {
  value = {
    raw_data = google_storage_bucket.raw_data_bucket.name
    processed_data = google_storage_bucket.processed_data_bucket.name
    backup = google_storage_bucket.backup_bucket.name
    temp = google_storage_bucket.temp_bucket.name
    quality_results = google_storage_bucket.quality_results_bucket.name
    functions_source = google_storage_bucket.functions_source.name
    model_artifacts = google_storage_bucket.model_artifacts_bucket.name
    custom = {for k, v in google_storage_bucket.custom_buckets : k => v.name}
  }
  description = "Map of storage bucket names created for the pipeline"
}

# BigQuery datasets
output "bigquery_datasets" {
  value = {
    main = module.bigquery.main_dataset_id
    metadata = module.bigquery.metadata_dataset_id
    quality = module.bigquery.quality_dataset_id
    healing = module.bigquery.healing_dataset_id
    monitoring = module.bigquery.monitoring_dataset_id
  }
  description = "Map of BigQuery dataset IDs created for the pipeline"
}

output "bigquery_tables" {
  value = module.bigquery.table_ids
  description = "Map of BigQuery table IDs created for the pipeline"
}

# Cloud Composer environment
output "composer_environment" {
  value = {
    name = module.composer.composer_environment_name
    gcs_bucket = module.composer.composer_gcs_bucket
    airflow_uri = module.composer.composer_airflow_uri
    dag_gcs_prefix = module.composer.composer_dag_gcs_prefix
  }
  description = "Details of the Cloud Composer environment"
}

# Monitoring resources
output "monitoring_resources" {
  value = {
    notification_channels = module.monitoring.notification_channels
    alert_policies = module.monitoring.alert_policies
    dashboards = module.monitoring.dashboards
  }
  description = "Map of monitoring resources created for the pipeline"
}

# Vertex AI resources
output "vertex_ai_resources" {
  value = {
    model_registry_id = module.vertex_ai.model_registry_id
    featurestore_id = module.vertex_ai.featurestore_id
    metadata_store_id = module.vertex_ai.metadata_store_id
    tensorboard_id = module.vertex_ai.tensorboard_id
    endpoint_ids = module.vertex_ai.endpoint_ids
    error_patterns_index_id = module.vertex_ai.error_patterns_index_id
  }
  description = "Map of Vertex AI resources created for the pipeline"
}

# Network resources
output "network_resources" {
  value = {
    network_id = google_compute_network.pipeline_network.id
    network_name = google_compute_network.pipeline_network.name
    subnet_id = google_compute_subnetwork.pipeline_subnet.id
    subnet_name = google_compute_subnetwork.pipeline_subnet.name
  }
  description = "Network resources created for the pipeline"
}

# Security resources
output "security_resources" {
  value = {
    kms_key_ring_id = var.enable_cmek ? google_kms_key_ring.key_ring[0].id : ""
    bigquery_crypto_key_id = var.enable_cmek ? google_kms_crypto_key.bigquery_crypto_key[0].id : ""
    storage_crypto_key_id = var.enable_cmek ? google_kms_crypto_key.storage_crypto_key[0].id : ""
    secret_ids = {for k, v in google_secret_manager_secret.secrets : k => v.id}
  }
  description = "Security resources created for the pipeline"
}