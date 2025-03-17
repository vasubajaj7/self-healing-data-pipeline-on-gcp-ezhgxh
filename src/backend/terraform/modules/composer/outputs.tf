# Output the fully qualified ID of the Cloud Composer environment
output "composer_environment_id" {
  description = "The fully qualified ID of the created Cloud Composer environment"
  value       = google_composer_environment.composer_environment.id
}

# Output the name of the Cloud Composer environment
output "composer_environment_name" {
  description = "The name of the created Cloud Composer environment"
  value       = google_composer_environment.composer_environment.name
}

# Output the GCS bucket used by the Cloud Composer environment
# This extracts the bucket name from the DAG GCS prefix using regex
output "composer_gcs_bucket" {
  description = "The GCS bucket associated with the Composer environment for storing DAGs, plugins, and logs"
  value       = regex("gs://([^/]+)/", google_composer_environment.composer_environment.config.0.dag_gcs_prefix)[0]
}

# Output the Airflow web UI URI
output "composer_airflow_uri" {
  description = "The URI of the Airflow web UI for the Composer environment"
  value       = google_composer_environment.composer_environment.config.0.airflow_uri
}

# Output the GCS prefix where DAGs should be uploaded
output "composer_dag_gcs_prefix" {
  description = "The GCS prefix where DAGs should be uploaded for the Composer environment"
  value       = google_composer_environment.composer_environment.config.0.dag_gcs_prefix
}

# Output the service account email used by the Composer environment
output "composer_service_account" {
  description = "The service account email used by the Composer environment"
  value       = google_service_account.composer_service_account.email
}

# Output the complete configuration of the Composer environment
output "composer_environment_config" {
  description = "The configuration details of the Composer environment for reference"
  value       = google_composer_environment.composer_environment.config
}