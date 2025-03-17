# IAM Resource Outputs
# These outputs expose the created IAM resources for use in other Terraform modules

# Service Account Email Outputs
output "orchestrator_service_account_email" {
  description = "Email address of the pipeline orchestrator service account"
  value       = google_service_account.pipeline_orchestrator.email
}

output "ingestion_service_account_email" {
  description = "Email address of the data ingestion service account"
  value       = google_service_account.data_ingestion.email
}

output "validator_service_account_email" {
  description = "Email address of the quality validator service account"
  value       = google_service_account.quality_validator.email
}

output "healing_service_account_email" {
  description = "Email address of the self-healing service account"
  value       = google_service_account.self_healing.email
}

output "monitoring_service_account_email" {
  description = "Email address of the monitoring and alerting service account"
  value       = google_service_account.monitoring_alerts.email
}

# Security Resource Outputs
output "kms_key_id" {
  description = "ID of the KMS encryption key (if CMEK is enabled)"
  value       = var.enable_cmek ? google_kms_crypto_key.pipeline_crypto_key[0].id : ""
}

output "api_credentials_secret_id" {
  description = "ID of the Secret Manager secret for API credentials"
  value       = google_secret_manager_secret.api_credentials_secret.id
}

output "db_credentials_secret_id" {
  description = "ID of the Secret Manager secret for database credentials"
  value       = google_secret_manager_secret.db_credentials_secret.id
}

# Custom Role Outputs
output "pipeline_operator_role_id" {
  description = "ID of the custom IAM role for pipeline operators"
  value       = google_project_iam_custom_role.pipeline_operator_role.id
}

output "pipeline_viewer_role_id" {
  description = "ID of the custom IAM role for pipeline viewers"
  value       = google_project_iam_custom_role.pipeline_viewer_role.id
}

# Service Account Keys (Sensitive)
output "service_account_keys" {
  description = "Map of service account keys (if enabled)"
  value = var.create_keys ? {
    orchestrator = google_service_account_key.orchestrator_key[0].private_key
    ingestion    = google_service_account_key.ingestion_key[0].private_key
    validator    = google_service_account_key.validator_key[0].private_key
    healing      = google_service_account_key.healing_key[0].private_key
    monitoring   = google_service_account_key.monitoring_key[0].private_key
  } : {}
  sensitive = true
}

# IAP Client Outputs (if enabled)
output "iap_client_id" {
  description = "OAuth client ID for Identity-Aware Proxy (if enabled)"
  value       = var.enable_iap ? google_iap_client.iap_client[0].client_id : ""
}

output "iap_client_secret" {
  description = "OAuth client secret for Identity-Aware Proxy (if enabled)"
  value       = var.enable_iap ? google_iap_client.iap_client[0].secret : ""
  sensitive   = true
}