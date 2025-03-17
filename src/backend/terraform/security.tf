# Security resources for Self-Healing Data Pipeline

# Define common labels and configurations
locals {
  # Common labels for security resources
  security_labels = merge(var.labels, {
    component = "security"
    environment = var.environment
  })

  # IAM roles for the pipeline service account
  service_account_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/composer.worker",
    "roles/secretmanager.secretAccessor",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter",
    "roles/aiplatform.user"
  ]

  # IAM roles for the Composer service account
  composer_service_account_roles = [
    "roles/composer.ServiceAgentV2Ext",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/secretmanager.secretAccessor",
    "roles/aiplatform.user"
  ]

  # Secrets to create in Secret Manager
  secrets = {
    "teams-webhook-url": {
      description = "Microsoft Teams webhook URL for alerts"
      value = var.teams_webhook_url
      labels = local.security_labels
    },
    "api-key-example": {
      description = "Example API key for external data source"
      value = "placeholder-to-be-updated-manually"
      labels = local.security_labels
    }
  }
}

# Get project information
data "google_project" "project" {
  project_id = var.project_id
}

# Create pipeline service account
resource "google_service_account" "pipeline_service_account" {
  account_id   = "${var.resource_prefix}-${var.service_account_name}-${var.environment}"
  project      = var.project_id
  display_name = "Self-Healing Pipeline Service Account"
  description  = "Service account for the self-healing data pipeline components"
}

# Create Composer service account
resource "google_service_account" "composer_service_account" {
  account_id   = "${var.resource_prefix}-composer-sa-${var.environment}"
  project      = var.project_id
  display_name = "Cloud Composer Service Account"
  description  = "Service account for Cloud Composer environment"
}

# Assign roles to pipeline service account
resource "google_project_iam_member" "pipeline_service_account_roles" {
  for_each = toset(local.service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Assign roles to Composer service account
resource "google_project_iam_member" "composer_service_account_roles" {
  for_each = toset(local.composer_service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.composer_service_account.email}"
}

# Create KMS key ring if CMEK is enabled
resource "google_kms_key_ring" "key_ring" {
  count    = var.enable_cmek ? 1 : 0
  name     = "${var.resource_prefix}-keyring-${var.environment}"
  project  = var.project_id
  location = "global"
}

# Create KMS crypto key for BigQuery
resource "google_kms_crypto_key" "bigquery_crypto_key" {
  count           = var.enable_cmek ? 1 : 0
  name            = "bigquery-key"
  key_ring        = google_kms_key_ring.key_ring[0].id
  rotation_period = "7776000s"  # 90 days
  purpose         = "ENCRYPT_DECRYPT"
  
  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }
  
  labels = local.security_labels
}

# Create KMS crypto key for Cloud Storage
resource "google_kms_crypto_key" "storage_crypto_key" {
  count           = var.enable_cmek ? 1 : 0
  name            = "storage-key"
  key_ring        = google_kms_key_ring.key_ring[0].id
  rotation_period = "7776000s"  # 90 days
  purpose         = "ENCRYPT_DECRYPT"
  
  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }
  
  labels = local.security_labels
}

# Grant BigQuery service agent access to the KMS key
resource "google_kms_crypto_key_iam_member" "bigquery_crypto_key_access" {
  count         = var.enable_cmek ? 1 : 0
  crypto_key_id = google_kms_crypto_key.bigquery_crypto_key[0].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquery.iam.gserviceaccount.com"
}

# Grant Storage service agent access to the KMS key
resource "google_kms_crypto_key_iam_member" "storage_crypto_key_access" {
  count         = var.enable_cmek ? 1 : 0
  crypto_key_id = google_kms_crypto_key.storage_crypto_key[0].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

# Create secrets in Secret Manager
resource "google_secret_manager_secret" "secrets" {
  for_each  = local.secrets
  secret_id = "${var.resource_prefix}-${each.key}-${var.environment}"
  project   = var.project_id
  
  labels = each.value.labels
  
  replication {
    automatic = true
  }
}

# Create secret versions
resource "google_secret_manager_secret_version" "secret_versions" {
  for_each    = local.secrets
  secret      = google_secret_manager_secret.secrets[each.key].id
  secret_data = each.value.value
}

# Grant pipeline service account access to secrets
resource "google_secret_manager_secret_iam_member" "pipeline_secret_access" {
  for_each  = local.secrets
  secret_id = google_secret_manager_secret.secrets[each.key].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant Composer service account access to secrets
resource "google_secret_manager_secret_iam_member" "composer_secret_access" {
  for_each  = local.secrets
  secret_id = google_secret_manager_secret.secrets[each.key].id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.composer_service_account.email}"
}

# Configure audit logging
resource "google_project_iam_audit_config" "audit_config" {
  project = var.project_id
  service = "allServices"
  
  audit_log_config {
    log_type = "ADMIN_READ"
  }
  
  audit_log_config {
    log_type = "DATA_WRITE"
  }
  
  audit_log_config {
    log_type = "DATA_READ"
  }
}

# Create service identities for Secret Manager and KMS
resource "google_project_service_identity" "secretmanager_identity" {
  provider = google-beta
  project  = var.project_id
  service  = "secretmanager.googleapis.com"
}

resource "google_project_service_identity" "kms_identity" {
  provider = google-beta
  project  = var.project_id
  service  = "cloudkms.googleapis.com"
}

# VPC Service Controls - for production environment only
data "google_access_context_manager_access_policy" "default" {
  count    = var.environment == "prod" ? 1 : 0
  provider = google-beta
}

resource "google_access_context_manager_access_level" "access_level" {
  count    = var.environment == "prod" ? 1 : 0
  provider = google-beta
  name     = "accessPolicies/${data.google_access_context_manager_access_policy.default[0].name}/accessLevels/${var.resource_prefix}-access-level-${var.environment}"
  title    = "${var.resource_prefix}-access-level-${var.environment}"
  
  basic {
    conditions {
      ip_subnetworks = [
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16"
      ]
      required_access_levels = []
    }
  }
}

resource "google_vpc_service_controls_service_perimeter" "service_perimeter" {
  count         = var.environment == "prod" ? 1 : 0
  provider      = google-beta
  name          = "accessPolicies/${data.google_access_context_manager_access_policy.default[0].name}/servicePerimeters/${var.resource_prefix}-perimeter-${var.environment}"
  title         = "${var.resource_prefix}-perimeter-${var.environment}"
  perimeter_type = "PERIMETER_TYPE_REGULAR"
  
  status {
    restricted_services = [
      "bigquery.googleapis.com",
      "storage.googleapis.com",
      "secretmanager.googleapis.com"
    ]
    
    resources = [
      "projects/${data.google_project.project.number}"
    ]
    
    access_levels = [
      "accessPolicies/${data.google_access_context_manager_access_policy.default[0].name}/accessLevels/${var.resource_prefix}-access-level-${var.environment}"
    ]
  }
}

# Outputs
output "pipeline_service_account_email" {
  description = "Email address of the pipeline service account"
  value       = google_service_account.pipeline_service_account.email
}

output "composer_service_account_email" {
  description = "Email address of the Composer service account"
  value       = google_service_account.composer_service_account.email
}

output "kms_key_ring_id" {
  description = "ID of the KMS key ring"
  value       = var.enable_cmek ? google_kms_key_ring.key_ring[0].id : ""
}

output "bigquery_crypto_key_id" {
  description = "ID of the BigQuery crypto key"
  value       = var.enable_cmek ? google_kms_crypto_key.bigquery_crypto_key[0].id : ""
}

output "storage_crypto_key_id" {
  description = "ID of the Storage crypto key"
  value       = var.enable_cmek ? google_kms_crypto_key.storage_crypto_key[0].id : ""
}

output "secret_ids" {
  description = "Map of created secret IDs"
  value       = {for k, v in google_secret_manager_secret.secrets : k => v.id}
}