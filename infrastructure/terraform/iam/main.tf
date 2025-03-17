# Provider configuration
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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Local variables
locals {
  iam_labels = merge(var.labels, {
    component   = "iam"
    environment = var.environment
  })
  service_account_prefix = "${var.resource_prefix}-sa"
}

# Data sources
data "google_project" "project" {
  project_id = var.project_id
}

data "google_access_context_manager_access_policy" "default" {
  count = var.environment == "prod" ? 1 : 0
}

# Service Accounts
resource "google_service_account" "pipeline_orchestrator" {
  account_id   = "${var.service_account_prefix}-orchestrator-${var.environment}"
  display_name = "Pipeline Orchestrator Service Account"
  description  = "Service account for Cloud Composer and pipeline orchestration"
  project      = var.project_id
}

resource "google_service_account" "data_ingestion" {
  account_id   = "${var.service_account_prefix}-ingestion-${var.environment}"
  display_name = "Data Ingestion Service Account"
  description  = "Service account for data extraction and loading operations"
  project      = var.project_id
}

resource "google_service_account" "quality_validator" {
  account_id   = "${var.service_account_prefix}-validator-${var.environment}"
  display_name = "Quality Validator Service Account"
  description  = "Service account for data quality validation operations"
  project      = var.project_id
}

resource "google_service_account" "self_healing" {
  account_id   = "${var.service_account_prefix}-healing-${var.environment}"
  display_name = "Self-Healing Service Account"
  description  = "Service account for AI-driven self-healing operations"
  project      = var.project_id
}

resource "google_service_account" "monitoring_alerts" {
  account_id   = "${var.service_account_prefix}-monitoring-${var.environment}"
  display_name = "Monitoring & Alerting Service Account"
  description  = "Service account for monitoring, alerting, and notification operations"
  project      = var.project_id
}

# IAM Role Bindings
resource "google_project_iam_member" "orchestrator_role_bindings" {
  for_each = toset(var.orchestrator_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.pipeline_orchestrator.email}"
}

resource "google_project_iam_member" "ingestion_role_bindings" {
  for_each = toset(var.ingestion_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.data_ingestion.email}"
}

resource "google_project_iam_member" "validator_role_bindings" {
  for_each = toset(var.validator_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.quality_validator.email}"
}

resource "google_project_iam_member" "healing_role_bindings" {
  for_each = toset(var.healing_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.self_healing.email}"
}

resource "google_project_iam_member" "monitoring_role_bindings" {
  for_each = toset(var.monitoring_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.monitoring_alerts.email}"
}

# Custom IAM Roles
resource "google_project_iam_custom_role" "pipeline_operator_role" {
  role_id     = "${var.resource_prefix}_pipeline_operator_${var.environment}"
  title       = "Pipeline Operator"
  description = "Custom role for pipeline operators with permissions to manage and monitor pipelines"
  permissions = var.operator_permissions
  project     = var.project_id
}

resource "google_project_iam_custom_role" "pipeline_viewer_role" {
  role_id     = "${var.resource_prefix}_pipeline_viewer_${var.environment}"
  title       = "Pipeline Viewer"
  description = "Custom role for pipeline viewers with read-only permissions"
  permissions = var.viewer_permissions
  project     = var.project_id
}

# KMS Resources (Conditional)
resource "google_kms_key_ring" "pipeline_keyring" {
  count    = var.enable_cmek ? 1 : 0
  name     = "${var.resource_prefix}-keyring-${var.environment}"
  location = var.region
  project  = var.project_id
}

resource "google_kms_crypto_key" "pipeline_crypto_key" {
  count           = var.enable_cmek ? 1 : 0
  name            = "${var.resource_prefix}-key-${var.environment}"
  key_ring        = google_kms_key_ring.pipeline_keyring[0].id
  rotation_period = "7776000s" # 90 days
  purpose         = "ENCRYPT_DECRYPT"

  version_template {
    algorithm        = "GOOGLE_SYMMETRIC_ENCRYPTION"
    protection_level = "SOFTWARE"
  }

  labels = local.iam_labels
}

# Secret Manager Resources
resource "google_secret_manager_secret" "api_credentials_secret" {
  secret_id = "${var.resource_prefix}-api-credentials-${var.environment}"
  labels    = local.iam_labels

  replication {
    automatic = true
  }

  project = var.project_id
}

resource "google_secret_manager_secret" "db_credentials_secret" {
  secret_id = "${var.resource_prefix}-db-credentials-${var.environment}"
  labels    = local.iam_labels

  replication {
    automatic = true
  }

  project = var.project_id
}

# Initial Secret Versions with placeholder values
resource "google_secret_manager_secret_version" "api_credentials_initial_version" {
  secret      = google_secret_manager_secret.api_credentials_secret.id
  secret_data = "{\"api_key\": \"placeholder\", \"api_secret\": \"placeholder\"}"
}

resource "google_secret_manager_secret_version" "db_credentials_initial_version" {
  secret      = google_secret_manager_secret.db_credentials_secret.id
  secret_data = "{\"username\": \"placeholder\", \"password\": \"placeholder\"}"
}

# Secret Manager IAM Bindings
resource "google_secret_manager_secret_iam_member" "orchestrator_api_secret_access" {
  secret_id = google_secret_manager_secret.api_credentials_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_orchestrator.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "ingestion_api_secret_access" {
  secret_id = google_secret_manager_secret.api_credentials_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.data_ingestion.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "orchestrator_db_secret_access" {
  secret_id = google_secret_manager_secret.db_credentials_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_orchestrator.email}"
  project   = var.project_id
}

resource "google_secret_manager_secret_iam_member" "ingestion_db_secret_access" {
  secret_id = google_secret_manager_secret.db_credentials_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.data_ingestion.email}"
  project   = var.project_id
}

# Service Account Keys (Conditional - Not recommended for production)
resource "google_service_account_key" "orchestrator_key" {
  count              = var.create_keys ? 1 : 0
  service_account_id = google_service_account.pipeline_orchestrator.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "google_service_account_key" "ingestion_key" {
  count              = var.create_keys ? 1 : 0
  service_account_id = google_service_account.data_ingestion.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "google_service_account_key" "validator_key" {
  count              = var.create_keys ? 1 : 0
  service_account_id = google_service_account.quality_validator.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "google_service_account_key" "healing_key" {
  count              = var.create_keys ? 1 : 0
  service_account_id = google_service_account.self_healing.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

resource "google_service_account_key" "monitoring_key" {
  count              = var.create_keys ? 1 : 0
  service_account_id = google_service_account.monitoring_alerts.name
  key_algorithm      = "KEY_ALG_RSA_2048"
  public_key_type    = "TYPE_X509_PEM_FILE"
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

# IAP Resources (Conditional)
resource "google_iap_brand" "iap_brand" {
  count            = var.enable_iap ? 1 : 0
  support_email    = var.support_email
  application_title = "Self-Healing Pipeline ${title(var.environment)}"
  project          = var.project_id
}

resource "google_iap_client" "iap_client" {
  count        = var.enable_iap ? 1 : 0
  display_name = "Self-Healing Pipeline IAP Client"
  brand        = google_iap_brand.iap_brand[0].name
}

resource "google_iap_web_backend_service_iam_binding" "iap_binding" {
  count               = var.enable_iap && var.backend_service_id != "" ? 1 : 0
  project             = var.project_id
  web_backend_service = var.backend_service_id
  role                = "roles/iap.httpsResourceAccessor"
  members = [
    "serviceAccount:${google_service_account.pipeline_orchestrator.email}",
    "serviceAccount:${google_service_account.monitoring_alerts.email}"
  ]
}

# Audit Logging Configuration
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

# VPC Service Controls for Production
resource "google_access_context_manager_service_perimeter" "pipeline_service_perimeter" {
  count         = var.environment == "prod" ? 1 : 0
  provider      = google-beta
  name          = "accessPolicies/${data.google_access_context_manager_access_policy.default[0].name}/servicePerimeters/${var.resource_prefix}-perimeter"
  title         = "${var.resource_prefix}-perimeter"
  perimeter_type = "PERIMETER_TYPE_REGULAR"

  status {
    restricted_services = [
      "bigquery.googleapis.com",
      "storage.googleapis.com",
      "secretmanager.googleapis.com"
    ]
    resources = ["projects/${data.google_project.project.number}"]
    vpc_accessible_services {
      enable_restriction = true
      allowed_services = [
        "bigquery.googleapis.com",
        "storage.googleapis.com",
        "secretmanager.googleapis.com"
      ]
    }
  }
}

# Outputs
output "orchestrator_service_account_email" {
  description = "Email of the pipeline orchestrator service account"
  value       = google_service_account.pipeline_orchestrator.email
}

output "ingestion_service_account_email" {
  description = "Email of the data ingestion service account"
  value       = google_service_account.data_ingestion.email
}

output "validator_service_account_email" {
  description = "Email of the quality validator service account"
  value       = google_service_account.quality_validator.email
}

output "healing_service_account_email" {
  description = "Email of the self-healing service account"
  value       = google_service_account.self_healing.email
}

output "monitoring_service_account_email" {
  description = "Email of the monitoring service account"
  value       = google_service_account.monitoring_alerts.email
}

output "kms_key_id" {
  description = "ID of the KMS encryption key (if CMEK is enabled)"
  value       = var.enable_cmek ? google_kms_crypto_key.pipeline_crypto_key[0].id : null
}

output "api_credentials_secret_id" {
  description = "ID of the API credentials secret"
  value       = google_secret_manager_secret.api_credentials_secret.id
}

output "db_credentials_secret_id" {
  description = "ID of the database credentials secret"
  value       = google_secret_manager_secret.db_credentials_secret.id
}

output "pipeline_operator_role_id" {
  description = "ID of the custom pipeline operator role"
  value       = google_project_iam_custom_role.pipeline_operator_role.id
}

output "pipeline_viewer_role_id" {
  description = "ID of the custom pipeline viewer role"
  value       = google_project_iam_custom_role.pipeline_viewer_role.id
}