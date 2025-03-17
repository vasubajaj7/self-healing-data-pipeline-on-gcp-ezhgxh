# Google Cloud Storage resources for the self-healing data pipeline

locals {
  # Common labels to apply to all storage resources
  storage_labels = merge(var.labels, {
    component = "storage"
  })

  # Standard prefix for bucket names including environment
  bucket_name_prefix = "${var.resource_prefix}-${var.environment}"

  # Default lifecycle rules for storage cost optimization
  default_lifecycle_rules = [
    {
      condition = {
        age = 30
        with_state = "ANY"
      },
      action = {
        type = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    },
    {
      condition = {
        age = 90
        with_state = "ANY"
      },
      action = {
        type = "SetStorageClass"
        storage_class = "COLDLINE"
      }
    },
    {
      condition = {
        age = 365
        with_state = "ANY"
      },
      action = {
        type = "SetStorageClass"
        storage_class = "ARCHIVE"
      }
    }
  ]

  # Lifecycle rules for temporary storage with shorter retention
  temp_lifecycle_rules = [
    {
      condition = {
        age = 7
        with_state = "ANY"
      },
      action = {
        type = "Delete"
      }
    }
  ]
}

# Raw data bucket for storing incoming data from various sources
resource "google_storage_bucket" "raw_data_bucket" {
  name          = "${local.bucket_name_prefix}-raw-data"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.default_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Processed data bucket for storing transformed and validated data
resource "google_storage_bucket" "processed_data_bucket" {
  name          = "${local.bucket_name_prefix}-processed-data"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.default_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Backup bucket for storing backups and archives
resource "google_storage_bucket" "backup_bucket" {
  name          = "${local.bucket_name_prefix}-backup"
  project       = var.project_id
  location      = var.secondary_region
  storage_class = "NEARLINE"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.default_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.secondary_region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Temporary storage bucket for pipeline processing
resource "google_storage_bucket" "temp_bucket" {
  name          = "${local.bucket_name_prefix}-temp"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = false
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.temp_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = true
}

# Quality validation results bucket
resource "google_storage_bucket" "quality_results_bucket" {
  name          = "${local.bucket_name_prefix}-quality-results"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.default_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Functions source code bucket
resource "google_storage_bucket" "functions_source" {
  name          = "${local.bucket_name_prefix}-functions"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule {
    condition {
      age        = 30
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = true
}

# Model artifacts bucket for AI models
resource "google_storage_bucket" "model_artifacts_bucket" {
  name          = "${local.bucket_name_prefix}-model-artifacts"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  
  labels = local.storage_labels
  
  lifecycle_rule = local.default_lifecycle_rules
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${var.region}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Dynamic buckets defined in the storage_buckets variable
resource "google_storage_bucket" "custom_buckets" {
  for_each = var.storage_buckets
  
  name          = "${local.bucket_name_prefix}-${each.key}"
  project       = var.project_id
  location      = each.value.location
  storage_class = each.value.storage_class
  
  uniform_bucket_level_access = true
  versioning {
    enabled = each.value.versioning
  }
  
  labels = local.storage_labels
  
  dynamic "lifecycle_rule" {
    for_each = each.value.lifecycle_rules
    content {
      condition {
        age                   = lifecycle_rule.value.condition.age
        created_before        = lifecycle_rule.value.condition.created_before
        with_state            = lifecycle_rule.value.condition.with_state
        matches_storage_class = lifecycle_rule.value.condition.matches_storage_class
      }
      action {
        type          = lifecycle_rule.value.action.type
        storage_class = lifecycle_rule.value.action.storage_class
      }
    }
  }
  
  encryption {
    default_kms_key_name = var.enable_cmek ? "projects/${var.project_id}/locations/${each.value.location}/keyRings/storage-keyring/cryptoKeys/storage-key" : null
  }
  
  force_destroy = var.environment != "prod"
}

# Grant service account access to raw data bucket
resource "google_storage_bucket_iam_member" "raw_data_bucket_access" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to processed data bucket
resource "google_storage_bucket_iam_member" "processed_data_bucket_access" {
  bucket = google_storage_bucket.processed_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to backup bucket
resource "google_storage_bucket_iam_member" "backup_bucket_access" {
  bucket = google_storage_bucket.backup_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to temp bucket
resource "google_storage_bucket_iam_member" "temp_bucket_access" {
  bucket = google_storage_bucket.temp_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to quality results bucket
resource "google_storage_bucket_iam_member" "quality_results_bucket_access" {
  bucket = google_storage_bucket.quality_results_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to functions source bucket
resource "google_storage_bucket_iam_member" "functions_source_access" {
  bucket = google_storage_bucket.functions_source.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to model artifacts bucket
resource "google_storage_bucket_iam_member" "model_artifacts_bucket_access" {
  bucket = google_storage_bucket.model_artifacts_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Grant service account access to custom buckets
resource "google_storage_bucket_iam_member" "custom_buckets_access" {
  for_each = google_storage_bucket.custom_buckets
  
  bucket = each.value.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Output bucket names for reference by other modules
output "raw_data_bucket_name" {
  value       = google_storage_bucket.raw_data_bucket.name
  description = "Name of the raw data bucket"
}

output "processed_data_bucket_name" {
  value       = google_storage_bucket.processed_data_bucket.name
  description = "Name of the processed data bucket"
}

output "backup_bucket_name" {
  value       = google_storage_bucket.backup_bucket.name
  description = "Name of the backup bucket"
}

output "temp_bucket_name" {
  value       = google_storage_bucket.temp_bucket.name
  description = "Name of the temporary storage bucket"
}

output "quality_results_bucket_name" {
  value       = google_storage_bucket.quality_results_bucket.name
  description = "Name of the quality results bucket"
}

output "functions_source_bucket_name" {
  value       = google_storage_bucket.functions_source.name
  description = "Name of the functions source bucket"
}

output "model_artifacts_bucket_name" {
  value       = google_storage_bucket.model_artifacts_bucket.name
  description = "Name of the model artifacts bucket"
}

output "custom_bucket_names" {
  value       = {for k, v in google_storage_bucket.custom_buckets : k => v.name}
  description = "Map of custom bucket names by key"
}