# Vertex AI resources for Self-Healing Data Pipeline
# These resources provide AI/ML capabilities for automatic issue detection and resolution

# Create local variables for consistent naming and tagging
locals {
  vertex_ai_labels = merge(var.labels, { component = "vertex-ai" })
  resource_name_prefix = "${var.resource_prefix}-${var.environment}"
}

# Enable the Vertex AI API
resource "google_project_service" "vertex_ai_api" {
  project = var.project_id
  service = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

# Create a KMS crypto key for Vertex AI if CMEK is enabled
resource "google_kms_crypto_key" "vertex_ai_key" {
  count = var.enable_cmek ? 1 : 0
  name = "${local.resource_name_prefix}-vertex-ai-key"
  key_ring = google_kms_key_ring.pipeline_keyring[0].id
  rotation_period = "7776000s"  # 90 days
  purpose = "ENCRYPT_DECRYPT"
  labels = local.vertex_ai_labels
  
  lifecycle {
    prevent_destroy = true
  }
}

# Grant necessary IAM roles to the pipeline service account for Vertex AI operations
resource "google_project_iam_member" "vertex_ai_service_account_roles" {
  for_each = {
    "roles/aiplatform.user"        = "Allows using Vertex AI services"
    "roles/aiplatform.admin"       = "Allows managing Vertex AI resources"
    "roles/storage.objectAdmin"    = "Allows managing objects in Cloud Storage for model artifacts"
  }
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.pipeline_service_account.email}"
}

# Create a Cloud Storage bucket for storing ML model artifacts
resource "google_storage_bucket" "model_artifacts_bucket" {
  name          = "${local.resource_name_prefix}-model-artifacts-${random_id.suffix.hex}"
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  labels = local.vertex_ai_labels
  
  lifecycle_rule {
    condition {
      age = 90
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
  
  encryption {
    default_kms_key_name = var.enable_cmek ? google_kms_crypto_key.vertex_ai_key[0].id : null
  }
}

# Create Vertex AI resources using the vertex_ai module
module "vertex_ai" {
  source = "./modules/vertex_ai"
  
  project_id = var.project_id
  region = var.vertex_ai_region
  environment = var.environment
  resource_prefix = var.resource_prefix
  labels = local.vertex_ai_labels
  enable_vertex_ai_pipelines = var.enable_vertex_ai_pipelines
  
  model_registry_name = "self-healing-model-registry"
  
  featurestore_name = "self-healing-featurestore"
  featurestore_online_serving_config = {
    fixed_node_count = 1
  }
  
  metadata_store_name = "self-healing-metadata-store"
  tensorboard_name = "self-healing-tensorboard"
  
  endpoints = {
    anomaly-detection = {
      display_name = "Anomaly Detection Endpoint"
      machine_type = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    data-correction = {
      display_name = "Data Correction Endpoint"
      machine_type = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    root-cause-analysis = {
      display_name = "Root Cause Analysis Endpoint"
      machine_type = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    },
    predictive-failure = {
      display_name = "Predictive Failure Endpoint"
      machine_type = "n1-standard-2"
      min_replica_count = 1
      max_replica_count = 3
    }
  }
  
  network_name = var.enable_private_services ? var.network_name : null
  subnet_name = var.enable_private_services ? var.subnet_name : null
  enable_private_endpoints = var.enable_private_services
  
  service_account_email = google_service_account.pipeline_service_account.email
  
  encryption_key_name = var.enable_cmek ? google_kms_crypto_key.vertex_ai_key[0].id : null
  enable_cmek = var.enable_cmek
  
  model_monitoring_config = {
    enable = true
    monitoring_interval_days = 1
    alert_email_addresses = var.alert_email_addresses
  }
  
  depends_on = [google_project_service.vertex_ai_api]
}

# Export Vertex AI resource identifiers
output "vertex_ai_endpoints" {
  description = "Map of Vertex AI endpoint names to their IDs"
  value = module.vertex_ai.endpoint_ids
}

output "vertex_ai_featurestore_id" {
  description = "The ID of the Vertex AI Featurestore"
  value = module.vertex_ai.featurestore_id
}

output "vertex_ai_metadata_store_id" {
  description = "The ID of the Vertex AI Metadata Store"
  value = module.vertex_ai.metadata_store_id
}

output "model_artifacts_bucket" {
  description = "The name of the bucket storing model artifacts"
  value = google_storage_bucket.model_artifacts_bucket.name
}