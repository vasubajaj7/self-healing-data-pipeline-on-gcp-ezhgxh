#########################################################################
# Self-Healing Data Pipeline - Cloud Functions Resources
#########################################################################

# Storage bucket for Cloud Functions source code
resource "google_storage_bucket" "functions_source" {
  name                        = "${var.resource_prefix}-${var.environment}-functions-source"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  labels = merge(var.labels, { "bucket-purpose" = "functions-source" })
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
  
  dynamic "encryption" {
    for_each = var.enable_cmek ? [1] : []
    content {
      default_kms_key_name = google_kms_crypto_key.storage_key[0].id
    }
  }
  
  force_destroy = true
}

# Local variable for common function labels
locals {
  function_common_labels = merge(var.labels, { "component" = "cloud-functions" })
}

#########################################################################
# Pub/Sub Topics for Event-Driven Functions
#########################################################################

# Topic for quality issues that need correction
resource "google_pubsub_topic" "quality_issues_topic" {
  name    = "${var.resource_prefix}-${var.environment}-quality-issues"
  project = var.project_id
  labels  = merge(var.labels, { "topic-purpose" = "quality-issues" })
}

# Topic for pipeline metrics
resource "google_pubsub_topic" "pipeline_metrics_topic" {
  name    = "${var.resource_prefix}-${var.environment}-pipeline-metrics"
  project = var.project_id
  labels  = merge(var.labels, { "topic-purpose" = "pipeline-metrics" })
}

# Topic for alerts
resource "google_pubsub_topic" "alerts_topic" {
  name    = "${var.resource_prefix}-${var.environment}-alerts"
  project = var.project_id
  labels  = merge(var.labels, { "topic-purpose" = "alerts" })
}

# Topic for query optimization requests
resource "google_pubsub_topic" "optimization_requests_topic" {
  name    = "${var.resource_prefix}-${var.environment}-optimization-requests"
  project = var.project_id
  labels  = merge(var.labels, { "topic-purpose" = "optimization-requests" })
}

# Topic for API data extraction requests
resource "google_pubsub_topic" "api_extraction_topic" {
  name    = "${var.resource_prefix}-${var.environment}-api-extraction"
  project = var.project_id
  labels  = merge(var.labels, { "topic-purpose" = "api-extraction" })
}

#########################################################################
# Cloud Function Source Code Packaging
#########################################################################

# Data source for creating ZIP archives of function source code
data "archive_file" "quality_validator_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/quality_validator"
  output_path = "${path.module}/tmp/quality_validator.zip"
}

data "archive_file" "data_corrector_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/data_corrector"
  output_path = "${path.module}/tmp/data_corrector.zip"
}

data "archive_file" "failure_predictor_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/failure_predictor"
  output_path = "${path.module}/tmp/failure_predictor.zip"
}

data "archive_file" "alert_notifier_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/alert_notifier"
  output_path = "${path.module}/tmp/alert_notifier.zip"
}

data "archive_file" "query_optimizer_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/query_optimizer"
  output_path = "${path.module}/tmp/query_optimizer.zip"
}

data "archive_file" "api_extractor_source" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/api_extractor"
  output_path = "${path.module}/tmp/api_extractor.zip"
}

# Storage bucket objects for function source code
resource "google_storage_bucket_object" "quality_validator_source" {
  name   = "functions/quality_validator_${data.archive_file.quality_validator_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.quality_validator_source.output_path
}

resource "google_storage_bucket_object" "data_corrector_source" {
  name   = "functions/data_corrector_${data.archive_file.data_corrector_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.data_corrector_source.output_path
}

resource "google_storage_bucket_object" "failure_predictor_source" {
  name   = "functions/failure_predictor_${data.archive_file.failure_predictor_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.failure_predictor_source.output_path
}

resource "google_storage_bucket_object" "alert_notifier_source" {
  name   = "functions/alert_notifier_${data.archive_file.alert_notifier_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.alert_notifier_source.output_path
}

resource "google_storage_bucket_object" "query_optimizer_source" {
  name   = "functions/query_optimizer_${data.archive_file.query_optimizer_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.query_optimizer_source.output_path
}

resource "google_storage_bucket_object" "api_extractor_source" {
  name   = "functions/api_extractor_${data.archive_file.api_extractor_source.output_md5}.zip"
  bucket = google_storage_bucket.functions_source.name
  source = data.archive_file.api_extractor_source.output_path
}

#########################################################################
# Secret Manager Resources
#########################################################################

# Secret for Microsoft Teams webhook URL
resource "google_secret_manager_secret" "teams_webhook_secret" {
  secret_id = "${var.resource_prefix}-${var.environment}-teams-webhook"
  project   = var.project_id
  labels    = var.labels
  
  replication {
    automatic = true
  }
}

# Reference to the pipeline service account
data "google_service_account" "pipeline_service_account" {
  account_id = "${var.resource_prefix}-${var.service_account_name}-${var.environment}"
  project    = var.project_id
}

#########################################################################
# Cloud Functions
#########################################################################

# Data Quality Validator Function
resource "google_cloudfunctions2_function" "quality_validator_function" {
  name        = "${var.resource_prefix}-${var.environment}-quality-validator"
  project     = var.project_id
  location    = var.region
  description = "Validates data quality using Great Expectations"
  
  build_config {
    runtime     = "python310"
    entry_point = "validate_data_quality"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.quality_validator_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 10
    min_instance_count  = 0
    available_memory    = "2048M"
    timeout_seconds     = 540  # 9 minutes
    
    environment_variables = {
      ENVIRONMENT = var.environment
      PROJECT_ID  = var.project_id
      REGION      = var.region
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.storage.object.v1.finalized"
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.raw_data_bucket.name
    }
    retry_policy = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "quality-validation" })
}

# Data Corrector Function (Self-Healing)
resource "google_cloudfunctions2_function" "data_corrector_function" {
  name        = "${var.resource_prefix}-${var.environment}-data-corrector"
  project     = var.project_id
  location    = var.region
  description = "Applies AI-driven corrections to data quality issues"
  
  build_config {
    runtime     = "python310"
    entry_point = "correct_data_issues"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.data_corrector_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 10
    min_instance_count  = 0
    available_memory    = "4096M"  # More memory for ML operations
    timeout_seconds     = 540      # 9 minutes
    
    environment_variables = {
      ENVIRONMENT        = var.environment
      PROJECT_ID         = var.project_id
      REGION             = var.region
      VERTEX_AI_ENDPOINT = var.enable_vertex_ai_pipelines ? google_vertex_ai_endpoint.prediction_endpoint[0].name : ""
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.quality_issues_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "data-correction" })
}

# Failure Predictor Function
resource "google_cloudfunctions2_function" "failure_predictor_function" {
  name        = "${var.resource_prefix}-${var.environment}-failure-predictor"
  project     = var.project_id
  location    = var.region
  description = "Predicts potential pipeline failures using ML models"
  
  build_config {
    runtime     = "python310"
    entry_point = "predict_failures"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.failure_predictor_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 5
    min_instance_count  = 0
    available_memory    = "2048M"
    timeout_seconds     = 300  # 5 minutes
    
    environment_variables = {
      ENVIRONMENT        = var.environment
      PROJECT_ID         = var.project_id
      REGION             = var.region
      VERTEX_AI_ENDPOINT = var.enable_vertex_ai_pipelines ? google_vertex_ai_endpoint.prediction_endpoint[0].name : ""
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.pipeline_metrics_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "failure-prediction" })
}

# Alert Notifier Function
resource "google_cloudfunctions2_function" "alert_notifier_function" {
  name        = "${var.resource_prefix}-${var.environment}-alert-notifier"
  project     = var.project_id
  location    = var.region
  description = "Sends alerts to notification channels"
  
  build_config {
    runtime     = "python310"
    entry_point = "send_notifications"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.alert_notifier_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 10
    min_instance_count  = 0
    available_memory    = "1024M"
    timeout_seconds     = 120  # 2 minutes
    
    environment_variables = {
      ENVIRONMENT = var.environment
      PROJECT_ID  = var.project_id
      REGION      = var.region
      TEAMS_WEBHOOK_URL = var.teams_webhook_url
    }
    
    secret_environment_variables {
      key        = "TEAMS_WEBHOOK_URL"
      project_id = var.project_id
      secret     = google_secret_manager_secret.teams_webhook_secret.secret_id
      version    = "latest"
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.alerts_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "alert-notification" })
}

# Query Optimizer Function
resource "google_cloudfunctions2_function" "query_optimizer_function" {
  name        = "${var.resource_prefix}-${var.environment}-query-optimizer"
  project     = var.project_id
  location    = var.region
  description = "Analyzes and optimizes BigQuery queries"
  
  build_config {
    runtime     = "python310"
    entry_point = "optimize_queries"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.query_optimizer_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 5
    min_instance_count  = 0
    available_memory    = "2048M"
    timeout_seconds     = 300  # 5 minutes
    
    environment_variables = {
      ENVIRONMENT = var.environment
      PROJECT_ID  = var.project_id
      REGION      = var.region
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.optimization_requests_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "query-optimization" })
}

# API Extractor Function
resource "google_cloudfunctions2_function" "api_extractor_function" {
  name        = "${var.resource_prefix}-${var.environment}-api-extractor"
  project     = var.project_id
  location    = var.region
  description = "Extracts data from external APIs"
  
  build_config {
    runtime     = "python310"
    entry_point = "extract_api_data"
    source {
      storage_source {
        bucket = google_storage_bucket.functions_source.name
        object = google_storage_bucket_object.api_extractor_source.name
      }
    }
  }
  
  service_config {
    max_instance_count  = 10
    min_instance_count  = 0
    available_memory    = "2048M"
    timeout_seconds     = 540  # 9 minutes
    
    environment_variables = {
      ENVIRONMENT   = var.environment
      PROJECT_ID    = var.project_id
      REGION        = var.region
      OUTPUT_BUCKET = google_storage_bucket.raw_data_bucket.name
    }
    
    secret_environment_variables {
      key        = "API_CREDENTIALS"
      project_id = var.project_id
      secret     = google_secret_manager_secret.api_credentials_secret.secret_id
      version    = "latest"
    }
    
    ingress_settings      = "ALLOW_INTERNAL_ONLY"
    service_account_email = data.google_service_account.pipeline_service_account.email
  }
  
  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.api_extraction_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
  
  labels = merge(var.labels, { "function-purpose" = "api-extraction" })
}

#########################################################################
# Outputs
#########################################################################

output "functions_source_bucket" {
  description = "Name of the storage bucket containing function source code"
  value       = google_storage_bucket.functions_source.name
}

output "quality_validator_function_name" {
  description = "Name of the quality validator Cloud Function"
  value       = google_cloudfunctions2_function.quality_validator_function.name
}

output "data_corrector_function_name" {
  description = "Name of the data corrector Cloud Function"
  value       = google_cloudfunctions2_function.data_corrector_function.name
}

output "failure_predictor_function_name" {
  description = "Name of the failure predictor Cloud Function"
  value       = google_cloudfunctions2_function.failure_predictor_function.name
}

output "alert_notifier_function_name" {
  description = "Name of the alert notifier Cloud Function"
  value       = google_cloudfunctions2_function.alert_notifier_function.name
}

output "query_optimizer_function_name" {
  description = "Name of the query optimizer Cloud Function"
  value       = google_cloudfunctions2_function.query_optimizer_function.name
}

output "api_extractor_function_name" {
  description = "Name of the API extractor Cloud Function"
  value       = google_cloudfunctions2_function.api_extractor_function.name
}

output "pubsub_topics" {
  description = "Map of Pub/Sub topic names by purpose"
  value = {
    quality_issues        = google_pubsub_topic.quality_issues_topic.name
    pipeline_metrics      = google_pubsub_topic.pipeline_metrics_topic.name
    alerts                = google_pubsub_topic.alerts_topic.name
    optimization_requests = google_pubsub_topic.optimization_requests_topic.name
    api_extraction        = google_pubsub_topic.api_extraction_topic.name
  }
}