# Storage resources for the test environment
# These resources include buckets for test data, staging, and archiving
# All buckets have force_destroy enabled for test cleanup

# Data source to get current project information
data "google_project" "current" {
  project_id = var.project_id
}

# Creates a Cloud Storage bucket for test data files
resource "google_storage_bucket" "test_data_bucket" {
  name                        = var.test_data_bucket_name != null ? var.test_data_bucket_name : "${var.resource_prefix}-data-${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.common_labels

  lifecycle_rule {
    condition {
      age        = 30
      with_state = "ANY"
    }
    action {
      type = "DELETE"
    }
  }

  lifecycle_rule {
    condition {
      age        = 7
      with_state = "ANY"
    }
    action {
      type          = "STORAGE_CLASS"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }

  cors {
    origin          = ["*"]
    method          = ["GET", "HEAD", "PUT", "POST", "DELETE"]
    response_header = ["*"]
    max_age_seconds = 3600
  }
}

# Creates a Cloud Storage bucket for temporary staging data
resource "google_storage_bucket" "test_staging_bucket" {
  name                        = "${var.resource_prefix}-staging-${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.common_labels

  lifecycle_rule {
    condition {
      age        = 1
      with_state = "ANY"
    }
    action {
      type = "DELETE"
    }
  }
}

# Creates a Cloud Storage bucket for archiving test results
resource "google_storage_bucket" "test_archive_bucket" {
  name                        = "${var.resource_prefix}-archive-${random_id.test_environment_suffix.hex}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "NEARLINE"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.common_labels

  lifecycle_rule {
    condition {
      age        = 90
      with_state = "ANY"
    }
    action {
      type = "DELETE"
    }
  }
}

# Grants the test service account access to the test data bucket
resource "google_storage_bucket_iam_binding" "test_data_bucket_access" {
  bucket  = google_storage_bucket.test_data_bucket.name
  role    = "roles/storage.admin"
  members = ["serviceAccount:${google_service_account.test_service_account.email}"]
}

# Grants the test service account access to the staging bucket
resource "google_storage_bucket_iam_binding" "test_staging_bucket_access" {
  bucket  = google_storage_bucket.test_staging_bucket.name
  role    = "roles/storage.admin"
  members = ["serviceAccount:${google_service_account.test_service_account.email}"]
}

# Grants the test service account access to the archive bucket
resource "google_storage_bucket_iam_binding" "test_archive_bucket_access" {
  bucket  = google_storage_bucket.test_archive_bucket.name
  role    = "roles/storage.admin"
  members = ["serviceAccount:${google_service_account.test_service_account.email}"]
}

# Uploads a sample CSV file to the test data bucket
resource "google_storage_bucket_object" "test_data_csv" {
  name         = "test_data/sample_data.csv"
  bucket       = google_storage_bucket.test_data_bucket.name
  source       = "${path.module}/../../../mock_data/gcs/sample_data.csv"
  content_type = "text/csv"
}

# Uploads a sample JSON file to the test data bucket
resource "google_storage_bucket_object" "test_data_json" {
  name         = "test_data/sample_data.json"
  bucket       = google_storage_bucket.test_data_bucket.name
  source       = "${path.module}/../../../mock_data/gcs/sample_data.json"
  content_type = "application/json"
}

# Uploads a sample Parquet file to the test data bucket
resource "google_storage_bucket_object" "test_data_parquet" {
  name         = "test_data/sample_data.parquet"
  bucket       = google_storage_bucket.test_data_bucket.name
  source       = "${path.module}/../../../mock_data/gcs/sample_data.parquet"
  content_type = "application/octet-stream"
}

# Uploads a sample Avro file to the test data bucket
resource "google_storage_bucket_object" "test_data_avro" {
  name         = "test_data/sample_data.avro"
  bucket       = google_storage_bucket.test_data_bucket.name
  source       = "${path.module}/../../../mock_data/gcs/sample_data.avro"
  content_type = "application/octet-stream"
}

# Creates a Pub/Sub topic for test data bucket notifications
resource "google_pubsub_topic" "test_data_notifications" {
  name    = "${var.resource_prefix}-data-notifications-${random_id.test_environment_suffix.hex}"
  project = var.project_id
  labels  = local.common_labels
}

# Grants the GCS service account permission to publish to the topic
resource "google_pubsub_topic_iam_binding" "test_data_notifications_binding" {
  project = var.project_id
  topic   = google_pubsub_topic.test_data_notifications.name
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:service-${data.google_project.current.number}@gs-project-accounts.iam.gserviceaccount.com"]
}

# Creates a notification configuration for the test data bucket
resource "google_storage_notification" "test_data_notification" {
  bucket         = google_storage_bucket.test_data_bucket.name
  payload_format = "JSON"
  topic          = google_pubsub_topic.test_data_notifications.name
  event_types    = ["OBJECT_FINALIZE", "OBJECT_DELETE"]
  custom_attributes = {
    environment = "test"
  }
  depends_on = [google_pubsub_topic_iam_binding.test_data_notifications_binding]
}

# Creates a subscription for the test data notifications topic
resource "google_pubsub_subscription" "test_data_notifications_subscription" {
  name                       = "${var.resource_prefix}-data-notifications-sub-${random_id.test_environment_suffix.hex}"
  topic                      = google_pubsub_topic.test_data_notifications.name
  project                    = var.project_id
  ack_deadline_seconds       = 20
  message_retention_duration = "604800s"  # 7 days
  retain_acked_messages      = true
  expiration_policy {
    ttl = "2592000s"  # 30 days
  }
  labels = local.common_labels
}

# Map of all test storage buckets for easy reference
locals {
  test_storage_buckets = {
    data    = google_storage_bucket.test_data_bucket.name
    staging = google_storage_bucket.test_staging_bucket.name
    archive = google_storage_bucket.test_archive_bucket.name
  }
}

# Outputs
output "test_data_bucket_name" {
  description = "The name of the test data bucket"
  value       = google_storage_bucket.test_data_bucket.name
}

output "test_staging_bucket_name" {
  description = "The name of the test staging bucket"
  value       = google_storage_bucket.test_staging_bucket.name
}

output "test_archive_bucket_name" {
  description = "The name of the test archive bucket"
  value       = google_storage_bucket.test_archive_bucket.name
}

output "test_data_notification_topic" {
  description = "The name of the Pub/Sub topic for test data notifications"
  value       = google_pubsub_topic.test_data_notifications.name
}

output "test_data_notification_subscription" {
  description = "The name of the subscription for test data notifications"
  value       = google_pubsub_subscription.test_data_notifications_subscription.name
}