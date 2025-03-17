# Terraform backend configuration for test environment
# Using GCS bucket for state storage to enable collaboration and state persistence
# The bucket should be created before applying this configuration
# State is stored under the test-environments prefix to organize different environment states

terraform {
  backend "gcs" {
    bucket      = "terraform-state-${var.project_id}"
    prefix      = "test-environments"
  }
}