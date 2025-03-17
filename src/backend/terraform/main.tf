########################################################
# Self-Healing Data Pipeline - Main Terraform Configuration
########################################################

# Local variables for common resource naming and tagging
locals {
  # Common labels to apply to all resources
  common_labels = merge(var.labels, {
    environment = var.environment
    project     = var.project_id
    managed_by  = "terraform"
    application = "self-healing-pipeline"
  })

  # Standard prefix for resource names including environment
  resource_name_prefix = "${var.resource_prefix}-${var.environment}"
  
  # Boolean flag for production environment
  is_production = var.environment == "prod"
}

# Retrieve information about the Google Cloud project
data "google_project" "project" {
  project_id = var.project_id
}

# Generate a random suffix for globally unique resource names
resource "random_id" "suffix" {
  byte_length = 4
  
  # Keep the suffix stable when these values don't change
  keepers = {
    project_id  = var.project_id
    environment = var.environment
  }
}

# Enable required Google Cloud APIs for the project
resource "google_project_service" "required_apis" {
  for_each = toset(var.required_apis)
  
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Add a delay after enabling APIs to ensure they are fully activated
resource "time_sleep" "api_enablement_delay" {
  depends_on      = [google_project_service.required_apis]
  create_duration = "30s"
}

# Create a storage bucket for Terraform state if specified
resource "google_storage_bucket" "terraform_state" {
  count         = var.create_terraform_state_bucket ? 1 : 0
  name          = "${var.resource_prefix}-${var.environment}-terraform-state"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  uniform_bucket_level_access = true
  labels                      = local.common_labels
  force_destroy               = false
  
  lifecycle {
    prevent_destroy = true
  }
}

# Output key values for reference by other modules or scripts
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