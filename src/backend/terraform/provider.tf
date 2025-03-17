###############################################
# Self-Healing Data Pipeline - Provider Configuration
###############################################

# Terraform configuration block defining provider requirements and backend configuration
terraform {
  # Specify minimum Terraform version
  required_version = ">= 1.0.0"
  
  # Define required providers with versions
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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9.0"
    }
  }
  
  # Backend configuration for Terraform state storage in GCS
  # Note: For environment-specific state buckets, use:
  # terraform init -backend-config="bucket=${var.resource_prefix}-${var.environment}-terraform-state"
  backend "gcs" {
    prefix = "terraform/state"
  }
}

# Google Cloud provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = "${var.region}-a"
}

# Google Cloud Beta provider configuration for beta features
provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = "${var.region}-a"
}

# Random provider for generating unique identifiers
provider "random" {
}

# Time provider for handling time-based operations
provider "time" {
}

# Enable required Google Cloud APIs for the project
resource "google_project_service" "required_apis" {
  for_each = toset(var.required_apis)
  
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}