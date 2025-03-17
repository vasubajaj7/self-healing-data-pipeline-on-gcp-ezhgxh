# Provider configuration for the GCP test environment
# Using specific provider versions to ensure consistency across environments

terraform {
  required_version = ">= 1.3.0"
  
  # Required providers block defines all providers needed for the test environment
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
}

# Main Google Cloud provider configuration
provider "google" {
  project             = var.project_id
  region              = var.region
  request_timeout     = "60s"
  user_project_override = true
}

# Google Cloud Beta provider for features not yet in the main provider
provider "google-beta" {
  project             = var.project_id
  region              = var.region
  request_timeout     = "60s"
  user_project_override = true
}

# Random provider for generating unique identifiers
provider "random" {
}

# Time provider for time-based operations
provider "time" {
}