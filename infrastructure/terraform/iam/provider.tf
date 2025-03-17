# Provider configuration for the IAM module
# This file defines the required providers and their versions needed to 
# manage Identity and Access Management resources for the self-healing data pipeline.

terraform {
  # Specify the required Terraform version
  required_version = "~> 1.5.0"
  
  # Define required providers with specific versions
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
  
  # Backend configuration for storing Terraform state
  # Note: Specific backend config should be provided at init time
  # Example: terraform init -backend-config="bucket=${PROJECT_ID}-terraform-state" -backend-config="prefix=iam"
  backend "gcs" {}
}

# Configure the Google Cloud provider with project and region
provider "google" {
  project = var.project_id
  region  = var.region
}

# Configure the Google Cloud Beta provider with project and region
provider "google-beta" {
  project = var.project_id
  region  = var.region
}