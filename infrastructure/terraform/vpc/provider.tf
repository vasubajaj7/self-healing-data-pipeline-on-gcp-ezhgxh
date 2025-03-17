# Provider configuration for VPC module
# This file defines the required providers and their configuration for the self-healing data pipeline's VPC infrastructure

terraform {
  required_version = "~> 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.80.0"
    }
  }

  # Backend configuration for storing Terraform state in Google Cloud Storage
  # The bucket name is based on the project ID and would typically be
  # provided during initialization using:
  # terraform init -backend-config="bucket=${PROJECT_ID}-terraform-state"
  backend "gcs" {
    prefix = "vpc"
  }
}

# Configure the Google Cloud provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Configure the Google Cloud Beta provider for beta features
provider "google-beta" {
  project = var.project_id
  region  = var.region
}