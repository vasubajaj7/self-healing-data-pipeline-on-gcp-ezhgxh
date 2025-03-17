# Terraform provider configuration for GKE deployment
# This file defines the providers required for deploying and managing 
# Google Kubernetes Engine clusters for the self-healing data pipeline

terraform {
  # Specify the required Terraform version
  required_version = "~> 1.5.0"

  # Define required providers with their versions
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.80.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5.0"
    }
  }

  # Backend configuration for storing Terraform state
  # Note: The bucket name needs to be provided at initialization time or through a backend.tf file
  # Example initialization: 
  # terraform init -backend-config="bucket=${PROJECT_ID}-terraform-state"
  backend "gcs" {
    prefix = "gke"
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

# Get Google client configuration to access the GKE cluster
data "google_client_config" "default" {}

# Configure Kubernetes provider for managing resources in the GKE cluster
# This depends on the GKE cluster being created first
provider "kubernetes" {
  host                   = "https://${google_container_cluster.primary.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth.0.cluster_ca_certificate)
}