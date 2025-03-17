# Core Terraform configuration for the GCP test environment, defining the foundational resources
# needed for testing the self-healing data pipeline, including network infrastructure, service accounts,
# and common resource configurations.

terraform {
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

# Common labels for all test resources
locals {
  common_labels = merge(var.labels, { "test-environment-id": random_id.test_environment_suffix.hex })
}

# Generate a random suffix for test environment resources to prevent name collisions
resource "random_id" "test_environment_suffix" {
  byte_length = 4
  keepers = {
    environment_id = var.test_environment_id != null ? var.test_environment_id : uuid()
  }
}

# Creates a VPC network for the test environment
resource "google_compute_network" "test_network" {
  name                            = "${var.test_network_name}-${random_id.test_environment_suffix.hex}"
  project                         = var.project_id
  auto_create_subnetworks         = false
  description                     = "VPC network for self-healing pipeline test environment"
  routing_mode                    = "REGIONAL"
  delete_default_routes_on_create = false
  mtu                             = 1500
}

# Creates a subnet within the test VPC network
resource "google_compute_subnetwork" "test_subnet" {
  name                     = "${var.test_subnet_name}-${random_id.test_environment_suffix.hex}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.test_network.id
  ip_cidr_range            = var.test_subnet_cidr
  private_ip_google_access = true
  description              = "Subnet for self-healing pipeline test environment"
}

# Creates a service account for test resources
resource "google_service_account" "test_service_account" {
  account_id   = "${var.test_service_account_name}-${random_id.test_environment_suffix.hex}"
  project      = var.project_id
  display_name = "Self-Healing Pipeline Test Service Account"
  description  = "Service account for self-healing pipeline test environment"
}

# Assigns IAM roles to the test service account
resource "google_project_iam_member" "test_service_account_roles" {
  for_each = toset(var.test_service_account_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.test_service_account.email}"
}

# Creates a firewall rule to allow internal communication within the test network
resource "google_compute_firewall" "test_allow_internal" {
  name        = "allow-internal-${random_id.test_environment_suffix.hex}"
  project     = var.project_id
  network     = google_compute_network.test_network.name
  description = "Allow internal communication within the test network"
  direction   = "INGRESS"
  
  source_ranges = [var.test_subnet_cidr]
  
  allow {
    protocol = "tcp"
  }
  
  allow {
    protocol = "udp"
  }
  
  allow {
    protocol = "icmp"
  }
}

# Creates a firewall rule to allow IAP access to test resources
resource "google_compute_firewall" "test_allow_iap" {
  name        = "allow-iap-${random_id.test_environment_suffix.hex}"
  project     = var.project_id
  network     = google_compute_network.test_network.name
  description = "Allow IAP access to test resources"
  direction   = "INGRESS"
  
  source_ranges = ["35.235.240.0/20"]  # IAP source range
  
  allow {
    protocol = "tcp"
    ports    = ["22", "3389"]  # SSH and RDP
  }
}

# Adds a delay to allow IAM permissions to propagate
resource "time_sleep" "wait_for_iam_propagation" {
  depends_on      = [google_project_iam_member.test_service_account_roles]
  create_duration = "30s"
}

# Sets an expiry time for the test environment based on TTL
resource "time_offset" "test_environment_expiry" {
  count        = var.auto_destroy_test_environment ? 1 : 0
  offset_hours = var.test_environment_ttl
}

# Schedules automatic cleanup of the test environment after TTL
resource "null_resource" "test_environment_cleanup" {
  count = var.auto_destroy_test_environment ? 1 : 0
  
  triggers = {
    expiry_time = time_offset.test_environment_expiry[0].rfc3339
  }
  
  provisioner "local-exec" {
    command = "echo 'Test environment ${random_id.test_environment_suffix.hex} will expire at ${time_offset.test_environment_expiry[0].rfc3339}' && sleep ${var.test_environment_ttl * 3600} && echo 'Destroying test environment ${random_id.test_environment_suffix.hex}' && cd ${path.module} && terraform destroy -auto-approve"
    interpreter = ["/bin/bash", "-c"]
  }
}

# Outputs
output "test_environment_id" {
  description = "The unique identifier for this test environment"
  value       = random_id.test_environment_suffix.hex
}

output "test_network_id" {
  description = "The ID of the test VPC network"
  value       = google_compute_network.test_network.id
}

output "test_subnet_id" {
  description = "The ID of the test subnet"
  value       = google_compute_subnetwork.test_subnet.id
}

output "test_service_account_email" {
  description = "The email of the test service account"
  value       = google_service_account.test_service_account.email
}

output "test_environment_expiry" {
  description = "The expiry time of the test environment"
  value       = var.auto_destroy_test_environment ? time_offset.test_environment_expiry[0].rfc3339 : "No expiry set"
}