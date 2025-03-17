# Main Terraform configuration for VPC in the self-healing data pipeline

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
  }
}

provider "google" {
  # Configuration options
}

provider "google-beta" {
  # Configuration options
}

# Local variables
locals {
  subnet_resource = ${var.enable_flow_logs ? google_compute_subnetwork.primary_subnet[0] : google_compute_subnetwork.primary_subnet_no_logs[0]}
}

# VPC Network
resource "google_compute_network" "vpc_network" {
  name                            = "${var.vpc_name}-${var.environment}"
  project                         = var.project_id
  auto_create_subnetworks         = false
  routing_mode                    = "GLOBAL"
  delete_default_routes_on_create = false
  description                     = "VPC network for the self-healing data pipeline ${var.environment} environment"
  mtu                             = 1460
}

# Primary subnet with flow logs
resource "google_compute_subnetwork" "primary_subnet" {
  name                     = "${var.vpc_name}-subnet-${var.environment}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.vpc_network.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = var.enable_private_google_access
  description              = "Primary subnet for the self-healing data pipeline ${var.environment} environment"
  
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }
  
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
  
  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = var.flow_logs_sampling
    metadata             = "INCLUDE_ALL_METADATA"
  }
  
  count = var.enable_flow_logs ? 1 : 0
}

# Primary subnet without flow logs
resource "google_compute_subnetwork" "primary_subnet_no_logs" {
  name                     = "${var.vpc_name}-subnet-${var.environment}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.vpc_network.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = var.enable_private_google_access
  description              = "Primary subnet for the self-healing data pipeline ${var.environment} environment"
  
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }
  
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
  
  count = var.enable_flow_logs ? 0 : 1
}

# Cloud Router for NAT gateway
resource "google_compute_router" "router" {
  name        = "${var.vpc_name}-router-${var.environment}"
  project     = var.project_id
  region      = var.region
  network     = google_compute_network.vpc_network.id
  description = "Router for the self-healing data pipeline ${var.environment} environment"
}

# Cloud NAT gateway for private instances to access the internet
resource "google_compute_router_nat" "nat" {
  name                               = "${var.vpc_name}-nat-${var.environment}"
  project                            = var.project_id
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  min_ports_per_vm                   = var.nat_min_ports_per_vm
  
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# VPC Access Connector for serverless services to connect to the VPC
resource "google_vpc_access_connector" "vpc_connector" {
  name          = "${var.vpc_name}-connector-${var.environment}"
  project       = var.project_id
  region        = var.region
  ip_cidr_range = var.connector_cidr
  network       = google_compute_network.vpc_network.id
  min_instances = 2
  max_instances = 10
  machine_type  = "e2-standard-4"
}

# Private IP range for private service access (e.g., Cloud SQL)
resource "google_compute_global_address" "private_service_access_range" {
  name          = "${var.vpc_name}-psa-range-${var.environment}"
  project       = var.project_id
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc_network.id
}

# Private service access connection for services like Cloud SQL
resource "google_service_networking_connection" "private_service_access" {
  network                 = google_compute_network.vpc_network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_service_access_range.name]
}

# Firewall rule to allow internal communication within the VPC
resource "google_compute_firewall" "allow_internal" {
  name        = "${var.vpc_name}-allow-internal-${var.environment}"
  project     = var.project_id
  network     = google_compute_network.vpc_network.id
  description = "Allow internal communication between instances in the VPC"
  direction   = "INGRESS"
  
  source_ranges = [
    var.subnet_cidr,
    var.pods_cidr,
    var.services_cidr
  ]
  
  allow {
    protocol = "tcp"
  }
  
  allow {
    protocol = "udp"
  }
  
  allow {
    protocol = "icmp"
  }
  
  priority = 1000
}

# Firewall rule to allow health checks from Google Cloud
resource "google_compute_firewall" "allow_health_checks" {
  name        = "${var.vpc_name}-allow-health-checks-${var.environment}"
  project     = var.project_id
  network     = google_compute_network.vpc_network.id
  description = "Allow health checks from Google Cloud load balancers and health check systems"
  direction   = "INGRESS"
  
  source_ranges = [
    "35.191.0.0/16",
    "130.211.0.0/22",
    "209.85.152.0/22",
    "209.85.204.0/22"
  ]
  
  allow {
    protocol = "tcp"
  }
  
  priority = 1000
}

# Firewall rule to allow SSH and RDP via Identity-Aware Proxy
resource "google_compute_firewall" "allow_iap" {
  name        = "${var.vpc_name}-allow-iap-${var.environment}"
  project     = var.project_id
  network     = google_compute_network.vpc_network.id
  description = "Allow SSH and RDP access via Identity-Aware Proxy"
  direction   = "INGRESS"
  
  source_ranges = ["35.235.240.0/20"]
  
  allow {
    protocol = "tcp"
    ports    = ["22", "3389"]
  }
  
  priority = 1000
}

# DNS policy for the VPC network
resource "google_dns_policy" "dns_policy" {
  name                      = "${var.vpc_name}-dns-policy-${var.environment}"
  project                   = var.project_id
  description               = "DNS policy for the self-healing data pipeline ${var.environment} environment"
  enable_inbound_forwarding = false
  enable_logging            = true
  
  networks {
    network_url = google_compute_network.vpc_network.id
  }
  
  alternative_name_server_config {
    target_name_servers {
      ipv4_address = "8.8.8.8"
    }
    target_name_servers {
      ipv4_address = "8.8.4.4"
    }
  }
}

# Output values
output "network_id" {
  value       = google_compute_network.vpc_network.id
  description = "The ID of the VPC network"
}

output "network_name" {
  value       = google_compute_network.vpc_network.name
  description = "The name of the VPC network"
}

output "network_self_link" {
  value       = google_compute_network.vpc_network.self_link
  description = "The self-link of the VPC network"
}

output "subnet_id" {
  value       = local.subnet_resource.id
  description = "The ID of the primary subnet"
}

output "subnet_name" {
  value       = local.subnet_resource.name
  description = "The name of the primary subnet"
}

output "subnet_self_link" {
  value       = local.subnet_resource.self_link
  description = "The self-link of the primary subnet"
}

output "subnet_cidr" {
  value       = local.subnet_resource.ip_cidr_range
  description = "The primary IP CIDR range of the subnet"
}

output "pods_cidr" {
  value       = local.subnet_resource.secondary_ip_range[0].ip_cidr_range
  description = "The secondary IP CIDR range for Kubernetes pods"
}

output "services_cidr" {
  value       = local.subnet_resource.secondary_ip_range[1].ip_cidr_range
  description = "The secondary IP CIDR range for Kubernetes services"
}

output "vpc_connector_id" {
  value       = google_vpc_access_connector.vpc_connector.id
  description = "The ID of the VPC access connector"
}

output "vpc_connector_name" {
  value       = google_vpc_access_connector.vpc_connector.name
  description = "The name of the VPC access connector"
}

output "vpc_connector_self_link" {
  value       = google_vpc_access_connector.vpc_connector.self_link
  description = "The self-link of the VPC access connector"
}

output "router_name" {
  value       = google_compute_router.router.name
  description = "The name of the Cloud Router"
}

output "nat_name" {
  value       = google_compute_router_nat.nat.name
  description = "The name of the Cloud NAT gateway"
}

output "private_service_access_connection" {
  value       = google_service_networking_connection.private_service_access.id
  description = "The ID of the private service access connection"
}

output "private_service_access_range" {
  value       = google_compute_global_address.private_service_access_range.name
  description = "The name of the allocated IP range for private service access"
}

output "firewall_rules" {
  value = {
    allow_internal      = google_compute_firewall.allow_internal.name
    allow_health_checks = google_compute_firewall.allow_health_checks.name
    allow_iap           = google_compute_firewall.allow_iap.name
  }
  description = "Map of firewall rule names created for the VPC"
}

output "dns_policy_name" {
  value       = google_dns_policy.dns_policy.name
  description = "The name of the DNS policy for the VPC network"
}

output "region" {
  value       = var.region
  description = "The region where the VPC resources are deployed"
}

output "environment" {
  value       = var.environment
  description = "The deployment environment (dev, staging, prod)"
}