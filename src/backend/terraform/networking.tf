# Provider configuration
provider "google" {
  # Using Google Cloud Provider version 4.80.0
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  # Using Google Cloud Beta Provider version 4.80.0
  project = var.project_id
  region  = var.region
}

# Local values for frequently used or computed values
locals {
  network_self_link = var.create_network ? google_compute_network.vpc_network[0].self_link : data.google_compute_network.existing_network[0].self_link
  subnet_self_link  = var.create_network ? google_compute_subnetwork.vpc_subnet[0].self_link : data.google_compute_subnetwork.existing_subnet[0].self_link
}

# Data sources for existing network and subnet (used when create_network is false)
data "google_compute_network" "existing_network" {
  count   = var.create_network ? 0 : 1
  name    = var.network_name
  project = var.project_id
}

data "google_compute_subnetwork" "existing_subnet" {
  count   = var.create_network ? 0 : 1
  name    = var.subnet_name
  project = var.project_id
  region  = var.region
}

# VPC Network for the self-healing data pipeline
resource "google_compute_network" "vpc_network" {
  count                           = var.create_network ? 1 : 0
  name                            = "${var.resource_prefix}-${var.network_name}-${var.environment}"
  project                         = var.project_id
  auto_create_subnetworks         = false
  routing_mode                    = "GLOBAL"
  delete_default_routes_on_create = false
  description                     = "VPC network for the self-healing data pipeline"
  mtu                             = 1460
}

# Subnet within the VPC network
resource "google_compute_subnetwork" "vpc_subnet" {
  count                    = var.create_network ? 1 : 0
  name                     = "${var.resource_prefix}-${var.subnet_name}-${var.environment}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.vpc_network[0].self_link
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
  description              = "Subnet for the self-healing data pipeline"
  
  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

# Private IP range for service connections (Cloud SQL, Redis, etc.)
resource "google_compute_global_address" "private_service_range" {
  count        = var.enable_private_services && var.create_network ? 1 : 0
  name         = "${var.resource_prefix}-private-service-range-${var.environment}"
  project      = var.project_id
  purpose      = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network      = google_compute_network.vpc_network[0].self_link
}

# Private service connection for Google managed services
resource "google_service_networking_connection" "private_service_connection" {
  count                   = var.enable_private_services && var.create_network ? 1 : 0
  network                 = google_compute_network.vpc_network[0].self_link
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_service_range[0].name]
}

# VPC Access Connector for serverless services (Cloud Functions, Cloud Run)
resource "google_vpc_access_connector" "vpc_connector" {
  count         = var.create_network ? 1 : 0
  name          = "${var.resource_prefix}-vpc-connector-${var.environment}"
  project       = var.project_id
  region        = var.region
  network       = google_compute_network.vpc_network[0].self_link
  ip_cidr_range = "10.8.0.0/28"
  machine_type  = "e2-standard-4"
  min_instances = 2
  max_instances = 10
  max_throughput = 1000
}

# Firewall rule to allow internal communication between pipeline components
resource "google_compute_firewall" "allow_internal" {
  count       = var.create_network ? 1 : 0
  name        = "${var.resource_prefix}-allow-internal-${var.environment}"
  project     = var.project_id
  network     = google_compute_network.vpc_network[0].self_link
  description = "Allow internal communication between pipeline components"
  direction   = "INGRESS"
  source_ranges = [var.subnet_cidr, "10.8.0.0/28"]
  
  allow {
    protocol = "tcp"
  }
  
  allow {
    protocol = "udp"
  }
  
  allow {
    protocol = "icmp"
  }
  
  priority    = 1000
  target_tags = ["pipeline-component"]
}

# Firewall rule to allow Google Cloud health checks
resource "google_compute_firewall" "allow_health_checks" {
  count       = var.create_network ? 1 : 0
  name        = "${var.resource_prefix}-allow-health-checks-${var.environment}"
  project     = var.project_id
  network     = google_compute_network.vpc_network[0].self_link
  description = "Allow health checks from Google Cloud"
  direction   = "INGRESS"
  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
  
  allow {
    protocol = "tcp"
  }
  
  priority    = 1000
  target_tags = ["pipeline-component"]
}

# Cloud Router for NAT gateway
resource "google_compute_router" "router" {
  count       = var.create_network ? 1 : 0
  name        = "${var.resource_prefix}-router-${var.environment}"
  project     = var.project_id
  region      = var.region
  network     = google_compute_network.vpc_network[0].self_link
  description = "Router for the self-healing data pipeline"
}

# NAT gateway for outbound internet access from private instances
resource "google_compute_router_nat" "nat_gateway" {
  count                              = var.create_network ? 1 : 0
  name                               = "${var.resource_prefix}-nat-${var.environment}"
  project                            = var.project_id
  router                             = google_compute_router.router[0].name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# Configure route exchange for the VPC peering connection
resource "google_compute_network_peering_routes_config" "peering_routes" {
  count                = var.enable_private_services && var.create_network ? 1 : 0
  project              = var.project_id
  peering              = "servicenetworking-googleapis-com"
  network              = google_compute_network.vpc_network[0].name
  import_custom_routes = true
  export_custom_routes = true
  
  depends_on = [google_service_networking_connection.private_service_connection]
}

# Outputs for use in other Terraform modules
output "network_name" {
  value       = var.create_network ? google_compute_network.vpc_network[0].name : data.google_compute_network.existing_network[0].name
  description = "The name of the VPC network"
}

output "network_self_link" {
  value       = var.create_network ? google_compute_network.vpc_network[0].self_link : data.google_compute_network.existing_network[0].self_link
  description = "The self-link of the VPC network"
}

output "subnet_name" {
  value       = var.create_network ? google_compute_subnetwork.vpc_subnet[0].name : data.google_compute_subnetwork.existing_subnet[0].name
  description = "The name of the subnet"
}

output "subnet_self_link" {
  value       = var.create_network ? google_compute_subnetwork.vpc_subnet[0].self_link : data.google_compute_subnetwork.existing_subnet[0].self_link
  description = "The self-link of the subnet"
}

output "vpc_connector_name" {
  value       = var.create_network ? google_compute_router.router[0].name : ""
  description = "The name of the VPC access connector for serverless services"
}

output "private_service_connection_name" {
  value       = var.enable_private_services && var.create_network ? google_service_networking_connection.private_service_connection[0].network : ""
  description = "The name of the private service connection"
}