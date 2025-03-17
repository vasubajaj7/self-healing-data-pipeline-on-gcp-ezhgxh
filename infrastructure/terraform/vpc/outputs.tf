# VPC Module Outputs
# ------------------
# This file defines all output variables that expose the created VPC resources' attributes
# to other Terraform modules or the root module, enabling resource referencing and integration
# across the infrastructure.

# VPC Network Outputs
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

# Subnet Outputs
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

# VPC Access Connector Outputs
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

# Cloud Router and NAT Outputs
output "router_name" {
  value       = google_compute_router.router.name
  description = "The name of the Cloud Router"
}

output "nat_name" {
  value       = google_compute_router_nat.nat.name
  description = "The name of the Cloud NAT gateway"
}

# Private Service Access Outputs
output "private_service_access_connection" {
  value       = google_service_networking_connection.private_service_access.id
  description = "The ID of the private service access connection"
}

output "private_service_access_range" {
  value       = google_compute_global_address.private_service_access_range.name
  description = "The name of the allocated IP range for private service access"
}

# Firewall Rule Outputs
output "firewall_rules" {
  value = {
    allow_internal      = google_compute_firewall.allow_internal.name
    allow_health_checks = google_compute_firewall.allow_health_checks.name
    allow_iap           = google_compute_firewall.allow_iap.name
  }
  description = "Map of firewall rule names created for the VPC"
}

# DNS Policy Output
output "dns_policy_name" {
  value       = google_dns_policy.dns_policy.name
  description = "The name of the DNS policy for the VPC network"
}

# Environment Information Outputs
output "region" {
  value       = var.region
  description = "The region where the VPC resources are deployed"
}

output "environment" {
  value       = var.environment
  description = "The deployment environment (dev, staging, prod)"
}