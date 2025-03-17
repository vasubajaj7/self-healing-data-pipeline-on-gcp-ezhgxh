# Cluster identification outputs
output "cluster_id" {
  description = "The unique identifier for the GKE cluster"
  value       = google_container_cluster.primary.id
}

output "cluster_name" {
  description = "The name of the GKE cluster"
  value       = google_container_cluster.primary.name
}

output "cluster_location" {
  description = "The location (region) of the GKE cluster"
  value       = google_container_cluster.primary.location
}

# Cluster access outputs
output "cluster_endpoint" {
  description = "The IP address of the Kubernetes master endpoint"
  value       = google_container_cluster.primary.endpoint
}

output "cluster_ca_certificate" {
  description = "The public certificate of the cluster's certificate authority, base64 encoded"
  value       = google_container_cluster.primary.master_auth.0.cluster_ca_certificate
  sensitive   = true
}

output "access_token" {
  description = "The access token for authenticating to the Kubernetes cluster"
  value       = data.google_client_config.default.access_token
  sensitive   = true
}

# Node pool outputs
output "general_purpose_node_pool_id" {
  description = "The ID of the general purpose node pool"
  value       = google_container_node_pool.general_purpose.id
}

output "general_purpose_node_pool_name" {
  description = "The name of the general purpose node pool"
  value       = google_container_node_pool.general_purpose.name
}

output "memory_optimized_node_pool_id" {
  description = "The ID of the memory optimized node pool"
  value       = google_container_node_pool.memory_optimized.id
}

output "memory_optimized_node_pool_name" {
  description = "The name of the memory optimized node pool"
  value       = google_container_node_pool.memory_optimized.name
}

output "compute_optimized_node_pool_id" {
  description = "The ID of the compute optimized node pool"
  value       = google_container_node_pool.compute_optimized.id
}

output "compute_optimized_node_pool_name" {
  description = "The name of the compute optimized node pool"
  value       = google_container_node_pool.compute_optimized.name
}

# Kubernetes provider configuration
output "kubernetes_provider_config" {
  description = "Configuration object for the Kubernetes provider"
  value = {
    host                   = "https://${google_container_cluster.primary.endpoint}"
    token                  = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth.0.cluster_ca_certificate)
  }
  sensitive = true
}