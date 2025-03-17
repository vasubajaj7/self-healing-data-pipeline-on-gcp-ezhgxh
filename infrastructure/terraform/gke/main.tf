# Terraform configuration for GKE cluster setup for self-healing data pipeline

# Provider configuration
provider "google" {
  version = "~> 4.80.0"
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  version = "~> 4.80.0"
  project = var.project_id
  region  = var.region
}

provider "random" {
  version = "~> 3.5.0"
}

# Provider configuration for Kubernetes
provider "kubernetes" {
  version                = "~> 2.23.0"
  host                   = "https://${google_container_cluster.primary.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
}

# Data sources
data "google_client_config" "default" {}

data "google_project" "project" {
  project_id = var.project_id
}

# Primary GKE cluster
resource "google_container_cluster" "primary" {
  name     = "${var.cluster_name}-${var.environment}"
  location = var.region

  # We'll create a separate node pool later
  remove_default_node_pool = true
  initial_node_count       = 1

  # Networking configuration
  network                  = var.network_name
  subnetwork               = var.subnetwork_name
  networking_mode          = "VPC_NATIVE"
  
  ip_allocation_policy {
    cluster_ipv4_cidr_block  = var.cluster_ipv4_cidr_block
    services_ipv4_cidr_block = var.services_ipv4_cidr_block
  }

  # Private cluster configuration
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = var.enable_private_endpoint
    master_ipv4_cidr_block  = var.master_ipv4_cidr_block
  }

  # Authorized networks for master access
  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.authorized_networks
      content {
        cidr_block   = cidr_blocks.value.cidr_block
        display_name = cidr_blocks.value.display_name
      }
    }
  }

  # Workload identity configuration
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Release channel for GKE updates
  release_channel {
    channel = "REGULAR"
  }

  # Maintenance window configuration
  maintenance_policy {
    recurring_window {
      start_time = "2022-01-01T02:00:00Z"
      end_time   = "2022-01-01T06:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA,SU"
    }
  }

  # Security configuration
  binary_authorization {
    evaluation_mode = "PROJECT_SINGLETON_POLICY_ENFORCE"
  }

  # Network policy for pod-to-pod communication
  network_policy {
    enabled  = true
    provider = "CALICO"
  }

  # Add-ons configuration
  addons_config {
    http_load_balancing {
      disabled = false
    }
    horizontal_pod_autoscaling {
      disabled = false
    }
    network_policy_config {
      disabled = false
    }
    gcp_filestore_csi_driver_config {
      enabled = true
    }
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Logging configuration
  logging_config {
    component_config {
      enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
    }
  }

  # Monitoring configuration
  monitoring_config {
    component_config {
      enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
    }
  }

  # Resource labels
  resource_labels = {
    environment  = var.environment
    managed-by   = "terraform"
    application  = "self-healing-pipeline"
  }
}

# General purpose node pool
resource "google_container_node_pool" "general_purpose" {
  name       = "general-purpose"
  cluster    = google_container_cluster.primary.id
  location   = var.region
  
  initial_node_count = var.gp_initial_node_count

  # Autoscaling configuration
  autoscaling {
    min_node_count = var.gp_min_node_count
    max_node_count = var.gp_max_node_count
  }

  # Node management configuration
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # Upgrade settings
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  # Node configuration
  node_config {
    machine_type = "e2-standard-4"
    disk_size_gb = 100
    disk_type    = "pd-standard"

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    service_account = var.node_service_account != null ? var.node_service_account : "${data.google_project.project.number}-compute@developer.gserviceaccount.com"

    # Workload identity for node pool
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Security hardening
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    # Labels and taints
    labels = {
      environment   = var.environment
      node-pool-type = "general-purpose"
    }

    taint = []

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
}

# Memory optimized node pool for ML workloads
resource "google_container_node_pool" "memory_optimized" {
  name       = "memory-optimized"
  cluster    = google_container_cluster.primary.id
  location   = var.region
  
  initial_node_count = var.mo_initial_node_count

  # Autoscaling configuration
  autoscaling {
    min_node_count = var.mo_min_node_count
    max_node_count = var.mo_max_node_count
  }

  # Node management configuration
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # Upgrade settings
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  # Node configuration
  node_config {
    machine_type = "e2-highmem-4"
    disk_size_gb = 100
    disk_type    = "pd-standard"

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    service_account = var.node_service_account != null ? var.node_service_account : "${data.google_project.project.number}-compute@developer.gserviceaccount.com"

    # Workload identity for node pool
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Security hardening
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    # Labels and taints
    labels = {
      environment   = var.environment
      node-pool-type = "memory-optimized"
    }

    taint = [
      {
        key    = "workload"
        value  = "memory-optimized"
        effect = "NO_SCHEDULE"
      }
    ]

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
}

# Compute optimized node pool for compute-intensive workloads
resource "google_container_node_pool" "compute_optimized" {
  name       = "compute-optimized"
  cluster    = google_container_cluster.primary.id
  location   = var.region
  
  initial_node_count = var.co_initial_node_count

  # Autoscaling configuration
  autoscaling {
    min_node_count = var.co_min_node_count
    max_node_count = var.co_max_node_count
  }

  # Node management configuration
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # Upgrade settings
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  # Node configuration
  node_config {
    machine_type = "e2-highcpu-8"
    disk_size_gb = 100
    disk_type    = "pd-standard"

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    service_account = var.node_service_account != null ? var.node_service_account : "${data.google_project.project.number}-compute@developer.gserviceaccount.com"

    # Workload identity for node pool
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Security hardening
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    # Labels and taints
    labels = {
      environment   = var.environment
      node-pool-type = "compute-optimized"
    }

    taint = [
      {
        key    = "workload"
        value  = "compute-optimized"
        effect = "NO_SCHEDULE"
      }
    ]

    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
}

# Backup plan for the GKE cluster
resource "google_gke_backup_backup_plan" "backup_plan" {
  name     = "${var.cluster_name}-${var.environment}-backup-plan"
  cluster  = google_container_cluster.primary.id
  location = var.region
  
  retention_policy {
    backup_delete_lock_days = 0
    backup_retain_days      = 30
  }
  
  backup_schedule {
    cron_schedule = "0 0 * * *"  # Daily backup at midnight
  }
  
  backup_config {
    include_volume_data = true
    include_secrets     = true
    all_namespaces      = true
  }
}

# Email notification channel for alerts
resource "google_monitoring_notification_channel" "email_channel" {
  display_name = "${var.cluster_name}-${var.environment}-alerts"
  type         = "email"
  labels = {
    email_address = var.notification_email
  }
  force_delete = false
  count        = var.notification_email != null ? 1 : 0
}

# Alert policy for high node memory usage
resource "google_monitoring_alert_policy" "node_memory_alert" {
  display_name = "GKE Node Memory Usage"
  combiner     = "OR"
  
  conditions {
    display_name = "GKE Node Memory Usage > 90%"
    
    condition_threshold {
      filter = "resource.type = \"k8s_node\" AND resource.labels.cluster_name = \"${google_container_cluster.primary.name}\" AND metric.type = \"kubernetes.io/node/memory/allocatable_utilization\""
      
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_MEAN"
        group_by_fields      = ["resource.label.node_name"]
      }
      
      comparison      = "COMPARISON_GT"
      threshold_value = 0.9
      duration        = "300s"
      
      trigger {
        count = 1
      }
    }
  }
  
  notification_channels = [google_monitoring_notification_channel.email_channel[0].name]
  
  documentation {
    content   = "Node memory usage is above 90%. Consider scaling up the node pool or optimizing workloads."
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"
  }
  
  count = var.notification_email != null ? 1 : 0
}

# Alert policy for high node CPU usage
resource "google_monitoring_alert_policy" "node_cpu_alert" {
  display_name = "GKE Node CPU Usage"
  combiner     = "OR"
  
  conditions {
    display_name = "GKE Node CPU Usage > 80%"
    
    condition_threshold {
      filter = "resource.type = \"k8s_node\" AND resource.labels.cluster_name = \"${google_container_cluster.primary.name}\" AND metric.type = \"kubernetes.io/node/cpu/allocatable_utilization\""
      
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_MEAN"
        group_by_fields      = ["resource.label.node_name"]
      }
      
      comparison      = "COMPARISON_GT"
      threshold_value = 0.8
      duration        = "300s"
      
      trigger {
        count = 1
      }
    }
  }
  
  notification_channels = [google_monitoring_notification_channel.email_channel[0].name]
  
  documentation {
    content   = "Node CPU usage is above 80%. Consider scaling up the node pool or optimizing workloads."
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "1800s"
  }
  
  count = var.notification_email != null ? 1 : 0
}

# Alert policy for high pod restart rate
resource "google_monitoring_alert_policy" "pod_restart_alert" {
  display_name = "GKE Pod Restart Rate"
  combiner     = "OR"
  
  conditions {
    display_name = "GKE Pod Restart Rate > 5 per hour"
    
    condition_threshold {
      filter = "resource.type = \"k8s_pod\" AND resource.labels.cluster_name = \"${google_container_cluster.primary.name}\" AND metric.type = \"kubernetes.io/container/restart_count\""
      
      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.pod_name", "resource.label.namespace_name"]
      }
      
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      duration        = "0s"
      
      trigger {
        count = 1
      }
    }
  }
  
  notification_channels = [google_monitoring_notification_channel.email_channel[0].name]
  
  documentation {
    content   = "Pod restart rate is high. Investigate the pod logs and events for potential issues."
    mime_type = "text/markdown"
  }
  
  alert_strategy {
    auto_close = "86400s"
  }
  
  count = var.notification_email != null ? 1 : 0
}