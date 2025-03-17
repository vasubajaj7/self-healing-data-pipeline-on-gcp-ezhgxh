# Project and region variables
variable "project_id" {
  description = "The Google Cloud Project ID where the GKE cluster will be deployed"
  type        = string

  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be provided and cannot be empty."
  }
}

variable "region" {
  description = "The Google Cloud region where the GKE cluster will be deployed"
  type        = string
  default     = "us-central1"

  validation {
    condition     = length(var.region) > 0
    error_message = "The region variable must be provided and cannot be empty."
  }
}

variable "environment" {
  description = "The deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "The environment variable must be one of: dev, staging, prod."
  }
}

# Cluster configuration variables
variable "cluster_name" {
  description = "The name of the GKE cluster"
  type        = string
  default     = "self-healing-pipeline"

  validation {
    condition     = length(var.cluster_name) > 0
    error_message = "The cluster_name variable must be provided and cannot be empty."
  }
}

# Networking variables
variable "network_name" {
  description = "The name of the VPC network for the GKE cluster"
  type        = string
  default     = "pipeline-vpc"

  validation {
    condition     = length(var.network_name) > 0
    error_message = "The network_name variable must be provided and cannot be empty."
  }
}

variable "subnetwork_name" {
  description = "The name of the subnetwork for the GKE cluster"
  type        = string
  default     = "pipeline-subnet"

  validation {
    condition     = length(var.subnetwork_name) > 0
    error_message = "The subnetwork_name variable must be provided and cannot be empty."
  }
}

variable "cluster_ipv4_cidr_block" {
  description = "The IP address range for pods in the GKE cluster"
  type        = string
  default     = "10.1.0.0/16"

  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.cluster_ipv4_cidr_block))
    error_message = "The cluster_ipv4_cidr_block variable must be a valid CIDR block."
  }
}

variable "services_ipv4_cidr_block" {
  description = "The IP address range for services in the GKE cluster"
  type        = string
  default     = "10.2.0.0/20"

  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.services_ipv4_cidr_block))
    error_message = "The services_ipv4_cidr_block variable must be a valid CIDR block."
  }
}

variable "master_ipv4_cidr_block" {
  description = "The IP address range for the master network in the GKE cluster"
  type        = string
  default     = "172.16.0.0/28"

  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.master_ipv4_cidr_block))
    error_message = "The master_ipv4_cidr_block variable must be a valid CIDR block."
  }
}

# Security variables
variable "enable_private_endpoint" {
  description = "Whether the master's internal IP address is used as the cluster endpoint"
  type        = bool
  default     = false
}

variable "authorized_networks" {
  description = "List of CIDR blocks that can access the Kubernetes master through HTTPS"
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = []
}

variable "node_service_account" {
  description = "The service account to be used by the node VMs"
  type        = string
}

# General Purpose node pool variables
variable "gp_initial_node_count" {
  description = "Initial node count for the general purpose node pool"
  type        = number
  default     = 1

  validation {
    condition     = var.gp_initial_node_count > 0
    error_message = "The gp_initial_node_count variable must be greater than 0."
  }
}

variable "gp_min_node_count" {
  description = "Minimum node count for the general purpose node pool autoscaling"
  type        = number
  default     = 1

  validation {
    condition     = var.gp_min_node_count > 0
    error_message = "The gp_min_node_count variable must be greater than 0."
  }
}

variable "gp_max_node_count" {
  description = "Maximum node count for the general purpose node pool autoscaling"
  type        = number
  default     = 5

  validation {
    condition     = var.gp_max_node_count >= var.gp_min_node_count
    error_message = "The gp_max_node_count variable must be greater than or equal to gp_min_node_count."
  }
}

# Memory Optimized node pool variables
variable "mo_initial_node_count" {
  description = "Initial node count for the memory optimized node pool"
  type        = number
  default     = 0

  validation {
    condition     = var.mo_initial_node_count >= 0
    error_message = "The mo_initial_node_count variable must be greater than or equal to 0."
  }
}

variable "mo_min_node_count" {
  description = "Minimum node count for the memory optimized node pool autoscaling"
  type        = number
  default     = 0

  validation {
    condition     = var.mo_min_node_count >= 0
    error_message = "The mo_min_node_count variable must be greater than or equal to 0."
  }
}

variable "mo_max_node_count" {
  description = "Maximum node count for the memory optimized node pool autoscaling"
  type        = number
  default     = 3

  validation {
    condition     = var.mo_max_node_count >= var.mo_min_node_count
    error_message = "The mo_max_node_count variable must be greater than or equal to mo_min_node_count."
  }
}

# Compute Optimized node pool variables
variable "co_initial_node_count" {
  description = "Initial node count for the compute optimized node pool"
  type        = number
  default     = 0

  validation {
    condition     = var.co_initial_node_count >= 0
    error_message = "The co_initial_node_count variable must be greater than or equal to 0."
  }
}

variable "co_min_node_count" {
  description = "Minimum node count for the compute optimized node pool autoscaling"
  type        = number
  default     = 0

  validation {
    condition     = var.co_min_node_count >= 0
    error_message = "The co_min_node_count variable must be greater than or equal to 0."
  }
}

variable "co_max_node_count" {
  description = "Maximum node count for the compute optimized node pool autoscaling"
  type        = number
  default     = 3

  validation {
    condition     = var.co_max_node_count >= var.co_min_node_count
    error_message = "The co_max_node_count variable must be greater than or equal to co_min_node_count."
  }
}

# Notification variables
variable "notification_email" {
  description = "Email address for GKE cluster monitoring alerts"
  type        = string
}