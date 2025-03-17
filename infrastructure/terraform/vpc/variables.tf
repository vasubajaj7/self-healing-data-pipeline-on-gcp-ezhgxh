# Variables for VPC infrastructure provisioning
# This file defines all input variables for the VPC module used in the self-healing data pipeline

variable "project_id" {
  description = "The Google Cloud Project ID where the VPC resources will be deployed"
  type        = string
  
  validation {
    condition     = length(var.project_id) > 0
    error_message = "The project_id variable must be provided and cannot be empty."
  }
}

variable "region" {
  description = "The Google Cloud region where the VPC resources will be deployed"
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

variable "vpc_name" {
  description = "The name of the VPC network"
  type        = string
  default     = "pipeline-vpc"
  
  validation {
    condition     = length(var.vpc_name) > 0
    error_message = "The vpc_name variable must be provided and cannot be empty."
  }
}

variable "subnet_cidr" {
  description = "The primary IP CIDR range for the subnet"
  type        = string
  default     = "10.0.0.0/20"
  
  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.subnet_cidr))
    error_message = "The subnet_cidr variable must be a valid CIDR block."
  }
}

variable "pods_cidr" {
  description = "The secondary IP CIDR range for Kubernetes pods"
  type        = string
  default     = "10.1.0.0/16"
  
  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.pods_cidr))
    error_message = "The pods_cidr variable must be a valid CIDR block."
  }
}

variable "services_cidr" {
  description = "The secondary IP CIDR range for Kubernetes services"
  type        = string
  default     = "10.2.0.0/20"
  
  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.services_cidr))
    error_message = "The services_cidr variable must be a valid CIDR block."
  }
}

variable "connector_cidr" {
  description = "The CIDR range for the VPC Access Connector"
  type        = string
  default     = "10.8.0.0/28"
  
  validation {
    condition     = can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", var.connector_cidr))
    error_message = "The connector_cidr variable must be a valid CIDR block."
  }
}

variable "enable_private_google_access" {
  description = "Whether to enable Private Google Access for the subnet"
  type        = bool
  default     = true
}

variable "enable_flow_logs" {
  description = "Whether to enable VPC flow logs for the subnet"
  type        = bool
  default     = true
}

variable "flow_logs_sampling" {
  description = "The sampling rate for VPC flow logs (1.0 means all logs)"
  type        = number
  default     = 0.5
  
  validation {
    condition     = var.flow_logs_sampling >= 0.0 && var.flow_logs_sampling <= 1.0
    error_message = "The flow_logs_sampling variable must be between 0.0 and 1.0."
  }
}

variable "nat_min_ports_per_vm" {
  description = "Minimum number of ports allocated to a VM from the NAT gateway"
  type        = number
  default     = 64
  
  validation {
    condition     = var.nat_min_ports_per_vm >= 64 && var.nat_min_ports_per_vm <= 65536
    error_message = "The nat_min_ports_per_vm variable must be between 64 and 65536."
  }
}