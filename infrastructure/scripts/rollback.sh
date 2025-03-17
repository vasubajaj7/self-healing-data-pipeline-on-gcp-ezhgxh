#!/bin/bash
# Rollback script for self-healing data pipeline
# This script automates the rollback of infrastructure and application components
# to a previous stable state across different environments (dev, staging, prod)

# Source bootstrap script for helper functions and environment variables
source "$(dirname "$0")/bootstrap.sh"

# Default values that can be overridden via command line arguments
ENVIRONMENT="dev"
REGION="us-central1"
RESOURCE_PREFIX="shp"
ROLLBACK_TERRAFORM=true
ROLLBACK_K8S=true
ROLLBACK_HELM=true
TERRAFORM_DIR="src/backend/terraform"
K8S_DIR="infrastructure/k8s"
HELM_DIR="infrastructure/helm"
TERRAFORM_STATE_BACKUP=""
K8S_REVISION="previous"
HELM_REVISION="previous"
DRY_RUN=false
SKIP_CONFIRMATION=false

# Display help message
function show_help() {
  echo "Usage: $0 --project-id=PROJECT_ID [options]"
  echo ""
  echo "Options:"
  echo "  --project-id PROJECT_ID       GCP project ID (required)"
  echo "  --environment, -e ENV         Target environment: dev, staging, prod (default: dev)"
  echo "  --region, -r REGION           GCP region (default: us-central1)"
  echo "  --resource-prefix PREFIX      Resource name prefix (default: shp)"
  echo "  --skip-terraform              Skip Terraform rollback"
  echo "  --skip-k8s                    Skip Kubernetes rollback"
  echo "  --skip-helm                   Skip Helm rollback"
  echo "  --terraform-dir DIR           Terraform directory (default: src/backend/terraform)"
  echo "  --k8s-dir DIR                 Kubernetes directory (default: infrastructure/k8s)"
  echo "  --helm-dir DIR                Helm directory (default: infrastructure/helm)"
  echo "  --terraform-state-backup FILE Path to Terraform state backup file"
  echo "  --k8s-revision REV            Kubernetes revision to rollback to (default: previous)"
  echo "  --helm-revision REV           Helm revision to rollback to (default: previous)"
  echo "  --dry-run                     Perform a dry run without making changes"
  echo "  --yes, -y                     Skip confirmation prompts"
  echo "  --help, -h                    Show this help message"
}

# Check if a command exists
function command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Log message with timestamp
function log() {
  local level=$1
  shift
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $@"
}

# Log info message
function log_info() {
  log "INFO" "$@"
}

# Log error message
function log_error() {
  log "ERROR" "$@" >&2
}

# Log warning message
function log_warning() {
  log "WARNING" "$@"
}

# Log success message
function log_success() {
  log "SUCCESS" "$@"
}

# Confirm action with user
function confirm_action() {
  local message=$1
  
  if [ "$SKIP_CONFIRMATION" = true ]; then
    return 0
  fi
  
  echo "$message"
  read -p "Continue? (y/n): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    return 1
  fi
  return 0
}

# Parse command line arguments
function parse_arguments() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --project-id)
        PROJECT_ID="$2"
        shift 2
        ;;
      --environment|-e)
        ENVIRONMENT="$2"
        shift 2
        ;;
      --region|-r)
        REGION="$2"
        shift 2
        ;;
      --resource-prefix)
        RESOURCE_PREFIX="$2"
        shift 2
        ;;
      --skip-terraform)
        ROLLBACK_TERRAFORM=false
        shift
        ;;
      --skip-k8s)
        ROLLBACK_K8S=false
        shift
        ;;
      --skip-helm)
        ROLLBACK_HELM=false
        shift
        ;;
      --terraform-dir)
        TERRAFORM_DIR="$2"
        shift 2
        ;;
      --k8s-dir)
        K8S_DIR="$2"
        shift 2
        ;;
      --helm-dir)
        HELM_DIR="$2"
        shift 2
        ;;
      --terraform-state-backup)
        TERRAFORM_STATE_BACKUP="$2"
        shift 2
        ;;
      --k8s-revision)
        K8S_REVISION="$2"
        shift 2
        ;;
      --helm-revision)
        HELM_REVISION="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      --yes|-y)
        SKIP_CONFIRMATION=true
        shift
        ;;
      --help|-h)
        show_help
        exit 0
        ;;
      *)
        echo "Unknown option: $1"
        show_help
        exit 1
        ;;
    esac
  done

  # Validate required parameters
  if [ -z "$PROJECT_ID" ]; then
    log_error "Error: --project-id is required"
    exit 1
  fi

  # Validate environment
  if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Error: environment must be one of: dev, staging, prod"
    exit 1
  fi
}

# Check prerequisites
function check_prerequisites() {
  log_info "Checking prerequisites..."
  
  # Check for required tools
  if ! command_exists "gcloud"; then
    log_error "gcloud is not installed"
    return 1
  fi
  
  if [ "$ROLLBACK_TERRAFORM" = true ] && ! command_exists "terraform"; then
    log_error "terraform is required for Terraform rollback but not installed"
    return 1
  fi
  
  if [ "$ROLLBACK_K8S" = true ] && ! command_exists "kubectl"; then
    log_error "kubectl is required for Kubernetes rollback but not installed"
    return 1
  fi
  
  if [ "$ROLLBACK_HELM" = true ] && ! command_exists "helm"; then
    log_error "helm is required for Helm rollback but not installed"
    return 1
  fi
  
  # Check if user is authenticated with gcloud
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    log_error "Not authenticated with gcloud. Run 'gcloud auth login' first."
    return 1
  fi
  
  # Check if user has necessary permissions
  log_info "Checking permissions for project $PROJECT_ID..."
  if ! gcloud projects describe "$PROJECT_ID" &>/dev/null; then
    log_error "Cannot access project $PROJECT_ID. Check if it exists and you have permission."
    return 1
  fi
  
  # Set default project
  gcloud config set project "$PROJECT_ID"
  
  # Check if GKE cluster exists if rolling back Kubernetes or Helm
  if [ "$ROLLBACK_K8S" = true ] || [ "$ROLLBACK_HELM" = true ]; then
    local cluster_name="${RESOURCE_PREFIX}-${ENVIRONMENT}-cluster"
    log_info "Checking if GKE cluster $cluster_name exists..."
    if ! gcloud container clusters describe "$cluster_name" --region="$REGION" &>/dev/null; then
      log_warning "GKE cluster $cluster_name does not exist in region $REGION"
      log_warning "Cannot rollback Kubernetes or Helm without a cluster"
      
      if [ "$ROLLBACK_K8S" = true ] || [ "$ROLLBACK_HELM" = true ]; then
        log_error "Kubernetes or Helm rollback is enabled but GKE cluster does not exist"
        return 1
      fi
    else
      # Configure kubectl to use the cluster
      log_info "Configuring kubectl to use cluster $cluster_name..."
      gcloud container clusters get-credentials "$cluster_name" --region="$REGION" --project="$PROJECT_ID"
    fi
  fi
  
  log_success "Prerequisites check passed."
  return 0
}

# Find latest Terraform state backup
function find_latest_terraform_backup() {
  if [ -n "$TERRAFORM_STATE_BACKUP" ]; then
    # Backup file already specified
    if [ ! -f "$TERRAFORM_STATE_BACKUP" ]; then
      log_error "Specified Terraform state backup file does not exist: $TERRAFORM_STATE_BACKUP"
      return 1
    fi
    log_info "Using specified Terraform state backup: $TERRAFORM_STATE_BACKUP"
    return 0
  fi
  
  # Look for the latest backup file
  log_info "Looking for latest Terraform state backup..."
  local backup_pattern="terraform-state-backup-${ENVIRONMENT}*.tfstate"
  local latest_backup=$(find . -name "$backup_pattern" -type f -printf "%T@ %p\n" 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
  
  if [ -z "$latest_backup" ]; then
    log_error "No Terraform state backup found matching pattern: $backup_pattern"
    return 1
  fi
  
  log_info "Found latest Terraform state backup: $latest_backup"
  TERRAFORM_STATE_BACKUP="$latest_backup"
  return 0
}

# Roll back infrastructure using Terraform
function rollback_terraform() {
  if [ "$ROLLBACK_TERRAFORM" = false ]; then
    log_info "Skipping Terraform rollback as requested"
    return 0
  fi

  log_info "Rolling back infrastructure using Terraform..."
  
  # Find latest Terraform state backup if not specified
  if ! find_latest_terraform_backup; then
    log_error "Failed to find Terraform state backup"
    return 1
  fi
  
  # Check if Terraform directory exists
  if [ ! -d "$TERRAFORM_DIR" ]; then
    log_error "Terraform directory $TERRAFORM_DIR does not exist"
    return 1
  fi
  
  # Change to Terraform directory
  pushd "$TERRAFORM_DIR" > /dev/null
  
  # Initialize Terraform
  log_info "Initializing Terraform..."
  if ! terraform init; then
    log_error "Failed to initialize Terraform"
    popd > /dev/null
    return 1
  fi
  
  # Select workspace based on environment
  log_info "Selecting Terraform workspace for environment: $ENVIRONMENT"
  if ! terraform workspace select "$ENVIRONMENT" 2>/dev/null; then
    log_error "Terraform workspace $ENVIRONMENT does not exist"
    popd > /dev/null
    return 1
  fi
  
  # Create backup of current state before rollback
  log_info "Creating backup of current Terraform state..."
  terraform state pull > "../../../terraform-state-current-${ENVIRONMENT}-$(date +%Y%m%d%H%M%S).tfstate"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would apply Terraform state from backup: $TERRAFORM_STATE_BACKUP"
  else
    # Confirm before applying
    if ! confirm_action "Ready to roll back Terraform state for environment: $ENVIRONMENT using backup: $TERRAFORM_STATE_BACKUP"; then
      log_info "Terraform rollback cancelled by user"
      popd > /dev/null
      return 0
    fi
    
    # Apply the backup state
    log_info "Applying Terraform state from backup..."
    if ! terraform state push "../../../$TERRAFORM_STATE_BACKUP"; then
      log_error "Failed to apply Terraform state from backup"
      popd > /dev/null
      return 1
    fi
    
    # Run terraform plan to see what would change
    log_info "Running terraform plan to see changes after rollback..."
    terraform plan
    
    # Verify rollback
    if ! verify_rollback "terraform"; then
      log_error "Terraform rollback verification failed"
      popd > /dev/null
      return 1
    fi
  fi
  
  # Return to original directory
  popd > /dev/null
  
  log_success "Terraform rollback completed successfully"
  return 0
}

# Roll back Kubernetes deployments
function rollback_kubernetes() {
  if [ "$ROLLBACK_K8S" = false ]; then
    log_info "Skipping Kubernetes rollback as requested"
    return 0
  fi

  log_info "Rolling back Kubernetes deployments..."
  
  # Define namespace
  local namespace="self-healing-pipeline-${ENVIRONMENT}"
  
  # Check if namespace exists
  if ! kubectl get namespace "$namespace" &>/dev/null; then
    log_error "Kubernetes namespace $namespace does not exist"
    return 1
  fi
  
  # Get deployments in the namespace
  local deployments=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
  if [ -z "$deployments" ]; then
    log_warning "No deployments found in namespace $namespace"
    return 0
  fi
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would roll back the following deployments in namespace $namespace:"
    for deployment in $deployments; do
      log_info "  - $deployment"
    done
  else
    # Confirm before rolling back
    if ! confirm_action "Ready to roll back Kubernetes deployments in namespace: $namespace"; then
      log_info "Kubernetes rollback cancelled by user"
      return 0
    fi
    
    # Roll back each deployment
    for deployment in $deployments; do
      log_info "Rolling back deployment: $deployment"
      
      # Get current revision
      local current_revision=$(kubectl rollout history deployment "$deployment" -n "$namespace" | grep -oP '(?<=revision:).*' | tail -1 | tr -d ' ')
      
      # Determine target revision
      local target_revision
      if [ "$K8S_REVISION" = "previous" ]; then
        if [ "$current_revision" -gt 1 ]; then
          target_revision=$((current_revision - 1))
        else
          log_warning "Deployment $deployment is already at revision 1, cannot roll back further"
          continue
        fi
      else
        target_revision="$K8S_REVISION"
      fi
      
      log_info "Rolling back deployment $deployment to revision: $target_revision"
      if ! kubectl rollout undo deployment "$deployment" -n "$namespace" --to-revision="$target_revision"; then
        log_error "Failed to roll back deployment $deployment"
        continue
      fi
      
      # Wait for rollback to complete
      log_info "Waiting for rollback of deployment $deployment to complete..."
      if ! kubectl rollout status deployment "$deployment" -n "$namespace" --timeout=300s; then
        log_error "Rollback of deployment $deployment did not complete within timeout"
        continue
      fi
    done
    
    # Verify rollback
    if ! verify_rollback "kubernetes"; then
      log_error "Kubernetes rollback verification failed"
      return 1
    fi
  fi
  
  log_success "Kubernetes rollback completed successfully"
  return 0
}

# Roll back Helm releases
function rollback_helm() {
  if [ "$ROLLBACK_HELM" = false ]; then
    log_info "Skipping Helm rollback as requested"
    return 0
  fi

  log_info "Rolling back Helm releases..."
  
  # Define namespace
  local namespace="self-healing-pipeline-${ENVIRONMENT}"
  
  # Check if namespace exists
  if ! kubectl get namespace "$namespace" &>/dev/null; then
    log_error "Kubernetes namespace $namespace does not exist"
    return 1
  fi
  
  # Define release name
  local release_name="self-healing-pipeline-${ENVIRONMENT}"
  
  # Check if release exists
  if ! helm status "$release_name" -n "$namespace" &>/dev/null; then
    log_warning "Helm release $release_name does not exist in namespace $namespace"
    return 0
  fi
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would roll back Helm release $release_name in namespace $namespace"
    helm history "$release_name" -n "$namespace"
  else
    # Get release history
    local history=$(helm history "$release_name" -n "$namespace" -o json)
    if [ -z "$history" ]; then
      log_warning "No history found for release $release_name"
      return 1
    fi
    
    # Determine target revision
    local current_revision=$(echo "$history" | jq -r '.[-1].revision')
    local target_revision
    
    if [ "$HELM_REVISION" = "previous" ]; then
      # Find the most recent successful revision that's not the current one
      target_revision=$(echo "$history" | jq -r '.[] | select(.status == "deployed" and .revision != '"$current_revision"') | .revision' | sort -nr | head -1)
      
      if [ -z "$target_revision" ]; then
        log_warning "No suitable previous revision found for release $release_name"
        return 1
      fi
    else
      target_revision="$HELM_REVISION"
    fi
    
    # Confirm before rolling back
    if ! confirm_action "Ready to roll back Helm release $release_name to revision $target_revision"; then
      log_info "Helm rollback cancelled by user"
      return 0
    fi
    
    log_info "Rolling back Helm release $release_name to revision: $target_revision"
    if ! helm rollback "$release_name" "$target_revision" -n "$namespace"; then
      log_error "Failed to roll back Helm release $release_name"
      return 1
    fi
    
    # Wait for rollback to complete
    log_info "Waiting for Helm rollback to complete..."
    sleep 10
    
    # Verify rollback
    if ! verify_rollback "helm"; then
      log_error "Helm rollback verification failed"
      return 1
    fi
  fi
  
  log_success "Helm rollback completed successfully"
  return 0
}

# Verify rollback success
function verify_rollback() {
  local rollback_type=$1
  
  log_info "Verifying $rollback_type rollback..."
  
  case "$rollback_type" in
    terraform)
      # For Terraform, we can check the state
      if [ -d "$TERRAFORM_DIR" ]; then
        pushd "$TERRAFORM_DIR" > /dev/null
        
        # Check if there are any errors in the state
        if ! terraform state list &>/dev/null; then
          log_error "Terraform state is not valid after rollback"
          popd > /dev/null
          return 1
        fi
        
        popd > /dev/null
      else
        log_warning "Terraform directory not found, skipping verification"
      fi
      ;;
      
    kubernetes)
      # For Kubernetes, check if all pods are running
      local namespace="self-healing-pipeline-${ENVIRONMENT}"
      
      # Wait a bit for pods to stabilize
      sleep 30
      
      # Check if all pods are running
      log_info "Checking if all pods are running after Kubernetes rollback..."
      local pod_status=$(kubectl get pods -n "$namespace" -o jsonpath='{.items[*].status.phase}')
      if [[ $pod_status == *"Failed"* ]] || [[ $pod_status == *"Pending"* ]] || [[ $pod_status == *"Unknown"* ]]; then
        log_error "Some pods are not running after Kubernetes rollback:"
        kubectl get pods -n "$namespace"
        return 1
      fi
      
      # Check if services are accessible
      log_info "Checking if services are accessible after Kubernetes rollback..."
      local services=$(kubectl get services -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
      for service in $services; do
        if ! kubectl get service "$service" -n "$namespace" &>/dev/null; then
          log_error "Service $service is not accessible after Kubernetes rollback"
          return 1
        fi
      done
      ;;
      
    helm)
      # For Helm, check release status and pod health
      local namespace="self-healing-pipeline-${ENVIRONMENT}"
      local release_name="self-healing-pipeline-${ENVIRONMENT}"
      
      # Check Helm release status
      log_info "Checking Helm release status after rollback..."
      local status=$(helm status "$release_name" -n "$namespace" -o json | jq -r '.info.status' 2>/dev/null)
      if [ "$status" != "deployed" ]; then
        log_error "Helm release $release_name is in $status state after rollback"
        return 1
      fi
      
      # Check if all pods are running
      log_info "Checking if all pods are running after Helm rollback..."
      local pod_status=$(kubectl get pods -n "$namespace" -o jsonpath='{.items[*].status.phase}')
      if [[ $pod_status == *"Failed"* ]] || [[ $pod_status == *"Pending"* ]] || [[ $pod_status == *"Unknown"* ]]; then
        log_error "Some pods are not running after Helm rollback:"
        kubectl get pods -n "$namespace"
        return 1
      fi
      ;;
      
    *)
      log_error "Unknown rollback type: $rollback_type"
      return 1
      ;;
  esac
  
  log_success "$rollback_type rollback verification passed"
  return 0
}

# Main function
function main() {
  # Parse arguments
  parse_arguments "$@"
  
  # Display rollback information
  log_info "=== Self-Healing Data Pipeline Rollback ==="
  log_info "Project ID:       $PROJECT_ID"
  log_info "Environment:      $ENVIRONMENT"
  log_info "Region:           $REGION"
  log_info "Resource Prefix:  $RESOURCE_PREFIX"
  log_info "Rollback Terraform: $ROLLBACK_TERRAFORM"
  log_info "Rollback K8s:     $ROLLBACK_K8S"
  log_info "Rollback Helm:    $ROLLBACK_HELM"
  log_info "Terraform Backup: $TERRAFORM_STATE_BACKUP"
  log_info "K8s Revision:     $K8S_REVISION"
  log_info "Helm Revision:    $HELM_REVISION"
  log_info "Dry Run:          $DRY_RUN"
  log_info "Skip Confirmation: $SKIP_CONFIRMATION"
  log_info "==================================="
  
  # Check prerequisites
  if ! check_prerequisites; then
    log_error "Prerequisites check failed"
    return 1
  fi
  
  # Confirm rollback
  if ! confirm_action "This will roll back the self-healing data pipeline in environment: $ENVIRONMENT. Are you sure?"; then
    log_info "Rollback cancelled by user"
    return 0
  fi
  
  # Roll back Terraform infrastructure
  if ! rollback_terraform; then
    log_error "Terraform rollback failed"
    return 1
  fi
  
  # Roll back Kubernetes resources
  if ! rollback_kubernetes; then
    log_error "Kubernetes rollback failed"
    return 1
  fi
  
  # Roll back Helm releases
  if ! rollback_helm; then
    log_error "Helm rollback failed"
    return 1
  fi
  
  # Display rollback summary
  log_success "=== Rollback Summary ==="
  log_success "Environment:      $ENVIRONMENT"
  log_success "Project ID:       $PROJECT_ID"
  log_success "Region:           $REGION"
  log_success "Terraform:        $([ "$ROLLBACK_TERRAFORM" = true ] && echo "Rolled back" || echo "Skipped")"
  log_success "Kubernetes:       $([ "$ROLLBACK_K8S" = true ] && echo "Rolled back" || echo "Skipped")"
  log_success "Helm:             $([ "$ROLLBACK_HELM" = true ] && echo "Rolled back" || echo "Skipped")"
  log_success "==================================="
  
  # Display next steps
  log_info "Next steps:"
  log_info "1. Verify the application is functioning correctly after rollback"
  log_info "2. Check the monitoring dashboards in Google Cloud Console"
  log_info "3. Investigate the root cause of the issue that required rollback"
  log_info "4. If needed, fix the issues and redeploy using: ./infrastructure/scripts/deploy.sh --project-id=$PROJECT_ID --environment=$ENVIRONMENT"
  
  return 0
}

# Execute main function
main "$@"
exit $?