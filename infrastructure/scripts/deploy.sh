#!/bin/bash
# Deployment script for self-healing data pipeline
# This script automates the deployment of infrastructure and application components
# to Google Cloud Platform across different environments (dev, staging, prod)

# Import helper functions and environment variables from bootstrap script
source "$(dirname "$0")/bootstrap.sh"

# Default values that can be overridden via command line arguments
ENVIRONMENT="dev"
REGION="us-central1"
RESOURCE_PREFIX="shp"
DEPLOY_TERRAFORM=true
DEPLOY_K8S=true
DEPLOY_HELM=true
DEPLOY_STRATEGY="rolling"
K8S_DIR="infrastructure/k8s"
HELM_DIR="infrastructure/helm"
TERRAFORM_DIR="src/backend/terraform"
VERSION="latest"
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
  echo "  --skip-terraform              Skip Terraform deployment"
  echo "  --skip-k8s                    Skip Kubernetes deployment"
  echo "  --skip-helm                   Skip Helm deployment"
  echo "  --strategy STRATEGY           Deployment strategy: rolling, blue-green, canary (default: rolling)"
  echo "  --k8s-dir DIR                 Kubernetes directory (default: infrastructure/k8s)"
  echo "  --helm-dir DIR                Helm directory (default: infrastructure/helm)"
  echo "  --terraform-dir DIR           Terraform directory (default: src/backend/terraform)"
  echo "  --version, -v VERSION         Version to deploy (default: latest)"
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
        DEPLOY_TERRAFORM=false
        shift
        ;;
      --skip-k8s)
        DEPLOY_K8S=false
        shift
        ;;
      --skip-helm)
        DEPLOY_HELM=false
        shift
        ;;
      --strategy)
        DEPLOY_STRATEGY="$2"
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
      --terraform-dir)
        TERRAFORM_DIR="$2"
        shift 2
        ;;
      --version|-v)
        VERSION="$2"
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
  
  # Validate deployment strategy
  if [[ ! "$DEPLOY_STRATEGY" =~ ^(rolling|blue-green|canary)$ ]]; then
    log_error "Error: deployment strategy must be one of: rolling, blue-green, canary"
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
  
  if [ "$DEPLOY_TERRAFORM" = true ] && ! command_exists "terraform"; then
    log_error "terraform is required for Terraform deployment but not installed"
    return 1
  fi
  
  if [ "$DEPLOY_K8S" = true ] && ! command_exists "kubectl"; then
    log_error "kubectl is required for Kubernetes deployment but not installed"
    return 1
  fi
  
  if [ "$DEPLOY_HELM" = true ] && ! command_exists "helm"; then
    log_error "helm is required for Helm deployment but not installed"
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
  
  # Check if GKE cluster exists if deploying Kubernetes or Helm
  if [ "$DEPLOY_K8S" = true ] || [ "$DEPLOY_HELM" = true ]; then
    local cluster_name="${RESOURCE_PREFIX}-${ENVIRONMENT}-cluster"
    log_info "Checking if GKE cluster $cluster_name exists..."
    if ! gcloud container clusters describe "$cluster_name" --region="$REGION" &>/dev/null; then
      log_warning "GKE cluster $cluster_name does not exist in region $REGION"
      log_warning "You may need to deploy Terraform first to create the cluster"
      
      if [ "$DEPLOY_TERRAFORM" = false ]; then
        log_error "Terraform deployment is disabled but GKE cluster does not exist"
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

# Deploy infrastructure using Terraform
function deploy_terraform() {
  if [ "$DEPLOY_TERRAFORM" = false ]; then
    log_info "Skipping Terraform deployment as requested"
    return 0
  fi

  log_info "Deploying infrastructure using Terraform..."
  
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
  
  # Select or create workspace based on environment
  log_info "Selecting Terraform workspace for environment: $ENVIRONMENT"
  if ! terraform workspace select "$ENVIRONMENT" 2>/dev/null; then
    log_info "Creating new Terraform workspace: $ENVIRONMENT"
    if ! terraform workspace new "$ENVIRONMENT"; then
      log_error "Failed to create Terraform workspace: $ENVIRONMENT"
      popd > /dev/null
      return 1
    fi
  fi
  
  # Generate Terraform plan
  log_info "Generating Terraform plan..."
  local var_file="env/${ENVIRONMENT}.tfvars"
  if [ ! -f "$var_file" ]; then
    log_error "Terraform variable file $var_file does not exist"
    popd > /dev/null
    return 1
  fi
  
  if ! terraform plan \
    -var="project_id=$PROJECT_ID" \
    -var="region=$REGION" \
    -var="environment=$ENVIRONMENT" \
    -var="resource_prefix=$RESOURCE_PREFIX" \
    -var-file="$var_file" \
    -out="terraform.plan"; then
    log_error "Failed to generate Terraform plan"
    popd > /dev/null
    return 1
  fi
  
  # Apply Terraform plan if not in dry run mode
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would apply Terraform plan"
  else
    # Confirm before applying
    if ! confirm_action "Ready to apply Terraform plan for environment: $ENVIRONMENT"; then
      log_info "Terraform deployment cancelled by user"
      popd > /dev/null
      return 0
    fi
    
    log_info "Applying Terraform plan..."
    if ! terraform apply "terraform.plan"; then
      log_error "Failed to apply Terraform plan"
      popd > /dev/null
      return 1
    fi
    
    # Create backup of Terraform state for potential rollback
    log_info "Creating backup of Terraform state..."
    terraform state pull > "../../../terraform-state-backup-${ENVIRONMENT}.tfstate"
  fi
  
  # Return to original directory
  popd > /dev/null
  
  log_success "Terraform deployment completed successfully"
  return 0
}

# Deploy Kubernetes resources
function deploy_kubernetes() {
  if [ "$DEPLOY_K8S" = false ]; then
    log_info "Skipping Kubernetes deployment as requested"
    return 0
  fi

  log_info "Deploying Kubernetes resources..."
  
  # Check if Kubernetes directory exists
  if [ ! -d "$K8S_DIR" ]; then
    log_error "Kubernetes directory $K8S_DIR does not exist"
    return 1
  fi
  
  # Determine the appropriate kustomize overlay
  local overlay_dir="$K8S_DIR/overlays/$ENVIRONMENT"
  if [ ! -d "$overlay_dir" ]; then
    log_error "Kubernetes overlay directory $overlay_dir does not exist"
    return 1
  fi
  
  # Generate Kubernetes manifests using kustomize
  log_info "Generating Kubernetes manifests using kustomize..."
  local manifests=$(kubectl kustomize "$overlay_dir")
  if [ -z "$manifests" ]; then
    log_error "Failed to generate Kubernetes manifests"
    return 1
  fi
  
  # Create namespace if it doesn't exist
  local namespace="self-healing-pipeline-${ENVIRONMENT}"
  if ! kubectl get namespace "$namespace" &>/dev/null; then
    log_info "Creating namespace: $namespace"
    if [ "$DRY_RUN" = false ]; then
      if ! kubectl create namespace "$namespace"; then
        log_error "Failed to create namespace: $namespace"
        return 1
      fi
    else
      log_info "[DRY RUN] Would create namespace: $namespace"
    fi
  fi
  
  # Apply Kubernetes manifests if not in dry run mode
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would apply Kubernetes manifests:"
    echo "$manifests"
  else
    # Confirm before applying
    if ! confirm_action "Ready to apply Kubernetes manifests for environment: $ENVIRONMENT"; then
      log_info "Kubernetes deployment cancelled by user"
      return 0
    fi
    
    # Apply manifests based on deployment strategy
    log_info "Applying Kubernetes manifests with $DEPLOY_STRATEGY strategy..."
    echo "$manifests" | kubectl apply -f -
    
    # Implement deployment strategy
    if ! implement_deployment_strategy "$DEPLOY_STRATEGY" "kubernetes"; then
      log_error "Failed to implement $DEPLOY_STRATEGY deployment strategy"
      return 1
    fi
    
    # Verify deployment
    if ! verify_deployment "kubernetes"; then
      log_error "Kubernetes deployment verification failed"
      if ! handle_rollback "kubernetes"; then
        log_error "Kubernetes rollback failed"
      fi
      return 1
    fi
  fi
  
  log_success "Kubernetes deployment completed successfully"
  return 0
}

# Deploy applications using Helm
function deploy_helm() {
  if [ "$DEPLOY_HELM" = false ]; then
    log_info "Skipping Helm deployment as requested"
    return 0
  fi

  log_info "Deploying applications using Helm..."
  
  # Check if Helm directory exists
  if [ ! -d "$HELM_DIR" ]; then
    log_error "Helm directory $HELM_DIR does not exist"
    return 1
  fi
  
  # Determine the chart directory
  local chart_dir="$HELM_DIR/self-healing-pipeline"
  if [ ! -d "$chart_dir" ]; then
    log_error "Helm chart directory $chart_dir does not exist"
    return 1
  fi
  
  # Determine the values file
  local values_file="$chart_dir/values-${ENVIRONMENT}.yaml"
  if [ ! -f "$values_file" ]; then
    log_warning "Environment-specific values file $values_file does not exist"
    log_warning "Using default values.yaml instead"
    values_file="$chart_dir/values.yaml"
  fi
  
  # Create namespace if it doesn't exist
  local namespace="self-healing-pipeline-${ENVIRONMENT}"
  if ! kubectl get namespace "$namespace" &>/dev/null; then
    log_info "Creating namespace: $namespace"
    if [ "$DRY_RUN" = false ]; then
      if ! kubectl create namespace "$namespace"; then
        log_error "Failed to create namespace: $namespace"
        return 1
      fi
    else
      log_info "[DRY RUN] Would create namespace: $namespace"
    fi
  fi
  
  # Generate Helm template
  log_info "Generating Helm template..."
  local release_name="self-healing-pipeline-${ENVIRONMENT}"
  
  # Apply Helm chart if not in dry run mode
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would deploy Helm chart:"
    helm template "$release_name" "$chart_dir" \
      --namespace="$namespace" \
      --values="$values_file" \
      --set global.environment="$ENVIRONMENT" \
      --set global.project.id="$PROJECT_ID" \
      --set global.project.region="$REGION" \
      --set global.image.tag="$VERSION"
  else
    # Confirm before applying
    if ! confirm_action "Ready to deploy Helm chart for environment: $ENVIRONMENT"; then
      log_info "Helm deployment cancelled by user"
      return 0
    fi
    
    # Check if release already exists
    local upgrade_flag="--install"
    if helm status "$release_name" -n "$namespace" &>/dev/null; then
      log_info "Helm release $release_name already exists, upgrading..."
      upgrade_flag="--upgrade"
    else
      log_info "Installing new Helm release: $release_name"
    fi
    
    # Apply Helm chart based on deployment strategy
    local strategy_flags=""
    case "$DEPLOY_STRATEGY" in
      rolling)
        strategy_flags="--atomic --timeout 10m"
        ;;
      blue-green)
        strategy_flags="--atomic --timeout 10m"
        # Additional blue-green setup would be implemented here
        ;;
      canary)
        strategy_flags="--atomic --timeout 10m"
        # Additional canary setup would be implemented here
        ;;
    esac
    
    log_info "Deploying Helm chart with $DEPLOY_STRATEGY strategy..."
    if ! helm $upgrade_flag "$release_name" "$chart_dir" \
      --namespace="$namespace" \
      --values="$values_file" \
      --set global.environment="$ENVIRONMENT" \
      --set global.project.id="$PROJECT_ID" \
      --set global.project.region="$REGION" \
      --set global.image.tag="$VERSION" \
      $strategy_flags; then
      log_error "Failed to deploy Helm chart"
      return 1
    fi
    
    # Implement deployment strategy
    if ! implement_deployment_strategy "$DEPLOY_STRATEGY" "helm"; then
      log_error "Failed to implement $DEPLOY_STRATEGY deployment strategy"
      return 1
    fi
    
    # Verify deployment
    if ! verify_deployment "helm"; then
      log_error "Helm deployment verification failed"
      if ! handle_rollback "helm"; then
        log_error "Helm rollback failed"
      fi
      return 1
    fi
  fi
  
  log_success "Helm deployment completed successfully"
  return 0
}

# Implement deployment strategy
function implement_deployment_strategy() {
  local strategy=$1
  local deployment_type=$2
  
  log_info "Implementing $strategy deployment strategy for $deployment_type..."
  
  case "$strategy" in
    rolling)
      # Rolling updates are the default for Kubernetes and Helm
      # Just need to wait for the rollout to complete
      if [ "$deployment_type" = "kubernetes" ]; then
        local namespace="self-healing-pipeline-${ENVIRONMENT}"
        local deployments=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
        
        for deployment in $deployments; do
          log_info "Waiting for rollout of deployment $deployment to complete..."
          if ! kubectl rollout status deployment "$deployment" -n "$namespace" --timeout=300s; then
            log_error "Rollout of deployment $deployment did not complete within timeout"
            return 1
          fi
        done
      elif [ "$deployment_type" = "helm" ]; then
        # Helm with --atomic flag already waits for resources to be ready
        log_info "Helm release deployment in progress..."
        sleep 10  # Give Helm a moment to start the deployment
      fi
      ;;
      
    blue-green)
      if [ "$deployment_type" = "kubernetes" ]; then
        # Blue-green for Kubernetes would involve:
        # 1. Deploy new version with different labels/selectors
        # 2. Verify new version is healthy
        # 3. Switch service selector to new version
        # 4. Remove old version after transition period
        log_info "Blue-green deployment for Kubernetes is not fully implemented"
        log_info "Using rolling update with additional verification"
        
        # Wait for deployments to be ready
        local namespace="self-healing-pipeline-${ENVIRONMENT}"
        local deployments=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
        
        for deployment in $deployments; do
          log_info "Waiting for rollout of deployment $deployment to complete..."
          if ! kubectl rollout status deployment "$deployment" -n "$namespace" --timeout=300s; then
            log_error "Rollout of deployment $deployment did not complete within timeout"
            return 1
          fi
        done
      elif [ "$deployment_type" = "helm" ]; then
        # Blue-green for Helm would involve similar steps
        log_info "Blue-green deployment for Helm is not fully implemented"
        log_info "Using atomic deployment with additional verification"
      fi
      ;;
      
    canary)
      if [ "$deployment_type" = "kubernetes" ]; then
        # Canary for Kubernetes would involve:
        # 1. Deploy small percentage of new version
        # 2. Monitor for issues
        # 3. Gradually increase percentage
        # 4. Complete rollout when confident
        log_info "Canary deployment for Kubernetes is not fully implemented"
        log_info "Using rolling update with additional verification"
        
        # Wait for deployments to be ready
        local namespace="self-healing-pipeline-${ENVIRONMENT}"
        local deployments=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
        
        for deployment in $deployments; do
          log_info "Waiting for rollout of deployment $deployment to complete..."
          if ! kubectl rollout status deployment "$deployment" -n "$namespace" --timeout=300s; then
            log_error "Rollout of deployment $deployment did not complete within timeout"
            return 1
          fi
        done
      elif [ "$deployment_type" = "helm" ]; then
        # Canary for Helm would involve similar steps
        log_info "Canary deployment for Helm is not fully implemented"
        log_info "Using atomic deployment with additional verification"
      fi
      ;;
      
    *)
      log_error "Unknown deployment strategy: $strategy"
      return 1
      ;;
  esac
  
  log_success "$strategy deployment strategy implemented successfully"
  return 0
}

# Verify deployment success
function verify_deployment() {
  local deployment_type=$1
  
  log_info "Verifying $deployment_type deployment..."
  
  case "$deployment_type" in
    terraform)
      # For Terraform, we can check the state
      if [ -d "$TERRAFORM_DIR" ]; then
        pushd "$TERRAFORM_DIR" > /dev/null
        
        # Check if there are any errors in the state
        if ! terraform state list &>/dev/null; then
          log_error "Terraform state is not valid"
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
      sleep 10
      
      # Check if all pods are running
      log_info "Checking if all pods are running..."
      local pod_status=$(kubectl get pods -n "$namespace" -o jsonpath='{.items[*].status.phase}')
      if [[ $pod_status == *"Failed"* ]] || [[ $pod_status == *"Pending"* ]] || [[ $pod_status == *"Unknown"* ]]; then
        log_error "Some pods are not running after deployment:"
        kubectl get pods -n "$namespace"
        return 1
      fi
      
      # Check if services are accessible
      log_info "Checking if services are accessible..."
      local services=$(kubectl get services -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
      for service in $services; do
        if ! kubectl get service "$service" -n "$namespace" &>/dev/null; then
          log_error "Service $service is not accessible after deployment"
          return 1
        fi
      done
      ;;
      
    helm)
      # For Helm, check release status and pod health
      local namespace="self-healing-pipeline-${ENVIRONMENT}"
      local release_name="self-healing-pipeline-${ENVIRONMENT}"
      
      # Check Helm release status
      log_info "Checking Helm release status..."
      local status=$(helm status "$release_name" -n "$namespace" -o json | jq -r '.info.status' 2>/dev/null)
      if [ "$status" != "deployed" ]; then
        log_error "Helm release $release_name is in $status state after deployment"
        return 1
      fi
      
      # Check if all pods are running
      log_info "Checking if all pods are running..."
      local pod_status=$(kubectl get pods -n "$namespace" -o jsonpath='{.items[*].status.phase}')
      if [[ $pod_status == *"Failed"* ]] || [[ $pod_status == *"Pending"* ]] || [[ $pod_status == *"Unknown"* ]]; then
        log_error "Some pods are not running after Helm deployment:"
        kubectl get pods -n "$namespace"
        return 1
      fi
      ;;
      
    *)
      log_error "Unknown deployment type: $deployment_type"
      return 1
      ;;
  esac
  
  log_success "$deployment_type deployment verification passed"
  return 0
}

# Handle rollback in case of deployment failure
function handle_rollback() {
  local deployment_type=$1
  
  log_warning "Initiating rollback for failed $deployment_type deployment..."
  
  case "$deployment_type" in
    terraform)
      # For Terraform, we can restore from backup state
      local backup_state="terraform-state-backup-${ENVIRONMENT}.tfstate"
      if [ -f "$backup_state" ]; then
        log_info "Found backup state file: $backup_state"
        
        if [ -d "$TERRAFORM_DIR" ]; then
          pushd "$TERRAFORM_DIR" > /dev/null
          
          # Create a backup of current state
          terraform state pull > "../../../terraform-state-current-${ENVIRONMENT}.tfstate"
          
          # Apply the backup state
          log_info "Applying backup state..."
          terraform state push "../../../$backup_state"
          
          popd > /dev/null
        else
          log_error "Terraform directory not found, cannot rollback"
          return 1
        fi
      else
        log_warning "No backup state file found at $backup_state"
        log_warning "Manual Terraform rollback may be required"
        return 1
      fi
      ;;
      
    kubernetes)
      # For Kubernetes, roll back deployments
      local namespace="self-healing-pipeline-${ENVIRONMENT}"
      local deployments=$(kubectl get deployments -n "$namespace" -o jsonpath='{.items[*].metadata.name}')
      
      for deployment in $deployments; do
        log_info "Rolling back deployment: $deployment"
        
        # Get current revision
        local current_revision=$(kubectl rollout history deployment "$deployment" -n "$namespace" | grep -oP '(?<=revision:).*' | tail -1 | tr -d ' ')
        
        # Use previous revision if available
        if [ "$current_revision" -gt 1 ]; then
          local target_revision=$((current_revision - 1))
          
          log_info "Rolling back to revision: $target_revision"
          if ! kubectl rollout undo deployment "$deployment" -n "$namespace" --to-revision="$target_revision"; then
            log_error "Failed to roll back deployment $deployment"
            continue
          fi
          
          # Wait for rollback to complete
          log_info "Waiting for rollback to complete..."
          if ! kubectl rollout status deployment "$deployment" -n "$namespace" --timeout=300s; then
            log_error "Rollback of deployment $deployment did not complete within timeout"
            continue
          fi
        else
          log_warning "Deployment $deployment is already at revision 1, cannot roll back further"
          continue
        fi
      done
      ;;
      
    helm)
      # For Helm, roll back release
      local namespace="self-healing-pipeline-${ENVIRONMENT}"
      local release_name="self-healing-pipeline-${ENVIRONMENT}"
      
      # Check if release exists
      if helm status "$release_name" -n "$namespace" &>/dev/null; then
        # Get release history
        local history=$(helm history "$release_name" -n "$namespace" -o json)
        if [ -z "$history" ]; then
          log_warning "No history found for release $release_name"
          return 1
        fi
        
        # Determine target revision
        local current_revision=$(echo "$history" | jq -r '.[-1].revision')
        
        # Find the most recent successful revision that's not the current one
        local target_revision=$(echo "$history" | jq -r '.[] | select(.status == "deployed" and .revision != '"$current_revision"') | .revision' | sort -nr | head -1)
        
        if [ -z "$target_revision" ]; then
          log_warning "No suitable previous revision found for release $release_name"
          return 1
        fi
        
        log_info "Rolling back to revision: $target_revision"
        if ! helm rollback "$release_name" "$target_revision" -n "$namespace"; then
          log_error "Failed to roll back Helm release $release_name"
          return 1
        fi
        
        # Verify rollback status
        local status=$(helm status "$release_name" -n "$namespace" -o json | jq -r '.info.status')
        if [ "$status" != "deployed" ]; then
          log_error "Helm release $release_name is in $status state after rollback"
          return 1
        fi
      else
        log_warning "Helm release $release_name does not exist, nothing to roll back"
        return 1
      fi
      ;;
      
    *)
      log_error "Unknown deployment type: $deployment_type"
      return 1
      ;;
  esac
  
  log_success "$deployment_type rollback completed successfully"
  return 0
}

# Main function
function main() {
  # Parse arguments
  parse_arguments "$@"
  
  # Display deployment information
  log_info "=== Self-Healing Data Pipeline Deployment ==="
  log_info "Project ID:       $PROJECT_ID"
  log_info "Environment:      $ENVIRONMENT"
  log_info "Region:           $REGION"
  log_info "Resource Prefix:  $RESOURCE_PREFIX"
  log_info "Deploy Terraform: $DEPLOY_TERRAFORM"
  log_info "Deploy K8s:       $DEPLOY_K8S"
  log_info "Deploy Helm:      $DEPLOY_HELM"
  log_info "Deploy Strategy:  $DEPLOY_STRATEGY"
  log_info "Version:          $VERSION"
  log_info "Dry Run:          $DRY_RUN"
  log_info "Skip Confirmation: $SKIP_CONFIRMATION"
  log_info "==================================="
  
  # Check prerequisites
  if ! check_prerequisites; then
    log_error "Prerequisites check failed"
    return 1
  fi
  
  # Confirm deployment
  if ! confirm_action "This will deploy the self-healing data pipeline to environment: $ENVIRONMENT. Are you sure?"; then
    log_info "Deployment cancelled by user"
    return 0
  fi
  
  # Deploy Terraform infrastructure
  if ! deploy_terraform; then
    log_error "Terraform deployment failed"
    return 1
  fi
  
  # Deploy Kubernetes resources
  if ! deploy_kubernetes; then
    log_error "Kubernetes deployment failed"
    return 1
  fi
  
  # Deploy Helm charts
  if ! deploy_helm; then
    log_error "Helm deployment failed"
    return 1
  fi
  
  # Display deployment summary
  log_success "=== Deployment Summary ==="
  log_success "Environment:      $ENVIRONMENT"
  log_success "Project ID:       $PROJECT_ID"
  log_success "Region:           $REGION"
  log_success "Terraform:        $([ "$DEPLOY_TERRAFORM" = true ] && echo "Deployed" || echo "Skipped")"
  log_success "Kubernetes:       $([ "$DEPLOY_K8S" = true ] && echo "Deployed" || echo "Skipped")"
  log_success "Helm:             $([ "$DEPLOY_HELM" = true ] && echo "Deployed" || echo "Skipped")"
  log_success "Version:          $VERSION"
  log_success "==================================="
  
  # Display next steps
  log_info "Next steps:"
  log_info "1. Access the application at: https://pipeline-${ENVIRONMENT}.${PROJECT_ID}.example.com"
  log_info "2. Check the monitoring dashboards in Google Cloud Console"
  log_info "3. Verify data pipeline functionality"
  log_info "4. If issues occur, use the rollback script: ./infrastructure/scripts/rollback.sh --project-id=$PROJECT_ID --environment=$ENVIRONMENT"
  
  return 0
}

# Execute main function
main "$@"
exit $?