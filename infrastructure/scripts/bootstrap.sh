#!/bin/bash
# Bootstrap script for self-healing data pipeline
# This script initializes the GCP project and creates foundational resources
# required before Terraform can be applied

# Default values that can be overridden via command line arguments
REGION="us-central1"
SECONDARY_REGION="us-east1"
RESOURCE_PREFIX="shp"
ENVIRONMENT="dev"
CREATE_PROJECT=false
ENABLE_APIS=true
SETUP_SERVICE_ACCOUNTS=true
SETUP_NETWORK=false
DRY_RUN=false

# Required APIs for the self-healing data pipeline
REQUIRED_APIS=(
  "compute.googleapis.com"
  "composer.googleapis.com"
  "bigquery.googleapis.com"
  "storage.googleapis.com"
  "cloudfunctions.googleapis.com"
  "cloudbuild.googleapis.com"
  "aiplatform.googleapis.com"
  "monitoring.googleapis.com"
  "logging.googleapis.com"
  "secretmanager.googleapis.com"
  "cloudkms.googleapis.com"
  "servicenetworking.googleapis.com"
  "vpcaccess.googleapis.com"
  "dns.googleapis.com"
)

# Display help message
function show_help() {
  echo "Usage: $0 --project-id=PROJECT_ID [options]"
  echo ""
  echo "Options:"
  echo "  --project-id PROJECT_ID       GCP project ID (required)"
  echo "  --region REGION               GCP region (default: us-central1)"
  echo "  --secondary-region REGION     Secondary GCP region (default: us-east1)"
  echo "  --resource-prefix PREFIX      Resource name prefix (default: shp)"
  echo "  --environment ENV             Target environment: dev, staging, prod (default: dev)"
  echo "  --billing-account ACCOUNT_ID  Billing account ID (required if creating project)"
  echo "  --create-project              Create a new GCP project"
  echo "  --terraform-state-bucket NAME Bucket name for Terraform state"
  echo "  --skip-apis                   Skip enabling APIs"
  echo "  --skip-service-accounts       Skip service account creation"
  echo "  --setup-network               Create basic network resources"
  echo "  --dry-run                     Perform a dry run without making changes"
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
      --region)
        REGION="$2"
        shift 2
        ;;
      --secondary-region)
        SECONDARY_REGION="$2"
        shift 2
        ;;
      --resource-prefix)
        RESOURCE_PREFIX="$2"
        shift 2
        ;;
      --environment)
        ENVIRONMENT="$2"
        shift 2
        ;;
      --billing-account)
        BILLING_ACCOUNT="$2"
        shift 2
        ;;
      --create-project)
        CREATE_PROJECT=true
        shift
        ;;
      --terraform-state-bucket)
        TERRAFORM_STATE_BUCKET="$2"
        shift 2
        ;;
      --skip-apis)
        ENABLE_APIS=false
        shift
        ;;
      --skip-service-accounts)
        SETUP_SERVICE_ACCOUNTS=false
        shift
        ;;
      --setup-network)
        SETUP_NETWORK=true
        shift
        ;;
      --dry-run)
        DRY_RUN=true
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
  
  # Validate billing account if creating project
  if [ "$CREATE_PROJECT" = true ] && [ -z "$BILLING_ACCOUNT" ]; then
    log_error "Error: --billing-account is required when creating a project"
    exit 1
  fi
  
  # Set default terraform state bucket name if not provided
  if [ -z "$TERRAFORM_STATE_BUCKET" ]; then
    TERRAFORM_STATE_BUCKET="${RESOURCE_PREFIX}-${ENVIRONMENT}-terraform-state"
  fi
}

# Check prerequisites
function check_prerequisites() {
  log_info "Checking prerequisites..."
  
  # Check for required tools
  for cmd in gcloud jq; do
    if ! command_exists "$cmd"; then
      log_error "$cmd is not installed"
      return 1
    fi
  done
  
  # Check if terraform is installed
  if ! command_exists "terraform"; then
    log_warning "terraform is not installed. You will need to install it before running Terraform deployments."
  fi
  
  # Check if user is authenticated with gcloud
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    log_error "Not authenticated with gcloud. Run 'gcloud auth login' first."
    return 1
  fi
  
  # Check if user has necessary permissions
  if [ "$CREATE_PROJECT" = false ]; then
    log_info "Checking permissions for project $PROJECT_ID..."
    if ! gcloud projects describe "$PROJECT_ID" &>/dev/null; then
      log_error "Cannot access project $PROJECT_ID. Check if it exists and you have permission."
      return 1
    fi
    
    # Set default project
    gcloud config set project "$PROJECT_ID"
  else
    # Check if user has permission to create projects
    log_info "Checking permissions to create projects..."
    if ! gcloud projects list --limit=1 &>/dev/null; then
      log_error "You don't have permission to create projects."
      return 1
    fi
    
    # Check if project ID is available
    if gcloud projects describe "$PROJECT_ID" &>/dev/null; then
      log_error "Project ID $PROJECT_ID already exists."
      return 1
    fi
  fi
  
  log_success "Prerequisites check passed."
  return 0
}

# Create a new GCP project
function create_project() {
  if [ "$CREATE_PROJECT" = false ]; then
    log_info "Skipping project creation as requested"
    return 0
  fi

  log_info "Creating new GCP project: $PROJECT_ID"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would create project: $PROJECT_ID"
    log_info "[DRY RUN] Would link to billing account: $BILLING_ACCOUNT"
  else
    # Create the project
    if ! gcloud projects create "$PROJECT_ID" \
      --name="Self-Healing Pipeline - ${ENVIRONMENT}" \
      --labels="environment=${ENVIRONMENT},application=self-healing-pipeline"; then
      log_error "Failed to create project $PROJECT_ID"
      return 1
    fi
    
    # Link to billing account
    log_info "Linking project to billing account: $BILLING_ACCOUNT"
    if ! gcloud billing projects link "$PROJECT_ID" \
      --billing-account="$BILLING_ACCOUNT"; then
      log_error "Failed to link project to billing account"
      return 1
    fi
    
    # Set as default project
    gcloud config set project "$PROJECT_ID"
  fi
  
  log_success "Project creation completed successfully"
  return 0
}

# Enable required APIs
function enable_required_apis() {
  if [ "$ENABLE_APIS" = false ]; then
    log_info "Skipping API enablement as requested"
    return 0
  fi

  log_info "Enabling required APIs for project $PROJECT_ID"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would enable the following APIs:"
    for api in "${REQUIRED_APIS[@]}"; do
      log_info "  - $api"
    done
  else
    # Enable APIs in batches to avoid quota issues
    local batch_size=5
    local total_apis=${#REQUIRED_APIS[@]}
    local batches=$(( (total_apis + batch_size - 1) / batch_size ))
    
    for ((i=0; i<batches; i++)); do
      local start=$((i * batch_size))
      local end=$((start + batch_size))
      if [ $end -gt $total_apis ]; then
        end=$total_apis
      fi
      
      local apis_to_enable=()
      for ((j=start; j<end; j++)); do
        apis_to_enable+=("${REQUIRED_APIS[$j]}")
      done
      
      log_info "Enabling APIs batch $((i+1))/$batches: ${apis_to_enable[*]}"
      if ! gcloud services enable ${apis_to_enable[@]} --project="$PROJECT_ID"; then
        log_error "Failed to enable APIs: ${apis_to_enable[*]}"
        return 1
      fi
    done
    
    # Wait for APIs to be fully enabled
    log_info "Waiting for APIs to be fully enabled..."
    sleep 30
  fi
  
  log_success "API enablement completed successfully"
  return 0
}

# Setup service accounts
function setup_service_accounts() {
  if [ "$SETUP_SERVICE_ACCOUNTS" = false ]; then
    log_info "Skipping service account setup as requested"
    return 0
  fi

  log_info "Setting up service accounts for project $PROJECT_ID"
  
  # Define service account names
  local pipeline_sa="${RESOURCE_PREFIX}-${ENVIRONMENT}-pipeline-sa"
  local composer_sa="${RESOURCE_PREFIX}-${ENVIRONMENT}-composer-sa"
  local bigquery_sa="${RESOURCE_PREFIX}-${ENVIRONMENT}-bigquery-sa"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would create the following service accounts:"
    log_info "  - $pipeline_sa@$PROJECT_ID.iam.gserviceaccount.com"
    log_info "  - $composer_sa@$PROJECT_ID.iam.gserviceaccount.com"
    log_info "  - $bigquery_sa@$PROJECT_ID.iam.gserviceaccount.com"
  else
    # Create pipeline service account
    log_info "Creating pipeline service account: $pipeline_sa"
    if ! gcloud iam service-accounts create "$pipeline_sa" \
      --display-name="Self-Healing Pipeline Service Account" \
      --description="Service account for self-healing data pipeline operations" \
      --project="$PROJECT_ID"; then
      log_error "Failed to create pipeline service account"
      return 1
    fi
    
    # Create Composer service account
    log_info "Creating Composer service account: $composer_sa"
    if ! gcloud iam service-accounts create "$composer_sa" \
      --display-name="Self-Healing Pipeline Composer Service Account" \
      --description="Service account for Cloud Composer environment" \
      --project="$PROJECT_ID"; then
      log_error "Failed to create Composer service account"
      return 1
    fi
    
    # Create BigQuery service account
    log_info "Creating BigQuery service account: $bigquery_sa"
    if ! gcloud iam service-accounts create "$bigquery_sa" \
      --display-name="Self-Healing Pipeline BigQuery Service Account" \
      --description="Service account for BigQuery operations" \
      --project="$PROJECT_ID"; then
      log_error "Failed to create BigQuery service account"
      return 1
    fi
    
    # Assign IAM roles to pipeline service account
    log_info "Assigning roles to pipeline service account"
    local pipeline_roles=(
      "roles/storage.admin"
      "roles/bigquery.dataEditor"
      "roles/bigquery.jobUser"
      "roles/cloudfunctions.developer"
      "roles/monitoring.editor"
    )
    
    for role in "${pipeline_roles[@]}"; do
      if ! gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${pipeline_sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --role="$role"; then
        log_error "Failed to assign role $role to pipeline service account"
        return 1
      fi
    done
    
    # Assign IAM roles to Composer service account
    log_info "Assigning roles to Composer service account"
    local composer_roles=(
      "roles/composer.worker"
      "roles/storage.objectAdmin"
      "roles/bigquery.dataEditor"
      "roles/bigquery.jobUser"
      "roles/aiplatform.user"
    )
    
    for role in "${composer_roles[@]}"; do
      if ! gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${composer_sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --role="$role"; then
        log_error "Failed to assign role $role to Composer service account"
        return 1
      fi
    done
    
    # Assign IAM roles to BigQuery service account
    log_info "Assigning roles to BigQuery service account"
    local bigquery_roles=(
      "roles/bigquery.dataOwner"
      "roles/bigquery.admin"
      "roles/storage.objectAdmin"
    )
    
    for role in "${bigquery_roles[@]}"; do
      if ! gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:${bigquery_sa}@${PROJECT_ID}.iam.gserviceaccount.com" \
        --role="$role"; then
        log_error "Failed to assign role $role to BigQuery service account"
        return 1
      fi
    done
  fi
  
  log_success "Service account setup completed successfully"
  return 0
}

# Create Terraform state bucket
function create_terraform_state_bucket() {
  log_info "Creating Terraform state bucket: $TERRAFORM_STATE_BUCKET"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would create GCS bucket: $TERRAFORM_STATE_BUCKET"
    log_info "[DRY RUN] Would enable versioning on the bucket"
  else
    # Check if bucket already exists
    if gsutil ls -b "gs://$TERRAFORM_STATE_BUCKET" &>/dev/null; then
      log_warning "Terraform state bucket already exists: $TERRAFORM_STATE_BUCKET"
    else
      # Create the bucket
      if ! gsutil mb -l "$REGION" -p "$PROJECT_ID" "gs://$TERRAFORM_STATE_BUCKET"; then
        log_error "Failed to create Terraform state bucket"
        return 1
      fi
      
      # Enable versioning
      if ! gsutil versioning set on "gs://$TERRAFORM_STATE_BUCKET"; then
        log_error "Failed to enable versioning on Terraform state bucket"
        return 1
      fi
      
      # Set lifecycle policy
      local lifecycle_config="{\"rule\": [{\"action\": {\"type\": \"Delete\"}, \"condition\": {\"numNewerVersions\": 10, \"isLive\": false}}]}"
      echo "$lifecycle_config" > /tmp/lifecycle_config.json
      if ! gsutil lifecycle set /tmp/lifecycle_config.json "gs://$TERRAFORM_STATE_BUCKET"; then
        log_warning "Failed to set lifecycle policy on Terraform state bucket"
      fi
      rm /tmp/lifecycle_config.json
    fi
    
    # Create backend.tf file
    log_info "Creating Terraform backend configuration"
    local terraform_dir="src/backend/terraform"
    if [ -d "$terraform_dir" ]; then
      local backend_file="$terraform_dir/backend.tf"
      cat > "$backend_file" << EOF
# Generated by bootstrap.sh - DO NOT EDIT MANUALLY
terraform {
  backend "gcs" {
    bucket = "$TERRAFORM_STATE_BUCKET"
    prefix = "terraform/state/$ENVIRONMENT"
  }
}
EOF
      log_success "Created Terraform backend configuration at $backend_file"
    else
      log_warning "Terraform directory not found at $terraform_dir"
      log_warning "You will need to configure the backend manually"
    fi
  fi
  
  log_success "Terraform state bucket setup completed successfully"
  return 0
}

# Setup basic network resources
function setup_basic_network() {
  if [ "$SETUP_NETWORK" = false ]; then
    log_info "Skipping network setup as requested"
    return 0
  fi

  log_info "Setting up basic network resources for project $PROJECT_ID"
  
  # Define network resources
  local network_name="${RESOURCE_PREFIX}-${ENVIRONMENT}-network"
  local subnet_name="${RESOURCE_PREFIX}-${ENVIRONMENT}-subnet"
  local subnet_cidr="10.0.0.0/20"
  
  if [ "$DRY_RUN" = true ]; then
    log_info "[DRY RUN] Would create the following network resources:"
    log_info "  - VPC Network: $network_name"
    log_info "  - Subnet: $subnet_name ($subnet_cidr)"
    log_info "  - Private Google Access: Enabled"
    log_info "  - Firewall rules for internal communication"
  else
    # Create VPC network
    log_info "Creating VPC network: $network_name"
    if ! gcloud compute networks create "$network_name" \
      --project="$PROJECT_ID" \
      --subnet-mode=custom; then
      log_error "Failed to create VPC network"
      return 1
    fi
    
    # Create subnet
    log_info "Creating subnet: $subnet_name"
    if ! gcloud compute networks subnets create "$subnet_name" \
      --project="$PROJECT_ID" \
      --network="$network_name" \
      --region="$REGION" \
      --range="$subnet_cidr" \
      --enable-private-ip-google-access; then
      log_error "Failed to create subnet"
      return 1
    fi
    
    # Create firewall rule for internal communication
    log_info "Creating firewall rule for internal communication"
    if ! gcloud compute firewall-rules create "${network_name}-allow-internal" \
      --project="$PROJECT_ID" \
      --network="$network_name" \
      --direction=INGRESS \
      --priority=1000 \
      --source-ranges="$subnet_cidr" \
      --action=ALLOW \
      --rules=all; then
      log_error "Failed to create internal firewall rule"
      return 1
    fi
    
    # Create firewall rule for SSH access
    log_info "Creating firewall rule for SSH access"
    if ! gcloud compute firewall-rules create "${network_name}-allow-ssh" \
      --project="$PROJECT_ID" \
      --network="$network_name" \
      --direction=INGRESS \
      --priority=1000 \
      --source-ranges="35.235.240.0/20" \
      --action=ALLOW \
      --rules=tcp:22; then
      log_error "Failed to create SSH firewall rule"
      return 1
    fi
  fi
  
  log_success "Network setup completed successfully"
  return 0
}

# Main function
function main() {
  # Parse arguments
  parse_arguments "$@"
  
  # Display bootstrap information
  log_info "=== Self-Healing Data Pipeline Bootstrap ==="
  log_info "Project ID:       $PROJECT_ID"
  log_info "Environment:      $ENVIRONMENT"
  log_info "Region:           $REGION"
  log_info "Secondary Region: $SECONDARY_REGION"
  log_info "Resource Prefix:  $RESOURCE_PREFIX"
  log_info "Create Project:   $CREATE_PROJECT"
  if [ "$CREATE_PROJECT" = true ]; then
    log_info "Billing Account:  $BILLING_ACCOUNT"
  fi
  log_info "Enable APIs:      $ENABLE_APIS"
  log_info "Setup Service Accounts: $SETUP_SERVICE_ACCOUNTS"
  log_info "Setup Network:    $SETUP_NETWORK"
  log_info "Terraform State Bucket: $TERRAFORM_STATE_BUCKET"
  log_info "Dry Run:          $DRY_RUN"
  log_info "==================================="
  
  # Check prerequisites
  if ! check_prerequisites; then
    log_error "Prerequisites check failed"
    return 1
  fi
  
  # Confirm bootstrap
  if ! confirm_action "Ready to bootstrap project: $PROJECT_ID for environment: $ENVIRONMENT"; then
    log_info "Bootstrap cancelled by user"
    return 0
  fi
  
  # Create project if requested
  if ! create_project; then
    log_error "Project creation failed"
    return 1
  fi
  
  # Enable required APIs
  if ! enable_required_apis; then
    log_error "API enablement failed"
    return 1
  fi
  
  # Setup service accounts
  if ! setup_service_accounts; then
    log_error "Service account setup failed"
    return 1
  fi
  
  # Create Terraform state bucket
  if ! create_terraform_state_bucket; then
    log_error "Terraform state bucket creation failed"
    return 1
  fi
  
  # Setup basic network if requested
  if ! setup_basic_network; then
    log_error "Network setup failed"
    return 1
  fi
  
  # Display bootstrap summary
  log_success "=== Bootstrap Summary ==="
  log_success "Environment:      $ENVIRONMENT"
  log_success "Project ID:       $PROJECT_ID"
  log_success "Region:           $REGION"
  log_success "Project:          $([ "$CREATE_PROJECT" = true ] && echo "Created" || echo "Existing")"
  log_success "APIs:             $([ "$ENABLE_APIS" = true ] && echo "Enabled" || echo "Skipped")"
  log_success "Service Accounts: $([ "$SETUP_SERVICE_ACCOUNTS" = true ] && echo "Created" || echo "Skipped")"
  log_success "Network:          $([ "$SETUP_NETWORK" = true ] && echo "Created" || echo "Skipped")"
  log_success "Terraform State:  $TERRAFORM_STATE_BUCKET"
  log_success "==================================="
  
  # Display next steps
  log_info "Next steps:"
  log_info "1. Review the bootstrap results"
  log_info "2. Run the deployment script to deploy the infrastructure:"
  log_info "   ./infrastructure/scripts/deploy.sh --project-id=$PROJECT_ID --environment=$ENVIRONMENT"
  
  return 0
}

# Execute main function
main "$@"
exit $?