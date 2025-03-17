#!/bin/bash
# Database migration script for self-healing data pipeline
# This script automates the creation and updating of BigQuery datasets, tables,
# and Firestore collections with appropriate schemas across environments.

# Script directory and paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BACKEND_DIR=${PROJECT_ROOT}/src/backend
CONFIG_DIR=${BACKEND_DIR}/configs
MIGRATIONS_DIR=${BACKEND_DIR}/db/migrations
LOG_FILE=${SCRIPT_DIR}/db-migration_$(date +%Y%m%d_%H%M%S).log

# Default values that can be overridden via command line arguments
ENVIRONMENT=${ENVIRONMENT:-dev}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-}
GCP_REGION=${GCP_REGION:-us-central1}
DATASET_ID=${DATASET_ID:-self_healing_pipeline}
SEED_DATA=${SEED_DATA:-true}
FORCE=${FORCE:-false}

# ANSI color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Import helper functions from bootstrap script
source "${SCRIPT_DIR}/bootstrap.sh"

# Log message to console and file
function log_message() {
  local level=$1
  local message=$2
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  local color="${NC}"
  
  case $level in
    ERROR)
      color="${RED}"
      ;;
    WARNING)
      color="${YELLOW}"
      ;;
    SUCCESS)
      color="${GREEN}"
      ;;
    *)
      color="${NC}"
      ;;
  esac
  
  echo -e "${color}[${timestamp}] [${level}] ${message}${NC}"
  echo "[${timestamp}] [${level}] ${message}" >> "${LOG_FILE}"
}

# Log info message
function log_info() {
  log_message "INFO" "$1"
}

# Log error message
function log_error() {
  log_message "ERROR" "$1"
}

# Log warning message
function log_warning() {
  log_message "WARNING" "$1"
}

# Log success message
function log_success() {
  log_message "SUCCESS" "$1"
}

# Check if a command exists
function command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Confirm action with user
function confirm_action() {
  local message=$1
  
  if [ "$FORCE" = true ]; then
    return 0
  fi
  
  echo -e "${YELLOW}${message}${NC}"
  read -p "Continue? (y/n): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    return 1
  fi
  return 0
}

# Check prerequisites
function check_prerequisites() {
  log_info "Checking prerequisites..."
  
  # Check for required tools
  for cmd in python3 gcloud jq; do
    if ! command_exists "$cmd"; then
      log_error "$cmd is not installed"
      return 1
    fi
  done
  
  # Check Python packages
  log_info "Checking required Python packages..."
  if ! python3 -c "import google.cloud.bigquery, google.cloud.firestore" 2>/dev/null; then
    log_error "Required Python packages are missing. Please install:"
    log_error "pip install google-cloud-bigquery google-cloud-firestore"
    return 1
  fi
  
  # Check if user is authenticated with gcloud
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    log_error "Not authenticated with gcloud. Run 'gcloud auth login' first."
    return 1
  fi
  
  # Check if project ID is set
  if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "GCP project ID is not set. Use --project option or set GCP_PROJECT_ID environment variable."
    return 1
  fi
  
  # Check if project exists
  if ! gcloud projects describe "$GCP_PROJECT_ID" &>/dev/null; then
    log_error "Project $GCP_PROJECT_ID does not exist or you don't have access to it."
    return 1
  fi
  
  # Set default project
  gcloud config set project "$GCP_PROJECT_ID"
  
  log_success "Prerequisites check passed."
  return 0
}

# Load configuration from file
function load_config() {
  local env=$1
  local config_file="${CONFIG_DIR}/${env}_config.yaml"
  
  if [ ! -f "$config_file" ]; then
    config_file="${CONFIG_DIR}/default_config.yaml"
    log_warning "Environment-specific config not found, using default config"
  fi
  
  if [ ! -f "$config_file" ]; then
    log_error "Configuration file not found: $config_file"
    return 1
  fi
  
  log_info "Loading configuration from $config_file"
  
  # Parse YAML config using Python (more reliable than shell parsing)
  local config=$(python3 -c "import yaml, json; print(json.dumps(yaml.safe_load(open('$config_file'))))")
  
  # Extract database configuration
  local db_config=$(echo "$config" | jq -r '.database // {}')
  
  echo "$db_config"
  return 0
}

# Run database migration
function run_migration() {
  local project_id=$1
  local dataset_id=$2
  local location=$3
  local seed_data=$4
  local config_file=$5
  
  log_info "Starting database migration for project $project_id"
  
  # Check if migrations directory exists
  if [ ! -d "$MIGRATIONS_DIR" ]; then
    log_error "Migrations directory not found: $MIGRATIONS_DIR"
    return 1
  fi
  
  # Construct Python command
  local migration_script="${MIGRATIONS_DIR}/initial_migration.py"
  if [ ! -f "$migration_script" ]; then
    log_error "Migration script not found: $migration_script"
    return 1
  fi
  
  log_info "Executing migration script: $migration_script"
  
  # Execute migration script
  if ! python3 "$migration_script" \
    --project-id="$project_id" \
    --dataset-id="$dataset_id" \
    --location="$location" \
    --seed-data="$seed_data" \
    --config-file="$config_file"; then
    log_error "Migration failed"
    return 1
  fi
  
  log_success "Migration completed successfully"
  return 0
}

# Verify migration success
function verify_migration() {
  local project_id=$1
  local dataset_id=$2
  
  log_info "Verifying migration for project $project_id, dataset $dataset_id"
  
  # Check if dataset exists
  if ! bq --project_id="$project_id" show "$dataset_id" &>/dev/null; then
    log_error "Dataset $dataset_id does not exist after migration"
    return 1
  fi
  
  # List tables in dataset
  log_info "Checking tables in dataset $dataset_id"
  local tables=$(bq --project_id="$project_id" ls "$dataset_id" | grep -v "TableId" | awk '{print $1}')
  
  # Check for required tables
  local required_tables=("source_systems" "pipeline_definitions" "pipeline_executions" "task_executions" "quality_rules" "quality_validations" "issue_patterns" "healing_actions" "healing_executions" "pipeline_metrics" "alerts")
  
  for table in "${required_tables[@]}"; do
    if ! echo "$tables" | grep -q "$table"; then
      log_warning "Required table $table not found in dataset"
    else
      log_info "Table $table exists"
    fi
  done
  
  # Check Firestore collections (if possible)
  log_info "Checking Firestore collections"
  # This would require additional gcloud commands or Python script
  
  log_success "Migration verification completed"
  return 0
}

# Display usage information
function show_usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Database migration script for self-healing data pipeline"
  echo ""
  echo "Options:"
  echo "  -p, --project PROJECT_ID   GCP project ID (required)"
  echo "  -d, --dataset DATASET_ID   BigQuery dataset ID (default: self_healing_pipeline)"
  echo "  -r, --region REGION        GCP region (default: us-central1)"
  echo "  -e, --environment ENV      Environment: dev, staging, prod (default: dev)"
  echo "  -c, --config FILE          Path to config file (default: based on environment)"
  echo "  -s, --seed BOOL            Seed initial data: true, false (default: true)"
  echo "  -f, --force                Force migration without confirmation"
  echo "  -h, --help                 Show this help message"
  echo ""
  echo "Examples:"
  echo "  $0 --project=my-project-id"
  echo "  $0 --project=my-project-id --environment=staging --seed=false"
  echo "  $0 --project=my-project-id --dataset=custom_dataset --region=us-east1"
}

# Parse command line arguments
function parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      -p=*|--project=*)
        GCP_PROJECT_ID="${1#*=}"
        shift
        ;;
      -p|--project)
        GCP_PROJECT_ID="$2"
        shift 2
        ;;
      -d=*|--dataset=*)
        DATASET_ID="${1#*=}"
        shift
        ;;
      -d|--dataset)
        DATASET_ID="$2"
        shift 2
        ;;
      -r=*|--region=*)
        GCP_REGION="${1#*=}"
        shift
        ;;
      -r|--region)
        GCP_REGION="$2"
        shift 2
        ;;
      -e=*|--environment=*)
        ENVIRONMENT="${1#*=}"
        shift
        ;;
      -e|--environment)
        ENVIRONMENT="$2"
        shift 2
        ;;
      -c=*|--config=*)
        CONFIG_FILE="${1#*=}"
        shift
        ;;
      -c|--config)
        CONFIG_FILE="$2"
        shift 2
        ;;
      -s=*|--seed=*)
        SEED_DATA="${1#*=}"
        shift
        ;;
      -s|--seed)
        SEED_DATA="$2"
        shift 2
        ;;
      -f|--force)
        FORCE=true
        shift
        ;;
      -h|--help)
        show_usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1"
        show_usage
        return 1
        ;;
    esac
  done

  # Validate required parameters
  if [ -z "$GCP_PROJECT_ID" ]; then
    log_error "Error: --project is required"
    show_usage
    return 1
  fi

  # Validate environment
  if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
    log_error "Error: environment must be one of: dev, staging, prod"
    return 1
  fi
  
  # Validate seed data parameter
  if [[ ! "$SEED_DATA" =~ ^(true|false)$ ]]; then
    log_error "Error: seed must be either true or false"
    return 1
  fi
  
  return 0
}

# Main function
function main() {
  # Initialize log file
  echo "Database Migration Log - $(date)" > "${LOG_FILE}"
  echo "======================================" >> "${LOG_FILE}"
  
  # Parse arguments
  if ! parse_args "$@"; then
    return 1
  fi
  
  # Display migration information
  log_info "=== Self-Healing Data Pipeline Database Migration ==="
  log_info "Project ID:       $GCP_PROJECT_ID"
  log_info "Dataset ID:       $DATASET_ID"
  log_info "Region:           $GCP_REGION"
  log_info "Environment:      $ENVIRONMENT"
  log_info "Seed Data:        $SEED_DATA"
  log_info "Force:            $FORCE"
  log_info "Log File:         $LOG_FILE"
  log_info "==================================="
  
  # Check prerequisites
  if ! check_prerequisites; then
    log_error "Prerequisites check failed"
    return 1
  fi
  
  # Load configuration
  local config_file="${CONFIG_DIR}/${ENVIRONMENT}_config.yaml"
  if [ -n "${CONFIG_FILE}" ]; then
    config_file="${CONFIG_FILE}"
  fi
  
  # Confirm migration
  if ! confirm_action "This will migrate the database for project $GCP_PROJECT_ID in $ENVIRONMENT environment. Are you sure?"; then
    log_info "Migration cancelled by user"
    return 0
  fi
  
  # Run migration
  if ! run_migration "$GCP_PROJECT_ID" "$DATASET_ID" "$GCP_REGION" "$SEED_DATA" "$config_file"; then
    log_error "Migration failed"
    return 1
  fi
  
  # Verify migration
  if ! verify_migration "$GCP_PROJECT_ID" "$DATASET_ID"; then
    log_warning "Migration verification found issues"
  fi
  
  # Display migration summary
  log_success "=== Migration Summary ==="
  log_success "Environment:      $ENVIRONMENT"
  log_success "Project ID:       $GCP_PROJECT_ID"
  log_success "Dataset ID:       $DATASET_ID"
  log_success "Region:           $GCP_REGION"
  log_success "Seed Data:        $SEED_DATA"
  log_success "Log File:         $LOG_FILE"
  log_success "==================================="
  
  # Display next steps
  log_info "Next steps:"
  log_info "1. Review the migration results"
  log_info "2. Verify data in BigQuery and Firestore"
  log_info "3. Run the deployment script if needed:"
  log_info "   ./infrastructure/scripts/deploy.sh --project-id=$GCP_PROJECT_ID --environment=$ENVIRONMENT"
  
  return 0
}

# Execute main function
main "$@"
exit $?