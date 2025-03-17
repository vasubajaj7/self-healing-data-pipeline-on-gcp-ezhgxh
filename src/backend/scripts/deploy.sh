#!/bin/bash
#
# Self-Healing Data Pipeline Deployment Script
#
# This script automates the deployment of infrastructure resources for the
# self-healing data pipeline on Google Cloud Platform using Terraform.
# It handles environment-specific configurations, validates prerequisites, 
# applies Terraform plans, and performs post-deployment verification.
#
# Prerequisites:
#   - Terraform (v1.0+)
#   - Google Cloud SDK (gcloud)
#   - jq
#   - Authenticated gcloud CLI

set -o errexit
set -o pipefail
set -o nounset

# Global variables
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
TERRAFORM_DIR="${PROJECT_ROOT}/src/backend/terraform"
CONFIG_DIR="${PROJECT_ROOT}/src/backend/configs"
LOG_FILE="${SCRIPT_DIR}/deploy_$(date +%Y%m%d_%H%M%S).log"
ENVIRONMENT=${ENVIRONMENT:-dev}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-}
GCP_REGION=${GCP_REGION:-us-central1}
RESOURCE_PREFIX=${RESOURCE_PREFIX:-shp}
AUTO_APPROVE=false
DRY_RUN=false

# Color codes for console output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to log messages to console and file
log_message() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    local log_entry="[${timestamp}] [${level}] ${message}"
    
    # Append to log file
    echo "${log_entry}" >> "${LOG_FILE}"
    
    # Print to console with appropriate color
    case ${level} in
        ERROR)
            echo -e "${RED}${log_entry}${NC}"
            ;;
        WARNING)
            echo -e "${YELLOW}${log_entry}${NC}"
            ;;
        SUCCESS)
            echo -e "${GREEN}${log_entry}${NC}"
            ;;
        *)
            echo -e "${log_entry}"
            ;;
    esac
}

# Function to show script usage
show_usage() {
    cat << EOF
USAGE: $(basename "$0") [OPTIONS]

DESCRIPTION:
    Deploys the self-healing data pipeline infrastructure to Google Cloud Platform
    using Terraform. Supports multiple environments and configuration options.

OPTIONS:
    -e, --environment ENV    Deployment environment: dev, staging, prod (default: dev)
    -p, --project ID         Google Cloud project ID (required)
    -r, --region REGION      Google Cloud region (default: us-central1)
    -x, --prefix PREFIX      Resource name prefix (default: shp)
    -d, --dry-run            Perform a dry run without applying changes
    -y, --auto-approve       Skip confirmation prompts
    -h, --help               Display this help message

ENVIRONMENT VARIABLES:
    GOOGLE_APPLICATION_CREDENTIALS    Path to GCP service account key file
    ENVIRONMENT                        Deployment environment (alternative to -e)
    GCP_PROJECT_ID                     Google Cloud project ID (alternative to -p)
    GCP_REGION                         Google Cloud region (alternative to -r)
    RESOURCE_PREFIX                    Prefix for resource naming (alternative to -x)

EXAMPLES:
    # Deploy to dev environment with project ID
    $(basename "$0") -p my-project-id
    
    # Deploy to production with specific region and auto-approve
    $(basename "$0") -e prod -p my-project-id -r us-west1 -y
    
    # Perform a dry run for staging environment
    $(basename "$0") -e staging -p my-project-id -d
EOF
}

# Function to check prerequisites
check_prerequisites() {
    log_message "Checking prerequisites..." "INFO"
    local status=0
    
    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        log_message "Terraform is not installed. Please install Terraform v1.0+ and try again." "ERROR"
        status=1
    else
        local tf_version=$(terraform version -json | jq -r '.terraform_version')
        log_message "Terraform version: ${tf_version}" "INFO"
    fi
    
    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        log_message "Google Cloud SDK is not installed. Please install gcloud and try again." "ERROR"
        status=1
    else
        local gcloud_version=$(gcloud version | head -n 1)
        log_message "Google Cloud SDK: ${gcloud_version}" "INFO"
    fi
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        log_message "jq is not installed. Please install jq and try again." "ERROR"
        status=1
    else
        local jq_version=$(jq --version)
        log_message "jq version: ${jq_version}" "INFO"
    fi
    
    # Verify GCP authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        log_message "Not authenticated with Google Cloud. Please run 'gcloud auth login' first." "ERROR"
        status=1
    else
        local active_account=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null | head -n 1)
        log_message "Authenticated as: ${active_account}" "INFO"
    fi
    
    # Check if GOOGLE_APPLICATION_CREDENTIALS is set
    if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
        log_message "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set. Service account authentication may be required." "WARNING"
    else
        if [[ ! -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
            log_message "Service account key file not found: ${GOOGLE_APPLICATION_CREDENTIALS}" "ERROR"
            status=1
        else
            log_message "Using service account key: ${GOOGLE_APPLICATION_CREDENTIALS}" "INFO"
        fi
    fi
    
    # Check if project ID is set
    if [[ -z "${GCP_PROJECT_ID}" ]]; then
        log_message "GCP project ID is not set. Use -p or --project to specify a project." "ERROR"
        status=1
    fi
    
    # Verify Terraform directory exists
    if [[ ! -d "${TERRAFORM_DIR}" ]]; then
        log_message "Terraform directory not found: ${TERRAFORM_DIR}" "ERROR"
        status=1
    fi
    
    return ${status}
}

# Function to parse command line arguments
parse_args() {
    local args=("$@")
    local OPTS=$(getopt -o he:p:r:x:dy --long help,environment:,project:,region:,prefix:,dry-run,auto-approve -n "$(basename "$0")" -- "${args[@]}")
    
    if [[ $? -ne 0 ]]; then
        log_message "Failed to parse arguments" "ERROR"
        show_usage
        return 1
    fi
    
    eval set -- "${OPTS}"
    
    while true; do
        case "$1" in
            -h|--help)
                show_usage
                exit 0
                ;;
            -e|--environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -p|--project)
                GCP_PROJECT_ID="$2"
                shift 2
                ;;
            -r|--region)
                GCP_REGION="$2"
                shift 2
                ;;
            -x|--prefix)
                RESOURCE_PREFIX="$2"
                shift 2
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -y|--auto-approve)
                AUTO_APPROVE=true
                shift
                ;;
            --)
                shift
                break
                ;;
            *)
                log_message "Invalid option: $1" "ERROR"
                show_usage
                return 1
                ;;
        esac
    done
    
    # Validate environment
    if [[ ! "${ENVIRONMENT}" =~ ^(dev|staging|prod)$ ]]; then
        log_message "Invalid environment: ${ENVIRONMENT}. Must be one of: dev, staging, prod" "ERROR"
        return 1
    fi
    
    # Validate required parameters
    if [[ -z "${GCP_PROJECT_ID}" ]]; then
        log_message "Google Cloud project ID is required. Use -p or --project option." "ERROR"
        show_usage
        return 1
    fi
    
    return 0
}

# Function to initialize Terraform
initialize_terraform() {
    log_message "Initializing Terraform..." "INFO"
    
    cd "${TERRAFORM_DIR}" || {
        log_message "Failed to change to Terraform directory: ${TERRAFORM_DIR}" "ERROR"
        return 1
    }
    
    local backend_config_file="${CONFIG_DIR}/backend-${ENVIRONMENT}.conf"
    
    # Check if backend config exists
    if [[ -f "${backend_config_file}" ]]; then
        log_message "Using backend configuration: ${backend_config_file}" "INFO"
        terraform init -backend-config="${backend_config_file}"
    else
        log_message "Backend configuration not found. Using default backend configuration." "WARNING"
        terraform init
    fi
    
    local init_status=$?
    if [[ ${init_status} -ne 0 ]]; then
        log_message "Terraform initialization failed" "ERROR"
        return ${init_status}
    fi
    
    log_message "Terraform initialized successfully" "SUCCESS"
    return 0
}

# Function to select or create Terraform workspace
select_terraform_workspace() {
    local env="$1"
    log_message "Selecting Terraform workspace for environment: ${env}" "INFO"
    
    # Check if workspace exists
    if terraform workspace list | grep -q " ${env}$"; then
        log_message "Selecting existing workspace: ${env}" "INFO"
        terraform workspace select "${env}"
    else
        log_message "Creating new workspace: ${env}" "INFO"
        terraform workspace new "${env}"
    fi
    
    local workspace_status=$?
    if [[ ${workspace_status} -ne 0 ]]; then
        log_message "Failed to select or create Terraform workspace: ${env}" "ERROR"
        return ${workspace_status}
    fi
    
    log_message "Using Terraform workspace: ${env}" "SUCCESS"
    return 0
}

# Function to create Terraform plan
create_terraform_plan() {
    local plan_file="${TERRAFORM_DIR}/tfplan_${ENVIRONMENT}_$(date +%Y%m%d_%H%M%S)"
    local vars_file="${CONFIG_DIR}/vars-${ENVIRONMENT}.tfvars"
    
    log_message "Creating Terraform plan for environment: ${ENVIRONMENT}" "INFO"
    
    # Check if variables file exists
    if [[ -f "${vars_file}" ]]; then
        log_message "Using variables file: ${vars_file}" "INFO"
        terraform plan \
            -var="project_id=${GCP_PROJECT_ID}" \
            -var="region=${GCP_REGION}" \
            -var="resource_prefix=${RESOURCE_PREFIX}" \
            -var="environment=${ENVIRONMENT}" \
            -var-file="${vars_file}" \
            -out="${plan_file}"
    else
        log_message "Variables file not found. Using default variables." "WARNING"
        terraform plan \
            -var="project_id=${GCP_PROJECT_ID}" \
            -var="region=${GCP_REGION}" \
            -var="resource_prefix=${RESOURCE_PREFIX}" \
            -var="environment=${ENVIRONMENT}" \
            -out="${plan_file}"
    fi
    
    local plan_status=$?
    if [[ ${plan_status} -ne 0 ]]; then
        log_message "Failed to create Terraform plan" "ERROR"
        return ${plan_status}
    fi
    
    log_message "Terraform plan created successfully: ${plan_file}" "SUCCESS"
    echo "${plan_file}"
    return 0
}

# Function to apply Terraform plan
apply_terraform_plan() {
    local plan_file="$1"
    
    if [[ "${DRY_RUN}" == "true" ]]; then
        log_message "Dry run mode enabled. Skipping Terraform apply." "WARNING"
        return 0
    fi
    
    log_message "Applying Terraform plan: ${plan_file}" "INFO"
    
    # Confirm before applying unless auto-approve is set
    if [[ "${AUTO_APPROVE}" != "true" ]]; then
        read -p "Do you want to apply the Terraform plan? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_message "Terraform apply cancelled by user" "WARNING"
            return 1
        fi
    fi
    
    terraform apply "${plan_file}"
    
    local apply_status=$?
    if [[ ${apply_status} -ne 0 ]]; then
        log_message "Terraform apply failed" "ERROR"
        return ${apply_status}
    fi
    
    log_message "Terraform apply completed successfully" "SUCCESS"
    return 0
}

# Function to capture and save Terraform outputs
capture_terraform_outputs() {
    local output_file="${SCRIPT_DIR}/terraform_output_${ENVIRONMENT}_$(date +%Y%m%d_%H%M%S).json"
    
    log_message "Capturing Terraform outputs..." "INFO"
    
    terraform output -json > "${output_file}"
    
    local output_status=$?
    if [[ ${output_status} -ne 0 ]]; then
        log_message "Failed to capture Terraform outputs" "ERROR"
        return ${output_status}
    fi
    
    log_message "Terraform outputs saved to: ${output_file}" "SUCCESS"
    return 0
}

# Function to verify deployment
verify_deployment() {
    log_message "Verifying deployment..." "INFO"
    
    if [[ "${DRY_RUN}" == "true" ]]; then
        log_message "Dry run mode enabled. Skipping deployment verification." "WARNING"
        return 0
    fi
    
    # Verify BigQuery resources
    log_message "Verifying BigQuery resources..." "INFO"
    if ! bq ls --project_id="${GCP_PROJECT_ID}" &> /dev/null; then
        log_message "Failed to verify BigQuery resources" "ERROR"
        return 1
    fi
    
    # Verify Cloud Composer environment
    log_message "Verifying Cloud Composer environment..." "INFO"
    local composer_env_name="${RESOURCE_PREFIX}-${ENVIRONMENT}-composer"
    if ! gcloud composer environments describe "${composer_env_name}" \
        --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" &> /dev/null; then
        log_message "Cloud Composer environment not found: ${composer_env_name}" "WARNING"
        # Don't fail the verification as Composer might not be deployed in all configurations
    else
        log_message "Cloud Composer environment verified: ${composer_env_name}" "SUCCESS"
    fi
    
    # Verify Cloud Storage buckets
    log_message "Verifying Cloud Storage buckets..." "INFO"
    local data_bucket="${RESOURCE_PREFIX}-${ENVIRONMENT}-data"
    if ! gsutil ls -p "${GCP_PROJECT_ID}" "gs://${data_bucket}" &> /dev/null; then
        log_message "Data bucket not found: ${data_bucket}" "ERROR"
        return 1
    fi
    
    log_message "Deployment verification completed successfully" "SUCCESS"
    return 0
}

# Main function
main() {
    local args=("$@")
    
    # Initialize log file
    touch "${LOG_FILE}"
    log_message "========== Self-Healing Data Pipeline Deployment ==========" "INFO"
    log_message "Starting deployment process at $(date)" "INFO"
    
    # Parse command line arguments
    if ! parse_args "${args[@]}"; then
        return 1
    fi
    
    log_message "Deployment configuration:" "INFO"
    log_message "  Environment: ${ENVIRONMENT}" "INFO"
    log_message "  GCP Project: ${GCP_PROJECT_ID}" "INFO"
    log_message "  GCP Region:  ${GCP_REGION}" "INFO"
    log_message "  Resource Prefix: ${RESOURCE_PREFIX}" "INFO"
    log_message "  Dry Run: ${DRY_RUN}" "INFO"
    log_message "  Auto Approve: ${AUTO_APPROVE}" "INFO"
    
    # Check prerequisites
    if ! check_prerequisites; then
        log_message "Prerequisites check failed. Please fix the issues and try again." "ERROR"
        return 1
    fi
    
    # Initialize Terraform
    if ! initialize_terraform; then
        log_message "Failed to initialize Terraform" "ERROR"
        return 1
    fi
    
    # Select Terraform workspace
    if ! select_terraform_workspace "${ENVIRONMENT}"; then
        log_message "Failed to select Terraform workspace" "ERROR"
        return 1
    fi
    
    # Create Terraform plan
    local plan_output
    plan_output=$(create_terraform_plan)
    if [[ $? -ne 0 ]]; then
        log_message "Failed to create Terraform plan" "ERROR"
        return 1
    fi
    
    # Apply Terraform plan
    if ! apply_terraform_plan "${plan_output}"; then
        log_message "Failed to apply Terraform plan" "ERROR"
        return 1
    fi
    
    # Capture Terraform outputs
    if ! capture_terraform_outputs; then
        log_message "Failed to capture Terraform outputs" "ERROR"
        return 1
    fi
    
    # Verify deployment
    if ! verify_deployment; then
        log_message "Deployment verification failed" "ERROR"
        return 1
    fi
    
    log_message "Deployment completed successfully" "SUCCESS"
    log_message "Deployment log saved to: ${LOG_FILE}" "INFO"
    log_message "==========================================================" "INFO"
    
    return 0
}

# Execute main function with all arguments
main "$@"
exit $?