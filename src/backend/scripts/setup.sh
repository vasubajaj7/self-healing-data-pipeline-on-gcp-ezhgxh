#!/bin/bash
#
# Self-Healing Data Pipeline Setup Script
#
# This script automates the setup of the self-healing data pipeline backend.
# It handles Python dependencies, infrastructure deployment, database migration,
# secret management, monitoring configuration, and Cloud Composer initialization.
#
# Usage: ./setup.sh [options]
#
# Options:
#   -e, --environment ENV       Deployment environment (dev, staging, prod)
#   -p, --project PROJECT_ID    Google Cloud project ID
#   -r, --region REGION         Google Cloud region
#   -x, --prefix PREFIX         Resource name prefix
#   --skip-infra                Skip infrastructure deployment
#   --skip-secrets                Skip secrets setup
#   --skip-db                   Skip database migration
#   --skip-composer             Skip Composer initialization
#   --skip-monitoring           Skip monitoring setup
#   --skip-deps                 Skip dependencies installation
#   -d, --dev                   Setup development environment
#   -h, --help                  Display help information

# Exit immediately if a command exits with a non-zero status
set -o errexit
set -o pipefail
set -o nounset

# Define script directory and related paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BACKEND_DIR="${PROJECT_ROOT}/src/backend"
CONFIG_DIR="${BACKEND_DIR}/configs"
LOG_FILE="${SCRIPT_DIR}/setup_$(date +%Y%m%d_%H%M%S).log"

# Get environment variables or set defaults
ENVIRONMENT=${ENVIRONMENT:-development}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-self-healing-pipeline-dev}
GCP_REGION=${GCP_REGION:-us-central1}
RESOURCE_PREFIX=${RESOURCE_PREFIX:-shp}
VENV_DIR="${BACKEND_DIR}/.venv"

# Flags to control which steps to skip
SKIP_INFRA=false
SKIP_SECRETS=false
SKIP_DB=false
SKIP_COMPOSER=false
SKIP_MONITORING=false
SKIP_DEPS=false
DEV_MODE=false

# Color codes for prettier output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Function to log a message to both console and log file
log_message() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local log_entry="[${timestamp}] [${level}] ${message}"

    # Append to log file
    echo "${log_entry}" >> "${LOG_FILE}"

    # Print to console with colors
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
            echo "${log_entry}"
            ;;
    esac
}

# Function to check if all required tools and configurations are available
check_prerequisites() {
    log_message "Checking prerequisites..." "INFO"
    local status=0

    # Check if python3 is installed
    if ! command -v python3 &> /dev/null; then
        log_message "python3 is not installed. Please install it and try again." "ERROR"
        status=1
    fi

    # Check if pip is installed
    if ! command -v pip &> /dev/null; then
        log_message "pip is not installed. Please install it and try again." "ERROR"
        status=1
    fi

    # Check if virtualenv is installed
    if ! command -v virtualenv &> /dev/null; then
        log_message "virtualenv is not installed. Please install it and try again." "ERROR"
        status=1
    fi

    # Check if gcloud CLI is installed
    if ! command -v gcloud &> /dev/null; then
        log_message "gcloud CLI is not installed. Please install it and try again." "ERROR"
        status=1
    fi

    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        log_message "Terraform is not installed. Please install Terraform v1.0+ and try again." "ERROR"
        status=1
    fi

    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        log_message "jq is not installed. Please install jq and try again." "ERROR"
        status=1
    fi

    # Check if yq is installed
    if ! command -v yq &> /dev/null; then
        log_message "yq is not installed. Please install yq and try again." "ERROR"
        status=1
    fi

    # Verify GCP authentication status
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

    return ${status}
}

# Function to setup Python virtual environment
setup_virtual_environment() {
    log_message "Setting up virtual environment..." "INFO"

    # Check if virtual environment already exists
    if [ -d "${VENV_DIR}" ]; then
        log_message "Virtual environment already exists: ${VENV_DIR}" "INFO"
    else
        # Create a new virtual environment
        virtualenv "${VENV_DIR}"
        if [ $? -ne 0 ]; then
            log_message "Failed to create virtual environment." "ERROR"
            return 1
        fi
        log_message "Created virtual environment: ${VENV_DIR}" "SUCCESS"
    fi

    # Activate the virtual environment
    source "${VENV_DIR}/bin/activate"

    # Upgrade pip to the latest version
    pip install --upgrade pip
    if [ $? -ne 0 ]; then
        log_message "Failed to upgrade pip." "ERROR"
        return 1
    fi
    log_message "Upgraded pip to the latest version." "SUCCESS"

    return 0
}

# Function to install Python dependencies
install_dependencies() {
    log_message "Installing Python dependencies..." "INFO"

    # Activate the virtual environment
    source "${VENV_DIR}/bin/activate"

    # Install production dependencies from requirements.txt
    pip install -r "${BACKEND_DIR}/requirements.txt"
    if [ $? -ne 0 ]; then
        log_message "Failed to install production dependencies." "ERROR"
        return 1
    fi
    log_message "Installed production dependencies." "SUCCESS"

    # Install development dependencies from requirements-dev.txt if in dev mode
    if [[ "${DEV_MODE}" == "true" ]]; then
        pip install -r "${BACKEND_DIR}/requirements-dev.txt"
        if [ $? -ne 0 ]; then
            log_message "Failed to install development dependencies." "ERROR"
            return 1
        fi
        log_message "Installed development dependencies." "SUCCESS"
    fi

    # Deactivate the virtual environment
    deactivate

    return 0
}

# Function to deploy infrastructure resources using Terraform
deploy_infrastructure() {
    if [[ "${SKIP_INFRA}" == "true" ]]; then
        log_message "Skipping infrastructure deployment." "WARNING"
        return 0
    fi

    log_message "Deploying infrastructure using Terraform..." "INFO"

    # Call deploy.sh script with appropriate parameters
    "${SCRIPT_DIR}/deploy.sh" -e "${ENVIRONMENT}" -p "${GCP_PROJECT_ID}" -r "${GCP_REGION}" -x "${RESOURCE_PREFIX}"
    local deploy_status=$?
    if [[ ${deploy_status} -ne 0 ]]; then
        log_message "Terraform deployment failed." "ERROR"
        return ${deploy_status}
    fi

    log_message "Terraform deployment completed successfully." "SUCCESS"
    return 0
}

# Function to set up Secret Manager and required secrets
setup_secrets() {
    if [[ "${SKIP_SECRETS}" == "true" ]]; then
        log_message "Skipping secrets setup." "WARNING"
        return 0
    fi

    log_message "Setting up Secret Manager and secrets..." "INFO"

    # Call secret_manager_setup.sh script with appropriate parameters
    "${SCRIPT_DIR}/secret_manager_setup.sh" -e "${ENVIRONMENT}" -p "${GCP_PROJECT_ID}" -r "${GCP_REGION}"
    local secrets_status=$?
    if [[ ${secrets_status} -ne 0 ]]; then
        log_message "Secret Manager setup failed." "ERROR"
        return ${secrets_status}
    fi

    log_message "Secret Manager setup completed successfully." "SUCCESS"
    return 0
}

# Function to run database migrations and seed initial data
migrate_database() {
    if [[ "${SKIP_DB}" == "true" ]]; then
        log_message "Skipping database migration." "WARNING"
        return 0
    fi

    log_message "Running database migrations..." "INFO"

    # Call migrate_db.sh script with appropriate parameters
    "${SCRIPT_DIR}/migrate_db.sh" -e "${ENVIRONMENT}" -p "${GCP_PROJECT_ID}" -r "${GCP_REGION}"
    local db_status=$?
    if [[ ${db_status} -ne 0 ]]; then
        log_message "Database migration failed." "ERROR"
        return ${db_status}
    fi

    log_message "Database migration completed successfully." "SUCCESS"
    return 0
}

# Function to initialize and configure Cloud Composer environment
initialize_composer() {
    if [[ "${SKIP_COMPOSER}" == "true" ]]; then
        log_message "Skipping Composer initialization." "WARNING"
        return 0
    fi

    log_message "Initializing Cloud Composer environment..." "INFO"

    # Call init_composer.sh script with appropriate parameters
    "${SCRIPT_DIR}/init_composer.sh" -e "${ENVIRONMENT}" -p "${GCP_PROJECT_ID}" -r "${GCP_REGION}"
    local composer_status=$?
    if [[ ${composer_status} -ne 0 ]]; then
        log_message "Cloud Composer initialization failed." "ERROR"
        return ${composer_status}
    fi

    log_message "Cloud Composer initialization completed successfully." "SUCCESS"
    return 0
}

# Function to set up monitoring, alerting, and dashboards
setup_monitoring() {
    if [[ "${SKIP_MONITORING}" == "true" ]]; then
        log_message "Skipping monitoring setup." "WARNING"
        return 0
    fi

    log_message "Setting up monitoring, alerting, and dashboards..." "INFO"

    # Call monitoring_setup.sh script with appropriate parameters
    "${SCRIPT_DIR}/monitoring_setup.sh" -e "${ENVIRONMENT}" -p "${GCP_PROJECT_ID}" -r "${GCP_REGION}"
    local monitoring_status=$?
    if [[ ${monitoring_status} -ne 0 ]]; then
        log_message "Monitoring setup failed." "ERROR"
        return ${monitoring_status}
    fi

    log_message "Monitoring setup completed successfully." "SUCCESS"
    return 0
}

# Function to set up git pre-commit hooks for development
setup_pre_commit_hooks() {
    if [[ "${DEV_MODE}" == "false" ]]; then
        log_message "Skipping pre-commit hooks setup (not in dev mode)." "WARNING"
        return 0
    fi

    log_message "Setting up pre-commit hooks..." "INFO"

    # Activate the virtual environment
    source "${VENV_DIR}/bin/activate"

    # Install pre-commit hooks
    pre-commit install
    local pre_commit_status=$?
    if [[ ${pre_commit_status} -ne 0 ]]; then
        log_message "Failed to install pre-commit hooks." "ERROR"
        deactivate
        return 1
    fi

    # Verify hooks were installed correctly
    if ! pre-commit run --all-files &> /dev/null; then
        log_message "Pre-commit hooks check failed." "ERROR"
        deactivate
        return 1
    fi

    log_message "Pre-commit hooks installed and verified." "SUCCESS"
    deactivate
    return 0
}

# Function to display script usage information
show_usage() {
    echo "Usage: ./setup.sh [options]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENV       Deployment environment (dev, staging, prod)"
    echo "  -p, --project PROJECT_ID    Google Cloud project ID"
    echo "  -r, --region REGION         Google Cloud region"
    echo "  -x, --prefix PREFIX         Resource name prefix"
    echo "  --skip-infra                Skip infrastructure deployment"
    echo "  --skip-secrets                Skip secrets setup"
    echo "  --skip-db                   Skip database migration"
    echo "  --skip-composer             Skip Composer initialization"
    echo "  --skip-monitoring           Skip monitoring setup"
    echo "  --skip-deps                 Skip dependencies installation"
    echo "  -d, --dev                   Setup development environment"
    echo "  -h, --help                  Display help information"
    echo ""
    echo "Environment variables:"
    echo "  GOOGLE_APPLICATION_CREDENTIALS    Path to GCP service account key file"
    echo "  ENVIRONMENT                 Deployment environment"
    echo "  GCP_PROJECT_ID              Google Cloud project ID"
    echo "  GCP_REGION                  Google Cloud region"
    echo "  RESOURCE_PREFIX             Prefix for resource naming"
}

# Function to parse command-line arguments
parse_args() {
    local args=("$@")
    local OPTS=$(getopt -o he:p:r:x:d --long help,environment:,project:,region:,prefix:,skip-infra,skip-secrets,skip-db,skip-composer,skip-monitoring,skip-deps -- "$@")

    if [[ $? -ne 0 ]]; then
        log_message "Failed to parse arguments." "ERROR"
        show_usage
        return 1
    fi

    eval set -- "$OPTS"

    while true; do
        case "$1" in
            -h|--help)
                show_usage
                return 0
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
            --skip-infra)
                SKIP_INFRA=true
                shift
                ;;
            --skip-secrets)
                SKIP_SECRETS=true
                shift
                ;;
            --skip-db)
                SKIP_DB=true
                shift
                ;;
            --skip-composer)
                SKIP_COMPOSER=true
                shift
                ;;
            --skip-monitoring)
                SKIP_MONITORING=true
                shift
                ;;
            --skip-deps)
                SKIP_DEPS=true
                shift
                ;;
            -d|--dev)
                DEV_MODE=true
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
    if [[ ! "${ENVIRONMENT}" =~ ^(development|staging|production)$ ]]; then
        log_message "Invalid environment: ${ENVIRONMENT}. Must be one of: development, staging, production" "ERROR"
        return 1
    fi

    return 0
}

# Main function
main() {
    # Parse command-line arguments
    parse_args "$@"
    if [[ $? -ne 0 ]]; then
        return 1
    fi

    # Initialize log file
    touch "${LOG_FILE}"
    log_message "========== Self-Healing Data Pipeline Setup ==========" "INFO"
    log_message "Starting setup process at $(date)" "INFO"

    # Check prerequisites
    if ! check_prerequisites; then
        log_message "Prerequisites check failed. Exiting." "ERROR"
        return 1
    fi

    # Setup virtual environment
    if [[ "${SKIP_DEPS}" == "true" ]]; then
        log_message "Skipping virtual environment setup and dependency installation." "WARNING"
    else
        if ! setup_virtual_environment; then
            log_message "Failed to setup virtual environment. Exiting." "ERROR"
            return 1
        fi

        # Install dependencies
        if ! install_dependencies; then
            log_message "Failed to install dependencies. Exiting." "ERROR"
            return 1
        fi
    fi

    # Deploy infrastructure
    if ! deploy_infrastructure; then
        log_message "Infrastructure deployment failed. Exiting." "ERROR"
        return 1
    fi

    # Setup secrets
    if ! setup_secrets; then
        log_message "Secret Manager setup failed. Exiting." "ERROR"
        return 1
    fi

    # Migrate database
    if ! migrate_database; then
        log_message "Database migration failed. Exiting." "ERROR"
        return 1
    fi

    # Initialize Composer
    if ! initialize_composer; then
        log_message "Cloud Composer initialization failed. Exiting." "ERROR"
        return 1
    fi

    # Setup monitoring
    if ! setup_monitoring; then
        log_message "Monitoring setup failed. Exiting." "ERROR"
        return 1
    fi

    # Setup pre-commit hooks
    if ! setup_pre_commit_hooks; then
        log_message "Pre-commit hooks setup failed. Continuing without them." "WARNING"
    fi

    log_message "Setup completed successfully." "SUCCESS"
    log_message "Log file: ${LOG_FILE}" "INFO"
    log_message "=======================================================" "INFO"

    return 0
}

# Execute the main function with all arguments
main "$@"
exit $?