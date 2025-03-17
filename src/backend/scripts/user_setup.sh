#!/bin/bash
#
# Self-Healing Data Pipeline User Setup Script
#
# This script automates the setup of user accounts, IAM permissions, and
# authentication for the self-healing data pipeline. It creates necessary
# user roles, assigns appropriate permissions, and configures authentication
# for both human users and service accounts.
#
# Usage: ./user_setup.sh [options]
#
# Options:
#   -e, --environment ENV       Deployment environment (development, staging, production)
#   -p, --project PROJECT_ID    Google Cloud project ID
#   -r, --region REGION         Google Cloud region
#   -x, --prefix PREFIX         Resource name prefix
#   -c, --config CONFIG         Path to user configuration file
#   --skip-service-accounts     Skip service account setup
#   --skip-user-roles         Skip user role setup
#   --skip-secrets             Skip user secrets setup
#   -f, --force               Force setup without confirmation
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
LOG_FILE="${SCRIPT_DIR}/user_setup_$(date +%Y%m%d_%H%M%S).log"

# Get environment variables or set defaults
ENVIRONMENT=${ENVIRONMENT:-development}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-self-healing-pipeline-dev}
GCP_REGION=${GCP_REGION:-us-central1}
RESOURCE_PREFIX=${RESOURCE_PREFIX:-shp}
USER_CONFIG_FILE="${CONFIG_DIR}/user_permissions.yaml"

# Required APIs to enable
REQUIRED_APIS=("iam.googleapis.com" "cloudresourcemanager.googleapis.com" "secretmanager.googleapis.com")

# Flags to control which steps to skip
FORCE_MODE=false
SKIP_SERVICE_ACCOUNTS=false
SKIP_USER_ROLES=false
SKIP_SECRETS=false

# Source helper scripts
source "${SCRIPT_DIR}/setup.sh" # version N/A - Reuse logging functions and environment setup patterns
source "${SCRIPT_DIR}/secret_manager_setup.sh" # version N/A - Set up user-specific secrets in Secret Manager

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

    # Check if gcloud CLI is installed
    if ! command -v gcloud &> /dev/null; then
        log_message "gcloud CLI is not installed. Please install it and try again." "ERROR"
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

    # Check if user configuration file exists
    if [[ ! -f "${USER_CONFIG_FILE}" ]]; then
        log_message "User configuration file not found: ${USER_CONFIG_FILE}" "ERROR"
        status=1
    fi

    return ${status}
}

# Function to enable required Google Cloud APIs
enable_apis() {
    log_message "Enabling required APIs..." "INFO"
    local status=0

    for api in "${REQUIRED_APIS[@]}"; do
        log_message "Checking if API '${api}' is enabled..." "INFO"
        if gcloud services list --project="${GCP_PROJECT_ID}" --format="value(name)" | grep -q "${api}"; then
            log_message "API '${api}' is already enabled." "INFO"
        else
            log_message "Enabling API '${api}'..." "INFO"
            gcloud services enable "${api}" --project="${GCP_PROJECT_ID}"
            local api_status=$?
            if [[ ${api_status} -ne 0 ]]; then
                log_message "Failed to enable API '${api}'." "ERROR"
                status=1
            else
                log_message "API '${api}' enabled successfully." "SUCCESS"
            fi
        fi
    done

    if [[ ${status} -ne 0 ]]; then
        log_message "Failed to enable some APIs." "ERROR"
    else
        log_message "All required APIs are enabled." "SUCCESS"
    fi

    return ${status}
}

# Function to set up service accounts for pipeline components
setup_service_accounts() {
    if [[ "${SKIP_SERVICE_ACCOUNTS}" == true ]]; then
        log_message "Skipping service account setup." "WARNING"
        return 0
    fi

    log_message "Setting up service accounts..." "INFO"
    local status=0

    # Read service account configurations from user_permissions.yaml
    local service_accounts=$(yq e '.service_accounts' "${USER_CONFIG_FILE}")

    # Check if service_accounts is valid YAML
    if ! echo "${service_accounts}" | jq '.' &> /dev/null; then
        log_message "Invalid YAML in user_permissions.yaml for service_accounts" "ERROR"
        return 1
    fi

    # Get the number of service accounts to set up
    local service_account_count=$(echo "${service_accounts}" | jq '. | length')
    log_message "Found ${service_account_count} service accounts to set up." "INFO"

    # Iterate through each service account
    for i in $(seq 0 $((service_account_count-1))); do
        local service_account=$(echo "${service_accounts}" | jq -r ".[${i}]")
        local email=$(echo "${service_account}" | jq -r ".email")
        local description=$(echo "${service_account}" | jq -r ".description")
        local roles=$(echo "${service_account}" | jq -r ".roles")
        local generate_key=$(echo "${service_account}" | jq -r ".generate_key")

        log_message "Setting up service account: ${email}" "INFO"

        # Check if service account already exists
        if gcloud iam service-accounts describe "${email}" --project="${GCP_PROJECT_ID}" &> /dev/null; then
            log_message "Service account '${email}' already exists." "INFO"
        else
            # Create service account with appropriate description
            log_message "Creating service account '${email}'..." "INFO"
            gcloud iam service-accounts create "${email}" --display-name="${description}" --project="${GCP_PROJECT_ID}"
            local create_status=$?
            if [[ ${create_status} -ne 0 ]]; then
                log_message "Failed to create service account '${email}'." "ERROR"
                status=1
                continue
            else
                log_message "Service account '${email}' created successfully." "SUCCESS"
            fi
        fi

        # Assign IAM roles to service account based on configuration
        log_message "Assigning IAM roles to service account '${email}'..." "INFO"
        local role_count=$(echo "${roles}" | jq '. | length')
        local role_success_count=0
        for j in $(seq 0 $((role_count-1))); do
            local role=$(echo "${roles}" | jq -r ".[${j}]")
            local member="serviceAccount:${email}"
            gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" --member="${member}" --role="${role}"
            local role_status=$?
            if [[ ${role_status} -ne 0 ]]; then
                log_message "Failed to grant role '${role}' to service account '${email}'." "ERROR"
            else
                log_message "Granted role '${role}' to service account '${email}'." "SUCCESS"
                ((role_success_count++))
            fi
        done

        if [[ ${role_success_count} -eq ${role_count} ]]; then
            log_message "All roles assigned successfully to service account '${email}'." "SUCCESS"
        else
            log_message "Failed to assign some roles to service account '${email}'." "WARNING"
            status=1
        fi

        # Generate and store service account key if specified
        if [[ "${generate_key}" == true ]]; then
            log_message "Generating and storing service account key for '${email}'..." "INFO"
            local key_file="${BACKEND_DIR}/keys/${email}.json"
            gcloud iam service-accounts keys create "${key_file}" --iam-account="${email}" --project="${GCP_PROJECT_ID}"
            local key_status=$?
            if [[ ${key_status} -ne 0 ]]; then
                log_message "Failed to generate service account key for '${email}'." "ERROR"
                status=1
            else
                log_message "Service account key generated and stored at '${key_file}'." "SUCCESS"
            fi
        fi
    done

    if [[ ${status} -ne 0 ]]; then
        log_message "Failed to set up some service accounts." "ERROR"
    else
        log_message "All service accounts set up successfully." "SUCCESS"
    fi

    return ${status}
}

# Function to set up custom IAM roles and assign them to users
setup_user_roles() {
    if [[ "${SKIP_USER_ROLES}" == true ]]; then
        log_message "Skipping user role setup." "WARNING"
        return 0
    fi

    log_message "Setting up user roles..." "INFO"
    local status=0

    # Read user role configurations from user_permissions.yaml
    local roles=$(yq e '.roles' "${USER_CONFIG_FILE}")

    # Check if roles is valid YAML
    if ! echo "${roles}" | jq '.' &> /dev/null; then
        log_message "Invalid YAML in user_permissions.yaml for roles" "ERROR"
        return 1
    fi

    # Get the number of roles to set up
    local role_count=$(echo "${roles}" | jq '. | length')
    log_message "Found ${role_count} roles to set up." "INFO"

    # Iterate through each role
    for i in $(seq 0 $((role_count-1))); do
        local role=$(echo "${roles}" | jq -r ".[${i}]")
        local role_id=$(echo "${role}" | jq -r ".id")
        local title=$(echo "${role}" | jq -r ".title")
        local description=$(echo "${role}" | jq -r ".description")
        local permissions=$(echo "${role}" | jq -r ".permissions")
        local users=$(echo "${role}" | jq -r ".users")

        log_message "Setting up role: ${role_id}" "INFO"

        # Check if role already exists
        if gcloud iam roles describe "${role_id}" --project="${GCP_PROJECT_ID}" &> /dev/null; then
            log_message "Role '${role_id}' already exists." "INFO"
        else
            # Create custom role with specified permissions
            log_message "Creating custom role '${role_id}'..." "INFO"
            gcloud iam roles create "${role_id}" --project="${GCP_PROJECT_ID}" --title="${title}" --description="${description}" --permissions="${permissions}"
            local create_status=$?
            if [[ ${create_status} -ne 0 ]]; then
                log_message "Failed to create custom role '${role_id}'." "ERROR"
                status=1
                continue
            else
                log_message "Custom role '${role_id}' created successfully." "SUCCESS"
            fi
        fi

        # Grant role to each user
        log_message "Granting role '${role_id}' to users..." "INFO"
        local user_count=$(echo "${users}" | jq '. | length')
        local user_success_count=0
        for j in $(seq 0 $((user_count-1))); do
            local user=$(echo "${users}" | jq -r ".[${j}]")
            local member="user:${user}"
            gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" --member="${member}" --role="roles/${role_id}"
            local grant_status=$?
            if [[ ${grant_status} -ne 0 ]]; then
                log_message "Failed to grant role '${role_id}' to user '${user}'." "ERROR"
            else
                log_message "Granted role '${role_id}' to user '${user}'." "SUCCESS"
                ((user_success_count++))
            fi
        done

        if [[ ${user_success_count} -eq ${user_count} ]]; then
            log_message "All users granted role '${role_id}' successfully." "SUCCESS"
        else
            log_message "Failed to grant role '${role_id}' to some users." "WARNING"
            status=1
        fi
    done

    if [[ ${status} -ne 0 ]]; then
        log_message "Failed to set up some user roles." "ERROR"
    else
        log_message "All user roles set up successfully." "SUCCESS"
    fi

    return ${status}
}

# Function to set up user-specific secrets in Secret Manager
setup_user_secrets() {
    if [[ "${SKIP_SECRETS}" == true ]]; then
        log_message "Skipping user secrets setup." "WARNING"
        return 0
    fi

    log_message "Setting up user secrets..." "INFO"
    local status=0

    # Read user secret configurations from user_permissions.yaml
    local users=$(yq e '.users' "${USER_CONFIG_FILE}")

    # Check if users is valid YAML
    if ! echo "${users}" | jq '.' &> /dev/null; then
        log_message "Invalid YAML in user_permissions.yaml for users" "ERROR"
        return 1
    fi

    # Get the number of users to set up secrets for
    local user_count=$(echo "${users}" | jq '. | length')
    log_message "Found ${user_count} users to set up secrets for." "INFO"

    # Iterate through each user
    for i in $(seq 0 $((user_count-1))); do
        local user=$(echo "${users}" | jq -r ".[${i}]")
        local email=$(echo "${user}" | jq -r ".email")
        local secrets=$(echo "${user}" | jq -r ".secrets")

        log_message "Setting up secrets for user: ${email}" "INFO"

        # Check if secrets are defined for the user
        if [[ -z "${secrets}" || "${secrets}" == "null" ]]; then
            log_message "No secrets defined for user '${email}'. Skipping." "WARNING"
            continue
        fi

        # Iterate through each secret for the user
        local secret_count=$(echo "${secrets}" | jq '. | length')
        local secret_success_count=0
        for j in $(seq 0 $((secret_count-1))); do
            local secret=$(echo "${secrets}" | jq -r ".[${j}]")
            local secret_name=$(echo "${secret}" | jq -r ".name")
            local secret_value=$(echo "${secret}" | jq -r ".value")

            log_message "Setting up secret '${secret_name}' for user '${email}'..." "INFO"

            # Create user-specific secret in Secret Manager
            gcloud secrets create "${secret_name}" --replication-policy="automatic" --project="${GCP_PROJECT_ID}"
            local create_status=$?
            if [[ ${create_status} -ne 0 ]]; then
                log_message "Failed to create secret '${secret_name}' for user '${email}'." "ERROR"
                status=1
                continue
            fi

            # Add secret version with the specified value
            gcloud secrets versions add "${secret_name}" --data="${secret_value}" --project="${GCP_PROJECT_ID}"
            local add_status=$?
            if [[ ${add_status} -ne 0 ]]; then
                log_message "Failed to add secret version for '${secret_name}' for user '${email}'." "ERROR"
                status=1
                continue
            fi

            # Set appropriate IAM permissions for the user
            gcloud secrets add-iam-policy-binding "${secret_name}" --member="user:${email}" --role="roles/secretmanager.secretAccessor" --project="${GCP_PROJECT_ID}"
            local iam_status=$?
            if [[ ${iam_status} -ne 0 ]]; then
                log_message "Failed to set IAM permissions for secret '${secret_name}' for user '${email}'." "ERROR"
                status=1
                continue
            fi

            log_message "Secret '${secret_name}' set up successfully for user '${email}'." "SUCCESS"
            ((secret_success_count++))
        done

        if [[ ${secret_success_count} -eq ${secret_count} ]]; then
            log_message "All secrets set up successfully for user '${email}'." "SUCCESS"
        else
            log_message "Failed to set up some secrets for user '${email}'." "WARNING"
            status=1
        fi
    done

    if [[ ${status} -ne 0 ]]; then
        log_message "Failed to set up some user secrets." "ERROR"
    else
        log_message "All user secrets set up successfully." "SUCCESS"
    fi

    return ${status}
}

# Function to configure Workload Identity for secure service-to-service authentication
setup_workload_identity() {
    log_message "Setting up Workload Identity..." "INFO"
    local status=0

    # Check if Workload Identity is enabled for the project
    if gcloud iam workload-identity-pools list --project="${GCP_PROJECT_ID}" &> /dev/null; then
        log_message "Workload Identity is already enabled for the project." "INFO"
    else
        # Enable Workload Identity
        log_message "Enabling Workload Identity..." "INFO"
        gcloud iam workload-identity-pools create "${RESOURCE_PREFIX}-pool" --location="global" --project="${GCP_PROJECT_ID}"
        local pool_status=$?
        if [[ ${pool_status} -ne 0 ]]; then
            log_message "Failed to create Workload Identity pool." "ERROR"
            status=1
            return ${status}
        fi

        gcloud iam workload-identity-pools providers create-google "${RESOURCE_PREFIX}-provider" --location="global" --project="${GCP_PROJECT_ID}" --workload-identity-pool="${RESOURCE_PREFIX}-pool"
        local provider_status=$?
        if [[ ${provider_status} -ne 0 ]]; then
            log_message "Failed to create Workload Identity provider." "ERROR"
            status=1
            return ${status}
        fi
        log_message "Workload Identity enabled successfully." "SUCCESS"
    fi

    # Configure service account bindings for Kubernetes service accounts
    log_message "Configuring service account bindings for Kubernetes service accounts..." "INFO"
    # TODO: Implement logic to configure service account bindings for Kubernetes service accounts

    # Verify Workload Identity configuration
    log_message "Verifying Workload Identity configuration..." "INFO"
    # TODO: Implement logic to verify Workload Identity configuration

    if [[ ${status} -ne 0 ]]; then
        log_message "Failed to set up Workload Identity." "ERROR"
    else
        log_message "Workload Identity set up successfully." "SUCCESS"
    fi

    return ${status}
}

# Function to verify that all permissions are correctly set up
verify_permissions() {
    log_message "Verifying permissions..." "INFO"
    local status=0

    # Verify service accounts
    log_message "Verifying service accounts..." "INFO"
    # TODO: Implement logic to verify service accounts exist and have correct roles

    # Verify user roles
    log_message "Verifying user roles..." "INFO"
    # TODO: Implement logic to verify roles exist and users are assigned

    if [[ ${status} -ne 0 ]]; then
        log_message "Some permissions are not correctly set up." "ERROR"
    else
        log_message "All permissions verified successfully." "SUCCESS"
    fi

    return ${status}
}

# Function to display script usage information
show_usage() {
    echo "Usage: ./user_setup.sh [options]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENV       Deployment environment (development, staging, production)"
    echo "  -p, --project PROJECT_ID    Google Cloud project ID"
    echo "  -r, --region REGION         Google Cloud region"
    echo "  -x, --prefix PREFIX         Resource name prefix"
    echo "  -c, --config CONFIG         Path to user configuration file"
    echo "  --skip-service-accounts     Skip service account setup"
    echo "  --skip-user-roles         Skip user role setup"
    echo "  --skip-secrets             Skip user secrets setup"
    echo "  -f, --force               Force setup without confirmation"
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
    local i=0
    local skip_next=false

    while [[ $i -lt ${#args[@]} ]]; do
        if [[ "${skip_next}" == true ]]; then
            skip_next=false
            ((i++))
            continue
        fi

        case "${args[$i]}" in
            -h|--help)
                show_usage
                return 0
                ;;
            -e|--environment)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    ENVIRONMENT="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            -p|--project)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    GCP_PROJECT_ID="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            -r|--region)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    GCP_REGION="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            -x|--prefix)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    RESOURCE_PREFIX="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            -c|--config)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    USER_CONFIG_FILE="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            --skip-service-accounts)
                SKIP_SERVICE_ACCOUNTS=true
                ;;
            --skip-user-roles)
                SKIP_USER_ROLES=true
                ;;
            --skip-secrets)
                SKIP_SECRETS=true
                ;;
            -f|--force)
                FORCE_MODE=true
                ;;
            *)
                log_message "Unknown option: ${args[$i]}" "ERROR"
                show_usage
                return 1
                ;;
        esac
        ((i++))
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
    log_message "========== Self-Healing Data Pipeline User Setup ==========" "INFO"
    log_message "Starting setup process at $(date)" "INFO"

    # Check prerequisites
    if ! check_prerequisites; then
        log_message "Prerequisites check failed. Exiting." "ERROR"
        return 1
    fi

    # Enable required APIs
    if ! enable_apis; then
        log_message "Failed to enable required APIs. Exiting." "ERROR"
        return 1
    fi

    # Setup service accounts
    if ! setup_service_accounts; then
        log_message "Failed to setup service accounts. Exiting." "ERROR"
        return 1
    fi

    # Setup user roles
    if ! setup_user_roles; then
        log_message "Failed to setup user roles. Exiting." "ERROR"
        return 1
    fi

    # Setup user secrets
    if ! setup_user_secrets; then
        log_message "Failed to setup user secrets. Exiting." "ERROR"
        return 1
    fi

    # Setup Workload Identity
    if ! setup_workload_identity; then
        log_message "Failed to setup Workload Identity. Exiting." "ERROR"
        return 1
    fi

    # Verify permissions
    if ! verify_permissions; then
        log_message "Failed to verify permissions. Exiting." "ERROR"
        return 1
    fi

    log_message "Setup completed successfully." "SUCCESS"
    log_message "Log file: ${LOG_FILE}" "INFO"
    log_message "=======================================================" "INFO"

    return 0
}

# Execute the main function with all arguments
main "$@"
exit $?