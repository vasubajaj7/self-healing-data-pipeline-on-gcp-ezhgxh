#!/bin/bash
# init_composer.sh
#
# This script initializes and configures a Cloud Composer (Apache Airflow) environment
# for the self-healing data pipeline. It sets up connections, variables, pools, and
# uploads DAGs and plugins.
#
# Usage: ./init_composer.sh [options]
#
# Options:
#   -e, --environment ENV       Deployment environment (dev, staging, prod)
#   -p, --project PROJECT_ID    Google Cloud project ID
#   -r, --region REGION         Google Cloud region
#   -n, --name ENV_NAME         Cloud Composer environment name
#   -s, --skip-connections      Skip setting up connections
#   -v, --skip-variables        Skip setting up variables
#   -o, --skip-pools            Skip setting up pools
#   -d, --skip-dags             Skip uploading DAGs
#   -g, --skip-plugins          Skip uploading plugins
#   -y, --skip-verify           Skip verification step
#   -h, --help                  Display help information

# Exit immediately if a command exits with a non-zero status
set -e

# Define script directory and related paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BACKEND_DIR="${PROJECT_ROOT}/src/backend"
CONFIG_DIR="${BACKEND_DIR}/configs"
AIRFLOW_CONFIG_DIR="${BACKEND_DIR}/airflow/config"
LOG_FILE="${SCRIPT_DIR}/init_composer_$(date +%Y%m%d_%H%M%S).log"

# Get environment variables or set defaults
ENVIRONMENT=${ENVIRONMENT:-development}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-self-healing-pipeline-dev}
GCP_REGION=${GCP_REGION:-us-central1}
COMPOSER_ENV_NAME=${COMPOSER_ENV_NAME:-self-healing-pipeline-composer}

# Flags to control which steps to skip
SKIP_CONNECTIONS=false
SKIP_VARIABLES=false
SKIP_POOLS=false
SKIP_DAGS=false
SKIP_PLUGINS=false
SKIP_VERIFY=false

# Color codes for prettier output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Composer environment details
COMPOSER_GCS_BUCKET=""
COMPOSER_AIRFLOW_URI=""
COMPOSER_DAG_FOLDER=""

# Function to log a message to both console and log file
log_message() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local log_entry="[${timestamp}] [${level}] ${message}"
    
    # Output to console with colors
    if [[ "${level}" == "ERROR" ]]; then
        echo -e "${RED}${log_entry}${NC}"
    elif [[ "${level}" == "WARNING" ]]; then
        echo -e "${YELLOW}${log_entry}${NC}"
    elif [[ "${level}" == "SUCCESS" ]]; then
        echo -e "${GREEN}${log_entry}${NC}"
    else
        echo "${log_entry}"
    fi
    
    # Output to log file
    echo "${log_entry}" >> "${LOG_FILE}"
}

# Function to check if all prerequisites are met
check_prerequisites() {
    log_message "Checking prerequisites..."
    
    # Check if gcloud CLI is installed
    if ! command -v gcloud &> /dev/null; then
        log_message "gcloud CLI is not installed. Please install it and try again." "ERROR"
        return 1
    fi
    
    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        log_message "jq is not installed. Please install it and try again." "ERROR"
        return 1
    fi
    
    # Check if python3 is installed
    if ! command -v python3 &> /dev/null; then
        log_message "python3 is not installed. Please install it and try again." "ERROR"
        return 1
    fi
    
    # Check if user is authenticated with gcloud
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        log_message "Not authenticated with gcloud. Please run 'gcloud auth login' and try again." "ERROR"
        return 1
    fi
    
    # Check if required environment variables are set
    if [[ -z "${GCP_PROJECT_ID}" ]]; then
        log_message "GCP_PROJECT_ID is not set. Please set it and try again." "ERROR"
        return 1
    fi
    
    if [[ -z "${GCP_REGION}" ]]; then
        log_message "GCP_REGION is not set. Please set it and try again." "ERROR"
        return 1
    fi
    
    if [[ -z "${COMPOSER_ENV_NAME}" ]]; then
        log_message "COMPOSER_ENV_NAME is not set. Please set it and try again." "ERROR"
        return 1
    fi
    
    # Check if Composer environment exists
    if ! gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
         --project="${GCP_PROJECT_ID}" \
         --location="${GCP_REGION}" &> /dev/null; then
        log_message "Composer environment '${COMPOSER_ENV_NAME}' does not exist in project '${GCP_PROJECT_ID}' and region '${GCP_REGION}'." "ERROR"
        return 1
    fi
    
    # Check if configuration directories exist
    if [[ ! -d "${AIRFLOW_CONFIG_DIR}" ]]; then
        log_message "Airflow configuration directory '${AIRFLOW_CONFIG_DIR}' does not exist." "ERROR"
        return 1
    fi
    
    # Check if required configuration files exist
    if [[ ! -f "${AIRFLOW_CONFIG_DIR}/connections.json" ]]; then
        log_message "Connections configuration file '${AIRFLOW_CONFIG_DIR}/connections.json' does not exist." "ERROR"
        return 1
    fi
    
    if [[ ! -f "${AIRFLOW_CONFIG_DIR}/variables.json" ]]; then
        log_message "Variables configuration file '${AIRFLOW_CONFIG_DIR}/variables.json' does not exist." "ERROR"
        return 1
    fi
    
    if [[ ! -f "${AIRFLOW_CONFIG_DIR}/pool_config.json" ]]; then
        log_message "Pool configuration file '${AIRFLOW_CONFIG_DIR}/pool_config.json' does not exist." "ERROR"
        return 1
    fi
    
    log_message "All prerequisites met." "SUCCESS"
    return 0
}

# Function to get details about the Composer environment
get_composer_details() {
    log_message "Getting Composer environment details..."
    
    # Get the GCS bucket associated with the Composer environment
    COMPOSER_GCS_BUCKET=$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
                         --project="${GCP_PROJECT_ID}" \
                         --location="${GCP_REGION}" \
                         --format="value(config.dagGcsPrefix)" | sed 's/\/dags//')
    
    if [[ -z "${COMPOSER_GCS_BUCKET}" ]]; then
        log_message "Failed to get GCS bucket for Composer environment." "ERROR"
        return 1
    fi
    
    # Get the Airflow web UI URL
    COMPOSER_AIRFLOW_URI=$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
                          --project="${GCP_PROJECT_ID}" \
                          --location="${GCP_REGION}" \
                          --format="value(config.airflowUri)")
    
    if [[ -z "${COMPOSER_AIRFLOW_URI}" ]]; then
        log_message "Failed to get Airflow URI for Composer environment." "ERROR"
        return 1
    fi
    
    # Get the DAG folder location
    COMPOSER_DAG_FOLDER="${COMPOSER_GCS_BUCKET}/dags"
    
    log_message "Composer environment details:" "INFO"
    log_message "  GCS Bucket: ${COMPOSER_GCS_BUCKET}" "INFO"
    log_message "  Airflow URI: ${COMPOSER_AIRFLOW_URI}" "INFO"
    log_message "  DAG Folder: ${COMPOSER_DAG_FOLDER}" "INFO"
    
    return 0
}

# Function to set up Airflow connections
setup_connections() {
    if [[ "${SKIP_CONNECTIONS}" == true ]]; then
        log_message "Skipping Airflow connections setup." "WARNING"
        return 0
    fi
    
    log_message "Setting up Airflow connections..."
    
    # Read the connections.json file
    local connections_file="${AIRFLOW_CONFIG_DIR}/connections.json"
    local connections=$(cat "${connections_file}")
    
    # Check if connections.json is valid JSON
    if ! echo "${connections}" | jq '.' &> /dev/null; then
        log_message "Invalid JSON in connections.json" "ERROR"
        return 1
    fi
    
    # Get the number of connections to set up
    local connection_count=$(echo "${connections}" | jq '. | length')
    log_message "Found ${connection_count} connections to set up." "INFO"
    
    # Set up each connection
    local success_count=0
    
    # Iterate through each connection
    for i in $(seq 0 $((connection_count-1))); do
        local conn=$(echo "${connections}" | jq -r ".[$i]")
        local conn_id=$(echo "${conn}" | jq -r ".conn_id")
        local conn_type=$(echo "${conn}" | jq -r ".conn_type")
        local conn_description=$(echo "${conn}" | jq -r ".description")
        local conn_host=$(echo "${conn}" | jq -r ".host // \"\"")
        local conn_login=$(echo "${conn}" | jq -r ".login // \"\"")
        local conn_password=$(echo "${conn}" | jq -r ".password // \"\"")
        local conn_schema=$(echo "${conn}" | jq -r ".schema // \"\"")
        local conn_port=$(echo "${conn}" | jq -r ".port // \"\"")
        local conn_extra=$(echo "${conn}" | jq -r ".extra // {} | @json")
        
        log_message "Setting up connection: ${conn_id}" "INFO"
        
        # Build the connection command
        local cmd="airflow connections add '${conn_id}'"
        cmd="${cmd} --conn-type '${conn_type}'"
        
        if [[ -n "${conn_description}" && "${conn_description}" != "null" ]]; then
            cmd="${cmd} --conn-description '${conn_description}'"
        fi
        
        if [[ -n "${conn_host}" && "${conn_host}" != "null" ]]; then
            cmd="${cmd} --conn-host '${conn_host}'"
        fi
        
        if [[ -n "${conn_login}" && "${conn_login}" != "null" ]]; then
            cmd="${cmd} --conn-login '${conn_login}'"
        fi
        
        if [[ -n "${conn_password}" && "${conn_password}" != "null" ]]; then
            cmd="${cmd} --conn-password '${conn_password}'"
        fi
        
        if [[ -n "${conn_schema}" && "${conn_schema}" != "null" ]]; then
            cmd="${cmd} --conn-schema '${conn_schema}'"
        fi
        
        if [[ -n "${conn_port}" && "${conn_port}" != "null" && "${conn_port}" != "0" ]]; then
            cmd="${cmd} --conn-port '${conn_port}'"
        fi
        
        if [[ -n "${conn_extra}" && "${conn_extra}" != "null" && "${conn_extra}" != "{}" ]]; then
            cmd="${cmd} --conn-extra '${conn_extra}'"
        fi
        
        # First, delete the connection if it already exists
        gcloud composer environments run "${COMPOSER_ENV_NAME}" \
            --project="${GCP_PROJECT_ID}" \
            --location="${GCP_REGION}" \
            connections -- delete "${conn_id}" &> /dev/null || true
        
        # Execute the command
        if gcloud composer environments run "${COMPOSER_ENV_NAME}" \
            --project="${GCP_PROJECT_ID}" \
            --location="${GCP_REGION}" \
            connections -- -c "${cmd}" &> /dev/null; then
            log_message "Successfully set up connection: ${conn_id}" "SUCCESS"
            ((success_count++))
        else
            log_message "Failed to set up connection: ${conn_id}" "ERROR"
        fi
    done
    
    log_message "Set up ${success_count} out of ${connection_count} connections." "INFO"
    
    if [[ "${success_count}" -eq "${connection_count}" ]]; then
        log_message "All connections set up successfully." "SUCCESS"
        return 0
    else
        log_message "Failed to set up some connections." "WARNING"
        return 1
    fi
}

# Function to set up Airflow variables
setup_variables() {
    if [[ "${SKIP_VARIABLES}" == true ]]; then
        log_message "Skipping Airflow variables setup." "WARNING"
        return 0
    fi
    
    log_message "Setting up Airflow variables..."
    
    # Read the variables.json file
    local variables_file="${AIRFLOW_CONFIG_DIR}/variables.json"
    local variables=$(cat "${variables_file}")
    
    # Check if variables.json is valid JSON
    if ! echo "${variables}" | jq '.' &> /dev/null; then
        log_message "Invalid JSON in variables.json" "ERROR"
        return 1
    fi
    
    # Get all variable keys (flattened)
    local variable_keys=$(echo "${variables}" | jq -r 'paths(scalars) | join(".")')
    local variable_count=$(echo "${variable_keys}" | wc -l)
    
    log_message "Found ${variable_count} variables to set up." "INFO"
    
    # Set up each variable
    local success_count=0
    
    # Function to process JSON data and create variables
    process_json_object() {
        local json="$1"
        local prefix="$2"
        
        # If the JSON is an object, process its keys
        if [[ $(echo "${json}" | jq 'type') == '"object"' ]]; then
            local keys=$(echo "${json}" | jq -r 'keys[]')
            
            for key in ${keys}; do
                local new_prefix="${prefix:+${prefix}.}${key}"
                local value=$(echo "${json}" | jq ".[\"${key}\"]")
                
                # If the value is an object or array, process recursively
                if [[ $(echo "${value}" | jq 'type') == '"object"' || $(echo "${value}" | jq 'type') == '"array"' ]]; then
                    process_json_object "${value}" "${new_prefix}"
                else
                    # It's a scalar value, so set the variable
                    if [[ $(echo "${value}" | jq 'type') == '"string"' ]]; then
                        # Remove quotes for string values
                        value=$(echo "${value}" | jq -r '.')
                    fi
                    
                    set_variable "${new_prefix}" "${value}"
                    
                    if [[ $? -eq 0 ]]; then
                        ((success_count++))
                    fi
                fi
            done
        elif [[ $(echo "${json}" | jq 'type') == '"array"' ]]; then
            # Special handling for arrays - store the whole array as a JSON string
            set_variable "${prefix}" "${json}"
            
            if [[ $? -eq 0 ]]; then
                ((success_count++))
            fi
        fi
    }
    
    # Function to set a single variable
    set_variable() {
        local var_name="$1"
        local var_value="$2"
        
        log_message "Setting variable: ${var_name}" "INFO"
        
        # Execute the command
        if gcloud composer environments run "${COMPOSER_ENV_NAME}" \
            --project="${GCP_PROJECT_ID}" \
            --location="${GCP_REGION}" \
            variables -- set "${var_name}" "${var_value}" &> /dev/null; then
            log_message "Successfully set variable: ${var_name}" "SUCCESS"
            return 0
        else
            log_message "Failed to set variable: ${var_name}" "ERROR"
            return 1
        fi
    }
    
    # Start processing the root object
    process_json_object "${variables}" ""
    
    log_message "Set up ${success_count} variables." "INFO"
    
    if [[ "${success_count}" -gt 0 ]]; then
        log_message "Variables set up successfully." "SUCCESS"
        return 0
    else
        log_message "Failed to set up variables." "WARNING"
        return 1
    fi
}

# Function to set up Airflow pools
setup_pools() {
    if [[ "${SKIP_POOLS}" == true ]]; then
        log_message "Skipping Airflow pools setup." "WARNING"
        return 0
    fi
    
    log_message "Setting up Airflow pools..."
    
    # Read the pool_config.json file
    local pools_file="${AIRFLOW_CONFIG_DIR}/pool_config.json"
    local pools=$(cat "${pools_file}")
    
    # Check if pool_config.json is valid JSON
    if ! echo "${pools}" | jq '.' &> /dev/null; then
        log_message "Invalid JSON in pool_config.json" "ERROR"
        return 1
    fi
    
    # Get the number of pools to set up
    local pool_count=$(echo "${pools}" | jq '. | length')
    log_message "Found ${pool_count} pools to set up." "INFO"
    
    # Set up each pool
    local success_count=0
    
    # Iterate through each pool
    for i in $(seq 0 $((pool_count-1))); do
        local pool=$(echo "${pools}" | jq -r ".[$i]")
        local pool_name=$(echo "${pool}" | jq -r ".name")
        local pool_slots=$(echo "${pool}" | jq -r ".slots")
        local pool_description=$(echo "${pool}" | jq -r ".description // \"\"")
        
        log_message "Setting up pool: ${pool_name}" "INFO"
        
        # Execute the command
        if gcloud composer environments run "${COMPOSER_ENV_NAME}" \
            --project="${GCP_PROJECT_ID}" \
            --location="${GCP_REGION}" \
            pools -- set "${pool_name}" "${pool_slots}" "${pool_description}" &> /dev/null; then
            log_message "Successfully set up pool: ${pool_name}" "SUCCESS"
            ((success_count++))
        else
            log_message "Failed to set up pool: ${pool_name}" "ERROR"
        fi
    done
    
    log_message "Set up ${success_count} out of ${pool_count} pools." "INFO"
    
    if [[ "${success_count}" -eq "${pool_count}" ]]; then
        log_message "All pools set up successfully." "SUCCESS"
        return 0
    else
        log_message "Failed to set up some pools." "WARNING"
        return 1
    fi
}

# Function to upload DAG files to the Composer environment
upload_dags() {
    if [[ "${SKIP_DAGS}" == true ]]; then
        log_message "Skipping DAG upload." "WARNING"
        return 0
    fi
    
    log_message "Uploading DAG files to Composer environment..."
    
    # Get the DAG folder in the local repository
    local dags_folder="${BACKEND_DIR}/airflow/dags"
    
    # Check if the DAGs folder exists
    if [[ ! -d "${dags_folder}" ]]; then
        log_message "DAGs folder '${dags_folder}' does not exist." "ERROR"
        return 1
    fi
    
    # Check if there are any DAG files
    local dag_count=$(find "${dags_folder}" -name "*.py" | wc -l)
    
    if [[ "${dag_count}" -eq 0 ]]; then
        log_message "No DAG files found in '${dags_folder}'." "WARNING"
        return 0
    fi
    
    log_message "Found ${dag_count} DAG files to upload." "INFO"
    
    # Upload DAG files to the Composer environment
    if gsutil -m cp -r "${dags_folder}"/* "${COMPOSER_DAG_FOLDER}/" &> /dev/null; then
        log_message "Successfully uploaded ${dag_count} DAG files." "SUCCESS"
        return 0
    else
        log_message "Failed to upload DAG files." "ERROR"
        return 1
    fi
}

# Function to upload plugin files to the Composer environment
upload_plugins() {
    if [[ "${SKIP_PLUGINS}" == true ]]; then
        log_message "Skipping plugin upload." "WARNING"
        return 0
    fi
    
    log_message "Uploading plugin files to Composer environment..."
    
    # Get the plugins folder in the local repository
    local plugins_folder="${BACKEND_DIR}/airflow/plugins"
    
    # Check if the plugins folder exists
    if [[ ! -d "${plugins_folder}" ]]; then
        log_message "Plugins folder '${plugins_folder}' does not exist." "WARNING"
        return 0
    fi
    
    # Check if there are any plugin files
    local plugin_count=$(find "${plugins_folder}" -name "*.py" | wc -l)
    
    if [[ "${plugin_count}" -eq 0 ]]; then
        log_message "No plugin files found in '${plugins_folder}'." "WARNING"
        return 0
    fi
    
    log_message "Found ${plugin_count} plugin files to upload." "INFO"
    
    # Get the plugins folder location in GCS
    local plugins_gcs_folder="${COMPOSER_GCS_BUCKET}/plugins"
    
    # Upload plugin files to the Composer environment
    if gsutil -m cp -r "${plugins_folder}"/* "${plugins_gcs_folder}/" &> /dev/null; then
        log_message "Successfully uploaded ${plugin_count} plugin files." "SUCCESS"
        return 0
    else
        log_message "Failed to upload plugin files." "ERROR"
        return 1
    fi
}

# Function to verify that the Composer environment is properly configured
verify_setup() {
    if [[ "${SKIP_VERIFY}" == true ]]; then
        log_message "Skipping verification." "WARNING"
        return 0
    fi
    
    log_message "Verifying Composer environment setup..."
    
    local verification_failed=false
    
    # Verify connections
    if [[ "${SKIP_CONNECTIONS}" != true ]]; then
        log_message "Verifying connections..." "INFO"
        
        # List connections and count them
        local connection_count=$(gcloud composer environments run "${COMPOSER_ENV_NAME}" \
                               --project="${GCP_PROJECT_ID}" \
                               --location="${GCP_REGION}" \
                               connections -- list | grep -v "Current" | wc -l)
        
        if [[ "${connection_count}" -gt 0 ]]; then
            log_message "Verified ${connection_count} connections." "SUCCESS"
        else
            log_message "No connections found. Verification failed." "ERROR"
            verification_failed=true
        fi
    fi
    
    # Verify variables
    if [[ "${SKIP_VARIABLES}" != true ]]; then
        log_message "Verifying variables..." "INFO"
        
        # List variables and count them
        local variable_count=$(gcloud composer environments run "${COMPOSER_ENV_NAME}" \
                             --project="${GCP_PROJECT_ID}" \
                             --location="${GCP_REGION}" \
                             variables -- list | grep -v "Current" | wc -l)
        
        if [[ "${variable_count}" -gt 0 ]]; then
            log_message "Verified ${variable_count} variables." "SUCCESS"
        else
            log_message "No variables found. Verification failed." "ERROR"
            verification_failed=true
        fi
    fi
    
    # Verify pools
    if [[ "${SKIP_POOLS}" != true ]]; then
        log_message "Verifying pools..." "INFO"
        
        # List pools and count them
        local pool_count=$(gcloud composer environments run "${COMPOSER_ENV_NAME}" \
                         --project="${GCP_PROJECT_ID}" \
                         --location="${GCP_REGION}" \
                         pools -- list | grep -v "Current" | wc -l)
        
        if [[ "${pool_count}" -gt 0 ]]; then
            log_message "Verified ${pool_count} pools." "SUCCESS"
        else
            log_message "No pools found. Verification failed." "ERROR"
            verification_failed=true
        fi
    fi
    
    # Verify DAGs
    if [[ "${SKIP_DAGS}" != true ]]; then
        log_message "Verifying DAGs..." "INFO"
        
        # Wait a bit for DAGs to be processed
        log_message "Waiting for DAGs to be processed..." "INFO"
        sleep 10
        
        # List DAGs and count them
        local dag_count=$(gcloud composer environments run "${COMPOSER_ENV_NAME}" \
                        --project="${GCP_PROJECT_ID}" \
                        --location="${GCP_REGION}" \
                        dags -- list | grep -v "Current" | wc -l)
        
        if [[ "${dag_count}" -gt 0 ]]; then
            log_message "Verified ${dag_count} DAGs." "SUCCESS"
        else
            log_message "No DAGs found. Verification failed." "ERROR"
            verification_failed=true
        fi
    fi
    
    if [[ "${verification_failed}" == true ]]; then
        log_message "Some verification checks failed." "WARNING"
        return 1
    else
        log_message "All verification checks passed." "SUCCESS"
        return 0
    fi
}

# Function to display script usage information
show_usage() {
    echo "Usage: ./init_composer.sh [options]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENV       Deployment environment (dev, staging, prod)"
    echo "  -p, --project PROJECT_ID    Google Cloud project ID"
    echo "  -r, --region REGION         Google Cloud region"
    echo "  -n, --name ENV_NAME         Cloud Composer environment name"
    echo "  -s, --skip-connections      Skip setting up connections"
    echo "  -v, --skip-variables        Skip setting up variables"
    echo "  -o, --skip-pools            Skip setting up pools"
    echo "  -d, --skip-dags             Skip uploading DAGs"
    echo "  -g, --skip-plugins          Skip uploading plugins"
    echo "  -y, --skip-verify           Skip verification step"
    echo "  -h, --help                  Display help information"
    echo ""
    echo "Examples:"
    echo "  ./init_composer.sh -e dev -p my-project-id -r us-central1 -n my-composer-env"
    echo "  ./init_composer.sh --skip-connections --skip-variables"
    echo ""
    echo "Environment variables:"
    echo "  ENVIRONMENT                 Deployment environment"
    echo "  GCP_PROJECT_ID              Google Cloud project ID"
    echo "  GCP_REGION                  Google Cloud region"
    echo "  COMPOSER_ENV_NAME           Cloud Composer environment name"
    echo "  GOOGLE_APPLICATION_CREDENTIALS    Path to GCP service account key file"
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
                exit 0
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
            -n|--name)
                if [[ -n "${args[$i+1]}" && "${args[$i+1]}" != -* ]]; then
                    COMPOSER_ENV_NAME="${args[$i+1]}"
                    skip_next=true
                else
                    log_message "Missing value for ${args[$i]} option." "ERROR"
                    show_usage
                    return 1
                fi
                ;;
            -s|--skip-connections)
                SKIP_CONNECTIONS=true
                ;;
            -v|--skip-variables)
                SKIP_VARIABLES=true
                ;;
            -o|--skip-pools)
                SKIP_POOLS=true
                ;;
            -d|--skip-dags)
                SKIP_DAGS=true
                ;;
            -g|--skip-plugins)
                SKIP_PLUGINS=true
                ;;
            -y|--skip-verify)
                SKIP_VERIFY=true
                ;;
            *)
                log_message "Unknown option: ${args[$i]}" "ERROR"
                show_usage
                return 1
                ;;
        esac
        
        ((i++))
    done
    
    return 0
}

# Main function
main() {
    # Parse command-line arguments
    parse_args "$@"
    if [[ $? -ne 0 ]]; then
        return 1
    fi
    
    # Print welcome message
    log_message "=======================================================" "INFO"
    log_message "Cloud Composer Environment Initialization Script" "INFO"
    log_message "=======================================================" "INFO"
    log_message "Environment: ${ENVIRONMENT}" "INFO"
    log_message "Project ID: ${GCP_PROJECT_ID}" "INFO"
    log_message "Region: ${GCP_REGION}" "INFO"
    log_message "Composer Environment: ${COMPOSER_ENV_NAME}" "INFO"
    log_message "Log File: ${LOG_FILE}" "INFO"
    log_message "=======================================================" "INFO"
    
    # Check prerequisites
    if ! check_prerequisites; then
        log_message "Prerequisites check failed. Exiting." "ERROR"
        return 1
    fi
    
    # Get Composer environment details
    if ! get_composer_details; then
        log_message "Failed to get Composer environment details. Exiting." "ERROR"
        return 1
    fi
    
    # Set up Airflow connections
    if ! setup_connections; then
        log_message "Failed to set up Airflow connections." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    # Set up Airflow variables
    if ! setup_variables; then
        log_message "Failed to set up Airflow variables." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    # Set up Airflow pools
    if ! setup_pools; then
        log_message "Failed to set up Airflow pools." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    # Upload DAG files
    if ! upload_dags; then
        log_message "Failed to upload DAG files." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    # Upload plugin files
    if ! upload_plugins; then
        log_message "Failed to upload plugin files." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    # Verify the setup
    if ! verify_setup; then
        log_message "Verification failed. Some components may not be properly configured." "WARNING"
        # Continue anyway, as this might be a partial success
    fi
    
    log_message "=======================================================" "INFO"
    log_message "Cloud Composer environment initialization completed." "SUCCESS"
    log_message "Airflow Web UI: ${COMPOSER_AIRFLOW_URI}" "INFO"
    log_message "=======================================================" "INFO"
    
    return 0
}

# Execute the main function with all arguments
main "$@"
exit $?