#!/bin/bash
# Shell script for database migration in the self-healing data pipeline.
# Handles the creation of BigQuery datasets, tables, and Firestore collections with appropriate schemas.
# Supports initial setup and schema evolution with optional data seeding.

# Set script to exit immediately if a command exits with a non-zero status
set -e

# Define script directory
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Define project root directory
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

# Define backend directory
BACKEND_DIR="${PROJECT_ROOT}/src/backend"

# Define configuration directory
CONFIG_DIR="${BACKEND_DIR}/configs"

# Define migrations directory
MIGRATIONS_DIR="${BACKEND_DIR}/db/migrations"

# Define log file path
LOG_FILE="${SCRIPT_DIR}/migrate_db_$(date +%Y%m%d_%H%M%S).log"

# Define environment (default to development)
ENVIRONMENT="${ENVIRONMENT:-development}"

# Define GCP project ID (default to development project)
GCP_PROJECT_ID="${GCP_PROJECT_ID:-self-healing-pipeline-dev}"

# Define GCP region (default to us-central1)
GCP_REGION="${GCP_REGION:-us-central1}"

# Define BigQuery dataset ID (default to self_healing_pipeline)
DATASET_ID="${DATASET_ID:-self_healing_pipeline}"

# Define whether to seed data (default to true)
SEED_DATA="${SEED_DATA:-true}"

# Function to log messages to console and log file
log_message() {
  local message="$1"
  local level="$2"

  # Format message with timestamp and log level
  local formatted_message="$(date +'%Y-%m-%d %H:%M:%S') - migrate_db.sh - ${level} - ${message}"

  # Print message to console with appropriate color
  case "$level" in
    ERROR)
      echo -e "\033[1;31m${formatted_message}\033[0m" # Red
      ;;
    WARNING)
      echo -e "\033[1;33m${formatted_message}\033[0m" # Yellow
      ;;
    INFO)
      echo "${formatted_message}"
      ;;
    SUCCESS)
      echo -e "\033[1;32m${formatted_message}\033[0m" # Green
      ;;
    *)
      echo "${formatted_message}"
      ;;
  esac

  # Append message to log file
  echo "${formatted_message}" >> "${LOG_FILE}"
}

# Function to check prerequisites
check_prerequisites() {
  log_message "Checking prerequisites..." "INFO"

  # Check if python3 is installed
  if ! command -v python3 &> /dev/null; then
    log_message "Error: python3 is not installed." "ERROR"
    return 1
  fi

  # Check if gcloud CLI is installed
  if ! command -v gcloud &> /dev/null; then
    log_message "Error: gcloud CLI is not installed." "ERROR"
    return 1
  fi

  # Check if jq is installed
  if ! command -v jq &> /dev/null; then
    log_message "Error: jq is not installed." "ERROR"
    return 1
  fi

  # Check if required Python packages are installed
  python3 -m venv venv || log_message "Error: Failed to create virtual environment." "ERROR"
  source venv/bin/activate
  pip install -r requirements.txt || log_message "Error: Failed to install required Python packages." "ERROR"
  deactivate

  # Verify GCP authentication status
  if ! gcloud auth list &> /dev/null; then
    log_message "Error: Not authenticated with gcloud CLI. Please run 'gcloud auth login'." "ERROR"
    return 1
  fi

  log_message "All prerequisites met." "SUCCESS"
  return 0
}

# Function to load configuration from environment-specific config file
load_config() {
  local environment="$1"
  log_message "Loading configuration for environment: ${environment}" "INFO"

  # Determine config file path based on environment
  local config_file="${CONFIG_DIR}/${environment}_config.yaml"

  # Check if config file exists
  if [ ! -f "${config_file}" ]; then
    log_message "Error: Configuration file not found: ${config_file}" "ERROR"
    return 1
  fi

  # Load and parse config file using jq
  local config=$(jq '.' "${config_file}")

  # Check if jq command was successful
  if [ $? -ne 0 ]; then
    log_message "Error: Failed to parse configuration file using jq." "ERROR"
    return 1
  fi

  # Extract database configuration settings
  # Example: DATABASE_HOST=$(echo "${config}" | jq -r '.database.host')

  log_message "Configuration loaded successfully." "SUCCESS"
  echo "${config}"
}

# Function to execute the database migration process
run_migration() {
  local project_id="$1"
  local dataset_id="$2"
  local location="$3"
  local seed_data="$4"
  local config_file="$5"

  log_message "Starting database migration process..." "INFO"

  # Construct Python command to execute initial_migration.py
  local python_command="python3 ${MIGRATIONS_DIR}/initial_migration.py"

  # Add command-line arguments for project_id, dataset_id, location, seed_data, and config_file
  if [ -n "${project_id}" ]; then
    python_command="${python_command} --project_id ${project_id}"
  fi
  if [ -n "${dataset_id}" ]; then
    python_command="${python_command} --dataset_id ${dataset_id}"
  fi
  if [ -n "${location}" ]; then
    python_command="${python_command} --location ${location}"
  fi
  if [ "${seed_data}" = "true" ]; then
    python_command="${python_command} --seed_data"
  fi
  if [ -n "${config_file}" ]; then
    python_command="${python_command} --config_file ${config_file}"
  fi

  # Execute the Python command
  log_message "Executing migration command: ${python_command}" "INFO"
  python3 -m venv venv || log_message "Error: Failed to create virtual environment." "ERROR"
  source venv/bin/activate
  pip install -r requirements.txt || log_message "Error: Failed to install required Python packages." "ERROR"
  eval "${python_command}"
  local status=$?
  deactivate

  # Check exit code of Python process
  if [ "${status}" -eq 0 ]; then
    log_message "Database migration completed successfully." "SUCCESS"
  else
    log_message "Database migration failed." "ERROR"
  fi

  # Return status code based on migration success
  return "${status}"
}

# Function to verify the migration was successful by checking database objects
verify_migration() {
  local project_id="$1"
  local dataset_id="$2"

  log_message "Verifying migration..." "INFO"

  # Check if BigQuery dataset exists
  gcloud bq datasets list --project="${project_id}" | grep -q "${dataset_id}"
  if [ $? -ne 0 ]; then
    log_message "Error: BigQuery dataset '${dataset_id}' does not exist." "ERROR"
    return 1
  fi

  # Verify required tables were created
  for table in "pipeline_executions" "quality_validations" "healing_executions" "pipeline_metrics" "alerts"; do
    gcloud bq tables list --project="${project_id}" "${dataset_id}" | grep -q "${table}"
    if [ $? -ne 0 ]; then
      log_message "Error: BigQuery table '${table}' does not exist in dataset '${dataset_id}'." "ERROR"
      return 1
    fi
  done

  # Check if Firestore collections exist
  # TODO: Add Firestore collection verification logic

  log_message "Migration verification successful." "SUCCESS"
  return 0
}

# Function to display script usage information
show_usage() {
  echo "Usage: $0 [options]"
  echo "Description: This script migrates the database for the self-healing data pipeline."
  echo
  echo "Options:"
  echo "  -p, --project <project_id>    Google Cloud project ID"
  echo "  -d, --dataset <dataset_id>    BigQuery dataset ID"
  echo "  -l, --location <location>     Google Cloud location/region"
  echo "  -e, --environment <environment> Deployment environment (development, staging, production)"
  echo "  -c, --config <config_file>    Path to configuration file"
  echo "  -s, --seed <true|false>       Seed initial data (default: true)"
  echo "  -f, --force                   Force migration without confirmation"
  echo "  -h, --help                    Display help information"
  echo
  echo "Examples:"
  echo "  $0 -p my-gcp-project -d my_dataset -l us-central1 -e production"
  echo "  $0 --config /path/to/config.yaml --seed false"
}

# Function to parse command-line arguments
parse_args() {
  while getopts "p:d:l:e:c:s:fh" opt; do
    case "$opt" in
      p)
        GCP_PROJECT_ID="$OPTARG"
        ;;
      d)
        DATASET_ID="$OPTARG"
        ;;
      l)
        GCP_REGION="$OPTARG"
        ;;
      e)
        ENVIRONMENT="$OPTARG"
        ;;
      c)
        CONFIG_FILE="$OPTARG"
        ;;
      s)
        SEED_DATA="$OPTARG"
        ;;
      f)
        FORCE_MIGRATION=true
        ;;
      h)
        show_usage
        return 1
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        show_usage
        return 1
        ;;
      :)
        echo "Option -$OPTARG requires an argument." >&2
        show_usage
        return 1
        ;;
    esac
  done
  shift $((OPTIND-1))

  # Validate required parameters
  if [ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
    log_message "Error: GOOGLE_APPLICATION_CREDENTIALS environment variable must be set." "ERROR"
    show_usage
    return 1
  fi

  return 0
}

# Main function
main() {
  # Parse command-line arguments
  if ! parse_args "$@"; then
    exit 1
  fi

  # Print welcome message and script information
  log_message "Self-Healing Data Pipeline - Database Migration Script" "INFO"
  log_message "Version: 1.0" "INFO"
  log_message "Log file: ${LOG_FILE}" "INFO"

  # Check prerequisites
  if ! check_prerequisites; then
    exit 1
  fi

  # Load configuration
  local config=$(load_config "${ENVIRONMENT}")
  if [ $? -ne 0 ]; then
    exit 1
  fi

  # Run migration
  if ! run_migration "${GCP_PROJECT_ID}" "${DATASET_ID}" "${GCP_REGION}" "${SEED_DATA}" "${CONFIG_FILE}"; then
    exit 1
  fi

  # Verify migration
  if ! verify_migration "${GCP_PROJECT_ID}" "${DATASET_ID}"; then
    exit 1
  fi

  # Print success message
  log_message "Database migration completed successfully!" "SUCCESS"

  # Exit with success status code
  exit 0
}

# Execute main function
main "$@"