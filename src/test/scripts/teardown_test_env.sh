#!/bin/bash
# Shell script that automates the teardown of test environments for the self-healing data pipeline.
# It serves as a wrapper for the Python-based test environment teardown process, providing a convenient
# command-line interface to destroy GCP resources and clean up local Docker environments to prevent
# resource leakage and unnecessary costs.

# Source: $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Project Root: $(cd "$SCRIPT_DIR/../../.." && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

# Python Env: python3
PYTHON_ENV=python3

# Teardown Script: $PROJECT_ROOT/src/test/environments/gcp/teardown_test_env.py
TEARDOWN_SCRIPT="$PROJECT_ROOT/src/test/environments/gcp/teardown_test_env.py"

# Default Env Info Path: $PROJECT_ROOT/src/test/environments/gcp/output.json
DEFAULT_ENV_INFO_PATH="$PROJECT_ROOT/src/test/environments/gcp/output.json"

# Log Level: INFO
LOG_LEVEL=INFO

# Function: print_usage
# Description: Prints the usage information for the script
# Parameters: None
# Returns: None
print_usage() {
  echo "Usage: $0 [options]"
  echo "Description: Tears down a test environment for the self-healing data pipeline."
  echo ""
  echo "Options:"
  echo "  -h, --help              Show this help message and exit"
  echo "  -e, --env-type <type>   Environment type (gcp or local). Required."
  echo "  -i, --env-id <id>       Test environment ID. Required for GCP."
  echo "  -p, --project-id <id>   GCP project ID. Required for GCP."
  echo "  -f, --env-file <path>   Path to the environment info JSON file. Default: $DEFAULT_ENV_INFO_PATH"
  echo "  -l, --log-level <level> Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: $LOG_LEVEL"
  echo "  -y, --yes               Automatic confirmation. Skips interactive prompt."
  echo ""
  echo "Examples:"
  echo "  $0 -e gcp -i test-env-1 -p my-gcp-project"
  echo "  $0 --env-type local"
}

# Function: check_dependencies
# Description: Checks if all required dependencies are installed
# Parameters: None
# Returns: 0 if all dependencies are available, 1 otherwise
check_dependencies() {
  # Check if Python 3 is installed
  if ! command -v $PYTHON_ENV &> /dev/null; then
    echo "Error: Python 3 is not installed."
    return 1
  fi

  # Check if Terraform is installed (for GCP environments)
  if [[ "$env_type" == "gcp" ]]; then
    if ! command -v terraform &> /dev/null; then
      echo "Error: Terraform is not installed."
      return 1
    fi
  fi

  # Check if Google Cloud SDK is installed (for GCP environments)
  if [[ "$env_type" == "gcp" ]]; then
    if ! command -v gcloud &> /dev/null; then
      echo "Error: Google Cloud SDK is not installed."
      return 1
    fi
  fi

  # Check if Docker and Docker Compose are installed (for local environments)
  if [[ "$env_type" == "local" ]]; then
    if ! command -v docker &> /dev/null; then
      echo "Error: Docker is not installed."
      return 1
    fi
    if ! command -v docker-compose &> /dev/null; then
      echo "Error: Docker Compose is not installed."
      return 1
    fi
  fi

  return 0
}

# Function: check_gcloud_auth
# Description: Checks if the user is authenticated with Google Cloud
# Parameters: None
# Returns: 0 if authenticated, 1 otherwise
check_gcloud_auth() {
  # Run gcloud auth list to check for active account
  active_account=$(gcloud auth list --format='value(account)' 2>/dev/null)

  # If no active account, print error message and instructions
  if [[ -z "$active_account" ]]; then
    echo "Error: Not authenticated with Google Cloud."
    echo "Please authenticate using 'gcloud auth login' and set the active project using 'gcloud config set project <project_id>'."
    return 1
  fi

  return 0
}

# Function: setup_python_env
# Description: Sets up the Python environment for running the teardown script
# Parameters: None
# Returns: 0 if successful, 1 otherwise
setup_python_env() {
  # Check if virtual environment exists
  if [[ ! -d "$PROJECT_ROOT/.venv" ]]; then
    # If not, create a virtual environment
    echo "Creating virtual environment..."
    $PYTHON_ENV -m venv "$PROJECT_ROOT/.venv" || return 1
  fi

  # Activate the virtual environment
  source "$PROJECT_ROOT/.venv/bin/activate" || return 1

  # Install required packages from requirements.txt
  echo "Installing required Python packages..."
  pip install -r "$PROJECT_ROOT/requirements.txt" || return 1

  return 0
}

# Function: parse_arguments
# Description: Parses command-line arguments for the script
# Parameters: args
# Returns: None
parse_arguments() {
  # Initialize default values for all parameters
  env_type=""
  env_id=""
  project_id=""
  env_file="$DEFAULT_ENV_INFO_PATH"
  log_level="$LOG_LEVEL"
  automatic_confirmation=false

  # Parse command-line arguments using getopts
  while getopts "he:i:p:f:l:y" opt; do
    case "$opt" in
      h)
        print_usage
        exit 0
        ;;
      e)
        env_type="$OPTARG"
        ;;
      i)
        env_id="$OPTARG"
        ;;
      p)
        project_id="$OPTARG"
        ;;
      f)
        env_file="$OPTARG"
        ;;
      l)
        log_level="$OPTARG"
        ;;
      y)
        automatic_confirmation=true
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        print_usage
        exit 1
        ;;
      :)
        echo "Option -$OPTARG requires an argument." >&2
        print_usage
        exit 1
        ;;
    esac
  done

  # Validate required parameters
  if [[ -z "$env_type" ]]; then
    echo "Error: --env-type is required." >&2
    print_usage
    exit 1
  fi

  if [[ "$env_type" == "gcp" ]]; then
    if [[ -z "$env_id" ]]; then
      echo "Error: --env-id is required for GCP environments." >&2
      print_usage
      exit 1
    fi
    if [[ -z "$project_id" ]]; then
      echo "Error: --project-id is required for GCP environments." >&2
      print_usage
      exit 1
    fi
  fi

  # Set global variables
  export env_type
  export env_id
  export project_id
  export env_file
  export log_level
  export automatic_confirmation
}

# Function: run_teardown_script
# Description: Runs the Python teardown script with the provided arguments
# Parameters: None
# Returns: Exit code from the Python script
run_teardown_script() {
  # Construct the command with all parameters
  command="$PYTHON_ENV \"$TEARDOWN_SCRIPT\" --env_info_path=\"$env_file\" --project_id=\"$project_id\" --test_env_id=\"$env_id\" --log_level=\"$log_level\""

  # Add automatic confirmation flag if provided
  if $automatic_confirmation; then
    command="$command --force"
  fi

  # Print the command being executed (if verbose)
  echo "Executing: $command"

  # Execute the Python script with the constructed command
  $command
  exit_code=$?

  # Capture and return the exit code
  return $exit_code
}

# Function: teardown_local_environment
# Description: Tears down a local test environment using Docker Compose
# Parameters: None
# Returns: 0 if successful, 1 otherwise
teardown_local_environment() {
  # Check if Docker and Docker Compose are installed
  if ! command -v docker &> /dev/null || ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker and Docker Compose are required for local environment teardown."
    return 1
  fi

  # Navigate to the local environment directory
  cd "$PROJECT_ROOT/src/test/environments/local" || return 1

  # Run docker-compose down to stop and remove containers
  echo "Tearing down local environment using Docker Compose..."
  docker-compose down ${remove_volumes_flag} || return 1

  return 0
}

# Function: main
# Description: Main function that orchestrates the test environment teardown
# Parameters: args
# Returns: Exit code (0 for success, non-zero for failure)
main() {
  # Parse command-line arguments
  parse_arguments "$@"

  # Check dependencies
  if [[ $(check_dependencies) -ne 0 ]]; then
    exit 1
  fi

  # If environment type is 'local', call teardown_local_environment
  if [[ "$env_type" == "local" ]]; then
    if $automatic_confirmation; then
      remove_volumes_flag="-v"
    fi
    teardown_local_environment || exit 1
    echo "Local test environment teardown completed successfully."
    exit 0
  fi

  # If environment type is 'gcp':
  if [[ "$env_type" == "gcp" ]]; then
    # Check Google Cloud authentication
    if [[ $(check_gcloud_auth) -ne 0 ]]; then
      exit 1
    fi

    # Setup Python environment if needed
    if [[ ! -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
      if [[ $(setup_python_env) -ne 0 ]]; then
        exit 1
      fi
    fi

    # Run the Python teardown script with parsed arguments
    run_teardown_script
    exit_code=$?

    # Print success or error message based on exit code
    if [[ "$exit_code" -eq 0 ]]; then
      echo "GCP test environment teardown completed successfully."
    else
      echo "GCP test environment teardown failed. Check the logs for details."
    fi

    # Return the exit code from the teardown process
    exit $exit_code
  fi

  echo "Error: Invalid environment type. Must be 'gcp' or 'local'." >&2
  print_usage
  exit 1
}

# Call the main function with command-line arguments
main "$@"