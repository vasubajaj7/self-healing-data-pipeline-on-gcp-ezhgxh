#!/bin/bash
# setup_test_env.sh - Script to automate the setup of test environments for the self-healing data pipeline.

# Set -e to exit immediately if a command exits with a non-zero status.
set -e

# Define global variables
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
PYTHON_ENV=python3
SETUP_SCRIPT="$PROJECT_ROOT/src/test/environments/gcp/setup_test_env.py"
DEFAULT_CONFIG_PATH="$PROJECT_ROOT/src/test/environments/gcp/config.yaml"
DEFAULT_OUTPUT_PATH="$PROJECT_ROOT/src/test/environments/gcp/output.json"
LOG_LEVEL="INFO"

# Function to print usage information
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo "Setup GCP test environment for self-healing pipeline."
    echo ""
    echo "Options:"
    echo "  -h, --help                   Display this help message and exit"
    echo "  -c, --config <config_file>   Path to the configuration file (default: $DEFAULT_CONFIG_PATH)"
    echo "  -p, --project-id <project_id>  GCP project ID"
    echo "  -r, --region <region>          GCP region"
    echo "  -e, --env-id <env_id>          Test environment ID"
    echo "  -d, --data-size <data_size>    Test data volume size (small, medium, large) (default: small)"
    echo "  -a, --auto-destroy            Enable auto-destroy of resources after TTL"
    echo "  -t, --ttl <ttl_hours>        Environment TTL in hours (default: 24)"
    echo "  -o, --output <output_file>     Path to the output file (default: $DEFAULT_OUTPUT_PATH)"
    echo "  -l, --log-level <log_level>    Logging level (DEBUG, INFO, WARNING, ERROR) (default: $LOG_LEVEL)"
    echo "  -s, --skip-terraform          Skip Terraform execution"
    echo "  -g, --skip-data-gen          Skip test data generation"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0") -c config.yaml -p my-project -r us-central1 -e test-env"
    echo "  $(basename "$0") --project-id my-project --region us-east1 --data-size medium --auto-destroy"
}

# Function to check if required dependencies are installed
check_dependencies() {
    # Check if Python 3 is installed
    if ! command -v python3 &> /dev/null; then
        echo "Error: Python 3 is required but not installed."
        return 1
    fi

    # Check if Terraform is installed
    if ! command -v terraform &> /dev/null; then
        echo "Error: Terraform is required but not installed."
        return 1
    fi

    # Check if Google Cloud SDK is installed
    if ! command -v gcloud &> /dev/null; then
        echo "Error: Google Cloud SDK is required but not installed."
        return 1
    fi

    # All dependencies are available
    return 0
}

# Function to check if the user is authenticated with Google Cloud
check_gcloud_auth() {
    # Run gcloud auth list to check for active account
    active_account=$(gcloud auth list --format='value(account)' 2>/dev/null)

    # If no active account, print error message and instructions
    if [ -z "$active_account" ]; then
        echo "Error: Not authenticated with Google Cloud."
        echo "Please authenticate using 'gcloud auth login' and set the active project using 'gcloud config set project <project_id>'."
        return 1
    fi

    # Authenticated
    return 0
}

# Function to set up the Python environment for running the setup script
setup_python_env() {
    # Check if virtual environment exists
    if [ ! -d "$PROJECT_ROOT/.venv" ]; then
        echo "Creating virtual environment..."
        $PYTHON_ENV -m venv "$PROJECT_ROOT/.venv"
    fi

    # Activate the virtual environment
    source "$PROJECT_ROOT/.venv/bin/activate"

    # Install required packages from requirements.txt
    echo "Installing required Python packages..."
    pip install --upgrade pip
    pip install -r "$PROJECT_ROOT/requirements.txt"

    # Check if installation was successful
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install required Python packages."
        return 1
    fi

    # Successful
    return 0
}

# Function to parse command-line arguments
parse_arguments() {
    # Initialize default values
    CONFIG_PATH="$DEFAULT_CONFIG_PATH"
    PROJECT_ID=""
    REGION=""
    ENV_ID=""
    DATA_SIZE="small"
    AUTO_DESTROY=false
    TTL_HOURS=24
    OUTPUT_PATH="$DEFAULT_OUTPUT_PATH"
    LOG_LEVEL="$LOG_LEVEL"
    SKIP_TERRAFORM=false
    SKIP_DATA_GEN=false

    # Parse command-line arguments using getopts
    while getopts "hc:p:r:e:d:ato:l:sg" opt; do
        case "$opt" in
            h)
                print_usage
                return 0
                ;;
            c)
                CONFIG_PATH="$OPTARG"
                ;;
            p)
                PROJECT_ID="$OPTARG"
                ;;
            r)
                REGION="$OPTARG"
                ;;
            e)
                ENV_ID="$OPTARG"
                ;;
            d)
                DATA_SIZE="$OPTARG"
                ;;
            a)
                AUTO_DESTROY=true
                ;;
            t)
                TTL_HOURS="$OPTARG"
                ;;
            o)
                OUTPUT_PATH="$OPTARG"
                ;;
            l)
                LOG_LEVEL="$OPTARG"
                ;;
            s)
                SKIP_TERRAFORM=true
                ;;
            g)
                SKIP_DATA_GEN=true
                ;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                print_usage
                return 1
                ;;
            :)
                echo "Option -$OPTARG requires an argument." >&2
                print_usage
                return 1
                ;;
        esac
    done

    # Shift the options
    shift $((OPTIND -1))

    # Validate required parameters
    if [ -z "$PROJECT_ID" ]; then
        echo "Error: Project ID is required."
        print_usage
        return 1
    fi

    if [ -z "$REGION" ]; then
        echo "Error: Region is required."
        print_usage
        return 1
    fi

    if [ -z "$ENV_ID" ]; then
        echo "Error: Environment ID is required."
        print_usage
        return 1
    fi

    # Set global variables
    export CONFIG_PATH PROJECT_ID REGION ENV_ID DATA_SIZE AUTO_DESTROY TTL_HOURS OUTPUT_PATH LOG_LEVEL SKIP_TERRAFORM SKIP_DATA_GEN

    # Successful
    return 0
}

# Function to run the Python setup script with the provided arguments
run_setup_script() {
    # Construct the command with all parameters
    command="$PYTHON_ENV $SETUP_SCRIPT --config \"$CONFIG_PATH\" --project_id \"$PROJECT_ID\" --region \"$REGION\" --env_id \"$ENV_ID\" --volume_size \"$DATA_SIZE\" --output \"$OUTPUT_PATH\" --log_level \"$LOG_LEVEL\""

    # Add optional parameters
    if [ "$AUTO_DESTROY" = true ]; then
        command="$command --auto_destroy"
    fi

    if [ "$TTL_HOURS" ]; then
        command="$command --ttl_hours \"$TTL_HOURS\""
    fi

    if [ "$SKIP_TERRAFORM" = true ]; then
        command="$command --skip_terraform"
    fi

    if [ "$SKIP_DATA_GEN" = true ]; then
        command="$command --skip_data_generation"
    fi

    # Print the command being executed (if verbose)
    echo "Executing: $command"

    # Execute the Python script with the constructed command
    $command

    # Capture and return the exit code
    local exit_code=$?
    return $exit_code
}

# Function to set up a local test environment using Docker Compose
setup_local_environment() {
    # Check if Docker and Docker Compose are installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is required but not installed."
        return 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        echo "Error: Docker Compose is required but not installed."
        return 1
    fi

    # Navigate to the local environment directory
    cd "$PROJECT_ROOT/src/test/environments/local"

    # Run docker-compose up -d to start the environment
    echo "Starting local environment with Docker Compose..."
    docker-compose up -d

    # Wait for services to be ready (implementation depends on services)
    echo "Waiting for services to be ready..."
    # Add specific checks for each service as needed

    # Successful
    return 0
}

# Main function
main() {
    # Parse command-line arguments
    parse_arguments "$@"
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # Check dependencies
    check_dependencies
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # If environment type is 'local', call setup_local_environment
    if [ "$ENVIRONMENT_TYPE" = "local" ]; then
        setup_local_environment
        exit_code=$?
        return $exit_code
    fi

    # If environment type is 'gcp':
    # Check Google Cloud authentication
    check_gcloud_auth
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # Setup Python environment if needed
    setup_python_env
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # Run the Python setup script with parsed arguments
    run_setup_script
    exit_code=$?

    # Print success or error message based on exit code
    if [ $exit_code -eq 0 ]; then
        echo "Test environment setup completed successfully."
    else
        echo "Test environment setup failed."
    fi

    # Return the exit code from the setup process
    return $exit_code
}

# Execute main function with all arguments
main "$@"
exit $?