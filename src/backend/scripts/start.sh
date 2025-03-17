#!/bin/bash
# Script directory
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Project root directory
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

# Backend directory
BACKEND_DIR="${PROJECT_ROOT}/src/backend"

# Configuration directory
CONFIG_DIR="${BACKEND_DIR}/configs"

# Default host and port
DEFAULT_HOST="0.0.0.0"
DEFAULT_PORT="8080"
DEFAULT_WORKERS="4"
DEFAULT_TIMEOUT="120"
DEFAULT_LOG_LEVEL="INFO"

# Environment variable for deployment environment
ENVIRONMENT="${ENVIRONMENT:-development}"

# Use Gunicorn in production, Uvicorn in development
USE_GUNICORN="true"

# Enable auto-reload for development server
ENABLE_RELOAD="false"

# Configuration path
CONFIG_PATH=""

# Function to log messages
log_message() {
  local message="$1"
  local level="$2"
  local color=""

  case "$level" in
    ERROR)
      color="\033[31m" # Red
      ;;
    WARNING)
      color="\033[33m" # Yellow
      ;;
    INFO)
      color="\033[0m"  # Normal
      ;;
    SUCCESS)
      color="\033[32m" # Green
      ;;
    *)
      color="\033[0m"  # Normal
      ;;
  esac

  local timestamp=$(date +%Y-%m-%d\ %H:%M:%S)
  echo -e "${timestamp} - start.sh - ${color}${level}: ${message}\033[0m"
}

# Function to show usage information
show_usage() {
  echo "Usage: $0 [options]"
  echo "Starts the self-healing data pipeline backend application."
  echo ""
  echo "Options:"
  echo "  -h, --host <host>        Host address to bind to (default: $DEFAULT_HOST)"
  echo "  -p, --port <port>        Port to listen on (default: $DEFAULT_PORT)"
  echo "  -e, --environment <env>  Deployment environment (development, staging, production)"
  echo "  -c, --config <path>      Path to configuration file"
  echo "  -l, --log-level <level>  Logging level (DEBUG, INFO, WARNING, ERROR)"
  echo "  -w, --workers <num>      Number of worker processes (for Gunicorn)"
  echo "  -t, --timeout <seconds>  Worker timeout in seconds (for Gunicorn)"
  echo "  -d, --development        Use development server (Uvicorn) instead of production (Gunicorn)"
  echo "  -r, --reload             Enable auto-reload for development server"
  echo "  --help                   Display help information"
  echo ""
  echo "Examples:"
  echo "  $0 -h 0.0.0.0 -p 8000 -e production"
  echo "  $0 --config /path/to/config.yaml -l DEBUG"
}

# Function to check environment
check_environment() {
  # Check if Python is installed
  if ! command -v python3 &> /dev/null; then
    log_message "Python 3 is not installed. Please install it." ERROR
    return 1
  fi

  # Check if required Python packages are installed
  if ! pip3 show fastapi uvicorn gunicorn google-cloud-secret-manager python-dotenv &> /dev/null; then
    log_message "Required Python packages are not installed. Installing..." WARNING
    pip3 install fastapi uvicorn gunicorn google-cloud-secret-manager python-dotenv
    if [ $? -ne 0 ]; then
      log_message "Failed to install required Python packages." ERROR
      return 1
    fi
  fi

  # Check if configuration directory exists
  if [ ! -d "$CONFIG_DIR" ]; then
    log_message "Configuration directory does not exist: $CONFIG_DIR" WARNING
  fi

  # Verify environment variables are set correctly
  if [ -z "$ENVIRONMENT" ]; then
    log_message "ENVIRONMENT variable is not set. Defaulting to 'development'." WARNING
  fi

  return 0
}

# Function to set up the environment
setup_environment() {
  # Set PYTHONPATH to include project root
  export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

  # Export necessary environment variables
  export ENVIRONMENT="$ENVIRONMENT"

  # Set up virtual environment if not in container
  if [ ! -f /.dockerenv ]; then
    if [ ! -d "$PROJECT_ROOT/venv" ]; then
      log_message "Creating virtual environment..." INFO
      python3 -m venv "$PROJECT_ROOT/venv"
      if [ $? -ne 0 ]; then
        log_message "Failed to create virtual environment." ERROR
        return 1
      fi
    fi
    source "$PROJECT_ROOT/venv/bin/activate"
  fi

  # Ensure configuration directory is accessible
  if [ ! -r "$CONFIG_DIR" ]; then
    log_message "Configuration directory is not readable: $CONFIG_DIR" WARNING
  fi

  return 0
}

# Function to start the development server
start_development_server() {
  local host="$1"
  local port="$2"
  local log_level="$3"
  local config_path="$4"

  local command="uvicorn app:app --host $host --port $port --log-level $log_level"
  if [ "$ENABLE_RELOAD" = "true" ]; then
    command="$command --reload"
  fi
  if [ ! -z "$config_path" ]; then
    command="$command --env-file $config_path"
  fi

  log_message "Starting Uvicorn development server on $host:$port with log level $log_level" INFO
  eval "$command"
  local exit_code=$?
  log_message "Uvicorn development server exited with code: $exit_code" INFO
  return $exit_code
}

# Function to start the production server
start_production_server() {
  local host="$1"
  local port="$2"
  local workers="$3"
  local timeout="$4"
  local log_level="$5"
  local config_path="$6"

  local command="gunicorn app:app --bind $host:$port --workers $workers --timeout $timeout --log-level $log_level"
  if [ ! -z "$config_path" ]; then
    command="$command --env-file $config_path"
  fi

  log_message "Starting Gunicorn production server on $host:$port with $workers workers and timeout $timeout" INFO
  eval "$command"
  local exit_code=$?
  log_message "Gunicorn production server exited with code: $exit_code" INFO
  return $exit_code
}

# Function to parse command-line arguments
parse_args() {
  while getopts "h:p:e:c:l:w:t:dr" opt; do
    case $opt in
      h)
        DEFAULT_HOST="$OPTARG"
        ;;
      p)
        DEFAULT_PORT="$OPTARG"
        ;;
      e)
        ENVIRONMENT="$OPTARG"
        ;;
      c)
        CONFIG_PATH="$OPTARG"
        ;;
      l)
        DEFAULT_LOG_LEVEL="$OPTARG"
        ;;
      w)
        DEFAULT_WORKERS="$OPTARG"
        ;;
      t)
        DEFAULT_TIMEOUT="$OPTARG"
        ;;
      d)
        USE_GUNICORN="false"
        ;;
      r)
        ENABLE_RELOAD="true"
        ;;
      \?)
        log_message "Invalid option: -$OPTARG" ERROR
        show_usage
        return 1
        ;;
      :)
        log_message "Option -$OPTARG requires an argument." ERROR
        show_usage
        return 1
        ;;
    esac
  done
  shift $((OPTIND-1))
  return 0
}

# Main function
main() {
  # Parse command-line arguments
  parse_args "$@"
  if [ $? -ne 0 ]; then
    return 1
  fi

  # Check environment
  check_environment
  if [ $? -ne 0 ]; then
    return 1
  fi

  # Set up environment
  setup_environment
  if [ $? -ne 0 ]; then
    return 1
  fi

  # Determine whether to use development or production server
  if [ "$USE_GUNICORN" = "false" ]; then
    log_message "Using Uvicorn development server" INFO
    start_server_func="start_development_server"
  else
    log_message "Using Gunicorn production server" INFO
    start_server_func="start_production_server"
  fi

  # Log startup information
  log_message "Starting self-healing data pipeline backend..." INFO
  log_message "Environment: $ENVIRONMENT" INFO
  log_message "Host: $DEFAULT_HOST" INFO
  log_message "Port: $DEFAULT_PORT" INFO
  log_message "Log level: $DEFAULT_LOG_LEVEL" INFO
  if [ ! -z "$CONFIG_PATH" ]; then
    log_message "Configuration path: $CONFIG_PATH" INFO
  fi

  # Start the appropriate server
  if [ "$start_server_func" = "start_development_server" ]; then
    start_development_server "$DEFAULT_HOST" "$DEFAULT_PORT" "$DEFAULT_LOG_LEVEL" "$CONFIG_PATH"
    exit_code=$?
  else
    start_production_server "$DEFAULT_HOST" "$DEFAULT_PORT" "$DEFAULT_WORKERS" "$DEFAULT_TIMEOUT" "$DEFAULT_LOG_LEVEL" "$CONFIG_PATH"
    exit_code=$?
  fi

  # Handle any errors during startup
  if [ $exit_code -ne 0 ]; then
    log_message "Backend application failed to start." ERROR
    return 1
  fi

  return 0
}

# Run the main function
main "$@"
exit $?