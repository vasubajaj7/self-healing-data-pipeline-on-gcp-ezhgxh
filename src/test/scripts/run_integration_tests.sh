#!/bin/bash
# shellcheck disable=SC2034,SC2046,SC2086,SC2094

# Description:
# Shell script that automates the execution of integration tests for the self-healing data pipeline project.
# It handles test environment setup, test execution with configurable options, and result reporting.

# Requirements Addressed:
# - Integration Testing (Technical Specifications/Testing Strategy/Integration Testing)
# - Test Automation (Technical Specifications/Testing Strategy/Test Automation)
# - Test Environment Management (Technical Specifications/Testing Strategy/Test Environment Architecture)

# Globals
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../" && pwd)"
TEST_DIR="$PROJECT_ROOT/src/test"
INTEGRATION_TEST_DIR="$TEST_DIR/integration"
REPORT_DIR="$TEST_DIR/reports/integration"
LOG_DIR="$TEST_DIR/logs"
LOG_FILE="$LOG_DIR/integration_tests_$(date +"%Y%m%d_%H%M%S").log"
ENV_SETUP_SCRIPT="$TEST_DIR/environments/gcp/setup_test_env.py"
ENV_TEARDOWN_SCRIPT="$TEST_DIR/environments/gcp/teardown_test_env.py"
ENV_CONFIG_FILE="$TEST_DIR/environments/gcp/config.yaml"
ENV_OUTPUT_FILE="$TEST_DIR/environments/gcp/output.json"
PYTEST_ARGS=""

# Function: print_usage
# Description: Displays script usage information
# Parameters: None
# Returns: void (Prints usage information to stdout)
print_usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Description: Runs integration tests for the self-healing data pipeline."
  echo ""
  echo "Options:"
  echo "  -s, --skip-env-setup    Skip GCP test environment setup"
  echo "  -t, --skip-env-teardown Skip GCP test environment teardown"
  echo "  -r, --skip-report       Skip test report generation"
  echo "  -c, --ci                Run in CI mode (generates JUnit reports)"
  echo "  -v, --verbose           Enable verbose output"
  echo "  -m, --markers           Specify pytest markers to run (default: integration)"
  echo "  -p, --path              Specify test path(s) to run"
  echo "  -k, --keyword           Only run tests matching the given keyword expression"
  echo "  -e, --env-config        Path to environment configuration file"
  echo "  -h, --help              Display usage information"
  echo ""
  echo "Examples:"
  echo "  $0 -v                    # Run integration tests with verbose output"
  echo "  $0 -m 'integration and not slow' # Run integration tests excluding slow tests"
  echo "  $0 -p path/to/test_file.py # Run tests in a specific file"
  echo "  $0 -s -t -r              # Skip env setup, teardown, and report generation"
}

# Function: log_message
# Description: Logs a message to both console and log file
# Parameters:
#   - level: string (Log level: INFO, WARNING, ERROR)
#   - message: string (Message to log)
# Returns: void (No return value)
log_message() {
  local level="$1"
  local message="$2"
  local timestamp=$(date +"%Y-%m-%d %H:%M:%S")

  # Determine color based on log level
  local color=""
  case "$level" in
    INFO) color="\033[32m" ;;  # Green
    WARNING) color="\033[33m" ;; # Yellow
    ERROR) color="\033[31m" ;;   # Red
  esac

  # Print to console with color
  if [ -n "$color" ]; then
    echo -e "${color}${timestamp} [${level}] ${message}\033[0m"
  else
    echo "${timestamp} [${level}] ${message}"
  fi

  # Append to log file
  echo "${timestamp} [${level}] ${message}" >> "$LOG_FILE"
}

# Function: setup_environment
# Description: Prepares the environment for test execution
# Parameters: None
# Returns: integer (0 for success, non-zero for failure)
setup_environment() {
  # Create report and log directories if they don't exist
  mkdir -p "$REPORT_DIR" "$LOG_DIR"

  # Initialize log file with header
  echo "--------------------------------------------------" > "$LOG_FILE"
  echo "$(date) - Starting integration tests" >> "$LOG_FILE"
  echo "--------------------------------------------------" >> "$LOG_FILE"

  # Check if required tools are installed
  if ! command -v pytest &> /dev/null; then
    log_message "ERROR" "pytest is not installed. Please install it using 'pip install pytest'"
    return 1
  fi

  if ! command -v python3 &> /dev/null; then
    log_message "ERROR" "python3 is not installed. Please install it."
    return 1
  fi

  # Export necessary environment variables for test configuration
  export PYTHONPATH="$PROJECT_ROOT"

  return 0
}

# Function: setup_test_environment
# Description: Sets up the GCP test environment for integration tests
# Parameters: None
# Returns: integer (0 for success, non-zero for failure)
setup_test_environment() {
  if [[ "$SKIP_ENV_SETUP" == "true" ]]; then
    log_message "INFO" "Skipping GCP test environment setup"
    return 0
  fi

  log_message "INFO" "Setting up GCP test environment"

  # Execute setup_test_env.py script with appropriate parameters
  python3 "$ENV_SETUP_SCRIPT" \
    --config "$ENV_CONFIG_FILE" \
    --output "$ENV_OUTPUT_FILE" \
    --log_level "INFO"

  local setup_result=$?

  if [ "$setup_result" -ne 0 ]; then
    log_message "ERROR" "GCP test environment setup failed"
    return 1
  fi

  # Export environment variables from output.json
  if [ -f "$ENV_OUTPUT_FILE" ]; then
    while IFS='=' read -r key value; do
      if [[ -n "$key" && -n "$value" ]]; then
        export "$key"="$value"
        log_message "INFO" "Exported environment variable: $key"
      fi
    done < <(jq -r 'to_entries | .[] | .key + "=" + @uri (.value)' "$ENV_OUTPUT_FILE")
  else
    log_message "WARNING" "Environment output file not found: $ENV_OUTPUT_FILE"
  fi

  log_message "INFO" "GCP test environment setup completed"
  return 0
}

# Function: run_integration_tests
# Description: Executes the integration tests using pytest
# Parameters: None
# Returns: integer (Exit code of the test execution)
run_integration_tests() {
  log_message "INFO" "Running integration tests"

  # Construct pytest command with appropriate options
  local pytest_command="pytest $PYTEST_ARGS"

  # Add markers, test paths, and report options
  pytest_command+=" -m '$TEST_MARKERS'"
  pytest_command+=" $TEST_PATHS"

  if [[ "$CI_MODE" == "true" ]]; then
    pytest_command+=" --junitxml=$REPORT_DIR/junit_report.xml"
  fi

  if [[ "$VERBOSE" == "true" ]]; then
    pytest_command+=" -v"
  fi

  log_message "INFO" "Executing pytest command: $pytest_command"

  # Execute pytest command
  eval "$pytest_command"
  local exit_code=$?

  log_message "INFO" "Integration tests completed with exit code: $exit_code"
  return $exit_code
}

# Function: generate_test_report
# Description: Generates a test report from the test results
# Parameters: None
# Returns: integer (0 for success, non-zero for failure)
generate_test_report() {
  if [[ "$SKIP_REPORT" == "true" ]]; then
    log_message "INFO" "Skipping test report generation"
    return 0
  fi

  log_message "INFO" "Generating test report"

  # Execute generate_test_report.py script with appropriate parameters
  python3 "$SCRIPT_DIR/generate_test_report.py" \
    --unit-dir "$TEST_DIR/reports/unit" \
    --integration-dir "$REPORT_DIR" \
    --output-dir "$REPORT_DIR" \
    --title "Self-Healing Data Pipeline Integration Test Report"

  local report_result=$?

  if [ "$report_result" -ne 0 ]; then
    log_message "ERROR" "Test report generation failed"
    return 1
  fi

  log_message "INFO" "Test report generation completed"
  return 0
}

# Function: teardown_test_environment
# Description: Cleans up the GCP test environment after tests
# Parameters: None
# Returns: integer (0 for success, non-zero for failure)
teardown_test_environment() {
  if [[ "$SKIP_ENV_TEARDOWN" == "true" ]]; then
    log_message "INFO" "Skipping GCP test environment teardown"
    return 0
  fi

  log_message "INFO" "Tearing down GCP test environment"

  # Execute teardown_test_env.py script with appropriate parameters
  python3 "$ENV_TEARDOWN_SCRIPT" \
    --env_info_path "$ENV_OUTPUT_FILE" \
    --log_level "INFO"

  local teardown_result=$?

  if [ "$teardown_result" -ne 0 ]; then
    log_message "ERROR" "GCP test environment teardown failed"
    return 1
  fi

  log_message "INFO" "GCP test environment teardown completed"
  return 0
}

# Function: parse_arguments
# Description: Parses command line arguments
# Parameters:
#   - args: array (Array of command line arguments)
# Returns: void (Sets global variables based on parsed arguments)
parse_arguments() {
  # Set default values for script parameters
  SKIP_ENV_SETUP="false"
  SKIP_ENV_TEARDOWN="false"
  SKIP_REPORT="false"
  CI_MODE="false"
  VERBOSE="false"
  TEST_MARKERS="integration"
  TEST_PATHS="$INTEGRATION_TEST_DIR"
  ENV_CONFIG_FILE="$TEST_DIR/environments/gcp/config.yaml"

  # Process command line arguments using getopts
  while getopts "strcvm:p:k:e:h" opt; do
    case "$opt" in
      s) SKIP_ENV_SETUP="true" ;;
      t) SKIP_ENV_TEARDOWN="true" ;;
      r) SKIP_REPORT="true" ;;
      c) CI_MODE="true" ;;
      v) VERBOSE="true" ;;
      m) TEST_MARKERS="$OPTARG" ;;
      p) TEST_PATHS="$OPTARG" ;;
      k) PYTEST_ARGS="$PYTEST_ARGS -k '$OPTARG'" ;;
      e) ENV_CONFIG_FILE="$OPTARG" ;;
      h) print_usage
         exit 0
         ;;
      \?) echo "Invalid option: -$OPTARG" >&2
         print_usage
         exit 1
         ;;
      :) echo "Option -$OPTARG requires an argument." >&2
         print_usage
         exit 1
         ;;
    esac
  done

  # Validate argument combinations
  if [[ "$SKIP_ENV_SETUP" == "true" && "$SKIP_ENV_TEARDOWN" == "false" ]]; then
    log_message "WARNING" "Skipping environment setup but not teardown. This might lead to errors."
  fi
}

# Main script execution flow
# Set default values for script parameters
SKIP_ENV_SETUP="false"
SKIP_ENV_TEARDOWN="false"
SKIP_REPORT="false"
CI_MODE="false"
VERBOSE="false"
TEST_MARKERS="integration"
TEST_PATHS="$INTEGRATION_TEST_DIR"

# Parse command line arguments
parse_arguments "$@"

# Setup test environment
setup_environment

# If not skipping environment setup, set up GCP test environment
if [[ "$SKIP_ENV_SETUP" == "false" ]]; then
  setup_test_environment
  local setup_env_result=$?
  if [ "$setup_env_result" -ne 0 ]; then
    log_message "ERROR" "Test environment setup failed. Exiting."
    exit 1
  fi
fi

# Run integration tests and store exit code
run_integration_tests
local test_result=$?

# Generate test report if not skipped
if [[ "$SKIP_REPORT" == "false" ]]; then
  generate_test_report
fi

# If not skipping environment teardown, tear down GCP test environment
if [[ "$SKIP_ENV_TEARDOWN" == "false" ]]; then
  teardown_test_environment
fi

# Log summary of test execution with pass/fail status
if [ "$test_result" -eq 0 ]; then
  log_message "INFO" "Integration tests PASSED"
else
  log_message "ERROR" "Integration tests FAILED"
fi

# Exit with test execution exit code
exit "$test_result"