#!/bin/bash
# run_e2e_tests.sh - Script to automate the execution of end-to-end tests for the self-healing data pipeline.

# Set -e to exit immediately if a command exits with a non-zero status.
set -e

# Define global variables
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
TEST_DIR="$PROJECT_ROOT/src/test"
E2E_DIR="$TEST_DIR/e2e"
REPORT_DIR="$TEST_DIR/reports/e2e"
LOG_FILE="$REPORT_DIR/e2e_test_run_$(date +"%Y%m%d_%H%M%S").log"

# Set default values for script parameters
FRAMEWORK="cypress"
BROWSER="chrome"
CI_MODE=false
HEADLESS=false
SPEC_PATTERN=""

# Function to print usage information
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo "Run end-to-end tests for the self-healing data pipeline."
    echo ""
    echo "Options:"
    echo "  -f, --framework   Test framework to use (cypress, playwright, or both)"
    echo "  -b, --browser     Browser to run tests in (chrome, firefox, webkit, or all)"
    echo "  -c, --ci          Run in CI mode (headless, with JUnit reports)"
    echo "  -s, --spec        Specific test spec pattern to run"
    echo "  -r, --report-dir  Directory for test reports"
    echo "  -e, --env         Environment to run tests against (dev, staging, prod)"
    echo "  -h, --headless    Run tests in headless mode"
    echo "  -v, --verbose     Enable verbose output"
    echo "  --help          Display usage information"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0") -f cypress -b chrome -c"
    echo "  $(basename "$0") --framework playwright --spec '**/login.spec.js'"
}

# Function to log a message to both console and log file
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    local formatted_message="[$timestamp] [$level] $message"

    # Determine color based on log level
    local color=""
    case "$level" in
        INFO) color="\033[32m" ;;  # Green
        WARNING) color="\033[33m" ;;  # Yellow
        ERROR) color="\033[31m" ;;    # Red
        *) color="\033[37m" ;;       # White (default)
    esac

    # Print to console with color
    echo -e "${color}$formatted_message\033[0m"

    # Append to log file
    echo "$formatted_message" >> "$LOG_FILE"
}

# Function to prepare the environment for test execution
setup_environment() {
    # Create report directories if they don't exist
    mkdir -p "$REPORT_DIR"

    # Initialize log file with header
    echo "--- Test Run ---" > "$LOG_FILE"
    log_message "INFO" "Starting test environment setup"

    # Check if required tools are installed (Node.js, npm)
    if ! command -v node &> /dev/null; then
        log_message "ERROR" "Node.js is required but not installed."
        return 1
    fi

    if ! command -v npm &> /dev/null; then
        log_message "ERROR" "npm is required but not installed."
        return 1
    fi

    # Install test dependencies if needed
    if [ ! -d "$E2E_DIR/node_modules" ]; then
        log_message "INFO" "Installing test dependencies"
        pushd "$E2E_DIR" > /dev/null
        npm install
        popd > /dev/null
        if [ $? -ne 0 ]; then
            log_message "ERROR" "Failed to install test dependencies"
            return 1
        fi
    else
        log_message "INFO" "Test dependencies already installed"
    fi

    # Start the application under test if required
    # (Add your application startup command here if needed)
    # For example:
    # npm start &
    # sleep 5  # Give the app some time to start

    log_message "INFO" "Test environment setup completed"
    return 0
}

# Function to run end-to-end tests using Cypress
run_cypress_tests() {
    log_message "INFO" "Starting Cypress tests"
    pushd "$E2E_DIR" > /dev/null

    # Construct Cypress command with appropriate options
    local cypress_command="npx cypress run"

    # Add browser option if specified
    if [ ! -z "$BROWSER" ] && [ "$BROWSER" != "all" ]; then
        cypress_command="$cypress_command --browser $BROWSER"
    fi

    # Add headless mode flag if in CI mode
    if [ "$CI_MODE" = true ] || [ "$HEADLESS" = true ]; then
        cypress_command="$cypress_command --headless"
    fi

    # Add spec pattern if specified
    if [ ! -z "$SPEC_PATTERN" ]; then
        cypress_command="$cypress_command --spec '$SPEC_PATTERN'"
    fi

    # Execute Cypress command
    log_message "INFO" "Executing Cypress command: $cypress_command"
    eval "$cypress_command"
    local exit_code=$?

    popd > /dev/null
    log_message "INFO" "Cypress tests completed with status: $exit_code"
    return $exit_code
}

# Function to run end-to-end tests using Playwright
run_playwright_tests() {
    log_message "INFO" "Starting Playwright tests"
    pushd "$E2E_DIR" > /dev/null

    # Construct Playwright command with appropriate options
    local playwright_command="npx playwright test"

    # Add project option if specified (browser type)
    if [ ! -z "$BROWSER" ] && [ "$BROWSER" != "all" ]; then
        playwright_command="$playwright_command --project=$BROWSER"
    fi

    # Add CI mode configuration if in CI mode
    if [ "$CI_MODE" = true ] || [ "$HEADLESS" = true ]; then
        playwright_command="$playwright_command --reporter=junit,html"
    fi

    # Add test pattern if specified
    if [ ! -z "$SPEC_PATTERN" ]; then
        playwright_command="$playwright_command '$SPEC_PATTERN'"
    fi

    # Execute Playwright command
    log_message "INFO" "Executing Playwright command: $playwright_command"
    eval "$playwright_command"
    local exit_code=$?

    popd > /dev/null
    log_message "INFO" "Playwright tests completed with status: $exit_code"
    return $exit_code
}

# Function to generate a report from test results
generate_report() {
    log_message "INFO" "Starting report generation"

    # Construct command for generate_test_report.py
    local report_command="$PYTHON_CMD $SCRIPT_DIR/generate_test_report.py"

    # Add input directory parameter
    report_command="$report_command --e2e-dir '$REPORT_DIR'"

    # Add output directory parameter
    report_command="$report_command --output-dir '$REPORT_DIR'"

    # Add report format options
    report_command="$report_command --format all"

    # Add CI mode flag if in CI mode
    if [ "$CI_MODE" = true ] || [ "$HEADLESS" = true ]; then
        report_command="$report_command --ci"
    fi

    # Execute report generation command
    log_message "INFO" "Executing report generation command: $report_command"
    eval "$report_command"
    local exit_code=$?

    log_message "INFO" "Report generation completed with status: $exit_code"
    return $exit_code
}

# Function to clean up the test environment after test execution
cleanup_environment() {
    log_message "INFO" "Starting test environment cleanup"

    # Stop the application under test if it was started by this script
    # (Add your application shutdown command here if needed)
    # For example:
    # pkill -f "npm start"

    # Remove temporary files
    # (Add commands to remove any temporary files created during the test run)

    # Reset test database if needed
    # (Add commands to reset the test database to a clean state)

    log_message "INFO" "Test environment cleanup completed"
    return 0
}

# Function to parse command line arguments
parse_arguments() {
    while getopts "f:b:csr:e:hv" opt; do
        case "$opt" in
            f)
                FRAMEWORK="$OPTARG"
                ;;
            b)
                BROWSER="$OPTARG"
                ;;
            c)
                CI_MODE=true
                ;;
            s)
                SPEC_PATTERN="$OPTARG"
                ;;
            r)
                REPORT_DIR="$OPTARG"
                ;;
            e)
                # Add logic to handle environment if needed
                ;;
            h)
                HEADLESS=true
                ;;
            v)
                VERBOSE=true
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

    # Validate argument combinations
    if [ "$FRAMEWORK" = "both" ] && [ ! -z "$BROWSER" ] && [ "$BROWSER" != "all" ]; then
        echo "Error: Browser option is not supported when framework is 'both'." >&2
        print_usage
        return 1
    fi

    # Display usage information if requested or if invalid arguments provided
    if [ "$1" = "--help" ]; then
        print_usage
        return 0
    fi

    return 0
}

# Main script execution flow
main() {
    # Set default values for script parameters
    FRAMEWORK="cypress"
    BROWSER="chrome"
    CI_MODE=false
    HEADLESS=false
    SPEC_PATTERN=""

    # Parse command line arguments using parse_arguments function
    parse_arguments "$@"
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # Setup test environment using setup_environment function
    setup_environment
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi

    # Run tests based on the specified framework
    local cypress_exit_code=0
    local playwright_exit_code=0

    if [ "$FRAMEWORK" = "cypress" ] || [ "$FRAMEWORK" = "both" ]; then
        run_cypress_tests
        cypress_exit_code=$?
    fi

    if [ "$FRAMEWORK" = "playwright" ] || [ "$FRAMEWORK" = "both" ]; then
        run_playwright_tests
        playwright_exit_code=$?
    fi

    # Combine exit codes if running both frameworks
    if [ "$FRAMEWORK" = "both" ]; then
        exit_code=$((cypress_exit_code + playwright_exit_code))
    else
        exit_code=$((cypress_exit_code + playwright_exit_code))
    fi

    # Generate test report using generate_report function
    generate_report
    report_exit_code=$?
    if [ $report_exit_code -ne 0 ]; then
        exit_code=1 # Set exit code to 1 if report generation fails
    fi

    # Clean up test environment using cleanup_environment function
    cleanup_environment
    cleanup_exit_code=$?
    if [ $cleanup_exit_code -ne 0 ]; then
        exit_code=1 # Set exit code to 1 if cleanup fails
    fi

    # Log summary of test execution with pass/fail status
    if [ $exit_code -eq 0 ]; then
        log_message "INFO" "End-to-end tests completed successfully"
    else
        log_message "ERROR" "End-to-end tests failed"
    fi

    # Exit with appropriate exit code
    return $exit_code
}

# Execute main function with all arguments
main "$@"
exit $?