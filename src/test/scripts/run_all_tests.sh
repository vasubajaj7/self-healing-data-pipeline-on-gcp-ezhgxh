#!/usr/bin/env bash
#
# Shell script that orchestrates the execution of all test types (unit, integration, performance, and end-to-end)
# for the self-healing data pipeline project. It provides a unified interface for running the complete test suite
# with configurable options for test selection, environment setup, and reporting.

# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

# Define global variables
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../" && pwd)
TEST_DIR="$PROJECT_ROOT/src/test"
REPORT_DIR="$TEST_DIR/reports"
COMBINED_REPORT_DIR="$REPORT_DIR/combined"
LOG_DIR="$TEST_DIR/logs"
LOG_FILE="$LOG_DIR/all_tests_$(date +"%Y%m%d_%H%M%S").log"
UNIT_TESTS_SCRIPT="$SCRIPT_DIR/run_unit_tests.sh"
INTEGRATION_TESTS_SCRIPT="$SCRIPT_DIR/run_integration_tests.sh"
PERFORMANCE_TESTS_SCRIPT="$SCRIPT_DIR/run_performance_tests.sh"
E2E_TESTS_SCRIPT="$SCRIPT_DIR/run_e2e_tests.sh"
REPORT_GENERATOR="$SCRIPT_DIR/generate_test_report.py"

# Function: print_usage
# Description: Displays script usage information
# Parameters: None
# Returns: void (Prints usage information to stdout)
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo
    echo "Run all tests for the self-healing data pipeline project."
    echo
    echo "Options:"
    echo "  -u, --skip-unit         Skip unit tests"
    echo "  -i, --skip-integration    Skip integration tests"
    echo "  -p, --skip-performance   Skip performance tests"
    echo "  -e, --skip-e2e          Skip end-to-end tests"
    echo "  -r, --skip-report       Skip combined report generation"
    echo "  -c, --ci                Run in CI mode (optimized for CI environments)"
    echo "  -C, --coverage           Generate coverage reports for unit tests"
    echo "  -v, --verbose           Enable verbose output"
    echo "  -f, --fail-fast         Stop after first test failure"
    echo "  -s, --size             Test data size for performance tests (small, medium, large)"
    echo "  -F, --framework        E2E test framework to use (cypress, playwright, or both)"
    echo "  -b, --browser          Browser to run E2E tests in (chrome, firefox, webkit, or all)"
    echo "  -h, --help              Display usage information"
    echo
    echo "Examples:"
    echo "  $(basename "$0") -c -C                 # Run all tests with coverage in CI mode"
    echo "  $(basename "$0") -u -i -p -e -r        # Skip all tests and report generation"
    echo "  $(basename "$0") -f -b chrome           # Run end-to-end tests with Chrome"
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
    echo "$(date) - Starting all tests" >> "$LOG_FILE"
    echo "--------------------------------------------------" >> "$LOG_FILE"

    # Check if required tools are installed (bash, python)
    if ! command -v bash &> /dev/null; then
        log_message "ERROR" "bash is required but not installed."
        return 1
    fi

    if ! command -v python3 &> /dev/null; then
        log_message "ERROR" "python3 is required but not installed."
        return 1
    fi

    # Export necessary environment variables for test configuration
    export TEST_DIR="$TEST_DIR"
    export REPORT_DIR="$REPORT_DIR"
    export LOG_DIR="$LOG_DIR"
    export LOG_FILE="$LOG_FILE"

    return 0
}

# Function: run_unit_tests
# Description: Executes unit tests using the run_unit_tests.sh script
# Parameters: None
# Returns: integer (Exit code of the test execution)
run_unit_tests() {
    log_message "INFO" "Running unit tests"

    # Construct command for run_unit_tests.sh with appropriate options
    local unit_test_cmd="bash $UNIT_TESTS_SCRIPT"

    # Add coverage flag if coverage is enabled
    if [[ "$COVERAGE" == "true" ]]; then
        unit_test_cmd="$unit_test_cmd -C"
    fi

    # Add CI mode flag if in CI mode
    if [[ "$CI_MODE" == "true" ]]; then
        unit_test_cmd="$unit_test_cmd -i"
    fi

    # Execute command using bash
    log_message "INFO" "Executing: $unit_test_cmd"
    eval "$unit_test_cmd"
    local exit_code=$?

    log_message "INFO" "Unit tests completed with status: $exit_code"
    return $exit_code
}

# Function: run_integration_tests
# Description: Executes integration tests using the run_integration_tests.sh script
# Parameters: None
# Returns: integer (Exit code of the test execution)
run_integration_tests() {
    log_message "INFO" "Running integration tests"

    # Construct command for run_integration_tests.sh with appropriate options
    local integration_test_cmd="bash $INTEGRATION_TESTS_SCRIPT"

    # Add CI mode flag if in CI mode
    if [[ "$CI_MODE" == "true" ]]; then
        integration_test_cmd="$integration_test_cmd -c"
    fi

    # Add verbose flag if verbose is enabled
    if [[ "$VERBOSE" == "true" ]]; then
        integration_test_cmd="$integration_test_cmd -v"
    fi

    # Execute command using bash
    log_message "INFO" "Executing: $integration_test_cmd"
    eval "$integration_test_cmd"
    local exit_code=$?

    log_message "INFO" "Integration tests completed with status: $exit_code"
    return $exit_code
}

# Function: run_performance_tests
# Description: Executes performance tests using the run_performance_tests.sh script
# Parameters: None
# Returns: integer (Exit code of the test execution)
run_performance_tests() {
    log_message "INFO" "Running performance tests"

    # Construct command for run_performance_tests.sh with appropriate options
    local performance_test_cmd="bash $PERFORMANCE_TESTS_SCRIPT"

    # Add test data size parameter
    if [ -n "$TEST_DATA_SIZE" ]; then
        performance_test_cmd="$performance_test_cmd -s $TEST_DATA_SIZE"
    fi

    # Add CI mode flag if in CI mode
    if [[ "$CI_MODE" == "true" ]]; then
        performance_test_cmd="$performance_test_cmd -c"
    fi

    # Execute command using bash
    log_message "INFO" "Executing: $performance_test_cmd"
    eval "$performance_test_cmd"
    local exit_code=$?

    log_message "INFO" "Performance tests completed with status: $exit_code"
    return $exit_code
}

# Function: run_e2e_tests
# Description: Executes end-to-end tests using the run_e2e_tests.sh script
# Parameters: None
# Returns: integer (Exit code of the test execution)
run_e2e_tests() {
    log_message "INFO" "Running end-to-end tests"

    # Construct command for run_e2e_tests.sh with appropriate options
    local e2e_test_cmd="bash $E2E_TESTS_SCRIPT"

    # Add framework parameter (cypress, playwright, or both)
    if [ -n "$E2E_FRAMEWORK" ]; then
        e2e_test_cmd="$e2e_test_cmd -F $E2E_FRAMEWORK"
    fi

    # Add browser parameter
    if [ -n "$BROWSER" ]; then
        e2e_test_cmd="$e2e_test_cmd -b $BROWSER"
    fi

    # Add headless flag if in CI mode
    if [[ "$CI_MODE" == "true" ]]; then
        e2e_test_cmd="$e2e_test_cmd -h"
    fi

    # Add CI mode flag if in CI mode
    if [[ "$CI_MODE" == "true" ]]; then
        e2e_test_cmd="$e2e_test_cmd -c"
    fi

    # Execute command using bash
    log_message "INFO" "Executing: $e2e_test_cmd"
    eval "$e2e_test_cmd"
    local exit_code=$?

    log_message "INFO" "End-to-end tests completed with status: $exit_code"
    return $exit_code
}

# Function: generate_combined_report
# Description: Generates a combined report from all test results
# Parameters: None
# Returns: integer (0 for success, non-zero for failure)
generate_combined_report() {
    log_message "INFO" "Generating combined test report"

    # Construct command for generate_test_report.py with appropriate parameters
    local report_cmd="$PYTHON_CMD $REPORT_GENERATOR"

    # Specify input directories for all test types
    report_cmd="$report_cmd --unit-dir '$REPORT_DIR/unit'"
    report_cmd="$report_cmd --integration-dir '$REPORT_DIR/integration'"
    report_cmd="$report_cmd --performance-dir '$REPORT_DIR/performance'"
    report_cmd="$report_cmd --e2e-dir '$REPORT_DIR/e2e'"

    # Specify output directory for combined report
    report_cmd="$report_cmd --output-dir '$COMBINED_REPORT_DIR'"

    # Specify report formats based on configuration
    report_cmd="$report_cmd --format all"

    # Execute command using python
    log_message "INFO" "Executing: $report_cmd"
    eval "$report_cmd"
    local exit_code=$?

    log_message "INFO" "Combined report generation completed with status: $exit_code"
    return $exit_code
}

# Function: parse_arguments
# Description: Parses command line arguments
# Parameters:
#   - args: array (Array of command line arguments)
# Returns: void (Sets global variables based on parsed arguments)
parse_arguments() {
    # Set default values for script parameters
    SKIP_UNIT=false
    SKIP_INTEGRATION=false
    SKIP_PERFORMANCE=false
    SKIP_E2E=false
    SKIP_REPORT=false
    CI_MODE=false
    COVERAGE=false
    VERBOSE=false
    FAIL_FAST=false
    TEST_DATA_SIZE="small"
    E2E_FRAMEWORK="cypress"
    BROWSER="chrome"

    # Process command line options using getopts
    while getopts "uipercCvfs:F:b:h" opt; do
        case "$opt" in
            u) SKIP_UNIT=true ;;
            i) SKIP_INTEGRATION=true ;;
            p) SKIP_PERFORMANCE=true ;;
            e) SKIP_E2E=true ;;
            r) SKIP_REPORT=true ;;
            c) CI_MODE=true ;;
            C) COVERAGE=true ;;
            v) VERBOSE=true ;;
            f) FAIL_FAST=true ;;
            s) TEST_DATA_SIZE="$OPTARG" ;;
            F) E2E_FRAMEWORK="$OPTARG" ;;
            b) BROWSER="$OPTARG" ;;
            h) print_usage; return 0 ;;
            \?) echo "Invalid option: -$OPTARG" >&2; print_usage; return 1 ;;
            :) echo "Option -$OPTARG requires an argument." >&2; print_usage; return 1 ;;
        esac
    done

    # Shift the options
    shift $((OPTIND -1))

    # Validate argument combinations
    if [[ "$E2E_FRAMEWORK" == "both" ]] && [[ "$BROWSER" != "chrome" && "$BROWSER" != "firefox" && "$BROWSER" != "webkit" && "$BROWSER" != "all" ]]; then
        echo "Error: When using 'both' frameworks, the browser must be 'chrome', 'firefox', 'webkit', or 'all'."
        print_usage
        return 1
    fi
}

# Main script execution flow
main() {
    # Set default values for script parameters
    SKIP_UNIT=false
    SKIP_INTEGRATION=false
    SKIP_PERFORMANCE=false
    SKIP_E2E=false
    SKIP_REPORT=false
    CI_MODE=false
    COVERAGE=false
    VERBOSE=false
    FAIL_FAST=false
    TEST_DATA_SIZE="small"
    E2E_FRAMEWORK="cypress"
    BROWSER="chrome"

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

    # Initialize overall exit code to 0
    local overall_exit_code=0

    # If not skipping unit tests, run unit tests and update exit code
    if [[ "$SKIP_UNIT" == "false" ]]; then
        run_unit_tests
        local unit_exit_code=$?
        overall_exit_code=$((overall_exit_code + unit_exit_code))
        log_message "INFO" "Unit tests: $(if [ $unit_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
        if [[ "$FAIL_FAST" == "true" ]] && [[ $unit_exit_code -ne 0 ]]; then
            log_message "WARNING" "Fail-fast enabled. Exiting after unit test failure."
            exit $overall_exit_code
        fi
    fi

    # If not skipping integration tests, run integration tests and update exit code
    if [[ "$SKIP_INTEGRATION" == "false" ]]; then
        run_integration_tests
        local integration_exit_code=$?
        overall_exit_code=$((overall_exit_code + integration_exit_code))
        log_message "INFO" "Integration tests: $(if [ $integration_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
        if [[ "$FAIL_FAST" == "true" ]] && [[ $integration_exit_code -ne 0 ]]; then
            log_message "WARNING" "Fail-fast enabled. Exiting after integration test failure."
            exit $overall_exit_code
        fi
    fi

    # If not skipping performance tests, run performance tests and update exit code
    if [[ "$SKIP_PERFORMANCE" == "false" ]]; then
        run_performance_tests
        local performance_exit_code=$?
        overall_exit_code=$((overall_exit_code + performance_exit_code))
        log_message "INFO" "Performance tests: $(if [ $performance_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
        if [[ "$FAIL_FAST" == "true" ]] && [[ $performance_exit_code -ne 0 ]]; then
            log_message "WARNING" "Fail-fast enabled. Exiting after performance test failure."
            exit $overall_exit_code
        fi
    fi

    # If not skipping e2e tests, run e2e tests and update exit code
    if [[ "$SKIP_E2E" == "false" ]]; then
        run_e2e_tests
        local e2e_exit_code=$?
        overall_exit_code=$((overall_exit_code + e2e_exit_code))
        log_message "INFO" "End-to-end tests: $(if [ $e2e_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
        if [[ "$FAIL_FAST" == "true" ]] && [[ $e2e_exit_code -ne 0 ]]; then
            log_message "WARNING" "Fail-fast enabled. Exiting after end-to-end test failure."
            exit $overall_exit_code
        fi
    fi

    # Generate combined report if not skipped
    if [[ "$SKIP_REPORT" == "false" ]]; then
        generate_combined_report
        local report_exit_code=$?
        if [ $report_exit_code -ne 0 ]; then
            log_message "ERROR" "Combined report generation failed"
            overall_exit_code=1 # Set exit code to 1 if report generation fails
        fi
    fi

    # Log summary of test execution with pass/fail status for each test type
    log_message "INFO" "Test Summary:"
    if [[ "$SKIP_UNIT" == "false" ]]; then
        log_message "INFO" "  Unit tests: $(if [ $unit_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
    fi
    if [[ "$SKIP_INTEGRATION" == "false" ]]; then
        log_message "INFO" "  Integration tests: $(if [ $integration_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
    fi
    if [[ "$SKIP_PERFORMANCE" == "false" ]]; then
        log_message "INFO" "  Performance tests: $(if [ $performance_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
    fi
    if [[ "$SKIP_E2E" == "false" ]]; then
        log_message "INFO" "  End-to-end tests: $(if [ $e2e_exit_code -eq 0 ]; then echo "PASSED"; else echo "FAILED"; fi)"
    fi

    # Exit with overall exit code
    if [ $overall_exit_code -eq 0 ]; then
        log_message "INFO" "All tests passed successfully!"
    else
        log_message "ERROR" "Some tests failed. Please check the logs for details."
    fi
    exit $overall_exit_code
}

# Execute main function with all arguments
main "$@"
exit $?