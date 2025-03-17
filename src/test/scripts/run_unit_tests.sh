#!/usr/bin/env bash
#
# Shell script to run unit tests for the self-healing data pipeline project,
# supporting both backend Python tests and frontend TypeScript/JavaScript tests
# with configurable options for test selection, coverage reporting, and CI integration.

# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

# Global variables for directory paths
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../" && pwd)
BACKEND_DIR="$PROJECT_ROOT/src/backend"
WEB_DIR="$PROJECT_ROOT/src/web"
TEST_DIR="$PROJECT_ROOT/src/test"
COVERAGE_DIR="$TEST_DIR/coverage"
REPORT_DIR="$TEST_DIR/reports/unit"

# Default values for script parameters
BACKEND_ONLY=false
FRONTEND_ONLY=false
COVERAGE=false
CI_MODE=false
MODULE=""
COMPONENT=""

# Print usage information
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo
    echo "Run unit tests for the self-healing data pipeline project."
    echo
    echo "Options:"
    echo "  -b, --backend            Run only backend tests"
    echo "  -f, --frontend           Run only frontend tests"
    echo "  -m, --module MODULE      Run tests for specific backend module"
    echo "  -c, --component COMPONENT Run tests for specific frontend component"
    echo "  -C, --coverage           Generate coverage reports"
    echo "  -i, --ci                 Run in CI mode (generates JUnit reports)"
    echo "  -h, --help               Display usage information"
    echo
    echo "Examples:"
    echo "  $(basename "$0") -b -C                 # Run all backend tests with coverage"
    echo "  $(basename "$0") -f -c auth            # Run frontend tests for auth component"
    echo "  $(basename "$0") -m data_quality       # Run backend tests for data_quality module"
    echo "  $(basename "$0") -C -i                 # Run all tests with coverage in CI mode"
}

# Setup the test environment
setup_environment() {
    # Create necessary directories if they don't exist
    mkdir -p "$COVERAGE_DIR"
    mkdir -p "$REPORT_DIR"

    # Set environment variables for testing
    export PYTHONPATH="$BACKEND_DIR:$PYTHONPATH"
    export NODE_ENV="test"
    
    # Check if required tools are installed
    if ! command -v pytest &> /dev/null && ! $FRONTEND_ONLY; then
        echo "Error: pytest is not installed. Please install it to run backend tests."
        exit 1
    fi
    
    if ! command -v npm &> /dev/null && ! $BACKEND_ONLY; then
        echo "Error: npm is not installed. Please install it to run frontend tests."
        exit 1
    fi
    
    # Additional environment setup for CI mode
    if $CI_MODE; then
        export TEST_ENV="ci"
        export PYTEST_ADDOPTS="--no-header --quiet"
        export PYTHONUNBUFFERED=1
    else
        export TEST_ENV="local"
    fi
}

# Run backend Python tests
run_backend_tests() {
    local module="$1"
    local exit_code=0
    
    echo "Running backend tests..."
    
    # Change directory to backend
    cd "$BACKEND_DIR"
    
    # Check if virtual environment exists and activate it
    if [ -d "venv" ]; then
        echo "Activating virtual environment..."
        # shellcheck disable=SC1091
        source venv/bin/activate || source venv/Scripts/activate
    fi
    
    # Build pytest command
    local pytest_cmd="python -m pytest -v"
    
    # Add module if specified
    if [ -n "$module" ]; then
        # Check if module exists
        if [ ! -d "tests/$module" ] && [ ! -f "tests/test_$module.py" ]; then
            echo "Error: Module '$module' not found in tests directory"
            return 1
        fi
        
        if [ -d "tests/$module" ]; then
            pytest_cmd="$pytest_cmd tests/$module"
        else
            pytest_cmd="$pytest_cmd tests/test_$module.py"
        fi
    else
        pytest_cmd="$pytest_cmd tests/"
    fi
    
    # Add coverage if requested
    if $COVERAGE; then
        pytest_cmd="$pytest_cmd --cov=. --cov-report=xml:$COVERAGE_DIR/backend-coverage.xml --cov-report=html:$COVERAGE_DIR/backend-html"
    fi
    
    # Add JUnit reporting if in CI mode
    if $CI_MODE; then
        pytest_cmd="$pytest_cmd --junitxml=$REPORT_DIR/backend-junit.xml"
    fi
    
    # Run the tests
    echo "Executing: $pytest_cmd"
    eval "$pytest_cmd" || exit_code=$?
    
    # Display coverage report summary if enabled
    if $COVERAGE && [ $exit_code -eq 0 ]; then
        echo "Coverage report generated at: $COVERAGE_DIR/backend-html/index.html"
    fi
    
    # Return to original directory
    cd - > /dev/null
    
    return $exit_code
}

# Run frontend JavaScript/TypeScript tests
run_frontend_tests() {
    local component="$1"
    local exit_code=0
    
    echo "Running frontend tests..."
    
    # Change directory to web
    cd "$WEB_DIR"
    
    # Make sure dependencies are installed
    if [ ! -d "node_modules" ]; then
        echo "Installing npm dependencies..."
        npm install --silent
    fi
    
    # Build npm test command
    local npm_cmd="npm test"
    
    # Add component if specified
    if [ -n "$component" ]; then
        # Check if component directory/file exists
        if [ ! -d "src/components/$component" ] && [ ! -d "src/$component" ]; then
            echo "Warning: Component '$component' directory not found, using as test pattern anyway"
        fi
        npm_cmd="$npm_cmd -- --testPathPattern=\"$component\""
    fi
    
    # Add coverage if requested
    if $COVERAGE; then
        npm_cmd="$npm_cmd -- --coverage --coverageDirectory=\"$COVERAGE_DIR/frontend-html\" --coverageReporters=\"text\" \"html\" \"lcov\" \"json\""
    fi
    
    # Add JUnit reporting if in CI mode
    if $CI_MODE; then
        npm_cmd="$npm_cmd -- --reporters=\"default\" \"jest-junit\""
        export JEST_JUNIT_OUTPUT_DIR="$REPORT_DIR"
        export JEST_JUNIT_OUTPUT_NAME="frontend-junit.xml"
    fi
    
    # Run the tests
    echo "Executing: $npm_cmd"
    eval "$npm_cmd" || exit_code=$?
    
    # Display coverage report summary if enabled
    if $COVERAGE && [ $exit_code -eq 0 ]; then
        echo "Coverage report generated at: $COVERAGE_DIR/frontend-html/index.html"
    fi
    
    # Return to original directory
    cd - > /dev/null
    
    return $exit_code
}

# Parse command line arguments
parse_arguments() {
    # Process command line options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -b|--backend)
                BACKEND_ONLY=true
                shift
                ;;
            -f|--frontend)
                FRONTEND_ONLY=true
                shift
                ;;
            -m|--module)
                if [ -z "${2:-}" ] || [[ "${2:-}" == -* ]]; then
                    echo "Error: Missing value for --module option"
                    print_usage
                    exit 1
                fi
                MODULE="$2"
                shift 2
                ;;
            -c|--component)
                if [ -z "${2:-}" ] || [[ "${2:-}" == -* ]]; then
                    echo "Error: Missing value for --component option"
                    print_usage
                    exit 1
                fi
                COMPONENT="$2"
                shift 2
                ;;
            -C|--coverage)
                COVERAGE=true
                shift
                ;;
            -i|--ci)
                CI_MODE=true
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo "Error: Unknown option $1"
                print_usage
                exit 1
                ;;
        esac
    done
    
    # Check for conflicting arguments
    if $BACKEND_ONLY && $FRONTEND_ONLY; then
        echo "Error: Cannot specify both --backend and --frontend"
        print_usage
        exit 1
    fi
    
    # Validate module and component arguments
    if [ -n "$MODULE" ] && $FRONTEND_ONLY; then
        echo "Error: Cannot specify --module with --frontend"
        print_usage
        exit 1
    fi
    
    if [ -n "$COMPONENT" ] && $BACKEND_ONLY; then
        echo "Error: Cannot specify --component with --backend"
        print_usage
        exit 1
    fi
}

# Main script execution
main() {
    local backend_exit_code=0
    local frontend_exit_code=0
    
    # Parse arguments
    parse_arguments "$@"
    
    # Setup test environment
    setup_environment
    
    echo "===================================="
    echo "Running unit tests"
    echo "===================================="
    echo "Mode: $([ $CI_MODE == true ] && echo "CI" || echo "Local")"
    echo "Coverage: $([ $COVERAGE == true ] && echo "Enabled" || echo "Disabled")"
    if [ -n "$MODULE" ]; then echo "Backend module: $MODULE"; fi
    if [ -n "$COMPONENT" ]; then echo "Frontend component: $COMPONENT"; fi
    echo "===================================="
    
    # Run backend tests if not frontend only
    if ! $FRONTEND_ONLY; then
        run_backend_tests "$MODULE" || backend_exit_code=$?
        
        # Report backend test results
        if [ $backend_exit_code -ne 0 ]; then
            echo "Backend tests failed with exit code $backend_exit_code"
        else
            echo "Backend tests completed successfully"
        fi
    fi
    
    # Run frontend tests if not backend only
    if ! $BACKEND_ONLY; then
        run_frontend_tests "$COMPONENT" || frontend_exit_code=$?
        
        # Report frontend test results
        if [ $frontend_exit_code -ne 0 ]; then
            echo "Frontend tests failed with exit code $frontend_exit_code"
        else
            echo "Frontend tests completed successfully"
        fi
    fi
    
    # Generate combined reports if in CI mode and coverage is enabled
    if $CI_MODE && $COVERAGE && ! $BACKEND_ONLY && ! $FRONTEND_ONLY; then
        echo "Generating combined coverage reports..."
        
        # Here we would integrate with a tool like codecov to merge reports
        # For example: codecov --file "$COVERAGE_DIR/backend-coverage.xml" "$COVERAGE_DIR/frontend-html/lcov.info"
        
        # Create a simple combined report index
        cat > "$COVERAGE_DIR/index.html" <<EOF
<!DOCTYPE html>
<html>
<head>
    <title>Combined Coverage Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        ul { list-style-type: none; padding: 0; }
        li { margin: 10px 0; }
        a { color: #0066cc; text-decoration: none; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <h1>Combined Coverage Report</h1>
    <ul>
        <li><a href="backend-html/index.html">Backend Coverage Report</a></li>
        <li><a href="frontend-html/index.html">Frontend Coverage Report</a></li>
    </ul>
</body>
</html>
EOF
        echo "Combined report index created at: $COVERAGE_DIR/index.html"
    fi
    
    # Summary
    echo "===================================="
    echo "Test Summary:"
    if ! $FRONTEND_ONLY; then
        echo "Backend tests: $([ $backend_exit_code -eq 0 ] && echo "PASSED" || echo "FAILED")"
    fi
    
    if ! $BACKEND_ONLY; then
        echo "Frontend tests: $([ $frontend_exit_code -eq 0 ] && echo "PASSED" || echo "FAILED")"
    fi
    echo "===================================="
    
    # Exit with non-zero if any test suite failed
    if [ $backend_exit_code -ne 0 ] || [ $frontend_exit_code -ne 0 ]; then
        echo "Some tests failed. Please check the logs for details."
        exit 1
    fi
    
    echo "All tests passed successfully!"
    exit 0
}

# Execute main function with all arguments
main "$@"