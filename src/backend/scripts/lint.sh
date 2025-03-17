#!/bin/bash
# Lint script for the self-healing data pipeline backend code

# Exit on error, pipefail ensures pipeline exits with non-zero status if any command fails
set -e
set -o pipefail

# Color definitions for output formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Print header
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}  Self-Healing Data Pipeline  ${NC}"
echo -e "${YELLOW}  Backend Python Code Linting  ${NC}"
echo -e "${YELLOW}=========================================${NC}"

# Check if required tools are installed
check_tool() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}Error: $1 is not installed or not in PATH${NC}"
        echo "Please install it using: pip install $1"
        exit 1
    fi
}

echo "Checking for required linting tools..."
REQUIRED_TOOLS=("flake8" "pylint" "mypy" "black" "isort" "bandit")
for tool in "${REQUIRED_TOOLS[@]}"; do
    check_tool "$tool"
done
echo -e "${GREEN}All required tools are available.${NC}"

# Define source directories to lint
SOURCE_DIRS=(
    "src/backend/ingestion"
    "src/backend/quality"
    "src/backend/self_healing"
    "src/backend/monitoring"
    "src/backend/optimization"
    "src/backend/api"
    "src/backend/utils"
    "src/backend/db"
    "src/backend/airflow"
)

# Check if all directories exist
echo "Checking source directories..."
for dir in "${SOURCE_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo -e "${RED}Error: Directory $dir does not exist${NC}"
        exit 1
    fi
done
echo -e "${GREEN}All source directories exist.${NC}"

# Convert array to space-separated string for command line
SOURCE_DIRS_STR="${SOURCE_DIRS[@]}"

# Initialize status variable to track overall success/failure
STATUS=0

# Function to run a linting tool and track status
run_tool() {
    local tool=$1
    local description=$2
    local command=$3
    
    echo -e "\n${YELLOW}Running $tool ($description)...${NC}"
    if eval "$command"; then
        echo -e "${GREEN}$tool passed!${NC}"
        return 0
    else
        echo -e "${RED}$tool failed!${NC}"
        STATUS=1
        return 1
    fi
}

# Run linting tools
run_tool "flake8" "Check for syntax errors and undefined names" "flake8 $SOURCE_DIRS_STR --count --select=E9,F63,F7,F82 --show-source --statistics"
run_tool "pylint" "Comprehensive static code analysis" "pylint $SOURCE_DIRS_STR --rcfile=src/backend/.pylintrc"
run_tool "mypy" "Static type checking" "mypy $SOURCE_DIRS_STR --config-file=src/backend/mypy.ini"
run_tool "black" "Code formatting verification" "black --check $SOURCE_DIRS_STR"
run_tool "isort" "Import sorting verification" "isort --check-only --profile black $SOURCE_DIRS_STR"
run_tool "bandit" "Security vulnerability scanning" "bandit -r $SOURCE_DIRS_STR -c src/backend/.bandit"

# Print summary
echo -e "\n${YELLOW}=========================================${NC}"
echo -e "${YELLOW}  Linting Summary  ${NC}"
echo -e "${YELLOW}=========================================${NC}"

if [ $STATUS -eq 0 ]; then
    echo -e "${GREEN}All checks passed successfully!${NC}"
else
    echo -e "${RED}Some checks failed. Please fix the issues and try again.${NC}"
fi

# Exit with appropriate status code
exit $STATUS