#!/bin/bash
# generate_test_data.sh - Script to generate test data for the self-healing data pipeline

# Directory setup
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
PYTHON_CMD=python3

# Default values
DEFAULT_OUTPUT_DIR="$PROJECT_ROOT/src/test/mock_data"
DEFAULT_SIZE=1000
DEFAULT_FORMAT="csv"

# Display usage information
function print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo "Generate test data for the self-healing data pipeline."
    echo ""
    echo "Options:"
    echo "  -h, --help                   Display this help message and exit"
    echo "  -s, --schema <schema_name>   Schema name to use (required)"
    echo "  -n, --size <number>          Number of records to generate (default: $DEFAULT_SIZE)"
    echo "  -o, --output <directory>     Output directory (default: $DEFAULT_OUTPUT_DIR)"
    echo "  -f, --format <format>        Output format: csv, json, parquet, avro (default: $DEFAULT_FORMAT)"
    echo "  -i, --issues <config_file>   Configuration file for data quality issues"
    echo "  -p, --performance            Generate performance test data (large dataset)"
    echo "  -l, --healing <type>         Generate data for self-healing testing"
    echo "                               Types: missing_values, invalid_types, format_errors, etc."
    echo "  -t, --suite <suite_type>     Generate a test suite (basic, quality, healing, performance)"
    echo ""
    echo "Examples:"
    echo "  $(basename "$0") -s customer_data -n 1000 -o ./test_data -f csv"
    echo "  $(basename "$0") -s sales_data -i issue_config.json -o ./test_data"
    echo "  $(basename "$0") -s product_data -p -n 100000 -o ./perf_test"
    echo "  $(basename "$0") -s order_data -l missing_values -o ./healing_test"
    echo "  $(basename "$0") -s customer_data -t quality -o ./test_suite"
}

# Generate standard test data
function generate_standard_data() {
    local schema_name="$1"
    local size="$2"
    local output_dir="$3"
    local format="$4"
    local filename="$5"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Build Python script for execution
    local python_script="
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.test.mock_data.generator.data_generator import DataGenerator, save_data_to_file
from src.test.mock_data.generator.schema_generator import load_sample_schema
from src.backend.constants import FileFormat

try:
    # Load the schema
    schema = load_sample_schema('$schema_name')
    
    # Generate data
    data_generator = DataGenerator()
    df = data_generator.generate_data(schema, size=$size)
    
    # Save to file
    output_path = '$output_dir/$filename'
    format_enum = getattr(FileFormat, '$format'.upper())
    data_generator.save_data(df, output_path, format_enum)
    
    print(f'Data successfully generated at: {output_path}')
    sys.exit(0)
except Exception as e:
    print(f'Error generating data: {str(e)}')
    sys.exit(1)
"
    
    echo "Generating standard test data using schema '$schema_name'..."
    $PYTHON_CMD -c "$python_script"
    
    return $?
}

# Generate test data with quality issues
function generate_data_with_issues() {
    local schema_name="$1"
    local size="$2"
    local output_dir="$3"
    local format="$4"
    local filename="$5"
    local issues_config="$6"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Build Python script for execution
    local python_script="
import sys
import json
sys.path.insert(0, '$PROJECT_ROOT')
from src.test.mock_data.generator.data_generator import DataGenerator, inject_data_quality_issues, save_data_to_file
from src.test.mock_data.generator.schema_generator import load_sample_schema
from src.backend.constants import FileFormat

try:
    # Load the schema
    schema = load_sample_schema('$schema_name')
    
    # Load issues configuration
    with open('$issues_config', 'r') as f:
        issues_config = json.load(f)
    
    # Generate clean data
    data_generator = DataGenerator()
    df = data_generator.generate_data(schema, size=$size)
    
    # Inject quality issues
    df_with_issues, issues_details = data_generator.generate_data_with_issues(
        schema, 
        size=$size, 
        issues_config=issues_config
    )
    
    # Save to file
    output_path = '$output_dir/$filename'
    format_enum = getattr(FileFormat, '$format'.upper())
    data_generator.save_data(df_with_issues, output_path, format_enum)
    
    # Save issues details for reference
    issues_details_path = '$output_dir/${schema_name}_issues_details.json'
    with open(issues_details_path, 'w') as f:
        json.dump(issues_details, f, indent=2)
    
    print(f'Data with quality issues successfully generated at: {output_path}')
    print(f'Issues details saved at: {issues_details_path}')
    sys.exit(0)
except Exception as e:
    print(f'Error generating data with issues: {str(e)}')
    sys.exit(1)
"
    
    echo "Generating test data with quality issues using schema '$schema_name'..."
    $PYTHON_CMD -c "$python_script"
    
    return $?
}

# Generate performance test data
function generate_performance_data() {
    local schema_name="$1"
    local size="$2"
    local output_dir="$3"
    local format="$4"
    local filename="$5"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Build Python script for execution
    local python_script="
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.test.mock_data.generator.data_generator import DataGenerator
from src.test.mock_data.generator.schema_generator import load_sample_schema
from src.backend.constants import FileFormat
from src.test.utils.test_data_generators import PerformanceTestDataGenerator

try:
    # Load the schema
    schema = load_sample_schema('$schema_name')
    
    # Generate large dataset
    data_generator = DataGenerator()
    df = data_generator.generate_large_dataset(schema, total_size=$size, batch_size=min(10000, $size))
    
    # Save to file
    output_path = '$output_dir/$filename'
    format_enum = getattr(FileFormat, '$format'.upper())
    data_generator.save_data(df, output_path, format_enum)
    
    print(f'Performance test data successfully generated at: {output_path}')
    sys.exit(0)
except Exception as e:
    print(f'Error generating performance data: {str(e)}')
    sys.exit(1)
"
    
    echo "Generating performance test data using schema '$schema_name'..."
    $PYTHON_CMD -c "$python_script"
    
    return $?
}

# Generate data for self-healing testing
function generate_healing_test_data() {
    local schema_name="$1"
    local size="$2"
    local output_dir="$3"
    local format="$4"
    local filename="$5"
    local healing_type="$6"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Build Python script for execution
    local python_script="
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from src.test.mock_data.generator.data_generator import DataGenerator, inject_data_quality_issues
from src.test.mock_data.generator.schema_generator import load_sample_schema
from src.backend.constants import FileFormat, HealingActionType
from src.test.utils.test_data_generators import SelfHealingTestGenerator

try:
    # Load the schema
    schema = load_sample_schema('$schema_name')
    
    # Create issues configuration for healing type
    issues_config = {
        '$healing_type': {
            'columns': ['*'],  # Apply to all columns
            'percentage': 0.2  # 20% of data will have issues
        }
    }
    
    # Generate clean data
    data_generator = DataGenerator()
    df = data_generator.generate_data(schema, size=$size)
    
    # Inject specific issues for healing
    df_with_issues, issues_details = inject_data_quality_issues(df, issues_config, schema)
    
    # Save to file
    output_path = '$output_dir/$filename'
    format_enum = getattr(FileFormat, '$format'.upper())
    data_generator.save_data(df_with_issues, output_path, format_enum)
    
    # Save clean data for comparison
    clean_output_path = '$output_dir/${schema_name}_clean.${format}'
    data_generator.save_data(df, clean_output_path, format_enum)
    
    # Save issues details for reference
    issues_details_path = '$output_dir/${schema_name}_healing_details.json'
    import json
    with open(issues_details_path, 'w') as f:
        json.dump(issues_details, f, indent=2)
    
    print(f'Self-healing test data successfully generated at: {output_path}')
    print(f'Clean data saved at: {clean_output_path}')
    print(f'Healing details saved at: {issues_details_path}')
    sys.exit(0)
except Exception as e:
    print(f'Error generating healing test data: {str(e)}')
    sys.exit(1)
"
    
    echo "Generating self-healing test data for '$healing_type' issues using schema '$schema_name'..."
    $PYTHON_CMD -c "$python_script"
    
    return $?
}

# Generate a comprehensive test suite
function generate_test_suite() {
    local schema_name="$1"
    local output_dir="$2"
    local suite_type="$3"
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Build Python script for execution
    local python_script="
import sys
import os
sys.path.insert(0, '$PROJECT_ROOT')
from src.test.mock_data.generator.data_generator import DataGenerator, inject_data_quality_issues
from src.test.mock_data.generator.schema_generator import load_sample_schema, SchemaGenerator
from src.backend.constants import FileFormat

try:
    # Load the schema
    schema = load_sample_schema('$schema_name')
    
    # Create output directory for suite
    suite_dir = os.path.join('$output_dir', '${schema_name}_${suite_type}_suite')
    os.makedirs(suite_dir, exist_ok=True)
    
    # Generate appropriate test data based on suite type
    data_generator = DataGenerator()
    
    if '$suite_type' == 'basic':
        # Generate multiple standard datasets with different sizes
        sizes = [100, 1000, 10000]
        formats = ['CSV', 'JSON', 'PARQUET']
        
        for size in sizes:
            for format_str in formats:
                df = data_generator.generate_data(schema, size=size)
                filename = f'${schema_name}_{size}.{format_str.lower()}'
                output_path = os.path.join(suite_dir, filename)
                format_enum = getattr(FileFormat, format_str)
                data_generator.save_data(df, output_path, format_enum)
    
    elif '$suite_type' == 'quality':
        # Generate datasets with various quality issues
        issue_types = ['missing_values', 'invalid_types', 'out_of_range', 'format_errors', 'duplicates']
        
        for issue_type in issue_types:
            issues_config = {
                issue_type: {
                    'columns': ['*'],
                    'percentage': 0.2
                }
            }
            
            df = data_generator.generate_data(schema, size=1000)
            df_with_issues, issues_details = inject_data_quality_issues(df, issues_config, schema)
            
            # Save data with issues
            filename = f'${schema_name}_{issue_type}.csv'
            output_path = os.path.join(suite_dir, filename)
            data_generator.save_data(df_with_issues, output_path, FileFormat.CSV)
            
            # Save issues details
            details_filename = f'${schema_name}_{issue_type}_details.json'
            details_path = os.path.join(suite_dir, details_filename)
            import json
            with open(details_path, 'w') as f:
                json.dump(issues_details, f, indent=2)
    
    elif '$suite_type' == 'healing':
        # Generate datasets for self-healing scenarios
        healing_types = ['missing_values', 'invalid_types', 'out_of_range', 'format_errors']
        
        for healing_type in healing_types:
            issues_config = {
                healing_type: {
                    'columns': ['*'],
                    'percentage': 0.2
                }
            }
            
            df = data_generator.generate_data(schema, size=1000)
            df_with_issues, issues_details = inject_data_quality_issues(df, issues_config, schema)
            
            # Save data with issues
            filename = f'${schema_name}_{healing_type}_issues.csv'
            output_path = os.path.join(suite_dir, filename)
            data_generator.save_data(df_with_issues, output_path, FileFormat.CSV)
            
            # Save clean data for comparison
            clean_filename = f'${schema_name}_{healing_type}_clean.csv'
            clean_path = os.path.join(suite_dir, clean_filename)
            data_generator.save_data(df, clean_path, FileFormat.CSV)
            
            # Save issues details
            details_filename = f'${schema_name}_{healing_type}_details.json'
            details_path = os.path.join(suite_dir, details_filename)
            import json
            with open(details_path, 'w') as f:
                json.dump(issues_details, f, indent=2)
    
    elif '$suite_type' == 'performance':
        # Generate datasets of various sizes for performance testing
        sizes = [1000, 10000, 100000]
        
        for size in sizes:
            batch_size = min(10000, size)
            df = data_generator.generate_large_dataset(schema, total_size=size, batch_size=batch_size)
            
            filename = f'${schema_name}_{size}.parquet'
            output_path = os.path.join(suite_dir, filename)
            data_generator.save_data(df, output_path, FileFormat.PARQUET)
    
    print(f'Test suite successfully generated in directory: {suite_dir}')
    sys.exit(0)
except Exception as e:
    print(f'Error generating test suite: {str(e)}')
    sys.exit(1)
"
    
    echo "Generating $suite_type test suite using schema '$schema_name'..."
    $PYTHON_CMD -c "$python_script"
    
    return $?
}

# Parse command line arguments
function parse_arguments() {
    # Initialize variables with default values
    SCHEMA_NAME=""
    SIZE=$DEFAULT_SIZE
    OUTPUT_DIR=$DEFAULT_OUTPUT_DIR
    FORMAT=$DEFAULT_FORMAT
    ISSUES_CONFIG=""
    PERFORMANCE=false
    HEALING_TYPE=""
    SUITE_TYPE=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                print_usage
                exit 0
                ;;
            -s|--schema)
                SCHEMA_NAME="$2"
                shift 2
                ;;
            -n|--size)
                SIZE="$2"
                shift 2
                ;;
            -o|--output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -f|--format)
                FORMAT="$2"
                shift 2
                ;;
            -i|--issues)
                ISSUES_CONFIG="$2"
                shift 2
                ;;
            -p|--performance)
                PERFORMANCE=true
                shift
                ;;
            -l|--healing)
                HEALING_TYPE="$2"
                shift 2
                ;;
            -t|--suite)
                SUITE_TYPE="$2"
                shift 2
                ;;
            *)
                echo "Error: Unknown option: $1"
                print_usage
                return 1
                ;;
        esac
    done
    
    # Validate required arguments
    if [ -z "$SCHEMA_NAME" ]; then
        echo "Error: Schema name is required"
        print_usage
        return 1
    fi
    
    # Set derived variables
    FILENAME="${SCHEMA_NAME}_${SIZE}.${FORMAT}"
    
    return 0
}

# Main function
function main() {
    # Parse command line arguments
    parse_arguments "$@"
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        return $exit_code
    fi
    
    # Determine which type of data to generate
    if [ ! -z "$SUITE_TYPE" ]; then
        # Generate a test suite
        generate_test_suite "$SCHEMA_NAME" "$OUTPUT_DIR" "$SUITE_TYPE"
    elif [ "$PERFORMANCE" = true ]; then
        # Generate performance test data
        generate_performance_data "$SCHEMA_NAME" "$SIZE" "$OUTPUT_DIR" "$FORMAT" "$FILENAME"
    elif [ ! -z "$HEALING_TYPE" ]; then
        # Generate self-healing test data
        generate_healing_test_data "$SCHEMA_NAME" "$SIZE" "$OUTPUT_DIR" "$FORMAT" "$FILENAME" "$HEALING_TYPE"
    elif [ ! -z "$ISSUES_CONFIG" ]; then
        # Generate data with quality issues
        generate_data_with_issues "$SCHEMA_NAME" "$SIZE" "$OUTPUT_DIR" "$FORMAT" "$FILENAME" "$ISSUES_CONFIG"
    else
        # Generate standard test data
        generate_standard_data "$SCHEMA_NAME" "$SIZE" "$OUTPUT_DIR" "$FORMAT" "$FILENAME"
    fi
    
    return $?
}

# Execute main function with all arguments
main "$@"
exit $?