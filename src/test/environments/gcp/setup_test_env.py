"""
Python script that automates the setup of a Google Cloud Platform test environment for the self-healing data pipeline.
It provisions GCP resources using Terraform, generates test data, and saves environment information for later use by tests and teardown processes.
"""

import os  # Operating system interfaces for file and path operations
import sys  # System-specific parameters and functions
import argparse  # Command-line argument parsing
import logging  # Logging facility for Python
import json  # JSON encoder and decoder
import subprocess  # Subprocess management for running Terraform commands
import time  # Time access and conversions
import uuid  # UUID generation for unique identifiers
import pathlib  # Object-oriented filesystem paths
import yaml  # YAML parser and emitter for configuration files

# Third-party library: pyyaml version: ^6.0
# Internal imports
from src.test.utils.gcp_test_utils import GCPTestContext, create_gcp_test_resource_path  # Utilities for GCP test environment setup and resource management
from src.backend.utils.auth.gcp_auth import get_default_credentials, get_service_account_credentials, get_project_id, get_gcp_location  # Authentication utilities for GCP services
from src.backend.utils.storage.gcs_client import GCSClient  # Client for interacting with Google Cloud Storage
from src.backend.utils.storage.bigquery_client import BigQueryClient  # Client for interacting with BigQuery
from src.test.utils.test_data_generators import generate_test_data, DataVolume  # Utilities for generating test data

# Global variables
TERRAFORM_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'terraform')
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
DEFAULT_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output.json')

# Initialize logger
logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Configures the logging system for the script"""
    # Configure logging format with timestamp, level, and message
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout
    )
    # Set the log level based on the provided parameter
    logger.setLevel(log_level.upper())
    # Add console handler for logging output
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)


def parse_arguments():
    """Parses command-line arguments for the script"""
    # Create ArgumentParser with description
    parser = argparse.ArgumentParser(description="Setup GCP test environment for self-healing pipeline.")

    # Add argument for configuration file path
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH, help="Path to the configuration file.")
    # Add argument for GCP project ID
    parser.add_argument("--project_id", type=str, help="GCP project ID.")
    # Add argument for GCP region
    parser.add_argument("--region", type=str, help="GCP region.")
    # Add argument for test environment ID
    parser.add_argument("--env_id", type=str, help="Test environment ID.")
    # Add argument for test data volume size
    parser.add_argument("--volume_size", type=str, default="small", help="Test data volume size (small, medium, large).")
    # Add argument for auto-destroy flag
    parser.add_argument("--auto_destroy", action="store_true", help="Enable auto-destroy of resources after TTL.")
    # Add argument for environment TTL in hours
    parser.add_argument("--ttl_hours", type=int, default=24, help="Environment TTL in hours.")
    # Add argument for output file path
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT_PATH, help="Path to the output file.")
    # Add argument for log level
    parser.add_argument("--log_level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    # Add argument for skipping Terraform execution
    parser.add_argument("--skip_terraform", action="store_true", help="Skip Terraform execution.")
    # Add argument for skipping test data generation
    parser.add_argument("--skip_data_generation", action="store_true", help="Skip test data generation.")

    # Parse and return arguments
    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """Loads configuration from a YAML file"""
    # Check if configuration file exists
    if not os.path.exists(config_path):
        logger.warning(f"Configuration file not found: {config_path}")
        return {}

    # Open and load YAML configuration file
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
        # Return the loaded configuration dictionary
        return config or {}


def merge_config_with_args(config: dict, args: argparse.Namespace) -> dict:
    """Merges configuration from file with command-line arguments"""
    # Create a copy of the configuration dictionary
    merged_config = config.copy()

    # Override configuration values with command-line arguments if provided
    if args.project_id:
        merged_config['gcp']['project_id'] = args.project_id
    if args.region:
        merged_config['gcp']['region'] = args.region
    if args.env_id:
        merged_config['env_id'] = args.env_id
    if args.volume_size:
        merged_config['test_data']['volume_size'] = args.volume_size
    merged_config['auto_destroy'] = args.auto_destroy
    merged_config['ttl_hours'] = args.ttl_hours

    # Generate a random environment ID if not provided
    if 'env_id' not in merged_config or not merged_config['env_id']:
        merged_config['env_id'] = str(uuid.uuid4())

    # Set default values for any missing configuration
    merged_config.setdefault('resource_prefix', 'test')
    merged_config.setdefault('labels', {'env': merged_config['env_id']})

    # Return the merged configuration
    return merged_config


def generate_terraform_vars(config: dict) -> dict:
    """Generates Terraform variables from configuration"""
    # Extract relevant configuration values
    project_id = config['gcp']['project_id']
    region = config['gcp']['region']
    env_id = config['env_id']
    resource_prefix = config['resource_prefix']
    labels = config['labels']
    network_name = f"{resource_prefix}-{env_id}-vpc"
    subnet_name = f"{resource_prefix}-{env_id}-subnet"
    service_account = f"{resource_prefix}-{env_id}-sa@{project_id}.iam.gserviceaccount.com"
    auto_destroy = config['auto_destroy']
    ttl_hours = config['ttl_hours']

    # Create a dictionary of Terraform variables
    tf_vars = {
        "project_id": project_id,
        "region": region,
        "env_id": env_id,
        "resource_prefix": resource_prefix,
        "labels": labels,
        "network_name": network_name,
        "subnet_name": subnet_name,
        "service_account": service_account,
        "auto_destroy": auto_destroy,
        "ttl_hours": ttl_hours
    }

    # Return the Terraform variables dictionary
    return tf_vars


def write_terraform_vars(tf_vars: dict, terraform_dir: str) -> str:
    """Writes Terraform variables to a tfvars file"""
    # Create the terraform.tfvars file path
    vars_file_path = os.path.join(terraform_dir, "terraform.tfvars")

    # Format each variable according to its type (string, number, bool, list, map)
    formatted_vars = []
    for key, value in tf_vars.items():
        if isinstance(value, str):
            formatted_vars.append(f'{key} = "{value}"')
        elif isinstance(value, (int, float)):
            formatted_vars.append(f'{key} = {value}')
        elif isinstance(value, bool):
            formatted_vars.append(f'{key} = {str(value).lower()}')
        elif isinstance(value, list):
            formatted_vars.append(f'{key} = [{", ".join(f"{item}" for item in value)}]')
        elif isinstance(value, dict):
            formatted_map = ", ".join(f'"{k}" = "{v}"' for k, v in value.items())
            formatted_vars.append(f'{key} = {{{formatted_map}}}')
        else:
            formatted_vars.append(f'{key} = null')

    # Write the formatted variables to the tfvars file
    with open(vars_file_path, "w") as vars_file:
        vars_file.write("\n".join(formatted_vars))

    # Return the path to the created file
    return vars_file_path


def run_terraform_init(terraform_dir: str) -> bool:
    """Initializes Terraform in the specified directory"""
    # Change to the Terraform directory
    original_dir = os.getcwd()
    os.chdir(terraform_dir)

    # Run 'terraform init' command
    command = ["terraform", "init"]
    process = subprocess.run(command, capture_output=True, text=True)

    # Change back to the original directory
    os.chdir(original_dir)

    # Check the return code of the command
    if process.returncode != 0:
        logger.error(f"Terraform init failed:\n{process.stderr}")
        # Return False if unsuccessful
        return False

    logger.info("Terraform initialized successfully.")
    # Return True if successful
    return True


class TerraformError(Exception):
    """Custom exception for Terraform execution errors"""

    def __init__(self, message: str, command: str, return_code: int, output: str):
        """Initialize the TerraformError exception"""
        super().__init__(message)
        self.message = message
        self.command = command
        self.return_code = return_code
        self.output = output

    def __str__(self):
        """String representation of the error"""
        return f"TerraformError: {self.message}\nCommand: {self.command}\nReturn Code: {self.return_code}\nOutput:\n{self.output}"


def run_terraform_apply(terraform_dir: str, vars_file: str) -> dict:
    """Applies Terraform configuration to provision resources"""
    # Change to the Terraform directory
    original_dir = os.getcwd()
    os.chdir(terraform_dir)

    # Run 'terraform apply' with auto-approve flag and vars file
    command = ["terraform", "apply", "-auto-approve", f"-var-file={vars_file}"]
    process = subprocess.run(command, capture_output=True, text=True)

    # Check the return code of the command
    if process.returncode != 0:
        os.chdir(original_dir)
        raise TerraformError(
            message="Terraform apply failed.",
            command=" ".join(command),
            return_code=process.returncode,
            output=process.stderr
        )

    # If successful, run 'terraform output -json' to get outputs
    command = ["terraform", "output", "-json"]
    output_process = subprocess.run(command, capture_output=True, text=True)

    # Change back to the original directory
    os.chdir(original_dir)

    # Check the return code of the command
    if output_process.returncode != 0:
        raise TerraformError(
            message="Terraform output failed.",
            command=" ".join(command),
            return_code=output_process.returncode,
            output=output_process.stderr
        )

    # Parse the JSON output into a dictionary
    try:
        tf_outputs = json.loads(output_process.stdout)
    except json.JSONDecodeError:
        raise TerraformError(
            message="Failed to decode Terraform output as JSON.",
            command=" ".join(command),
            return_code=output_process.returncode,
            output=output_process.stdout
        )

    # Return the outputs dictionary
    return tf_outputs


def generate_environment_info(config: dict, tf_outputs: dict) -> dict:
    """Generates environment information from configuration and Terraform outputs"""
    # Create a dictionary with basic environment information
    env_info = {
        "env_id": config['env_id'],
        "project_id": config['gcp']['project_id'],
        "region": config['gcp']['region'],
        "resource_prefix": config['resource_prefix']
    }

    # Add Terraform outputs (network, subnet, service account)
    env_info["network"] = tf_outputs["network_name"]["value"]
    env_info["subnet"] = tf_outputs["subnet_name"]["value"]
    env_info["service_account"] = tf_outputs["service_account_email"]["value"]

    # Add resource names and identifiers
    env_info["bucket_name"] = config['gcs']['bucket']
    env_info["dataset_id"] = config['bigquery']['dataset']

    # Add expiry time if auto-destroy is enabled
    if config['auto_destroy']:
        ttl_hours = config['ttl_hours']
        expiry_time = time.time() + ttl_hours * 3600
        env_info["expiry_time"] = expiry_time

    # Return the environment information dictionary
    return env_info


def save_environment_info(env_info: dict, output_path: str) -> bool:
    """Saves environment information to a JSON file"""
    # Create the directory for the output file if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write the environment information to the JSON file
    with open(output_path, "w") as output_file:
        json.dump(env_info, output_file, indent=4)

    logger.info(f"Environment information saved to: {output_path}")
    # Return True if successful
    return True


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""

    def __init__(self, message: str):
        """Initialize the ConfigurationError exception"""
        super().__init__(message)
        self.message = message


def generate_and_upload_test_data(env_info: dict, volume_size: str) -> bool:
    """Generates test data and uploads it to GCP resources"""
    # Convert volume_size string to DataVolume enum
    try:
        data_volume = DataVolume[volume_size.upper()]
    except KeyError:
        raise ConfigurationError(f"Invalid volume size: {volume_size}. Must be one of: {', '.join(DataVolume.__members__.keys())}")

    # Get service account credentials from environment info
    service_account_email = env_info['service_account']
    credentials, project_id = get_default_credentials()

    # Initialize GCS and BigQuery clients with credentials
    gcs_client = GCSClient(project_id=project_id, credentials=credentials)
    bq_client = BigQueryClient(project_id=project_id, credentials=credentials)

    # Extract bucket names and dataset IDs from environment info
    bucket_name = env_info['bucket_name']
    dataset_id = env_info['dataset_id']

    # Generate test data with specified volume size
    test_data = generate_test_data(volume=data_volume)

    # Upload generated data to GCS buckets
    for file_name, file_content in test_data['gcs_data'].items():
        gcs_client.upload_file(bucket_name=bucket_name, file_path=file_name, file_content=file_content)
        logger.info(f"Uploaded {file_name} to gs://{bucket_name}/{file_name}")

    # Load generated data to BigQuery tables
    for table_name, table_data in test_data['bq_data'].items():
        bq_client.load_data_into_bigquery(dataset_id=dataset_id, table_name=table_name, data=table_data)
        logger.info(f"Loaded data into {dataset_id}.{table_name}")

    # Log the data generation and upload process
    logger.info(f"Successfully generated and uploaded test data (volume: {volume_size})")
    # Return True if successful
    return True


def main():
    """Main function that orchestrates the test environment setup"""
    # Parse command-line arguments
    args = parse_arguments()

    # Setup logging with specified level
    setup_logging(args.log_level)

    # Load configuration from file
    config = load_config(args.config)

    # Merge configuration with command-line arguments
    config = merge_config_with_args(config, args)

    # Generate Terraform variables from configuration
    tf_vars = generate_terraform_vars(config)

    # Write Terraform variables to tfvars file
    vars_file = write_terraform_vars(tf_vars, TERRAFORM_DIR)

    # If not skipping Terraform, initialize and apply Terraform
    if not args.skip_terraform:
        if not run_terraform_init(TERRAFORM_DIR):
            return 1
        try:
            tf_outputs = run_terraform_apply(TERRAFORM_DIR, vars_file)
        except TerraformError as e:
            logger.error(str(e))
            return 1
    else:
        logger.info("Skipping Terraform execution.")
        tf_outputs = {}

    # Generate environment information from config and Terraform outputs
    env_info = generate_environment_info(config, tf_outputs)

    # Save environment information to output file
    save_environment_info(env_info, args.output)

    # If not skipping data generation, generate and upload test data
    if not args.skip_data_generation:
        try:
            generate_and_upload_test_data(env_info, config['test_data']['volume_size'])
        except ConfigurationError as e:
            logger.error(str(e))
            return 1
    else:
        logger.info("Skipping test data generation.")

    # Log success message with environment details
    logger.info(f"Test environment setup complete. Environment ID: {config['env_id']}, Project ID: {config['gcp']['project_id']}, Region: {config['gcp']['region']}")
    # Return exit code (0 for success, non-zero for failure)
    return 0


if __name__ == "__main__":
    sys.exit(main())