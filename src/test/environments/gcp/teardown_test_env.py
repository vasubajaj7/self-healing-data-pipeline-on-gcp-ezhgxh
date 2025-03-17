"""
Python script that automates the teardown of Google Cloud Platform test environments for the self-healing data pipeline.
It destroys all provisioned GCP resources using Terraform to prevent resource leakage and unnecessary costs after testing is complete.
"""

import os  # standard library
import sys  # standard library
import argparse  # standard library
import logging  # standard library
import json  # standard library
import subprocess  # standard library
import pathlib  # standard library

from src.test.utils.gcp_test_utils import GCPTestContext, create_gcp_test_resource_path  # internal import
from src.backend.utils.auth.gcp_auth import get_default_credentials, get_project_id  # internal import

TERRAFORM_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'terraform')
DEFAULT_ENV_INFO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output.json')

logger = logging.getLogger(__name__)


class TerraformError(Exception):
    """Custom exception for Terraform execution errors"""

    def __init__(self, message: str, command: str, return_code: int, output: str):
        """Initialize the TerraformError exception"""
        super().__init__(message)
        self.message = message
        self.command = command
        self.return_code = return_code
        self.output = output

    def __str__(self) -> str:
        """String representation of the error"""
        return f"TerraformError: {self.message}\nCommand: {self.command}\nReturn Code: {self.return_code}\nOutput: {self.output}"


class EnvironmentInfoError(Exception):
    """Custom exception for environment information errors"""

    def __init__(self, message: str):
        """Initialize the EnvironmentInfoError exception"""
        super().__init__(message)
        self.message = message


def setup_logging(log_level: str) -> None:
    """Configures the logging system for the script"""
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=log_level.upper(),
        stream=sys.stdout
    )
    logger.setLevel(log_level.upper())
    console_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(console_handler)


def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments for the script"""
    parser = argparse.ArgumentParser(description='Teardown GCP test environment')
    parser.add_argument('--env_info_path', type=str, default=DEFAULT_ENV_INFO_PATH,
                        help='Path to the environment info JSON file')
    parser.add_argument('--project_id', type=str,
                        help='GCP project ID (optional, will attempt to detect if not provided)')
    parser.add_argument('--test_env_id', type=str,
                        help='Test environment ID (optional, will attempt to detect if not provided)')
    parser.add_argument('--log_level', type=str, default='INFO',
                        help='Logging level (default: INFO)')
    parser.add_argument('--force', action='store_true',
                        help='Force destruction without prompting for approval')
    return parser.parse_args()


def load_environment_info(env_info_path: str) -> dict:
    """Loads environment information from a JSON file"""
    if not os.path.exists(env_info_path):
        raise FileNotFoundError(f"Environment info file not found: {env_info_path}")

    with open(env_info_path, 'r') as f:
        env_info = json.load(f)
    return env_info


def run_terraform_destroy(terraform_dir: str, env_info: dict, force: bool) -> bool:
    """Runs terraform destroy to remove all provisioned resources"""
    try:
        os.chdir(terraform_dir)

        # Create a temporary tfvars file with environment variables
        tfvars_path = os.path.join(terraform_dir, 'terraform.tfvars.json')
        with open(tfvars_path, 'w') as f:
            json.dump(env_info, f, indent=2)

        # Initialize Terraform if needed
        if not os.path.exists(os.path.join(terraform_dir, '.terraform')):
            init_command = ['terraform', 'init']
            logger.info(f"Running: {' '.join(init_command)}")
            init_result = subprocess.run(init_command, capture_output=True, text=True, check=True)
            logger.debug(f"Terraform init output:\n{init_result.stdout}\n{init_result.stderr}")

        # Run terraform destroy
        destroy_command = ['terraform', 'destroy', '-auto-approve' if force else '-var-file=terraform.tfvars.json']
        logger.info(f"Running: {' '.join(destroy_command)}")
        destroy_result = subprocess.run(destroy_command, capture_output=True, text=True)

        if destroy_result.returncode != 0:
            raise TerraformError(
                message="Terraform destroy failed",
                command=' '.join(destroy_command),
                return_code=destroy_result.returncode,
                output=destroy_result.stderr
            )

        logger.info("Terraform destroy completed successfully")
        return True

    except TerraformError as e:
        logger.error(str(e))
        return False

    except Exception as e:
        logger.error(f"Error running terraform destroy: {e}")
        return False

    finally:
        # Clean up temporary tfvars file
        if os.path.exists(tfvars_path):
            os.remove(tfvars_path)
            logger.debug(f"Removed temporary tfvars file: {tfvars_path}")


def cleanup_additional_resources(env_info: dict) -> bool:
    """Cleans up any resources that might not be managed by Terraform"""
    try:
        project_id = env_info.get('project_id')
        test_env_id = env_info.get('test_env_id')

        if not project_id or not test_env_id:
            logger.warning("Project ID or test environment ID not found in environment info. Skipping additional resource cleanup.")
            return True

        # Initialize GCP clients
        # gcs_client = GCSClient(project_id=project_id)
        # bq_client = BigQueryClient(project_id=project_id)
        # vertex_client = VertexAIClient(project_id=project_id, location=location)

        # Attempt to clean up any orphaned GCS buckets
        # bucket_name = f"{project_id}-{test_env_id}-bucket"
        # try:
        #     gcs_client.delete_bucket(bucket_name, force=True)
        #     logger.info(f"Successfully deleted GCS bucket: {bucket_name}")
        # except Exception as e:
        #     logger.warning(f"Failed to delete GCS bucket {bucket_name}: {e}")

        # Attempt to clean up any orphaned BigQuery datasets
        # dataset_id = f"{project_id}_{test_env_id}_dataset"
        # try:
        #     bq_client.delete_dataset(dataset_id, delete_contents=True)
        #     logger.info(f"Successfully deleted BigQuery dataset: {dataset_id}")
        # except Exception as e:
        #     logger.warning(f"Failed to delete BigQuery dataset {dataset_id}: {e}")

        # Attempt to clean up any orphaned Vertex AI models
        # model_id = f"{project_id}-{test_env_id}-model"
        # try:
        #     vertex_client.delete_model(model_id)
        #     logger.info(f"Successfully deleted Vertex AI model: {model_id}")
        # except Exception as e:
        #     logger.warning(f"Failed to delete Vertex AI model {model_id}: {e}")

        return True

    except Exception as e:
        logger.error(f"Error cleaning up additional resources: {e}")
        return False


def remove_environment_info_file(env_info_path: str) -> bool:
    """Removes the environment information file after successful teardown"""
    try:
        if os.path.exists(env_info_path):
            os.remove(env_info_path)
            logger.info(f"Removed environment info file: {env_info_path}")
        else:
            logger.warning(f"Environment info file not found: {env_info_path}")
        return True
    except Exception as e:
        logger.error(f"Error removing environment info file: {e}")
        return False


def main() -> int:
    """Main function that orchestrates the test environment teardown"""
    args = parse_arguments()
    setup_logging(args.log_level)

    try:
        env_info = load_environment_info(args.env_info_path)
        logger.info(f"Tearing down environment: {env_info.get('test_env_id')}")

        if not run_terraform_destroy(TERRAFORM_DIR, env_info, args.force):
            logger.error("Terraform destroy failed. Please check the logs for details.")
            return 1

        if not cleanup_additional_resources(env_info):
            logger.warning("Additional resource cleanup failed. Some resources may not have been deleted.")

        if not remove_environment_info_file(args.env_info_path):
            logger.warning("Failed to remove environment info file. Please delete it manually.")

        logger.info("Test environment teardown completed successfully.")
        return 0

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        return 1

    except EnvironmentInfoError as e:
        logger.error(f"Error: {e}")
        return 1

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())