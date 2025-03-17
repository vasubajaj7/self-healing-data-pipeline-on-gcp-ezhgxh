import os  # Access environment variables and file paths
import sys  # Access command line arguments and Python path
import logging  # Configure logging for the load tests
import argparse  # Parse command line arguments for test configuration
from locust import events  # Locust event hooks for test lifecycle management
from locust import User  # Load testing framework for simulating user behavior
from locust import between  # Load testing framework for defining wait times
from tasks.ingestion_tasks import IngestionUser  # Import user class for testing ingestion API endpoints
from tasks.quality_tasks import QualityUser  # Import user class for testing quality API endpoints
from tasks.healing_tasks import HealingUser  # Import user class for testing self-healing API endpoints
from tasks.api_tasks import ApiUser  # Import user class for testing general API endpoints
from src.test.utils.api_test_utils import load_test_config  # Import utility for loading test configuration

# Initialize logger
logger = logging.getLogger(__name__)

# Default values for command line arguments
DEFAULT_HOST = "http://localhost:8000"
DEFAULT_USERS = 100
DEFAULT_SPAWN_RATE = 10
DEFAULT_RUN_TIME = "10m"
USER_DISTRIBUTION = {'api': 10, 'ingestion': 30, 'quality': 30, 'healing': 30}


def setup_logging():
    """Configure logging for the load test"""
    # Configure logging format and level
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)

    # Set up file handler for logging to file
    file_handler = logging.FileHandler('load_test.log')
    file_handler.setFormatter(logging.Formatter(log_format))

    # Set up console handler for logging to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def parse_arguments():
    """Parse command line arguments for test configuration"""
    # Create argument parser
    parser = argparse.ArgumentParser(description="Run load tests for the self-healing data pipeline.")

    # Add arguments for host, users, spawn rate, run time, and config file
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="Host URL for the API")
    parser.add_argument("--users", type=int, default=DEFAULT_USERS, help="Number of users to simulate")
    parser.add_argument("--spawn-rate", type=int, default=DEFAULT_SPAWN_RATE, help="Rate at which users are spawned")
    parser.add_argument("--run-time", type=str, default=DEFAULT_RUN_TIME, help="Duration of the load test (e.g., 10s, 5m, 1h)")
    parser.add_argument("--config-file", type=str, help="Path to the test configuration file")

    # Parse arguments from command line
    args = parser.parse_args()

    # Return parsed arguments
    return args


def load_config(args):
    """Load test configuration from file or environment variables"""
    # Check if config file is specified in arguments
    if args.config_file:
        try:
            # If config file exists, load configuration from file
            config = load_test_config(args.config_file)
            logger.info(f"Loaded configuration from file: {args.config_file}")
        except FileNotFoundError:
            logger.warning(f"Configuration file not found: {args.config_file}. Using default configuration.")
            config = {}
    else:
        # Otherwise, use default configuration
        config = {}

    # Override configuration with command line arguments
    if args.host:
        config['host'] = args.host
    if args.users:
        config['users'] = args.users
    if args.spawn_rate:
        config['spawn_rate'] = args.spawn_rate
    if args.run_time:
        config['run_time'] = args.run_time

    # Return final configuration
    return config


def on_test_start(environment):
    """Event handler for test start"""
    # Log test start message
    logger.info("Load test started")

    # Log test configuration details
    logger.info(f"Test configuration: {environment.test_start_time}")
    logger.info(f"Host: {environment.host}")
    logger.info(f"Number of users: {environment.runner.user_count}")
    logger.info(f"Spawn rate: {environment.runner.spawn_rate}")
    logger.info(f"Run time: {environment.options.run_time}")

    # Initialize any required resources for the test
    pass


def on_test_stop(environment):
    """Event handler for test stop"""
    # Log test completion message
    logger.info("Load test completed")

    # Log summary of test results
    logger.info(f"Total requests: {environment.runner.stats.total.num_requests}")
    logger.info(f"Total failures: {environment.runner.stats.total.fail_ratio}")

    # Clean up any resources created during the test
    pass


def main():
    """Main function to set up and run the load test"""
    # Set up logging
    setup_logging()

    # Parse command line arguments
    args = parse_arguments()

    # Load test configuration
    config = load_config(args)

    # Register event handlers
    events.test_start.add_listener(on_test_start)
    events.test_stop.add_listener(on_test_stop)

    # Configure Locust command line options
    locust_options = [
        __file__,
        "--headless",
        "--users", str(config.get('users', DEFAULT_USERS)),
        "--spawn-rate", str(config.get('spawn_rate', DEFAULT_SPAWN_RATE)),
        "--time", config.get('run_time', DEFAULT_RUN_TIME),
        "--host", config.get('host', DEFAULT_HOST),
        "--report-file", "locust_report.html",
        "--html", "locust_report.html",
        "--logfile", "locust.log",
        "--user-classes", "ApiUser,IngestionUser,QualityUser,HealingUser"
    ]

    # Start Locust programmatically if not running via command line
    if not os.getenv("LOCUST_MODE") == "commandline":
        from locust.main import main as locust_main
        sys.argv = locust_options
        locust_main()


class UserDistribution:
    """Class to manage the distribution of different user types in the load test"""

    def __init__(self, user_classes, distribution):
        """Initialize the UserDistribution with user classes and distribution"""
        # Store user classes dictionary
        self.user_classes = user_classes

        # Store distribution dictionary
        self.distribution = distribution

        # Validate that distribution percentages sum to 100
        total_percentage = sum(distribution.values())
        if total_percentage != 100:
            raise ValueError(f"Distribution percentages must sum to 100, but got {total_percentage}")

        # Validate that all user types in distribution exist in user_classes
        for user_type in distribution.keys():
            if user_type not in user_classes:
                raise ValueError(f"User type '{user_type}' in distribution does not exist in user_classes")

    def get_user_classes(self):
        """Get the list of user classes with their weights for Locust"""
        # Create list of tuples with user classes and their weights
        user_classes_with_weights = []
        for user_type, weight in self.distribution.items():
            user_classes_with_weights.append((self.user_classes[user_type], weight))

        # Return the list for Locust to use in user distribution
        return user_classes_with_weights


if __name__ == "__main__":
    main()