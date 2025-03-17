import os
import sys
import argparse
import logging

from fastapi import FastAPI

from .config import get_config
from .constants import ENV_DEVELOPMENT
from .logging_config import configure_logging
from .utils.logging.logger import get_logger
from .api.app import create_app

# Initialize logger
logger = get_logger(__name__)

# Global variables
app = None
config = get_config()
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000

def parse_arguments():
    """Parse command line arguments for the application"""
    parser = argparse.ArgumentParser(description="Self-Healing Data Pipeline API")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="Host for the API")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port for the API")
    parser.add_argument("--config_path", type=str, help="Path to the configuration file")
    parser.add_argument("--log_level", type=str, help="Log level for the application")
    parser.add_argument("--environment", type=str, help="Execution environment")
    return parser.parse_args()

def initialize_app(args):
    """Initialize the application with configuration and logging"""
    # Set up logging with provided log level and environment
    environment = get_environment(args.environment)
    configure_logging(log_level=args.log_level, environment=environment)
    logger.info("Initializing application")

    # Create and configure the FastAPI application
    fast_app = create_app()

    # Set global app variable
    global app
    app = fast_app

    # Return the configured application
    return fast_app

def get_environment(env_arg):
    """Determine the current execution environment"""
    # If env_arg is provided, use it
    if env_arg:
        return env_arg

    # Otherwise try to get environment from config
    try:
        config = get_config()
        environment = config.get_environment()
        if environment:
            return environment
    except Exception:
        pass

    # Fall back to environment variable if config not available
    environment = os.environ.get("APP_ENVIRONMENT")
    if environment:
        return environment

    # Default to development if not specified
    return ENV_DEVELOPMENT

def main():
    """Main entry point for the application"""
    try:
        # Parse command line arguments
        args = parse_arguments()

        # Initialize the application
        fast_app = initialize_app(args)

        # Get host and port from arguments or configuration
        host = args.host or config.get("api.host", DEFAULT_HOST)
        port = args.port or int(config.get("api.port", DEFAULT_PORT))

        # Log application startup information
        logger.info(f"Starting application on {host}:{port}")

        # If running directly (not imported), start the application
        if __name__ == "__main__":
            import uvicorn  # version 2.0.0+
            uvicorn.run(fast_app, host=host, port=port)

    except Exception as e:
        # Handle any exceptions during startup
        logger.error(f"Application startup failed: {e}")
        sys.exit(1)  # Exit with an error code

# Run the main function
if __name__ == "__main__":
    main()