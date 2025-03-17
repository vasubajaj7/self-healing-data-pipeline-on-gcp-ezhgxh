"""
Centralized logging configuration module for the self-healing data pipeline.

This module provides functionality to configure logging across different environments
(development, staging, production) with appropriate handlers and formatters.
It enables structured logging and integrates with Google Cloud Logging for
cloud environments to support better observability and monitoring.
"""

import logging
import os
import sys
import typing
import yaml
from typing import Dict, Any, Optional, List

from constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING,
    ENV_PRODUCTION,
    DEFAULT_LOG_LEVEL
)
from config import get_config
from utils.logging.log_formatter import (
    JsonFormatter,
    ColoredFormatter,
    StructuredFormatter,
    get_formatter_for_environment
)
from utils.logging.log_handler import (
    get_console_handler,
    get_file_handler,
    get_cloud_logging_handler,
    create_default_handlers
)

# Default configuration values
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILE = "pipeline.log"
LOG_CONFIG_FILE = "logging_config.yaml"
LOG_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', LOG_CONFIG_FILE)


def configure_logging(log_level: str = None, environment: str = None) -> None:
    """Configures the logging system based on environment and configuration settings.
    
    This is the main entry point for setting up logging in the application.
    It configures the root logger with the appropriate handlers and formatters
    based on the current environment and settings.
    
    Args:
        log_level: The logging level to use (DEBUG, INFO, WARNING, etc.)
                  If None, the level will be read from config or default to INFO
        environment: The deployment environment (development, staging, production)
                    If None, it will be determined automatically
    """
    # Get app configuration
    config = get_config()
    
    # Determine environment if not provided
    if not environment:
        environment = get_environment()
    
    # Set log level from parameter, config, or default
    if log_level is None:
        log_level = config.get('logging.level', DEFAULT_LOG_LEVEL)
    
    # Convert log level string to constant if needed
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        logging.warning(f"Invalid log level: {log_level}, defaulting to {DEFAULT_LOG_LEVEL}")
        numeric_level = getattr(logging, DEFAULT_LOG_LEVEL)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Try to load configuration from YAML
    config_dict = load_logging_config()
    
    # If we have a valid config file, use dictConfig
    if config_dict:
        try:
            logging.config.dictConfig(config_dict)
            logging.info(f"Logging configured from file: {LOG_CONFIG_PATH}")
            return
        except Exception as e:
            logging.warning(f"Error applying logging configuration from file: {e}")
            # Fall back to code-based configuration
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create appropriate handlers based on environment
    handlers = create_default_handlers(log_level, environment)
    
    # Add handlers to root logger
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Set propagation for root logger
    root_logger.propagate = False
    
    logging.info(f"Logging configured for environment: {environment}, level: {log_level}")


def load_logging_config() -> Optional[Dict[str, Any]]:
    """Loads logging configuration from YAML file if available.
    
    Returns:
        A dictionary with logging configuration or None if file not found
    """
    try:
        # Check if logging config file exists
        if not os.path.exists(LOG_CONFIG_PATH):
            return None
        
        # Load and parse YAML configuration
        with open(LOG_CONFIG_PATH, 'r') as config_file:
            config = yaml.safe_load(config_file)
            return config
    except Exception as e:
        # Handle file not found or invalid YAML
        logging.warning(f"Could not load logging configuration from {LOG_CONFIG_PATH}: {e}")
        return None


def get_environment() -> str:
    """Determines the current execution environment.
    
    Returns:
        The current environment (development, staging, or production)
    """
    # Try to get environment from application config
    try:
        config = get_config()
        environment = config.get_environment()
        if environment:
            return environment
    except Exception:
        pass
    
    # Fall back to environment variable
    environment = os.environ.get('APP_ENVIRONMENT')
    if environment:
        return environment
    
    # Default to development if not specified
    return ENV_DEVELOPMENT


def configure_file_logging(log_level: str = None, log_dir: str = None, log_file: str = None) -> logging.Handler:
    """Configures file-based logging with rotation.
    
    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        log_dir: Directory to store log files (defaults to "logs")
        log_file: Log file name (defaults to "pipeline.log")
        
    Returns:
        Configured logging handler for file output
    """
    # Use defaults if not specified
    if log_dir is None:
        log_dir = DEFAULT_LOG_DIR
    
    if log_file is None:
        log_file = DEFAULT_LOG_FILE
    
    # Get current environment
    environment = get_environment()
    
    # Create the file handler
    handler = get_file_handler(log_level, log_dir, log_file, environment)
    
    return handler


def configure_console_logging(log_level: str = None, environment: str = None) -> logging.Handler:
    """Configures console-based logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        environment: Deployment environment (development, staging, production)
        
    Returns:
        Configured logging handler for console output
    """
    # Get current environment if not specified
    if environment is None:
        environment = get_environment()
    
    # Create the console handler
    handler = get_console_handler(log_level, environment)
    
    return handler


def configure_cloud_logging(log_level: str = None, project_id: str = None) -> Optional[logging.Handler]:
    """Configures Google Cloud Logging integration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        project_id: Google Cloud project ID (if None, retrieved from config)
        
    Returns:
        Configured cloud logging handler or None if not in cloud environment
    """
    # Skip if not running in cloud
    if not is_running_in_cloud():
        return None
    
    # Get GCP project ID from parameter or config
    if project_id is None:
        try:
            config = get_config()
            project_id = config.get_gcp_project_id()
        except Exception:
            logging.warning("Failed to get GCP project ID from config")
            return None
    
    # Create the cloud logging handler
    handler = get_cloud_logging_handler(log_level, project_id, "self_healing_pipeline")
    
    return handler


def is_running_in_cloud() -> bool:
    """Determines if the application is running in Google Cloud environment.
    
    Returns:
        True if running in cloud environment, False otherwise
    """
    # Check for GCP-specific environment variables
    gcp_indicators = [
        'GOOGLE_CLOUD_PROJECT',
        'GCP_PROJECT',
        'GCLOUD_PROJECT',
        'GCP_PROJECT_ID',
        'GOOGLE_APPLICATION_CREDENTIALS'
    ]
    
    # Check for Kubernetes environment variables
    k8s_indicators = [
        'KUBERNETES_SERVICE_HOST',
        'KUBERNETES_PORT',
        'K8S_NODE_NAME'
    ]
    
    # Check if any cloud indicators are present
    for indicator in gcp_indicators + k8s_indicators:
        if os.environ.get(indicator):
            return True
    
    return False


def reset_logging() -> None:
    """Resets the logging configuration to default state.
    
    This is useful for testing or when reconfiguring logging at runtime.
    """
    # Get root logger
    root_logger = logging.getLogger()
    
    # Remove all handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Reset log level to default
    root_logger.setLevel(logging.INFO)
    
    # Add a NullHandler to prevent "No handlers found" warnings
    root_logger.addHandler(logging.NullHandler())