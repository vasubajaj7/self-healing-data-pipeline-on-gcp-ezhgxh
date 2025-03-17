"""
Provides logging handlers for the self-healing data pipeline application.

This module implements various logging handlers including console, file, and cloud logging
handlers with appropriate formatters. It supports contextual logging through handler
wrappers and environment-specific configurations.
"""

import logging
import os
import sys
import typing
from logging.handlers import RotatingFileHandler

# google-cloud-logging 3.5.0+
from google.cloud import logging as cloud_logging

# Import environment constants
from ...constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING,
    ENV_PRODUCTION,
    DEFAULT_LOG_LEVEL
)

# Import configuration utility
from ...config import get_config

# Import custom formatters
from .log_formatter import (
    JsonFormatter,
    ColoredFormatter,
    StructuredFormatter,
    get_formatter_for_environment
)

# Default log format constants
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_FILE = "pipeline.log"
MAX_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5


def get_console_handler(log_level: str, environment: str) -> logging.Handler:
    """
    Creates and configures a console logging handler.

    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        environment: Execution environment (development, staging, production)

    Returns:
        Configured console handler
    """
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level or DEFAULT_LOG_LEVEL))

    # Get formatter based on environment
    formatter = get_formatter_for_environment(environment, DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    console_handler.setFormatter(formatter)

    return console_handler


def get_file_handler(
    log_level: str,
    log_dir: str,
    log_file: str,
    environment: str
) -> logging.Handler:
    """
    Creates and configures a rotating file logging handler.

    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        log_dir: Directory to store log files
        log_file: Log file name
        environment: Execution environment (development, staging, production)

    Returns:
        Configured file handler
    """
    # Use default values if not provided
    actual_log_level = log_level or DEFAULT_LOG_LEVEL
    actual_log_dir = log_dir or DEFAULT_LOG_DIR
    actual_log_file = log_file or DEFAULT_LOG_FILE

    # Create logs directory if it doesn't exist
    os.makedirs(actual_log_dir, exist_ok=True)

    # Full path to log file
    log_file_path = os.path.join(actual_log_dir, actual_log_file)

    # Create rotating file handler
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=MAX_LOG_FILE_SIZE,
        backupCount=BACKUP_COUNT
    )
    file_handler.setLevel(getattr(logging, actual_log_level))

    # Get formatter based on environment
    formatter = get_formatter_for_environment(environment, DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    file_handler.setFormatter(formatter)

    return file_handler


def get_cloud_logging_handler(log_level: str, project_id: str, log_name: str) -> typing.Optional[logging.Handler]:
    """
    Creates and configures a Google Cloud Logging handler.

    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        project_id: Google Cloud project ID
        log_name: Name for the log in Cloud Logging

    Returns:
        Configured cloud logging handler or None if not in cloud environment
    """
    # Return None if not running in cloud environment
    if not is_running_in_cloud():
        return None

    try:
        # Use default log level if not provided
        actual_log_level = log_level or DEFAULT_LOG_LEVEL

        # Get project ID from config if not provided
        actual_project_id = project_id or get_config().get_gcp_project_id()
        if not actual_project_id:
            return None

        # Use default log name if not provided
        actual_log_name = log_name or "self_healing_pipeline"

        # Create cloud logging client
        client = cloud_logging.Client(project=actual_project_id)

        # Create cloud logging handler
        cloud_handler = cloud_logging.handlers.CloudLoggingHandler(
            client,
            name=actual_log_name
        )
        cloud_handler.setLevel(getattr(logging, actual_log_level))

        return cloud_handler
    except Exception as e:
        # Log to stdout since proper logging might not be set up yet
        print(f"Error setting up cloud logging: {e}")
        return None


def create_default_handlers(log_level: str, environment: str) -> typing.List[logging.Handler]:
    """
    Creates a set of default handlers based on environment.

    Args:
        log_level: Logging level (DEBUG, INFO, etc.)
        environment: Execution environment (development, staging, production)

    Returns:
        List of configured logging handlers
    """
    # Use default log level if not provided
    actual_log_level = log_level or DEFAULT_LOG_LEVEL

    # Get current environment if not provided
    actual_environment = environment or get_config().get_environment()

    handlers = []

    # Console handler for all environments
    console_handler = get_console_handler(actual_log_level, actual_environment)
    handlers.append(console_handler)

    # File handler for development and staging environments
    if actual_environment in [ENV_DEVELOPMENT, ENV_STAGING]:
        file_handler = get_file_handler(actual_log_level, None, None, actual_environment)
        handlers.append(file_handler)

    # Cloud logging handler for staging and production environments
    if actual_environment in [ENV_STAGING, ENV_PRODUCTION]:
        cloud_handler = get_cloud_logging_handler(actual_log_level, None, None)
        if cloud_handler:
            handlers.append(cloud_handler)

    return handlers


def is_running_in_cloud() -> bool:
    """
    Determines if the application is running in Google Cloud environment.

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


class ContextualHandler(logging.Handler):
    """
    Wrapper for logging handlers that adds context information to log records.

    This handler delegates actual logging to a wrapped handler but enriches
    log records with contextual information before passing them on.
    """

    def __init__(self, handler: logging.Handler, include_context: bool = True):
        """
        Initializes the contextual handler wrapper.

        Args:
            handler: The handler to wrap
            include_context: Whether to include context information in logs
        """
        super().__init__()
        self.handler = handler
        self.include_context = include_context
        # Set level to match the wrapped handler
        self.setLevel(handler.level)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emits a log record with added context information.

        Args:
            record: The log record to emit
        """
        # Add context information if enabled
        if self.include_context:
            self.add_context_to_record(record)

        # Delegate to wrapped handler
        self.handler.emit(record)

    def setLevel(self, level: int) -> None:
        """
        Sets the logging level for both wrapper and wrapped handler.

        Args:
            level: The logging level to set
        """
        super().setLevel(level)
        self.handler.setLevel(level)

    def setFormatter(self, formatter: logging.Formatter) -> None:
        """
        Sets the formatter for the wrapped handler.

        Args:
            formatter: The formatter to set
        """
        self.handler.setFormatter(formatter)

    def flush(self) -> None:
        """Flushes the wrapped handler."""
        self.handler.flush()

    def close(self) -> None:
        """Closes the wrapped handler."""
        self.handler.close()
        super().close()

    def handle(self, record: logging.LogRecord) -> bool:
        """
        Handles the log record by delegating to the wrapped handler.

        Args:
            record: The log record to handle

        Returns:
            True if record was handled
        """
        if not self.filter(record):
            return False
        
        self.emit(record)
        return True

    def add_context_to_record(self, record: logging.LogRecord) -> None:
        """
        Adds context information to a log record.

        Args:
            record: The log record to update
        """
        try:
            # Import here to avoid circular imports
            from ..context.logger_context import get_context
            
            # Get current thread's logging context
            context = get_context()
            
            # Add context values to the record
            for key, value in context.items():
                if not hasattr(record, key):
                    setattr(record, key, value)
                    
        except (ImportError, Exception):
            # Silently fail if context module not available or error occurs
            pass