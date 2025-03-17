"""
Utility module for determining and managing the current execution environment
(development, staging, production) for the self-healing data pipeline.

This module provides functions to detect the environment from various sources
(environment variables, GCP metadata) and check environment-specific conditions.
"""

import os
import logging
from typing import Optional

from src.backend.constants import ENV_DEVELOPMENT, ENV_STAGING, ENV_PRODUCTION

# Configure logging
logger = logging.getLogger(__name__)

# Environment variable name used to explicitly set the environment
ENV_VAR_NAME = "PIPELINE_ENVIRONMENT"

# Cache for the current environment
_current_environment = None


class EnvironmentError(Exception):
    """Exception raised for environment-related errors."""
    
    def __init__(self, message: str):
        """Initialize with error message.
        
        Args:
            message: The error message describing the environment issue
        """
        super().__init__(message)
        logger.error(f"Environment error: {message}")


def validate_environment(environment: str) -> bool:
    """Validates that an environment name is one of the allowed values.
    
    Args:
        environment: The environment name to validate
        
    Returns:
        True if environment is valid, False otherwise
    """
    valid_environments = [ENV_DEVELOPMENT, ENV_STAGING, ENV_PRODUCTION]
    
    if environment not in valid_environments:
        logger.warning(f"Invalid environment: '{environment}'. "
                      f"Must be one of {valid_environments}")
        return False
    
    return True


def detect_gcp_environment() -> Optional[str]:
    """Attempts to detect environment from GCP metadata.
    
    Returns:
        Detected environment or None if not detectable
    """
    try:
        # Try to access GCP metadata to determine if running on GCP
        import requests
        
        # GCP metadata server endpoint
        metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/labels/environment"
        
        # Request with required header for GCP metadata server
        headers = {"Metadata-Flavor": "Google"}
        
        # Set a short timeout to avoid hanging if not on GCP
        response = requests.get(metadata_url, headers=headers, timeout=1)
        
        if response.status_code == 200:
            env_label = response.text.lower()
            
            # Map potential environment labels to our constants
            if env_label == "prod":
                return ENV_PRODUCTION
            elif env_label == "staging" or env_label == "stage":
                return ENV_STAGING
            elif env_label == "dev":
                return ENV_DEVELOPMENT
            elif validate_environment(env_label):
                return env_label
                
        logger.debug("No environment found in GCP metadata")
        return None
        
    except Exception as e:
        # Handle any exceptions when trying to access metadata
        # This could happen if not running on GCP or no network access
        logger.debug(f"Failed to detect GCP environment: {str(e)}")
        return None


def get_environment() -> str:
    """Determines the current execution environment.
    
    Returns:
        Current environment name (development, staging, or production)
    """
    global _current_environment
    
    # Return cached environment if already determined
    if _current_environment is not None:
        return _current_environment
    
    # Try to get environment from environment variable
    env = os.environ.get(ENV_VAR_NAME)
    
    # If environment variable exists and is valid, use it
    if env and validate_environment(env):
        _current_environment = env
        logger.info(f"Environment set from environment variable: {env}")
        return _current_environment
    
    # Try to detect from GCP metadata
    env = detect_gcp_environment()
    if env:
        _current_environment = env
        logger.info(f"Environment detected from GCP metadata: {env}")
        return _current_environment
    
    # Default to development environment if not determined
    _current_environment = ENV_DEVELOPMENT
    logger.info(f"Environment defaulting to: {_current_environment}")
    
    return _current_environment


def set_environment(environment: str) -> None:
    """Explicitly sets the current environment (mainly for testing).
    
    Args:
        environment: The environment name to set
        
    Raises:
        EnvironmentError: If the provided environment is invalid
    """
    global _current_environment
    
    if not validate_environment(environment):
        raise EnvironmentError(f"Invalid environment: {environment}")
    
    _current_environment = environment
    logger.info(f"Environment explicitly set to: {environment}")


def reset_environment() -> None:
    """Resets the cached environment value, forcing re-detection.
    
    This is useful for testing or when environment conditions might have changed.
    """
    global _current_environment
    
    _current_environment = None
    logger.debug("Environment cache reset")


def is_development() -> bool:
    """Checks if the current environment is development.
    
    Returns:
        True if current environment is development, False otherwise
    """
    return get_environment() == ENV_DEVELOPMENT


def is_staging() -> bool:
    """Checks if the current environment is staging.
    
    Returns:
        True if current environment is staging, False otherwise
    """
    return get_environment() == ENV_STAGING


def is_production() -> bool:
    """Checks if the current environment is production.
    
    Returns:
        True if current environment is production, False otherwise
    """
    return get_environment() == ENV_PRODUCTION