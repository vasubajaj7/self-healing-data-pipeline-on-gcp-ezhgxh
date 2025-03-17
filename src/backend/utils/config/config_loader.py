"""
Utility module for loading and managing configuration files for the self-healing data pipeline.
Provides functions to load configuration from YAML files, merge configurations from different
sources, and handle environment-specific configurations.
"""

import os
import yaml
import logging
import typing
from deepmerge import always_merger  # version 1.1.0+

from ...constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING,
    ENV_PRODUCTION,
    DEFAULT_CONFIG_PATH
)
from .environment import get_environment
from .secret_manager import get_secret, get_json_secret

# Configure logging
logger = logging.getLogger(__name__)

# Constants
CONFIG_FILE_EXTENSION = ".yaml"
SECRET_REFERENCE_PREFIX = "secret://"


class ConfigLoadError(Exception):
    """Exception raised for configuration loading errors."""
    
    def __init__(self, message: str, original_exception: Exception = None):
        """Initialize with error message and original exception.
        
        Args:
            message: Error message describing the issue
            original_exception: The original exception that caused this error
        """
        super().__init__(message)
        self.original_exception = original_exception
        logger.error(f"Configuration load error: {message}", 
                    exc_info=original_exception is not None)


def load_yaml_config(file_path: str) -> dict:
    """Loads configuration from a YAML file.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Configuration dictionary from YAML file or empty dict if file not found
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Configuration file not found: {file_path}")
            return {}
        
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
            return config or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {str(e)}")
        return {}
    except Exception as e:
        logger.error(f"Error loading configuration file {file_path}: {str(e)}")
        return {}


def load_environment_config(base_dir: str, environment: str = None) -> dict:
    """Loads environment-specific configuration based on current environment.
    
    Args:
        base_dir: Base directory containing configuration files
        environment: Environment name, or None to determine from get_environment()
        
    Returns:
        Environment-specific configuration dictionary
    """
    # If environment is not provided, determine it
    if not environment:
        environment = get_environment()
    
    # Construct environment-specific config file path
    env_config_path = os.path.join(base_dir, f"{environment}{CONFIG_FILE_EXTENSION}")
    
    # Load the environment-specific configuration
    return load_yaml_config(env_config_path)


def merge_configs(configs: typing.List[dict]) -> dict:
    """Merges multiple configuration dictionaries with later ones taking precedence.
    
    Args:
        configs: List of configuration dictionaries to merge
        
    Returns:
        Merged configuration dictionary
    """
    result = {}
    
    for config in configs:
        if config:  # Only merge non-empty configs
            result = always_merger.merge(result, config)
    
    return result


def load_config_hierarchy(base_config_path: str = None, environment: str = None) -> dict:
    """Loads and merges configuration from default and environment-specific files.
    
    Args:
        base_config_path: Path to the base configuration file
        environment: Environment name, or None to determine from get_environment()
        
    Returns:
        Merged configuration from all sources
    """
    # Use default config path if not provided
    if not base_config_path:
        base_config_path = DEFAULT_CONFIG_PATH
    
    # Load default configuration
    default_config = load_yaml_config(base_config_path)
    
    # Determine base directory for environment-specific config
    base_dir = get_config_directory(base_config_path)
    
    # If environment is not provided, determine it
    if not environment:
        environment = get_environment()
    
    # Load environment-specific configuration
    env_config = load_environment_config(base_dir, environment)
    
    # Merge configurations with environment taking precedence
    config = merge_configs([default_config, env_config])
    
    return config


def resolve_config_secrets(config: dict) -> dict:
    """Resolves secret references in configuration values.
    
    Args:
        config: Configuration dictionary with potential secret references
        
    Returns:
        Configuration with resolved secrets
    """
    if not config:
        return {}
    
    resolved_config = {}
    
    for key, value in config.items():
        if isinstance(value, dict):
            # Recursively process nested dictionaries
            resolved_config[key] = resolve_config_secrets(value)
        elif isinstance(value, list):
            # Process lists of values
            resolved_list = []
            for item in value:
                if isinstance(item, dict):
                    resolved_list.append(resolve_config_secrets(item))
                elif isinstance(item, str) and item.startswith(SECRET_REFERENCE_PREFIX):
                    secret_name = item[len(SECRET_REFERENCE_PREFIX):]
                    try:
                        resolved_list.append(get_secret(secret_name))
                    except Exception as e:
                        logger.error(f"Error retrieving secret '{secret_name}': {str(e)}")
                        resolved_list.append(item)  # Keep original reference if retrieval fails
                else:
                    resolved_list.append(item)
            resolved_config[key] = resolved_list
        elif isinstance(value, str) and value.startswith(SECRET_REFERENCE_PREFIX):
            # Resolve secret reference
            secret_name = value[len(SECRET_REFERENCE_PREFIX):]
            try:
                resolved_config[key] = get_secret(secret_name)
            except Exception as e:
                logger.error(f"Error retrieving secret '{secret_name}': {str(e)}")
                resolved_config[key] = value  # Keep original reference if retrieval fails
        else:
            # Keep value as is
            resolved_config[key] = value
    
    return resolved_config


def load_config_from_env_vars(prefix: str) -> dict:
    """Loads configuration from environment variables with a specific prefix.
    
    Args:
        prefix: Prefix for environment variables to include
        
    Returns:
        Configuration from environment variables
    """
    config = {}
    
    # Get all environment variables
    for key, value in os.environ.items():
        # Check if the variable starts with the prefix
        if key.startswith(prefix):
            # Remove prefix and convert to lowercase for config key
            config_key = key[len(prefix):].lower().replace("__", ".").replace("_", ".")
            
            # Split the key into parts for nested dict structure
            parts = config_key.split(".")
            
            # Build nested dictionary structure
            current = config
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
                
            # Set the value in the deepest level
            current[parts[-1]] = value
    
    return config


def get_config_directory(config_path: str) -> str:
    """Gets the directory containing configuration files.
    
    Args:
        config_path: Path to a configuration file or directory
        
    Returns:
        Directory path containing configuration files
    """
    if os.path.isfile(config_path):
        return os.path.dirname(config_path)
    elif os.path.isdir(config_path):
        return config_path
    else:
        # If path doesn't exist, assume it's a file path and return the directory
        return os.path.dirname(config_path)