"""
Core configuration management module for the self-healing data pipeline.

This module provides functionality to load, access, and manage configuration settings
from various sources including YAML files, environment variables, and defaults.
It implements a singleton pattern to ensure consistent configuration across the application.
"""

import os
import sys
import yaml
import logging
from typing import Any, Dict, List, Optional, Union
from deepmerge import always_merger  # version 1.1.0+

from google.cloud import secretmanager  # version 2.15.0+

from backend.constants import (
    ENV_DEVELOPMENT,
    ENV_STAGING,
    ENV_PRODUCTION,
    DEFAULT_CONFIG_PATH,
    GCP_PROJECT_ID_ENV_VAR,
    GCP_LOCATION_ENV_VAR,
    GCP_DEFAULT_LOCATION,
    DEFAULT_LOG_LEVEL,
    DEFAULT_CONFIDENCE_THRESHOLD,
    SelfHealingMode
)

# Setup module logger
logger = logging.getLogger(__name__)

# Singleton instance
_config_instance = None


def get_config(config_path: Optional[str] = None) -> 'Config':
    """Returns the singleton Config instance, creating it if it doesn't exist.

    Args:
        config_path (str, optional): Path to the configuration file.
            Defaults to the value from constants.DEFAULT_CONFIG_PATH.

    Returns:
        Config: Singleton Config instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config(config_path or DEFAULT_CONFIG_PATH)
    return _config_instance


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """Loads configuration from a YAML file.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        dict: Configuration dictionary from YAML file or empty dict if file not found.
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Configuration file not found: {file_path}")
            return {}
            
        with open(file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
            return config or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration file {file_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Error loading configuration file {file_path}: {e}")
        return {}


def get_environment_config(environment: str, base_config_dir: str) -> Dict[str, Any]:
    """Loads environment-specific configuration based on current environment.

    Args:
        environment (str): Environment name (development, staging, production)
        base_config_dir (str): Base directory containing configuration files

    Returns:
        dict: Environment-specific configuration dictionary
    """
    # Construct the environment-specific config file path
    env_config_file = os.path.join(
        os.path.dirname(base_config_dir), 
        f"{environment}_config.yaml"
    )
    return load_yaml_config(env_config_file)


def resolve_secret(secret_name: str, project_id: str, version: str = "latest") -> Optional[str]:
    """Resolves a secret value from Google Cloud Secret Manager.

    Args:
        secret_name (str): Name of the secret
        project_id (str): Google Cloud project ID
        version (str, optional): Secret version. Defaults to "latest".

    Returns:
        str: Secret value or None if error occurs
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret {secret_name}: {e}")
        return None


class Config:
    """Configuration manager class that handles loading, merging, and accessing
    configuration from multiple sources.
    """

    def __init__(self, config_path: str):
        """Initializes the Config instance with the specified configuration path.

        Args:
            config_path (str): Path to the main configuration file
        """
        self._config = {}
        self._config_path = config_path
        self._environment = ENV_DEVELOPMENT
        self._initialized = False
        self._load_config()

    def _load_config(self) -> None:
        """Loads configuration from all sources and merges them."""
        # Load default configuration
        default_config = load_yaml_config(self._config_path)
        
        # Determine current environment
        self._environment = os.environ.get("APP_ENVIRONMENT", ENV_DEVELOPMENT)
        logger.info(f"Loading configuration for environment: {self._environment}")
        
        # Load environment-specific configuration
        env_config = get_environment_config(self._environment, self._config_path)
        
        # Load configuration from environment variables
        env_var_config = self._load_environment_variables()
        
        # Merge configurations with priority: env vars > env-specific > default
        config = {}
        always_merger.merge(config, default_config)
        always_merger.merge(config, env_config)
        always_merger.merge(config, env_var_config)
        
        # Resolve any secret references in the configuration
        config = self._resolve_secrets(config)
        
        self._config = config
        self._initialized = True
        logger.info("Configuration loaded successfully")

    def _load_environment_variables(self) -> Dict[str, Any]:
        """Loads configuration from environment variables.

        Returns:
            dict: Configuration from environment variables
        """
        config = {}
        
        # Process environment variables that match the pattern APP_*
        for key, value in os.environ.items():
            if key.startswith("APP_"):
                # Convert environment variable names to configuration keys
                # e.g., APP_DATABASE_HOST becomes database.host
                config_key = key[4:].lower().replace("__", ".").replace("_", ".")
                config_parts = config_key.split(".")
                
                # Build nested dictionary structure
                current = config
                for part in config_parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                
                # Set the value in the deepest level
                current[config_parts[-1]] = value
                
        return config

    def _resolve_secrets(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Resolves secret references in the configuration.

        Args:
            config_dict (dict): Configuration dictionary possibly containing secret references

        Returns:
            dict: Configuration with resolved secrets
        """
        # Recursively process the configuration dictionary
        if isinstance(config_dict, dict):
            for key, value in config_dict.items():
                if isinstance(value, (dict, list)):
                    config_dict[key] = self._resolve_secrets(value)
                elif isinstance(value, str) and value.startswith("secret://"):
                    # Extract secret name from reference
                    secret_name = value[9:]
                    project_id = self.get_gcp_project_id()
                    secret_value = resolve_secret(secret_name, project_id)
                    if secret_value:
                        config_dict[key] = secret_value
                    else:
                        logger.warning(f"Failed to resolve secret: {secret_name}")
        elif isinstance(config_dict, list):
            for i, item in enumerate(config_dict):
                config_dict[i] = self._resolve_secrets(item)
                
        return config_dict

    def get(self, key: str, default: Any = None) -> Any:
        """Gets a configuration value by key with optional default.

        Args:
            key (str): Configuration key (dot notation for nested values)
            default (Any, optional): Default value if key not found. Defaults to None.

        Returns:
            Any: Configuration value or default
        """
        if not self._initialized:
            self._load_config()
            
        # Handle nested keys with dot notation
        try:
            parts = key.split('.')
            result = self._config
            for part in parts:
                result = result[part]
            return result
        except (KeyError, TypeError):
            return default

    def get_environment(self) -> str:
        """Gets the current environment name.

        Returns:
            str: Environment name (development, staging, production)
        """
        return self._environment

    def is_development(self) -> bool:
        """Checks if the current environment is development.

        Returns:
            bool: True if in development environment
        """
        return self._environment == ENV_DEVELOPMENT

    def is_staging(self) -> bool:
        """Checks if the current environment is staging.

        Returns:
            bool: True if in staging environment
        """
        return self._environment == ENV_STAGING

    def is_production(self) -> bool:
        """Checks if the current environment is production.

        Returns:
            bool: True if in production environment
        """
        return self._environment == ENV_PRODUCTION

    def get_gcp_project_id(self) -> str:
        """Gets the Google Cloud project ID.

        Returns:
            str: GCP project ID
        """
        return self.get("gcp.project_id") or os.environ.get(GCP_PROJECT_ID_ENV_VAR)

    def get_gcp_location(self) -> str:
        """Gets the Google Cloud location/region.

        Returns:
            str: GCP location
        """
        return (
            self.get("gcp.location") or 
            os.environ.get(GCP_LOCATION_ENV_VAR) or 
            GCP_DEFAULT_LOCATION
        )

    def get_bigquery_dataset(self) -> str:
        """Gets the BigQuery dataset name.

        Returns:
            str: BigQuery dataset name
        """
        return self.get("bigquery.dataset")

    def get_gcs_bucket(self) -> str:
        """Gets the Google Cloud Storage bucket name.

        Returns:
            str: GCS bucket name
        """
        bucket = self.get("gcs.bucket")
        if not bucket:
            # Construct default bucket name from prefix and project ID
            prefix = self.get("gcs.bucket_prefix", "self-healing-pipeline")
            project_id = self.get_gcp_project_id()
            if project_id:
                bucket = f"{prefix}-{project_id}"
        return bucket

    def get_composer_environment(self) -> str:
        """Gets the Cloud Composer environment name.

        Returns:
            str: Cloud Composer environment name
        """
        return self.get("composer.environment")

    def get_self_healing_mode(self) -> SelfHealingMode:
        """Gets the self-healing operational mode.

        Returns:
            SelfHealingMode: Self-healing mode enum value
        """
        mode_str = self.get("self_healing.mode", SelfHealingMode.SEMI_AUTOMATIC.value)
        try:
            return SelfHealingMode(mode_str)
        except ValueError:
            logger.warning(f"Invalid self-healing mode: {mode_str}, using SEMI_AUTOMATIC")
            return SelfHealingMode.SEMI_AUTOMATIC

    def get_self_healing_confidence_threshold(self) -> float:
        """Gets the confidence threshold for self-healing actions.

        Returns:
            float: Confidence threshold value
        """
        return float(self.get("self_healing.confidence_threshold", DEFAULT_CONFIDENCE_THRESHOLD))

    def get_log_level(self) -> str:
        """Gets the configured logging level.

        Returns:
            str: Log level name (DEBUG, INFO, etc.)
        """
        return self.get("logging.level", DEFAULT_LOG_LEVEL)

    def reload(self) -> None:
        """Reloads the configuration from all sources."""
        self._initialized = False
        self._load_config()
        logger.info("Configuration reloaded")