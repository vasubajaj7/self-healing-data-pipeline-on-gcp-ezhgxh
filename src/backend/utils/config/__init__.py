"""
Configuration utilities package for the self-healing data pipeline.

This package provides comprehensive utilities for managing configuration across
different environments, including:

- Loading configuration from YAML files
- Environment-specific configuration management
- Secret management with Google Cloud Secret Manager
- Configuration merging and hierarchy support
- Environment detection and validation

These utilities ensure consistent configuration access throughout the pipeline
while handling environment-specific settings and secure credential management.
"""

from .config_loader import (
    load_yaml_config,
    load_environment_config,
    merge_configs,
    load_config_hierarchy,
    resolve_config_secrets,
    load_config_from_env_vars,
    get_config_directory,
    ConfigLoadError
)

from .environment import (
    get_environment,
    set_environment,
    reset_environment,
    is_development,
    is_staging,
    is_production,
    EnvironmentError
)

from .secret_manager import (
    get_secret,
    get_json_secret,
    get_latest_secret_version,
    clear_secret_cache,
    store_local_secret,
    SecretManagerError
)

# Define public API
__all__ = [
    "load_yaml_config",
    "load_environment_config",
    "merge_configs",
    "load_config_hierarchy",
    "resolve_config_secrets",
    "load_config_from_env_vars",
    "get_config_directory",
    "ConfigLoadError",
    "get_environment",
    "set_environment",
    "reset_environment",
    "is_development",
    "is_staging",
    "is_production",
    "EnvironmentError",
    "get_secret",
    "get_json_secret",
    "get_latest_secret_version",
    "clear_secret_cache",
    "store_local_secret",
    "SecretManagerError"
]