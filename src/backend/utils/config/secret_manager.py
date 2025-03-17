"""
Utility module for securely managing and accessing secrets in the self-healing data pipeline.
Provides functions to retrieve secrets from Google Cloud Secret Manager with environment-specific
handling and caching for performance optimization.
"""

import os
import json
import time
import typing

from google.cloud.secretmanager import SecretManagerServiceClient  # version 2.12.0+
from cachetools import TTLCache  # version 5.0.0+

from ...constants import ENV_DEVELOPMENT, ENV_PRODUCTION, GCP_PROJECT_ID_ENV_VAR
from ..config.environment import get_environment, is_development, is_production
from ..auth.gcp_auth import get_project_id, get_default_credentials, is_running_on_gcp
from ..retry.retry_decorator import retry
from ..logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Secret Manager client (singleton)
_secret_client = None

# Cache for secrets with timestamps
_secret_cache = {}

# Default TTL for cached secrets (5 minutes)
SECRET_CACHE_TTL_SECONDS = 300

# Default path for local secrets in development
LOCAL_SECRETS_PATH = os.environ.get('LOCAL_SECRETS_PATH', os.path.join(os.path.expanduser('~'), '.pipeline', 'secrets'))


class SecretManagerError(Exception):
    """Exception raised for Secret Manager access errors."""
    
    def __init__(self, message: str, original_exception: Exception = None):
        """Initialize with error message and original exception.
        
        Args:
            message: Error message describing the issue
            original_exception: The original exception that caused this error
        """
        super().__init__(message)
        self.original_exception = original_exception
        logger.error(f"Secret Manager Error: {message}", exc_info=original_exception is not None)


def get_secret_client() -> SecretManagerServiceClient:
    """Gets or creates a Secret Manager client.
    
    Returns:
        Secret Manager client
        
    Raises:
        SecretManagerError: If authentication fails
    """
    global _secret_client
    
    # Return existing client if already initialized
    if _secret_client is not None:
        return _secret_client
    
    try:
        # Get credentials from GCP auth utility
        credentials = get_default_credentials()
        
        # Create the Secret Manager client
        _secret_client = SecretManagerServiceClient(credentials=credentials)
        logger.debug("Secret Manager client initialized")
        
        return _secret_client
        
    except Exception as e:
        error_message = "Failed to initialize Secret Manager client"
        logger.error(f"{error_message}: {str(e)}")
        raise SecretManagerError(error_message, e)


@retry(max_attempts=3, strategy='exponential')
def get_secret(secret_name: str, version: str = "latest", use_cache: bool = True) -> str:
    """Retrieves a secret value from Secret Manager or local storage.
    
    Args:
        secret_name: Name of the secret to retrieve
        version: Version of the secret (default: "latest")
        use_cache: Whether to use caching for this request
        
    Returns:
        Secret value as string
        
    Raises:
        SecretManagerError: If the secret cannot be accessed
    """
    # Check if secret is in cache and caching is enabled
    if use_cache and secret_name in _secret_cache:
        cache_entry = _secret_cache[secret_name]
        # Check if cache entry is still valid
        current_time = time.time()
        if current_time - cache_entry['timestamp'] < SECRET_CACHE_TTL_SECONDS:
            logger.debug(f"Using cached value for secret: {secret_name}")
            return cache_entry['value']
    
    # In development, try to load from local file if not running on GCP
    if is_development() and not is_running_on_gcp():
        logger.debug(f"Attempting to load secret from local storage: {secret_name}")
        local_secret = load_local_secret(secret_name)
        if local_secret is not None:
            if use_cache:
                # Cache the local secret
                _secret_cache[secret_name] = {
                    'value': local_secret,
                    'timestamp': time.time()
                }
            return local_secret
    
    try:
        # Get project ID for constructing secret name
        project_id = get_project_id()
        
        # Get Secret Manager client
        client = get_secret_client()
        
        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
        
        # Access the secret version
        response = client.access_secret_version(request={"name": name})
        
        # Get the secret payload
        payload = response.payload.data.decode("UTF-8")
        
        # Cache the secret if caching is enabled
        if use_cache:
            _secret_cache[secret_name] = {
                'value': payload,
                'timestamp': time.time()
            }
        
        logger.debug(f"Successfully retrieved secret: {secret_name}")
        return payload
        
    except Exception as e:
        error_message = f"Failed to access secret: {secret_name}"
        logger.error(f"{error_message}: {str(e)}")
        raise SecretManagerError(error_message, e)


def get_json_secret(secret_name: str, version: str = "latest", use_cache: bool = True) -> dict:
    """Retrieves a JSON-formatted secret and parses it.
    
    Args:
        secret_name: Name of the secret to retrieve
        version: Version of the secret (default: "latest")
        use_cache: Whether to use caching for this request
        
    Returns:
        Parsed JSON secret as dictionary
        
    Raises:
        SecretManagerError: If the secret cannot be accessed or is not valid JSON
    """
    try:
        # Get the secret as a string
        secret_str = get_secret(secret_name, version, use_cache)
        
        # Parse as JSON
        return json.loads(secret_str)
        
    except json.JSONDecodeError as e:
        error_message = f"Failed to parse secret as JSON: {secret_name}"
        logger.error(f"{error_message}: {str(e)}")
        raise SecretManagerError(error_message, e)


def get_latest_secret_version(secret_name: str) -> str:
    """Gets the latest version of a secret.
    
    Args:
        secret_name: Name of the secret
        
    Returns:
        Latest version string
        
    Raises:
        SecretManagerError: If the versions cannot be retrieved
    """
    try:
        # Get project ID
        project_id = get_project_id()
        
        # Get Secret Manager client
        client = get_secret_client()
        
        # List versions of the secret
        parent = f"projects/{project_id}/secrets/{secret_name}"
        response = client.list_secret_versions(request={"parent": parent})
        
        # Find the latest enabled version
        latest_version = None
        for version in response:
            if version.state.name == 'ENABLED' and (latest_version is None or version.create_time > latest_version.create_time):
                latest_version = version
        
        if latest_version is None:
            raise SecretManagerError(f"No enabled versions found for secret: {secret_name}")
        
        # Extract version number from name
        # Name format: projects/*/secrets/*/versions/{version}
        version_name = latest_version.name
        version_id = version_name.split('/')[-1]
        
        return version_id
        
    except Exception as e:
        error_message = f"Failed to get latest version for secret: {secret_name}"
        logger.error(f"{error_message}: {str(e)}")
        raise SecretManagerError(error_message, e)


def clear_secret_cache(secret_name: str = None) -> None:
    """Clears the secret cache.
    
    Args:
        secret_name: If provided, only clears the specified secret from cache
    """
    global _secret_cache
    
    if secret_name:
        if secret_name in _secret_cache:
            del _secret_cache[secret_name]
            logger.debug(f"Cleared cache for secret: {secret_name}")
    else:
        # Clear all cached secrets
        _secret_cache.clear()
        logger.debug("Cleared entire secret cache")


def store_local_secret(secret_name: str, secret_value: str) -> bool:
    """Stores a secret locally for development use.
    
    Args:
        secret_name: Name of the secret
        secret_value: Value of the secret
        
    Returns:
        True if successful, False otherwise
    """
    # Only allow local secrets in development environment
    if not is_development():
        logger.warning("Attempted to store local secret in non-development environment")
        return False
    
    try:
        # Ensure secrets directory exists
        os.makedirs(LOCAL_SECRETS_PATH, exist_ok=True)
        
        # Construct path for the secret file
        secret_path = os.path.join(LOCAL_SECRETS_PATH, secret_name)
        
        # Write the secret value to the file
        with open(secret_path, 'w') as secret_file:
            secret_file.write(secret_value)
        
        # Set secure permissions (readable only by owner)
        try:
            os.chmod(secret_path, 0o600)
        except Exception as perm_error:
            # This might fail on Windows or other platforms
            logger.warning(f"Could not set file permissions for {secret_path}: {str(perm_error)}")
        
        logger.debug(f"Stored local secret: {secret_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store local secret {secret_name}: {str(e)}")
        return False


def load_local_secret(secret_name: str) -> typing.Optional[str]:
    """Loads a secret from local storage for development use.
    
    Args:
        secret_name: Name of the secret
        
    Returns:
        Secret value or None if not found
    """
    try:
        # Construct path for the secret file
        secret_path = os.path.join(LOCAL_SECRETS_PATH, secret_name)
        
        # Check if file exists
        if not os.path.exists(secret_path):
            logger.debug(f"Local secret not found: {secret_name}")
            return None
        
        # Read the secret value from the file
        with open(secret_path, 'r') as secret_file:
            secret_value = secret_file.read()
        
        logger.debug(f"Loaded local secret: {secret_name}")
        return secret_value
        
    except Exception as e:
        logger.error(f"Failed to load local secret {secret_name}: {str(e)}")
        return None