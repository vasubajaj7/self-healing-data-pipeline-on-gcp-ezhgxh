"""
Provides utilities for Google Cloud Platform authentication and credential management for the self-healing data pipeline.
Handles service account authentication, default credentials, and environment-specific authentication strategies.
"""

import os
import json
import typing

# Google Auth v2.6.0+
import google.auth
import google.auth.transport.requests
import google.oauth2.service_account
import google.cloud.iam_credentials

from ...constants import (
    ENV_DEVELOPMENT,
    ENV_PRODUCTION,
    GCP_PROJECT_ID_ENV_VAR,
    GCP_LOCATION_ENV_VAR,
    GCP_DEFAULT_LOCATION
)
from ..logging.logger import get_logger
from ..config.environment import (
    get_environment,
    is_development,
    is_production
)
from ..retry.retry_decorator import retry

# Initialize logger
logger = get_logger(__name__)

# Cached credentials
_default_credentials = None
_service_account_credentials = {}

# Default OAuth scopes for GCP authentication
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class GCPAuthError(Exception):
    """Exception raised for GCP authentication errors."""
    
    def __init__(self, message: str, original_exception: Exception = None):
        """Initialize with error message and original exception.
        
        Args:
            message: Error message describing the authentication failure
            original_exception: The original exception that caused this error
        """
        super().__init__(message)
        self.original_exception = original_exception
        logger.error(f"GCP Authentication Error: {message}", exc_info=original_exception is not None)


@retry(max_attempts=3, strategy='exponential')
def get_default_credentials(scopes: typing.List[str] = None) -> google.auth.credentials.Credentials:
    """Gets the default Google Cloud credentials for the current environment.
    
    Args:
        scopes: OAuth scopes to apply to the credentials
        
    Returns:
        Google Cloud credentials
        
    Raises:
        GCPAuthError: If unable to get default credentials
    """
    global _default_credentials
    
    # Return cached credentials if available
    if _default_credentials is not None:
        # Apply scopes if specified
        if scopes:
            return _default_credentials.with_scopes(scopes)
        return _default_credentials
    
    try:
        logger.debug("Getting default credentials")
        credentials, project_id = google.auth.default(scopes=scopes or DEFAULT_SCOPES)
        
        # Cache the credentials
        _default_credentials = credentials
        
        logger.info(f"Successfully obtained default credentials for project: {project_id}")
        return credentials
    
    except Exception as e:
        error_message = "Failed to get default GCP credentials"
        logger.error(f"{error_message}: {str(e)}")
        raise GCPAuthError(error_message, e)


@retry(max_attempts=3, strategy='exponential')
def get_service_account_credentials(
    service_account_file: str, 
    scopes: typing.List[str] = None
) -> google.oauth2.service_account.Credentials:
    """Gets credentials for a specific service account.
    
    Args:
        service_account_file: Path to service account key file
        scopes: OAuth scopes to apply to the credentials
        
    Returns:
        Service account credentials
        
    Raises:
        GCPAuthError: If unable to get service account credentials
    """
    global _service_account_credentials
    
    # Return cached credentials if available
    if service_account_file in _service_account_credentials:
        credentials = _service_account_credentials[service_account_file]
        # Apply scopes if specified
        if scopes:
            return credentials.with_scopes(scopes)
        return credentials
    
    try:
        logger.debug(f"Loading service account credentials from {service_account_file}")
        
        if not os.path.exists(service_account_file):
            raise FileNotFoundError(f"Service account file not found: {service_account_file}")
        
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes or DEFAULT_SCOPES
        )
        
        # Cache the credentials
        _service_account_credentials[service_account_file] = credentials
        
        logger.info(f"Successfully loaded service account credentials from {service_account_file}")
        return credentials
    
    except Exception as e:
        error_message = f"Failed to get service account credentials from {service_account_file}"
        logger.error(f"{error_message}: {str(e)}")
        raise GCPAuthError(error_message, e)


def get_credentials_for_service(
    service_name: str, 
    scopes: typing.List[str] = None
) -> google.auth.credentials.Credentials:
    """Gets appropriate credentials for a specific service based on environment.
    
    Args:
        service_name: Name of the service requiring authentication
        scopes: OAuth scopes to apply to the credentials
        
    Returns:
        Appropriate credentials for the service
        
    Raises:
        GCPAuthError: If unable to get credentials
    """
    try:
        # Use environment-specific authentication strategy
        env = get_environment()
        
        if is_development():
            # In development, try to use service-specific credentials file
            service_account_env_var = f"GCP_SERVICE_ACCOUNT_{service_name.upper()}"
            service_account_file = os.environ.get(service_account_env_var)
            
            if service_account_file and os.path.exists(service_account_file):
                logger.debug(f"Using service-specific credentials for {service_name}")
                return get_service_account_credentials(service_account_file, scopes)
        
        # Default to application default credentials
        logger.debug(f"Using default credentials for {service_name}")
        return get_default_credentials(scopes)
    
    except Exception as e:
        error_message = f"Failed to get credentials for service {service_name}"
        logger.error(f"{error_message}: {str(e)}")
        raise GCPAuthError(error_message, e)


@retry(max_attempts=3, strategy='exponential')
def impersonate_service_account(
    target_service_account: str,
    source_credentials: google.auth.credentials.Credentials,
    scopes: typing.List[str] = None
) -> google.auth.credentials.Credentials:
    """Creates credentials that impersonate a service account.
    
    Args:
        target_service_account: Email of the service account to impersonate
        source_credentials: Credentials with permissions to impersonate
        scopes: OAuth scopes to apply to the credentials
        
    Returns:
        Impersonated service account credentials
        
    Raises:
        GCPAuthError: If impersonation fails
    """
    try:
        logger.debug(f"Impersonating service account: {target_service_account}")
        
        # Create IAM Credentials client using source credentials
        client = google.cloud.iam_credentials.IAMCredentialsClient(
            credentials=source_credentials
        )
        
        # Construct the full resource name
        name = f"projects/-/serviceAccounts/{target_service_account}"
        
        # Generate access token for the target service account
        response = client.generate_access_token(
            name=name,
            scope=scopes or DEFAULT_SCOPES
        )
        
        # Create credentials from the token
        from google.oauth2 import credentials as oauth2_credentials
        
        credentials = oauth2_credentials.Credentials(
            token=response.access_token,
            expiry=response.expire_time
        )
        
        logger.info(f"Successfully impersonated service account: {target_service_account}")
        return credentials
    
    except Exception as e:
        error_message = f"Failed to impersonate service account {target_service_account}"
        logger.error(f"{error_message}: {str(e)}")
        raise GCPAuthError(error_message, e)


def get_project_id(credentials: google.auth.credentials.Credentials = None) -> str:
    """Gets the current GCP project ID from credentials or environment.
    
    Args:
        credentials: Google Cloud credentials (optional)
        
    Returns:
        GCP project ID
        
    Raises:
        ValueError: If project ID cannot be determined
    """
    # Try to get project ID from credentials
    if credentials and hasattr(credentials, 'project_id') and credentials.project_id:
        return credentials.project_id
    
    # Try to get from environment variable
    project_id = os.environ.get(GCP_PROJECT_ID_ENV_VAR)
    
    if not project_id:
        try:
            # Try to get from default credentials
            _, project_id = google.auth.default()
        except Exception:
            pass
    
    if not project_id:
        error_message = f"Unable to determine GCP project ID. Set {GCP_PROJECT_ID_ENV_VAR} environment variable."
        logger.error(error_message)
        raise ValueError(error_message)
    
    return project_id


def get_gcp_location() -> str:
    """Gets the GCP location (region/zone) from environment or default.
    
    Returns:
        GCP location
    """
    # Try to get from environment variable
    location = os.environ.get(GCP_LOCATION_ENV_VAR)
    
    # Use default if not specified
    if not location:
        location = GCP_DEFAULT_LOCATION
        logger.debug(f"Using default GCP location: {location}")
    
    return location


def is_running_on_gcp() -> bool:
    """Determines if the application is running on Google Cloud Platform.
    
    Returns:
        True if running on GCP, False otherwise
    """
    # Check for GCP-specific environment variables
    gcp_indicators = [
        'GOOGLE_CLOUD_PROJECT',
        'GCP_PROJECT',
        'GCLOUD_PROJECT',
        GCP_PROJECT_ID_ENV_VAR,
        'GOOGLE_APPLICATION_CREDENTIALS'
    ]

    # Check if any GCP indicators are present
    for indicator in gcp_indicators:
        if os.environ.get(indicator):
            return True

    # Try to access GCP metadata server as a fallback
    try:
        metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/id"
        headers = {"Metadata-Flavor": "Google"}
        
        # Don't import requests at the top level to avoid dependency issues
        import requests
        response = requests.get(metadata_url, headers=headers, timeout=1)
        return response.status_code == 200
    except Exception:
        # Suppress any connection errors or import errors
        return False


def clear_credentials_cache() -> None:
    """Clears the credentials cache.
    
    This is useful for testing or when you need to force re-authentication.
    """
    global _default_credentials, _service_account_credentials
    
    _default_credentials = None
    _service_account_credentials.clear()
    
    logger.debug("Credentials cache cleared")