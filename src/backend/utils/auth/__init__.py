"""
Entry point for the authentication utilities module that provides a unified interface for various authentication mechanisms
including GCP authentication, OAuth 2.0, and token management. This module simplifies authentication across the self-healing data pipeline.
"""

import typing

from .gcp_auth import (  # Google Auth v2.6.0+
    get_default_credentials,
    get_service_account_credentials,
    get_credentials_for_service,
    impersonate_service_account,
    get_project_id,
    get_gcp_location,
    is_running_on_gcp,
    clear_credentials_cache,
    GCPAuthError,
    DEFAULT_SCOPES
)
from .oauth_client import (  # version 2.31.x
    OAuthClient,
    OAuthError,
    get_client_credentials_token,
    get_authorization_code_token,
    refresh_token,
    get_authorization_url,
    get_service_oauth_token,
    create_authorized_headers
)
from .token_manager import (
    TokenManager,
    TokenError,
    is_token_valid,
    decode_token,
    get_token_expiration,
    calculate_expires_at
)


# Global instances for OAuthClient and TokenManager
oauth_client_instance = OAuthClient()
token_manager_instance = TokenManager()


def get_auth_for_service(service_name: str, auth_type: str) -> typing.Union[str, object]:
    """Factory function that returns the appropriate authentication mechanism for a service

    Args:
        service_name (str): Name of the service requiring authentication
        auth_type (str): Type of authentication required (gcp, oauth, token)

    Returns:
        Union[str, object]: Authentication token or credentials object depending on auth_type
    """
    try:
        if auth_type == 'gcp':
            # Return GCP credentials for the service
            return get_credentials_for_service(service_name)
        elif auth_type == 'oauth':
            # Return OAuth token for the service
            return oauth_client_instance.get_token(service_name)
        elif auth_type == 'token':
            # Return token from the token manager
            return token_manager_instance.get_token(service_name)
        else:
            # Raise ValueError for unknown auth_type
            raise ValueError(f"Unknown authentication type: {auth_type}")
    except Exception as e:
        # Handle and log authentication errors
        raise AuthenticationError(f"Failed to get authentication for service {service_name}: {e}", service_name=service_name) from e


__all__ = [
    'get_default_credentials',
    'get_service_account_credentials',
    'get_credentials_for_service',
    'impersonate_service_account',
    'get_project_id',
    'get_gcp_location',
    'is_running_on_gcp',
    'clear_credentials_cache',
    'GCPAuthError',
    'DEFAULT_SCOPES',
    'OAuthClient',
    'OAuthError',
    'get_client_credentials_token',
    'get_authorization_code_token',
    'refresh_token',
    'get_authorization_url',
    'get_service_oauth_token',
    'create_authorized_headers',
    'TokenManager',
    'TokenError',
    'is_token_valid',
    'decode_token',
    'get_token_expiration',
    'calculate_expires_at',
    'get_auth_for_service',
    'oauth_client_instance',
    'token_manager_instance'
]