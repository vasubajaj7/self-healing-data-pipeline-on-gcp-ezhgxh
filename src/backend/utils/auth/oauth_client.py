"""
Implements OAuth 2.0 client functionality for authenticating with external APIs and services.
Provides a unified interface for obtaining, refreshing, and managing OAuth tokens with support
for different grant types and authentication flows.
"""

import json
import time
import typing
import urllib.parse
import base64

import requests  # version 2.31.x
from requests import Response

from ...constants import (
    DEFAULT_MAX_RETRY_ATTEMPTS,
    RETRY_BACKOFF_FACTOR
)
from ..logging.logger import get_logger
from ..retry.retry_decorator import retry
from .gcp_auth import get_credentials_for_service
from .token_manager import TokenManager
from ..http.http_client import HttpClient
from ..config.config_loader import load_config
from ..config.environment import get_environment
from ..errors.error_types import AuthenticationError


# Initialize logger
logger = get_logger(__name__)

# Load OAuth configuration settings
oauth_config = load_config().get('oauth', {})

# Global TokenManager instance
token_manager = TokenManager()

# Global HttpClient instance
http_client = HttpClient()


@retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
def get_client_credentials_token(
    client_id: str,
    client_secret: str,
    token_url: str,
    scopes: typing.List[str]
) -> dict:
    """Obtains an OAuth token using the client credentials grant type.

    Args:
        client_id: The client ID.
        client_secret: The client secret.
        token_url: The token endpoint URL.
        scopes: List of scopes to request.

    Returns:
        Token response containing access_token, token_type, expires_in, etc.
    """
    try:
        # Prepare request parameters for client credentials grant
        params = {'grant_type': 'client_credentials'}

        # Add scopes to request if provided
        if scopes:
            params['scope'] = ' '.join(scopes)

        # Encode client_id and client_secret for Basic Authentication
        auth_str = f"{client_id}:{client_secret}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()

        # Set request headers with Basic Authentication
        headers = {'Authorization': f'Basic {encoded_auth}'}

        # Make POST request to token_url with parameters
        response: Response = requests.post(token_url, headers=headers, data=params)
        response.raise_for_status()

        # Parse and return JSON response
        token_response = response.json()
        logger.debug(f"Successfully obtained client credentials token from {token_url}")
        return token_response

    except requests.exceptions.RequestException as e:
        # Handle and log authentication errors
        logger.error(f"Error obtaining client credentials token from {token_url}: {e}")
        raise AuthenticationError(f"Failed to obtain client credentials token: {e}", service_name=token_url)


@retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
def get_authorization_code_token(
    client_id: str,
    client_secret: str,
    token_url: str,
    authorization_code: str,
    redirect_uri: str
) -> dict:
    """Obtains an OAuth token using the authorization code grant type.

    Args:
        client_id: The client ID.
        client_secret: The client secret.
        token_url: The token endpoint URL.
        authorization_code: The authorization code received from the authorization server.
        redirect_uri: The redirect URI used in the authorization request.

    Returns:
        Token response containing access_token, refresh_token, token_type, expires_in, etc.
    """
    try:
        # Prepare request parameters for authorization code grant
        params = {'grant_type': 'authorization_code',
                  'code': authorization_code,
                  'redirect_uri': redirect_uri}

        # Encode client_id and client_secret for Basic Authentication
        auth_str = f"{client_id}:{client_secret}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()

        # Set request headers with Basic Authentication
        headers = {'Authorization': f'Basic {encoded_auth}'}

        # Make POST request to token_url with parameters
        response: Response = requests.post(token_url, headers=headers, data=params)
        response.raise_for_status()

        # Parse and return JSON response
        token_response = response.json()
        logger.debug(f"Successfully obtained authorization code token from {token_url}")
        return token_response

    except requests.exceptions.RequestException as e:
        # Handle and log authentication errors
        logger.error(f"Error obtaining authorization code token from {token_url}: {e}")
        raise AuthenticationError(f"Failed to obtain authorization code token: {e}", service_name=token_url)


@retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
def refresh_token(
    client_id: str,
    client_secret: str,
    token_url: str,
    refresh_token: str
) -> dict:
    """Refreshes an OAuth token using a refresh token.

    Args:
        client_id: The client ID.
        client_secret: The client secret.
        token_url: The token endpoint URL.
        refresh_token: The refresh token.

    Returns:
        Token response containing new access_token, token_type, expires_in, etc.
    """
    try:
        # Prepare request parameters for refresh token grant
        params = {'grant_type': 'refresh_token',
                  'refresh_token': refresh_token}

        # Encode client_id and client_secret for Basic Authentication
        auth_str = f"{client_id}:{client_secret}"
        encoded_auth = base64.b64encode(auth_str.encode()).decode()

        # Set request headers with Basic Authentication
        headers = {'Authorization': f'Basic {encoded_auth}'}

        # Make POST request to token_url with parameters
        response: Response = requests.post(token_url, headers=headers, data=params)
        response.raise_for_status()

        # Parse and return JSON response
        token_response = response.json()
        logger.debug(f"Successfully refreshed token from {token_url}")
        return token_response

    except requests.exceptions.RequestException as e:
        # Handle and log authentication errors
        logger.error(f"Error refreshing token from {token_url}: {e}")
        raise AuthenticationError(f"Failed to refresh token: {e}", service_name=token_url)


def get_authorization_url(
    client_id: str,
    authorization_url: str,
    redirect_uri: str,
    scopes: typing.List[str] = None,
    state: str = None,
    extra_params: dict = None
) -> str:
    """Generates an authorization URL for the authorization code flow.

    Args:
        client_id: The client ID.
        authorization_url: The authorization endpoint URL.
        redirect_uri: The redirect URI.
        scopes: List of scopes to request.
        state: A string value that the authorization server includes when redirecting back to the client.
               The client should verify that this value matches the state it provided in the authorization request.
        extra_params: A dictionary of any extra parameters to include in the authorization URL.

    Returns:
        Full authorization URL to redirect the user to.
    """
    try:
        # Prepare query parameters with client_id and redirect_uri
        params = {'client_id': client_id,
                  'redirect_uri': redirect_uri,
                  'response_type': 'code'}

        # Add scopes as space-delimited string if provided
        if scopes:
            params['scope'] = ' '.join(scopes)

        # Add state parameter if provided for CSRF protection
        if state:
            params['state'] = state

        # Add any extra parameters provided
        if extra_params:
            params.update(extra_params)

        # Construct the full authorization URL
        url = f"{authorization_url}?" + urllib.parse.urlencode(params)
        logger.debug(f"Generated authorization URL: {url}")
        return url

    except Exception as e:
        # Handle and log URL construction errors
        logger.error(f"Error generating authorization URL: {e}")
        raise AuthenticationError(f"Failed to generate authorization URL: {e}", service_name=authorization_url)


def get_service_oauth_token(service_name: str, force_refresh: bool = False) -> str:
    """Gets an OAuth token for a specific service using configured credentials.

    Args:
        service_name: Name of the service to get the token for.
        force_refresh: Whether to force a token refresh.

    Returns:
        OAuth access token for the service.
    """
    try:
        # Check if token exists in cache and is valid (unless force_refresh)
        token = token_manager.get_token(service_name, force_refresh)
        return token

    except Exception as e:
        # Handle and log authentication errors
        logger.error(f"Error getting OAuth token for service {service_name}: {e}")
        raise AuthenticationError(f"Failed to get OAuth token for service {service_name}: {e}", service_name=service_name)


def create_authorized_headers(token: str, additional_headers: dict = None) -> dict:
    """Creates HTTP headers with OAuth Bearer token authorization.

    Args:
        token: OAuth access token.
        additional_headers: Additional headers to include in the request.

    Returns:
        Headers dictionary with Authorization header.
    """
    # Create headers dictionary with Authorization: Bearer {token}
    headers = {'Authorization': f'Bearer {token}'}

    # Add any additional headers provided
    if additional_headers:
        headers.update(additional_headers)

    # Return the complete headers dictionary
    return headers


class OAuthClient:
    """Client for OAuth 2.0 authentication flows and token management."""

    def __init__(self, config: dict = None, token_manager: TokenManager = None, http_client: HttpClient = None):
        """Initialize the OAuth client with configuration.

        Args:
            config: Configuration dictionary for OAuth settings.
            token_manager: TokenManager instance for managing tokens.
            http_client: HttpClient instance for making HTTP requests.
        """
        # Store provided config or load from application configuration
        self._config = config or load_config().get('oauth', {})

        # Store provided token_manager or create a new instance
        self._token_manager = token_manager or TokenManager()

        # Store provided http_client or create a new instance
        self._http_client = http_client or HttpClient()

        logger.info("OAuthClient initialized")

    def get_token(self, service_name: str, force_refresh: bool = False) -> str:
        """Gets an OAuth token for a specific service.

        Args:
            service_name: Name of the service to get the token for.
            force_refresh: Whether to force a token refresh.

        Returns:
            OAuth access token.
        """
        try:
            # Call get_service_oauth_token with service name and force_refresh flag
            token = get_service_oauth_token(service_name, force_refresh)
            return token

        except Exception as e:
            # Handle and log authentication errors
            logger.error(f"Error getting token for service {service_name}: {e}")
            raise AuthenticationError(f"Failed to get token for service {service_name}: {e}", service_name=service_name)

    def get_token_for_service(self, client_id: str, client_secret: str, token_url: str, scopes: typing.List[str], grant_type: str) -> dict:
        """Gets an OAuth token for a service with specific configuration.

        Args:
            client_id: The client ID.
            client_secret: The client secret.
            token_url: The token endpoint URL.
            scopes: List of scopes to request.
            grant_type: The OAuth grant type.

        Returns:
            Token response containing access_token and other details.
        """
        try:
            # Determine appropriate OAuth flow based on grant_type
            if grant_type == 'client_credentials':
                # For client_credentials, call get_client_credentials_token
                token_response = get_client_credentials_token(client_id, client_secret, token_url, scopes)
            else:
                # For other grant types, raise NotImplementedError
                raise NotImplementedError(f"Grant type '{grant_type}' not implemented")

            # Return the token response
            return token_response

        except Exception as e:
            # Handle and log authentication errors
            logger.error(f"Error getting token for service with grant_type {grant_type}: {e}")
            raise AuthenticationError(f"Failed to get token for service with grant_type {grant_type}: {e}", service_name=token_url)

    def refresh_service_token(self, service_name: str) -> str:
        """Forces a refresh of the OAuth token for a service.

        Args:
            service_name: Name of the service to refresh the token for.

        Returns:
            New OAuth access token.
        """
        try:
            # Call get_service_oauth_token with service name and force_refresh=True
            token = get_service_oauth_token(service_name, force_refresh=True)
            return token

        except Exception as e:
            # Handle and log authentication errors
            logger.error(f"Error refreshing token for service {service_name}: {e}")
            raise AuthenticationError(f"Failed to refresh token for service {service_name}: {e}", service_name=service_name)

    def create_authorized_request(self, service_name: str, method: str, url: str, headers: dict = None, params: dict = None, json_data: dict = None) -> HttpResponse:
        """Creates an HTTP request with OAuth authorization.

        Args:
            service_name: Name of the service to get the token for.
            method: HTTP method (GET, POST, etc.).
            url: URL to request.
            headers: Additional headers to include in the request.
            params: Query parameters.
            json_data: JSON data to send in the request body.

        Returns:
            Response from the authorized request.
        """
        try:
            # Get OAuth token for the service
            token = self.get_token(service_name)

            # Create authorized headers with the token
            auth_headers = create_authorized_headers(token, headers)

            # Make HTTP request with the authorized headers
            response = self._http_client.request(method=method, url=url, headers=auth_headers, params=params, json_data=json_data)

            # Return the HTTP response
            return response

        except Exception as e:
            # Handle and log request errors
            logger.error(f"Error creating authorized request for service {service_name}: {e}")
            raise AuthenticationError(f"Failed to create authorized request for service {service_name}: {e}", service_name=service_name)

    def get_authorization_url(self, service_name: str, redirect_uri: str, state: str = None, extra_params: dict = None) -> str:
        """Generates an authorization URL for the authorization code flow.

        Args:
            service_name: Name of the service to get the authorization URL for.
            redirect_uri: The redirect URI.
            state: A string value that the authorization server includes when redirecting back to the client.
                   The client should verify that this value matches the state it provided in the authorization request.
            extra_params: A dictionary of any extra parameters to include in the authorization URL.

        Returns:
            Authorization URL.
        """
        try:
            # Get service configuration from oauth_config
            service_config = self._config.get(service_name)
            if not service_config:
                raise AuthenticationError(f"No OAuth configuration found for service: {service_name}", service_name=service_name)

            client_id = service_config.get('client_id')
            authorization_url = service_config.get('authorization_url')

            if not client_id or not authorization_url:
                raise AuthenticationError(f"Missing client_id or authorization_url in service configuration for {service_name}", service_name=service_name)

            # Call get_authorization_url function with service configuration
            auth_url = get_authorization_url(client_id, authorization_url, redirect_uri, service_config.get('scopes'), state, extra_params)
            return auth_url

        except Exception as e:
            # Handle and log URL generation errors
            logger.error(f"Error generating authorization URL for service {service_name}: {e}")
            raise AuthenticationError(f"Failed to generate authorization URL for service {service_name}: {e}", service_name=service_name)

    def exchange_code_for_token(self, service_name: str, code: str, redirect_uri: str) -> dict:
        """Exchanges an authorization code for an OAuth token.

        Args:
            service_name: Name of the service to exchange the code for.
            code: The authorization code.
            redirect_uri: The redirect URI.

        Returns:
            Token response.
        """
        try:
            # Get service configuration from oauth_config
            service_config = self._config.get(service_name)
            if not service_config:
                raise AuthenticationError(f"No OAuth configuration found for service: {service_name}", service_name=service_name)

            client_id = service_config.get('client_id')
            client_secret = service_config.get('client_secret')
            token_url = service_config.get('token_url')

            if not client_id or not client_secret or not token_url:
                 raise AuthenticationError(f"Missing client_id, client_secret, or token_url in service configuration for {service_name}", service_name=service_name)

            # Call get_authorization_code_token with code and service configuration
            token_response = get_authorization_code_token(client_id, client_secret, token_url, code, redirect_uri)

            # Cache the token with the token manager
            if token_response and 'access_token' in token_response and 'expires_in' in token_response:
                self._token_manager.store_token(
                    service_name=service_name,
                    access_token=token_response['access_token'],
                    expires_in=token_response['expires_in'],
                    refresh_token=token_response.get('refresh_token'),
                    additional_data=token_response.get('additional_data')
                )
            else:
                raise AuthenticationError(f"Invalid token data returned from authorization code exchange for service: {service_name}", service_name=service_name)

            # Return the token response
            return token_response

        except Exception as e:
            # Handle and log token exchange errors
            logger.error(f"Error exchanging code for token for service {service_name}: {e}")
            raise AuthenticationError(f"Failed to exchange code for token for service {service_name}: {e}", service_name=service_name)


class OAuthError(AuthenticationError):
    """Exception raised for OAuth-related errors."""

    def __init__(self, message: str, original_exception: Exception = None, response_data: dict = None):
        """Initialize with error message and details."""
        super().__init__(message=message, service_name="OAuth", auth_details={})
        self.original_exception = original_exception
        self.response_data = response_data
        logger.error(f"OAuthError: {message}", exc_info=original_exception is not None)