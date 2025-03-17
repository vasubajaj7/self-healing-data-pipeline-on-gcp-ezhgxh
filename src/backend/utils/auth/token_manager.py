"""
Manages authentication tokens for various services with caching, automatic refresh, and expiration handling.
Provides a unified interface for token acquisition, storage, and validation across the self-healing data pipeline.
"""

import time
import json
import typing
from datetime import datetime
import threading
import base64

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR
from ..logging.logger import get_logger
from ..retry.retry_decorator import retry
from ..http.http_client import HttpClient
from ..errors.error_types import AuthenticationError
from ..config.config_loader import load_config

# Initialize logger
logger = get_logger(__name__)

# Global HTTP client and configuration
http_client = HttpClient()
config = load_config().get('auth', {})


def is_token_valid(token_data: dict, buffer_seconds: int) -> bool:
    """
    Checks if a token is valid based on its expiration time.

    Args:
        token_data: Dictionary containing token information, including 'expires_at'.
        buffer_seconds: Buffer time in seconds to consider the token valid before actual expiration.

    Returns:
        True if token is valid, False otherwise.
    """
    if not token_data:
        return False

    try:
        current_time = time.time()
        expires_at = token_data['expires_at']
        
        # Apply buffer to ensure token doesn't expire during use
        if current_time + buffer_seconds < expires_at:
            return True
        else:
            return False
    except (KeyError, TypeError) as e:
        logger.error(f"Error checking token validity: {e}")
        return False


def decode_token(token: str) -> dict:
    """
    Decodes a JWT token to extract payload information.

    Args:
        token: JWT token string.

    Returns:
        Decoded token payload as a dictionary.
    """
    try:
        header, payload, signature = token.split('.')
        
        # Base64 decode the payload part
        decoded_payload = base64.b64decode(payload + '==').decode('utf-8')
        
        # Parse the decoded payload as JSON
        return json.loads(decoded_payload)
    except Exception as e:
        logger.error(f"Error decoding token: {e}")
        return {}


def get_token_expiration(token_or_data: typing.Union[str, dict]) -> int:
    """
    Extracts the expiration time from a token or token data.

    Args:
        token_or_data: JWT token string or token data dictionary.

    Returns:
        Token expiration time in seconds since epoch.
    """
    try:
        if isinstance(token_or_data, str):
            payload = decode_token(token_or_data)
            return int(payload['exp'])
        elif isinstance(token_or_data, dict):
            return int(token_or_data['expires_at'])
        else:
            logger.warning(f"Unexpected input type: {type(token_or_data)}")
            return 0
    except KeyError as e:
        logger.error(f"Expiration time not found in token data: {e}")
        return 0
    except Exception as e:
        logger.error(f"Error extracting expiration time: {e}")
        return 0


def calculate_expires_at(expires_in: int) -> int:
    """
    Calculates absolute expiration time from relative expiration seconds.

    Args:
        expires_in: Token expiration time in seconds from now.

    Returns:
        Absolute expiration time in seconds since epoch.
    """
    current_time = time.time()
    expires_at = current_time + expires_in
    return int(expires_at)


class TokenManager:
    """
    Manages authentication tokens with caching and automatic refresh.
    """

    def __init__(self, default_buffer_seconds: int = 300):
        """
        Initialize the token manager with an empty cache.
        """
        self._token_cache = {}  # type: dict
        self._lock = threading.RLock()
        self._refresh_functions = {}  # type: dict
        self._default_buffer_seconds = default_buffer_seconds
        logger.info("TokenManager initialized")

    def get_token(self, service_name: str, force_refresh: bool = False) -> str:
        """
        Gets a token for a specific service, refreshing if necessary.

        Args:
            service_name: Name of the service to get the token for.
            force_refresh: Whether to force a token refresh.

        Returns:
            Valid access token.
        """
        with self._lock:
            # Check if token exists in cache and is valid (unless force_refresh)
            token_data = self._token_cache.get(service_name)
            if not force_refresh and token_data and is_token_valid(token_data, self._default_buffer_seconds):
                logger.debug(f"Using cached token for service: {service_name}")
                return token_data['access_token']

            # If token needs refresh and refresh function exists, call it
            if service_name in self._refresh_functions:
                logger.info(f"Refreshing token for service: {service_name}")
                access_token = self.refresh_token(service_name)
                return access_token
            else:
                # If no refresh function, raise AuthenticationError
                raise AuthenticationError(
                    message=f"No refresh function registered for service: {service_name}",
                    service_name=service_name,
                    auth_details={}
                )

    def store_token(self, service_name: str, access_token: str, expires_in: int, refresh_token: str = None, additional_data: dict = None) -> dict:
        """
        Stores a token in the cache with associated metadata.

        Args:
            service_name: Name of the service the token is for.
            access_token: The access token string.
            expires_in: Token expiration time in seconds from now.
            refresh_token: Refresh token string (optional).
            additional_data: Additional data to store with the token (optional).

        Returns:
            Complete token data stored in cache.
        """
        with self._lock:
            # Calculate absolute expiration time from expires_in
            expires_at = calculate_expires_at(expires_in)

            # Create token data dictionary with access_token and expires_at
            token_data = {
                'access_token': access_token,
                'expires_at': expires_at
            }

            # Add refresh_token if provided
            if refresh_token:
                token_data['refresh_token'] = refresh_token

            # Add any additional data provided
            if additional_data:
                token_data.update(additional_data)

            # Store token data in cache under service_name
            self._token_cache[service_name] = token_data

            logger.debug(f"Stored token for service: {service_name} (expires_at: {expires_at})")
            return token_data

    def clear_token(self, service_name: str) -> bool:
        """
        Removes a token from the cache.

        Args:
            service_name: Name of the service to clear the token for.

        Returns:
            True if token was removed, False if not found.
        """
        with self._lock:
            if service_name in self._token_cache:
                del self._token_cache[service_name]
                logger.debug(f"Cleared token for service: {service_name}")
                return True
            else:
                logger.debug(f"No token found for service: {service_name}")
                return False

    def clear_all_tokens(self) -> None:
        """
        Clears all tokens from the cache.
        """
        with self._lock:
            self._token_cache.clear()
            logger.info("Cleared all tokens from cache")

    def register_refresh_function(self, service_name: str, refresh_function: typing.Callable) -> None:
        """
        Registers a function to refresh a specific service's token.

        Args:
            service_name: Name of the service to register the refresh function for.
            refresh_function: Function to call to refresh the token.
        """
        with self._lock:
            self._refresh_functions[service_name] = refresh_function
            logger.info(f"Registered refresh function for service: {service_name}")

    def get_token_data(self, service_name: str) -> dict:
        """
        Gets the complete token data for a service.

        Args:
            service_name: Name of the service to get the token data for.

        Returns:
            Token data including access_token, expires_at, etc.
        """
        with self._lock:
            token_data = self._token_cache.get(service_name)
            logger.debug(f"Retrieved token data for service: {service_name}")
            return token_data

    def is_token_valid(self, service_name: str, buffer_seconds: int = None) -> bool:
        """
        Checks if a service's token is valid.

        Args:
            service_name: Name of the service to check the token for.
            buffer_seconds: Buffer time in seconds to consider the token valid before actual expiration.

        Returns:
            True if token is valid, False otherwise.
        """
        token_data = self.get_token_data(service_name)
        if not token_data:
            return False
        
        # Use global is_token_valid function to check validity
        buffer_seconds = buffer_seconds if buffer_seconds is not None else self._default_buffer_seconds
        return is_token_valid(token_data, buffer_seconds)

    @retry(max_attempts=DEFAULT_MAX_RETRY_ATTEMPTS, backoff_factor=RETRY_BACKOFF_FACTOR)
    def refresh_token(self, service_name: str) -> str:
        """
        Refreshes a token using its refresh function.

        Args:
            service_name: Name of the service to refresh the token for.

        Returns:
            New access token.
        """
        with self._lock:
            if service_name not in self._refresh_functions:
                raise AuthenticationError(
                    message=f"No refresh function registered for service: {service_name}",
                    service_name=service_name,
                    auth_details={}
                )

            refresh_function = self._refresh_functions[service_name]
            try:
                # Call refresh function to get new token
                new_token_data = refresh_function()

                # Store new token in cache
                if new_token_data and 'access_token' in new_token_data and 'expires_in' in new_token_data:
                    self.store_token(
                        service_name=service_name,
                        access_token=new_token_data['access_token'],
                        expires_in=new_token_data['expires_in'],
                        refresh_token=new_token_data.get('refresh_token'),
                        additional_data=new_token_data.get('additional_data')
                    )
                    logger.info(f"Successfully refreshed token for service: {service_name}")
                    return new_token_data['access_token']
                else:
                    raise AuthenticationError(
                        message=f"Invalid token data returned from refresh function for service: {service_name}",
                        service_name=service_name,
                        auth_details=new_token_data if new_token_data else {}
                    )
            except Exception as e:
                logger.error(f"Error refreshing token for service {service_name}: {e}")
                raise


class TokenError(AuthenticationError):
    """
    Exception raised for token-related errors.
    """

    def __init__(self, message: str, original_exception: Exception = None):
        """
        Initialize with error message and original exception.
        """
        super().__init__(
            message=message,
            service_name="TokenManager",
            auth_details={},
        )
        self.original_exception = original_exception
        logger.error(f"TokenError: {message}", exc_info=original_exception is not None)