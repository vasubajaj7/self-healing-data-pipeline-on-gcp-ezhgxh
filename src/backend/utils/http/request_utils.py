"""
Utility functions for HTTP request preparation, URL building, and request configuration.

Provides helper functions to standardize request handling across the application,
including parameter formatting, header preparation, and authentication handling.
"""

import json
import urllib.parse
import base64
from typing import Dict, Union, Optional, Any
from dataclasses import dataclass

from ...constants import DEFAULT_TIMEOUT
from ..errors.error_types import ValidationError, ConfigurationError
from ..logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Default headers for HTTP requests
DEFAULT_HEADERS = {'User-Agent': 'SelfHealingPipeline/1.0'}


def prepare_request_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """
    Prepares and validates request parameters.
    
    Args:
        params: Dictionary of request parameters
    
    Returns:
        Validated and prepared request parameters
    
    Raises:
        ValidationError: If params is not a dictionary
    """
    # Check if params is None, return empty dict if so
    if params is None:
        return {}
    
    # Validate that params is a dictionary
    if not isinstance(params, dict):
        raise ValidationError(
            message="Request parameters must be a dictionary",
            validation_details={"params_type": str(type(params))}
        )
    
    # Create a new dict with non-None values converted to strings
    prepared_params = {}
    for key, value in params.items():
        if value is not None:
            prepared_params[key] = str(value)
    
    return prepared_params


def prepare_request_headers(
    headers: Optional[Dict[str, str]] = None, 
    default_headers: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Prepares and validates request headers.
    
    Args:
        headers: Custom headers to include in the request
        default_headers: Default headers to use if not overridden by custom headers
    
    Returns:
        Merged and validated request headers
    
    Raises:
        ValidationError: If headers is not a dictionary
    """
    # Initialize with default_headers or global DEFAULT_HEADERS
    result_headers = (default_headers or DEFAULT_HEADERS).copy()
    
    # If headers is None, return default headers
    if headers is None:
        return result_headers
    
    # Validate that headers is a dictionary
    if not isinstance(headers, dict):
        raise ValidationError(
            message="Request headers must be a dictionary",
            validation_details={"headers_type": str(type(headers))}
        )
    
    # Merge custom headers with default headers (custom takes precedence)
    result_headers.update(headers)
    
    return result_headers


def prepare_request_body(
    data: Optional[Union[Dict[str, Any], str, bytes]] = None,
    json_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Prepares request body based on data type.
    
    Args:
        data: Raw request data (can be dict, str, or bytes)
        json_data: JSON data to be serialized
    
    Returns:
        Request configuration with appropriate body parameters
    
    Raises:
        ValidationError: If both data and json_data are provided
    """
    result = {}
    
    # Validate that both data and json_data aren't provided
    if data is not None and json_data is not None:
        raise ValidationError(
            message="Cannot provide both 'data' and 'json_data'",
            validation_details={
                "has_data": data is not None,
                "has_json_data": json_data is not None
            }
        )
    
    # Handle JSON data
    if json_data is not None:
        result['json'] = json_data
    
    # Handle data based on type
    elif data is not None:
        if isinstance(data, dict):
            result['json'] = data
        elif isinstance(data, (str, bytes)):
            result['data'] = data
        else:
            raise ValidationError(
                message="Request data must be a dictionary, string, or bytes",
                validation_details={"data_type": str(type(data))}
            )
    
    return result


def prepare_auth_header(auth_type: str, auth_config: Dict[str, Any]) -> Dict[str, str]:
    """
    Prepares authentication headers based on auth type.
    
    Args:
        auth_type: Authentication type (none, api_key, basic, bearer, oauth2)
        auth_config: Authentication configuration parameters
    
    Returns:
        Authentication headers
    
    Raises:
        ConfigurationError: If auth configuration is invalid or missing required parameters
    """
    headers = {}
    
    # No authentication
    if auth_type is None or auth_type.lower() == 'none':
        return headers
    
    # API key authentication
    elif auth_type.lower() == 'api_key':
        if 'key_name' not in auth_config or 'key_value' not in auth_config:
            raise ConfigurationError(
                message="API key auth requires 'key_name' and 'key_value'",
                config_details={"auth_type": auth_type}
            )
        headers[auth_config['key_name']] = auth_config['key_value']
    
    # Basic authentication
    elif auth_type.lower() == 'basic':
        if 'username' not in auth_config or 'password' not in auth_config:
            raise ConfigurationError(
                message="Basic auth requires 'username' and 'password'",
                config_details={"auth_type": auth_type}
            )
        username = auth_config['username']
        password = auth_config['password']
        auth_str = f"{username}:{password}"
        encoded = base64.b64encode(auth_str.encode()).decode()
        headers['Authorization'] = f"Basic {encoded}"
    
    # Bearer token / OAuth2 authentication
    elif auth_type.lower() in ('bearer', 'oauth2'):
        if 'token' not in auth_config:
            raise ConfigurationError(
                message="Bearer/OAuth2 auth requires 'token'",
                config_details={"auth_type": auth_type}
            )
        headers['Authorization'] = f"Bearer {auth_config['token']}"
    
    # Unknown authentication type
    else:
        raise ConfigurationError(
            message=f"Unsupported authentication type: {auth_type}",
            config_details={"auth_type": auth_type}
        )
    
    return headers


def build_url(base_url: str, path: Optional[str] = None) -> str:
    """
    Builds a complete URL from base URL and path.
    
    Args:
        base_url: Base URL
        path: Relative path to append to base URL
    
    Returns:
        Complete URL
    
    Raises:
        ValidationError: If base_url is not provided
    """
    # Validate base_url
    if not base_url:
        raise ValidationError(
            message="Base URL is required",
            validation_details={"base_url": base_url}
        )
    
    # If path is None, return base_url
    if not path:
        return base_url
    
    # Strip trailing slash from base_url if present
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    # Strip leading slash from path if present
    if path.startswith('/'):
        path = path[1:]
    
    # Join base_url and path with a slash
    return f"{base_url}/{path}"


def validate_timeout(timeout: Optional[Union[int, float]] = None) -> int:
    """
    Validates and normalizes timeout value.
    
    Args:
        timeout: Timeout value in seconds
    
    Returns:
        Validated timeout value
    
    Raises:
        ValidationError: If timeout is negative
    """
    # If timeout is None, return DEFAULT_TIMEOUT
    if timeout is None:
        return DEFAULT_TIMEOUT
    
    # Validate that timeout is a number
    if not isinstance(timeout, (int, float)):
        raise ValidationError(
            message="Timeout must be a number",
            validation_details={"timeout_type": str(type(timeout))}
        )
    
    # Validate that timeout is positive
    if timeout < 0:
        raise ValidationError(
            message="Timeout must be a positive number",
            validation_details={"timeout": timeout}
        )
    
    # Convert to int and return
    return int(timeout)


def format_curl_command(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Union[Dict[str, Any], str, bytes]] = None,
    json_data: Optional[Dict[str, Any]] = None
) -> str:
    """
    Formats a curl command equivalent to the request for debugging.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        headers: Request headers
        data: Request data
        json_data: Request JSON data
    
    Returns:
        Formatted curl command
    """
    curl_parts = [f"curl -X {method.upper()} '{url}'"]
    
    # Add headers
    if headers:
        for key, value in headers.items():
            # Mask sensitive headers
            masked_value = value
            if key.lower() in ('authorization', 'api-key', 'x-api-key', 'apikey'):
                masked_value = '********'
            curl_parts.append(f"-H '{key}: {masked_value}'")
    
    # Add JSON data
    if json_data:
        json_str = json.dumps(json_data)
        curl_parts.append(f"--data '{json_str}'")
    
    # Add raw data
    elif data:
        if isinstance(data, dict):
            data_str = json.dumps(data)
            curl_parts.append(f"--data '{data_str}'")
        else:
            # Convert bytes to string if necessary
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            curl_parts.append(f"--data '{data}'")
    
    return " \\\n  ".join(curl_parts)


@dataclass
class RequestConfig:
    """
    Data class for HTTP request configuration.
    """
    method: str
    url: str
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    data: Optional[Union[Dict[str, Any], str, bytes]] = None
    json_data: Optional[Dict[str, Any]] = None
    timeout: int = DEFAULT_TIMEOUT
    verify: bool = True
    allow_redirects: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the request configuration to a dictionary.
        
        Returns:
            Dictionary representation of the request configuration
        """
        result = {}
        for key, value in self.__dict__.items():
            if value is not None:
                result[key] = value
        return result
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'RequestConfig':
        """
        Create a RequestConfig from a dictionary.
        
        Args:
            config_dict: Dictionary with configuration values
        
        Returns:
            RequestConfig instance
        """
        return cls(**config_dict)
    
    def validate(self) -> bool:
        """
        Validate the request configuration.
        
        Returns:
            True if configuration is valid
        
        Raises:
            ValidationError: If configuration is invalid
        """
        if not self.method:
            raise ValidationError(
                message="Request method is required",
                validation_details={"method": self.method}
            )
        
        if not self.url:
            raise ValidationError(
                message="Request URL is required",
                validation_details={"url": self.url}
            )
        
        if self.data is not None and self.json_data is not None:
            raise ValidationError(
                message="Cannot provide both 'data' and 'json_data'",
                validation_details={
                    "has_data": self.data is not None,
                    "has_json_data": self.json_data is not None
                }
            )
        
        return True