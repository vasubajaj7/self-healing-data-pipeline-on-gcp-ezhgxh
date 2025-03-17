"""
Utility functions for handling HTTP responses in the self-healing data pipeline.

Provides standardized methods for response parsing, content extraction, status checking,
and error handling. Supports the HTTP client implementation with robust response
processing capabilities.
"""

import json
from typing import Any, Dict, List, Optional, Union

import requests  # version 2.31.x

from ..errors.error_types import ValidationError, DataError
from ..logging.logger import get_logger

# Set up module logger
logger = get_logger(__name__)


def parse_json_response(response: requests.Response, raise_error: bool = False) -> Union[dict, list, None]:
    """
    Parses a JSON response with error handling.
    
    Args:
        response: The HTTP response to parse
        raise_error: Whether to raise an error on parsing failure
        
    Returns:
        Parsed JSON data or None if parsing fails
        
    Raises:
        DataError: If parsing fails and raise_error is True
    """
    if response is None:
        logger.warning("Cannot parse None response")
        return None
    
    try:
        return response.json()
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse JSON response: {str(e)}"
        if raise_error:
            raise DataError(
                message=error_msg,
                data_source="http_response",
                data_details={
                    "status_code": response.status_code,
                    "url": response.url,
                    "response_text": response.text[:200] + "..." if len(response.text) > 200 else response.text
                }
            )
        
        logger.error(error_msg)
        return None
    except Exception as e:
        error_msg = f"Unexpected error parsing JSON response: {str(e)}"
        if raise_error:
            raise DataError(
                message=error_msg,
                data_source="http_response",
                data_details={
                    "status_code": response.status_code,
                    "url": response.url,
                    "response_text": response.text[:200] + "..." if len(response.text) > 200 else response.text
                }
            )
        
        logger.error(error_msg)
        return None


def extract_response_value(data: Union[dict, list], path: str, default: Any = None) -> Any:
    """
    Extracts a value from a nested JSON structure using a path.
    
    Args:
        data: The data structure to extract from
        path: Dot-notation path to the value (e.g., "data.items[0].name")
        default: Default value to return if path not found
        
    Returns:
        Extracted value or default if not found
    """
    if data is None:
        return default
    
    parts = path.split('.')
    current = data
    
    try:
        for part in parts:
            if '[' in part and part.endswith(']'):
                # Handle array indexing like items[0]
                array_part, idx_part = part.split('[', 1)
                idx = int(idx_part[:-1])  # Remove trailing ']'
                
                if array_part:
                    current = current[array_part]
                current = current[idx]
            else:
                current = current[part]
        return current
    except (KeyError, IndexError, TypeError) as e:
        logger.debug(f"Path '{path}' not found in data: {str(e)}")
        return default


def is_success_response(response: requests.Response) -> bool:
    """
    Checks if a response has a successful status code (2xx).
    
    Args:
        response: The HTTP response to check
        
    Returns:
        True if response status is in the 2xx range
    """
    if response is None:
        return False
    
    return 200 <= response.status_code < 300


def is_redirect_response(response: requests.Response) -> bool:
    """
    Checks if a response has a redirect status code (3xx).
    
    Args:
        response: The HTTP response to check
        
    Returns:
        True if response status is in the 3xx range
    """
    if response is None:
        return False
    
    return 300 <= response.status_code < 400


def is_client_error_response(response: requests.Response) -> bool:
    """
    Checks if a response has a client error status code (4xx).
    
    Args:
        response: The HTTP response to check
        
    Returns:
        True if response status is in the 4xx range
    """
    if response is None:
        return False
    
    return 400 <= response.status_code < 500


def is_server_error_response(response: requests.Response) -> bool:
    """
    Checks if a response has a server error status code (5xx).
    
    Args:
        response: The HTTP response to check
        
    Returns:
        True if response status is in the 5xx range
    """
    if response is None:
        return False
    
    return 500 <= response.status_code < 600


def get_response_content_type(response: requests.Response) -> str:
    """
    Extracts the content type from a response.
    
    Args:
        response: The HTTP response to check
        
    Returns:
        Content type or empty string if not found
    """
    if response is None:
        return ""
    
    return response.headers.get('Content-Type', '')


def is_json_response(response: requests.Response) -> bool:
    """
    Checks if a response contains JSON data based on content type.
    
    Args:
        response: The HTTP response to check
        
    Returns:
        True if response contains JSON data
    """
    content_type = get_response_content_type(response)
    return 'application/json' in content_type


def format_response_for_logging(
    response: requests.Response, 
    include_body: bool = True, 
    max_body_length: int = 1000
) -> dict:
    """
    Formats a response object for safe logging.
    
    Args:
        response: The HTTP response to format
        include_body: Whether to include the response body
        max_body_length: Maximum length of body to include
        
    Returns:
        Formatted response data safe for logging
    """
    if response is None:
        return {}
    
    # Start with basic response info
    formatted = {
        'status_code': response.status_code,
        'url': response.url,
        'elapsed': response.elapsed.total_seconds(),
    }
    
    # Add headers with sensitive information redacted
    formatted['headers'] = {
        k: v if k.lower() not in ['authorization', 'cookie', 'x-api-key', 'api-key', 'token', 'secret'] 
          else '[REDACTED]'
        for k, v in response.headers.items()
    }
    
    # Add response body if requested
    if include_body and hasattr(response, 'content') and response.content:
        try:
            # Try to parse as JSON for structured logging
            if is_json_response(response):
                body = parse_json_response(response)
                if body:
                    # If body is a dict, we can potentially mask sensitive fields
                    if isinstance(body, dict):
                        # Mask sensitive fields if present
                        for sensitive_field in ['password', 'token', 'secret', 'key', 'credential']:
                            if sensitive_field in body:
                                body[sensitive_field] = '[REDACTED]'
                    formatted['body'] = body
                    return formatted
        except Exception:
            # Fall back to text handling if JSON parsing fails
            pass
            
        # Handle as text if not JSON or JSON parsing failed
        body = response.text
        if len(body) > max_body_length:
            body = body[:max_body_length] + '...'
        formatted['body'] = body
    
    return formatted


class ResponseParser:
    """
    Class for parsing and extracting data from HTTP responses.
    """
    
    def __init__(self, response: requests.Response):
        """
        Initialize with a response object.
        
        Args:
            response: The HTTP response to parse
        """
        self.response = response
    
    def parse_json(self, raise_error: bool = False) -> Union[dict, list, None]:
        """
        Parse response content as JSON.
        
        Args:
            raise_error: Whether to raise an error on parsing failure
            
        Returns:
            Parsed JSON data or None if parsing fails
            
        Raises:
            DataError: If parsing fails and raise_error is True
        """
        return parse_json_response(self.response, raise_error)
    
    def extract_value(self, path: str, default: Any = None) -> Any:
        """
        Extract a value from parsed JSON using a path.
        
        Args:
            path: Dot-notation path to the value
            default: Default value to return if path not found
            
        Returns:
            Extracted value or default if not found
            
        Raises:
            DataError: If JSON parsing fails
        """
        data = self.parse_json(raise_error=True)
        return extract_response_value(data, path, default)
    
    def extract_from_response(self, path: str, default: Any = None) -> Any:
        """
        Extract a value directly from the response using a path.
        
        Args:
            path: Dot-notation path to the value
            default: Default value to return if path not found
            
        Returns:
            Extracted value or default if not found
        """
        # Use raise_error=False to avoid exceptions for normal extraction
        data = self.parse_json(raise_error=False)
        return extract_response_value(data, path, default)
    
    def get_error_details(self, error_paths: dict) -> dict:
        """
        Extract error details from an error response.
        
        Args:
            error_paths: Dictionary mapping of result keys to response paths
                         (e.g., {'message': 'error.message', 'code': 'error.code'})
            
        Returns:
            Dictionary containing extracted error details with status code
        """
        result = {
            'status_code': self.response.status_code
        }
        
        # Try to parse response as JSON, don't raise error as this is error handling
        data = self.parse_json(raise_error=False)
        
        # Extract values using the provided paths
        if data:
            for key, path in error_paths.items():
                result[key] = extract_response_value(data, path, default=None)
        else:
            # If JSON parsing failed, include the raw text (truncated if necessary)
            result['raw_response'] = (
                self.response.text[:500] + "..." if len(self.response.text) > 500 
                else self.response.text
            )
        
        return result