"""
HTTP client implementation for the self-healing data pipeline.

Provides a robust, reusable HTTP client with retry capabilities, circuit breaker pattern
integration, and comprehensive error handling. Supports various authentication methods
and request/response processing.
"""

import json
import enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Union, Any
import urllib.parse

import requests  # version 2.31.x

from ...constants import DEFAULT_MAX_RETRY_ATTEMPTS, AlertSeverity
from ..errors.error_types import (
    ConnectionError, TimeoutError, AuthenticationError, 
    RateLimitError, ServiceUnavailableError, ErrorCategory
)
from ..logging.logger import get_logger
from ..retry.retry_decorator import retry, retry_with_circuit_breaker
from ..retry.circuit_breaker import CircuitBreaker, get_circuit_breaker
from ..http.request_utils import (
    prepare_request_params, prepare_request_headers, prepare_request_body,
    prepare_auth_header, build_url, validate_timeout, format_curl_command,
    RequestConfig
)
from ..http.response_utils import (
    parse_json_response, extract_response_value, is_success_response,
    is_client_error_response, is_server_error_response, format_response_for_logging,
    ResponseParser
)
from ...config import get_config

# Initialize logger
logger = get_logger(__name__)

# Default timeout for HTTP requests in seconds
DEFAULT_TIMEOUT = 60

# Default headers for HTTP requests
DEFAULT_HEADERS = {'User-Agent': 'SelfHealingPipeline/1.0'}


class HttpMethod(enum.Enum):
    """Enumeration of HTTP methods supported by the client."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ApiAuthType(enum.Enum):
    """Enumeration of authentication types supported by the client."""
    NONE = "NONE"
    API_KEY = "API_KEY"
    BASIC = "BASIC"
    OAUTH2 = "OAUTH2"
    JWT = "JWT"


def map_requests_error_to_pipeline_error(exception: Exception, service_name: str) -> Exception:
    """
    Maps requests library exceptions to pipeline-specific error types.
    
    Args:
        exception: Original requests exception
        service_name: Name of the service that caused the error
        
    Returns:
        Mapped pipeline-specific error
    """
    # Map requests exceptions to pipeline-specific error types
    if isinstance(exception, requests.ConnectionError):
        return ConnectionError(
            message=f"Connection error when connecting to {service_name}: {str(exception)}",
            service_name=service_name,
            connection_details={"error": str(exception)},
            retryable=True
        )
    elif isinstance(exception, requests.Timeout):
        return TimeoutError(
            message=f"Request timed out when connecting to {service_name}: {str(exception)}",
            operation=f"http_request_{service_name}",
            timeout_seconds=0,  # Actual timeout unknown from exception
            retryable=True
        )
    elif isinstance(exception, requests.TooManyRedirects):
        return ConnectionError(
            message=f"Too many redirects when connecting to {service_name}: {str(exception)}",
            service_name=service_name,
            connection_details={"error": str(exception), "type": "too_many_redirects"},
            retryable=False
        )
    elif isinstance(exception, requests.HTTPError):
        # Get status code from response
        status_code = getattr(exception.response, 'status_code', 0)
        
        # Map based on status code
        if status_code == 401:
            return AuthenticationError(
                message=f"Authentication failed for {service_name}: {str(exception)}",
                service_name=service_name,
                auth_details={"error": str(exception), "status_code": status_code}
            )
        elif status_code == 403:
            from ..errors.error_types import AuthorizationError
            return AuthorizationError(
                message=f"Authorization failed for {service_name}: {str(exception)}",
                service_name=service_name,
                resource=getattr(exception.response, 'url', 'unknown'),
                action="http_request"
            )
        elif status_code == 429:
            return RateLimitError(
                message=f"Rate limit exceeded for {service_name}: {str(exception)}",
                service_name=service_name,
                rate_limit_details={"error": str(exception), "status_code": status_code},
                retry_after=float(getattr(exception.response.headers, 'Retry-After', '60'))
            )
        elif 500 <= status_code < 600:
            return ServiceUnavailableError(
                message=f"Service {service_name} unavailable: {str(exception)}",
                service_name=service_name,
                service_details={"error": str(exception), "status_code": status_code},
                retryable=True
            )
        else:
            # For other HTTP errors, create a generic ConnectionError
            return ConnectionError(
                message=f"HTTP error when connecting to {service_name}: {str(exception)}",
                service_name=service_name,
                connection_details={"error": str(exception), "status_code": status_code},
                retryable=False
            )
    
    # For any other exceptions, return as is
    return exception


class HttpResponse:
    """Wrapper class for HTTP responses with additional utility methods."""
    
    def __init__(self, response: requests.Response):
        """
        Initialize with a requests.Response object.
        
        Args:
            response: The HTTP response to wrap
        """
        self.response = response
        self.parser = ResponseParser(response)
    
    def is_success(self) -> bool:
        """
        Check if the response indicates success (2xx status code).
        
        Returns:
            True if response indicates success
        """
        return is_success_response(self.response)
    
    def is_client_error(self) -> bool:
        """
        Check if the response indicates a client error (4xx status code).
        
        Returns:
            True if response indicates a client error
        """
        return is_client_error_response(self.response)
    
    def is_server_error(self) -> bool:
        """
        Check if the response indicates a server error (5xx status code).
        
        Returns:
            True if response indicates a server error
        """
        return is_server_error_response(self.response)
    
    def json(self, raise_error: bool = False) -> Union[dict, list, None]:
        """
        Parse response body as JSON.
        
        Args:
            raise_error: Whether to raise an error if parsing fails
            
        Returns:
            Parsed JSON data or None if parsing fails
        """
        return self.parser.parse_json(raise_error)
    
    def extract(self, path: str, default: Any = None) -> Any:
        """
        Extract a specific value from JSON response using a path.
        
        Args:
            path: Dot-notation path to the value
            default: Default value to return if not found
            
        Returns:
            Extracted value or default if not found
        """
        return self.parser.extract_from_response(path, default)
    
    def raise_for_status(self) -> None:
        """
        Raise an exception for unsuccessful status codes.
        
        Raises:
            ConnectionError, AuthenticationError, etc.: Based on status code
        """
        if not self.is_success():
            # Get error details if available
            error_details = {}
            try:
                # Try to parse error from JSON response
                json_data = self.json(raise_error=False)
                if json_data and isinstance(json_data, dict):
                    error_details = json_data
            except Exception:
                pass
            
            # Get status code
            status_code = self.response.status_code
            
            # Generate appropriate error based on status code
            if 400 <= status_code < 500:
                if status_code == 401:
                    raise AuthenticationError(
                        message=f"Authentication failed: {self.response.reason}",
                        service_name=self.response.url,
                        auth_details=error_details
                    )
                elif status_code == 403:
                    from ..errors.error_types import AuthorizationError
                    raise AuthorizationError(
                        message=f"Authorization failed: {self.response.reason}",
                        service_name=self.response.url,
                        resource=self.response.url,
                        action="http_request"
                    )
                elif status_code == 429:
                    retry_after = self.response.headers.get('Retry-After', '60')
                    raise RateLimitError(
                        message=f"Rate limit exceeded: {self.response.reason}",
                        service_name=self.response.url,
                        rate_limit_details=error_details,
                        retry_after=float(retry_after)
                    )
                else:
                    raise ConnectionError(
                        message=f"Client error: {status_code} {self.response.reason}",
                        service_name=self.response.url,
                        connection_details={"status_code": status_code, "details": error_details},
                        retryable=False
                    )
            elif 500 <= status_code < 600:
                raise ServiceUnavailableError(
                    message=f"Server error: {status_code} {self.response.reason}",
                    service_name=self.response.url,
                    service_details={"status_code": status_code, "details": error_details},
                    retryable=True
                )
    
    def get_error_details(self, error_paths: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Extract error details from an error response.
        
        Args:
            error_paths: Dictionary mapping of result keys to response paths
            
        Returns:
            Dictionary containing extracted error details
        """
        if error_paths is None:
            error_paths = {
                "message": "error.message",
                "code": "error.code",
                "details": "error.details"
            }
        
        return self.parser.get_error_details(error_paths)
    
    def log_response(self, level: str = "debug", include_body: bool = True) -> None:
        """
        Log the response details at the specified level.
        
        Args:
            level: Log level (debug, info, warning, error)
            include_body: Whether to include response body in log
        """
        formatted = format_response_for_logging(self.response, include_body)
        
        log_method = getattr(logger, level.lower(), logger.debug)
        log_method(f"HTTP Response: {formatted}")


class HttpClient:
    """HTTP client with retry, circuit breaker, and error handling capabilities."""
    
    def __init__(
        self,
        base_url: str,
        default_headers: Dict[str, str] = None,
        auth_type: ApiAuthType = ApiAuthType.NONE,
        auth_config: Dict[str, Any] = None,
        timeout: int = DEFAULT_TIMEOUT,
        verify_ssl: bool = True,
        service_name: str = None,
        use_circuit_breaker: bool = False,
        max_retries: int = DEFAULT_MAX_RETRY_ATTEMPTS
    ):
        """
        Initialize the HTTP client with configuration.
        
        Args:
            base_url: Base URL for the API
            default_headers: Default headers to include in all requests
            auth_type: Authentication type to use
            auth_config: Authentication configuration parameters
            timeout: Default timeout in seconds
            verify_ssl: Whether to verify SSL certificates
            service_name: Service name for error reporting and circuit breaker
            use_circuit_breaker: Whether to use circuit breaker pattern
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url
        self.default_headers = default_headers or {}
        self.auth_type = auth_type
        self.auth_config = auth_config or {}
        self.timeout = validate_timeout(timeout)
        self.verify_ssl = verify_ssl
        self.service_name = service_name or urllib.parse.urlparse(base_url).netloc
        self.use_circuit_breaker = use_circuit_breaker
        self.max_retries = max_retries
        
        # Create session for connection pooling
        self.session = requests.Session()
        
        # Set default session parameters
        self.session.verify = verify_ssl
        
        # Initialize response parser
        self.parser = None
    
    def request(
        self,
        method: Union[str, HttpMethod],
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        data: Union[Dict[str, Any], str, bytes] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = None,
        verify: bool = None,
        raise_for_status: bool = False,
        allow_redirects: bool = True
    ) -> HttpResponse:
        """
        Send an HTTP request with retry and circuit breaker capabilities.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            json_data: JSON data to send in request body
            timeout: Request timeout in seconds
            verify: Whether to verify SSL certificates
            raise_for_status: Whether to raise an exception for error status codes
            allow_redirects: Whether to follow redirects
            
        Returns:
            HttpResponse wrapper around the response
        """
        # Prepare URL by joining base_url and path
        url = build_url(self.base_url, path)
        
        # Prepare request parameters
        params = prepare_request_params(params)
        
        # Prepare request headers
        headers = prepare_request_headers(headers, self.default_headers)
        
        # Prepare request body
        body_kwargs = prepare_request_body(data, json_data)
        
        # Prepare authentication headers if needed
        if self.auth_type != ApiAuthType.NONE and self.auth_config:
            auth_headers = prepare_auth_header(self.auth_type.value, self.auth_config)
            headers.update(auth_headers)
        
        # Set timeout and verify values
        timeout = validate_timeout(timeout) if timeout is not None else self.timeout
        verify = verify if verify is not None else self.verify_ssl
        
        # Prepare request kwargs
        kwargs = {
            'params': params,
            'headers': headers,
            'timeout': timeout,
            'verify': verify,
            'allow_redirects': allow_redirects,
            **body_kwargs
        }
        
        # Log the request details
        if isinstance(method, HttpMethod):
            method_str = method.value
        else:
            method_str = method
            
        logger.debug(f"HTTP {method_str} request to {url}")
        logger.debug(f"Request details: {format_curl_command(method_str, url, headers, data, json_data)}")
        
        # Execute the request with retry and circuit breaker
        try:
            response = self._execute_request(method_str, url, kwargs)
            
            # Create HttpResponse wrapper
            http_response = HttpResponse(response)
            
            # Log response details
            http_response.log_response(level="debug")
            
            # Raise exception if raise_for_status is True and response indicates an error
            if raise_for_status:
                http_response.raise_for_status()
            
            return http_response
        
        except Exception as e:
            # Log error
            logger.error(f"Error executing HTTP request: {str(e)}")
            
            # Map to pipeline-specific error if possible
            mapped_error = map_requests_error_to_pipeline_error(e, self.service_name)
            
            # Re-raise the mapped error
            raise mapped_error
    
    def _execute_request(self, method: str, url: str, kwargs: Dict[str, Any]) -> requests.Response:
        """
        Execute the HTTP request with retry and circuit breaker.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            kwargs: Request parameters
            
        Returns:
            Raw response from the request
        """
        # Define the request function
        def make_request():
            try:
                return self.session.request(method=method, url=url, **kwargs)
            except Exception as e:
                # Map to pipeline-specific error if possible
                raise map_requests_error_to_pipeline_error(e, self.service_name)
        
        # Use circuit breaker if enabled
        if self.use_circuit_breaker:
            # Get or create a circuit breaker for this service
            circuit_breaker = get_circuit_breaker(self.service_name)
            
            # Create a decorated function with retry and circuit breaker
            decorated_function = retry(
                max_attempts=self.max_retries,
                exceptions_to_retry=[Exception],
                use_circuit_breaker=True,
                circuit_breaker_service=self.service_name
            )(make_request)
            
            # Execute with retry and circuit breaker
            return decorated_function()
        else:
            # Use standard retry without circuit breaker
            decorated_function = retry(
                max_attempts=self.max_retries,
                exceptions_to_retry=[Exception]
            )(make_request)
            
            # Execute with retry
            return decorated_function()
    
    def get(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a GET request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.GET,
            path=path,
            params=params,
            headers=headers,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def post(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        data: Union[Dict[str, Any], str, bytes] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a POST request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            json_data: JSON data to send in request body
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.POST,
            path=path,
            params=params,
            headers=headers,
            data=data,
            json_data=json_data,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def put(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        data: Union[Dict[str, Any], str, bytes] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a PUT request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            json_data: JSON data to send in request body
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.PUT,
            path=path,
            params=params,
            headers=headers,
            data=data,
            json_data=json_data,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def delete(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        data: Union[Dict[str, Any], str, bytes] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a DELETE request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            json_data: JSON data to send in request body
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.DELETE,
            path=path,
            params=params,
            headers=headers,
            data=data,
            json_data=json_data,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def patch(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        data: Union[Dict[str, Any], str, bytes] = None,
        json_data: Dict[str, Any] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a PATCH request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            data: Request body data
            json_data: JSON data to send in request body
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.PATCH,
            path=path,
            params=params,
            headers=headers,
            data=data,
            json_data=json_data,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def head(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send a HEAD request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.HEAD,
            path=path,
            params=params,
            headers=headers,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def options(
        self,
        path: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        timeout: int = None,
        raise_for_status: bool = False
    ) -> HttpResponse:
        """
        Send an OPTIONS request.
        
        Args:
            path: URL path to append to base_url
            params: Query parameters
            headers: HTTP headers
            timeout: Request timeout in seconds
            raise_for_status: Whether to raise an exception for error status codes
            
        Returns:
            HttpResponse wrapper around the response
        """
        return self.request(
            method=HttpMethod.OPTIONS,
            path=path,
            params=params,
            headers=headers,
            timeout=timeout,
            raise_for_status=raise_for_status
        )
    
    def close(self) -> None:
        """
        Close the session and release resources.
        """
        if self.session:
            self.session.close()
            logger.debug(f"HTTP client for {self.service_name} closed")
    
    def __enter__(self):
        """
        Context manager entry point.
        
        Returns:
            Self reference for context manager
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.close()