"""
Package initialization file for the HTTP utilities module.

Exposes key classes and functions from the HTTP client, request utilities, and response
utilities submodules to provide a unified interface for HTTP operations throughout the
self-healing data pipeline.
"""

# Import HTTP client classes and utilities
from .http_client import (
    HttpClient,
    HttpResponse,
    HttpMethod,
    ApiAuthType,
    map_requests_error_to_pipeline_error
)

# Import request preparation utilities
from .request_utils import (
    prepare_request_params,
    prepare_request_headers,
    prepare_request_body,
    prepare_auth_header,
    build_url,
    validate_timeout,
    format_curl_command,
    RequestConfig
)

# Import response handling utilities
from .response_utils import (
    parse_json_response,
    extract_response_value,
    is_success_response,
    is_client_error_response,
    is_server_error_response,
    is_redirect_response,
    get_response_content_type,
    is_json_response,
    format_response_for_logging,
    ResponseParser
)

# Define the public API
__all__ = [
    # HTTP client classes
    'HttpClient',
    'HttpResponse',
    'HttpMethod',
    'ApiAuthType',
    'map_requests_error_to_pipeline_error',
    
    # Request utilities
    'prepare_request_params',
    'prepare_request_headers',
    'prepare_request_body',
    'prepare_auth_header',
    'build_url',
    'validate_timeout',
    'format_curl_command',
    'RequestConfig',
    
    # Response utilities
    'parse_json_response',
    'extract_response_value',
    'is_success_response',
    'is_client_error_response',
    'is_server_error_response',
    'is_redirect_response',
    'get_response_content_type',
    'is_json_response',
    'format_response_for_logging',
    'ResponseParser'
]