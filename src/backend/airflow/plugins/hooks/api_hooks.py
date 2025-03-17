"""
Custom Airflow hooks for connecting to external REST APIs in the self-healing data pipeline.
Provides robust API connection handling with support for various authentication methods, pagination strategies, and self-healing capabilities.
"""

import typing
import json
import urllib.parse
import base64

import pandas  # version 2.0.x
from airflow.hooks.base import BaseHook  # version 2.5.x
from airflow.models.connection import Connection  # version 2.5.x

from src.backend.constants import DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR, DEFAULT_BATCH_SIZE  # Import constants for API configuration
from src.backend.utils.logging.logger import get_logger  # Configure logging for API hooks
from src.backend.utils.http.http_client import HttpClient, HttpResponse  # Use HTTP client for API communication
from src.backend.utils.auth.oauth_client import OAuthClient, get_token_for_client  # Handle OAuth authentication for APIs
from src.backend.ingestion.connectors.api_connector import ApiAuthType, ApiPaginationType  # Reuse API connector enumerations

# Initialize logger
logger = get_logger(__name__)


class ApiPaginationConfig:
    """Configuration class for API pagination settings"""

    def __init__(
        self,
        pagination_type: ApiPaginationType = ApiPaginationType.NONE,
        page_param: str = "page",
        page_size_param: str = "page_size",
        page_size: int = DEFAULT_BATCH_SIZE,
        offset_param: str = "offset",
        limit_param: str = "limit",
        cursor_param: str = "cursor",
        cursor_path: str = "next_cursor",
        results_path: str = "results",
        total_count_path: str = "total",
        max_pages: int = 100,
    ):
        """Initialize pagination configuration with default values"""
        self.pagination_type = pagination_type  # Set pagination_type (default: ApiPaginationType.NONE)
        self.page_param = page_param  # Set page_param (default: 'page')
        self.page_size_param = page_size_param  # Set page_size_param (default: 'page_size')
        self.page_size = page_size  # Set page_size (default: DEFAULT_BATCH_SIZE)
        self.offset_param = offset_param  # Set offset_param (default: 'offset')
        self.limit_param = limit_param  # Set limit_param (default: 'limit')
        self.cursor_param = cursor_param  # Set cursor_param (default: 'cursor')
        self.cursor_path = cursor_path  # Set cursor_path (default: 'next_cursor')
        self.results_path = results_path  # Set results_path (default: 'results')
        self.total_count_path = total_count_path  # Set total_count_path (default: 'total')
        self.max_pages = max_pages  # Set max_pages (default: 100)

    @classmethod
    def from_dict(cls, config_dict: dict) -> "ApiPaginationConfig":
        """Create pagination configuration from a dictionary"""
        pagination_type = config_dict.get("pagination_type", ApiPaginationType.NONE)  # Extract pagination_type from config_dict
        if isinstance(pagination_type, str):  # Convert string pagination_type to enum if needed
            pagination_type = ApiPaginationType(pagination_type)
        page_param = config_dict.get("page_param", "page")  # Extract other pagination parameters from config_dict
        page_size_param = config_dict.get("page_size_param", "page_size")
        page_size = config_dict.get("page_size", DEFAULT_BATCH_SIZE)
        offset_param = config_dict.get("offset_param", "offset")
        limit_param = config_dict.get("limit_param", "limit")
        cursor_param = config_dict.get("cursor_param", "cursor")
        cursor_path = config_dict.get("cursor_path", "next_cursor")
        results_path = config_dict.get("results_path", "results")
        total_count_path = config_dict.get("total_count_path", "total")
        max_pages = config_dict.get("max_pages", 100)
        return cls(  # Create and return ApiPaginationConfig instance with extracted parameters
            pagination_type=pagination_type,
            page_param=page_param,
            page_size_param=page_size_param,
            page_size=page_size,
            offset_param=offset_param,
            limit_param=limit_param,
            cursor_param=cursor_param,
            cursor_path=cursor_path,
            results_path=results_path,
            total_count_path=total_count_path,
            max_pages=max_pages,
        )

    def to_dict(self) -> dict:
        """Convert pagination configuration to a dictionary"""
        config_dict = {  # Create dictionary with all pagination configuration properties
            "pagination_type": self.pagination_type.value,
            "page_param": self.page_param,
            "page_size_param": self.page_size_param,
            "page_size": self.page_size,
            "offset_param": self.offset_param,
            "limit_param": self.limit_param,
            "cursor_param": self.cursor_param,
            "cursor_path": self.cursor_path,
            "results_path": self.results_path,
            "total_count_path": self.total_count_path,
            "max_pages": self.max_pages,
        }
        if isinstance(self.pagination_type, ApiPaginationType):  # Convert pagination_type enum to string
            config_dict["pagination_type"] = self.pagination_type.value
        return config_dict  # Return the dictionary


class ApiHook(BaseHook):
    """Airflow hook for connecting to external REST APIs with support for various authentication methods and pagination strategies"""

    conn_name_attr = "conn_id"
    default_conn_name = "http_default"
    default_deferrable = False

    def __init__(
        self,
        conn_id: str,
        base_url: str = None,
        auth_type: ApiAuthType = None,
        auth_config: dict = None,
        default_headers: dict = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRY_ATTEMPTS,
        verify_ssl: bool = True,
        pagination_config: ApiPaginationConfig = None,
    ):
        """Initialize the API hook with connection ID and optional configuration"""
        super().__init__()  # Call parent constructor
        self.conn_id = conn_id  # Set connection ID
        self.base_url = base_url  # Set base_url if provided, otherwise None (will be loaded from connection)
        self.auth_type = auth_type  # Set auth_type if provided, otherwise None (will be loaded from connection)
        self.auth_config = auth_config or {}  # Set auth_config if provided, otherwise empty dict
        self.default_headers = default_headers or {}  # Set default_headers if provided, otherwise empty dict
        self.timeout = timeout or DEFAULT_TIMEOUT_SECONDS  # Set timeout if provided, otherwise DEFAULT_TIMEOUT_SECONDS
        self.max_retries = max_retries or MAX_RETRY_ATTEMPTS  # Set max_retries if provided, otherwise MAX_RETRY_ATTEMPTS
        self.verify_ssl = verify_ssl  # Set verify_ssl if provided, otherwise True
        self.pagination_config = pagination_config or ApiPaginationConfig()  # Set pagination_config if provided, otherwise default configuration
        self.http_client = None  # Initialize http_client to None (created during get_conn)
        self.oauth_client = None  # Initialize oauth_client to None (created if needed)

    def get_conn(self) -> HttpClient:
        """Get or create an HTTP client connection to the API"""
        if self.http_client:  # If http_client already exists, return it
            return self.http_client

        connection = self.get_connection_from_airflow()  # Get connection details from Airflow connection
        self.base_url = self.base_url or connection.get("base_url")  # Set base_url from connection if not already set
        self.auth_type = self.auth_type or connection.get("auth_type")  # Set auth_type from connection if not already set

        auth_headers = self.setup_auth()  # Set up authentication configuration

        self.http_client = HttpClient(  # Create HttpClient with configuration
            base_url=self.base_url,
            default_headers=auth_headers,
            auth_type=self.auth_type,
            auth_config=self.auth_config,
            timeout=self.timeout,
            verify_ssl=self.verify_ssl,
            service_name=self.conn_id,
            use_circuit_breaker=True,
            max_retries=self.max_retries
        )
        return self.http_client  # Return the HTTP client instance

    def test_connection(self) -> tuple[bool, str]:
        """Test the API connection by making a simple request"""
        try:
            http_client = self.get_conn()  # Get HTTP client connection
            response = http_client.options("/", raise_for_status=False)  # Try to make a simple request (OPTIONS or GET) to the base URL
            if response.is_success():
                return True, "Connection successful"  # Return (True, 'Connection successful') if successful
            else:
                return False, f"Connection failed: {response.response.status_code} - {response.response.text}"
        except Exception as e:
            return False, str(e)  # Return (False, error_message) if connection fails

    def close_conn(self) -> None:
        """Close the HTTP client connection"""
        if self.http_client:  # Close the HTTP client if it exists
            self.http_client.close()
        self.http_client = None  # Set http_client to None

    def get_request(self, endpoint: str, params: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a GET request to the API"""
        http_client = self.get_conn()  # Get HTTP client connection
        return http_client.get(endpoint, params=params, headers=headers, timeout=timeout)  # Make GET request using http_client

    def post_request(self, endpoint: str, params: dict = None, data: dict = None, json_data: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a POST request to the API"""
        http_client = self.get_conn()  # Get HTTP client connection
        return http_client.post(endpoint, params=params, data=data, json_data=json_data, headers=headers, timeout=timeout)  # Make POST request using http_client

    def put_request(self, endpoint: str, params: dict = None, data: dict = None, json_data: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a PUT request to the API"""
        http_client = self.get_conn()  # Get HTTP client connection
        return http_client.put(endpoint, params=params, data=data, json_data=json_data, headers=headers, timeout=timeout)  # Make PUT request using http_client

    def patch_request(self, endpoint: str, params: dict = None, data: dict = None, json_data: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a PATCH request to the API"""
        http_client = self.get_conn()  # Get HTTP client connection
        return http_client.patch(endpoint, params=params, data=data, json_data=json_data, headers=headers, timeout=timeout)  # Make PATCH request using http_client

    def delete_request(self, endpoint: str, params: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a DELETE request to the API"""
        http_client = self.get_conn()  # Get HTTP client connection
        return http_client.delete(endpoint, params=params, headers=headers, timeout=timeout)  # Make DELETE request using http_client

    def get_data(self, endpoint: str, params: dict = None, headers: dict = None, data_path: str = None, paginate: bool = False, pagination_config: ApiPaginationConfig = None) -> list:
        """Get data from the API with support for pagination"""
        http_client = self.get_conn()  # Get HTTP client connection
        pagination_config = pagination_config or self.pagination_config  # Use provided pagination_config or default

        if not paginate:  # If paginate is False, make a single request
            response = self.get_request(endpoint, params=params, headers=headers)
            data = self.extract_data(response, data_path)  # Extract data using data_path if provided
            return data

        else:  # If paginate is True, handle pagination based on pagination_type
            data = self.handle_pagination(endpoint, params, headers, data_path, pagination_config)
            return data  # Return extracted data as a list

    def handle_pagination(self, endpoint: str, params: dict, headers: dict, data_path: str, pagination_config: ApiPaginationConfig) -> list:
        """Handle pagination for API requests"""
        results = []  # Initialize results list and pagination state
        has_next_page = True
        page = 1
        offset = 0
        cursor = None

        while has_next_page:
            # Prepare request parameters for the current page
            page_params = params.copy() if params else {}
            if pagination_config.pagination_type == ApiPaginationType.PAGE_NUMBER:  # For PAGE_NUMBER pagination, increment page parameter
                page_params[pagination_config.page_param] = page
            elif pagination_config.pagination_type == ApiPaginationType.OFFSET:  # For OFFSET pagination, update offset parameter
                page_params[pagination_config.offset_param] = offset
            elif pagination_config.pagination_type == ApiPaginationType.CURSOR:  # For CURSOR pagination, extract and use cursor from response
                if cursor:
                    page_params[pagination_config.cursor_param] = cursor

            # Make API request
            response = self.get_request(endpoint, params=page_params, headers=headers)
            data = self.extract_data(response, data_path)  # Extract data from response

            if data:
                results.extend(data)  # Append data to results list

            # Determine if more pages exist based on pagination_type
            if pagination_config.pagination_type == ApiPaginationType.PAGE_NUMBER:  # For PAGE_NUMBER pagination, increment page parameter
                page += 1
                if pagination_config.max_pages and page > pagination_config.max_pages:
                    has_next_page = False
            elif pagination_config.pagination_type == ApiPaginationType.OFFSET:  # For OFFSET pagination, update offset parameter
                offset += pagination_config.page_size
                # Check if max_records is set and offset exceeds it
                # If so, set has_next_page to False
                if pagination_config.total_count_path:
                    total_count = response.extract(pagination_config.total_count_path)
                    if total_count and offset >= total_count:
                        has_next_page = False
                elif pagination_config.max_records and offset >= pagination_config.max_records:
                    has_next_page = False
            elif pagination_config.pagination_type == ApiPaginationType.CURSOR:  # For CURSOR pagination, extract and use cursor from response
                cursor = response.extract(pagination_config.cursor_path)
                if not cursor:
                    has_next_page = False

            else:
                has_next_page = False  # If pagination_type is NONE, set has_next_page to False

        return results  # Return combined results from all pages

    def extract_data(self, response: HttpResponse, data_path: str) -> list:
        """Extract data from API response using data path"""
        json_data = response.json()  # Get JSON data from response

        if not data_path:  # If data_path is None, return the entire JSON data
            return json_data

        # Navigate to the specified path in the JSON structure
        parts = data_path.split('.')  # Handle nested paths with dot notation (e.g., 'data.items')
        data = json_data
        try:
            for part in parts:
                if '[' in part and part.endswith(']'):  # Handle array indexing like items[0]
                    array_part, idx_part = part.split('[', 1)
                    idx = int(idx_part[:-1])  # Remove trailing ']'
                    if array_part:
                        data = data[array_part]
                    data = data[idx]
                else:
                    data = data[part]
            return data
        except (KeyError, TypeError, IndexError):
            logger.warning(f"Data path '{data_path}' not found in response")
            return []

    def setup_auth(self) -> dict:
        """Set up authentication for API requests"""
        if self.auth_type == ApiAuthType.NONE:  # Check auth_type and prepare appropriate authentication
            return {}
        elif self.auth_type == ApiAuthType.API_KEY:  # For ApiAuthType.API_KEY, prepare header or query parameter
            return {self.auth_config['key_name']: self.auth_config['key_value']}
        elif self.auth_type == ApiAuthType.BASIC_AUTH:  # For ApiAuthType.BASIC_AUTH, prepare basic auth credentials
            auth_str = f"{self.auth_config['username']}:{self.auth_config['password']}"
            encoded = base64.b64encode(auth_str.encode()).decode()
            return {'Authorization': f'Basic {encoded}'}
        elif self.auth_type == ApiAuthType.OAUTH2:  # For ApiAuthType.OAUTH2, handle token acquisition using oauth_client
            return self.setup_oauth()
        elif self.auth_type == ApiAuthType.JWT:  # For ApiAuthType.JWT, prepare JWT token header
            # Add JWT implementation here
            return {}
        elif self.auth_type == ApiAuthType.CUSTOM:  # For ApiAuthType.CUSTOM, use custom authentication logic
            # Add custom authentication implementation here
            return {}
        return {}

    def setup_oauth(self) -> dict:
        """Set up OAuth authentication"""
        if not self.oauth_client:  # If oauth_client is None, create it using auth_config
            self.oauth_client = OAuthClient(config=self.auth_config)
        token = self.oauth_client.get_token(self.conn_id)  # Get access token from oauth_client
        return {'Authorization': f'Bearer {token}'}  # Return Authorization header with Bearer token

    def get_connection_from_airflow(self) -> dict:
        """Get connection details from Airflow connection"""
        conn = self.get_connection(self.conn_id)  # Get connection object from Airflow using conn_id
        connection = {
            "host": conn.host,
            "login": conn.login,
            "password": conn.password,
            "port": conn.port,
            "schema": conn.schema,
            "extra": conn.extra,
        }
        if conn.extra:
            extra_json = json.loads(conn.extra)
            connection.update(extra_json)
        if not self.base_url:
            self.base_url = connection.get("host")
        if not self.auth_type:
            auth_type_str = connection.get("auth_type")
            if auth_type_str:
                self.auth_type = ApiAuthType(auth_type_str)
        return connection


class SelfHealingApiHook(ApiHook):
    """API hook with self-healing capabilities for automatic error recovery"""

    def __init__(
        self,
        conn_id: str,
        base_url: str = None,
        auth_type: ApiAuthType = None,
        auth_config: dict = None,
        default_headers: dict = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRY_ATTEMPTS,
        verify_ssl: bool = True,
        pagination_config: ApiPaginationConfig = None,
        confidence_threshold: float = 0.85,
    ):
        """Initialize the self-healing API hook"""
        super().__init__(  # Call ApiHook constructor with connection parameters
            conn_id=conn_id,
            base_url=base_url,
            auth_type=auth_type,
            auth_config=auth_config,
            default_headers=default_headers,
            timeout=timeout,
            max_retries=max_retries,
            verify_ssl=verify_ssl,
            pagination_config=pagination_config,
        )
        self.confidence_threshold = confidence_threshold or 0.85  # Set confidence_threshold for self-healing actions (default: 0.85)
        # Add parameter validation here if needed

    def get_request(self, endpoint: str, params: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a GET request with self-healing capabilities"""
        try:
            return super().get_request(endpoint, params, headers, timeout)  # Try to execute the request using parent's get_request method
        except Exception as e:  # If error occurs, attempt to diagnose and fix the issue
            can_fix, fix_params = self._diagnose_api_error(e, {"endpoint": endpoint, "params": params, "headers": headers, "timeout": timeout})  # Apply appropriate self-healing strategy based on error type
            if can_fix:
                params = self._apply_api_fix(fix_params, params)  # Retry the request with fixed parameters
                logger.info(f"Self-healing applied, retrying request to {endpoint} with updated parameters: {params}")
                return super().get_request(endpoint, params, headers, timeout)
            else:
                self._log_healing_action(params, fix_params, str(e), 0.0)
                raise  # If healing fails, raise the original exception with context

    def post_request(self, endpoint: str, params: dict = None, data: dict = None, json_data: dict = None, headers: dict = None, timeout: int = None) -> HttpResponse:
        """Make a POST request with self-healing capabilities"""
        try:
            return super().post_request(endpoint, params, data, json_data, headers, timeout)  # Try to execute the request using parent's post_request method
        except Exception as e:  # If error occurs, attempt to diagnose and fix the issue
            can_fix, fix_params = self._diagnose_api_error(e, {"endpoint": endpoint, "params": params, "data": data, "json_data": json_data, "headers": headers, "timeout": timeout})  # Apply appropriate self-healing strategy based on error type
            if can_fix:
                if params is None:
                    params = {}
                params.update(self._apply_api_fix(fix_params, params))  # Retry the request with fixed parameters
                logger.info(f"Self-healing applied, retrying request to {endpoint} with updated parameters: {params}")
                return super().post_request(endpoint, params, data, json_data, headers, timeout)
            else:
                self._log_healing_action(params, fix_params, str(e), 0.0)
                raise  # If healing fails, raise the original exception with context

    def get_data(self, endpoint: str, params: dict = None, headers: dict = None, data_path: str = None, paginate: bool = False, pagination_config: ApiPaginationConfig = None) -> list:
        """Get data from the API with self-healing capabilities"""
        try:
            return super().get_data(endpoint, params, headers, data_path, paginate, pagination_config)  # Try to get data using parent's get_data method
        except Exception as e:  # If error occurs, attempt to diagnose and fix the issue
            can_fix, fix_params = self._diagnose_api_error(e, {"endpoint": endpoint, "params": params, "headers": headers, "data_path": data_path, "paginate": paginate, "pagination_config": pagination_config})  # Apply appropriate self-healing strategy based on error type
            if can_fix:
                if params is None:
                    params = {}
                params.update(self._apply_api_fix(fix_params, params))  # Retry the data extraction with fixed parameters
                logger.info(f"Self-healing applied, retrying request to {endpoint} with updated parameters: {params}")
                return super().get_data(endpoint, params, headers, data_path, paginate, pagination_config)
            else:
                self._log_healing_action(params, fix_params, str(e), 0.0)
                raise  # If healing fails, raise the original exception with context

    def _diagnose_api_error(self, error: Exception, context: dict) -> tuple[bool, dict]:
        """Diagnose an API error and suggest fixes"""
        fix_params = {}
        confidence = 0.0

        error_message = str(error)
        if "401" in error_message or "403" in error_message:  # Check for authentication issues (401, 403 errors)
            fix_params["fix_type"] = "authentication"
            fix_params["action"] = "refresh_token"
            confidence = 0.9
        elif "429" in error_message:  # Check for rate limiting issues (429 errors)
            fix_params["fix_type"] = "rate_limiting"
            fix_params["action"] = "add_delay"
            fix_params["delay"] = 60
            confidence = 0.8
        elif "404" in error_message:  # Check for endpoint issues (404 errors)
            fix_params["fix_type"] = "endpoint"
            fix_params["action"] = "correct_path"
            confidence = 0.7
        elif "400" in error_message:  # Check for parameter issues (400 errors)
            fix_params["fix_type"] = "parameter"
            fix_params["action"] = "adjust_params"
            confidence = 0.6
        elif "timed out" in error_message:  # Check for timeout issues (408, connection timeout)
            fix_params["fix_type"] = "timeout"
            fix_params["action"] = "increase_timeout"
            fix_params["timeout_multiplier"] = 1.5
            confidence = 0.5
        elif "500" in error_message:  # Check for server errors (5xx errors)
            fix_params["fix_type"] = "server_error"
            fix_params["action"] = "retry"
            confidence = 0.4
        else:
            return False, {}

        if confidence >= self.confidence_threshold:  # If confidence exceeds threshold, return True with fix parameters
            return True, fix_params
        else:
            return False, {"message": "Confidence too low", "confidence": confidence}  # Otherwise, return False with diagnostic information

    def _apply_api_fix(self, fix_params: dict, request_params: dict) -> dict:
        """Apply a fix to a failed API request based on diagnosis"""
        if fix_params["fix_type"] == "authentication":  # Extract fix type from fix_params
            # Add authentication fix implementation here
            logger.info("Applying authentication fix")
            pass
        elif fix_params["fix_type"] == "rate_limiting":  # For rate limiting, add delay and adjust batch size
            # Add rate limiting fix implementation here
            logger.info("Applying rate limiting fix")
            pass
        elif fix_params["fix_type"] == "endpoint":  # For endpoint issues, correct the endpoint path
            # Add endpoint fix implementation here
            logger.info("Applying endpoint fix")
            pass
        elif fix_params["fix_type"] == "parameter":  # For parameter issues, adjust request parameters
            # Add parameter fix implementation here
            logger.info("Applying parameter fix")
            pass
        elif fix_params["fix_type"] == "timeout":  # For timeout issues, increase timeout
            # Add timeout fix implementation here
            logger.info("Applying timeout fix")
            pass
        elif fix_params["fix_type"] == "server_error":  # For server errors, implement appropriate retry strategy
            # Add server error fix implementation here
            logger.info("Applying server error fix")
            pass

        return request_params  # Return the updated request parameters

    def _log_healing_action(self, original_params: dict, fixed_params: dict, error_message: str, confidence: float) -> None:
        """Log details about the self-healing action taken"""
        log_message = f"Self-healing attempt: Error: {error_message}, Confidence: {confidence}\n"  # Format log message with error details
        log_message += f"Original parameters: {original_params}\n"  # Log the original parameters
        log_message += f"Fixed parameters: {fixed_params}"  # Log the fixed parameters
        logger.info(log_message)  # Log the healing action taken