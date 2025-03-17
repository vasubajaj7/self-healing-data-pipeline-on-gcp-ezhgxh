"""
Implementation of the API connector for the self-healing data pipeline.
This connector is responsible for establishing connections to external REST APIs,
handling authentication, managing API requests, and extracting data with support
for various pagination strategies and error recovery mechanisms.
"""

import typing
import json
import enum
import urllib.parse
import pandas  # version 2.0.x
from datetime import datetime

from .base_connector import BaseConnector, ConnectorFactory  # Ensure correct usage of BaseConnector methods
from ...constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS  # Ensure constants are used correctly
from ...utils.logging.logger import get_logger  # Ensure logger is used for all operations
from ...utils.http.http_client import HttpClient, HttpResponse  # Ensure HttpClient is used for API communication
from ...utils.retry.retry_decorator import retry_with_backoff  # Ensure retry logic is applied to API operations
from ..errors.error_handler import handle_error, with_error_handling  # Ensure error handling is applied to all operations

# Initialize logger
logger = get_logger(__name__)


class ApiAuthType(enum.Enum):
    """Enumeration of supported API authentication types"""
    NONE = "NONE"
    API_KEY = "API_KEY"
    BASIC_AUTH = "BASIC_AUTH"
    OAUTH2 = "OAUTH2"
    JWT = "JWT"
    CUSTOM = "CUSTOM"

    def __init__(self):
        """Initialize the enum"""
        pass


class ApiPaginationType(enum.Enum):
    """Enumeration of supported API pagination strategies"""
    NONE = "NONE"
    PAGE_NUMBER = "PAGE_NUMBER"
    OFFSET = "OFFSET"
    CURSOR = "CURSOR"
    LINK_HEADER = "LINK_HEADER"

    def __init__(self):
        """Initialize the enum"""
        pass


class ApiConnector(BaseConnector):
    """
    Connector for external REST APIs with support for various authentication methods,
    pagination strategies, and error recovery
    """

    def __init__(
        self,
        source_id: str,
        source_name: str,
        source_type: DataSourceType,
        connection_config: dict
    ):
        """
        Initialize the API connector with connection configuration

        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            source_type: Type of data source (from DataSourceType enum)
            connection_config: Configuration parameters for connecting to the source
        """
        # Call parent constructor with source information
        super().__init__(source_id, source_name, source_type, connection_config)

        # Extract API-specific configuration parameters
        self.base_url = connection_config.get("base_url")
        self.auth_type = ApiAuthType(connection_config.get("auth_type", ApiAuthType.NONE.value))
        self.auth_config = connection_config.get("auth_config", {})
        self.timeout = connection_config.get("timeout", DEFAULT_TIMEOUT_SECONDS)
        self.max_retries = connection_config.get("max_retries", MAX_RETRY_ATTEMPTS)
        self.verify_ssl = connection_config.get("verify_ssl", True)
        self.default_headers = connection_config.get("default_headers", {})
        self.pagination_type = ApiPaginationType(connection_config.get("pagination_type", ApiPaginationType.NONE.value))
        self.pagination_config = connection_config.get("pagination_config", {})
        self.rate_limit_config = connection_config.get("rate_limit_config", {})

        # Initialize api_stats dictionary for tracking API usage
        self.api_stats = {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "total_time_ms": 0,
            "avg_time_ms": 0,
            "status_codes": {}
        }

        # Initialize http_client to None (created during connect)
        self.http_client = None

        logger.info(f"Initialized API connector for {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'ApiConnector', 'operation': 'connect'}, raise_exception=False)
    def connect(self) -> bool:
        """
        Establish connection to the API

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Create HttpClient instance with configuration
            self.http_client = HttpClient(
                base_url=self.base_url,
                default_headers=self.default_headers,
                auth_type=self.auth_type,
                auth_config=self.auth_config,
                timeout=self.timeout,
                verify_ssl=self.verify_ssl,
                service_name=self.source_name,
                use_circuit_breaker=True,
                max_retries=self.max_retries
            )

            # Set up authentication based on auth_type
            auth_headers = self.setup_authentication()
            if auth_headers:
                self.http_client.default_headers.update(auth_headers)

            # Make a test request to verify connectivity
            test_response = self.http_client.get("/", raise_for_status=False)
            if not test_response.is_success():
                logger.warning(f"Test request failed: {test_response.response.status_code} - {test_response.response.text}")
                return False

            # Update connection state and statistics
            self._update_connection_state(connected=True, success=True)
            self.successful_connections += 1
            logger.info(f"Successfully connected to API for {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            # Update connection state and statistics
            self._update_connection_state(connected=False, success=False)
            self.failed_connections += 1
            logger.error(f"Failed to connect to API for {self.source_name} (ID: {self.source_id}): {str(e)}")
            return False

    @with_error_handling(context={'component': 'ApiConnector', 'operation': 'disconnect'}, raise_exception=False)
    def disconnect(self) -> bool:
        """
        Close connection to the API

        Returns:
            True if disconnection successful, False otherwise
        """
        try:
            # Close the HTTP client if it exists
            if self.http_client:
                self.http_client.close()
                self.http_client = None

            # Update connection state and statistics
            self._update_connection_state(connected=False, success=True)
            logger.info(f"Successfully disconnected from API for {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            # Update connection state and statistics
            self._update_connection_state(connected=False, success=False)
            logger.error(f"Failed to disconnect from API for {self.source_name} (ID: {self.source_id}): {str(e)}")
            return False

    @with_error_handling(context={'component': 'ApiConnector', 'operation': 'extract_data'}, raise_exception=True)
    def extract_data(self, extraction_params: dict) -> typing.Tuple[typing.Optional[pandas.DataFrame], dict]:
        """
        Extract data from the API based on extraction parameters

        Args:
            extraction_params: Parameters controlling the extraction process,
                including what data to extract and how to extract it

        Returns:
            Tuple containing:
                - Extracted data as pandas DataFrame (or None if extraction failed)
                - Metadata dictionary with extraction details
        """
        # Validate extraction parameters
        if not self._validate_extraction_params(extraction_params):
            raise ValueError("Invalid extraction parameters")

        # Ensure connection is established
        if not self.is_connected:
            if not self.connect():
                raise ConnectionError(f"Failed to connect to API for data extraction", service_name=self.source_name, connection_details={})

        # Prepare request parameters (endpoint, method, headers, etc.)
        endpoint_path = extraction_params.get("endpoint_path")
        method = extraction_params.get("method", "GET")
        params = extraction_params.get("params", {})
        headers = extraction_params.get("headers", {})
        json_data = extraction_params.get("json_data")
        data = extraction_params.get("data")

        # Handle pagination if enabled
        if self.pagination_type != ApiPaginationType.NONE:
            all_results = self.handle_pagination(method, endpoint_path, params, headers, json_data, data)
        else:
            # Make API request(s) with retry logic
            response = self.make_request(method, endpoint_path, params, headers, json_data, data)
            all_results = [response]

        # Process response data into standardized format
        extracted_data = []
        response_metadata = {}
        for response in all_results:
            data, metadata = self.process_response(response, extraction_params)
            if data is not None:
                extracted_data.append(data)
            response_metadata.update(metadata)

        # Combine data from all pages into a single DataFrame
        if extracted_data:
            combined_data = pandas.concat(extracted_data, ignore_index=True)
        else:
            combined_data = None

        # Collect metadata about the extraction
        metadata = self._format_metadata(response_metadata)

        # Update API statistics
        self._update_api_stats(response, combined_data is not None, metadata.get("extraction_time_ms", 0))

        # Return extracted data and metadata
        return combined_data, metadata

    @with_error_handling(context={'component': 'ApiConnector', 'operation': 'get_source_schema'}, raise_exception=True)
    def get_source_schema(self, endpoint_path: str) -> dict:
        """
        Retrieve the schema information for an API endpoint

        Args:
            endpoint_path: Path to the API endpoint

        Returns:
            Schema definition for the specified endpoint
        """
        # Ensure connection is established
        if not self.is_connected:
            if not self.connect():
                raise ConnectionError(f"Failed to connect to API for schema retrieval", service_name=self.source_name, connection_details={})

        # Check if schema discovery is supported for this API
        # Make OPTIONS request to endpoint if supported
        # Alternatively, make a sample GET request and infer schema
        # Process response to extract schema information
        # Format schema in standardized structure
        # Return schema definition dictionary
        raise NotImplementedError("Schema discovery not yet implemented for API connector")

    def validate_connection_config(self, config: dict) -> bool:
        """
        Validate the API connection configuration

        Args:
            config: Connection configuration to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        # Check for required parameters (base_url)
        if "base_url" not in config:
            logger.error("Missing required parameter: base_url")
            return False

        # Validate URL format
        try:
            urllib.parse.urlparse(config["base_url"])
        except Exception:
            logger.error("Invalid URL format for base_url")
            return False

        # Validate authentication configuration based on auth_type
        auth_type = config.get("auth_type", ApiAuthType.NONE.value)
        if auth_type != ApiAuthType.NONE.value:
            auth_config = config.get("auth_config", {})
            if not auth_config:
                logger.error("Missing authentication configuration")
                return False
            # Add more specific validation for each auth type if needed

        # Validate pagination configuration if specified
        pagination_type = config.get("pagination_type", ApiPaginationType.NONE.value)
        if pagination_type != ApiPaginationType.NONE.value:
            pagination_config = config.get("pagination_config", {})
            if not pagination_config:
                logger.error("Missing pagination configuration")
                return False
            # Add more specific validation for each pagination type if needed

        # Validate other API-specific parameters
        # Add more validation as needed

        return True

    def setup_authentication(self) -> dict:
        """
        Set up authentication based on the configured authentication type

        Returns:
            Authentication configuration for requests
        """
        # Check auth_type and prepare appropriate authentication
        # For API_KEY, prepare header or query parameter
        # For BASIC_AUTH, prepare basic auth credentials
        # For OAUTH2, handle token acquisition and refresh
        # For JWT, prepare JWT token header
        # For CUSTOM, use custom authentication logic
        # Return authentication configuration
        return {}

    @retry_with_backoff(max_attempts=3, service_name='api_service', failure_threshold=5, reset_timeout=60.0)
    def make_request(self, method: str, endpoint_path: str, params: dict = None, headers: dict = None, json_data: dict = None, data: typing.Any = None) -> HttpResponse:
        """
        Make an HTTP request to the API with retry and error handling

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint_path: Path to the API endpoint
            params: Query parameters
            headers: HTTP headers
            json_data: JSON data to send in request body
            data: Request body data

        Returns:
            Response from the API
        """
        # Construct full URL from base_url and endpoint_path
        url = urllib.parse.urljoin(self.base_url, endpoint_path)

        # Merge default_headers with provided headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        # Apply authentication to request
        # Make HTTP request using http_client
        if self.http_client is None:
            raise ConnectionError("HTTP Client not initialized. Call connect() first.", service_name=self.source_name, connection_details={})
        
        response = self.http_client.request(method=method, path=endpoint_path, params=params, headers=request_headers, json_data=json_data, data=data)

        # Handle rate limiting if encountered
        self.handle_rate_limiting(response)

        # Update API statistics with request information
        self._update_api_stats(response, response.is_success(), response.response.elapsed.total_seconds())

        # Return response object
        return response

    def handle_pagination(self, method: str, endpoint_path: str, params: dict = None, headers: dict = None, json_data: dict = None, data: typing.Any = None) -> list:
        """
        Handle pagination for API requests based on pagination configuration

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint_path: Path to the API endpoint
            params: Query parameters
            headers: HTTP headers
            json_data: JSON data to send in request body
            data: Request body data

        Returns:
            Combined results from all pages
        """
        # Check if pagination is enabled
        if self.pagination_type == ApiPaginationType.NONE:
            return [self.make_request(method, endpoint_path, params, headers, json_data, data)]

        # Initialize results list and pagination state
        all_results = []
        has_next_page = True
        current_page = 1
        offset = 0
        cursor = None
        next_link = None

        # Make initial request
        while has_next_page:
            # Prepare request parameters for the current page
            page_params = params.copy() if params else {}
            if self.pagination_type == ApiPaginationType.PAGE_NUMBER:
                page_params[self.pagination_config.get("page_param", "page")] = current_page
            elif self.pagination_type == ApiPaginationType.OFFSET:
                page_params[self.pagination_config.get("offset_param", "offset")] = offset
            elif self.pagination_type == ApiPaginationType.CURSOR:
                if cursor:
                    page_params[self.pagination_config.get("cursor_param", "cursor")] = cursor
            elif self.pagination_type == ApiPaginationType.LINK_HEADER:
                if next_link:
                    endpoint_path = next_link  # Override endpoint path with next link

            # Make API request
            response = self.make_request(method, endpoint_path, page_params, headers, json_data, data)

            # Process response and extract data
            all_results.append(response)

            # Determine if more pages exist based on pagination_type
            if self.pagination_type == ApiPaginationType.PAGE_NUMBER:
                current_page += 1
                if self.pagination_config.get("max_pages") and current_page > self.pagination_config["max_pages"]:
                    has_next_page = False
            elif self.pagination_type == ApiPaginationType.OFFSET:
                offset += self.pagination_config.get("page_size", 100)
                if self.pagination_config.get("max_records") and offset >= self.pagination_config["max_records"]:
                    has_next_page = False
            elif self.pagination_type == ApiPaginationType.CURSOR:
                cursor = response.extract(self.pagination_config.get("next_cursor_path"))
                if not cursor:
                    has_next_page = False
            elif self.pagination_type == ApiPaginationType.LINK_HEADER:
                next_link = response.response.links.get("next", {}).get("url")
                if not next_link:
                    has_next_page = False
            else:
                has_next_page = False  # Should not happen, but just in case

        # Combine results from all pages
        return all_results

    def process_response(self, response: HttpResponse, extraction_params: dict) -> typing.Tuple[typing.Optional[pandas.DataFrame], dict]:
        """
        Process API response into standardized format

        Args:
            response: Response from the API
            extraction_params: Extraction parameters

        Returns:
            Tuple containing:
                - Extracted data as pandas DataFrame
                - Response metadata
        """
        # Check response status and handle errors
        if not response.is_success():
            logger.warning(f"API request failed: {response.response.status_code} - {response.response.text}")
            return None, {}

        # Extract data from response based on content type
        try:
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            return None, {}

        # Apply data path selector if specified in extraction_params
        data_path = extraction_params.get("data_path")
        if data_path:
            data = response.extract(data_path)

        # Convert data to appropriate format (JSON, DataFrame, etc.)
        df = pandas.json_normalize(data)

        # Apply field mappings if specified
        field_mappings = extraction_params.get("field_mappings")
        if field_mappings:
            df = df.rename(columns=field_mappings)

        # Apply filters if specified
        # Add filter implementation here

        # Collect response metadata (timing, headers, etc.)
        metadata = {
            "status_code": response.response.status_code,
            "content_type": response.response.headers.get("Content-Type"),
            "record_count": len(df) if df is not None else 0,
            "extraction_time_ms": response.response.elapsed.total_seconds() * 1000
        }

        return df, metadata

    def handle_rate_limiting(self, response: HttpResponse) -> bool:
        """
        Handle rate limiting for API requests

        Args:
            response: Response from the API

        Returns:
            True if rate limiting was handled, False otherwise
        """
        # Check if response indicates rate limiting (429 status)
        if response.response.status_code == 429:
            # Extract rate limit information from headers
            retry_after = response.response.headers.get("Retry-After")

            # Calculate appropriate wait time
            wait_time = int(retry_after) if retry_after else 60  # Default to 60 seconds

            # Log rate limiting information
            logger.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")

            # Wait for the specified time
            time.sleep(wait_time)

            # Update rate limiting statistics
            # Add rate limiting statistics implementation here

            return True

        return False

    def get_api_stats(self) -> dict:
        """
        Get statistics about API usage

        Returns:
            API usage statistics
        """
        # Collect current API statistics
        # Calculate derived metrics (success rate, average time, etc.)
        # Return formatted statistics dictionary
        return self.api_stats

    def reset_api_stats(self) -> None:
        """
        Reset the API usage statistics
        """
        # Reset all API statistics to initial values
        self.api_stats = {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "total_time_ms": 0,
            "avg_time_ms": 0,
            "status_codes": {}
        }

    def _update_api_stats(self, response: HttpResponse, success: bool, duration: float) -> None:
        """
        Update API statistics after a request

        Args:
            response: The HTTP response
            success: Whether the request was successful
            duration: The request duration in milliseconds
        """
        # Update request counters
        self.api_stats["requests"] += 1

        # Update success/failure counters
        if success:
            self.api_stats["successes"] += 1
        else:
            self.api_stats["failures"] += 1

        # Update timing statistics
        self.api_stats["total_time_ms"] += duration
        self.api_stats["avg_time_ms"] = self.api_stats["total_time_ms"] / self.api_stats["requests"]

        # Update status code statistics
        status_code = str(response.response.status_code)
        if status_code in self.api_stats["status_codes"]:
            self.api_stats["status_codes"][status_code] += 1
        else:
            self.api_stats["status_codes"][status_code] = 1

        # Log statistics update if verbose
        logger.debug(f"Updated API statistics: {self.api_stats}")


# Register the API connector with the connector factory
ConnectorFactory().register_connector(DataSourceType.API, ApiConnector)