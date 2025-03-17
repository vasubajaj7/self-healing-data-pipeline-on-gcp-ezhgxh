"""
Example implementation of a custom connector for the self-healing data pipeline.

This connector demonstrates how to extend the BaseConnector class to create a
specialized connector for a custom data source, including error handling,
data extraction, and integration with the pipeline's self-healing capabilities.
"""

import typing
import pandas  # version 2.0.x
import datetime
import requests  # version 2.31.x
import json

from backend.constants import DataSourceType, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS  # Import constants for connector configuration and data source types
from backend.ingestion.connectors.base_connector import BaseConnector, ConnectorFactory  # Import base connector class for inheritance
from backend.ingestion.connectors.base_connector import ConnectorFactory  # Import connector factory for registration
from backend.utils.logging.logger import get_logger  # Configure logging for connector operations
from backend.ingestion.errors.error_handler import handle_error, with_error_handling, retry_with_backoff  # Handle errors during connector operations
from backend.ingestion.extractors.file_extractor import FileExtractor  # Import file extractor for processing files from custom source

# Initialize logger
logger = get_logger(__name__)

# Default API timeout in seconds
DEFAULT_API_TIMEOUT = 30

# Default number of API retries
DEFAULT_API_RETRIES = 3


@ConnectorFactory.register_connector(DataSourceType.CUSTOM)
class CustomAPIConnector(BaseConnector):
    """
    Custom connector implementation for a REST API data source.
    """

    def __init__(self, source_id: str, source_name: str, connection_config: dict):
        """
        Initialize the custom API connector with source information and connection configuration.

        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name of the data source
            connection_config: Configuration parameters for connecting to the source
        """
        # Call parent BaseConnector constructor with source_id, source_name, DataSourceType.CUSTOM, and connection_config
        super().__init__(source_id, source_name, DataSourceType.CUSTOM, connection_config)

        # Initialize requests session to None (will be created on connect)
        self._session: typing.Optional[requests.Session] = None

        # Extract base_url from connection_config
        self.base_url: str = self.connection_config.get('base_url')

        # Extract authentication details from connection_config
        self.auth: typing.Dict[str, str] = self.connection_config.get('auth', {})

        # Extract headers from connection_config
        self.headers: typing.Dict[str, str] = self.connection_config.get('headers', {})

        # Set timeout from connection_config or use default
        self.timeout: int = self.connection_config.get('timeout', DEFAULT_API_TIMEOUT)

        # Validate connection configuration
        if not self.validate_connection_config(connection_config):
            logger.error(f"Invalid connection configuration for {source_name} (ID: {source_id})")
            raise ValueError(f"Invalid connection configuration for {source_name}")

        # Log connector initialization
        logger.info(f"Initialized custom API connector for {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'CustomAPIConnector', 'operation': 'connect'}, raise_exception=False)
    def connect(self) -> bool:
        """
        Establish connection to the custom API.

        Returns:
            True if connection successful, False otherwise
        """
        # Create new requests Session
        self._session = requests.Session()

        # Configure session with headers, auth, and timeout
        self._session.headers.update(self.headers)
        if self.auth:
            self._session.auth = tuple(self.auth.values())  # type: ignore
        self._session.timeout = self.timeout

        # Perform test request to verify connectivity
        try:
            response = self._make_request('GET', '/status')
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            # Update connection state using _update_connection_state
            self._update_connection_state(True, True)
            # Log connection success or failure
            logger.info(f"Successfully connected to {self.source_name} (ID: {self.source_id})")
            # Return connection success status
            return True
        except requests.RequestException as e:
            # Update connection state using _update_connection_state
            self._update_connection_state(False, False)
            # Log connection success or failure
            logger.error(f"Failed to connect to {self.source_name} (ID: {self.source_id}): {str(e)}")
            # Return connection success status
            return False

    @with_error_handling(context={'component': 'CustomAPIConnector', 'operation': 'disconnect'}, raise_exception=False)
    def disconnect(self) -> bool:
        """
        Close connection to the custom API.

        Returns:
            True if disconnection successful, False otherwise
        """
        # Close requests Session if it exists
        if self._session:
            self._session.close()
        # Set session to None
        self._session = None
        # Update connection state using _update_connection_state
        self._update_connection_state(False, True)
        # Log disconnection
        logger.info(f"Disconnected from {self.source_name} (ID: {self.source_id})")
        # Return True (disconnection always succeeds)
        return True

    @with_error_handling(context={'component': 'CustomAPIConnector', 'operation': 'extract_data'}, raise_exception=True)
    def extract_data(self, extraction_params: dict) -> typing.Tuple[typing.Optional[pandas.DataFrame], typing.Dict[str, typing.Any]]:
        """
        Extract data from the custom API based on extraction parameters.

        Args:
            extraction_params: Parameters controlling the extraction process

        Returns:
            (data, metadata) - Extracted data and associated metadata
        """
        # Validate connection is established
        if not self.is_connected:
            raise ConnectionError(f"Not connected to {self.source_name} (ID: {self.source_id})", self.source_name, {})

        # Validate extraction parameters
        if not self._validate_extraction_params(extraction_params):
            raise ValueError(f"Invalid extraction parameters: {extraction_params}")

        # Extract endpoint, method, and query parameters
        endpoint = extraction_params['endpoint']
        method = extraction_params.get('method', 'GET')
        params = extraction_params.get('params', {})
        data = extraction_params.get('data', {})
        headers = extraction_params.get('headers', {})

        # Construct full API URL
        url = f"{self.base_url}{endpoint}"

        # Perform API request with retry logic
        response = self._make_request(method, endpoint, params, data, headers)

        # Parse API response (JSON, XML, etc.)
        df = self._parse_response(response, extraction_params)

        # Generate metadata about extraction
        metadata = self._format_api_metadata({}, response, extraction_params)

        # Return extracted data and metadata
        return df, metadata

    @with_error_handling(context={'component': 'CustomAPIConnector', 'operation': 'get_source_schema'}, raise_exception=True)
    def get_source_schema(self, endpoint_name: str) -> dict:
        """
        Retrieve the schema information for an API endpoint.

        Args:
            endpoint_name: Name of the endpoint to get schema for

        Returns:
            Schema definition for the specified endpoint
        """
        # Validate connection is established
        if not self.is_connected:
            raise ConnectionError(f"Not connected to {self.source_name} (ID: {self.source_id})", self.source_name, {})

        # Construct schema discovery URL (may use OpenAPI/Swagger if available)
        schema_url = f"{self.base_url}/schema/{endpoint_name}"

        # Perform API request to get schema information
        response = self._make_request('GET', schema_url)

        # Parse schema information from response
        try:
            schema = response.json()
        except json.JSONDecodeError:
            # If schema not available, infer from sample data
            logger.warning(f"Schema not available at {schema_url}, inferring from sample data")
            # Extract sample data
            sample_data, _ = self.extract_data({'endpoint': endpoint_name})
            # Infer schema from sample data
            schema = FileExtractor.infer_schema(sample_data, FileFormat.JSON)
        # Return schema as standardized dictionary
        return schema

    def validate_connection_config(self, config: dict) -> bool:
        """
        Validate the custom API connection configuration.

        Args:
            config: Connection configuration to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        # Check if config is a dictionary
        if not isinstance(config, dict):
            logger.error("Connection configuration must be a dictionary")
            return False

        # Verify required fields are present (base_url)
        if 'base_url' not in config:
            logger.error("Connection configuration must contain 'base_url'")
            return False

        # Validate authentication configuration if present
        if 'auth' in config:
            auth = config['auth']
            if not isinstance(auth, dict):
                logger.error("Authentication configuration must be a dictionary")
                return False
            if 'username' not in auth or 'password' not in auth:
                logger.warning("Authentication configuration should contain 'username' and 'password'")

        # Validate optional fields if present (timeout, headers, etc.)
        if 'timeout' in config and not isinstance(config['timeout'], int):
            logger.error("Timeout must be an integer")
            return False
        if 'headers' in config and not isinstance(config['headers'], dict):
            logger.error("Headers must be a dictionary")
            return False

        # Log validation results
        logger.info(f"Connection configuration validated successfully for {self.source_name} (ID: {self.source_id})")
        # Return validation result as boolean
        return True

    @retry_with_backoff(max_retries=DEFAULT_API_RETRIES)
    def _make_request(self, method: str, endpoint: str, params: dict = None, data: dict = None, headers: dict = None) -> requests.Response:
        """
        Make an HTTP request to the API with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            headers: Request headers

        Returns:
            API response object
        """
        # Validate session is established
        if not self._session:
            raise ConnectionError(f"Not connected to {self.source_name} (ID: {self.source_id})", self.source_name, {})

        # Construct full URL from base_url and endpoint
        url = f"{self.base_url}{endpoint}"

        # Merge default headers with request-specific headers
        req_headers = self.headers.copy()
        if headers:
            req_headers.update(headers)

        # Perform HTTP request with specified method, params, and data
        try:
            response = self._session.request(method, url, params=params, json=data, headers=req_headers)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise

        # Check response status code
        if response.status_code >= 400:
            logger.warning(f"API request returned error status: {response.status_code}")

        # Return response object if successful
        return response

    def _parse_response(self, response: requests.Response, extraction_params: dict) -> pandas.DataFrame:
        """
        Parse API response into a pandas DataFrame.

        Args:
            response: API response object
            extraction_params: Extraction parameters

        Returns:
            Parsed response data
        """
        # Check response content type
        content_type = response.headers.get('Content-Type')
        if 'application/json' in content_type:
            # For JSON, parse using json.loads
            data = json.loads(response.text)
            # Extract data using jq-style selectors if specified
            if 'jq_path' in extraction_params:
                # TODO: Implement jq-style selector logic
                logger.warning("jq_path extraction not yet implemented")
                pass
            # Convert to pandas DataFrame
            df = pandas.json_normalize(data)
            # Apply transformations if specified
            if 'transformations' in extraction_params:
                # TODO: Implement transformations
                logger.warning("Transformations not yet implemented")
                pass
            # Handle pagination if needed
            if 'pagination' in extraction_params:
                # TODO: Implement pagination
                logger.warning("Pagination not yet implemented")
                pass
            # Return DataFrame with parsed data
            return df
        else:
            logger.error(f"Unsupported content type: {content_type}")
            raise ValueError(f"Unsupported content type: {content_type}")

    def _validate_extraction_params(self, extraction_params: dict) -> bool:
        """
        Validate extraction parameters for API requests.

        Args:
            extraction_params: Parameters to validate

        Returns:
            True if parameters are valid, False otherwise
        """
        # Check if extraction_params is a dictionary
        if not isinstance(extraction_params, dict):
            logger.error("Extraction parameters must be a dictionary")
            return False

        # Verify endpoint is specified
        if 'endpoint' not in extraction_params:
            logger.error("Extraction parameters must contain 'endpoint'")
            return False

        # Validate HTTP method if specified
        if 'method' in extraction_params:
            method = extraction_params['method']
            if method not in ['GET', 'POST', 'PUT', 'DELETE']:
                logger.error(f"Invalid HTTP method: {method}")
                return False

        # Validate query parameters if present
        if 'params' in extraction_params and not isinstance(extraction_params['params'], dict):
            logger.error("Query parameters must be a dictionary")
            return False

        # Validate data payload if present
        if 'data' in extraction_params and not isinstance(extraction_params['data'], dict):
            logger.error("Data payload must be a dictionary")
            return False

        # Log validation results
        logger.info(f"Extraction parameters validated successfully for {self.source_name} (ID: {self.source_id})")
        # Return validation result as boolean
        return True

    def _format_api_metadata(self, raw_metadata: dict, response: requests.Response, extraction_params: dict) -> dict:
        """
        Format API-specific metadata.

        Args:
            raw_metadata: Raw metadata from the API
            response: API response object
            extraction_params: Extraction parameters

        Returns:
            Formatted metadata
        """
        # Create base metadata structure
        metadata = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_type': self.source_type.value,
            'extraction_time': datetime.datetime.now().isoformat(),
            'api_endpoint': extraction_params.get('endpoint'),
            'http_method': response.request.method,
            'http_status_code': response.status_code,
            'response_time_ms': response.elapsed.total_seconds() * 1000
        }

        # Add API-specific information (endpoint, method, status code)
        # Add response headers (excluding sensitive information)
        # Add timing information (request duration)
        # Add pagination information if applicable
        # Add extraction parameters used
        # Add any additional metadata from raw_metadata

        # Return formatted metadata dictionary
        return metadata