"""
Implementation of the Google Cloud Storage connector for the self-healing data pipeline.
This connector provides a robust interface for extracting data from GCS buckets,
handling various file formats, and integrating with the pipeline's error handling
and self-healing capabilities.
"""

import typing
import pandas  # version 2.0.x
from datetime import datetime  # standard library
from google.cloud import exceptions  # version 2.9.0+

from ...constants import DataSourceType, FileFormat, DEFAULT_TIMEOUT_SECONDS, MAX_RETRY_ATTEMPTS
from .base_connector import BaseConnector, ConnectorFactory
from ...utils.logging.logger import get_logger
from ...utils.storage.gcs_client import GCSClient
from ..errors.error_handler import handle_error, with_error_handling
from ..extractors.file_extractor import FileExtractor

# Initialize logger
logger = get_logger(__name__)


@ConnectorFactory.register_connector(DataSourceType.GCS)
class GCSConnector(BaseConnector):
    """
    Connector for Google Cloud Storage that provides data extraction capabilities
    from GCS buckets.
    """

    def __init__(
        self,
        source_id: str,
        source_name: str,
        connection_config: dict
    ):
        """
        Initialize the GCS connector with source information and connection configuration.

        Args:
            source_id (str): Unique identifier for the data source.
            source_name (str): Human-readable name of the data source.
            connection_config (dict): Configuration parameters for connecting to GCS.
        """
        # Call parent BaseConnector constructor with source details
        super().__init__(
            source_id=source_id,
            source_name=source_name,
            source_type=DataSourceType.GCS,
            connection_config=connection_config
        )

        # Initialize GCS client to None (will be created on connect)
        self._gcs_client: typing.Optional[GCSClient] = None

        # Initialize file extractor for processing GCS files
        self._file_extractor: FileExtractor = FileExtractor(
            source_id=source_id,
            source_name=source_name,
            extraction_config=connection_config
        )

        # Initialize bucket cache dictionary for performance
        self.bucket_cache: dict = {}

        # Validate connection configuration
        if not self.validate_connection_config(connection_config):
            logger.error(f"Invalid connection configuration for {source_name} (ID: {source_id})")
            raise ValueError(f"Invalid connection configuration for {source_name}")

        logger.info(f"Initialized GCS connector for {source_name} (ID: {source_id})")

    @with_error_handling(context={'component': 'GCSConnector', 'operation': 'connect'}, raise_exception=False)
    def connect(self) -> bool:
        """
        Establish connection to Google Cloud Storage.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            # Extract project_id and location from connection_config
            project_id = self.connection_config.get('project_id')
            location = self.connection_config.get('location')

            # Create GCSClient instance with project_id and location
            self._gcs_client = GCSClient(project_id=project_id, location=location)

            # Verify connection by listing buckets or accessing a test bucket
            # For example, list the first 5 buckets
            buckets = self._gcs_client.list_buckets(max_results=5)
            logger.debug(f"Successfully listed buckets: {buckets}")

            # Update connection state
            self._update_connection_state(connected=True, success=True)
            logger.info(f"Successfully connected to GCS for {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            # Handle connection errors
            logger.error(f"Failed to connect to GCS: {str(e)}")
            self._update_connection_state(connected=False, success=False)
            return False

    @with_error_handling(context={'component': 'GCSConnector', 'operation': 'disconnect'}, raise_exception=False)
    def disconnect(self) -> bool:
        """
        Close connection to Google Cloud Storage.

        Returns:
            bool: True if disconnection successful, False otherwise.
        """
        try:
            # Set GCS client to None
            self._gcs_client = None

            # Clear bucket cache
            self.bucket_cache.clear()

            # Update connection state
            self._update_connection_state(connected=False, success=True)
            logger.info(f"Successfully disconnected from GCS for {self.source_name} (ID: {self.source_id})")
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from GCS: {str(e)}")
            self._update_connection_state(connected=False, success=False)
            return True

    @with_error_handling(context={'component': 'GCSConnector', 'operation': 'extract_data'}, raise_exception=True)
    def extract_data(self, extraction_params: dict) -> tuple[typing.Optional[pandas.DataFrame], dict[str, typing.Any]]:
        """
        Extract data from GCS based on extraction parameters.

        Args:
            extraction_params (dict): Parameters controlling the extraction process.

        Returns:
            tuple[typing.Optional[pandas.DataFrame], dict[str, typing.Any]]:
            Extracted data and associated metadata.
        """
        # Validate connection is established
        if not self.is_connected:
            raise ConnectionError(
                message="Not connected to GCS. Call connect() first.",
                service_name="GCS",
                connection_details=self.connection_config
            )

        # Validate extraction parameters
        if not self._validate_extraction_params(extraction_params):
            raise ValueError(f"Invalid extraction parameters: {extraction_params}")

        # Determine extraction mode
        extraction_mode = self._determine_extraction_mode(extraction_params)

        # Extract data based on mode
        if extraction_mode == 'single_file':
            data, metadata = self._extract_single_file(extraction_params)
        elif extraction_mode == 'multiple_files':
            data, metadata = self._extract_multiple_files(extraction_params)
        elif extraction_mode == 'pattern':
            data, metadata = self._extract_files_by_pattern(extraction_params)
        else:
            raise ValueError(f"Unsupported extraction mode: {extraction_mode}")

        return data, metadata

    @with_error_handling(context={'component': 'GCSConnector', 'operation': 'get_source_schema'}, raise_exception=True)
    def get_source_schema(self, object_name: str) -> dict[str, typing.Any]:
        """
        Retrieve the schema information for a GCS object.

        Args:
            object_name (str): Name of the object (file) to get schema for.

        Returns:
            dict[str, typing.Any]: Schema definition for the specified object.
        """
        # Validate connection is established
        if not self.is_connected:
            raise ConnectionError(
                message="Not connected to GCS. Call connect() first.",
                service_name="GCS",
                connection_details=self.connection_config
            )

        # Parse object_name to extract bucket and blob path
        bucket_name, blob_name = self._parse_gcs_path(object_name)

        # Verify object exists
        try:
            self._gcs_client.get_blob(bucket_name, blob_name)
        except exceptions.NotFound:
            raise FileNotFoundError(f"Object not found: gs://{bucket_name}/{blob_name}")

        # Determine file format from object name or metadata
        file_format = self.connection_config.get('file_format')
        if not file_format:
            # Download a small sample of the file content to detect the format
            sample_content = self._gcs_client.download_blob_as_bytes(bucket_name, blob_name, start=0, end=2048)
            file_format = detect_file_format(blob_name, sample_content)
        elif isinstance(file_format, str):
            file_format = FileFormat(file_format.upper())

        # Extract a sample of the data to infer the schema
        data, _ = self.extract_data({'bucket_name': bucket_name, 'blob_name': blob_name, 'file_format': file_format.value})

        # For structured formats (CSV, JSON, Avro, Parquet), infer schema from sample data
        if isinstance(data, pandas.DataFrame) and not data.empty:
            schema = infer_schema(data, file_format)
        else:
            # For unstructured formats, return basic metadata as schema
            schema = {
                'fields': [{'name': 'text', 'data_type': 'string'}],
                'metadata': {'format': file_format.value, 'description': 'Unstructured text data'}
            }

        return schema

    def validate_connection_config(self, config: dict) -> bool:
        """
        Validate the GCS connection configuration.

        Args:
            config (dict): Connection configuration to validate.

        Returns:
            bool: True if configuration is valid, False otherwise.
        """
        if not isinstance(config, dict):
            logger.error("Connection configuration must be a dictionary")
            return False

        # Verify required fields are present (project_id or use default)
        if 'project_id' not in config:
            logger.warning("project_id not found in connection configuration, using default from environment")

        # Validate optional fields if present (location, timeout, etc.)
        if 'location' in config and not isinstance(config['location'], str):
            logger.error("location must be a string")
            return False

        if 'timeout' in config:
            try
                timeout = int(config['timeout'])
                if timeout <= 0:
                    logger.error("timeout must be a positive integer")
                    return False
            except ValueError:
                logger.error("timeout must be an integer")
                return False

        return True

    def _extract_single_file(self, extraction_params: dict) -> tuple[pandas.DataFrame, dict]:
        """
        Extract data from a single GCS file.

        Args:
            extraction_params (dict): Parameters including bucket_name and blob_name.

        Returns:
            tuple[pandas.DataFrame, dict]: Extracted data and metadata.
        """
        # Extract bucket_name and blob_name from extraction_params
        bucket_name = extraction_params['bucket_name']
        blob_name = extraction_params['blob_name']

        # Verify file exists using GCS client
        try:
            self._gcs_client.get_blob(bucket_name, blob_name)
        except exceptions.NotFound:
            raise FileNotFoundError(f"File not found: gs://{bucket_name}/{blob_name}")

        # Use file extractor to process the file
        data, metadata = self._file_extractor.extract_file(f"gs://{bucket_name}/{blob_name}", extraction_params)

        return data, metadata

    def _extract_multiple_files(self, extraction_params: dict) -> tuple[pandas.DataFrame, dict]:
        """
        Extract data from multiple specified GCS files.

        Args:
            extraction_params (dict): Parameters including file_list.

        Returns:
            tuple[pandas.DataFrame, dict]: Combined data and metadata.
        """
        # Extract file_list from extraction_params
        file_list = extraction_params['file_list']

        # Initialize lists to store data and metadata
        all_data = []
        all_metadata = []

        # Process each file in the list
        for file_path in file_list:
            # Verify file exists
            bucket_name, blob_name = self._parse_gcs_path(file_path)
            try:
                self._gcs_client.get_blob(bucket_name, blob_name)
            except exceptions.NotFound:
                logger.warning(f"File not found, skipping: {file_path}")
                continue

            # Use file extractor to process each file
            data, metadata = self._file_extractor.extract_file(file_path, extraction_params)

            # Append data and metadata to lists
            if data is not None:
                all_data.append(data)
            all_metadata.append(metadata)

        # Combine data from all files
        if all_data:
            combined_data = pandas.concat(all_data, ignore_index=True)
        else:
            combined_data = pandas.DataFrame()

        # Aggregate metadata from all files
        metadata = self._format_gcs_metadata({'file_count': len(all_metadata)})

        return combined_data, metadata

    def _extract_files_by_pattern(self, extraction_params: dict) -> tuple[pandas.DataFrame, dict]:
        """
        Extract data from GCS files matching a pattern.

        Args:
            extraction_params (dict): Parameters including bucket_name and prefix/pattern.

        Returns:
            tuple[pandas.DataFrame, dict]: Combined data and metadata.
        """
        # Extract bucket_name and prefix/pattern from extraction_params
        bucket_name = extraction_params['bucket_name']
        prefix = extraction_params.get('prefix', '')
        pattern = extraction_params.get('pattern', '*')

        # List blobs in bucket matching the pattern
        blobs = self._gcs_client.list_blobs(bucket_name, prefix=prefix, pattern=pattern)

        # Initialize lists to store data and metadata
        all_data = []
        all_metadata = []

        # Process each matching file
        for blob in blobs:
            # Use file extractor to process each file
            data, metadata = self._file_extractor.extract_file(f"gs://{bucket_name}/{blob.name}", extraction_params)

            # Append data and metadata to lists
            if data is not None:
                all_data.append(data)
            all_metadata.append(metadata)

        # Combine data from all files
        if all_data:
            combined_data = pandas.concat(all_data, ignore_index=True)
        else:
            combined_data = pandas.DataFrame()

        # Aggregate metadata from all files
        metadata = self._format_gcs_metadata({'file_count': len(all_metadata)})

        return combined_data, metadata

    def _get_bucket(self, bucket_name: str):
        """
        Get a GCS bucket with caching.

        Args:
            bucket_name (str): Name of the bucket.

        Returns:
            object: GCS bucket object.
        """
        # Check if bucket is in cache
        if bucket_name in self.bucket_cache:
            return self.bucket_cache[bucket_name]

        # If not cached, get bucket using GCS client
        bucket = self._gcs_client.get_bucket(bucket_name)

        # Add bucket to cache
        self.bucket_cache[bucket_name] = bucket

        return bucket

    def _validate_extraction_params(self, extraction_params: dict) -> bool:
        """
        Validate extraction parameters for GCS.

        Args:
            extraction_params (dict): Parameters to validate.

        Returns:
            bool: True if parameters are valid, False otherwise.
        """
        if not isinstance(extraction_params, dict):
            logger.error("Extraction parameters must be a dictionary")
            return False

        # Verify extraction mode is specified or can be determined
        try:
            self._determine_extraction_mode(extraction_params)
        except ValueError as e:
            logger.error(f"Could not determine extraction mode: {e}")
            return False

        # Validate parameters based on extraction mode
        extraction_mode = self._determine_extraction_mode(extraction_params)
        if extraction_mode == 'single_file':
            if 'bucket_name' not in extraction_params or 'blob_name' not in extraction_params:
                logger.error("bucket_name and blob_name are required for single file extraction")
                return False
        elif extraction_mode == 'multiple_files':
            if 'file_list' not in extraction_params or not isinstance(extraction_params['file_list'], list):
                logger.error("file_list is required and must be a list for multiple files extraction")
                return False
        elif extraction_mode == 'pattern':
            if 'bucket_name' not in extraction_params or 'pattern' not in extraction_params:
                logger.error("bucket_name and pattern are required for pattern-based extraction")
                return False

        # Validate format specification if present
        if 'file_format' in extraction_params:
            file_format = extraction_params['file_format']
            if isinstance(file_format, str):
                try:
                    FileFormat(file_format.upper())  # Validate it's a valid enum value
                except ValueError:
                    logger.error(f"Invalid file format: {file_format}")
                    return False
            elif not isinstance(file_format, FileFormat):
                logger.error(f"file_format must be a string or FileFormat enum, got {type(file_format)}")
                return False

        return True

    def _determine_extraction_mode(self, extraction_params: dict) -> str:
        """
        Determine the extraction mode based on parameters.

        Args:
            extraction_params (dict): Parameters to determine mode from.

        Returns:
            str: Extraction mode (single_file, multiple_files, or pattern).
        """
        # Check for explicit mode in extraction_params
        if 'mode' in extraction_params:
            return extraction_params['mode']

        # If bucket_name and blob_name are present, use single_file mode
        if 'bucket_name' in extraction_params and 'blob_name' in extraction_params:
            return 'single_file'

        # If file_list is present, use multiple_files mode
        if 'file_list' in extraction_params:
            return 'multiple_files'

        # If bucket_name and prefix/pattern are present, use pattern mode
        if 'bucket_name' in extraction_params and 'pattern' in extraction_params:
            return 'pattern'

        # If mode cannot be determined, raise ValueError
        raise ValueError("Could not determine extraction mode from parameters")

    def _format_gcs_metadata(self, raw_metadata: dict) -> dict:
        """
        Format GCS-specific metadata.

        Args:
            raw_metadata (dict): Source-specific metadata to format.

        Returns:
            dict: Formatted metadata dictionary.
        """
        # Create base metadata structure
        metadata = super()._format_metadata(raw_metadata)

        # Add GCS-specific information
        metadata['storage_type'] = 'GCS'
        metadata['bucket'] = self.connection_config.get('bucket_name')
        metadata['project'] = self.connection_config.get('project_id')
        metadata['location'] = self.connection_config.get('location')

        # Add file-specific metadata (format, size, count)
        metadata['file_format'] = self.connection_config.get('file_format')
        metadata['file_size'] = raw_metadata.get('file_size')
        metadata['file_count'] = raw_metadata.get('file_count')

        return metadata

    def _parse_gcs_path(self, gcs_path: str) -> tuple[str, str]:
        """
        Parse a GCS path into bucket and blob names.

        Args:
            gcs_path (str): GCS path in the format 'gs://bucket-name/path/to/file'.

        Returns:
            tuple[str, str]: Tuple of (bucket_name, blob_name).
        """
        if not gcs_path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path: {gcs_path}")

        # Remove the 'gs://' prefix
        path = gcs_path[5:]

        # Split into bucket and blob
        parts = path.split('/', 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ''

        return bucket_name, blob_name