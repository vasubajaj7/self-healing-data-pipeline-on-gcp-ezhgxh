"""
Storage service for data staging operations in the self-healing data pipeline.

This module provides a unified interface for storing and retrieving data during
the ingestion pipeline process, with a primary focus on Google Cloud Storage operations.
It includes:
- Abstract StorageService interface
- GCS implementation with production-ready features
- Local file system implementation for testing and development
- Comprehensive error handling and retry mechanisms
- Monitoring and statistics collection

The module supports various data formats, file operations, and includes self-healing
capabilities to handle transient storage issues.
"""

import os
import io
import typing
import pandas as pd
import uuid
from typing import Dict, List, Optional, Union, Any

from ...constants import FileFormat, DEFAULT_MAX_RETRY_ATTEMPTS
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.gcs_client import GCSClient
from ..errors.error_handler import with_error_handling
from ...utils.errors.error_types import StorageError, DataFormatError

# Set up logger
logger = get_logger(__name__)

# Default storage type
DEFAULT_STORAGE_TYPE = "gcs"


def get_storage_service(storage_type: str = None, config_override: dict = None) -> 'StorageService':
    """
    Factory function that creates a storage service instance based on configuration.
    
    Args:
        storage_type: Type of storage service to create ('gcs' or 'local')
        config_override: Optional configuration overrides
        
    Returns:
        StorageService: Configured storage service instance
    """
    config_override = config_override or {}
    config = get_config()
    
    # Determine storage type from parameter, environment, or default
    actual_storage_type = storage_type or config.get("storage.type", DEFAULT_STORAGE_TYPE)
    
    # Create appropriate storage service based on type
    if actual_storage_type.lower() == "gcs":
        logger.info(f"Creating GCS storage service")
        return GCSStorageService(config_override)
    elif actual_storage_type.lower() == "local":
        logger.info(f"Creating local file storage service")
        return LocalFileStorageService(config_override)
    else:
        logger.warning(f"Unsupported storage type: {actual_storage_type}. Falling back to GCS.")
        return GCSStorageService(config_override)


class StorageService:
    """
    Abstract base class defining the interface for storage services.
    
    This class provides a common interface for all storage service implementations,
    regardless of the underlying storage system.
    """
    
    def __init__(self, config_override: dict = None):
        """
        Initialize the storage service with configuration.
        
        Args:
            config_override: Optional configuration overrides
        """
        self._config = get_config()
        
        # Apply any configuration overrides
        if config_override:
            for key, value in config_override.items():
                if key.startswith("storage."):
                    # Use only storage-related overrides
                    self._config.get_config = lambda k, default=None: value if k == key else self._config.get(k, default)
        
        self._storage_type = None
        logger.debug(f"Initialized {self.__class__.__name__}")
    
    def store_data(
        self, 
        data: Union[str, bytes, io.IOBase, pd.DataFrame],
        path: str,
        format: FileFormat = None,
        metadata: dict = None,
        options: dict = None
    ) -> dict:
        """
        Store data in the storage system.
        
        Args:
            data: Data to store (string, bytes, file-like object, or DataFrame)
            path: Path where the data should be stored
            format: Format of the data (CSV, JSON, etc.)
            metadata: Additional metadata to store with the data
            options: Additional options for the storage operation
            
        Returns:
            dict: Storage result with metadata
        """
        raise NotImplementedError("Subclasses must implement store_data method")
    
    def retrieve_data(
        self,
        path: str,
        format: FileFormat = None,
        as_dataframe: bool = False,
        options: dict = None
    ) -> Union[str, bytes, pd.DataFrame]:
        """
        Retrieve data from the storage system.
        
        Args:
            path: Path to the data
            format: Expected format of the data
            as_dataframe: Whether to return data as a DataFrame
            options: Additional options for the retrieval operation
            
        Returns:
            Retrieved data as string, bytes, or DataFrame
        """
        raise NotImplementedError("Subclasses must implement retrieve_data method")
    
    def delete_data(self, path: str) -> bool:
        """
        Delete data from the storage system.
        
        Args:
            path: Path to the data to delete
            
        Returns:
            bool: True if deletion was successful
        """
        raise NotImplementedError("Subclasses must implement delete_data method")
    
    def list_data(self, prefix: str = "", delimiter: str = None) -> list:
        """
        List data in the storage system matching a prefix.
        
        Args:
            prefix: Prefix to filter results
            delimiter: Delimiter to use for hierarchical listings
            
        Returns:
            list: List of paths matching the prefix
        """
        raise NotImplementedError("Subclasses must implement list_data method")
    
    def data_exists(self, path: str) -> bool:
        """
        Check if data exists at the specified path.
        
        Args:
            path: Path to check
            
        Returns:
            bool: True if data exists
        """
        raise NotImplementedError("Subclasses must implement data_exists method")
    
    def get_metadata(self, path: str) -> dict:
        """
        Get metadata for data at the specified path.
        
        Args:
            path: Path to the data
            
        Returns:
            dict: Metadata for the data
        """
        raise NotImplementedError("Subclasses must implement get_metadata method")
    
    def update_metadata(self, path: str, metadata: dict) -> dict:
        """
        Update metadata for data at the specified path.
        
        Args:
            path: Path to the data
            metadata: Metadata to update
            
        Returns:
            dict: Updated metadata
        """
        raise NotImplementedError("Subclasses must implement update_metadata method")
    
    def copy_data(self, source_path: str, destination_path: str) -> dict:
        """
        Copy data from one path to another.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Copy result with metadata
        """
        raise NotImplementedError("Subclasses must implement copy_data method")
    
    def move_data(self, source_path: str, destination_path: str) -> dict:
        """
        Move data from one path to another.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Move result with metadata
        """
        raise NotImplementedError("Subclasses must implement move_data method")


class GCSStorageService(StorageService):
    """
    Google Cloud Storage implementation of the storage service.
    
    This class provides a concrete implementation of the StorageService
    interface using Google Cloud Storage as the underlying storage system.
    """
    
    def __init__(self, config_override: dict = None):
        """
        Initialize the GCS storage service.
        
        Args:
            config_override: Optional configuration overrides
        """
        super().__init__(config_override)
        self._storage_type = "gcs"
        
        # Initialize GCS client
        self._gcs_client = GCSClient(
            project_id=self._config.get_gcp_project_id(),
            location=self._config.get_gcp_location()
        )
        
        # Get bucket name from configuration
        self._bucket_name = self._config.get("storage.gcs.bucket", self._config.get_gcs_bucket())
        
        # Ensure bucket exists
        if not self._gcs_client.bucket_exists(self._bucket_name):
            logger.info(f"Bucket {self._bucket_name} does not exist, creating it")
            self._gcs_client.create_bucket(self._bucket_name)
        
        # Initialize storage statistics
        self._storage_stats = {
            "operations": {
                "store": 0,
                "retrieve": 0,
                "delete": 0,
                "list": 0,
                "copy": 0,
                "move": 0
            },
            "bytes": {
                "uploaded": 0,
                "downloaded": 0
            },
            "last_operation_time": None
        }
        
        logger.info(f"Initialized GCS storage service with bucket: {self._bucket_name}")
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'store_data'}, raise_exception=True)
    def store_data(
        self, 
        data: Union[str, bytes, io.IOBase, pd.DataFrame],
        path: str,
        format: FileFormat = None,
        metadata: dict = None,
        options: dict = None
    ) -> dict:
        """
        Store data in Google Cloud Storage.
        
        Args:
            data: Data to store (string, bytes, file-like object, or DataFrame)
            path: Path where the data should be stored
            format: Format of the data (CSV, JSON, etc.)
            metadata: Additional metadata to store with the data
            options: Additional options for the storage operation
            
        Returns:
            dict: Storage result with metadata
        """
        options = options or {}
        metadata = metadata or {}
        
        # Prepare data based on type and format
        prepared_data = self._prepare_data_for_storage(data, format)
        
        # Determine content type based on format
        content_type = None
        if format:
            content_type_map = {
                FileFormat.CSV: "text/csv",
                FileFormat.JSON: "application/json",
                FileFormat.AVRO: "application/avro",
                FileFormat.PARQUET: "application/parquet",
                FileFormat.ORC: "application/orc",
                FileFormat.XML: "application/xml",
                FileFormat.TEXT: "text/plain"
            }
            content_type = content_type_map.get(format)
        
        logger.debug(f"Storing data at path: {path}")
        
        # Upload to GCS
        metadata_with_format = {**metadata}
        if format:
            metadata_with_format["content_format"] = format.value
        
        result = self._gcs_client.upload_blob(
            bucket_name=self._bucket_name,
            source_data=prepared_data,
            destination_blob_name=path,
            content_type=content_type,
            metadata=metadata_with_format
        )
        
        # Update storage statistics
        data_size = result.get("size", 0)
        self._update_stats("store", data_size)
        
        return {
            "path": path,
            "size": data_size,
            "md5_hash": result.get("md5_hash"),
            "metadata": metadata_with_format
        }
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'retrieve_data'}, raise_exception=True)
    def retrieve_data(
        self,
        path: str,
        format: FileFormat = None,
        as_dataframe: bool = False,
        options: dict = None
    ) -> Union[str, bytes, pd.DataFrame]:
        """
        Retrieve data from Google Cloud Storage.
        
        Args:
            path: Path to the data
            format: Expected format of the data
            as_dataframe: Whether to return data as a DataFrame
            options: Additional options for the retrieval operation
            
        Returns:
            Retrieved data as string, bytes, or DataFrame
        """
        options = options or {}
        
        # Get blob metadata to determine format if not provided
        if not format:
            blob_metadata = self._gcs_client.get_blob_metadata(
                bucket_name=self._bucket_name,
                blob_name=path
            )
            
            # Check if format is in metadata
            if blob_metadata and "content_format" in blob_metadata:
                try:
                    format = FileFormat(blob_metadata["content_format"])
                except ValueError:
                    logger.warning(f"Invalid format in metadata: {blob_metadata['content_format']}")
        
        logger.debug(f"Retrieving data from path: {path}")
        
        # Download from GCS
        data = self._gcs_client.download_blob(
            bucket_name=self._bucket_name,
            blob_name=path
        )
        
        # Update storage statistics
        self._update_stats("retrieve", len(data) if isinstance(data, (str, bytes)) else 0)
        
        # Convert to DataFrame if requested
        if as_dataframe:
            data = self._convert_to_dataframe(data, format, options)
        
        return data
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'delete_data'}, raise_exception=True)
    def delete_data(self, path: str) -> bool:
        """
        Delete data from Google Cloud Storage.
        
        Args:
            path: Path to the data to delete
            
        Returns:
            bool: True if deletion was successful
        """
        logger.debug(f"Deleting data at path: {path}")
        
        result = self._gcs_client.delete_blob(
            bucket_name=self._bucket_name,
            blob_name=path
        )
        
        # Update storage statistics
        self._update_stats("delete", 0)
        
        return result
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'list_data'}, raise_exception=True)
    def list_data(self, prefix: str = "", delimiter: str = None) -> list:
        """
        List data in Google Cloud Storage matching a prefix.
        
        Args:
            prefix: Prefix to filter results
            delimiter: Delimiter to use for hierarchical listings
            
        Returns:
            list: List of paths matching the prefix
        """
        logger.debug(f"Listing data with prefix: {prefix}")
        
        blobs = self._gcs_client.list_blobs(
            bucket_name=self._bucket_name,
            prefix=prefix,
            delimiter=delimiter
        )
        
        # Update storage statistics
        self._update_stats("list", 0)
        
        return blobs
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'data_exists'}, raise_exception=True)
    def data_exists(self, path: str) -> bool:
        """
        Check if data exists in Google Cloud Storage.
        
        Args:
            path: Path to check
            
        Returns:
            bool: True if data exists
        """
        logger.debug(f"Checking if data exists at path: {path}")
        
        return self._gcs_client.blob_exists(
            bucket_name=self._bucket_name,
            blob_name=path
        )
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'get_metadata'}, raise_exception=True)
    def get_metadata(self, path: str) -> dict:
        """
        Get metadata for data in Google Cloud Storage.
        
        Args:
            path: Path to the data
            
        Returns:
            dict: Metadata for the data
        """
        logger.debug(f"Getting metadata for path: {path}")
        
        return self._gcs_client.get_blob_metadata(
            bucket_name=self._bucket_name,
            blob_name=path
        )
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'update_metadata'}, raise_exception=True)
    def update_metadata(self, path: str, metadata: dict) -> dict:
        """
        Update metadata for data in Google Cloud Storage.
        
        Args:
            path: Path to the data
            metadata: Metadata to update
            
        Returns:
            dict: Updated metadata
        """
        logger.debug(f"Updating metadata for path: {path}")
        
        return self._gcs_client.update_blob_metadata(
            bucket_name=self._bucket_name,
            blob_name=path,
            metadata=metadata
        )
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'copy_data'}, raise_exception=True)
    def copy_data(self, source_path: str, destination_path: str) -> dict:
        """
        Copy data within Google Cloud Storage.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Copy result with metadata
        """
        logger.debug(f"Copying data from {source_path} to {destination_path}")
        
        result = self._gcs_client.copy_blob(
            bucket_name=self._bucket_name,
            blob_name=source_path,
            destination_bucket_name=self._bucket_name,
            destination_blob_name=destination_path
        )
        
        # Update storage statistics
        self._update_stats("copy", result.get("size", 0))
        
        return {
            "source_path": source_path,
            "destination_path": destination_path,
            "size": result.get("size", 0),
            "md5_hash": result.get("md5_hash"),
            "metadata": result.get("metadata", {})
        }
    
    @with_error_handling(context={'component': 'GCSStorageService', 'operation': 'move_data'}, raise_exception=True)
    def move_data(self, source_path: str, destination_path: str) -> dict:
        """
        Move data within Google Cloud Storage.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Move result with metadata
        """
        logger.debug(f"Moving data from {source_path} to {destination_path}")
        
        # Copy first, then delete source
        copy_result = self.copy_data(source_path, destination_path)
        self.delete_data(source_path)
        
        # Update storage statistics
        self._update_stats("move", copy_result.get("size", 0))
        
        return {
            "source_path": source_path,
            "destination_path": destination_path,
            "size": copy_result.get("size", 0),
            "md5_hash": copy_result.get("md5_hash"),
            "metadata": copy_result.get("metadata", {})
        }
    
    def get_storage_stats(self) -> dict:
        """
        Get statistics about storage operations.
        
        Returns:
            dict: Storage statistics
        """
        return self._storage_stats.copy()
    
    def reset_storage_stats(self) -> None:
        """
        Reset storage statistics.
        """
        self._storage_stats = {
            "operations": {
                "store": 0,
                "retrieve": 0,
                "delete": 0,
                "list": 0,
                "copy": 0,
                "move": 0
            },
            "bytes": {
                "uploaded": 0,
                "downloaded": 0
            },
            "last_operation_time": None
        }
    
    def _prepare_data_for_storage(
        self, 
        data: Union[str, bytes, io.IOBase, pd.DataFrame],
        format: FileFormat
    ) -> Union[str, bytes, io.IOBase]:
        """
        Internal method to prepare data for storage based on type and format.
        
        Args:
            data: Data to prepare
            format: Target format
            
        Returns:
            Prepared data ready for storage
        """
        # Handle DataFrame
        if isinstance(data, pd.DataFrame):
            if not format:
                format = FileFormat.CSV  # Default to CSV if not specified
            
            buffer = io.BytesIO()
            
            if format == FileFormat.CSV:
                data.to_csv(buffer, index=False)
            elif format == FileFormat.JSON:
                data.to_json(buffer, orient="records")
            elif format == FileFormat.PARQUET:
                data.to_parquet(buffer, index=False)
            elif format == FileFormat.AVRO:
                try:
                    import pandavro
                    pandavro.to_avro(buffer, data)
                except ImportError:
                    raise ImportError("pandavro package is required for AVRO format support")
            else:
                raise DataFormatError(f"Unsupported format for DataFrame conversion: {format}")
            
            buffer.seek(0)
            return buffer
        
        # Handle string (convert to bytes)
        elif isinstance(data, str):
            return data.encode('utf-8')
        
        # Handle bytes or file-like object
        elif isinstance(data, (bytes, io.IOBase)):
            return data
        
        # Handle unsupported types
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")
    
    def _convert_to_dataframe(
        self, 
        data: Union[str, bytes],
        format: FileFormat,
        options: dict
    ) -> pd.DataFrame:
        """
        Internal method to convert retrieved data to DataFrame.
        
        Args:
            data: Data to convert
            format: Data format
            options: Conversion options
            
        Returns:
            Data as DataFrame
        """
        if not format:
            # Try to infer format
            if isinstance(data, bytes) and data[:4] == b'PAR1':
                format = FileFormat.PARQUET
            elif isinstance(data, bytes) and data[:3] == b'ORC':
                format = FileFormat.ORC
            elif isinstance(data, bytes) and data[:3] == b'{"t':
                format = FileFormat.JSON
            else:
                # Default to CSV if can't determine
                format = FileFormat.CSV
        
        # Create BytesIO object for binary formats
        if isinstance(data, bytes):
            data_buffer = io.BytesIO(data)
        else:
            data_buffer = io.StringIO(data)
        
        # Convert based on format
        if format == FileFormat.CSV:
            return pd.read_csv(data_buffer, **options)
        elif format == FileFormat.JSON:
            return pd.read_json(data_buffer, **options)
        elif format == FileFormat.PARQUET:
            return pd.read_parquet(data_buffer, **options)
        elif format == FileFormat.AVRO:
            try:
                import pandavro
                return pandavro.from_avro(data_buffer)
            except ImportError:
                raise ImportError("pandavro package is required for AVRO format support")
        else:
            raise DataFormatError(f"Unsupported format for DataFrame conversion: {format}")
    
    def _update_stats(self, operation: str, data_size: int) -> None:
        """
        Internal method to update storage statistics.
        
        Args:
            operation: Operation type (store, retrieve, etc.)
            data_size: Size of data in bytes
        """
        # Update operation count
        if operation in self._storage_stats["operations"]:
            self._storage_stats["operations"][operation] += 1
        
        # Update byte counts
        if operation in ["store", "copy", "move"]:
            self._storage_stats["bytes"]["uploaded"] += data_size
        elif operation == "retrieve":
            self._storage_stats["bytes"]["downloaded"] += data_size
        
        # Update timestamp
        self._storage_stats["last_operation_time"] = pd.Timestamp.now().isoformat()


class LocalFileStorageService(StorageService):
    """
    Local file system implementation of the storage service (primarily for testing).
    
    This class provides a concrete implementation of the StorageService
    interface using the local file system as the underlying storage system.
    It's primarily intended for testing and development purposes.
    """
    
    def __init__(self, config_override: dict = None):
        """
        Initialize the local file storage service.
        
        Args:
            config_override: Optional configuration overrides
        """
        super().__init__(config_override)
        self._storage_type = "local"
        
        # Get base path from configuration
        self._base_path = self._config.get("storage.local.base_path", "./data/staging")
        
        # Ensure base path exists
        os.makedirs(self._base_path, exist_ok=True)
        
        # Initialize storage statistics
        self._storage_stats = {
            "operations": {
                "store": 0,
                "retrieve": 0,
                "delete": 0,
                "list": 0,
                "copy": 0,
                "move": 0
            },
            "bytes": {
                "written": 0,
                "read": 0
            },
            "last_operation_time": None
        }
        
        logger.info(f"Initialized local file storage service with base path: {self._base_path}")
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'store_data'}, raise_exception=True)
    def store_data(
        self, 
        data: Union[str, bytes, io.IOBase, pd.DataFrame],
        path: str,
        format: FileFormat = None,
        metadata: dict = None,
        options: dict = None
    ) -> dict:
        """
        Store data in the local file system.
        
        Args:
            data: Data to store (string, bytes, file-like object, or DataFrame)
            path: Path where the data should be stored
            format: Format of the data (CSV, JSON, etc.)
            metadata: Additional metadata to store with the data
            options: Additional options for the storage operation
            
        Returns:
            dict: Storage result with metadata
        """
        options = options or {}
        metadata = metadata or {}
        full_path = self._get_full_path(path)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        # Handle DataFrame
        if isinstance(data, pd.DataFrame):
            if not format:
                format = FileFormat.CSV  # Default format for DataFrame
            
            if format == FileFormat.CSV:
                data.to_csv(full_path, index=False)
            elif format == FileFormat.JSON:
                data.to_json(full_path, orient="records")
            elif format == FileFormat.PARQUET:
                data.to_parquet(full_path, index=False)
            else:
                raise DataFormatError(f"Unsupported format for DataFrame storage: {format}")
        
        # Handle file-like object
        elif isinstance(data, io.IOBase):
            with open(full_path, 'wb') as f:
                f.write(data.read())
        
        # Handle string
        elif isinstance(data, str):
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(data)
        
        # Handle bytes
        elif isinstance(data, bytes):
            with open(full_path, 'wb') as f:
                f.write(data)
        
        # Handle unsupported type
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")
        
        # Store metadata if provided
        if metadata:
            metadata_path = self._get_metadata_path(path)
            with open(metadata_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(metadata, f)
        
        # Get file size
        file_size = os.path.getsize(full_path)
        
        # Update storage statistics
        self._update_stats("store", file_size)
        
        return {
            "path": path,
            "size": file_size,
            "metadata": metadata
        }
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'retrieve_data'}, raise_exception=True)
    def retrieve_data(
        self,
        path: str,
        format: FileFormat = None,
        as_dataframe: bool = False,
        options: dict = None
    ) -> Union[str, bytes, pd.DataFrame]:
        """
        Retrieve data from the local file system.
        
        Args:
            path: Path to the data
            format: Expected format of the data
            as_dataframe: Whether to return data as a DataFrame
            options: Additional options for the retrieval operation
            
        Returns:
            Retrieved data as string, bytes, or DataFrame
        """
        options = options or {}
        full_path = self._get_full_path(path)
        
        # Check if file exists
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {path}")
        
        # If format not provided, try to infer from file extension
        if not format:
            ext = os.path.splitext(path)[1].lower()
            if ext == '.csv':
                format = FileFormat.CSV
            elif ext == '.json':
                format = FileFormat.JSON
            elif ext == '.parquet':
                format = FileFormat.PARQUET
            elif ext == '.avro':
                format = FileFormat.AVRO
            elif ext == '.orc':
                format = FileFormat.ORC
            elif ext == '.xml':
                format = FileFormat.XML
            else:
                format = FileFormat.TEXT
        
        # Handle DataFrame request
        if as_dataframe:
            if format == FileFormat.CSV:
                data = pd.read_csv(full_path, **options)
            elif format == FileFormat.JSON:
                data = pd.read_json(full_path, **options)
            elif format == FileFormat.PARQUET:
                data = pd.read_parquet(full_path, **options)
            else:
                raise DataFormatError(f"Unsupported format for DataFrame conversion: {format}")
        else:
            # Determine read mode
            if format in [FileFormat.CSV, FileFormat.JSON, FileFormat.XML, FileFormat.TEXT]:
                with open(full_path, 'r', encoding='utf-8') as f:
                    data = f.read()
            else:
                with open(full_path, 'rb') as f:
                    data = f.read()
        
        # Update storage statistics
        file_size = os.path.getsize(full_path)
        self._update_stats("retrieve", file_size)
        
        return data
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'delete_data'}, raise_exception=True)
    def delete_data(self, path: str) -> bool:
        """
        Delete data from the local file system.
        
        Args:
            path: Path to the data to delete
            
        Returns:
            bool: True if deletion was successful
        """
        full_path = self._get_full_path(path)
        metadata_path = self._get_metadata_path(path)
        
        # Delete file if it exists
        if os.path.exists(full_path):
            os.remove(full_path)
        
        # Delete metadata file if it exists
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        
        # Update storage statistics
        self._update_stats("delete", 0)
        
        return True
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'list_data'}, raise_exception=True)
    def list_data(self, prefix: str = "", delimiter: str = None) -> list:
        """
        List data in the local file system matching a prefix.
        
        Args:
            prefix: Prefix to filter results
            delimiter: Delimiter to use (ignored in local implementation)
            
        Returns:
            list: List of paths matching the prefix
        """
        prefix_path = os.path.join(self._base_path, prefix)
        prefix_dir = os.path.dirname(prefix_path)
        
        # Ensure directory exists
        if not os.path.exists(prefix_dir):
            return []
        
        # Get all files in directory
        results = []
        for root, dirs, files in os.walk(prefix_dir):
            for file in files:
                # Skip metadata files
                if file.endswith('.metadata.json'):
                    continue
                
                # Construct relative path
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, self._base_path)
                
                # Apply prefix filter
                if prefix and not rel_path.startswith(prefix):
                    continue
                
                results.append(rel_path)
        
        # Update storage statistics
        self._update_stats("list", 0)
        
        return results
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'data_exists'}, raise_exception=True)
    def data_exists(self, path: str) -> bool:
        """
        Check if data exists in the local file system.
        
        Args:
            path: Path to check
            
        Returns:
            bool: True if data exists
        """
        full_path = self._get_full_path(path)
        return os.path.exists(full_path)
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'get_metadata'}, raise_exception=True)
    def get_metadata(self, path: str) -> dict:
        """
        Get metadata for data in the local file system.
        
        Args:
            path: Path to the data
            
        Returns:
            dict: Metadata for the data
        """
        metadata_path = self._get_metadata_path(path)
        
        # Return empty dict if metadata file doesn't exist
        if not os.path.exists(metadata_path):
            return {}
        
        # Read metadata from file
        with open(metadata_path, 'r', encoding='utf-8') as f:
            import json
            return json.load(f)
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'update_metadata'}, raise_exception=True)
    def update_metadata(self, path: str, metadata: dict) -> dict:
        """
        Update metadata for data in the local file system.
        
        Args:
            path: Path to the data
            metadata: Metadata to update
            
        Returns:
            dict: Updated metadata
        """
        metadata_path = self._get_metadata_path(path)
        
        # Get existing metadata
        existing_metadata = {}
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                import json
                existing_metadata = json.load(f)
        
        # Update metadata
        updated_metadata = {**existing_metadata, **metadata}
        
        # Write updated metadata
        with open(metadata_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(updated_metadata, f)
        
        return updated_metadata
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'copy_data'}, raise_exception=True)
    def copy_data(self, source_path: str, destination_path: str) -> dict:
        """
        Copy data within the local file system.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Copy result with metadata
        """
        import shutil
        
        source_full_path = self._get_full_path(source_path)
        dest_full_path = self._get_full_path(destination_path)
        
        # Check if source exists
        if not os.path.exists(source_full_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        # Ensure destination directory exists
        os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
        
        # Copy file
        shutil.copy2(source_full_path, dest_full_path)
        
        # Copy metadata if it exists
        source_metadata_path = self._get_metadata_path(source_path)
        if os.path.exists(source_metadata_path):
            dest_metadata_path = self._get_metadata_path(destination_path)
            shutil.copy2(source_metadata_path, dest_metadata_path)
        
        # Get file size
        file_size = os.path.getsize(dest_full_path)
        
        # Update storage statistics
        self._update_stats("copy", file_size)
        
        # Get metadata
        metadata = self.get_metadata(destination_path)
        
        return {
            "source_path": source_path,
            "destination_path": destination_path,
            "size": file_size,
            "metadata": metadata
        }
    
    @with_error_handling(context={'component': 'LocalFileStorageService', 'operation': 'move_data'}, raise_exception=True)
    def move_data(self, source_path: str, destination_path: str) -> dict:
        """
        Move data within the local file system.
        
        Args:
            source_path: Source path
            destination_path: Destination path
            
        Returns:
            dict: Move result with metadata
        """
        import shutil
        
        source_full_path = self._get_full_path(source_path)
        dest_full_path = self._get_full_path(destination_path)
        
        # Check if source exists
        if not os.path.exists(source_full_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        # Ensure destination directory exists
        os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
        
        # Get file size before move
        file_size = os.path.getsize(source_full_path)
        
        # Get metadata before move
        metadata = self.get_metadata(source_path)
        
        # Move file
        shutil.move(source_full_path, dest_full_path)
        
        # Move metadata if it exists
        source_metadata_path = self._get_metadata_path(source_path)
        if os.path.exists(source_metadata_path):
            dest_metadata_path = self._get_metadata_path(destination_path)
            shutil.move(source_metadata_path, dest_metadata_path)
        
        # Update storage statistics
        self._update_stats("move", file_size)
        
        return {
            "source_path": source_path,
            "destination_path": destination_path,
            "size": file_size,
            "metadata": metadata
        }
    
    def get_storage_stats(self) -> dict:
        """
        Get statistics about storage operations.
        
        Returns:
            dict: Storage statistics
        """
        return self._storage_stats.copy()
    
    def reset_storage_stats(self) -> None:
        """
        Reset storage statistics.
        """
        self._storage_stats = {
            "operations": {
                "store": 0,
                "retrieve": 0,
                "delete": 0,
                "list": 0,
                "copy": 0,
                "move": 0
            },
            "bytes": {
                "written": 0,
                "read": 0
            },
            "last_operation_time": None
        }
    
    def _get_full_path(self, path: str) -> str:
        """
        Internal method to construct full file path.
        
        Args:
            path: Relative path
            
        Returns:
            str: Full file path
        """
        return os.path.join(self._base_path, path)
    
    def _get_metadata_path(self, path: str) -> str:
        """
        Internal method to construct metadata file path.
        
        Args:
            path: Data file path
            
        Returns:
            str: Metadata file path
        """
        return f"{self._get_full_path(path)}.metadata.json"
    
    def _update_stats(self, operation: str, data_size: int) -> None:
        """
        Internal method to update storage statistics.
        
        Args:
            operation: Operation type (store, retrieve, etc.)
            data_size: Size of data in bytes
        """
        # Update operation count
        if operation in self._storage_stats["operations"]:
            self._storage_stats["operations"][operation] += 1
        
        # Update byte counts
        if operation in ["store", "copy", "move"]:
            self._storage_stats["bytes"]["written"] += data_size
        elif operation == "retrieve":
            self._storage_stats["bytes"]["read"] += data_size
        
        # Update timestamp
        self._storage_stats["last_operation_time"] = pd.Timestamp.now().isoformat()