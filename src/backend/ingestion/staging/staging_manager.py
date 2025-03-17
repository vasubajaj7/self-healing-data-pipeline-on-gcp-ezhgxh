"""
Manages the staging area for data ingestion in the self-healing data pipeline.

Handles the storage, retrieval, and management of data during the ingestion process,
including normalization, metadata tracking, and integration with the self-healing system.
"""

import os
import uuid
import datetime
import typing
import pandas as pd
import json
from typing import Dict, List, Any, Union, Optional

# Internal imports
from ...constants import FileFormat  # Import file format constants
from ...config import get_config  # Access application configuration settings
from ...utils.logging.logger import get_logger  # Configure logging for staging operations
from .storage_service import StorageService  # Utilize storage service for data staging
from .data_normalizer import DataNormalizer  # Normalize data during staging process
from ..metadata.metadata_tracker import MetadataTracker  # Track metadata about staged data
from ..metadata.schema_registry import SchemaRegistry  # Access schema information for data normalization
from ..errors.error_handler import with_error_handling  # Apply error handling decorator to staging methods
from ...utils.errors.error_types import StagingError, DataFormatError  # Import error types for staging operations

# Set up logger
logger = get_logger(__name__)

# Global variables
DEFAULT_STAGING_PREFIX = "staging/"


def generate_staging_id() -> str:
    """Generates a unique identifier for staged data

    Returns:
        str: Unique staging ID
    """
    # Generate a UUID
    staging_uuid = uuid.uuid4()
    # Format as a string with 'stg_' prefix
    staging_id = f"stg_{str(staging_uuid)}"
    # Return the formatted staging ID
    return staging_id


class StagingManager:
    """Manages the staging area for data during the ingestion process"""

    def __init__(self, storage_service: StorageService, data_normalizer: DataNormalizer,
                 metadata_tracker: MetadataTracker, schema_registry: SchemaRegistry):
        """Initialize the StagingManager with required services

        Args:
            storage_service: StorageService instance
            data_normalizer: DataNormalizer instance
            metadata_tracker: MetadataTracker instance
            schema_registry: SchemaRegistry instance
        """
        # Store service references
        self._storage_service = storage_service
        self._data_normalizer = data_normalizer
        self._metadata_tracker = metadata_tracker
        self._schema_registry = schema_registry

        # Load configuration using get_config()
        self._config = get_config()

        # Set staging base path from configuration or use default
        self._staging_base_path = self._config.get("staging.base_path", DEFAULT_STAGING_PREFIX)

        # Initialize staging statistics tracking
        self._staging_stats = {
            "stage_data_calls": 0,
            "retrieve_data_calls": 0,
            "update_staged_data_calls": 0,
            "delete_staged_data_calls": 0,
            "list_staged_data_calls": 0,
            "get_staging_metadata_calls": 0,
            "normalize_staged_data_calls": 0,
            "apply_self_healing_calls": 0,
            "cleanup_expired_data_calls": 0,
            "last_operation_time": None
        }

        # Initialize logging for this component
        logger.info("StagingManager initialized")

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'stage_data'}, raise_exception=True)
    def stage_data(self, data: object, data_format: FileFormat, source_id: str, metadata: dict, options: dict) -> dict:
        """Stages data for further processing in the pipeline

        Args:
            data: The data to stage
            data_format: The format of the data
            source_id: The ID of the data source
            metadata: Additional metadata to store with the data
            options: Additional options for staging

        Returns:
            Staging result with staging_id and metadata
        """
        # Generate staging_id using generate_staging_id()
        staging_id = generate_staging_id()

        # Determine staging path based on source_id and staging_id
        staging_path = self._get_staging_path(source_id, staging_id, data_format)

        # Prepare metadata with source information and timestamps
        staging_metadata = {
            "source_id": source_id,
            "staging_time": datetime.datetime.utcnow().isoformat(),
            "data_format": data_format.value
        }
        staging_metadata.update(metadata)

        # Determine if normalization is needed based on options
        normalize = options.get("normalize", False)

        # If normalization needed, get schema and normalize data
        if normalize:
            target_schema = options.get("target_schema")
            if not target_schema:
                target_schema = self._schema_registry.get_schema(source_id)
            data = self._data_normalizer.normalize_data(data, data_format, target_schema, options)

        # Store data in staging area using storage_service
        storage_result = self._storage_service.store_data(data, staging_path, data_format, staging_metadata, options)

        # Track staging metadata using metadata_tracker
        metadata_record_id = self._track_staging_metadata(staging_id, staging_path, data_format, source_id, storage_result.get("metadata", {}))

        # Update staging statistics
        self._update_stats("stage_data", data_format, storage_result.get("size", 0))

        # Return staging result with staging_id and metadata
        return {
            "staging_id": staging_id,
            "metadata_record_id": metadata_record_id,
            "staging_path": staging_path,
            "metadata": staging_metadata
        }

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'retrieve_data'}, raise_exception=True)
    def retrieve_data(self, staging_id: str, as_dataframe: bool, options: dict) -> tuple:
        """Retrieves staged data by staging_id

        Args:
            staging_id: The ID of the staged data
            as_dataframe: Whether to return the data as a DataFrame
            options: Additional options for retrieval

        Returns:
            Retrieved data and associated metadata
        """
        # Get staging metadata to determine storage location
        metadata = self.get_staging_metadata(staging_id)
        if not metadata:
            raise StagingError(f"Staging metadata not found for ID: {staging_id}")

        staging_path = metadata.get("staging_path")
        data_format = FileFormat(metadata.get("data_format"))

        # Retrieve data from storage using storage_service
        data = self._storage_service.retrieve_data(staging_path, data_format, as_dataframe, options)

        # If as_dataframe is True, ensure data is returned as DataFrame
        if as_dataframe and not isinstance(data, pd.DataFrame):
            raise DataFormatError("Could not convert staged data to DataFrame", data_source=staging_path)

        # Update access timestamp in metadata
        self._update_staging_metadata(staging_id, {"last_accessed": datetime.datetime.utcnow().isoformat()})

        # Update staging statistics
        self._update_stats("retrieve_data", data_format, len(str(data)))

        # Return data and metadata as tuple
        return data, metadata

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'update_staged_data'}, raise_exception=True)
    def update_staged_data(self, staging_id: str, updated_data: object, update_metadata: dict) -> dict:
        """Updates previously staged data

        Args:
            staging_id: The ID of the staged data to update
            updated_data: The updated data
            update_metadata: Metadata updates

        Returns:
            Updated staging metadata
        """
        # Get existing staging metadata
        metadata = self.get_staging_metadata(staging_id)
        if not metadata:
            raise StagingError(f"Staging metadata not found for ID: {staging_id}")

        staging_path = metadata.get("staging_path")
        data_format = FileFormat(metadata.get("data_format"))

        # Store updated data in same location
        storage_result = self._storage_service.store_data(updated_data, staging_path, data_format, update_metadata)

        # Update metadata with update_metadata and new timestamp
        updated_metadata = self._update_staging_metadata(staging_id, update_metadata)

        # Track update in metadata history
        # TODO: Implement metadata history tracking

        # Update staging statistics
        self._update_stats("update_staged_data", data_format, storage_result.get("size", 0))

        # Return updated metadata
        return updated_metadata

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'delete_staged_data'}, raise_exception=True)
    def delete_staged_data(self, staging_id: str) -> bool:
        """Deletes staged data by staging_id

        Args:
            staging_id: The ID of the staged data to delete

        Returns:
            True if deletion was successful
        """
        # Get staging metadata to determine storage location
        metadata = self.get_staging_metadata(staging_id)
        if not metadata:
            raise StagingError(f"Staging metadata not found for ID: {staging_id}")

        staging_path = metadata.get("staging_path")

        # Delete data from storage using storage_service
        success = self._storage_service.delete_data(staging_path)

        # Mark metadata as deleted
        self._update_staging_metadata(staging_id, {"deleted": True, "deleted_at": datetime.datetime.utcnow().isoformat()})

        # Update staging statistics
        self._update_stats("delete_staged_data", FileFormat(metadata.get("data_format")), 0)

        # Return success status
        return success

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'list_staged_data'}, raise_exception=True)
    def list_staged_data(self, source_id: str = None, start_time: datetime.datetime = None, end_time: datetime.datetime = None, limit: int = 100) -> list:
        """Lists staged data matching criteria

        Args:
            source_id: Filter by source ID
            start_time: Filter by start time
            end_time: Filter by end time
            limit: Limit the number of results

        Returns:
            List of staging metadata matching criteria
        """
        # Build query criteria based on parameters
        query = {}
        if source_id:
            query["source_id"] = source_id
        if start_time:
            query["staging_time>="] = start_time.isoformat()
        if end_time:
            query["staging_time<="] = end_time.isoformat()

        # Query metadata for staged data matching criteria
        metadata_list = self._metadata_tracker.search_metadata(query, limit=limit)

        # Apply limit if specified
        if limit:
            metadata_list = metadata_list[:limit]

        # Update staging statistics
        self._update_stats("list_staged_data", None, 0)

        # Return list of staging metadata
        return metadata_list

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'get_staging_metadata'}, raise_exception=True)
    def get_staging_metadata(self, staging_id: str) -> dict:
        """Gets metadata for staged data

        Args:
            staging_id: The ID of the staged data

        Returns:
            Staging metadata
        """
        # Query metadata for staging record
        metadata = self._metadata_tracker.get_metadata_record(staging_id)

        # Return staging metadata or None if not found
        return metadata

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'normalize_staged_data'}, raise_exception=True)
    def normalize_staged_data(self, staging_id: str, target_schema: dict = None, options: dict = None) -> str:
        """Normalizes staged data according to a schema

        Args:
            staging_id: The ID of the staged data to normalize
            target_schema: The schema to normalize against
            options: Additional options for normalization

        Returns:
            New staging_id for normalized data
        """
        # Retrieve staged data and metadata
        data, metadata = self.retrieve_data(staging_id, as_dataframe=False, options={})
        source_format = FileFormat(metadata.get("data_format"))
        source_id = metadata.get("source_id")

        # If target_schema not provided, try to get from schema_registry
        if not target_schema:
            target_schema = self._schema_registry.get_schema(source_id)

        # Normalize data using data_normalizer
        normalized_data = self._data_normalizer.normalize_data(data, source_format, target_schema, options)

        # Generate new staging_id for normalized data
        new_staging_id = generate_staging_id()

        # Stage normalized data with reference to original
        new_metadata = {
            "original_staging_id": staging_id,
            "normalization_time": datetime.datetime.utcnow().isoformat()
        }
        new_staging_result = self.stage_data(normalized_data, source_format, source_id, new_metadata, options)

        # Return new staging_id
        return new_staging_result["staging_id"]

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'apply_self_healing'}, raise_exception=True)
    def apply_self_healing(self, staging_id: str, healing_id: str, action_type: str, action_params: dict) -> str:
        """Applies self-healing actions to staged data

        Args:
            staging_id: The ID of the staged data to heal
            healing_id: The ID of the self-healing action
            action_type: The type of self-healing action to apply
            action_params: Parameters for the self-healing action

        Returns:
            New staging_id for healed data
        """
        # Retrieve staged data and metadata
        data, metadata = self.retrieve_data(staging_id, as_dataframe=False, options={})
        source_format = FileFormat(metadata.get("data_format"))
        source_id = metadata.get("source_id")

        # Apply healing action based on action_type
        if action_type == "data_correction":
            # For data corrections, modify data according to action_params
            # TODO: Implement data correction logic based on action_params
            healed_data = data  # Placeholder
        elif action_type == "schema_adaptation":
            # For schema issues, normalize with adjusted schema
            # TODO: Implement schema adaptation logic
            healed_data = data  # Placeholder
        else:
            raise ValueError(f"Unsupported self-healing action type: {action_type}")

        # Generate new staging_id for healed data
        new_staging_id = generate_staging_id()

        # Stage healed data with reference to original and healing action
        new_metadata = {
            "original_staging_id": staging_id,
            "healing_id": healing_id,
            "healing_time": datetime.datetime.utcnow().isoformat()
        }
        new_staging_result = self.stage_data(healed_data, source_format, source_id, new_metadata, {})

        # Track healing action in metadata
        # TODO: Implement metadata tracking for self-healing actions

        # Return new staging_id
        return new_staging_result["staging_id"]

    def get_staging_stats(self) -> dict:
        """Gets statistics about staging operations

        Returns:
            Staging statistics
        """
        # Return a copy of the staging statistics dictionary
        return self._staging_stats.copy()

    def reset_staging_stats(self) -> None:
        """Resets staging statistics"""
        # Reset the staging statistics dictionary to initial state
        self._staging_stats = {
            "stage_data_calls": 0,
            "retrieve_data_calls": 0,
            "update_staged_data_calls": 0,
            "delete_staged_data_calls": 0,
            "list_staged_data_calls": 0,
            "get_staging_metadata_calls": 0,
            "normalize_staged_data_calls": 0,
            "apply_self_healing_calls": 0,
            "cleanup_expired_data_calls": 0,
            "last_operation_time": None
        }

    @with_error_handling(context={'component': 'StagingManager', 'operation': 'cleanup_expired_data'}, raise_exception=True)
    def cleanup_expired_data(self, retention_days: int) -> int:
        """Cleans up staged data older than retention period

        Args:
            retention_days: The number of days to retain staged data

        Returns:
            Number of staging records cleaned up
        """
        # Calculate cutoff date based on retention period
        cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=retention_days)

        # Query for staging records older than cutoff date
        expired_records = self.list_staged_data(end_time=cutoff_date)

        # Delete each expired staging record
        deleted_count = 0
        for record in expired_records:
            try:
                self.delete_staged_data(record["staging_id"])
                deleted_count += 1
            except Exception as e:
                logger.warning(f"Failed to delete expired staging data: {record['staging_id']}, error: {e}")

        # Return count of deleted records
        return deleted_count

    def _get_staging_path(self, source_id: str, staging_id: str, data_format: FileFormat) -> str:
        """Internal method to generate staging path

        Args:
            source_id: The ID of the data source
            staging_id: The unique staging ID
            data_format: The format of the data

        Returns:
            Full path for staged data
        """
        # Combine staging_base_path, source_id, and staging_id
        base_path = os.path.join(self._staging_base_path, source_id, staging_id)

        # Add appropriate file extension based on data_format
        extension = data_format.value.lower()
        full_path = f"{base_path}.{extension}"

        # Return the full staging path
        return full_path

    def _track_staging_metadata(self, staging_id: str, staging_path: str, data_format: FileFormat, source_id: str, metadata: dict) -> str:
        """Internal method to track staging metadata

        Args:
            staging_id: The unique staging ID
            staging_path: The full path to the staged data
            data_format: The format of the data
            source_id: The ID of the data source
            metadata: Additional metadata to store

        Returns:
            Metadata record ID
        """
        # Create staging metadata record
        metadata_record = {
            "staging_id": staging_id,
            "staging_path": staging_path,
            "data_format": data_format.value,
            "source_id": source_id,
            **metadata
        }

        # Store metadata record using metadata_tracker
        metadata_record_id = self._metadata_tracker.track_data_quality_metadata(
            execution_id=staging_id,
            validation_id=staging_id,
            dataset=source_id,
            table=staging_id,
            validation_results=metadata_record,
            quality_score=1.0
        )

        # Return metadata record ID
        return metadata_record_id

    def _update_staging_metadata(self, staging_id: str, updates: dict) -> dict:
        """Internal method to update staging metadata

        Args:
            staging_id: The ID of the staged data
            updates: The updates to apply to the metadata

        Returns:
            Updated metadata
        """
        # Get existing metadata record
        metadata = self.get_staging_metadata(staging_id)
        if not metadata:
            raise StagingError(f"Staging metadata not found for ID: {staging_id}")

        # Apply updates to metadata
        metadata.update(updates)

        # Update timestamp information
        metadata["updated_at"] = datetime.datetime.utcnow().isoformat()

        # Save updated metadata record
        # TODO: Implement metadata history tracking
        return metadata

    def _update_stats(self, operation: str, data_format: FileFormat, data_size: int) -> None:
        """Internal method to update staging statistics

        Args:
            operation: Operation type (stage_data, retrieve_data, etc.)
            data_format: The format of the data
            data_size: The size of the data in bytes
        """
        # Increment operation count in statistics
        if operation in self._staging_stats:
            self._staging_stats[operation] += 1

        # Update format-specific statistics
        if data_format:
            format_key = data_format.value
            if format_key not in self._staging_stats:
                self._staging_stats[format_key] = 0
            self._staging_stats[format_key] += 1

        # Update data size statistics
        # TODO: Implement data size tracking

        # Update timestamp of last operation
        self._staging_stats["last_operation_time"] = datetime.datetime.utcnow().isoformat()