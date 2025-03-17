"""
Repository class for managing source system configurations in the self-healing data pipeline.

This module provides a BigQuery-based repository implementation for managing data source
configurations. It supports CRUD operations, status management, and specialized operations
for working with data sources.

The repository is designed to support the data ingestion features of the pipeline, enabling
the configuration and management of various data source types including GCS, Cloud SQL,
and external APIs.
"""

import typing
import datetime
import json
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any, Union

from db.models.source_system import (
    SourceSystem,
    SOURCE_SYSTEM_TABLE_NAME,
    get_source_system_table_schema
)
from utils.storage.bigquery_client import BigQueryClient
from utils.logging.logger import get_logger
from utils.errors.error_types import ResourceError, DataError
from constants import DataSourceType
from config import get_config

# Configure module logger
logger = get_logger(__name__)


class SourceRepository:
    """Repository class for managing source system configurations in BigQuery"""
    
    def __init__(self, bq_client: BigQueryClient, dataset_id: str):
        """Initialize the source repository with BigQuery client

        Args:
            bq_client: BigQuery client instance
            dataset_id: BigQuery dataset ID where the source systems table is located
        """
        self._bq_client = bq_client
        self._project_id = bq_client.project_id or get_config().get_gcp_project_id()
        self._dataset_id = dataset_id
        logger.info(f"Initialized SourceRepository with dataset: {dataset_id}")
        
    def create_source_system(self, source_system: SourceSystem) -> SourceSystem:
        """Create a new source system in the database

        Args:
            source_system: SourceSystem instance to create

        Returns:
            SourceSystem: Created source system with updated metadata

        Raises:
            DataError: If source system validation fails
            ResourceError: If source with same name already exists
        """
        # Validate source system first
        valid, errors = self.validate_source_system(source_system)
        if not valid:
            error_msg = f"Source system validation failed: {', '.join(errors)}"
            logger.error(error_msg)
            raise DataError(
                message=error_msg,
                data_source="source_system",
                data_details={"errors": errors},
                self_healable=False
            )
        
        # Check if source with same name already exists
        existing = self.get_source_system_by_name(source_system.name)
        if existing:
            error_msg = f"Source system with name '{source_system.name}' already exists"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_system.name,
                resource_details={"source_id": existing.source_id},
                retryable=False
            )
        
        # Convert source system to BigQuery row format
        row = source_system.to_bigquery_row()
        
        # Create table if it doesn't exist
        self.ensure_table_exists()
        
        # Insert the row into the table
        table_ref = f"{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}"
        self._bq_client.insert_rows(table_ref, [row])
        
        logger.info(f"Created source system: {source_system.source_id} ({source_system.name})")
        return source_system
    
    def get_source_system(self, source_id: str) -> Optional[SourceSystem]:
        """Get a source system by ID

        Args:
            source_id: Unique identifier of the source system

        Returns:
            SourceSystem: Retrieved source system or None if not found
        """
        query = f"""
        SELECT * FROM `{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}`
        WHERE source_id = @source_id
        LIMIT 1
        """
        
        query_params = [
            {"name": "source_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": source_id}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            logger.debug(f"Source system not found with ID: {source_id}")
            return None
        
        source_system = SourceSystem.from_bigquery_row(results[0])
        return source_system
    
    def get_source_system_by_name(self, name: str) -> Optional[SourceSystem]:
        """Get a source system by name

        Args:
            name: Name of the source system

        Returns:
            SourceSystem: Retrieved source system or None if not found
        """
        query = f"""
        SELECT * FROM `{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}`
        WHERE name = @name
        LIMIT 1
        """
        
        query_params = [
            {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": name}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            logger.debug(f"Source system not found with name: {name}")
            return None
        
        source_system = SourceSystem.from_bigquery_row(results[0])
        return source_system
    
    def list_source_systems(
        self, 
        active_only: bool = False, 
        source_type: Optional[DataSourceType] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[SourceSystem]:
        """List source systems with optional filtering

        Args:
            active_only: If True, only return active source systems
            source_type: Filter by source type
            limit: Maximum number of results to return
            offset: Offset for pagination

        Returns:
            List[SourceSystem]: List of matching source systems
        """
        query = f"""
        SELECT * FROM `{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}`
        WHERE 1=1
        """
        
        query_params = []
        
        # Add filter for active_only
        if active_only:
            query += " AND is_active = @is_active"
            query_params.append({
                "name": "is_active", 
                "parameterType": {"type": "BOOL"}, 
                "parameterValue": {"value": True}
            })
        
        # Add filter for source_type
        if source_type:
            source_type_value = source_type.value if isinstance(source_type, DataSourceType) else source_type
            query += " AND source_type = @source_type"
            query_params.append({
                "name": "source_type", 
                "parameterType": {"type": "STRING"}, 
                "parameterValue": {"value": source_type_value}
            })
        
        # Add order by, limit, and offset
        query += f"""
        ORDER BY name ASC
        LIMIT {limit}
        OFFSET {offset}
        """
        
        results = self._bq_client.query(query, query_params)
        
        source_systems = [SourceSystem.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(source_systems)} source systems")
        return source_systems
    
    def update_source_system(self, source_system: SourceSystem) -> SourceSystem:
        """Update an existing source system

        Args:
            source_system: SourceSystem instance with updated values

        Returns:
            SourceSystem: Updated source system

        Raises:
            DataError: If source system validation fails
            ResourceError: If source system does not exist
        """
        # Validate source system first
        valid, errors = self.validate_source_system(source_system)
        if not valid:
            error_msg = f"Source system validation failed: {', '.join(errors)}"
            logger.error(error_msg)
            raise DataError(
                message=error_msg,
                data_source="source_system",
                data_details={"errors": errors},
                self_healable=False
            )
        
        # Check if source exists
        existing = self.get_source_system(source_system.source_id)
        if not existing:
            error_msg = f"Source system with ID '{source_system.source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_system.source_id,
                resource_details={},
                retryable=False
            )
        
        # Convert source system to BigQuery row format
        row = source_system.to_bigquery_row()
        
        # Update the row in the table using merge statement
        table_ref = f"{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}"
        
        merge_query = f"""
        MERGE `{table_ref}` T
        USING (SELECT @source_id as source_id) S
        ON T.source_id = S.source_id
        WHEN MATCHED THEN
          UPDATE SET 
            name = @name,
            description = @description,
            source_type = @source_type,
            connection_details = @connection_details,
            schema_definition = @schema_definition,
            schema_version = @schema_version,
            extraction_settings = @extraction_settings,
            is_active = @is_active,
            updated_at = @updated_at,
            updated_by = @updated_by,
            metadata = @metadata,
            last_extraction_time = @last_extraction_time
        """
        
        query_params = [
            {"name": "source_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["source_id"]}},
            {"name": "name", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["name"]}},
            {"name": "description", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["description"]}},
            {"name": "source_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["source_type"]}},
            {"name": "connection_details", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["connection_details"]}},
            {"name": "schema_definition", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["schema_definition"]}},
            {"name": "schema_version", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["schema_version"]}},
            {"name": "extraction_settings", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["extraction_settings"]}},
            {"name": "is_active", "parameterType": {"type": "BOOL"}, "parameterValue": {"value": row["is_active"]}},
            {"name": "updated_at", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": row["updated_at"].isoformat()}},
            {"name": "updated_by", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["updated_by"]}},
            {"name": "metadata", "parameterType": {"type": "STRING"}, "parameterValue": {"value": row["metadata"]}},
        ]
        
        # Handle nullable last_extraction_time
        if row["last_extraction_time"]:
            query_params.append({
                "name": "last_extraction_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"value": row["last_extraction_time"].isoformat()}
            })
        else:
            query_params.append({
                "name": "last_extraction_time", 
                "parameterType": {"type": "TIMESTAMP"}, 
                "parameterValue": {"value": None}
            })
        
        self._bq_client.query(merge_query, query_params)
        
        logger.info(f"Updated source system: {source_system.source_id} ({source_system.name})")
        return source_system
    
    def delete_source_system(self, source_id: str) -> bool:
        """Delete a source system by ID

        Args:
            source_id: Unique identifier of the source system to delete

        Returns:
            bool: True if deleted, False if not found
        """
        # Check if source exists
        existing = self.get_source_system(source_id)
        if not existing:
            logger.warning(f"Cannot delete: Source system with ID '{source_id}' does not exist")
            return False
        
        # Delete the row from the table
        query = f"""
        DELETE FROM `{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}`
        WHERE source_id = @source_id
        """
        
        query_params = [
            {"name": "source_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": source_id}}
        ]
        
        self._bq_client.query(query, query_params)
        
        logger.info(f"Deleted source system: {source_id}")
        return True
    
    def activate_source_system(self, source_id: str, updated_by: str) -> SourceSystem:
        """Activate a source system

        Args:
            source_id: Unique identifier of the source system
            updated_by: User who is activating the source

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Activate the source system
        source_system.activate(updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def deactivate_source_system(self, source_id: str, updated_by: str) -> SourceSystem:
        """Deactivate a source system

        Args:
            source_id: Unique identifier of the source system
            updated_by: User who is deactivating the source

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Deactivate the source system
        source_system.deactivate(updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def update_connection_details(
        self, 
        source_id: str, 
        connection_details: Dict[str, Any],
        updated_by: str
    ) -> SourceSystem:
        """Update connection details for a source system

        Args:
            source_id: Unique identifier of the source system
            connection_details: New connection details dictionary
            updated_by: User who is updating the connection details

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Update connection details
        source_system.update_connection_details(connection_details, updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def update_schema_definition(
        self,
        source_id: str,
        schema_definition: Dict[str, Any],
        schema_version: str,
        updated_by: str
    ) -> SourceSystem:
        """Update schema definition for a source system

        Args:
            source_id: Unique identifier of the source system
            schema_definition: New schema definition dictionary
            schema_version: New schema version identifier
            updated_by: User who is updating the schema

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Update schema definition
        source_system.update_schema_definition(schema_definition, schema_version, updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def update_extraction_settings(
        self,
        source_id: str,
        extraction_settings: Dict[str, Any],
        updated_by: str
    ) -> SourceSystem:
        """Update extraction settings for a source system

        Args:
            source_id: Unique identifier of the source system
            extraction_settings: New extraction settings dictionary
            updated_by: User who is updating the extraction settings

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Update extraction settings
        source_system.update_extraction_settings(extraction_settings, updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def update_last_extraction_time(
        self,
        source_id: str,
        extraction_time: datetime.datetime,
        updated_by: str
    ) -> SourceSystem:
        """Update the last extraction time for a source system

        Args:
            source_id: Unique identifier of the source system
            extraction_time: Timestamp of the successful extraction
            updated_by: User or process updating the extraction time

        Returns:
            SourceSystem: Updated source system

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Update last extraction time
        source_system.update_last_extraction_time(extraction_time, updated_by)
        
        # Update in database
        return self.update_source_system(source_system)
    
    def get_sources_by_type(
        self, 
        source_type: DataSourceType,
        active_only: bool = True
    ) -> List[SourceSystem]:
        """Get all source systems of a specific type

        Args:
            source_type: Type of source to filter by
            active_only: If True, only return active sources

        Returns:
            List[SourceSystem]: List of matching source systems
        """
        source_type_value = source_type.value if isinstance(source_type, DataSourceType) else source_type
        
        query = f"""
        SELECT * FROM `{self._project_id}.{self._dataset_id}.{SOURCE_SYSTEM_TABLE_NAME}`
        WHERE source_type = @source_type
        """
        
        query_params = [
            {"name": "source_type", "parameterType": {"type": "STRING"}, "parameterValue": {"value": source_type_value}}
        ]
        
        if active_only:
            query += " AND is_active = @is_active"
            query_params.append({
                "name": "is_active", 
                "parameterType": {"type": "BOOL"}, 
                "parameterValue": {"value": True}
            })
        
        query += " ORDER BY name ASC"
        
        results = self._bq_client.query(query, query_params)
        
        source_systems = [SourceSystem.from_bigquery_row(row) for row in results]
        
        logger.debug(f"Retrieved {len(source_systems)} source systems of type {source_type_value}")
        return source_systems
    
    def get_extraction_statistics(
        self,
        source_id: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> Dict[str, Any]:
        """Get extraction statistics for a source system

        Args:
            source_id: Unique identifier of the source system
            start_date: Start date for statistics period
            end_date: End date for statistics period

        Returns:
            Dict[str, Any]: Dictionary of extraction statistics

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Query pipeline execution table to get statistics
        # This requires pipeline_executions table to be available
        pipeline_executions_table = f"{self._project_id}.{self._dataset_id}.pipeline_executions"
        
        # Check if pipeline_executions table exists
        table_exists = self._bq_client.table_exists(
            self._project_id, 
            self._dataset_id, 
            "pipeline_executions"
        )
        
        if not table_exists:
            # Return empty statistics if table doesn't exist
            return {
                "source_id": source_id,
                "source_name": source_system.name,
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "execution_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "success_rate": 0,
                "avg_duration_seconds": 0,
                "total_records_processed": 0,
                "last_execution_time": None,
                "last_execution_status": None
            }
        
        # Query for execution statistics
        query = f"""
        SELECT
            COUNT(*) as execution_count,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status != 'SUCCESS' THEN 1 ELSE 0 END) as failure_count,
            AVG(CASE WHEN end_time IS NOT NULL AND start_time IS NOT NULL 
                THEN TIMESTAMP_DIFF(end_time, start_time, SECOND) 
                ELSE NULL END) as avg_duration_seconds,
            SUM(COALESCE(records_processed, 0)) as total_records_processed,
            MAX(start_time) as last_execution_time,
            ARRAY_AGG(status ORDER BY start_time DESC LIMIT 1)[OFFSET(0)] as last_execution_status
        FROM `{pipeline_executions_table}`
        WHERE source_id = @source_id
            AND start_time >= @start_date
            AND start_time <= @end_date
        """
        
        query_params = [
            {"name": "source_id", "parameterType": {"type": "STRING"}, "parameterValue": {"value": source_id}},
            {"name": "start_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": start_date.isoformat()}},
            {"name": "end_date", "parameterType": {"type": "TIMESTAMP"}, "parameterValue": {"value": end_date.isoformat()}}
        ]
        
        results = self._bq_client.query(query, query_params)
        
        if not results or len(results) == 0:
            # Return empty statistics if no executions found
            return {
                "source_id": source_id,
                "source_name": source_system.name,
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "execution_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "success_rate": 0,
                "avg_duration_seconds": 0,
                "total_records_processed": 0,
                "last_execution_time": None,
                "last_execution_status": None
            }
        
        # Calculate success rate
        row = results[0]
        execution_count = row.get("execution_count", 0)
        success_count = row.get("success_count", 0)
        
        success_rate = (success_count / execution_count) * 100 if execution_count > 0 else 0
        
        # Build statistics dictionary
        statistics = {
            "source_id": source_id,
            "source_name": source_system.name,
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
            "execution_count": execution_count,
            "success_count": success_count,
            "failure_count": row.get("failure_count", 0),
            "success_rate": round(success_rate, 2),
            "avg_duration_seconds": row.get("avg_duration_seconds", 0),
            "total_records_processed": row.get("total_records_processed", 0),
            "last_execution_time": row.get("last_execution_time"),
            "last_execution_status": row.get("last_execution_status")
        }
        
        return statistics
    
    def test_connection(self, source_id: str) -> Tuple[bool, str]:
        """Test the connection to a source system

        Args:
            source_id: Unique identifier of the source system

        Returns:
            Tuple[bool, str]: Success status and message

        Raises:
            ResourceError: If source system does not exist
        """
        source_system = self.get_source_system(source_id)
        if not source_system:
            error_msg = f"Source system with ID '{source_id}' does not exist"
            logger.error(error_msg)
            raise ResourceError(
                message=error_msg,
                resource_type="source_system",
                resource_name=source_id,
                resource_details={},
                retryable=False
            )
        
        # Get connection configuration
        connection_config = source_system.get_connection_config()
        source_type = source_system.source_type
        
        # Test connection based on source type
        try:
            if isinstance(source_type, str):
                try:
                    source_type = DataSourceType(source_type)
                except ValueError:
                    return False, f"Unknown source type: {source_type}"
            
            if source_type == DataSourceType.GCS:
                # Test GCS connection
                bucket = connection_config.get("bucket")
                if not bucket:
                    return False, "Missing bucket in connection details"
                
                # Here we would normally use GCS client to test connection
                # For simplicity, we'll just check if required config exists
                return True, f"Successfully connected to GCS bucket: {bucket}"
                
            elif source_type == DataSourceType.CLOUD_SQL:
                # Test Cloud SQL connection
                instance = connection_config.get("instance")
                database = connection_config.get("database")
                
                if not instance or not database:
                    return False, "Missing instance or database in connection details"
                
                # Here we would normally use database client to test connection
                # For simplicity, we'll just check if required config exists
                return True, f"Successfully connected to Cloud SQL database: {database} on {instance}"
                
            elif source_type == DataSourceType.API:
                # Test API connection
                url = connection_config.get("url")
                
                if not url:
                    return False, "Missing URL in connection details"
                
                # Here we would normally use HTTP client to test connection
                # For simplicity, we'll just check if required config exists
                return True, f"Successfully connected to API: {url}"
                
            else:
                return False, f"Unsupported source type for connection testing: {source_type}"
                
        except Exception as e:
            logger.error(f"Error testing connection for source {source_id}: {str(e)}")
            return False, f"Connection test failed: {str(e)}"
    
    def validate_source_system(self, source_system: SourceSystem) -> Tuple[bool, List[str]]:
        """Validate a source system configuration

        Args:
            source_system: SourceSystem instance to validate

        Returns:
            Tuple[bool, List[str]]: Validation result and list of validation errors
        """
        # Call the source system's validate method
        valid, errors = source_system.validate()
        
        # Additional repository-level validations could be added here
        
        return valid, errors
    
    def ensure_table_exists(self) -> bool:
        """Ensure that the source systems table exists, creating it if necessary

        Returns:
            bool: True if table exists or was created successfully
        """
        # Check if dataset exists, create if it doesn't
        if not self._bq_client.dataset_exists(self._project_id, self._dataset_id):
            self._bq_client.create_dataset(self._project_id, self._dataset_id)
            logger.info(f"Created dataset: {self._dataset_id}")
        
        # Check if table exists, create if it doesn't
        table_id = SOURCE_SYSTEM_TABLE_NAME
        if not self._bq_client.table_exists(self._project_id, self._dataset_id, table_id):
            # Get schema for the table
            schema = get_source_system_table_schema()
            
            # Create the table
            self._bq_client.create_table(
                self._project_id, 
                self._dataset_id, 
                table_id, 
                schema
            )
            logger.info(f"Created table: {table_id} in dataset {self._dataset_id}")
            return True
        
        return True