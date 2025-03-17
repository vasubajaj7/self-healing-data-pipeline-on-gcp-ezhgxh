"""
Defines the SourceSystem model class that represents data source configurations in the
self-healing data pipeline. This model stores essential information about external data sources
including connection details, schema definitions, and extraction settings for various source 
types like GCS, Cloud SQL, and external APIs.
"""

import datetime
import uuid
import json
import typing
from typing import Dict, List, Optional, Any, Tuple

from ...constants import DataSourceType, FileFormat
from ...utils.logging.logger import get_logger
from ..schema.bigquery_schema import get_schema_field, SchemaField

# Configure logger
logger = get_logger(__name__)

# Define the table name for source systems
SOURCE_SYSTEM_TABLE_NAME = "source_systems"


def generate_source_id() -> str:
    """
    Generates a unique identifier for a source system.
    
    Returns:
        str: Unique source ID with 'src_' prefix
    """
    return f"src_{uuid.uuid4().hex}"


def get_source_system_table_schema() -> List[SchemaField]:
    """
    Returns the BigQuery table schema for the source systems table.
    
    Returns:
        List[SchemaField]: List of schema fields defining the table
    """
    return [
        get_schema_field("source_id", "STRING", "REQUIRED", "Unique identifier for the source system"),
        get_schema_field("name", "STRING", "REQUIRED", "Display name of the source system"),
        get_schema_field("description", "STRING", "NULLABLE", "Description of the source system"),
        get_schema_field("source_type", "STRING", "REQUIRED", "Type of source (GCS, CLOUD_SQL, API, etc.)"),
        get_schema_field("connection_details", "STRING", "REQUIRED", "Connection details as JSON string"),
        get_schema_field("schema_definition", "STRING", "NULLABLE", "Schema definition as JSON string"),
        get_schema_field("schema_version", "STRING", "NULLABLE", "Schema version identifier"),
        get_schema_field("extraction_settings", "STRING", "NULLABLE", "Extraction configuration as JSON string"),
        get_schema_field("is_active", "BOOLEAN", "REQUIRED", "Whether the source is active"),
        get_schema_field("created_at", "TIMESTAMP", "REQUIRED", "Timestamp when the source was created"),
        get_schema_field("updated_at", "TIMESTAMP", "REQUIRED", "Timestamp when the source was last updated"),
        get_schema_field("created_by", "STRING", "REQUIRED", "User who created the source"),
        get_schema_field("updated_by", "STRING", "NULLABLE", "User who last updated the source"),
        get_schema_field("metadata", "STRING", "NULLABLE", "Additional metadata as JSON string"),
        get_schema_field("last_extraction_time", "TIMESTAMP", "NULLABLE", "Timestamp of last successful extraction")
    ]


class SourceSystem:
    """
    Model class representing a data source configuration with connection details and extraction settings.
    """
    
    def __init__(
        self,
        name: str,
        source_type: DataSourceType,
        description: str = "",
        source_id: Optional[str] = None,
        connection_details: Optional[Dict[str, Any]] = None,
        schema_definition: Optional[Dict[str, Any]] = None,
        schema_version: str = "1.0",
        extraction_settings: Optional[Dict[str, Any]] = None,
        is_active: bool = True,
        created_by: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        last_extraction_time: Optional[datetime.datetime] = None
    ):
        """
        Initialize a new source system with provided parameters.
        
        Args:
            name: Display name of the source system
            source_type: Type of source (GCS, CLOUD_SQL, API, etc.)
            description: Description of the source system
            source_id: Unique identifier for the source system, generated if not provided
            connection_details: Connection details specific to the source type
            schema_definition: Schema definition for the source data
            schema_version: Schema version identifier
            extraction_settings: Configuration for data extraction
            is_active: Whether the source is active
            created_by: User who created the source
            metadata: Additional metadata
            last_extraction_time: Timestamp of last successful extraction
        """
        self.source_id = source_id or generate_source_id()
        self.name = name
        self.description = description
        self.source_type = source_type
        self.connection_details = connection_details or {}
        self.schema_definition = schema_definition or {}
        self.schema_version = schema_version
        self.extraction_settings = extraction_settings or {}
        self.is_active = is_active
        self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        self.created_by = created_by
        self.updated_by = created_by
        self.metadata = metadata or {}
        self.last_extraction_time = last_extraction_time
        
        logger.info(f"Created new source system: {self.source_id} ({self.name})")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the source system to a dictionary representation.
        
        Returns:
            dict: Dictionary representation of the source system
        """
        return {
            "source_id": self.source_id,
            "name": self.name,
            "description": self.description,
            "source_type": self.source_type.value if isinstance(self.source_type, DataSourceType) else self.source_type,
            "connection_details": self.connection_details,
            "schema_definition": self.schema_definition,
            "schema_version": self.schema_version,
            "extraction_settings": self.extraction_settings,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "metadata": self.metadata,
            "last_extraction_time": self.last_extraction_time.isoformat() if self.last_extraction_time else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SourceSystem':
        """
        Create a SourceSystem instance from a dictionary.
        
        Args:
            data: Dictionary containing source system data
            
        Returns:
            SourceSystem: New instance created from the dictionary
        """
        # Convert string source type to enum if needed
        source_type = data.get("source_type")
        if isinstance(source_type, str):
            try:
                source_type = DataSourceType(source_type)
            except ValueError:
                logger.warning(f"Invalid source type: {source_type}, using as string")
        
        # Convert timestamp strings to datetime objects
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.datetime.fromisoformat(created_at)
            
        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.datetime.fromisoformat(updated_at)
            
        last_extraction_time = data.get("last_extraction_time")
        if isinstance(last_extraction_time, str) and last_extraction_time:
            last_extraction_time = datetime.datetime.fromisoformat(last_extraction_time)
        
        # Create instance with extracted data
        instance = cls(
            name=data.get("name"),
            source_type=source_type,
            description=data.get("description", ""),
            source_id=data.get("source_id"),
            connection_details=data.get("connection_details", {}),
            schema_definition=data.get("schema_definition", {}),
            schema_version=data.get("schema_version", "1.0"),
            extraction_settings=data.get("extraction_settings", {}),
            is_active=data.get("is_active", True),
            created_by=data.get("created_by", "system"),
            metadata=data.get("metadata", {})
        )
        
        # Set timestamps directly to preserve original values
        if created_at:
            instance.created_at = created_at
        if updated_at:
            instance.updated_at = updated_at
        if last_extraction_time:
            instance.last_extraction_time = last_extraction_time
        
        return instance
    
    @classmethod
    def from_bigquery_row(cls, row: Dict[str, Any]) -> 'SourceSystem':
        """
        Create a SourceSystem instance from a BigQuery row.
        
        Args:
            row: BigQuery row data
            
        Returns:
            SourceSystem: New instance created from the BigQuery row
        """
        # Parse JSON fields
        connection_details = json.loads(row.get("connection_details", "{}"))
        schema_definition = json.loads(row.get("schema_definition", "{}"))
        extraction_settings = json.loads(row.get("extraction_settings", "{}"))
        metadata = json.loads(row.get("metadata", "{}"))
        
        # Convert source_type string to enum
        source_type_str = row.get("source_type")
        try:
            source_type = DataSourceType(source_type_str)
        except ValueError:
            logger.warning(f"Invalid source type in BigQuery: {source_type_str}, using as string")
            source_type = source_type_str
        
        # Create the instance
        return cls(
            name=row.get("name"),
            source_type=source_type,
            description=row.get("description", ""),
            source_id=row.get("source_id"),
            connection_details=connection_details,
            schema_definition=schema_definition,
            schema_version=row.get("schema_version", "1.0"),
            extraction_settings=extraction_settings,
            is_active=row.get("is_active", True),
            created_by=row.get("created_by", "system"),
            metadata=metadata,
            last_extraction_time=row.get("last_extraction_time")
        )
    
    def to_bigquery_row(self) -> Dict[str, Any]:
        """
        Convert the source system to a format suitable for BigQuery insertion.
        
        Returns:
            dict: Dictionary formatted for BigQuery insertion
        """
        return {
            "source_id": self.source_id,
            "name": self.name,
            "description": self.description,
            "source_type": self.source_type.value if isinstance(self.source_type, DataSourceType) else self.source_type,
            "connection_details": json.dumps(self.connection_details),
            "schema_definition": json.dumps(self.schema_definition),
            "schema_version": self.schema_version,
            "extraction_settings": json.dumps(self.extraction_settings),
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "metadata": json.dumps(self.metadata),
            "last_extraction_time": self.last_extraction_time
        }
    
    def update(self, update_data: Dict[str, Any], updated_by: str) -> None:
        """
        Update the source system with new values.
        
        Args:
            update_data: Dictionary of fields to update
            updated_by: User making the update
        """
        # Update basic fields
        for field in ["name", "description"]:
            if field in update_data:
                setattr(self, field, update_data[field])
        
        # Update source_type if provided (with conversion to enum)
        if "source_type" in update_data:
            source_type = update_data["source_type"]
            if isinstance(source_type, str):
                try:
                    source_type = DataSourceType(source_type)
                except ValueError:
                    logger.warning(f"Invalid source type: {source_type}, using as string")
            self.source_type = source_type
        
        # Update complex fields if provided
        for field in ["connection_details", "schema_definition", "extraction_settings", "metadata"]:
            if field in update_data:
                current_value = getattr(self, field, {}) or {}
                new_value = update_data[field]
                if isinstance(new_value, dict):
                    # Merge dictionaries for partial updates
                    current_value.update(new_value)
                    setattr(self, field, current_value)
                else:
                    # Replace with new value if not a dictionary
                    setattr(self, field, new_value)
        
        # Update other fields directly if provided
        for field in ["schema_version", "is_active", "last_extraction_time"]:
            if field in update_data:
                setattr(self, field, update_data[field])
        
        # Always update these fields
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        
        logger.info(f"Updated source system: {self.source_id} (by {updated_by})")
    
    def activate(self, updated_by: str) -> None:
        """
        Activate the source system.
        
        Args:
            updated_by: User activating the source
        """
        self.is_active = True
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Activated source system: {self.source_id} (by {updated_by})")
    
    def deactivate(self, updated_by: str) -> None:
        """
        Deactivate the source system.
        
        Args:
            updated_by: User deactivating the source
        """
        self.is_active = False
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Deactivated source system: {self.source_id} (by {updated_by})")
    
    def update_connection_details(self, connection_details: Dict[str, Any], updated_by: str) -> None:
        """
        Update the connection details for the source system.
        
        Args:
            connection_details: New connection details
            updated_by: User updating the connection details
        """
        self.connection_details = connection_details
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated connection details for source system: {self.source_id} (by {updated_by})")
    
    def update_schema_definition(self, schema_definition: Dict[str, Any], schema_version: str, updated_by: str) -> None:
        """
        Update the schema definition for the source system.
        
        Args:
            schema_definition: New schema definition
            schema_version: New schema version
            updated_by: User updating the schema
        """
        self.schema_definition = schema_definition
        self.schema_version = schema_version
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated schema definition for source system: {self.source_id} (by {updated_by})")
    
    def update_extraction_settings(self, extraction_settings: Dict[str, Any], updated_by: str) -> None:
        """
        Update the extraction settings for the source system.
        
        Args:
            extraction_settings: New extraction settings
            updated_by: User updating the extraction settings
        """
        self.extraction_settings = extraction_settings
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated extraction settings for source system: {self.source_id} (by {updated_by})")
    
    def update_last_extraction_time(self, extraction_time: datetime.datetime, updated_by: str) -> None:
        """
        Update the last extraction time for the source system.
        
        Args:
            extraction_time: Timestamp of successful extraction
            updated_by: User or process updating the extraction time
        """
        self.last_extraction_time = extraction_time
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated last extraction time for source system: {self.source_id} (by {updated_by})")
    
    def update_metadata(self, metadata: Dict[str, Any], updated_by: str) -> None:
        """
        Update the metadata for the source system.
        
        Args:
            metadata: New metadata dictionary
            updated_by: User updating the metadata
        """
        current_metadata = self.metadata or {}
        current_metadata.update(metadata)
        self.metadata = current_metadata
        self.updated_at = datetime.datetime.now()
        self.updated_by = updated_by
        logger.info(f"Updated metadata for source system: {self.source_id} (by {updated_by})")
    
    def get_connection_config(self) -> Dict[str, Any]:
        """
        Get the connection configuration for the source system.
        
        Returns:
            dict: Connection configuration dictionary
        """
        # Start with a copy of the connection details
        config = dict(self.connection_details)
        
        # Add source type and source ID
        config["source_type"] = self.source_type.value if isinstance(self.source_type, DataSourceType) else self.source_type
        config["source_id"] = self.source_id
        
        # Add relevant extraction settings
        if self.extraction_settings:
            # Add extraction mode, batch size, etc. if defined
            for key in ["extraction_mode", "batch_size", "timeout", "retry_count"]:
                if key in self.extraction_settings:
                    config[key] = self.extraction_settings[key]
        
        return config
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate the source system for completeness and correctness.
        
        Returns:
            tuple: (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields
        if not self.name:
            errors.append("Name is required")
            
        if not self.source_type:
            errors.append("Source type is required")
        
        # Validate source type is valid enum
        if isinstance(self.source_type, str):
            try:
                DataSourceType(self.source_type)
            except ValueError:
                errors.append(f"Invalid source type: {self.source_type}")
        
        # Validate connection details based on source type
        if isinstance(self.source_type, DataSourceType) or (
            isinstance(self.source_type, str) and self.source_type in [t.value for t in DataSourceType]
        ):
            source_type_value = self.source_type.value if isinstance(self.source_type, DataSourceType) else self.source_type
            
            if not self.connection_details:
                errors.append("Connection details are required")
            else:
                # GCS validation
                if source_type_value == DataSourceType.GCS.value:
                    if "bucket" not in self.connection_details:
                        errors.append("GCS connection details must include 'bucket'")
                    if "path_prefix" not in self.connection_details:
                        errors.append("GCS connection details should include 'path_prefix'")
                
                # Cloud SQL validation
                elif source_type_value == DataSourceType.CLOUD_SQL.value:
                    for field in ["instance", "database", "username"]:
                        if field not in self.connection_details:
                            errors.append(f"Cloud SQL connection details must include '{field}'")
                
                # API validation
                elif source_type_value == DataSourceType.API.value:
                    if "url" not in self.connection_details:
                        errors.append("API connection details must include 'url'")
                    if "method" not in self.connection_details:
                        errors.append("API connection details should include 'method'")
        
        # Validate schema definition if provided
        if self.schema_definition and not isinstance(self.schema_definition, dict):
            errors.append("Schema definition must be a dictionary")
        
        # Validate extraction settings based on source type
        if self.extraction_settings:
            if not isinstance(self.extraction_settings, dict):
                errors.append("Extraction settings must be a dictionary")
            
            # Specific validations based on source type
            if isinstance(self.source_type, DataSourceType) or (
                isinstance(self.source_type, str) and self.source_type in [t.value for t in DataSourceType]
            ):
                source_type_value = self.source_type.value if isinstance(self.source_type, DataSourceType) else self.source_type
                
                # GCS extraction settings validation
                if source_type_value == DataSourceType.GCS.value:
                    if "file_format" in self.extraction_settings:
                        file_format = self.extraction_settings["file_format"]
                        if isinstance(file_format, str):
                            try:
                                FileFormat(file_format)
                            except ValueError:
                                errors.append(f"Invalid file format: {file_format}")
                
                # Cloud SQL extraction settings validation
                elif source_type_value == DataSourceType.CLOUD_SQL.value:
                    if "incremental_column" in self.extraction_settings and not self.extraction_settings.get("incremental_query"):
                        errors.append("Incremental column requires incremental query")
        
        return len(errors) == 0, errors