"""
Schema Registry for the self-healing data pipeline.

This module implements a centralized schema registry that manages schema definitions
across different data sources and systems. It provides capabilities for schema versioning,
evolution tracking, compatibility checking, and integration with BigQuery.

The registry serves as a source of truth for data schemas in the pipeline and enables
self-healing capabilities through schema evolution management.
"""

import uuid
import datetime
import json
from typing import Dict, List, Optional, Union, Any, Tuple
import semver  # version 2.13.0+

# Internal imports
from ...constants import DataSourceType, FileFormat
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.storage.firestore_client import FirestoreClient
from ...utils.storage.bigquery_client import BigQueryClient
from ...utils.schema.schema_utils import (
    detect_schema_format,
    validate_schema,
    compare_schemas,
    get_schema_fingerprint,
    extract_schema_from_data,
    is_schema_compatible,
    create_bigquery_schema
)
from ...utils.errors.error_types import SchemaValidationError, SchemaEvolutionError

# Configure logging
logger = get_logger(__name__)

# Default configuration values
DEFAULT_SCHEMA_COLLECTION = "schema_registry"
DEFAULT_SCHEMA_TABLE = "schema_registry"
DEFAULT_COMPATIBILITY_TYPE = "BACKWARD"


def create_schema_record(schema_name: str, schema: dict, schema_format: str, version: str) -> str:
    """
    Creates a new schema record with a unique identifier.
    
    Args:
        schema_name: Name of the schema
        schema: Schema definition
        schema_format: Format of the schema (bigquery, avro, etc.)
        version: Schema version
        
    Returns:
        The unique identifier for the created schema record
    """
    schema_id = str(uuid.uuid4())
    
    # Create schema record
    schema_record = {
        "schema_id": schema_id,
        "schema_name": schema_name,
        "schema": schema,
        "schema_format": schema_format,
        "version": version,
        "created_at": datetime.datetime.utcnow().isoformat(),
        "environment": get_config().get_environment()
    }
    
    # Register the schema
    registry = SchemaRegistry()
    registry._store_schema_record(schema_record)
    
    return schema_id


class SchemaRegistry:
    """
    Central registry for managing and tracking schema definitions across the pipeline.
    
    This class provides methods for schema registration, validation, compatibility checking,
    and integration with data systems like BigQuery. It serves as a source of truth for
    schema definitions and enables self-healing capabilities through schema evolution.
    """
    
    def __init__(self):
        """
        Initialize the schema registry with storage clients and configuration.
        """
        # Initialize storage clients
        self._firestore_client = FirestoreClient()
        
        # Get configuration
        config = get_config()
        
        # Set collection and table names from configuration or use defaults
        self._schema_collection = config.get("schema_registry.collection", DEFAULT_SCHEMA_COLLECTION)
        self._schema_table = config.get("schema_registry.table", DEFAULT_SCHEMA_TABLE)
        
        # Configure BigQuery storage if enabled
        self._enable_bigquery_storage = config.get("schema_registry.enable_bigquery_storage", True)
        if self._enable_bigquery_storage:
            self._bigquery_client = BigQueryClient()
        else:
            self._bigquery_client = None
            
        # Initialize schema cache for performance
        self._schema_cache = {}
        
        logger.info(f"Schema registry initialized with collection {self._schema_collection}")
        
    def register_schema(self, 
                      schema_name: str, 
                      schema: dict, 
                      schema_format: str = None, 
                      version: str = None,
                      source_id: str = None,
                      description: str = None) -> str:
        """
        Registers a schema in the registry with versioning.
        
        Args:
            schema_name: Name of the schema
            schema: Schema definition dictionary
            schema_format: Format of the schema (bigquery, avro, etc.)
            version: Optional explicit version, otherwise computed
            source_id: Optional reference to source system
            description: Optional description of the schema
            
        Returns:
            The schema record ID
        
        Raises:
            SchemaValidationError: If schema validation fails
        """
        # Validate schema
        try:
            validate_schema(schema, schema_format)
        except Exception as e:
            logger.error(f"Schema validation failed for {schema_name}: {str(e)}")
            raise SchemaValidationError(
                message=f"Invalid schema: {str(e)}",
                validation_details={"schema_name": schema_name, "error": str(e)}
            )
        
        # Detect schema format if not provided
        if not schema_format:
            schema_format = detect_schema_format(schema)
            logger.debug(f"Detected schema format: {schema_format}")
        
        # Generate schema fingerprint for comparison
        fingerprint = get_schema_fingerprint(schema, schema_format)
        
        # Check if schema with same fingerprint already exists
        existing_schemas = self.get_schema_history(schema_name)
        existing_schema = None
        
        for schema_record in existing_schemas:
            if schema_record.get("fingerprint") == fingerprint:
                logger.info(f"Schema with identical fingerprint already exists: {schema_record.get('schema_id')}")
                return schema_record.get("schema_id")
            
            # Keep track of the most recent schema for versioning
            if not existing_schema or semver.compare(schema_record.get("version"), existing_schema.get("version")) > 0:
                existing_schema = schema_record
        
        # Generate version if not provided
        if not version:
            version = self._get_next_version(schema_name, schema, existing_schema)
            logger.debug(f"Generated schema version: {version}")
        
        # Create schema ID
        schema_id = str(uuid.uuid4())
        
        # Create schema record
        schema_record = self._create_schema_record(
            schema_id=schema_id,
            schema_name=schema_name,
            schema=schema,
            schema_format=schema_format,
            version=version,
            fingerprint=fingerprint,
            source_id=source_id,
            description=description
        )
        
        # Store schema record
        success = self._store_schema_record(schema_record)
        
        if success:
            logger.info(f"Schema registered successfully: {schema_id}")
            return schema_id
        else:
            logger.error(f"Failed to register schema: {schema_name}")
            raise RuntimeError(f"Failed to register schema: {schema_name}")
    
    def get_schema(self, schema_name: str, version: str = None) -> dict:
        """
        Retrieves a schema by name and version.
        
        Args:
            schema_name: Name of the schema
            version: Optional version, gets latest if not specified
            
        Returns:
            The schema record or None if not found
        """
        # Check cache first
        cache_key = f"{schema_name}:{version or 'latest'}"
        if cache_key in self._schema_cache:
            logger.debug(f"Schema found in cache: {cache_key}")
            return self._schema_cache[cache_key]
        
        # Query parameters
        query_params = {"schema_name": schema_name}
        
        # If version is specified, add it to query
        if version:
            query_params["version"] = version
            query = self._firestore_client.query(
                collection=self._schema_collection,
                filters=query_params
            )
            
            # Get first matching schema
            schemas = list(query.limit(1).stream())
            if schemas:
                schema_record = schemas[0].to_dict()
                # Add to cache
                self._schema_cache[cache_key] = schema_record
                return schema_record
            return None
        else:
            # Get all versions and find latest
            query = self._firestore_client.query(
                collection=self._schema_collection,
                filters={"schema_name": schema_name}
            )
            
            # Find latest version
            latest_schema = None
            latest_version = "0.0.0"
            
            for doc in query.stream():
                schema_record = doc.to_dict()
                schema_version = schema_record.get("version", "0.0.0")
                
                if not latest_schema or semver.compare(schema_version, latest_version) > 0:
                    latest_schema = schema_record
                    latest_version = schema_version
            
            if latest_schema:
                # Add to cache
                self._schema_cache[cache_key] = latest_schema
                return latest_schema
            
            return None
    
    def get_schema_version(self, schema_name: str, version: str = None) -> dict:
        """
        Retrieves a specific version of a schema.
        
        Args:
            schema_name: Name of the schema
            version: Optional version, gets latest if not specified
            
        Returns:
            The schema definition or None if not found
        """
        schema_record = self.get_schema(schema_name, version)
        if schema_record:
            return schema_record.get("schema")
        return None
    
    def get_schema_history(self, schema_name: str) -> List[dict]:
        """
        Retrieves the version history of a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of schema versions with metadata, sorted by version
        """
        query = self._firestore_client.query(
            collection=self._schema_collection,
            filters={"schema_name": schema_name}
        )
        
        # Get all versions
        schema_records = [doc.to_dict() for doc in query.stream()]
        
        # Sort by version (descending)
        schema_records.sort(
            key=lambda x: x.get("version", "0.0.0"), 
            reverse=True
        )
        
        return schema_records
    
    def validate_schema_compatibility(self, 
                                    schema_name: str, 
                                    new_schema: dict, 
                                    compatibility_type: str = None,
                                    base_version: str = None) -> dict:
        """
        Validates compatibility between schema versions.
        
        Args:
            schema_name: Name of the schema
            new_schema: New schema definition to validate
            compatibility_type: Type of compatibility to check (BACKWARD, FORWARD, FULL)
            base_version: Optional specific version to check against, uses latest if not specified
            
        Returns:
            Compatibility validation result
        """
        # Default compatibility type if not specified
        if not compatibility_type:
            compatibility_type = get_config().get(
                "schema_registry.default_compatibility",
                DEFAULT_COMPATIBILITY_TYPE
            )
        
        # Get base schema
        base_schema_record = self.get_schema(schema_name, base_version)
        if not base_schema_record:
            logger.warning(f"No existing schema found for compatibility check: {schema_name}")
            return {
                "compatible": True,
                "reason": "No existing schema found for comparison",
                "compatibility_type": compatibility_type
            }
        
        base_schema = base_schema_record.get("schema")
        base_format = base_schema_record.get("schema_format")
        
        # Validate new schema
        try:
            validate_schema(new_schema, base_format)
        except Exception as e:
            return {
                "compatible": False,
                "reason": f"Schema validation failed: {str(e)}",
                "compatibility_type": compatibility_type
            }
        
        # Check compatibility
        is_compatible, details = is_schema_compatible(
            base_schema=base_schema,
            new_schema=new_schema,
            compatibility_type=compatibility_type,
            schema_format=base_format
        )
        
        # Create result object
        result = {
            "compatible": is_compatible,
            "compatibility_type": compatibility_type,
            "base_version": base_schema_record.get("version"),
            "details": details
        }
        
        if not is_compatible:
            result["reason"] = details.get("reason", "Unknown compatibility issue")
        
        return result
    
    def compare_schemas(self, schema_name: str, version1: str, version2: str) -> dict:
        """
        Compares two schema versions and identifies differences.
        
        Args:
            schema_name: Name of the schema
            version1: First version to compare
            version2: Second version to compare
            
        Returns:
            Comparison result with differences
        """
        # Get both schema versions
        schema1_record = self.get_schema(schema_name, version1)
        schema2_record = self.get_schema(schema_name, version2)
        
        if not schema1_record or not schema2_record:
            missing = []
            if not schema1_record:
                missing.append(f"version {version1}")
            if not schema2_record:
                missing.append(f"version {version2}")
                
            raise ValueError(f"Cannot compare schemas: missing {', '.join(missing)}")
        
        schema1 = schema1_record.get("schema")
        schema2 = schema2_record.get("schema")
        schema_format = schema1_record.get("schema_format")
        
        # Use schema_utils to compare schemas
        comparison = compare_schemas(schema1, schema2, schema_format)
        
        # Format result
        result = {
            "schema_name": schema_name,
            "version1": version1,
            "version2": version2,
            "differences": comparison,
            "added_fields": comparison.get("added", []),
            "removed_fields": comparison.get("removed", []),
            "modified_fields": comparison.get("modified", []),
            "compatible": len(comparison.get("breaking_changes", [])) == 0
        }
        
        return result
    
    def detect_schema_drift(self, schema_name: str, data_sample: dict, version: str = None) -> dict:
        """
        Detects drift between registered schema and actual data.
        
        Args:
            schema_name: Name of the schema
            data_sample: Sample of data to check for drift
            version: Optional specific version to check against, uses latest if not specified
            
        Returns:
            Schema drift analysis
        """
        # Get registered schema
        schema_record = self.get_schema(schema_name, version)
        if not schema_record:
            logger.warning(f"No schema found for drift detection: {schema_name}")
            return {
                "schema_name": schema_name,
                "drift_detected": False,
                "reason": "No schema found for comparison",
                "version": version or "latest"
            }
        
        registered_schema = schema_record.get("schema")
        schema_format = schema_record.get("schema_format")
        
        # Extract schema from data sample
        try:
            extracted_schema = extract_schema_from_data(data_sample, schema_format)
        except Exception as e:
            logger.error(f"Failed to extract schema from data: {str(e)}")
            return {
                "schema_name": schema_name,
                "drift_detected": True,
                "reason": f"Could not extract schema from data: {str(e)}",
                "version": schema_record.get("version")
            }
        
        # Compare schemas
        comparison = compare_schemas(registered_schema, extracted_schema, schema_format)
        
        # Analyze drift
        has_drift = (
            len(comparison.get("added", [])) > 0 or 
            len(comparison.get("removed", [])) > 0 or 
            len(comparison.get("modified", [])) > 0
        )
        
        # Calculate drift metrics
        drift_score = 0
        total_fields = len(registered_schema.get("fields", []))
        if total_fields > 0:
            changed_fields = (
                len(comparison.get("added", [])) + 
                len(comparison.get("removed", [])) + 
                len(comparison.get("modified", []))
            )
            drift_score = changed_fields / total_fields
        
        drift_severity = "LOW"
        if drift_score > 0.5:
            drift_severity = "HIGH"
        elif drift_score > 0.2:
            drift_severity = "MEDIUM"
        
        # Create drift analysis result
        result = {
            "schema_name": schema_name,
            "version": schema_record.get("version"),
            "drift_detected": has_drift,
            "drift_score": drift_score,
            "drift_severity": drift_severity,
            "added_fields": comparison.get("added", []),
            "removed_fields": comparison.get("removed", []),
            "modified_fields": comparison.get("modified", []),
            "breaking_changes": comparison.get("breaking_changes", [])
        }
        
        return result
    
    def suggest_schema_evolution(self, 
                               schema_name: str, 
                               data_sample: dict,
                               compatibility_type: str = None,
                               base_version: str = None) -> dict:
        """
        Suggests schema evolution based on detected drift.
        
        Args:
            schema_name: Name of the schema
            data_sample: Sample of data to check for drift
            compatibility_type: Type of compatibility to maintain
            base_version: Optional specific version to base evolution on
            
        Returns:
            Suggested schema evolution
        """
        # Default compatibility type if not specified
        if not compatibility_type:
            compatibility_type = get_config().get(
                "schema_registry.default_compatibility",
                DEFAULT_COMPATIBILITY_TYPE
            )
        
        # Detect schema drift
        drift_analysis = self.detect_schema_drift(schema_name, data_sample, base_version)
        
        if not drift_analysis.get("drift_detected"):
            return {
                "schema_name": schema_name,
                "evolution_needed": False,
                "reason": "No schema drift detected",
                "version": drift_analysis.get("version")
            }
        
        # Get base schema
        schema_record = self.get_schema(schema_name, base_version)
        if not schema_record:
            return {
                "schema_name": schema_name,
                "evolution_needed": False,
                "reason": "No base schema found",
                "version": base_version or "latest"
            }
        
        base_schema = schema_record.get("schema")
        schema_format = schema_record.get("schema_format")
        
        # Extract schema from data
        extracted_schema = extract_schema_from_data(data_sample, schema_format)
        
        # Create evolved schema
        evolved_schema = base_schema.copy()
        
        # Handle added fields
        fields = evolved_schema.get("fields", [])
        field_names = [f["name"] for f in fields]
        
        for added_field in drift_analysis.get("added_fields", []):
            # Find the field in extracted schema
            for field in extracted_schema.get("fields", []):
                if field["name"] == added_field:
                    # Add if doesn't exist
                    if added_field not in field_names:
                        # Make nullable to maintain backward compatibility
                        if compatibility_type == "BACKWARD":
                            field["mode"] = "NULLABLE"
                        fields.append(field)
                        field_names.append(added_field)
        
        # Handle modified fields based on compatibility type
        for modified_field in drift_analysis.get("modified_fields", []):
            if compatibility_type == "BACKWARD":
                # For backward compatibility, we can't change field types in a breaking way
                continue
            elif compatibility_type == "FORWARD":
                # For forward compatibility, we can adapt to new data format
                # Find the field in extracted schema
                for field in extracted_schema.get("fields", []):
                    if field["name"] == modified_field:
                        # Replace the field definition
                        for i, f in enumerate(fields):
                            if f["name"] == modified_field:
                                fields[i] = field
                                break
            elif compatibility_type == "FULL":
                # For full compatibility, we need to ensure both backward and forward
                # This is more restrictive, so we wouldn't change field types
                continue
        
        # Update fields in evolved schema
        evolved_schema["fields"] = fields
        
        # Validate compatibility of evolved schema
        compatibility = self.validate_schema_compatibility(
            schema_name=schema_name,
            new_schema=evolved_schema,
            compatibility_type=compatibility_type,
            base_version=schema_record.get("version")
        )
        
        # Create evolution suggestion
        suggestion = {
            "schema_name": schema_name,
            "base_version": schema_record.get("version"),
            "evolution_needed": True,
            "suggested_schema": evolved_schema,
            "compatibility": compatibility,
            "drift_analysis": drift_analysis,
            "compatibility_type": compatibility_type
        }
        
        return suggestion
    
    def extract_schema_from_bigquery(self, 
                                   dataset_id: str, 
                                   table_id: str, 
                                   schema_name: str = None,
                                   schema_format: str = "bigquery",
                                   register: bool = False) -> dict:
        """
        Extracts schema from a BigQuery table.
        
        Args:
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            schema_name: Name to register the schema as (defaults to table_id)
            schema_format: Format to store the schema in
            register: Whether to register the extracted schema
            
        Returns:
            Extracted schema
        """
        if not self._bigquery_client:
            raise RuntimeError("BigQuery client not initialized")
        
        # Use table_id as schema_name if not provided
        if not schema_name:
            schema_name = table_id
        
        # Get table schema from BigQuery
        table_ref = f"{dataset_id}.{table_id}"
        try:
            table = self._bigquery_client.get_table(table_ref)
            schema = table.schema
            
            # Convert to dictionary format
            schema_dict = {
                "fields": []
            }
            
            for field in schema:
                schema_dict["fields"].append({
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode or "NULLABLE",
                    "description": field.description
                })
            
            # Register schema if requested
            if register:
                self.register_schema(
                    schema_name=schema_name,
                    schema=schema_dict,
                    schema_format=schema_format,
                    description=f"Extracted from BigQuery table {table_ref}"
                )
            
            return schema_dict
        except Exception as e:
            logger.error(f"Failed to extract schema from BigQuery table {table_ref}: {str(e)}")
            raise RuntimeError(f"BigQuery schema extraction failed: {str(e)}")
    
    def extract_schema_from_data(self, 
                               data_sample: dict, 
                               schema_name: str = None,
                               schema_format: str = "bigquery",
                               register: bool = False) -> dict:
        """
        Extracts schema from a data sample.
        
        Args:
            data_sample: Data sample to extract schema from
            schema_name: Name to register the schema as
            schema_format: Format to store the schema in
            register: Whether to register the extracted schema
            
        Returns:
            Extracted schema
        """
        try:
            # Extract schema using utility function
            schema = extract_schema_from_data(data_sample, schema_format)
            
            # Register schema if requested and name is provided
            if register and schema_name:
                self.register_schema(
                    schema_name=schema_name,
                    schema=schema,
                    schema_format=schema_format,
                    description="Extracted from data sample"
                )
            
            return schema
        except Exception as e:
            logger.error(f"Failed to extract schema from data sample: {str(e)}")
            raise RuntimeError(f"Schema extraction from data failed: {str(e)}")
    
    def register_source_schema(self, 
                             source_id: str,
                             source_type: str,
                             schema: dict,
                             schema_name: str = None,
                             schema_format: str = None) -> str:
        """
        Registers schema for a data source.
        
        Args:
            source_id: Unique identifier for the data source
            source_type: Type of data source (e.g., GCS, CLOUD_SQL)
            schema: Schema definition
            schema_name: Optional name for the schema (defaults to source_id)
            schema_format: Format of the schema (detected if not provided)
            
        Returns:
            The schema record ID
        """
        # Use source_id as schema_name if not provided
        if not schema_name:
            schema_name = f"{source_type.lower()}_{source_id}"
        
        # Register schema with source reference
        return self.register_schema(
            schema_name=schema_name,
            schema=schema,
            schema_format=schema_format,
            source_id=source_id,
            description=f"Schema for {source_type} source: {source_id}"
        )
    
    def apply_schema_to_bigquery(self, 
                               schema_name: str, 
                               version: str, 
                               dataset_id: str, 
                               table_id: str,
                               create_if_not_exists: bool = False) -> bool:
        """
        Applies a registered schema to a BigQuery table.
        
        Args:
            schema_name: Name of the schema
            version: Schema version to apply
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            create_if_not_exists: Whether to create the table if it doesn't exist
            
        Returns:
            True if successful, False otherwise
        """
        if not self._bigquery_client:
            raise RuntimeError("BigQuery client not initialized")
        
        # Get schema
        schema_record = self.get_schema(schema_name, version)
        if not schema_record:
            logger.error(f"Schema not found: {schema_name} version {version}")
            return False
        
        schema = schema_record.get("schema")
        schema_format = schema_record.get("schema_format")
        
        # Convert to BigQuery schema format if needed
        if schema_format != "bigquery":
            bigquery_schema = create_bigquery_schema(schema, schema_format)
        else:
            bigquery_schema = schema
        
        # Check if table exists
        table_ref = f"{dataset_id}.{table_id}"
        table_exists = self._bigquery_client.table_exists(dataset_id, table_id)
        
        if table_exists:
            # Update schema
            try:
                self._bigquery_client.update_table_schema(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    schema=bigquery_schema
                )
                logger.info(f"Updated schema for BigQuery table {table_ref}")
                return True
            except Exception as e:
                logger.error(f"Failed to update schema for BigQuery table {table_ref}: {str(e)}")
                return False
        elif create_if_not_exists:
            # Create table
            try:
                self._bigquery_client.create_table(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    schema=bigquery_schema
                )
                logger.info(f"Created BigQuery table {table_ref} with schema")
                return True
            except Exception as e:
                logger.error(f"Failed to create BigQuery table {table_ref}: {str(e)}")
                return False
        else:
            logger.warning(f"BigQuery table {table_ref} does not exist and create_if_not_exists is False")
            return False
    
    def export_schema_registry(self, 
                             start_date: datetime.datetime = None, 
                             end_date: datetime.datetime = None) -> bool:
        """
        Exports the schema registry to BigQuery for analysis.
        
        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            True if export successful, False otherwise
        """
        if not self._bigquery_client or not self._enable_bigquery_storage:
            logger.warning("BigQuery storage not enabled for schema registry")
            return False
        
        try:
            # Build query filters
            filters = {}
            if start_date:
                filters["created_at>="] = start_date.isoformat()
            if end_date:
                filters["created_at<="] = end_date.isoformat()
            
            # Query Firestore for schema records
            query = self._firestore_client.query(
                collection=self._schema_collection,
                filters=filters
            )
            
            # Get all records
            schema_records = [doc.to_dict() for doc in query.stream()]
            
            if not schema_records:
                logger.warning("No schema records found for export")
                return False
            
            # Format records for BigQuery
            for record in schema_records:
                # Convert schema to JSON string
                if "schema" in record and isinstance(record["schema"], dict):
                    record["schema"] = json.dumps(record["schema"])
            
            # Insert into BigQuery
            dataset_id = get_config().get("schema_registry.bigquery_dataset")
            table_id = self._schema_table
            
            self._bigquery_client.insert_rows_json(
                dataset_id=dataset_id,
                table_id=table_id,
                json_rows=schema_records
            )
            
            logger.info(f"Exported {len(schema_records)} schema records to BigQuery")
            return True
        except Exception as e:
            logger.error(f"Failed to export schema registry to BigQuery: {str(e)}")
            return False
    
    def search_schemas(self, search_criteria: dict, limit: int = 100) -> List[dict]:
        """
        Searches for schemas based on criteria.
        
        Args:
            search_criteria: Dictionary of search criteria
            limit: Maximum number of results to return
            
        Returns:
            List of matching schema records
        """
        try:
            # Build query
            query = self._firestore_client.query(
                collection=self._schema_collection,
                filters=search_criteria
            )
            
            # Get results
            schema_records = [doc.to_dict() for doc in query.limit(limit).stream()]
            
            return schema_records
        except Exception as e:
            logger.error(f"Failed to search schemas: {str(e)}")
            return []
    
    def _store_schema_record(self, schema_record: dict) -> bool:
        """
        Internal method to store a schema record.
        
        Args:
            schema_record: Schema record to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Store in Firestore
            schema_id = schema_record.get("schema_id")
            self._firestore_client.set_document(
                collection=self._schema_collection,
                document_id=schema_id,
                data=schema_record
            )
            
            # Store in BigQuery if enabled
            if self._enable_bigquery_storage and self._bigquery_client:
                # Format record for BigQuery
                bq_record = schema_record.copy()
                
                # Convert schema to JSON string
                if "schema" in bq_record and isinstance(bq_record["schema"], dict):
                    bq_record["schema"] = json.dumps(bq_record["schema"])
                
                dataset_id = get_config().get("schema_registry.bigquery_dataset")
                self._bigquery_client.insert_rows_json(
                    dataset_id=dataset_id,
                    table_id=self._schema_table,
                    json_rows=[bq_record]
                )
            
            # Update cache
            cache_key = f"{schema_record.get('schema_name')}:{schema_record.get('version')}"
            self._schema_cache[cache_key] = schema_record
            
            # Also update latest cache entry
            latest_key = f"{schema_record.get('schema_name')}:latest"
            latest_schema = self._schema_cache.get(latest_key)
            
            if not latest_schema or semver.compare(
                schema_record.get("version"), 
                latest_schema.get("version")
            ) > 0:
                self._schema_cache[latest_key] = schema_record
            
            return True
        except Exception as e:
            logger.error(f"Failed to store schema record: {str(e)}")
            return False
    
    def _create_schema_record(self,
                            schema_id: str,
                            schema_name: str,
                            schema: dict,
                            schema_format: str,
                            version: str,
                            fingerprint: str,
                            source_id: str = None,
                            description: str = None) -> dict:
        """
        Internal method to create a standardized schema record.
        
        Args:
            schema_id: Unique identifier for the schema
            schema_name: Name of the schema
            schema: Schema definition
            schema_format: Format of the schema
            version: Schema version
            fingerprint: Schema fingerprint for comparison
            source_id: Optional reference to source system
            description: Optional description of the schema
            
        Returns:
            Formatted schema record
        """
        # Create base record
        record = {
            "schema_id": schema_id,
            "schema_name": schema_name,
            "schema": schema,
            "schema_format": schema_format,
            "version": version,
            "fingerprint": fingerprint,
            "created_at": datetime.datetime.utcnow().isoformat(),
            "environment": get_config().get_environment()
        }
        
        # Add optional fields
        if source_id:
            record["source_id"] = source_id
            
        if description:
            record["description"] = description
        
        return record
    
    def _get_next_version(self, 
                         schema_name: str, 
                         new_schema: dict, 
                         current_schema: dict = None) -> str:
        """
        Internal method to determine the next version for a schema.
        
        Args:
            schema_name: Name of the schema
            new_schema: New schema definition
            current_schema: Current schema record or None
            
        Returns:
            Next semantic version
        """
        # If no current schema, start at 1.0.0
        if not current_schema:
            return "1.0.0"
        
        current_version = current_schema.get("version", "0.0.0")
        current_schema_def = current_schema.get("schema", {})
        schema_format = current_schema.get("schema_format", "bigquery")
        
        # Compare schemas
        comparison = compare_schemas(current_schema_def, new_schema, schema_format)
        
        # Determine version increment based on changes
        if len(comparison.get("breaking_changes", [])) > 0:
            # Major version increment for breaking changes
            return semver.bump_major(current_version)
        elif len(comparison.get("added", [])) > 0 or len(comparison.get("modified", [])) > 0:
            # Minor version increment for compatible additions/changes
            return semver.bump_minor(current_version)
        elif len(comparison.get("removed", [])) > 0:
            # Minor version for removals (could be breaking, depends on compatibility)
            return semver.bump_minor(current_version)
        else:
            # Patch version for non-structural changes
            return semver.bump_patch(current_version)


class SchemaEvolution:
    """
    Manages schema evolution and compatibility across versions.
    
    This class provides functionality for planning and executing schema
    changes while maintaining compatibility constraints. It works with
    the SchemaRegistry to track schema versions and manage transitions.
    """
    
    def __init__(self, registry: SchemaRegistry):
        """
        Initialize the SchemaEvolution with a SchemaRegistry instance.
        
        Args:
            registry: SchemaRegistry instance
        """
        self._registry = registry
    
    def plan_evolution(self, 
                     schema_name: str, 
                     changes: dict, 
                     base_version: str = None,
                     compatibility_type: str = None) -> dict:
        """
        Plans a schema evolution based on changes.
        
        Args:
            schema_name: Name of the schema
            changes: Dictionary of changes to apply
            base_version: Optional base version, uses latest if not specified
            compatibility_type: Type of compatibility to maintain
            
        Returns:
            Evolution plan with compatibility assessment
        """
        # Get base schema
        base_schema_record = self._registry.get_schema(schema_name, base_version)
        if not base_schema_record:
            raise ValueError(f"Base schema not found: {schema_name} version {base_version}")
        
        base_schema = base_schema_record.get("schema")
        schema_format = base_schema_record.get("schema_format")
        
        # Apply changes to create evolved schema
        evolved_schema = base_schema.copy()
        
        # Process field additions
        if "add_fields" in changes:
            fields = evolved_schema.get("fields", [])
            field_names = [f["name"] for f in fields]
            
            for new_field in changes["add_fields"]:
                if new_field["name"] not in field_names:
                    fields.append(new_field)
                    field_names.append(new_field["name"])
                else:
                    logger.warning(f"Field already exists: {new_field['name']}")
            
            evolved_schema["fields"] = fields
        
        # Process field removals
        if "remove_fields" in changes:
            fields = evolved_schema.get("fields", [])
            evolved_schema["fields"] = [f for f in fields if f["name"] not in changes["remove_fields"]]
        
        # Process field modifications
        if "modify_fields" in changes:
            fields = evolved_schema.get("fields", [])
            field_dict = {f["name"]: f for f in fields}
            
            for modified_field in changes["modify_fields"]:
                name = modified_field["name"]
                if name in field_dict:
                    # Update field with new properties
                    for k, v in modified_field.items():
                        if k != "name":
                            field_dict[name][k] = v
                else:
                    logger.warning(f"Field not found for modification: {name}")
            
            # Rebuild fields list
            evolved_schema["fields"] = list(field_dict.values())
        
        # Validate compatibility
        compatibility = self._registry.validate_schema_compatibility(
            schema_name=schema_name,
            new_schema=evolved_schema,
            compatibility_type=compatibility_type,
            base_version=base_schema_record.get("version")
        )
        
        # Create evolution plan
        plan = {
            "schema_name": schema_name,
            "base_version": base_schema_record.get("version"),
            "evolved_schema": evolved_schema,
            "changes": changes,
            "compatibility": compatibility,
            "next_version": self._get_next_version(base_schema_record.get("version"), compatibility)
        }
        
        return plan
    
    def execute_evolution(self, 
                        schema_name: str, 
                        evolution_plan: dict,
                        force: bool = False) -> dict:
        """
        Executes a schema evolution plan.
        
        Args:
            schema_name: Name of the schema
            evolution_plan: Evolution plan from plan_evolution()
            force: Whether to force evolution even if not compatible
            
        Returns:
            Evolution result with new schema version
        """
        # Validate plan
        if "evolved_schema" not in evolution_plan:
            raise ValueError("Invalid evolution plan: missing evolved_schema")
        
        # Check compatibility unless force is true
        if not force and not evolution_plan.get("compatibility", {}).get("compatible", False):
            raise SchemaEvolutionError(
                message="Cannot execute evolution: not compatible with base schema",
                schema_details={
                    "schema_name": schema_name,
                    "base_version": evolution_plan.get("base_version"),
                    "compatibility": evolution_plan.get("compatibility")
                }
            )
        
        # Register new schema version
        evolved_schema = evolution_plan["evolved_schema"]
        next_version = evolution_plan.get("next_version")
        
        # Register the evolved schema
        schema_id = self._registry.register_schema(
            schema_name=schema_name,
            schema=evolved_schema,
            version=next_version,
            description=f"Evolution from {evolution_plan.get('base_version')}"
        )
        
        # Track evolution execution
        evolution_tracking_id = self.track_evolution_execution(
            schema_name=schema_name,
            from_version=evolution_plan.get("base_version"),
            to_version=next_version,
            changes=evolution_plan.get("changes", {}),
            success=True,
            execution_details={
                "forced": force,
                "compatibility": evolution_plan.get("compatibility")
            }
        )
        
        # Get the new schema record
        new_schema_record = self._registry.get_schema(schema_name, next_version)
        
        # Return evolution result
        result = {
            "schema_name": schema_name,
            "schema_id": schema_id,
            "base_version": evolution_plan.get("base_version"),
            "new_version": next_version,
            "evolution_tracking_id": evolution_tracking_id,
            "success": True,
            "new_schema": new_schema_record
        }
        
        return result
    
    def generate_migration_script(self,
                                schema_name: str,
                                from_version: str,
                                to_version: str,
                                target_system: str) -> str:
        """
        Generates a migration script for schema evolution.
        
        Args:
            schema_name: Name of the schema
            from_version: Source schema version
            to_version: Target schema version
            target_system: System to generate migration script for (e.g., bigquery)
            
        Returns:
            Migration script for the target system
        """
        # Get source and target schemas
        source_schema_record = self._registry.get_schema(schema_name, from_version)
        target_schema_record = self._registry.get_schema(schema_name, to_version)
        
        if not source_schema_record or not target_schema_record:
            missing = []
            if not source_schema_record:
                missing.append(f"source version {from_version}")
            if not target_schema_record:
                missing.append(f"target version {to_version}")
                
            raise ValueError(f"Cannot generate migration script: missing {', '.join(missing)}")
        
        source_schema = source_schema_record.get("schema")
        target_schema = target_schema_record.get("schema")
        schema_format = source_schema_record.get("schema_format")
        
        # Compare schemas
        comparison = compare_schemas(source_schema, target_schema, schema_format)
        
        # Generate migration script based on target system
        if target_system.lower() == "bigquery":
            table_placeholder = f"<dataset>.{schema_name}"
            script_lines = [
                f"-- Migration script for schema: {schema_name}",
                f"-- From version: {source_schema_record.get('version')}",
                f"-- To version: {target_schema_record.get('version')}",
                f"-- Generated: {datetime.datetime.utcnow().isoformat()}",
                "",
                "-- Replace <dataset> with your actual dataset name",
                ""
            ]
            
            # Add new columns
            if comparison.get("added"):
                script_lines.append("-- Add new columns")
                for field_name in comparison.get("added", []):
                    # Find field definition in target schema
                    target_fields = target_schema_record.get("schema", {}).get("fields", [])
                    field_def = next((f for f in target_fields if f["name"] == field_name), None)
                    
                    if field_def:
                        field_type = field_def.get("type", "STRING")
                        script_lines.append(
                            f"ALTER TABLE `{table_placeholder}` ADD COLUMN IF NOT EXISTS "
                            f"`{field_name}` {field_type};"
                        )
                script_lines.append("")
            
            # Modify columns
            if comparison.get("modified"):
                script_lines.append("-- Modify columns")
                for field_name in comparison.get("modified", []):
                    # Find field definition in target schema
                    target_fields = target_schema_record.get("schema", {}).get("fields", [])
                    field_def = next((f for f in target_fields if f["name"] == field_name), None)
                    
                    if field_def:
                        field_type = field_def.get("type", "STRING")
                        script_lines.append(
                            f"ALTER TABLE `{table_placeholder}` ALTER COLUMN "
                            f"`{field_name}` SET DATA TYPE {field_type};"
                        )
                script_lines.append("")
            
            # Drop columns
            if comparison.get("removed"):
                script_lines.append("-- Drop columns (commented out for safety)")
                for field_name in comparison.get("removed", []):
                    script_lines.append(
                        f"-- ALTER TABLE `{table_placeholder}` DROP COLUMN IF EXISTS `{field_name}`;"
                    )
                script_lines.append("")
            
            return "\n".join(script_lines)
        else:
            raise ValueError(f"Unsupported target system: {target_system}")
    
    def analyze_compatibility(self, 
                            schema_name: str, 
                            version1: str, 
                            version2: str,
                            compatibility_type: str = None) -> dict:
        """
        Analyzes compatibility between schema versions.
        
        Args:
            schema_name: Name of the schema
            version1: First version to analyze
            version2: Second version to analyze
            compatibility_type: Type of compatibility to check
            
        Returns:
            Detailed compatibility analysis
        """
        # Get both schema versions
        schema1_record = self._registry.get_schema(schema_name, version1)
        schema2_record = self._registry.get_schema(schema_name, version2)
        
        if not schema1_record or not schema2_record:
            missing = []
            if not schema1_record:
                missing.append(f"version {version1}")
            if not schema2_record:
                missing.append(f"version {version2}")
                
            raise ValueError(f"Cannot analyze compatibility: missing {', '.join(missing)}")
        
        # Check compatibility in both directions
        forward_compatibility = self._registry.validate_schema_compatibility(
            schema_name=schema_name,
            new_schema=schema2_record.get("schema"),
            compatibility_type=compatibility_type or "FORWARD",
            base_version=version1
        )
        
        backward_compatibility = self._registry.validate_schema_compatibility(
            schema_name=schema_name,
            new_schema=schema1_record.get("schema"),
            compatibility_type=compatibility_type or "BACKWARD",
            base_version=version2
        )
        
        # Compare schemas for detailed analysis
        comparison = self._registry.compare_schemas(schema_name, version1, version2)
        
        # Create analysis result
        analysis = {
            "schema_name": schema_name,
            "version1": version1,
            "version2": version2,
            "forward_compatible": forward_compatibility.get("compatible", False),
            "backward_compatible": backward_compatibility.get("compatible", False),
            "full_compatible": (
                forward_compatibility.get("compatible", False) and
                backward_compatibility.get("compatible", False)
            ),
            "comparison": comparison,
            "forward_compatibility_details": forward_compatibility,
            "backward_compatibility_details": backward_compatibility
        }
        
        return analysis
    
    def track_evolution_execution(self,
                                schema_name: str,
                                from_version: str,
                                to_version: str,
                                changes: dict,
                                success: bool,
                                execution_details: dict = None) -> str:
        """
        Tracks the execution of a schema evolution.
        
        Args:
            schema_name: Name of the schema
            from_version: Source schema version
            to_version: Target schema version
            changes: Changes applied in the evolution
            success: Whether the evolution was successful
            execution_details: Additional execution details
            
        Returns:
            Evolution tracking record ID
        """
        # Generate ID for tracking record
        tracking_id = str(uuid.uuid4())
        
        # Create tracking record
        tracking_record = {
            "tracking_id": tracking_id,
            "schema_name": schema_name,
            "from_version": from_version,
            "to_version": to_version,
            "changes": changes,
            "success": success,
            "execution_time": datetime.datetime.utcnow().isoformat(),
            "execution_details": execution_details or {},
            "environment": get_config().get_environment()
        }
        
        # Store tracking record
        try:
            collection = get_config().get(
                "schema_registry.evolution_tracking_collection", 
                "schema_evolution_tracking"
            )
            
            # Store in Firestore
            firestore_client = FirestoreClient()
            firestore_client.set_document(
                collection=collection,
                document_id=tracking_id,
                data=tracking_record
            )
            
            return tracking_id
        except Exception as e:
            logger.error(f"Failed to track schema evolution execution: {str(e)}")
            # Return ID even if storage fails
            return tracking_id
    
    def _get_next_version(self, current_version: str, compatibility: dict) -> str:
        """
        Determines the next version based on compatibility assessment.
        
        Args:
            current_version: Current schema version
            compatibility: Compatibility assessment
            
        Returns:
            Next semantic version
        """
        if not compatibility.get("compatible", True):
            # Major version increment for incompatible changes
            return semver.bump_major(current_version)
        elif compatibility.get("details", {}).get("has_changes", False):
            # Minor version increment for compatible changes
            return semver.bump_minor(current_version)
        else:
            # Patch version for non-structural changes
            return semver.bump_patch(current_version)