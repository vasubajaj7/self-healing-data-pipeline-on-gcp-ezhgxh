"""
Initialization module for the database schema package that exports schema utilities for BigQuery and Firestore.

This module provides a unified interface for schema management across different database systems used in the
self-healing data pipeline. It exports classes and functions for defining, validating, and managing schemas
for BigQuery tables and Firestore collections.

Key components:
- BigQuery schema utilities (prefixed with 'bq_')
- Firestore schema utilities (prefixed with 'fs_')
- Schema management classes for both BigQuery and Firestore
- Predefined collection schemas for common data structures

This module simplifies the creation, validation, and evolution of schemas across the pipeline,
supporting the Schema Validation Framework and Data Model Integration requirements.
"""

# Import utilities for BigQuery schema management
from .bigquery_schema import (
    SchemaField as BigQuerySchemaField,
    SchemaManager as BigQuerySchemaManager,
    get_schema_field as bq_get_schema_field,
    schema_to_json as bq_schema_to_json,
    json_to_schema as bq_json_to_schema,
    merge_schemas as bq_merge_schemas,
    validate_schema_compatibility as bq_validate_schema_compatibility,
    get_field_by_name as bq_get_field_by_name,
    create_schema_from_dict as bq_create_schema_from_dict,
    infer_schema_from_data as bq_infer_schema_from_data,
    compare_schemas as bq_compare_schemas,
    FIELD_TYPE_MAPPING as BQ_FIELD_TYPE_MAPPING
)

# Import utilities for Firestore schema management
from .firestore_schema import (
    FirestoreSchema,
    FirestoreSchemaManager,
    CollectionSchemas,
    get_schema_field as fs_get_schema_field,
    schema_to_json as fs_schema_to_json,
    json_to_schema as fs_json_to_schema,
    validate_document as fs_validate_document,
    create_schema_from_dict as fs_create_schema_from_dict,
    infer_schema_from_document as fs_infer_schema_from_document,
    merge_schemas as fs_merge_schemas,
    validate_schema_compatibility as fs_validate_schema_compatibility,
    get_field_by_name as fs_get_field_by_name,
    FIELD_TYPE_MAPPING as FS_FIELD_TYPE_MAPPING
)

# Import logging utilities
from ...utils.logging.logger import get_logger

# Configure module logger
logger = get_logger(__name__)

# Define which symbols are exported when using 'from backend.db.schema import *'
__all__ = [
    # BigQuery schema classes
    'BigQuerySchemaField',
    'BigQuerySchemaManager',
    
    # Firestore schema classes
    'FirestoreSchema',
    'FirestoreSchemaManager',
    'CollectionSchemas',
    
    # BigQuery schema functions
    'bq_get_schema_field',
    'bq_schema_to_json',
    'bq_json_to_schema',
    'bq_merge_schemas',
    'bq_validate_schema_compatibility',
    'bq_get_field_by_name',
    'bq_create_schema_from_dict',
    'bq_infer_schema_from_data',
    'bq_compare_schemas',
    
    # Firestore schema functions
    'fs_get_schema_field',
    'fs_schema_to_json',
    'fs_json_to_schema',
    'fs_validate_document',
    'fs_create_schema_from_dict',
    'fs_infer_schema_from_document',
    'fs_merge_schemas',
    'fs_validate_schema_compatibility',
    'fs_get_field_by_name'
]