"""
Storage utilities module for the self-healing data pipeline.

This module provides a unified interface for interacting with various Google Cloud
storage services (BigQuery, Cloud Storage, Firestore) with enhanced error handling,
retry capabilities, and monitoring integration.
"""

# BigQuery client and utilities
from .bigquery_client import (
    BigQueryClient,
    BigQueryJobConfig,
    format_query_parameters,
    get_table_schema,
    format_table_reference,
)

# Google Cloud Storage client and utilities
from .gcs_client import (
    GCSClient,
    map_gcs_exception_to_pipeline_error,
    get_content_type,
)

# Firestore client and utilities
from .firestore_client import (
    FirestoreClient,
    map_firestore_exception_to_pipeline_error,
)

# Define exports for wildcard imports
__all__ = [
    "BigQueryClient",
    "BigQueryJobConfig",
    "format_query_parameters",
    "get_table_schema", 
    "format_table_reference",
    "GCSClient",
    "map_gcs_exception_to_pipeline_error",
    "get_content_type",
    "FirestoreClient",
    "map_firestore_exception_to_pipeline_error",
]