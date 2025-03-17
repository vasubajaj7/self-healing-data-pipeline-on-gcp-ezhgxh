"""
Initializes the metadata package for the self-healing data pipeline, exposing key classes and functions for tracking data lineage, pipeline metadata, and schema information. This package provides comprehensive metadata management capabilities to support data ingestion, quality validation, and self-healing processes.
"""

from .lineage_tracker import LineageTracker, LineageQuery, create_lineage_record  # version: N/A
from .metadata_tracker import MetadataTracker, MetadataQuery, create_metadata_record  # version: N/A
from .schema_registry import SchemaRegistry, SchemaEvolution, create_schema_record  # version: N/A

__all__ = [
    "LineageTracker",
    "LineageQuery",
    "create_lineage_record",
    "MetadataTracker",
    "MetadataQuery",
    "create_metadata_record",
    "SchemaRegistry",
    "SchemaEvolution",
    "create_schema_record"
]