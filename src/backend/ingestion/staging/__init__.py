"""
Initialization file for the staging module of the data ingestion pipeline.

Exports key classes and functions for managing the staging area, data normalization,
and storage operations. This module is essential for the self-healing data pipeline's
ability to stage, transform, and prepare data for quality validation and processing.
"""

# Import the StagingManager class for data staging operations
from .staging_manager import StagingManager

# Import the DataNormalizer class for data normalization
from .data_normalizer import DataNormalizer

# Import the StorageService abstract base class
from .storage_service import StorageService

# Import the GCS implementation of StorageService
from .storage_service import GCSStorageService

# Import the local file system implementation of StorageService
from .storage_service import LocalFileStorageService

# Import factory function for creating storage service instances
from .storage_service import get_storage_service

# Import utility function for generating unique staging IDs
from .staging_manager import generate_staging_id

# Import standalone function for DataFrame normalization
from .data_normalizer import normalize_dataframe

# Import standalone function for JSON normalization
from .data_normalizer import normalize_json

# Import standalone function for CSV normalization
from .data_normalizer import normalize_csv_data

__all__ = [
    "StagingManager",
    "DataNormalizer",
    "StorageService",
    "GCSStorageService",
    "LocalFileStorageService",
    "get_storage_service",
    "generate_staging_id",
    "normalize_dataframe",
    "normalize_json",
    "normalize_csv_data"
]