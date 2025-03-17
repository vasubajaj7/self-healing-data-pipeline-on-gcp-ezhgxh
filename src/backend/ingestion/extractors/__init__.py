"""
Initialization file for the extractors module that exposes the various data
extraction components for the self-healing data pipeline. This module provides
a collection of specialized extractors for different data sources and
extraction patterns, including file-based extraction, API integration, batch
processing, and incremental data loading.
"""

from .api_extractor import ApiExtractor  # Import API extraction functionality
from .batch_extractor import BatchExtractor  # Import batch processing functionality
from .file_extractor import FileExtractor, detect_file_format, infer_schema  # Import file extraction functionality
from .incremental_extractor import IncrementalExtractor  # Import incremental extraction functionality

__all__ = ["ApiExtractor", "BatchExtractor", "FileExtractor", "IncrementalExtractor", "detect_file_format", "infer_schema"]