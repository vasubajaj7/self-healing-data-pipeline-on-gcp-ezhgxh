"""
File extractor component for processing various file formats from storage sources.

This module provides functionality to extract and process files from Google Cloud Storage
and local filesystems with support for CSV, JSON, Parquet, Avro, and text formats.
It includes schema inference, transformation capabilities, and self-healing features.
"""

import os
import io
import json
import csv
import pandas as pd
import pyarrow  # version 12.0.0+
import fastavro  # version 1.7.0+
from typing import Union, Dict, List, Optional, Tuple, Any, BinaryIO, TextIO

from ...constants import FileFormat, DEFAULT_MAX_RETRY_ATTEMPTS
from ...utils.logging.logger import get_logger
from ..errors.error_handler import with_error_handling, retry_with_backoff
from ...utils.storage.gcs_client import GCSClient

# Set up logger
logger = get_logger(__name__)

# Default values
DEFAULT_ENCODING = "utf-8"
DEFAULT_CSV_DELIMITER = ","
DEFAULT_CSV_QUOTECHAR = "\""
DEFAULT_SAMPLE_SIZE = 1000


def detect_file_format(file_path: str, file_content: bytes = None) -> FileFormat:
    """Detects the file format based on file extension or content inspection.
    
    Args:
        file_path: Path to the file
        file_content: Optional file content for inspection
        
    Returns:
        Detected file format as FileFormat enum
    """
    # First try to determine from file extension
    if file_path:
        ext = os.path.splitext(file_path.lower())[1]
        if ext == '.csv':
            return FileFormat.CSV
        elif ext == '.json':
            return FileFormat.JSON
        elif ext == '.avro':
            return FileFormat.AVRO
        elif ext == '.parquet':
            return FileFormat.PARQUET
        elif ext == '.orc':
            return FileFormat.ORC
        elif ext == '.xml':
            return FileFormat.XML
    
    # If extension doesn't give a clear answer, inspect content
    if file_content:
        # Check for JSON format (starts with { or [)
        if file_content.startswith(b'{') or file_content.startswith(b'['):
            return FileFormat.JSON
        
        # Check for Parquet magic bytes
        if file_content.startswith(b'PAR1'):
            return FileFormat.PARQUET
        
        # Check for Avro magic bytes (specific Avro marker)
        if file_content.startswith(b'Obj\x01'):
            return FileFormat.AVRO
        
        # Check for CSV format (look for commas and newlines pattern)
        if b',' in file_content and b'\n' in file_content:
            lines = file_content.split(b'\n', 5)
            if len(lines) > 1:
                # Check if number of commas is consistent across first few lines
                comma_counts = [line.count(b',') for line in lines if line]
                if len(set(comma_counts)) <= 1:  # All non-empty lines have same number of commas
                    return FileFormat.CSV
    
    # Default to TEXT if format cannot be determined
    logger.warning(f"Could not determine file format for {file_path}, defaulting to TEXT")
    return FileFormat.TEXT


def infer_schema(data_sample: pd.DataFrame, file_format: FileFormat) -> dict:
    """Infers the schema from file data based on format.
    
    Args:
        data_sample: Sample data as pandas DataFrame
        file_format: Format of the file
        
    Returns:
        Dictionary containing inferred schema information
    """
    schema = {
        'fields': [],
        'metadata': {
            'inferred_from': file_format.value,
            'sample_size': len(data_sample),
            'inference_time': pd.Timestamp.now().isoformat()
        }
    }
    
    # Infer field information from DataFrame
    for column in data_sample.columns:
        dtype = data_sample[column].dtype
        null_count = data_sample[column].isna().sum()
        
        field_info = {
            'name': column,
            'data_type': str(dtype),
            'nullable': null_count > 0,
            'null_count': int(null_count),
            'unique_count': int(data_sample[column].nunique())
        }
        
        # Add min/max for numeric columns
        if pd.api.types.is_numeric_dtype(dtype):
            if not data_sample[column].isna().all():
                field_info['min_value'] = float(data_sample[column].min())
                field_info['max_value'] = float(data_sample[column].max())
        
        # Add additional metadata for datetime columns
        if pd.api.types.is_datetime64_dtype(dtype):
            if not data_sample[column].isna().all():
                field_info['min_value'] = data_sample[column].min().isoformat()
                field_info['max_value'] = data_sample[column].max().isoformat()
        
        schema['fields'].append(field_info)
    
    return schema


class FileExtractor:
    """Extractor for processing files of various formats with schema inference and data transformation."""
    
    def __init__(self, source_id: str, source_name: str, extraction_config: dict):
        """Initialize the file extractor with source information and extraction configuration.
        
        Args:
            source_id: Unique identifier for the data source
            source_name: Human-readable name for the data source
            extraction_config: Configuration parameters for extraction
        """
        self.source_id = source_id
        self.source_name = source_name
        self.extraction_config = extraction_config or {}
        
        # Initialize GCS client if needed
        self._gcs_client = None
        if self.extraction_config.get('use_gcs', False):
            self._gcs_client = GCSClient()
        
        # Register format-specific handler methods
        self.format_handlers = self.register_format_handlers()
        
        logger.info(f"Initialized FileExtractor for source: {source_name} (ID: {source_id})")
    
    @with_error_handling(context={'component': 'FileExtractor', 'operation': 'extract_file'}, raise_exception=True)
    def extract_file(self, file_path: str, extraction_params: dict = None) -> Tuple[pd.DataFrame, dict]:
        """Extract data from a file with format detection and parsing.
        
        Args:
            file_path: Path to the file to extract data from
            extraction_params: Additional parameters for the extraction process
            
        Returns:
            Tuple of (DataFrame with extracted data, metadata dictionary)
        """
        extraction_params = extraction_params or {}
        
        # Validate parameters
        if not self.validate_extraction_params(extraction_params):
            raise ValueError(f"Invalid extraction parameters: {extraction_params}")
        
        logger.info(f"Extracting file: {file_path}")
        
        # Determine if file is in GCS or local filesystem
        if file_path.startswith('gs://'):
            bucket_name, blob_name = self._parse_gcs_path(file_path)
            return self.extract_gcs_file(bucket_name, blob_name, extraction_params)
        else:
            return self.extract_local_file(file_path, extraction_params)
    
    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def extract_gcs_file(self, bucket_name: str, blob_name: str, extraction_params: dict = None) -> Tuple[pd.DataFrame, dict]:
        """Extract data from a file in Google Cloud Storage.
        
        Args:
            bucket_name: GCS bucket name
            blob_name: GCS blob name/path
            extraction_params: Additional parameters for the extraction process
            
        Returns:
            Tuple of (DataFrame with extracted data, metadata dictionary)
        """
        extraction_params = extraction_params or {}
        
        # Ensure GCS client is initialized
        if not self._gcs_client:
            self._gcs_client = GCSClient()
        
        logger.info(f"Extracting file from GCS: gs://{bucket_name}/{blob_name}")
        
        # Download blob content
        content = self._gcs_client.download_blob_as_bytes(bucket_name, blob_name)
        if not content:
            raise ValueError(f"Empty or non-existent file: gs://{bucket_name}/{blob_name}")
        
        # Detect format if not specified
        file_format = extraction_params.get('file_format')
        if not file_format:
            detected_format = detect_file_format(blob_name, content)
            file_format = detected_format
            logger.info(f"Detected file format: {detected_format.value}")
        elif isinstance(file_format, str):
            file_format = FileFormat(file_format.upper())
        
        # Get the appropriate parser function
        if file_format not in self.format_handlers:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        parser_func = self.format_handlers[file_format]
        
        # Parse the file content
        data = parser_func(content, extraction_params)
        
        # Apply transformations if specified
        if 'transformations' in extraction_params:
            data = self.transform_data(data, extraction_params['transformations'])
        
        # Generate metadata
        file_path = f"gs://{bucket_name}/{blob_name}"
        additional_metadata = {
            'storage_type': 'GCS',
            'bucket': bucket_name,
            'blob': blob_name,
            'size': len(content)
        }
        
        metadata = self.get_file_metadata(
            file_path,
            file_format,
            data,
            extraction_params,
            additional_metadata
        )
        
        return data, metadata
    
    @retry_with_backoff(max_retries=DEFAULT_MAX_RETRY_ATTEMPTS)
    def extract_local_file(self, file_path: str, extraction_params: dict = None) -> Tuple[pd.DataFrame, dict]:
        """Extract data from a local file.
        
        Args:
            file_path: Path to the local file
            extraction_params: Additional parameters for the extraction process
            
        Returns:
            Tuple of (DataFrame with extracted data, metadata dictionary)
        """
        extraction_params = extraction_params or {}
        
        logger.info(f"Extracting local file: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Read file content
        with open(file_path, 'rb') as f:
            content = f.read()
        
        if not content:
            raise ValueError(f"Empty file: {file_path}")
        
        # Detect format if not specified
        file_format = extraction_params.get('file_format')
        if not file_format:
            detected_format = detect_file_format(file_path, content)
            file_format = detected_format
            logger.info(f"Detected file format: {detected_format.value}")
        elif isinstance(file_format, str):
            file_format = FileFormat(file_format.upper())
        
        # Get the appropriate parser function
        if file_format not in self.format_handlers:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        parser_func = self.format_handlers[file_format]
        
        # Parse the file content
        data = parser_func(content, extraction_params)
        
        # Apply transformations if specified
        if 'transformations' in extraction_params:
            data = self.transform_data(data, extraction_params['transformations'])
        
        # Generate metadata
        additional_metadata = {
            'storage_type': 'local',
            'file_size': os.path.getsize(file_path),
            'file_modified': pd.Timestamp(os.path.getmtime(file_path), unit='s').isoformat()
        }
        
        metadata = self.get_file_metadata(
            file_path,
            file_format,
            data,
            extraction_params,
            additional_metadata
        )
        
        return data, metadata
    
    def parse_csv(self, content: Union[str, bytes, io.IOBase], params: dict) -> pd.DataFrame:
        """Parse CSV file content into structured data.
        
        Args:
            content: CSV content as string, bytes, or file-like object
            params: CSV-specific parsing parameters
            
        Returns:
            Pandas DataFrame with parsed data
        """
        # Extract CSV-specific parameters with defaults
        delimiter = params.get('delimiter', DEFAULT_CSV_DELIMITER)
        quotechar = params.get('quotechar', DEFAULT_CSV_QUOTECHAR)
        encoding = params.get('encoding', DEFAULT_ENCODING)
        header = params.get('header', 'infer')
        skip_rows = params.get('skip_rows', None)
        usecols = params.get('usecols', None)
        
        try:
            # Try primary encoding
            return pd.read_csv(
                io.BytesIO(content) if isinstance(content, bytes) else content,
                delimiter=delimiter,
                quotechar=quotechar,
                encoding=encoding,
                header=header,
                skiprows=skip_rows,
                usecols=usecols
            )
        except UnicodeDecodeError:
            # Fall back to alternative encodings
            logger.warning(f"Failed to decode with {encoding}, trying alternative encodings")
            for alt_encoding in ['utf-8-sig', 'latin1', 'cp1252']:
                if alt_encoding != encoding:
                    try:
                        return pd.read_csv(
                            io.BytesIO(content) if isinstance(content, bytes) else content,
                            delimiter=delimiter,
                            quotechar=quotechar,
                            encoding=alt_encoding,
                            header=header,
                            skiprows=skip_rows,
                            usecols=usecols
                        )
                    except UnicodeDecodeError:
                        continue
            
            # If all encodings fail, raise the original error
            raise
    
    def parse_json(self, content: Union[str, bytes, io.IOBase], params: dict) -> pd.DataFrame:
        """Parse JSON file content into structured data.
        
        Args:
            content: JSON content as string, bytes, or file-like object
            params: JSON-specific parsing parameters
            
        Returns:
            Pandas DataFrame with parsed data
        """
        # Extract JSON-specific parameters
        encoding = params.get('encoding', DEFAULT_ENCODING)
        orient = params.get('orient', None)
        lines = params.get('lines', False)
        record_path = params.get('record_path', None)
        
        # Convert bytes to string if needed
        if isinstance(content, bytes):
            content = content.decode(encoding)
        
        try:
            # Handle different JSON structures
            if lines:
                # JSON Lines format (one JSON object per line)
                return pd.read_json(
                    io.StringIO(content) if isinstance(content, str) else content,
                    lines=True,
                    orient=orient
                )
            elif record_path:
                # Nested JSON with records at a specific path
                if isinstance(content, str):
                    data = json.loads(content)
                elif hasattr(content, 'read'):
                    data = json.load(content)
                else:
                    data = content
                
                # Navigate to the specified record path
                path_parts = record_path.split('.')
                for part in path_parts:
                    if part in data:
                        data = data[part]
                    else:
                        raise ValueError(f"Record path '{record_path}' not found in JSON")
                
                return pd.json_normalize(data)
            else:
                # Standard JSON
                return pd.read_json(
                    io.StringIO(content) if isinstance(content, str) else content,
                    orient=orient
                )
        except Exception as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            raise
    
    def parse_parquet(self, content: Union[str, bytes, io.IOBase], params: dict) -> pd.DataFrame:
        """Parse Parquet file content into structured data.
        
        Args:
            content: Parquet content as path, bytes, or file-like object
            params: Parquet-specific parsing parameters
            
        Returns:
            Pandas DataFrame with parsed data
        """
        # Extract Parquet-specific parameters
        columns = params.get('columns', None)
        filters = params.get('filters', None)
        
        try:
            # If content is a file path, use it directly
            if isinstance(content, str) and os.path.exists(content):
                return pd.read_parquet(content, columns=columns, filters=filters)
            
            # If content is bytes or file-like, use BytesIO
            buffer = io.BytesIO(content) if isinstance(content, bytes) else content
            return pd.read_parquet(buffer, columns=columns, filters=filters)
        except Exception as e:
            logger.error(f"Error parsing Parquet file: {str(e)}")
            raise
    
    def parse_avro(self, content: Union[str, bytes, io.IOBase], params: dict) -> pd.DataFrame:
        """Parse Avro file content into structured data.
        
        Args:
            content: Avro content as path, bytes, or file-like object
            params: Avro-specific parsing parameters
            
        Returns:
            Pandas DataFrame with parsed data
        """
        try:
            # If content is a file path, open it
            if isinstance(content, str) and os.path.exists(content):
                with open(content, 'rb') as f:
                    avro_content = f.read()
            elif isinstance(content, bytes):
                avro_content = content
            else:
                # If it's a file-like object, read it
                avro_content = content.read()
            
            # Parse Avro content
            buffer = io.BytesIO(avro_content)
            records = list(fastavro.reader(buffer))
            
            # Convert to DataFrame
            return pd.DataFrame.from_records(records)
        except Exception as e:
            logger.error(f"Error parsing Avro file: {str(e)}")
            raise
    
    def parse_text(self, content: Union[str, bytes, io.IOBase], params: dict) -> pd.DataFrame:
        """Parse plain text file content into structured data.
        
        Args:
            content: Text content as string, bytes, or file-like object
            params: Text-specific parsing parameters
            
        Returns:
            Pandas DataFrame with parsed data
        """
        # Extract text-specific parameters
        encoding = params.get('encoding', DEFAULT_ENCODING)
        line_delimiter = params.get('line_delimiter', '\n')
        
        try:
            # Convert bytes to string if needed
            if isinstance(content, bytes):
                text_content = content.decode(encoding)
            elif isinstance(content, str):
                text_content = content
            else:
                # If it's a file-like object, read it
                text_content = content.read()
                if isinstance(text_content, bytes):
                    text_content = text_content.decode(encoding)
            
            # Split into lines
            lines = text_content.split(line_delimiter)
            
            # Create DataFrame with one column
            return pd.DataFrame(lines, columns=['text'])
        except Exception as e:
            logger.error(f"Error parsing text file: {str(e)}")
            raise
    
    def transform_data(self, data: pd.DataFrame, transformations: dict) -> pd.DataFrame:
        """Apply transformations to extracted data.
        
        Args:
            data: DataFrame to transform
            transformations: Dictionary of transformation specifications
            
        Returns:
            Transformed DataFrame
        """
        # Make a copy to avoid modifying the original
        result = data.copy()
        
        # Apply column renames if specified
        if 'rename_columns' in transformations:
            result = result.rename(columns=transformations['rename_columns'])
        
        # Apply type conversions if specified
        if 'convert_types' in transformations:
            for col, dtype in transformations['convert_types'].items():
                if col in result.columns:
                    try:
                        result[col] = result[col].astype(dtype)
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col} to {dtype}: {str(e)}")
        
        # Apply filters if specified
        if 'filters' in transformations:
            for filter_spec in transformations['filters']:
                column = filter_spec.get('column')
                operator = filter_spec.get('operator')
                value = filter_spec.get('value')
                
                if column and operator and column in result.columns:
                    try:
                        if operator == '==':
                            result = result[result[column] == value]
                        elif operator == '!=':
                            result = result[result[column] != value]
                        elif operator == '>':
                            result = result[result[column] > value]
                        elif operator == '>=':
                            result = result[result[column] >= value]
                        elif operator == '<':
                            result = result[result[column] < value]
                        elif operator == '<=':
                            result = result[result[column] <= value]
                        elif operator == 'in':
                            result = result[result[column].isin(value)]
                        elif operator == 'not in':
                            result = result[~result[column].isin(value)]
                    except Exception as e:
                        logger.warning(f"Failed to apply filter on {column}: {str(e)}")
        
        # Apply custom transformations function if specified
        if 'custom_function' in transformations and callable(transformations['custom_function']):
            try:
                result = transformations['custom_function'](result)
            except Exception as e:
                logger.error(f"Failed to apply custom transformation: {str(e)}")
                raise
        
        return result
    
    def get_file_metadata(
        self,
        file_path: str,
        file_format: FileFormat,
        data: pd.DataFrame,
        extraction_params: dict,
        additional_metadata: dict = None
    ) -> dict:
        """Generate metadata about the extracted file.
        
        Args:
            file_path: Path to the file
            file_format: Format of the file
            data: Extracted data
            extraction_params: Parameters used for extraction
            additional_metadata: Additional metadata to include
            
        Returns:
            Dictionary containing file metadata
        """
        # Create base metadata structure
        metadata = {
            'source_id': self.source_id,
            'source_name': self.source_name,
            'file_path': file_path,
            'file_format': file_format.value,
            'extraction_time': pd.Timestamp.now().isoformat(),
            'row_count': len(data),
            'column_count': len(data.columns),
            'columns': list(data.columns),
            'extraction_params': {
                k: v for k, v in extraction_params.items() 
                if k not in ['transformations']  # Exclude complex objects
            }
        }
        
        # Add schema information if data is not empty
        if not data.empty:
            # Infer schema from a sample of the data if it's large
            sample_size = min(len(data), DEFAULT_SAMPLE_SIZE)
            data_sample = data.sample(sample_size) if sample_size < len(data) else data
            metadata['schema'] = infer_schema(data_sample, file_format)
        
        # Add additional metadata if provided
        if additional_metadata:
            metadata.update(additional_metadata)
        
        return metadata
    
    def validate_extraction_params(self, extraction_params: dict) -> bool:
        """Validate extraction parameters for file extraction.
        
        Args:
            extraction_params: Parameters to validate
            
        Returns:
            True if parameters are valid, False otherwise
        """
        # Check if extraction_params is a dictionary
        if not isinstance(extraction_params, dict):
            logger.error("Extraction parameters must be a dictionary")
            return False
        
        # If file_format is specified, it should be a valid FileFormat enum or string
        if 'file_format' in extraction_params:
            file_format = extraction_params['file_format']
            if isinstance(file_format, str):
                try:
                    FileFormat(file_format.upper())  # Validate it's a valid enum value
                except ValueError:
                    logger.error(f"Invalid file format: {file_format}")
                    return False
            elif not isinstance(file_format, FileFormat):
                logger.error(f"file_format must be a string or FileFormat enum, got {type(file_format)}")
                return False
        
        # Validate transformations if specified
        if 'transformations' in extraction_params:
            transformations = extraction_params['transformations']
            if not isinstance(transformations, dict):
                logger.error("Transformations must be a dictionary")
                return False
            
            # Validate rename_columns is a dictionary mapping old names to new names
            if 'rename_columns' in transformations and not isinstance(transformations['rename_columns'], dict):
                logger.error("rename_columns must be a dictionary")
                return False
            
            # Validate convert_types is a dictionary mapping column names to data types
            if 'convert_types' in transformations and not isinstance(transformations['convert_types'], dict):
                logger.error("convert_types must be a dictionary")
                return False
            
            # Validate filters is a list of filter specifications
            if 'filters' in transformations and not isinstance(transformations['filters'], list):
                logger.error("filters must be a list")
                return False
            
            # Validate custom_function is callable if specified
            if 'custom_function' in transformations and not callable(transformations['custom_function']):
                logger.error("custom_function must be callable")
                return False
        
        return True
    
    def register_format_handlers(self) -> Dict[FileFormat, callable]:
        """Register handler functions for different file formats.
        
        Returns:
            Dictionary mapping formats to handler functions
        """
        return {
            FileFormat.CSV: self.parse_csv,
            FileFormat.JSON: self.parse_json,
            FileFormat.PARQUET: self.parse_parquet,
            FileFormat.AVRO: self.parse_avro,
            FileFormat.TEXT: self.parse_text
        }
    
    def _parse_gcs_path(self, gcs_path: str) -> Tuple[str, str]:
        """Parse a GCS path into bucket and blob names.
        
        Args:
            gcs_path: GCS path in the format 'gs://bucket-name/path/to/file'
            
        Returns:
            Tuple of (bucket_name, blob_name)
        """
        if not gcs_path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path: {gcs_path}")
        
        # Remove the 'gs://' prefix
        path = gcs_path[5:]
        
        # Split into bucket and blob
        parts = path.split('/', 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ''
        
        return bucket_name, blob_name