"""
Data normalization module for the ingestion pipeline.

This module provides functionality to normalize data from various source formats
into a standardized structure suitable for further processing. It supports multiple 
file formats and implements schema-based normalization to ensure data consistency.
"""

import pandas as pd
import numpy as np
import json
import io
from typing import Dict, List, Any, Union, Optional
import pyarrow  # version 12.0.x
import fastavro  # version 1.7.x

from ...constants import FileFormat
from ...config import get_config
from ...utils.logging.logger import get_logger
from ...utils.schema.schema_utils import extract_schema_from_data, is_schema_compatible
from ..errors.error_handler import with_error_handling
from ...utils.errors.error_types import DataFormatError, SchemaValidationError

# Set up logger
logger = get_logger(__name__)

@with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_dataframe'}, raise_exception=True)
def normalize_dataframe(df: pd.DataFrame, target_schema: Dict, options: Dict) -> pd.DataFrame:
    """
    Normalizes a pandas DataFrame to match a target schema.
    
    Args:
        df: The source DataFrame to normalize
        target_schema: The schema to normalize against
        options: Additional options for normalization
        
    Returns:
        Normalized DataFrame conforming to the target schema
    
    Raises:
        DataFormatError: If the data cannot be normalized to match the schema
        SchemaValidationError: If the schema validation fails
    """
    if df is None or df.empty:
        logger.warning("Empty DataFrame provided for normalization")
        if 'allow_empty' in options and options['allow_empty']:
            # Create empty DataFrame with target schema columns
            return pd.DataFrame(columns=[col['name'] for col in target_schema.get('columns', [])])
        else:
            raise DataFormatError("Empty DataFrame provided for normalization", data_source="dataframe")
    
    if target_schema is None:
        raise ValueError("Target schema cannot be None")
    
    logger.debug(f"Normalizing DataFrame with {len(df)} rows to match target schema")
    
    # Extract column information from schema
    schema_columns = {col['name']: col for col in target_schema.get('columns', [])}
    
    # Get the list of columns in the schema
    expected_columns = list(schema_columns.keys())
    
    # Check current columns in the DataFrame
    current_columns = df.columns.tolist()
    
    # Add missing columns with default values
    for col_name, col_def in schema_columns.items():
        if col_name not in current_columns:
            logger.debug(f"Adding missing column '{col_name}' with default value")
            default_value = col_def.get('default')
            df[col_name] = default_value
    
    # Remove extra columns if specified in options
    if options.get('remove_extra_columns', True):
        extra_columns = [col for col in current_columns if col not in expected_columns]
        if extra_columns:
            logger.debug(f"Removing extra columns: {extra_columns}")
            df = df.drop(columns=extra_columns)
    
    # Reorder columns to match schema
    df = df[expected_columns]
    
    # Convert data types to match schema
    for col_name, col_def in schema_columns.items():
        col_type = col_def.get('type', 'string')
        try:
            # Convert the column to the appropriate type
            if col_type == 'integer':
                df[col_name] = df[col_name].fillna(col_def.get('default', 0)).astype('int64')
            elif col_type == 'number' or col_type == 'float':
                df[col_name] = df[col_name].fillna(col_def.get('default', 0.0)).astype('float64')
            elif col_type == 'boolean':
                df[col_name] = df[col_name].fillna(col_def.get('default', False)).astype('bool')
            elif col_type == 'datetime':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                if 'format' in col_def:
                    df[col_name] = df[col_name].dt.strftime(col_def['format'])
            elif col_type == 'string':
                df[col_name] = df[col_name].fillna(col_def.get('default', '')).astype('str')
                if df[col_name].str.lower().isin(['nan', 'none', 'null']).any():
                    df.loc[df[col_name].str.lower().isin(['nan', 'none', 'null']), col_name] = col_def.get('default', '')
            # Add other type conversions as needed
        except Exception as e:
            logger.warning(f"Error converting column '{col_name}' to type '{col_type}': {str(e)}")
            # Apply default value for failed conversions
            df[col_name] = col_def.get('default', None)
    
    # Handle null values based on schema nullability
    for col_name, col_def in schema_columns.items():
        nullable = col_def.get('nullable', True)
        if not nullable:
            default_value = col_def.get('default')
            if default_value is not None:
                df[col_name] = df[col_name].fillna(default_value)
            elif df[col_name].isna().any():
                non_null_count = df[col_name].count()
                total_count = len(df)
                null_pct = (total_count - non_null_count) / total_count * 100
                error_msg = f"Column '{col_name}' contains {null_pct:.2f}% null values but is not nullable"
                
                # Check options for how to handle this error
                if options.get('strict_nullability', True):
                    raise SchemaValidationError(error_msg, {
                        'column': col_name,
                        'null_count': total_count - non_null_count,
                        'total_count': total_count
                    })
                else:
                    logger.warning(error_msg)
    
    # Apply any format-specific transformations
    if 'transformations' in options:
        for transform in options['transformations']:
            transform_type = transform.get('type')
            if transform_type == 'replace':
                col = transform.get('column')
                old_value = transform.get('old_value')
                new_value = transform.get('new_value')
                if col and old_value is not None and new_value is not None:
                    df[col] = df[col].replace(old_value, new_value)
            elif transform_type == 'expression':
                col = transform.get('column')
                expr = transform.get('expression')
                if col and expr:
                    df[col] = df.eval(expr)
    
    logger.debug(f"DataFrame normalized successfully with final shape {df.shape}")
    return df

@with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_json'}, raise_exception=True)
def normalize_json(json_data: Union[Dict, List, str], target_schema: Dict, options: Dict) -> Union[Dict, List]:
    """
    Normalizes JSON data to match a target schema.
    
    Args:
        json_data: The source JSON data to normalize (can be dict, list, or JSON string)
        target_schema: The schema to normalize against
        options: Additional options for normalization
        
    Returns:
        Normalized JSON data conforming to the target schema
        
    Raises:
        DataFormatError: If the data cannot be normalized to match the schema
        SchemaValidationError: If the schema validation fails
    """
    # Parse JSON string if provided as string
    if isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            raise DataFormatError(f"Invalid JSON format: {str(e)}", data_source="json_string")
    
    if not json_data:
        logger.warning("Empty JSON data provided for normalization")
        if 'allow_empty' in options and options['allow_empty']:
            # Return empty structure based on schema type
            return {} if target_schema.get('type') == 'object' else []
        else:
            raise DataFormatError("Empty JSON data provided for normalization", data_source="json")
    
    if target_schema is None:
        raise ValueError("Target schema cannot be None")
    
    logger.debug("Normalizing JSON data to match target schema")
    
    # Handle both single objects and arrays of objects
    if isinstance(json_data, list):
        return [_normalize_json_object(item, target_schema, options) for item in json_data]
    else:
        return _normalize_json_object(json_data, target_schema, options)

def _normalize_json_object(obj: Dict, target_schema: Dict, options: Dict) -> Dict:
    """
    Normalizes a single JSON object to match a target schema.
    
    Args:
        obj: The JSON object to normalize
        target_schema: The schema to normalize against
        options: Additional options for normalization
        
    Returns:
        Normalized JSON object
    """
    if not isinstance(obj, dict):
        raise DataFormatError(f"Expected JSON object, got {type(obj).__name__}", data_source="json")
    
    # Extract properties from schema
    schema_properties = target_schema.get('properties', {})
    expected_fields = list(schema_properties.keys())
    
    # Create normalized object
    normalized = {}
    
    # Add all expected fields with appropriate values
    for field, field_schema in schema_properties.items():
        field_type = field_schema.get('type', 'string')
        default_value = field_schema.get('default')
        
        # Check if field exists in source data
        if field in obj:
            value = obj[field]
            # Convert to appropriate type
            try:
                if field_type == 'integer':
                    value = int(value) if value is not None else default_value
                elif field_type == 'number':
                    value = float(value) if value is not None else default_value
                elif field_type == 'boolean':
                    if isinstance(value, str):
                        value = value.lower() in ('true', '1', 'yes', 'y')
                    else:
                        value = bool(value) if value is not None else default_value
                elif field_type == 'string':
                    value = str(value) if value is not None else default_value
                elif field_type == 'array':
                    if not isinstance(value, list):
                        value = [value] if value is not None else []
                elif field_type == 'object':
                    if not isinstance(value, dict):
                        value = default_value or {}
                    else:
                        # Recursively normalize nested objects if items schema is provided
                        items_schema = field_schema.get('items', {})
                        if items_schema and isinstance(value, dict):
                            value = _normalize_json_object(value, {'properties': items_schema}, options)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting field '{field}' to type '{field_type}': {str(e)}")
                value = default_value
        else:
            # Use default value for missing fields
            value = default_value
        
        # Add field to normalized object
        normalized[field] = value
    
    # Include extra fields if not removing them
    if not options.get('remove_extra_fields', True):
        for field, value in obj.items():
            if field not in expected_fields:
                normalized[field] = value
    
    return normalized

@with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_csv_data'}, raise_exception=True)
def normalize_csv_data(csv_data: Union[str, bytes, io.IOBase], target_schema: Dict, options: Dict) -> pd.DataFrame:
    """
    Normalizes CSV data to match a target schema.
    
    Args:
        csv_data: The source CSV data to normalize (string, bytes, or file-like object)
        target_schema: The schema to normalize against
        options: Additional options for normalization
        
    Returns:
        Normalized DataFrame conforming to the target schema
        
    Raises:
        DataFormatError: If the data cannot be normalized to match the schema
        SchemaValidationError: If the schema validation fails
    """
    logger.debug("Normalizing CSV data to match target schema")
    
    # Extract CSV-specific options
    csv_options = options.get('csv_options', {})
    delimiter = csv_options.get('delimiter', ',')
    header = csv_options.get('header', 0)
    encoding = csv_options.get('encoding', 'utf-8')
    
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(
            csv_data, 
            delimiter=delimiter, 
            header=header, 
            encoding=encoding, 
            skip_blank_lines=True,
            **{k: v for k, v in csv_options.items() if k not in ['delimiter', 'header', 'encoding']}
        )
        
        # Use the normalize_dataframe function for further normalization
        return normalize_dataframe(df, target_schema, options)
    
    except pd.errors.EmptyDataError:
        logger.warning("Empty CSV data provided for normalization")
        if 'allow_empty' in options and options['allow_empty']:
            return pd.DataFrame(columns=[col['name'] for col in target_schema.get('columns', [])])
        else:
            raise DataFormatError("Empty CSV data provided for normalization", data_source="csv")
    
    except pd.errors.ParserError as e:
        raise DataFormatError(f"CSV parsing error: {str(e)}", data_source="csv")
    
    except Exception as e:
        raise DataFormatError(f"Error normalizing CSV data: {str(e)}", data_source="csv")

class DataNormalizer:
    """
    Class for normalizing data from various sources into a standardized format.
    
    This class handles normalization of data from different file formats (CSV, JSON, Avro, Parquet)
    into a consistent structure based on a target schema. It also tracks normalization statistics
    and provides methods for schema inference when needed.
    """
    
    def __init__(self):
        """
        Initialize the DataNormalizer with configuration.
        """
        # Load configuration
        self._config = get_config()
        
        # Initialize statistics tracking
        self._normalization_stats = {
            'total_normalizations': 0,
            'by_format': {},
            'total_records': 0,
            'total_fields': 0,
            'modified_fields': 0,
        }
        
        # Initialize schema cache for performance
        self._schema_cache = {}
        
        logger.debug("DataNormalizer initialized")
    
    @with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_data'}, raise_exception=True)
    def normalize_data(self, data: object, source_format: FileFormat, target_schema: Dict, options: Dict) -> object:
        """
        Normalize data based on its format and target schema.
        
        Args:
            data: The source data to normalize
            source_format: Format of the source data (from FileFormat enum)
            target_schema: The schema to normalize against (can be None for auto-inference)
            options: Additional options for normalization
            
        Returns:
            Normalized data in appropriate format
            
        Raises:
            DataFormatError: If the data format is unsupported or invalid
            SchemaValidationError: If the schema validation fails
        """
        if data is None:
            raise ValueError("Data cannot be None")
        
        # Set default options if not provided
        if options is None:
            options = {}
        
        # Infer schema if not provided
        target_schema = self._infer_schema_if_needed(data, target_schema, source_format)
        
        logger.debug(f"Normalizing data from format {source_format.value}")
        
        # Call appropriate method based on format
        if source_format == FileFormat.CSV:
            result = self.normalize_csv_data(data, target_schema, options)
        elif source_format == FileFormat.JSON:
            result = self.normalize_json_data(data, target_schema, options)
        elif source_format == FileFormat.AVRO:
            result = self.normalize_avro_data(data, target_schema, options)
        elif source_format == FileFormat.PARQUET:
            result = self.normalize_parquet_data(data, target_schema, options)
        else:
            raise DataFormatError(
                f"Unsupported format: {source_format.value}", 
                data_source=source_format.value
            )
        
        # Update statistics based on normalization results
        if isinstance(result, pd.DataFrame):
            self._update_stats(source_format, len(result), len(result.columns), 0)  # TODO: Track modifications
        elif isinstance(result, dict):
            self._update_stats(source_format, 1, len(result), 0)
        elif isinstance(result, list):
            self._update_stats(source_format, len(result), sum(len(item) for item in result if isinstance(item, dict)), 0)
        
        logger.debug(f"Data normalized successfully from format {source_format.value}")
        return result
    
    @with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_dataframe'}, raise_exception=True)
    def normalize_dataframe(self, df: pd.DataFrame, target_schema: Dict, options: Dict) -> pd.DataFrame:
        """
        Normalize a pandas DataFrame to match a target schema.
        
        Args:
            df: The source DataFrame to normalize
            target_schema: The schema to normalize against
            options: Additional options for normalization
            
        Returns:
            Normalized DataFrame conforming to the target schema
        """
        result = normalize_dataframe(df, target_schema, options)
        
        # Update statistics
        if result is not None:
            self._update_stats(FileFormat.CSV, len(result), len(result.columns), 0)  # Assuming CSV since it's a DataFrame
        
        return result
    
    @with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_json_data'}, raise_exception=True)
    def normalize_json_data(self, json_data: Union[Dict, List, str], target_schema: Dict, options: Dict) -> Union[Dict, List]:
        """
        Normalize JSON data to match a target schema.
        
        Args:
            json_data: The source JSON data to normalize
            target_schema: The schema to normalize against
            options: Additional options for normalization
            
        Returns:
            Normalized JSON data conforming to the target schema
        """
        result = normalize_json(json_data, target_schema, options)
        
        # Update statistics
        if isinstance(result, dict):
            self._update_stats(FileFormat.JSON, 1, len(result), 0)
        elif isinstance(result, list):
            self._update_stats(FileFormat.JSON, len(result), sum(len(item) for item in result if isinstance(item, dict)), 0)
        
        return result
    
    @with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_avro_data'}, raise_exception=True)
    def normalize_avro_data(self, avro_data: Union[bytes, io.IOBase], target_schema: Dict, options: Dict) -> Union[pd.DataFrame, List]:
        """
        Normalize Avro data to match a target schema.
        
        Args:
            avro_data: The source Avro data to normalize
            target_schema: The schema to normalize against
            options: Additional options for normalization
            
        Returns:
            Normalized data from Avro (as DataFrame or list of records)
        """
        logger.debug("Normalizing Avro data to match target schema")
        
        # Read Avro data
        records = []
        try:
            if isinstance(avro_data, io.IOBase):
                avro_records = list(fastavro.reader(avro_data))
            else:
                # For bytes, use BytesIO wrapper
                with io.BytesIO(avro_data) as bio:
                    avro_records = list(fastavro.reader(bio))
            
            # Check if we have any records
            if not avro_records:
                logger.warning("Empty Avro data provided for normalization")
                if 'allow_empty' in options and options['allow_empty']:
                    if options.get('return_dataframe', True):
                        return pd.DataFrame(columns=[col['name'] for col in target_schema.get('columns', [])])
                    else:
                        return []
                else:
                    raise DataFormatError("Empty Avro data provided for normalization", data_source="avro")
            
            records = avro_records
            
        except Exception as e:
            raise DataFormatError(f"Error reading Avro data: {str(e)}", data_source="avro")
        
        # Convert to DataFrame if specified (default) or normalize as JSON
        if options.get('return_dataframe', True):
            df = pd.DataFrame(records)
            result = self.normalize_dataframe(df, target_schema, options)
        else:
            result = self.normalize_json_data(records, target_schema, options)
        
        # Update statistics
        if isinstance(result, pd.DataFrame):
            self._update_stats(FileFormat.AVRO, len(result), len(result.columns), 0)
        else:
            self._update_stats(FileFormat.AVRO, len(result), sum(len(item) for item in result if isinstance(item, dict)), 0)
        
        return result
    
    @with_error_handling(context={'component': 'DataNormalizer', 'operation': 'normalize_parquet_data'}, raise_exception=True)
    def normalize_parquet_data(self, parquet_data: Union[bytes, io.IOBase], target_schema: Dict, options: Dict) -> pd.DataFrame:
        """
        Normalize Parquet data to match a target schema.
        
        Args:
            parquet_data: The source Parquet data to normalize
            target_schema: The schema to normalize against
            options: Additional options for normalization
            
        Returns:
            Normalized DataFrame from Parquet data
        """
        logger.debug("Normalizing Parquet data to match target schema")
        
        try:
            # Read Parquet data
            if isinstance(parquet_data, io.IOBase):
                table = pyarrow.parquet.read_table(parquet_data)
            else:
                # For bytes, use BytesIO wrapper
                with io.BytesIO(parquet_data) as bio:
                    table = pyarrow.parquet.read_table(bio)
            
            # Convert to DataFrame
            df = table.to_pandas()
            
            # Apply DataFrame normalization
            result = self.normalize_dataframe(df, target_schema, options)
            
            # Update statistics
            self._update_stats(FileFormat.PARQUET, len(result), len(result.columns), 0)
            
            return result
            
        except Exception as e:
            raise DataFormatError(f"Error normalizing Parquet data: {str(e)}", data_source="parquet")
    
    def get_normalization_stats(self) -> Dict:
        """
        Get statistics about normalization operations.
        
        Returns:
            Dictionary with normalization statistics
        """
        return self._normalization_stats.copy()
    
    def reset_normalization_stats(self) -> None:
        """
        Reset normalization statistics.
        """
        self._normalization_stats = {
            'total_normalizations': 0,
            'by_format': {},
            'total_records': 0,
            'total_fields': 0,
            'modified_fields': 0,
        }
        logger.debug("Normalization statistics reset")
    
    def _infer_schema_if_needed(self, data: object, target_schema: Dict, source_format: FileFormat) -> Dict:
        """
        Infer schema from data if target schema is not provided.
        
        Args:
            data: The source data
            target_schema: The provided target schema (may be None)
            source_format: Format of the source data
            
        Returns:
            The target schema (provided or inferred)
        """
        if target_schema is not None:
            return target_schema
        
        # Check if we already have a cached schema for this format
        cache_key = f"{source_format.value}_{hash(str(data)) if isinstance(data, (str, bytes)) else id(data)}"
        if cache_key in self._schema_cache:
            logger.debug(f"Using cached schema for {source_format.value}")
            return self._schema_cache[cache_key]
        
        logger.debug(f"Inferring schema from {source_format.value} data")
        
        try:
            # Use schema_utils to extract schema
            inferred_schema = extract_schema_from_data(data, source_format)
            
            # Cache the inferred schema
            self._schema_cache[cache_key] = inferred_schema
            
            return inferred_schema
        except Exception as e:
            logger.error(f"Error inferring schema from data: {str(e)}")
            raise
    
    def _update_stats(self, source_format: FileFormat, record_count: int, field_count: int, modified_count: int) -> None:
        """
        Update normalization statistics after an operation.
        
        Args:
            source_format: Format of the source data
            record_count: Number of records processed
            field_count: Number of fields processed
            modified_count: Number of fields modified
        """
        # Update total counts
        self._normalization_stats['total_normalizations'] += 1
        self._normalization_stats['total_records'] += record_count
        self._normalization_stats['total_fields'] += field_count
        self._normalization_stats['modified_fields'] += modified_count
        
        # Update format-specific counts
        format_key = source_format.value
        if format_key not in self._normalization_stats['by_format']:
            self._normalization_stats['by_format'][format_key] = {
                'normalizations': 0,
                'records': 0,
                'fields': 0,
                'modified': 0
            }
        
        self._normalization_stats['by_format'][format_key]['normalizations'] += 1
        self._normalization_stats['by_format'][format_key]['records'] += record_count
        self._normalization_stats['by_format'][format_key]['fields'] += field_count
        self._normalization_stats['by_format'][format_key]['modified'] += modified_count