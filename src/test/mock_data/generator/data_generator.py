"""
Provides utilities for generating test data based on schema definitions for the self-healing data pipeline.

This module enables the creation of realistic test datasets with configurable characteristics,
including the ability to inject specific data quality issues for testing validation and
self-healing capabilities.
"""

import os
import json
import random
import datetime
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Callable

import pandas as pd
import numpy as np
from faker import Faker  # version 18.x.x
import pyarrow as pa  # version 12.0.x
import pyarrow.parquet as pq

from src.backend.constants import FileFormat, DataSourceType, QualityDimension, HealingActionType
from src.test.mock_data.generator.schema_generator import SchemaGenerator, load_sample_schema
from src.test.utils.test_helpers import create_temp_file, create_temp_directory, create_test_dataframe

# Global constants
MOCK_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DEFAULT_DATA_SIZE = 1000
DEFAULT_NULL_PROBABILITY = 0.05
DEFAULT_ERROR_PROBABILITY = 0.1
QUALITY_ISSUE_TYPES = [
    'missing_values', 
    'invalid_types', 
    'out_of_range', 
    'format_errors', 
    'relationship_errors', 
    'duplicates'
]

# Registry for data type generators
DATA_TYPE_GENERATORS = {}

def generate_data_from_schema(schema: Dict, size: int = DEFAULT_DATA_SIZE, options: Dict = None) -> pd.DataFrame:
    """
    Generates test data based on a schema definition.
    
    Args:
        schema: Schema definition containing field specifications
        size: Number of rows to generate
        options: Additional options for data generation
            - null_probability: Probability of generating null values for nullable fields
            - error_probability: Probability of generating error values
            - locale: Locale for Faker data generation
            - seed: Random seed for reproducibility
            - date_range: Tuple of (start_date, end_date) for date fields
    
    Returns:
        Generated test data as DataFrame
    """
    if options is None:
        options = {}
    
    # Initialize Faker with locale if provided
    locale = options.get('locale', 'en_US')
    seed = options.get('seed')
    faker = Faker(locale)
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
    
    # Extract options
    null_probability = options.get('null_probability', DEFAULT_NULL_PROBABILITY)
    
    # Initialize data container
    data_columns = {}
    
    # Process each field in the schema
    for field in schema.get('fields', []):
        field_name = field.get('name')
        field_type = field.get('type')
        field_mode = field.get('mode', 'NULLABLE')
        
        # Generate data for this field
        field_options = {
            'null_probability': null_probability if field_mode == 'NULLABLE' else 0,
            'date_range': options.get('date_range'),
            'error_probability': options.get('error_probability', 0)
        }
        
        data_columns[field_name] = generate_field_data(field, size, faker, field_options)
    
    # Create DataFrame from generated data
    df = pd.DataFrame(data_columns)
    
    return df

def generate_field_data(field_def: Dict, size: int, faker: Faker, options: Dict = None) -> List:
    """
    Generates data for a specific field based on its type and constraints.
    
    Args:
        field_def: Field definition from the schema
        size: Number of values to generate
        faker: Faker instance for generating realistic data
        options: Additional options for data generation
    
    Returns:
        List of generated values for the field
    """
    if options is None:
        options = {}
    
    field_type = field_def.get('type', 'string').lower()
    field_mode = field_def.get('mode', 'NULLABLE')
    
    # Determine if field can contain nulls
    can_be_null = field_mode == 'NULLABLE'
    null_probability = options.get('null_probability', DEFAULT_NULL_PROBABILITY) if can_be_null else 0
    
    # Generate values based on field type
    if field_type == 'string':
        values = generate_string_data(field_def, size, faker, options)
    elif field_type in ('integer', 'int64'):
        values = generate_numeric_data(field_def, size, 'integer', options)
    elif field_type in ('float', 'float64', 'numeric'):
        values = generate_numeric_data(field_def, size, 'float', options)
    elif field_type == 'boolean':
        values = generate_boolean_data(field_def, size, options)
    elif field_type == 'date':
        values = generate_datetime_data(field_def, size, faker, {'datetime_type': 'date', **options})
    elif field_type == 'timestamp':
        values = generate_datetime_data(field_def, size, faker, {'datetime_type': 'timestamp', **options})
    elif field_type == 'array':
        values = generate_array_data(field_def, size, faker, options)
    elif field_type == 'record':
        values = generate_record_data(field_def, size, faker, options)
    else:
        # Default to string for unknown types
        values = generate_string_data(field_def, size, faker, options)
    
    # Insert nulls based on null_probability
    if null_probability > 0:
        for i in range(size):
            if random.random() < null_probability:
                values[i] = None
    
    return values

def register_data_generator(field_type: str, generator_func: Callable) -> None:
    """
    Registers a custom data generator function for a specific field type.
    
    Args:
        field_type: Type of field to register the generator for
        generator_func: Function that generates data for the field type
    """
    DATA_TYPE_GENERATORS[field_type] = generator_func

def generate_string_data(field_def: Dict, size: int, faker: Faker, options: Dict = None) -> List[str]:
    """
    Generates string data based on field constraints.
    
    Args:
        field_def: Field definition from the schema
        size: Number of values to generate
        faker: Faker instance for generating realistic data
        options: Additional options for data generation
    
    Returns:
        List of generated string values
    """
    if options is None:
        options = {}
    
    # Extract constraints from field definition
    min_length = field_def.get('min_length', 1)
    max_length = field_def.get('max_length', 100)
    pattern = field_def.get('pattern')
    
    # Determine string subtype from field description or options
    description = field_def.get('description', '').lower()
    string_subtype = options.get('string_subtype')
    
    if not string_subtype:
        # Try to infer subtype from field name or description
        if any(kw in description or kw in field_def.get('name', '').lower() 
               for kw in ['name', 'person', 'customer', 'employee']):
            string_subtype = 'name'
        elif any(kw in description or kw in field_def.get('name', '').lower() 
                 for kw in ['email', 'mail', 'e-mail']):
            string_subtype = 'email'
        elif any(kw in description or kw in field_def.get('name', '').lower() 
                 for kw in ['address', 'street', 'city', 'state']):
            string_subtype = 'address'
        elif any(kw in description or kw in field_def.get('name', '').lower() 
                 for kw in ['phone', 'telephone', 'mobile']):
            string_subtype = 'phone'
        else:
            string_subtype = 'text'
    
    # Generate values
    values = []
    for _ in range(size):
        if string_subtype == 'name':
            value = faker.name()
        elif string_subtype == 'email':
            value = faker.email()
        elif string_subtype == 'address':
            value = faker.address().replace('\n', ', ')
        elif string_subtype == 'phone':
            value = faker.phone_number()
        else:
            # Generate random text with appropriate length
            if min_length == max_length:
                value = faker.pystr(min_length, min_length)
            else:
                value = faker.pystr(min_length, max_length)
        
        # Apply pattern constraint if specified
        if pattern:
            # This is a simplified approach - for real implementation, would need regex
            value = value[:max_length]  # Just truncate for now
        
        values.append(value)
    
    return values

def generate_numeric_data(field_def: Dict, size: int, numeric_type: str, options: Dict = None) -> List[Union[int, float]]:
    """
    Generates numeric data based on field constraints.
    
    Args:
        field_def: Field definition from the schema
        size: Number of values to generate
        numeric_type: Type of numeric data ('integer' or 'float')
        options: Additional options for data generation
    
    Returns:
        List of generated numeric values
    """
    if options is None:
        options = {}
    
    # Extract constraints from field definition
    min_value = field_def.get('minimum', -1000000 if numeric_type == 'integer' else -1000.0)
    max_value = field_def.get('maximum', 1000000 if numeric_type == 'integer' else 1000.0)
    precision = field_def.get('precision')
    
    # Determine distribution type
    distribution = options.get('distribution', 'uniform')
    
    # Generate values
    values = []
    
    if distribution == 'uniform':
        if numeric_type == 'integer':
            values = [random.randint(min_value, max_value) for _ in range(size)]
        else:  # float
            values = [random.uniform(min_value, max_value) for _ in range(size)]
    elif distribution == 'normal':
        mean = (max_value + min_value) / 2
        std_dev = (max_value - min_value) / 6  # ~99.7% of values within range
        
        raw_values = np.random.normal(mean, std_dev, size)
        
        # Clip values to ensure they're within range
        raw_values = np.clip(raw_values, min_value, max_value)
        
        if numeric_type == 'integer':
            values = [int(round(v)) for v in raw_values]
        else:  # float
            values = list(raw_values)
    
    # Apply precision for float values
    if numeric_type == 'float' and precision is not None:
        values = [round(v, precision) for v in values]
    
    return values

def generate_boolean_data(field_def: Dict, size: int, options: Dict = None) -> List[bool]:
    """
    Generates boolean data with configurable true/false ratio.
    
    Args:
        field_def: Field definition from the schema
        size: Number of values to generate
        options: Additional options for data generation
    
    Returns:
        List of generated boolean values
    """
    if options is None:
        options = {}
    
    # Extract true ratio from options or use default
    true_ratio = options.get('true_ratio', 0.5)
    
    # Generate values
    values = [random.random() < true_ratio for _ in range(size)]
    
    return values

def generate_datetime_data(field_def: Dict, size: int, faker: Faker, options: Dict = None) -> List:
    """
    Generates date or timestamp data based on field constraints.
    
    Args:
        field_def: Field definition from the schema
        size: Number of values to generate
        faker: Faker instance for generating realistic data
        options: Additional options for data generation
    
    Returns:
        List of generated datetime values
    """
    if options is None:
        options = {}
    
    # Extract constraints from field definition
    min_date = field_def.get('minimum')
    max_date = field_def.get('maximum')
    format_str = field_def.get('format')
    
    # Use date_range from options if provided
    date_range = options.get('date_range')
    if date_range:
        min_date, max_date = date_range
    
    # Set reasonable defaults if not specified
    if not min_date:
        min_date = datetime.datetime(2000, 1, 1)
    elif isinstance(min_date, str):
        min_date = datetime.datetime.fromisoformat(min_date.replace('Z', '+00:00'))
    
    if not max_date:
        max_date = datetime.datetime.now()
    elif isinstance(max_date, str):
        max_date = datetime.datetime.fromisoformat(max_date.replace('Z', '+00:00'))
    
    # Generate values
    values = []
    
    for _ in range(size):
        fake_date = faker.date_time_between(min_date, max_date)
        
        # Return appropriate type based on requested datetime_type
        datetime_type = options.get('datetime_type', 'timestamp')
        
        if datetime_type == 'date':
            value = fake_date.date()
            
            # Apply format if specified
            if format_str:
                value = value.strftime(format_str)
        else:  # timestamp
            value = fake_date
            
            # Apply format if specified
            if format_str:
                value = value.strftime(format_str)
        
        values.append(value)
    
    return values

def generate_array_data(field_def: Dict, size: int, faker: Faker, options: Dict = None) -> List[List]:
    """
    Generates array data with items of specified type.
    
    Args:
        field_def: Field definition from the schema
        size: Number of arrays to generate
        faker: Faker instance for generating realistic data
        options: Additional options for data generation
    
    Returns:
        List of generated arrays
    """
    if options is None:
        options = {}
    
    # Extract constraints from field definition
    item_type = field_def.get('item_type', 'string')
    min_items = field_def.get('min_items', 0)
    max_items = field_def.get('max_items', 5)
    
    # Create a field definition for item type
    item_field_def = {
        'name': f"{field_def.get('name')}_item",
        'type': item_type,
        'mode': 'REQUIRED'  # Array items are typically required
    }
    
    # Copy any constraints specific to the item type
    for key in field_def.keys():
        if key.startswith('item_') and key != 'item_type':
            # Convert item_min_length to min_length for the item field
            item_key = key.replace('item_', '')
            item_field_def[item_key] = field_def[key]
    
    # Generate arrays
    arrays = []
    
    for _ in range(size):
        # Determine random array length
        array_length = random.randint(min_items, max_items)
        
        # Generate items
        items = generate_field_data(item_field_def, array_length, faker, options)
        
        arrays.append(items)
    
    return arrays

def generate_record_data(field_def: Dict, size: int, faker: Faker, options: Dict = None) -> List[Dict]:
    """
    Generates nested record data based on subfield definitions.
    
    Args:
        field_def: Field definition from the schema
        size: Number of records to generate
        faker: Faker instance for generating realistic data
        options: Additional options for data generation
    
    Returns:
        List of generated record objects
    """
    if options is None:
        options = {}
    
    # Extract subfields
    subfields = field_def.get('fields', [])
    
    # Generate records
    records = []
    
    for _ in range(size):
        record = {}
        
        # Generate value for each subfield
        for subfield in subfields:
            subfield_name = subfield.get('name')
            
            # Generate a single value (size=1) for this subfield
            subfield_values = generate_field_data(subfield, 1, faker, options)
            
            # Add to record
            record[subfield_name] = subfield_values[0]
        
        records.append(record)
    
    return records

def inject_data_quality_issues(df: pd.DataFrame, issues_config: Dict, schema: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects specific data quality issues into a dataset for testing.
    
    Args:
        df: Original DataFrame without issues
        issues_config: Configuration specifying which issues to inject
            Each key should be an issue type, with a dictionary of parameters
        schema: Optional schema definition for type-specific issues
    
    Returns:
        (DataFrame with issues, issue details dictionary)
    """
    # Create a copy to avoid modifying the original
    df_with_issues = df.copy()
    
    # Track injected issues
    injected_issues = {}
    
    # Process each issue type in the configuration
    for issue_type, config in issues_config.items():
        if issue_type == 'missing_values':
            df_with_issues, issue_details = inject_missing_values(df_with_issues, config)
            injected_issues['missing_values'] = issue_details
        
        elif issue_type == 'invalid_types':
            df_with_issues, issue_details = inject_invalid_types(df_with_issues, config, schema)
            injected_issues['invalid_types'] = issue_details
        
        elif issue_type == 'out_of_range':
            df_with_issues, issue_details = inject_out_of_range_values(df_with_issues, config, schema)
            injected_issues['out_of_range'] = issue_details
        
        elif issue_type == 'format_errors':
            df_with_issues, issue_details = inject_format_errors(df_with_issues, config, schema)
            injected_issues['format_errors'] = issue_details
        
        elif issue_type == 'relationship_errors':
            df_with_issues, issue_details = inject_relationship_errors(df_with_issues, config, schema)
            injected_issues['relationship_errors'] = issue_details
        
        elif issue_type == 'duplicates':
            df_with_issues, issue_details = inject_duplicates(df_with_issues, config)
            injected_issues['duplicates'] = issue_details
    
    return df_with_issues, injected_issues

def inject_missing_values(df: pd.DataFrame, config: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects missing values (nulls) into specified columns.
    
    Args:
        df: Original DataFrame
        config: Configuration for missing values
            - columns: List of columns to inject nulls into
            - percentage: Percentage of values to replace with nulls
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Create a copy to avoid modifying the original
    df_modified = df.copy()
    
    # Extract configuration
    columns = config.get('columns', [])
    percentage = config.get('percentage', 0.1)  # Default to 10%
    
    # Track details of injected nulls
    null_details = {'columns': {}}
    
    # Validate columns exist in DataFrame
    valid_columns = [col for col in columns if col in df_modified.columns]
    
    # Inject nulls into each column
    for column in valid_columns:
        # Calculate how many values to nullify
        num_rows = len(df_modified)
        num_nulls = int(num_rows * percentage)
        
        # Randomly select indices to nullify
        null_indices = random.sample(range(num_rows), num_nulls)
        
        # Set selected values to None
        df_modified.loc[null_indices, column] = None
        
        # Track details
        null_details['columns'][column] = {
            'count': num_nulls,
            'percentage': percentage,
            'indices': null_indices
        }
    
    null_details['total_nulls_injected'] = sum(detail['count'] for detail in null_details['columns'].values())
    
    return df_modified, null_details

def inject_invalid_types(df: pd.DataFrame, config: Dict, schema: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects values with incorrect types into specified columns.
    
    Args:
        df: Original DataFrame
        config: Configuration for invalid types
            - columns: List of columns to inject invalid types into
            - percentage: Percentage of values to replace
        schema: Optional schema definition for determining expected types
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Create a copy to avoid modifying the original
    df_modified = df.copy()
    
    # Extract configuration
    columns = config.get('columns', [])
    percentage = config.get('percentage', 0.05)  # Default to 5%
    
    # Track details of injected type errors
    type_details = {'columns': {}}
    
    # Get schema field definitions if available
    field_types = {}
    if schema:
        for field in schema.get('fields', []):
            field_types[field.get('name')] = field.get('type', 'string')
    
    # Validate columns exist in DataFrame
    valid_columns = [col for col in columns if col in df_modified.columns]
    
    # Inject invalid types into each column
    for column in valid_columns:
        # Calculate how many values to modify
        num_rows = len(df_modified)
        num_invalid = int(num_rows * percentage)
        
        # Randomly select indices to modify
        invalid_indices = random.sample(range(num_rows), num_invalid)
        
        # Determine current type and invalid type
        current_type = field_types.get(column, str(df_modified[column].dtype))
        
        # Generate invalid values based on current type
        invalid_values = []
        
        for idx in invalid_indices:
            if pd.api.types.is_numeric_dtype(df_modified[column].dtype):
                # For numeric columns, insert strings
                invalid_values.append(f"invalid-{random.randint(1000, 9999)}")
            
            elif pd.api.types.is_string_dtype(df_modified[column].dtype):
                # For string columns, insert inappropriate objects
                invalid_values.append(random.choice([
                    complex(1, 2),  # Complex number
                    {'key': 'value'},  # Dictionary
                    [1, 2, 3]  # List
                ]))
            
            elif pd.api.types.is_datetime64_dtype(df_modified[column].dtype):
                # For date columns, insert invalid date strings
                invalid_values.append(f"not-a-date-{random.randint(1000, 9999)}")
            
            elif pd.api.types.is_bool_dtype(df_modified[column].dtype):
                # For boolean columns, insert non-boolean values
                invalid_values.append(random.choice([
                    "true",  # String 'true' instead of boolean True
                    "false",  # String 'false' instead of boolean False
                    random.randint(0, 100)  # Integer instead of boolean
                ]))
            
            else:
                # For other types, insert a string
                invalid_values.append(f"invalid-{random.randint(1000, 9999)}")
        
        # Apply invalid values
        for i, idx in enumerate(invalid_indices):
            df_modified.at[idx, column] = invalid_values[i]
        
        # Track details
        type_details['columns'][column] = {
            'count': num_invalid,
            'percentage': percentage,
            'indices': invalid_indices,
            'original_type': current_type,
            'sample_invalid_values': invalid_values[:5]
        }
    
    type_details['total_invalid_injected'] = sum(detail['count'] for detail in type_details['columns'].values())
    
    return df_modified, type_details

def inject_out_of_range_values(df: pd.DataFrame, config: Dict, schema: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects values outside of valid ranges into specified columns.
    
    Args:
        df: Original DataFrame
        config: Configuration for out-of-range values
            - columns: List of columns to inject out-of-range values into
            - percentage: Percentage of values to replace
        schema: Optional schema definition for determining valid ranges
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Create a copy to avoid modifying the original
    df_modified = df.copy()
    
    # Extract configuration
    columns = config.get('columns', [])
    percentage = config.get('percentage', 0.05)  # Default to 5%
    
    # Track details of injected range errors
    range_details = {'columns': {}}
    
    # Get schema constraints if available
    field_constraints = {}
    if schema:
        for field in schema.get('fields', []):
            field_name = field.get('name')
            constraints = {}
            
            if 'minimum' in field:
                constraints['min'] = field['minimum']
            if 'maximum' in field:
                constraints['max'] = field['maximum']
            if 'min_length' in field:
                constraints['min_length'] = field['min_length']
            if 'max_length' in field:
                constraints['max_length'] = field['max_length']
            
            if constraints:
                field_constraints[field_name] = constraints
    
    # Validate columns exist in DataFrame
    valid_columns = [col for col in columns if col in df_modified.columns]
    
    # Inject out-of-range values into each column
    for column in valid_columns:
        # Calculate how many values to modify
        num_rows = len(df_modified)
        num_invalid = int(num_rows * percentage)
        
        # Randomly select indices to modify
        invalid_indices = random.sample(range(num_rows), num_invalid)
        
        # Generate out-of-range values based on column type and constraints
        invalid_values = []
        
        # Get constraints for this column
        constraints = field_constraints.get(column, {})
        
        for idx in invalid_indices:
            if pd.api.types.is_numeric_dtype(df_modified[column].dtype):
                # For numeric columns, generate values outside min/max
                min_val = constraints.get('min')
                max_val = constraints.get('max')
                
                if min_val is not None and max_val is not None:
                    # Generate value outside both bounds
                    if random.choice([True, False]):
                        invalid_values.append(min_val - random.randint(1, 100))
                    else:
                        invalid_values.append(max_val + random.randint(1, 100))
                elif min_val is not None:
                    # Generate value below minimum
                    invalid_values.append(min_val - random.randint(1, 100))
                elif max_val is not None:
                    # Generate value above maximum
                    invalid_values.append(max_val + random.randint(1, 100))
                else:
                    # No specific constraints, generate extreme values
                    if pd.api.types.is_integer_dtype(df_modified[column].dtype):
                        invalid_values.append(random.choice([-10**9, 10**9]))
                    else:
                        invalid_values.append(random.choice([-10**6, 10**6]) * random.random())
            
            elif pd.api.types.is_string_dtype(df_modified[column].dtype):
                # For string columns, generate strings exceeding max length
                max_length = constraints.get('max_length')
                
                if max_length:
                    # Generate string longer than max_length
                    invalid_values.append('x' * (max_length + random.randint(1, 100)))
                else:
                    # No specific length constraint, generate very long string
                    invalid_values.append('x' * 1000)
            
            elif pd.api.types.is_datetime64_dtype(df_modified[column].dtype):
                # For date columns, generate dates outside valid range
                min_date = constraints.get('min')
                max_date = constraints.get('max')
                
                if min_date:
                    if isinstance(min_date, str):
                        min_date = pd.to_datetime(min_date)
                    # Generate date before minimum
                    days_before = random.randint(1, 365)
                    invalid_values.append(min_date - pd.Timedelta(days=days_before))
                elif max_date:
                    if isinstance(max_date, str):
                        max_date = pd.to_datetime(max_date)
                    # Generate date after maximum
                    days_after = random.randint(1, 365)
                    invalid_values.append(max_date + pd.Timedelta(days=days_after))
                else:
                    # No specific constraints, generate extreme dates
                    extreme_years = [1800, 2200]  # Years far in the past or future
                    year = random.choice(extreme_years)
                    month = random.randint(1, 12)
                    day = random.randint(1, 28)
                    invalid_values.append(pd.Timestamp(year=year, month=month, day=day))
            
            else:
                # For other types, add a generic invalid value
                invalid_values.append(None)  # Placeholder, may need customization for other types
        
        # Apply invalid values
        for i, idx in enumerate(invalid_indices):
            if invalid_values[i] is not None:  # Skip None placeholders
                df_modified.at[idx, column] = invalid_values[i]
        
        # Track details
        range_details['columns'][column] = {
            'count': num_invalid,
            'percentage': percentage,
            'indices': invalid_indices,
            'constraints': constraints,
            'sample_invalid_values': [str(v) for v in invalid_values[:5] if v is not None]
        }
    
    range_details['total_range_violations'] = sum(detail['count'] for detail in range_details['columns'].values())
    
    return df_modified, range_details

def inject_format_errors(df: pd.DataFrame, config: Dict, schema: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects incorrectly formatted values into specified columns.
    
    Args:
        df: Original DataFrame
        config: Configuration for format errors
            - columns: List of columns to inject format errors into
            - percentage: Percentage of values to replace
        schema: Optional schema definition for determining expected formats
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Create a copy to avoid modifying the original
    df_modified = df.copy()
    
    # Extract configuration
    columns = config.get('columns', [])
    percentage = config.get('percentage', 0.05)  # Default to 5%
    formats = config.get('formats', {})  # Column-specific format info
    
    # Track details of injected format errors
    format_details = {'columns': {}}
    
    # Get schema format information if available
    field_formats = {}
    if schema:
        for field in schema.get('fields', []):
            field_name = field.get('name')
            if 'format' in field:
                field_formats[field_name] = field['format']
            elif 'pattern' in field:
                field_formats[field_name] = field['pattern']
    
    # Merge schema formats with configuration formats
    for column, format_info in formats.items():
        field_formats[column] = format_info
    
    # Validate columns exist in DataFrame
    valid_columns = [col for col in columns if col in df_modified.columns]
    
    # Helper function to generate format errors based on expected format
    def generate_format_error(expected_format, current_value):
        if expected_format == 'email':
            # Malformed email addresses
            return random.choice([
                "user@missing-domain",
                "no-at-symbol.com",
                "double@@at.com",
                "spaces in@email.com",
                "missing-tld@domain"
            ])
        elif expected_format == 'phone':
            # Malformed phone numbers
            return random.choice([
                "123-456",  # Incomplete
                "abcdefghij",  # Letters
                "12345678901234567890",  # Too long
                "(1234) 567",  # Incomplete with parentheses
                "+--123--456"  # Invalid characters
            ])
        elif expected_format in ('date', 'date-time'):
            # Malformed dates or timestamps
            return random.choice([
                "2023/13/45",  # Invalid date
                "Jan 35, 2023",  # Invalid day
                "2023-02-30",  # Invalid day for month
                "2023-02-15 25:70:99",  # Invalid time
                "not-a-date-at-all"
            ])
        elif expected_format == 'url':
            # Malformed URLs
            return random.choice([
                "http:/missing-slash",
                "https://no-tld",
                "www.missing-protocol.com",
                "https://.com",
                "http://invalid chars.com"
            ])
        else:
            # Generic format error - just reverse the string if it's a string
            if isinstance(current_value, str):
                return current_value[::-1]  # Reverse the string
            else:
                return str(current_value) + "-malformed"
    
    # Inject format errors into each column
    for column in valid_columns:
        # Skip if no format information available for this column
        if column not in field_formats and not isinstance(df_modified[column].iloc[0], str):
            continue
        
        # Calculate how many values to modify
        num_rows = len(df_modified)
        num_invalid = int(num_rows * percentage)
        
        # Randomly select indices to modify
        invalid_indices = random.sample(range(num_rows), num_invalid)
        
        # Get expected format
        expected_format = field_formats.get(column, 'generic')
        
        # Generate format errors
        invalid_values = []
        
        for idx in invalid_indices:
            current_value = df_modified.at[idx, column]
            invalid_value = generate_format_error(expected_format, current_value)
            invalid_values.append(invalid_value)
        
        # Apply invalid values
        for i, idx in enumerate(invalid_indices):
            df_modified.at[idx, column] = invalid_values[i]
        
        # Track details
        format_details['columns'][column] = {
            'count': num_invalid,
            'percentage': percentage,
            'indices': invalid_indices,
            'expected_format': expected_format,
            'sample_invalid_values': invalid_values[:5]
        }
    
    format_details['total_format_errors'] = sum(detail['count'] for detail in format_details['columns'].values())
    
    return df_modified, format_details

def inject_relationship_errors(df: pd.DataFrame, config: Dict, schema: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects referential integrity errors between related columns.
    
    Args:
        df: Original DataFrame
        config: Configuration for relationship errors
            - relationships: List of relationship definitions (source and target columns)
            - percentage: Percentage of relationships to break
        schema: Optional schema definition for determining relationships
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Create a copy to avoid modifying the original
    df_modified = df.copy()
    
    # Extract configuration
    relationships = config.get('relationships', [])
    percentage = config.get('percentage', 0.05)  # Default to 5%
    
    # Use relationships from schema if available and none provided in config
    if not relationships and schema and 'relationships' in schema:
        relationships = schema['relationships']
    
    # Track details of injected relationship errors
    relationship_details = {'relationships': {}}
    
    # Process each relationship
    for relation_idx, relation in enumerate(relationships):
        # Extract source and target information
        source_col = relation.get('source_field')
        target_col = relation.get('target_field')
        relation_type = relation.get('type', 'foreign_key')
        
        # Skip if source or target column missing
        if not source_col or not target_col or source_col not in df_modified.columns or target_col not in df_modified.columns:
            continue
        
        # Calculate how many relationships to break
        num_rows = len(df_modified)
        num_to_break = int(num_rows * percentage)
        
        # Randomly select indices to modify
        indices_to_break = random.sample(range(num_rows), num_to_break)
        
        # Track changes for this relationship
        rel_key = f"{source_col}_{target_col}"
        relationship_details['relationships'][rel_key] = {
            'source_column': source_col,
            'target_column': target_col,
            'relationship_type': relation_type,
            'count': num_to_break,
            'percentage': percentage,
            'indices': indices_to_break,
            'sample_changes': []
        }
        
        # Break the relationships
        for idx in indices_to_break:
            old_value = df_modified.at[idx, source_col]
            
            # Generate a value that's not in the target column
            valid_values = set(df_modified[target_col].dropna().unique())
            
            if valid_values:
                # Get current distinct values in the target column
                current_value = df_modified.at[idx, source_col]
                
                # Generate values until we find one that's not in the valid set
                invalid_value = current_value
                attempts = 0
                
                while invalid_value in valid_values and attempts < 100:
                    if isinstance(current_value, (int, float, np.number)):
                        # For numeric values, modify by a random factor
                        multiplier = random.uniform(0.5, 2.0)
                        offset = random.randint(1, 100)
                        if random.choice([True, False]):
                            invalid_value = current_value * multiplier + offset
                        else:
                            invalid_value = current_value * multiplier - offset
                    elif isinstance(current_value, str):
                        # For string values, append or prepend something
                        if random.choice([True, False]):
                            invalid_value = current_value + f"-invalid-{random.randint(1000, 9999)}"
                        else:
                            invalid_value = f"invalid-{random.randint(1000, 9999)}-" + current_value
                    else:
                        # For other types, convert to string and modify
                        invalid_value = f"invalid-{random.randint(1000, 9999)}"
                    
                    attempts += 1
                
                # Apply the invalid value
                df_modified.at[idx, source_col] = invalid_value
                
                # Track sample changes (up to 5)
                if len(relationship_details['relationships'][rel_key]['sample_changes']) < 5:
                    relationship_details['relationships'][rel_key]['sample_changes'].append({
                        'index': idx,
                        'old_value': old_value,
                        'new_value': invalid_value
                    })
    
    relationship_details['total_relationships_broken'] = sum(
        detail['count'] for detail in relationship_details['relationships'].values()
    )
    
    return df_modified, relationship_details

def inject_duplicates(df: pd.DataFrame, config: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Injects duplicate rows into the dataset.
    
    Args:
        df: Original DataFrame
        config: Configuration for duplicate injection
            - percentage: Percentage of additional rows as duplicates
            - key_columns: Columns to consider for defining duplicates
    
    Returns:
        (Modified DataFrame, issue details)
    """
    # Extract configuration
    percentage = config.get('percentage', 0.05)  # Default to 5%
    key_columns = config.get('key_columns', None)  # Optional subset of columns for duplicate definition
    
    # Calculate number of duplicates to add
    num_rows = len(df)
    num_duplicates = int(num_rows * percentage)
    
    # Track details of injected duplicates
    duplicate_details = {
        'count': num_duplicates,
        'percentage': percentage,
        'sample_duplicates': []
    }
    
    # Select rows to duplicate
    rows_to_duplicate = random.sample(range(num_rows), min(num_duplicates, num_rows))
    
    # Create duplicates
    duplicates = []
    
    for idx in rows_to_duplicate:
        original_row = df.iloc[idx].copy()
        
        # Create a duplicate (with optional modifications to non-key columns)
        if key_columns:
            # Only columns in key_columns remain identical
            duplicate_row = df.iloc[random.randint(0, num_rows-1)].copy()
            for col in key_columns:
                duplicate_row[col] = original_row[col]
        else:
            # Exact duplicate
            duplicate_row = original_row.copy()
        
        duplicates.append(duplicate_row)
        
        # Track sample duplicates (up to 5)
        if len(duplicate_details['sample_duplicates']) < 5:
            duplicate_details['sample_duplicates'].append({
                'original_index': idx,
                'key_columns': key_columns,
                'added_at_index': num_rows + len(duplicates) - 1
            })
    
    # Add duplicates to DataFrame
    df_with_duplicates = pd.concat([df, pd.DataFrame(duplicates)], ignore_index=True)
    
    return df_with_duplicates, duplicate_details

def save_data_to_file(df: pd.DataFrame, file_path: str, file_format: FileFormat, options: Dict = None) -> str:
    """
    Saves generated data to a file in the specified format.
    
    Args:
        df: DataFrame to save
        file_path: Path where to save the file
        file_format: Format to use (from FileFormat enum)
        options: Additional options for saving
    
    Returns:
        Path to the saved file
    """
    if options is None:
        options = {}
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
    
    # Save according to format
    if file_format == FileFormat.CSV:
        # Extract CSV-specific options
        csv_options = {
            'index': options.get('include_index', False),
            'sep': options.get('separator', ','),
            'encoding': options.get('encoding', 'utf-8'),
            'header': options.get('include_header', True),
            'quoting': options.get('quoting', 1),  # QUOTE_ALL
            'date_format': options.get('date_format', None),
            'na_rep': options.get('null_string', '')
        }
        df.to_csv(file_path, **csv_options)
    
    elif file_format == FileFormat.JSON:
        # Extract JSON-specific options
        json_options = {
            'orient': options.get('orient', 'records'),
            'date_format': options.get('date_format', None),
            'double_precision': options.get('double_precision', 10),
            'lines': options.get('lines', False),
            'indent': options.get('indent', None)
        }
        df.to_json(file_path, **json_options)
    
    elif file_format == FileFormat.PARQUET:
        # Extract Parquet-specific options
        parquet_options = {
            'engine': options.get('engine', 'pyarrow'),
            'compression': options.get('compression', 'snappy'),
            'index': options.get('include_index', False)
        }
        df.to_parquet(file_path, **parquet_options)
    
    elif file_format == FileFormat.AVRO:
        # Handle Avro separately as pandas doesn't have direct support
        try:
            import fastavro
            table = pa.Table.from_pandas(df)
            
            # Convert to Avro
            avro_schema = options.get('avro_schema')
            
            if not avro_schema:
                # Generate a basic schema
                fields = []
                for name, dtype in df.dtypes.items():
                    if pd.api.types.is_integer_dtype(dtype):
                        field_type = "long"
                    elif pd.api.types.is_float_dtype(dtype):
                        field_type = "double"
                    elif pd.api.types.is_bool_dtype(dtype):
                        field_type = "boolean"
                    elif pd.api.types.is_datetime64_dtype(dtype):
                        field_type = {"type": "long", "logicalType": "timestamp-micros"}
                    else:
                        field_type = "string"
                    
                    fields.append({"name": name, "type": ["null", field_type]})
                
                avro_schema = {
                    "namespace": "example.avro",
                    "type": "record",
                    "name": "data",
                    "fields": fields
                }
            
            # Convert to records for Avro
            records = df.to_dict('records')
            
            # Write to file
            with open(file_path, 'wb') as avro_file:
                fastavro.writer(avro_file, avro_schema, records)
                
        except ImportError:
            raise ImportError("fastavro is required for Avro format support")
    
    else:
        raise NotImplementedError(f"Saving data in {file_format} format is not implemented")
    
    return file_path

def load_data_from_file(file_path: str, file_format: FileFormat, options: Dict = None) -> pd.DataFrame:
    """
    Loads data from a file in the specified format.
    
    Args:
        file_path: Path to the file to load
        file_format: Format of the file (from FileFormat enum)
        options: Additional options for loading
    
    Returns:
        Loaded data as DataFrame
    """
    if options is None:
        options = {}
    
    # Verify file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Load according to format
    if file_format == FileFormat.CSV:
        # Extract CSV-specific options
        csv_options = {
            'index_col': options.get('index_column', None),
            'sep': options.get('separator', ','),
            'encoding': options.get('encoding', 'utf-8'),
            'header': 0 if options.get('has_header', True) else None,
            'quoting': options.get('quoting', 1),  # QUOTE_ALL
            'parse_dates': options.get('parse_dates', False),
            'na_values': options.get('null_strings', None)
        }
        df = pd.read_csv(file_path, **csv_options)
    
    elif file_format == FileFormat.JSON:
        # Extract JSON-specific options
        json_options = {
            'orient': options.get('orient', 'records'),
            'convert_dates': options.get('convert_dates', True),
            'lines': options.get('lines', False)
        }
        df = pd.read_json(file_path, **json_options)
    
    elif file_format == FileFormat.PARQUET:
        # Extract Parquet-specific options
        parquet_options = {
            'engine': options.get('engine', 'pyarrow')
        }
        df = pd.read_parquet(file_path, **parquet_options)
    
    elif file_format == FileFormat.AVRO:
        # Handle Avro separately
        try:
            import fastavro
            
            with open(file_path, 'rb') as avro_file:
                reader = fastavro.reader(avro_file)
                records = [r for r in reader]
            
            df = pd.DataFrame.from_records(records)
                
        except ImportError:
            raise ImportError("fastavro is required for Avro format support")
    
    else:
        raise NotImplementedError(f"Loading data from {file_format} format is not implemented")
    
    return df

def generate_data_batch(schema: Dict, batch_size: int, options: Dict = None) -> pd.DataFrame:
    """
    Generates a batch of data for large dataset creation.
    
    Args:
        schema: Schema definition containing field specifications
        batch_size: Number of rows to generate in this batch
        options: Additional options for data generation
    
    Returns:
        Generated batch of data
    """
    return generate_data_from_schema(schema, batch_size, options)

class DataGenerator:
    """
    Main class for generating test data based on schema definitions.
    
    This class provides methods to generate test data with configurable characteristics,
    inject data quality issues, and save/load data files in various formats.
    """
    
    def __init__(self):
        """
        Initialize the DataGenerator with default generators.
        """
        # Initialize Faker for generating realistic data
        self._faker = Faker()
        
        # Initialize schema generator
        self._schema_generator = SchemaGenerator()
        
        # Initialize data generators dictionary
        self._data_generators = {}
        
        # Initialize issue generators dictionary
        self._issue_generators = {}
        
        # Register built-in data type generators
        self.register_data_generator('string', generate_string_data)
        self.register_data_generator('integer', lambda field, size, faker, options: 
                                   generate_numeric_data(field, size, 'integer', options))
        self.register_data_generator('float', lambda field, size, faker, options: 
                                   generate_numeric_data(field, size, 'float', options))
        self.register_data_generator('boolean', generate_boolean_data)
        self.register_data_generator('date', lambda field, size, faker, options:
                                   generate_datetime_data(field, size, faker, {'datetime_type': 'date', **options}))
        self.register_data_generator('timestamp', lambda field, size, faker, options:
                                   generate_datetime_data(field, size, faker, {'datetime_type': 'timestamp', **options}))
        self.register_data_generator('array', generate_array_data)
        self.register_data_generator('record', generate_record_data)
        
        # Register built-in issue generators
        self.register_issue_generator('missing_values', inject_missing_values)
        self.register_issue_generator('invalid_types', inject_invalid_types)
        self.register_issue_generator('out_of_range', inject_out_of_range_values)
        self.register_issue_generator('format_errors', inject_format_errors)
        self.register_issue_generator('relationship_errors', inject_relationship_errors)
        self.register_issue_generator('duplicates', inject_duplicates)
    
    def register_data_generator(self, data_type: str, generator_func: Callable) -> None:
        """
        Registers a custom data generator function.
        
        Args:
            data_type: Type of data to generate
            generator_func: Function that generates data of this type
        """
        self._data_generators[data_type] = generator_func
    
    def register_issue_generator(self, issue_type: str, generator_func: Callable) -> None:
        """
        Registers a custom issue generator function.
        
        Args:
            issue_type: Type of issue to generate
            generator_func: Function that generates this type of issue
        """
        self._issue_generators[issue_type] = generator_func
    
    def generate_data(self, schema: Dict, size: int = DEFAULT_DATA_SIZE, options: Dict = None) -> pd.DataFrame:
        """
        Generates test data based on a schema definition.
        
        Args:
            schema: Schema definition containing field specifications
            size: Number of rows to generate
            options: Additional options for data generation
        
        Returns:
            Generated test data as DataFrame
        """
        return generate_data_from_schema(schema, size, options)
    
    def generate_data_with_issues(self, schema: Dict, size: int = DEFAULT_DATA_SIZE, 
                                issues_config: Dict = None, options: Dict = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Generates test data with injected quality issues.
        
        Args:
            schema: Schema definition containing field specifications
            size: Number of rows to generate
            issues_config: Configuration of issues to inject
            options: Additional options for data generation
        
        Returns:
            Tuple of (DataFrame with issues, issue details dictionary)
        """
        if issues_config is None:
            issues_config = {}
        
        # Generate clean data first
        df = self.generate_data(schema, size, options)
        
        # Inject issues if configured
        if issues_config:
            df_with_issues, issues_details = inject_data_quality_issues(df, issues_config, schema)
            return df_with_issues, issues_details
        
        # Return clean data with empty issues detail if no issues configured
        return df, {}
    
    def save_data(self, df: pd.DataFrame, file_path: str, file_format: FileFormat, options: Dict = None) -> str:
        """
        Saves generated data to a file.
        
        Args:
            df: DataFrame to save
            file_path: Path where to save the file
            file_format: Format to use
            options: Additional save options
        
        Returns:
            Path to the saved file
        """
        return save_data_to_file(df, file_path, file_format, options)
    
    def load_data(self, file_path: str, file_format: FileFormat, options: Dict = None) -> pd.DataFrame:
        """
        Loads data from a file.
        
        Args:
            file_path: Path to the file to load
            file_format: Format of the file
            options: Additional load options
        
        Returns:
            Loaded data as DataFrame
        """
        return load_data_from_file(file_path, file_format, options)
    
    def generate_large_dataset(self, schema: Dict, total_size: int, batch_size: int = 10000, 
                             options: Dict = None) -> pd.DataFrame:
        """
        Generates a large dataset in batches to manage memory.
        
        Args:
            schema: Schema definition containing field specifications
            total_size: Total number of rows to generate
            batch_size: Number of rows per batch
            options: Additional options for data generation
        
        Returns:
            Large generated dataset
        """
        if options is None:
            options = {}
        
        # Calculate number of full batches and remaining rows
        num_batches = total_size // batch_size
        remaining_rows = total_size % batch_size
        
        # Generate batches
        batches = []
        
        for i in range(num_batches):
            batch_df = generate_data_batch(schema, batch_size, options)
            batches.append(batch_df)
        
        # Generate remaining rows if any
        if remaining_rows > 0:
            remaining_df = generate_data_batch(schema, remaining_rows, options)
            batches.append(remaining_df)
        
        # Combine all batches
        combined_df = pd.concat(batches, ignore_index=True)
        
        return combined_df
    
    def generate_large_file(self, schema: Dict, total_size: int, file_path: str, 
                          file_format: FileFormat, batch_size: int = 10000, 
                          options: Dict = None) -> str:
        """
        Generates a large dataset and saves directly to file in batches.
        
        Args:
            schema: Schema definition containing field specifications
            total_size: Total number of rows to generate
            file_path: Path where to save the file
            file_format: Format to use
            batch_size: Number of rows per batch
            options: Additional options for data generation and saving
        
        Returns:
            Path to the generated file
        """
        if options is None:
            options = {}
        
        # Calculate number of full batches and remaining rows
        num_batches = total_size // batch_size
        remaining_rows = total_size % batch_size
        
        # Initialize file based on format
        if file_format == FileFormat.CSV:
            # Write header first
            header_df = generate_data_batch(schema, 0, options)  # Empty DataFrame with correct columns
            header_options = {**options, 'include_header': True}
            self.save_data(header_df, file_path, file_format, header_options)
            append_mode = 'a'
            batch_options = {**options, 'include_header': False}
        else:
            # For other formats, may need format-specific initialization
            # For now, just use the first batch to create the file
            append_mode = 'w'
            batch_options = options
        
        # Generate and save batches
        for i in range(num_batches):
            batch_df = generate_data_batch(schema, batch_size, options)
            
            if i == 0 and file_format != FileFormat.CSV:
                # First batch creates the file
                self.save_data(batch_df, file_path, file_format, batch_options)
            elif file_format == FileFormat.CSV:
                # Append to CSV
                batch_df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                # Other formats might need different append logic
                # This is a simplified approach - might need format-specific handling
                existing_df = self.load_data(file_path, file_format, options)
                combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
                self.save_data(combined_df, file_path, file_format, batch_options)
        
        # Generate and save remaining rows if any
        if remaining_rows > 0:
            remaining_df = generate_data_batch(schema, remaining_rows, options)
            
            if num_batches == 0:
                # No batches were processed, create the file
                self.save_data(remaining_df, file_path, file_format, batch_options)
            elif file_format == FileFormat.CSV:
                # Append to CSV
                remaining_df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                # Other formats might need different append logic
                existing_df = self.load_data(file_path, file_format, options)
                combined_df = pd.concat([existing_df, remaining_df], ignore_index=True)
                self.save_data(combined_df, file_path, file_format, batch_options)
        
        return file_path