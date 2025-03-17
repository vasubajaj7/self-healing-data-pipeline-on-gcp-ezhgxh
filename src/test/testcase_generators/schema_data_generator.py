"""
Base generator for creating schema and data pairs with various configurations for testing the self-healing data pipeline.

This module provides foundational functionality for generating test data with controlled characteristics,
which is used by other specialized test case generators.
"""

import os
import json
import random
import uuid
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import numpy as np
from faker import Faker

from src.backend.constants import FileFormat, ValidationRuleType, QualityDimension
from src.test.utils.test_helpers import (
    create_temp_directory,
    create_temp_file,
    create_test_dataframe,
    generate_unique_id
)

# Constants
TEST_CASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'testcases')
DEFAULT_NUM_VARIATIONS = 5
DEFAULT_DATA_SIZE = 100
DEFAULT_FILE_FORMAT = FileFormat.JSON

# Supported data types
SUPPORTED_DATA_TYPES = ['string', 'integer', 'float', 'boolean', 'timestamp', 'date', 'array', 'struct']

# Data type generators for each supported type
DATA_TYPE_GENERATORS = {}


def generate_schema(config: Dict) -> Dict:
    """
    Generates a schema definition with specified characteristics.
    
    Args:
        config: Configuration for the schema generation including:
            - num_columns: Number of columns to generate
            - column_prefix: Prefix for column names
            - data_types: List of data types to use
            - include_required: Whether to include required fields
            - include_nullable: Whether to include nullable fields
            - include_unique: Whether to include unique fields
            - include_constraints: Whether to include constraints
            
    Returns:
        Generated schema definition as a dictionary
    """
    # Extract configuration parameters
    num_columns = config.get('num_columns', random.randint(3, 10))
    column_prefix = config.get('column_prefix', 'column')
    data_types = config.get('data_types', SUPPORTED_DATA_TYPES)
    include_required = config.get('include_required', True)
    include_nullable = config.get('include_nullable', True)
    include_unique = config.get('include_unique', True)
    include_constraints = config.get('include_constraints', True)
    
    # Initialize schema dictionary
    schema = {
        'schema_id': config.get('schema_id', f"schema-{uuid.uuid4()}"),
        'schema_name': config.get('schema_name', 'Test Schema'),
        'version': config.get('version', '1.0'),
        'description': config.get('description', 'Schema for testing'),
        'fields': []
    }
    
    # Generate specified number of columns
    for i in range(num_columns):
        # Determine column name
        if 'columns' in config and i < len(config['columns']):
            column_name = config['columns'][i].get('name', f"{column_prefix}_{i+1}")
        else:
            column_name = f"{column_prefix}_{i+1}"
        
        # Determine data type
        if 'columns' in config and i < len(config['columns']):
            data_type = config['columns'][i].get('type', random.choice(data_types))
        else:
            data_type = random.choice(data_types)
        
        # Determine constraints
        constraints = {}
        if include_required and (random.random() > 0.3 or ('columns' in config and i < len(config['columns']) and config['columns'][i].get('required', False))):
            constraints['required'] = True
        
        if include_nullable and (random.random() > 0.7 or ('columns' in config and i < len(config['columns']) and config['columns'][i].get('nullable', False))):
            constraints['nullable'] = True
        
        if include_unique and (random.random() > 0.8 or ('columns' in config and i < len(config['columns']) and config['columns'][i].get('unique', False))):
            constraints['unique'] = True
        
        if include_constraints:
            if data_type == 'string':
                constraints['max_length'] = random.randint(10, 100)
            elif data_type in ['integer', 'float']:
                constraints['min'] = random.randint(-100, 0)
                constraints['max'] = random.randint(1, 1000)
        
        # Add column to schema
        schema['fields'].append(generate_column_schema(column_name, data_type, constraints))
    
    return schema


def generate_data(schema: Dict, config: Dict) -> pd.DataFrame:
    """
    Generates a pandas DataFrame with data conforming to the provided schema.
    
    Args:
        schema: Schema definition
        config: Configuration for data generation including:
            - num_rows: Number of rows to generate
            - null_percentage: Percentage of null values to include
            - error_percentage: Percentage of error values to include
            - random_seed: Seed for random number generation
            
    Returns:
        Generated data as a pandas DataFrame
    """
    # Extract configuration parameters
    num_rows = config.get('num_rows', DEFAULT_DATA_SIZE)
    null_percentage = config.get('null_percentage', 0)
    error_percentage = config.get('error_percentage', 0)
    random_seed = config.get('random_seed')
    
    # Set random seed if provided
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)
    
    # Initialize column specifications for create_test_dataframe
    columns_spec = {}
    
    # Configure columns based on schema
    for field in schema['fields']:
        name = field['name']
        data_type = field['type']
        
        # Basic column specification
        column_spec = {'type': _map_schema_type_to_pandas_type(data_type)}
        
        # Add type-specific configurations
        if data_type == 'string':
            column_spec['length'] = field.get('max_length', 10)
        elif data_type == 'integer':
            column_spec['min'] = field.get('min', -1000)
            column_spec['max'] = field.get('max', 1000)
        elif data_type == 'float':
            column_spec['min'] = field.get('min', -1000.0)
            column_spec['max'] = field.get('max', 1000.0)
        elif data_type == 'boolean':
            pass  # No special config needed
        elif data_type == 'timestamp' or data_type == 'date':
            column_spec['start'] = '2020-01-01'
            column_spec['end'] = '2023-01-01'
        elif data_type == 'array':
            # For simplicity, arrays are represented as strings in tests
            column_spec = {'type': 'str', 'length': 20}
        elif data_type == 'struct':
            # For simplicity, structs are represented as strings in tests
            column_spec = {'type': 'str', 'length': 30}
        
        columns_spec[name] = column_spec
    
    # Generate data using create_test_dataframe helper
    df = create_test_dataframe(columns_spec, num_rows)
    
    # Apply null values if specified
    if null_percentage > 0:
        for col in df.columns:
            null_mask = np.random.random(size=len(df)) < (null_percentage / 100)
            df.loc[null_mask, col] = None
    
    # Apply error values if specified
    if error_percentage > 0:
        for col in df.columns:
            error_mask = np.random.random(size=len(df)) < (error_percentage / 100)
            if df[col].dtype == 'int64':
                df.loc[error_mask, col] = "invalid_integer"
            elif df[col].dtype == 'float64':
                df.loc[error_mask, col] = "invalid_float"
            elif df[col].dtype == 'bool':
                df.loc[error_mask, col] = "invalid_boolean"
            elif pd.api.types.is_datetime64_dtype(df[col]):
                df.loc[error_mask, col] = "invalid_date"
    
    return df


def generate_schema_data_pair(schema_config: Dict, data_config: Dict) -> Tuple[Dict, pd.DataFrame]:
    """
    Generates a matching schema and data pair based on configuration.
    
    Args:
        schema_config: Configuration for schema generation
        data_config: Configuration for data generation
        
    Returns:
        Tuple of (schema, data)
    """
    schema = generate_schema(schema_config)
    data = generate_data(schema, data_config)
    return schema, data


def save_schema(schema: Dict, file_path: str) -> str:
    """
    Saves a schema definition to a JSON file.
    
    Args:
        schema: Schema definition to save
        file_path: Path to save the schema file
        
    Returns:
        Path to the saved schema file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write JSON to file
    with open(file_path, 'w') as f:
        json.dump(schema, f, indent=2)
    
    return file_path


def save_data(data: pd.DataFrame, file_path: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> str:
    """
    Saves a DataFrame to a file in the specified format.
    
    Args:
        data: DataFrame to save
        file_path: Path to save the data file
        file_format: Format to save the data in
        
    Returns:
        Path to the saved data file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Save data in appropriate format
    if file_format == FileFormat.CSV:
        data.to_csv(file_path, index=False)
    elif file_format == FileFormat.JSON:
        data.to_json(file_path, orient='records', date_format='iso')
    elif file_format == FileFormat.PARQUET:
        data.to_parquet(file_path, index=False)
    elif file_format == FileFormat.AVRO:
        # Requires additional library like fastavro
        try:
            import fastavro
            with open(file_path, 'wb') as f:
                schema = {
                    'type': 'record',
                    'name': 'TestData',
                    'fields': [{'name': col, 'type': ['null', 'string']} for col in data.columns]
                }
                fastavro.writer(f, schema, data.to_dict('records'))
        except ImportError:
            raise ImportError("fastavro is required to save data in Avro format")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    return file_path


def load_schema(file_path: str) -> Dict:
    """
    Loads a schema definition from a JSON file.
    
    Args:
        file_path: Path to the schema file
        
    Returns:
        Loaded schema definition
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Schema file not found: {file_path}")
    
    # Read JSON from file
    with open(file_path, 'r') as f:
        schema = json.load(f)
    
    return schema


def load_data(file_path: str, file_format: Optional[FileFormat] = None) -> pd.DataFrame:
    """
    Loads data from a file in the specified format.
    
    Args:
        file_path: Path to the data file
        file_format: Format of the data file
        
    Returns:
        Loaded data as a DataFrame
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    # Determine file format if not specified
    if file_format is None:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.csv':
            file_format = FileFormat.CSV
        elif ext == '.json':
            file_format = FileFormat.JSON
        elif ext == '.parquet':
            file_format = FileFormat.PARQUET
        elif ext == '.avro':
            file_format = FileFormat.AVRO
        else:
            raise ValueError(f"Cannot determine file format from extension: {ext}")
    
    # Load data in appropriate format
    if file_format == FileFormat.CSV:
        return pd.read_csv(file_path)
    elif file_format == FileFormat.JSON:
        return pd.read_json(file_path, orient='records')
    elif file_format == FileFormat.PARQUET:
        return pd.read_parquet(file_path)
    elif file_format == FileFormat.AVRO:
        # Requires additional library like fastavro
        try:
            import fastavro
            with open(file_path, 'rb') as f:
                records = list(fastavro.reader(f))
            return pd.DataFrame.from_records(records)
        except ImportError:
            raise ImportError("fastavro is required to load data from Avro format")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def generate_column_schema(name: str, data_type: str, constraints: Dict) -> Dict:
    """
    Generates schema for a single column with specified characteristics.
    
    Args:
        name: Column name
        data_type: Data type for the column
        constraints: Dictionary of constraints for the column
        
    Returns:
        Column schema definition
    """
    column_schema = {
        'name': name,
        'type': data_type
    }
    
    # Add constraints if provided
    for constraint, value in constraints.items():
        column_schema[constraint] = value
    
    # Add type-specific metadata
    if data_type == 'string':
        if 'max_length' not in column_schema:
            column_schema['max_length'] = 100
    elif data_type == 'integer':
        if 'min' not in column_schema:
            column_schema['min'] = -1000
        if 'max' not in column_schema:
            column_schema['max'] = 1000
    elif data_type == 'float':
        if 'min' not in column_schema:
            column_schema['min'] = -1000.0
        if 'max' not in column_schema:
            column_schema['max'] = 1000.0
    elif data_type == 'timestamp':
        if 'format' not in column_schema:
            column_schema['format'] = 'yyyy-MM-dd HH:mm:ss'
    elif data_type == 'date':
        if 'format' not in column_schema:
            column_schema['format'] = 'yyyy-MM-dd'
    elif data_type == 'array':
        if 'items' not in column_schema:
            column_schema['items'] = {'type': 'string'}
    elif data_type == 'struct':
        if 'fields' not in column_schema:
            column_schema['fields'] = [
                {'name': 'nested_field_1', 'type': 'string'},
                {'name': 'nested_field_2', 'type': 'integer'}
            ]
    
    return column_schema


def generate_quality_issue_test_cases(base_schema: Dict, base_data: pd.DataFrame, 
                                     issue_types: List[str], num_variations: int = DEFAULT_NUM_VARIATIONS) -> List[Dict]:
    """
    Generates test cases with various quality issues for testing.
    
    Args:
        base_schema: Base schema definition
        base_data: Base data without issues
        issue_types: Types of issues to generate
        num_variations: Number of variations to generate for each issue type
        
    Returns:
        List of test cases with quality issues
    """
    test_cases = []
    
    for issue_type in issue_types:
        for i in range(num_variations):
            # Create a copy of base data
            data_with_issue = base_data.copy()
            
            # Apply issue based on type
            if issue_type == 'missing_values':
                # Randomly select columns and introduce null values
                for col in random.sample(list(data_with_issue.columns), k=random.randint(1, len(data_with_issue.columns))):
                    null_mask = np.random.random(size=len(data_with_issue)) < random.uniform(0.1, 0.5)
                    data_with_issue.loc[null_mask, col] = None
            
            elif issue_type == 'type_mismatch':
                # Randomly select columns and introduce type mismatches
                for col in random.sample(list(data_with_issue.columns), k=random.randint(1, len(data_with_issue.columns))):
                    error_mask = np.random.random(size=len(data_with_issue)) < random.uniform(0.1, 0.3)
                    data_with_issue.loc[error_mask, col] = "INVALID_TYPE"
            
            elif issue_type == 'out_of_range':
                # Find numeric columns and introduce out-of-range values
                numeric_cols = data_with_issue.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    for col in random.sample(list(numeric_cols), k=min(random.randint(1, 3), len(numeric_cols))):
                        error_mask = np.random.random(size=len(data_with_issue)) < random.uniform(0.1, 0.3)
                        field_def = next((f for f in base_schema['fields'] if f['name'] == col), None)
                        if field_def:
                            if field_def.get('type') == 'integer':
                                if 'max' in field_def:
                                    data_with_issue.loc[error_mask, col] = field_def['max'] + random.randint(1, 1000)
                            elif field_def.get('type') == 'float':
                                if 'max' in field_def:
                                    data_with_issue.loc[error_mask, col] = field_def['max'] + random.uniform(1, 1000)
            
            elif issue_type == 'duplicates':
                # Duplicate random rows
                if len(data_with_issue) > 1:
                    dup_count = random.randint(1, min(10, len(data_with_issue) // 2))
                    for _ in range(dup_count):
                        row_idx = random.randint(0, len(data_with_issue) - 1)
                        data_with_issue = pd.concat([data_with_issue, data_with_issue.iloc[[row_idx]]])
            
            elif issue_type == 'format_error':
                # Find string columns and introduce format errors
                string_cols = data_with_issue.select_dtypes(include=['object']).columns
                if len(string_cols) > 0:
                    for col in random.sample(list(string_cols), k=min(random.randint(1, 3), len(string_cols))):
                        error_mask = np.random.random(size=len(data_with_issue)) < random.uniform(0.1, 0.3)
                        data_with_issue.loc[error_mask, col] = "###INVALID_FORMAT###"
            
            # Create test case
            test_case = {
                'name': f"{issue_type}_variation_{i+1}",
                'issue_type': issue_type,
                'description': f"Test case with {issue_type} issues (variation {i+1})",
                'schema': base_schema,
                'clean_data': base_data,
                'data_with_issues': data_with_issue
            }
            
            test_cases.append(test_case)
    
    return test_cases


def save_test_case(schema: Dict, data: pd.DataFrame, test_case_name: str, 
                  output_dir: str = TEST_CASE_DIR, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
    """
    Saves a schema-data test case to files.
    
    Args:
        schema: Schema definition
        data: DataFrame containing the data
        test_case_name: Name for the test case
        output_dir: Directory to save the test case
        file_format: Format to save the data in
        
    Returns:
        Dictionary with paths to saved files
    """
    # Create output directory if it doesn't exist
    test_case_dir = os.path.join(output_dir, test_case_name)
    os.makedirs(test_case_dir, exist_ok=True)
    
    # Save schema to JSON file
    schema_file = os.path.join(test_case_dir, 'schema.json')
    schema_path = save_schema(schema, schema_file)
    
    # Determine file extension for data
    ext = '.json'
    if file_format == FileFormat.CSV:
        ext = '.csv'
    elif file_format == FileFormat.PARQUET:
        ext = '.parquet'
    elif file_format == FileFormat.AVRO:
        ext = '.avro'
    
    # Save data to file
    data_file = os.path.join(test_case_dir, f'data{ext}')
    data_path = save_data(data, data_file, file_format)
    
    return {
        'test_case_name': test_case_name,
        'schema_path': schema_path,
        'data_path': data_path
    }


def load_test_case(test_case_name: str, input_dir: str = TEST_CASE_DIR, 
                 file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
    """
    Loads a previously saved schema-data test case.
    
    Args:
        test_case_name: Name of the test case
        input_dir: Directory containing the test case
        file_format: Format of the data file
        
    Returns:
        Dictionary with loaded schema and data
    """
    # Construct paths to test case files
    test_case_dir = os.path.join(input_dir, test_case_name)
    schema_file = os.path.join(test_case_dir, 'schema.json')
    
    # Determine file extension for data
    ext = '.json'
    if file_format == FileFormat.CSV:
        ext = '.csv'
    elif file_format == FileFormat.PARQUET:
        ext = '.parquet'
    elif file_format == FileFormat.AVRO:
        ext = '.avro'
    
    data_file = os.path.join(test_case_dir, f'data{ext}')
    
    # Load schema and data
    schema = load_schema(schema_file)
    data = load_data(data_file, file_format)
    
    return {
        'test_case_name': test_case_name,
        'schema': schema,
        'data': data,
        'schema_path': schema_file,
        'data_path': data_file
    }


def _map_schema_type_to_pandas_type(schema_type: str) -> str:
    """
    Maps schema data types to pandas data types.
    
    Args:
        schema_type: Schema data type
        
    Returns:
        Pandas equivalent data type
    """
    type_mapping = {
        'string': 'str',
        'integer': 'int',
        'float': 'float',
        'boolean': 'bool',
        'timestamp': 'datetime',
        'date': 'datetime',
        'array': 'str',  # Simplified for testing
        'struct': 'str'   # Simplified for testing
    }
    
    return type_mapping.get(schema_type, 'str')


class DataTypeGenerator:
    """Base class for type-specific data generators."""
    
    def generate_schema(self, config: Dict) -> Dict:
        """
        Generate schema for this data type.
        
        Args:
            config: Configuration for schema generation
            
        Returns:
            Schema definition for this type
        """
        raise NotImplementedError("Subclasses must implement generate_schema")
    
    def generate_data(self, schema: Dict, num_rows: int, config: Dict) -> List:
        """
        Generate data for this data type.
        
        Args:
            schema: Schema definition for this type
            num_rows: Number of rows to generate
            config: Configuration for data generation
            
        Returns:
            List of generated values
        """
        raise NotImplementedError("Subclasses must implement generate_data")


class SchemaDataTestCase:
    """Class representing a test case with schema and data."""
    
    def __init__(self, schema: Dict, data: pd.DataFrame, metadata: Dict = None):
        """
        Initialize a SchemaDataTestCase.
        
        Args:
            schema: Schema definition
            data: DataFrame containing the data
            metadata: Additional metadata about the test case
        """
        self.schema = schema
        self.data = data
        self.metadata = metadata or {}
        self.file_paths = {}
    
    def save(self, test_case_name: str, output_dir: str = TEST_CASE_DIR, 
             file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Save the test case to files.
        
        Args:
            test_case_name: Name for the test case
            output_dir: Directory to save the test case
            file_format: Format to save the data in
            
        Returns:
            Dictionary with paths to saved files
        """
        file_paths = save_test_case(self.schema, self.data, test_case_name, output_dir, file_format)
        self.file_paths = file_paths
        return file_paths
    
    def to_dict(self) -> Dict:
        """
        Convert the test case to a dictionary representation.
        
        Returns:
            Dictionary representation of the test case
        """
        return {
            'schema': self.schema,
            'data': self.data.to_dict(orient='records'),
            'metadata': self.metadata,
            'file_paths': self.file_paths
        }
    
    @classmethod
    def from_dict(cls, test_case_dict: Dict) -> 'SchemaDataTestCase':
        """
        Create a SchemaDataTestCase from a dictionary.
        
        Args:
            test_case_dict: Dictionary representation of a test case
            
        Returns:
            SchemaDataTestCase instance
        """
        schema = test_case_dict['schema']
        data = pd.DataFrame(test_case_dict['data'])
        metadata = test_case_dict.get('metadata', {})
        
        test_case = cls(schema, data, metadata)
        
        if 'file_paths' in test_case_dict:
            test_case.file_paths = test_case_dict['file_paths']
        
        return test_case
    
    @classmethod
    def load(cls, test_case_name: str, input_dir: str = TEST_CASE_DIR, 
             file_format: FileFormat = DEFAULT_FILE_FORMAT) -> 'SchemaDataTestCase':
        """
        Load a test case from files.
        
        Args:
            test_case_name: Name of the test case
            input_dir: Directory containing the test case
            file_format: Format of the data file
            
        Returns:
            SchemaDataTestCase instance
        """
        test_case_data = load_test_case(test_case_name, input_dir, file_format)
        
        test_case = cls(
            schema=test_case_data['schema'],
            data=test_case_data['data']
        )
        
        test_case.file_paths = {
            'test_case_name': test_case_name,
            'schema_path': test_case_data['schema_path'],
            'data_path': test_case_data['data_path']
        }
        
        return test_case


class TestCaseGenerator:
    """Base class for generating test cases with schema and data pairs."""
    
    def __init__(self, output_dir: str = TEST_CASE_DIR):
        """
        Initialize the TestCaseGenerator.
        
        Args:
            output_dir: Directory to save generated test cases
        """
        # Initialize schema generators for different data types
        self._schema_generators = {}
        
        # Initialize data generators for different data types
        self._data_generators = {}
        
        # Set output directory
        self._output_dir = output_dir
    
    def generate_basic_test_case(self, schema_config: Dict, data_config: Dict, 
                               test_case_name: str, save_files: bool = True) -> Dict:
        """
        Generates a basic test case with schema and data.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema and data
        """
        # Generate schema and data
        schema, data = generate_schema_data_pair(schema_config, data_config)
        
        # Create SchemaDataTestCase
        test_case = SchemaDataTestCase(schema, data, {
            'test_case_name': test_case_name,
            'test_case_type': 'basic',
            'schema_config': schema_config,
            'data_config': data_config
        })
        
        # Save test case if requested
        if save_files:
            file_format = data_config.get('file_format', DEFAULT_FILE_FORMAT)
            test_case.save(test_case_name, self._output_dir, file_format)
        
        return test_case.to_dict()
    
    def generate_schema_evolution_test_case(self, original_schema_config: Dict, evolved_schema_config: Dict, 
                                         data_config: Dict, test_case_name: str, save_files: bool = True) -> Dict:
        """
        Generates a test case for schema evolution testing.
        
        Args:
            original_schema_config: Configuration for original schema
            evolved_schema_config: Configuration for evolved schema
            data_config: Configuration for data generation
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with original and evolved schemas and data
        """
        # Generate original schema and data
        original_schema, original_data = generate_schema_data_pair(original_schema_config, data_config)
        
        # Ensure evolved schema includes original fields plus changes
        if 'based_on_original' in evolved_schema_config and evolved_schema_config['based_on_original']:
            # Start with a copy of the original schema
            evolved_schema = original_schema.copy()
            
            # Apply evolution changes
            if 'add_fields' in evolved_schema_config:
                for field in evolved_schema_config['add_fields']:
                    evolved_schema['fields'].append(generate_column_schema(
                        field['name'], field['type'], field.get('constraints', {})
                    ))
            
            if 'remove_fields' in evolved_schema_config:
                evolved_schema['fields'] = [
                    f for f in evolved_schema['fields'] 
                    if f['name'] not in evolved_schema_config['remove_fields']
                ]
            
            if 'modify_fields' in evolved_schema_config:
                for mod in evolved_schema_config['modify_fields']:
                    field_idx = next((i for i, f in enumerate(evolved_schema['fields']) if f['name'] == mod['name']), None)
                    if field_idx is not None:
                        if 'type' in mod:
                            evolved_schema['fields'][field_idx]['type'] = mod['type']
                        if 'constraints' in mod:
                            for k, v in mod['constraints'].items():
                                evolved_schema['fields'][field_idx][k] = v
            
            # Update schema version
            evolved_schema['version'] = str(float(evolved_schema.get('version', '1.0')) + 0.1)
        else:
            # Generate completely new schema
            evolved_schema = generate_schema(evolved_schema_config)
        
        # Generate data for evolved schema
        evolved_data = generate_data(evolved_schema, data_config)
        
        # Create test case dictionary
        test_case = {
            'test_case_name': test_case_name,
            'test_case_type': 'schema_evolution',
            'original_schema': original_schema,
            'evolved_schema': evolved_schema,
            'original_data': original_data.to_dict(orient='records'),
            'evolved_data': evolved_data.to_dict(orient='records'),
            'schema_config': {
                'original': original_schema_config,
                'evolved': evolved_schema_config
            },
            'data_config': data_config
        }
        
        # Save test case if requested
        if save_files:
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            file_format = data_config.get('file_format', DEFAULT_FILE_FORMAT)
            
            # Save original schema and data
            original_schema_path = save_schema(original_schema, os.path.join(test_case_dir, 'original_schema.json'))
            original_data_path = save_data(original_data, os.path.join(test_case_dir, 'original_data'), file_format)
            
            # Save evolved schema and data
            evolved_schema_path = save_schema(evolved_schema, os.path.join(test_case_dir, 'evolved_schema.json'))
            evolved_data_path = save_data(evolved_data, os.path.join(test_case_dir, 'evolved_data'), file_format)
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'original_schema_path': original_schema_path,
                'original_data_path': original_data_path,
                'evolved_schema_path': evolved_schema_path,
                'evolved_data_path': evolved_data_path
            }
        
        return test_case
    
    def generate_data_quality_test_case(self, schema_config: Dict, data_config: Dict, 
                                     quality_issues: List[str], test_case_name: str, 
                                     save_files: bool = True) -> Dict:
        """
        Generates a test case for data quality testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            quality_issues: List of quality issue types to include
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, clean data, and data with quality issues
        """
        # Generate schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Generate test cases with quality issues
        issue_test_cases = generate_quality_issue_test_cases(
            schema, clean_data, quality_issues, data_config.get('num_variations', DEFAULT_NUM_VARIATIONS)
        )
        
        # Create test case dictionary
        test_case = {
            'test_case_name': test_case_name,
            'test_case_type': 'data_quality',
            'schema': schema,
            'clean_data': clean_data.to_dict(orient='records'),
            'issue_test_cases': issue_test_cases,
            'schema_config': schema_config,
            'data_config': data_config,
            'quality_issues': quality_issues
        }
        
        # Save test case if requested
        if save_files:
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            file_format = data_config.get('file_format', DEFAULT_FILE_FORMAT)
            
            # Save schema
            schema_path = save_schema(schema, os.path.join(test_case_dir, 'schema.json'))
            
            # Save clean data
            clean_data_path = save_data(clean_data, os.path.join(test_case_dir, 'clean_data'), file_format)
            
            # Save data with issues
            issue_data_paths = {}
            for idx, issue_case in enumerate(issue_test_cases):
                issue_name = issue_case['name']
                issue_data = pd.DataFrame(issue_case['data_with_issues'])
                issue_path = save_data(
                    issue_data, 
                    os.path.join(test_case_dir, f'issue_{idx+1}_{issue_name}'), 
                    file_format
                )
                issue_data_paths[issue_name] = issue_path
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'schema_path': schema_path,
                'clean_data_path': clean_data_path,
                'issue_data_paths': issue_data_paths
            }
        
        return test_case
    
    def generate_self_healing_test_case(self, schema_config: Dict, data_config: Dict, 
                                     healing_issues: List[Dict], test_case_name: str, 
                                     save_files: bool = True) -> Dict:
        """
        Generates a test case for self-healing testing.
        
        Args:
            schema_config: Configuration for schema generation
            data_config: Configuration for data generation
            healing_issues: List of healing issues to include
            test_case_name: Name for the test case
            save_files: Whether to save the test case to files
            
        Returns:
            Test case with schema, clean data, corrupted data, and expected corrections
        """
        # Generate schema and clean data
        schema, clean_data = generate_schema_data_pair(schema_config, data_config)
        
        # Apply issues to create corrupted data
        corrupted_data = clean_data.copy()
        corrections = []
        
        for issue in healing_issues:
            issue_type = issue['type']
            issue_config = issue.get('config', {})
            
            if issue_type == 'missing_values':
                # Apply missing values issue
                columns = issue_config.get('columns', random.sample(list(corrupted_data.columns), k=random.randint(1, len(corrupted_data.columns))))
                percentage = issue_config.get('percentage', random.uniform(0.1, 0.3))
                
                for col in columns:
                    null_mask = np.random.random(size=len(corrupted_data)) < percentage
                    corrupted_data.loc[null_mask, col] = None
                
                # Create correction
                for col in columns:
                    corrections.append({
                        'issue_type': 'missing_values',
                        'column': col,
                        'correction_type': 'imputation',
                        'correction_details': {
                            'method': 'statistical',
                            'strategy': 'mean' if corrupted_data[col].dtype in [np.float64, np.int64] else 'most_frequent'
                        }
                    })
            
            elif issue_type == 'invalid_format':
                # Apply invalid format issue
                columns = issue_config.get('columns', random.sample(list(corrupted_data.select_dtypes(include=['object']).columns), k=random.randint(1, len(corrupted_data.select_dtypes(include=['object']).columns))))
                percentage = issue_config.get('percentage', random.uniform(0.1, 0.3))
                
                for col in columns:
                    error_mask = np.random.random(size=len(corrupted_data)) < percentage
                    corrupted_data.loc[error_mask, col] = "###INVALID_FORMAT###"
                
                # Create correction
                for col in columns:
                    corrections.append({
                        'issue_type': 'invalid_format',
                        'column': col,
                        'correction_type': 'format_correction',
                        'correction_details': {
                            'method': 'pattern_based',
                            'pattern': issue_config.get('expected_pattern', 'standard')
                        }
                    })
            
            elif issue_type == 'outliers':
                # Apply outliers issue
                numeric_cols = corrupted_data.select_dtypes(include=['number']).columns
                columns = issue_config.get('columns', random.sample(list(numeric_cols), k=min(random.randint(1, 3), len(numeric_cols))))
                percentage = issue_config.get('percentage', random.uniform(0.05, 0.15))
                
                for col in columns:
                    error_mask = np.random.random(size=len(corrupted_data)) < percentage
                    # Get field definition
                    field_def = next((f for f in schema['fields'] if f['name'] == col), None)
                    if field_def:
                        if 'max' in field_def:
                            multiplier = random.uniform(1.5, 10.0)
                            corrupted_data.loc[error_mask, col] = corrupted_data.loc[error_mask, col] * multiplier
                
                # Create correction
                for col in columns:
                    corrections.append({
                        'issue_type': 'outliers',
                        'column': col,
                        'correction_type': 'outlier_treatment',
                        'correction_details': {
                            'method': 'statistical',
                            'strategy': 'clip' if random.random() > 0.5 else 'remove'
                        }
                    })
        
        # Create test case dictionary
        test_case = {
            'test_case_name': test_case_name,
            'test_case_type': 'self_healing',
            'schema': schema,
            'clean_data': clean_data.to_dict(orient='records'),
            'corrupted_data': corrupted_data.to_dict(orient='records'),
            'expected_corrections': corrections,
            'schema_config': schema_config,
            'data_config': data_config,
            'healing_issues': healing_issues
        }
        
        # Save test case if requested
        if save_files:
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            file_format = data_config.get('file_format', DEFAULT_FILE_FORMAT)
            
            # Save schema
            schema_path = save_schema(schema, os.path.join(test_case_dir, 'schema.json'))
            
            # Save clean data
            clean_data_path = save_data(clean_data, os.path.join(test_case_dir, 'clean_data'), file_format)
            
            # Save corrupted data
            corrupted_data_path = save_data(corrupted_data, os.path.join(test_case_dir, 'corrupted_data'), file_format)
            
            # Save expected corrections
            corrections_path = os.path.join(test_case_dir, 'expected_corrections.json')
            with open(corrections_path, 'w') as f:
                json.dump(corrections, f, indent=2)
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'schema_path': schema_path,
                'clean_data_path': clean_data_path,
                'corrupted_data_path': corrupted_data_path,
                'corrections_path': corrections_path
            }
        
        return test_case
    
    def generate_comprehensive_test_suite(self, suite_config: Dict, suite_name: str, 
                                       save_files: bool = True) -> Dict:
        """
        Generates a comprehensive test suite with multiple test cases.
        
        Args:
            suite_config: Configuration for the test suite
            suite_name: Name for the test suite
            save_files: Whether to save the test cases to files
            
        Returns:
            Complete test suite with multiple test cases
        """
        # Create output directory for test suite
        suite_dir = os.path.join(self._output_dir, suite_name)
        os.makedirs(suite_dir, exist_ok=True)
        
        suite_results = {
            'suite_name': suite_name,
            'test_cases': {},
            'manifest': {
                'basic_test_cases': [],
                'schema_evolution_test_cases': [],
                'data_quality_test_cases': [],
                'self_healing_test_cases': []
            }
        }
        
        # Generate basic test cases
        if 'basic_test_cases' in suite_config:
            for tc_config in suite_config['basic_test_cases']:
                tc_name = tc_config.get('name', f"basic_{generate_unique_id()}")
                tc_output_dir = os.path.join(suite_dir, tc_name)
                
                test_case = self.generate_basic_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['basic_test_cases'].append(tc_name)
        
        # Generate schema evolution test cases
        if 'schema_evolution_test_cases' in suite_config:
            for tc_config in suite_config['schema_evolution_test_cases']:
                tc_name = tc_config.get('name', f"schema_evolution_{generate_unique_id()}")
                tc_output_dir = os.path.join(suite_dir, tc_name)
                
                test_case = self.generate_schema_evolution_test_case(
                    tc_config.get('original_schema_config', {}),
                    tc_config.get('evolved_schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['schema_evolution_test_cases'].append(tc_name)
        
        # Generate data quality test cases
        if 'data_quality_test_cases' in suite_config:
            for tc_config in suite_config['data_quality_test_cases']:
                tc_name = tc_config.get('name', f"data_quality_{generate_unique_id()}")
                tc_output_dir = os.path.join(suite_dir, tc_name)
                
                test_case = self.generate_data_quality_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('quality_issues', ['missing_values', 'type_mismatch']),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['data_quality_test_cases'].append(tc_name)
        
        # Generate self-healing test cases
        if 'self_healing_test_cases' in suite_config:
            for tc_config in suite_config['self_healing_test_cases']:
                tc_name = tc_config.get('name', f"self_healing_{generate_unique_id()}")
                tc_output_dir = os.path.join(suite_dir, tc_name)
                
                test_case = self.generate_self_healing_test_case(
                    tc_config.get('schema_config', {}),
                    tc_config.get('data_config', {}),
                    tc_config.get('healing_issues', [{'type': 'missing_values'}]),
                    tc_name,
                    save_files
                )
                
                suite_results['test_cases'][tc_name] = test_case
                suite_results['manifest']['self_healing_test_cases'].append(tc_name)
        
        # Generate test suite manifest
        if save_files:
            manifest_path = os.path.join(suite_dir, 'manifest.json')
            with open(manifest_path, 'w') as f:
                json.dump(suite_results['manifest'], f, indent=2)
            
            suite_results['manifest_path'] = manifest_path
        
        return suite_results
    
    def save_test_case(self, test_case: Dict, test_case_name: str, 
                     file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Saves a test case to files.
        
        Args:
            test_case: Test case to save
            test_case_name: Name for the test case
            file_format: Format to save the data in
            
        Returns:
            Updated test case with file paths
        """
        # Determine test case type and save appropriately
        test_case_type = test_case.get('test_case_type', 'basic')
        
        if test_case_type == 'basic':
            schema = test_case['schema']
            data = pd.DataFrame(test_case['data']) if isinstance(test_case['data'], list) else test_case['data']
            
            file_paths = save_test_case(schema, data, test_case_name, self._output_dir, file_format)
            test_case['file_paths'] = file_paths
        
        elif test_case_type == 'schema_evolution':
            # Create test case directory
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            # Save original schema and data
            original_schema = test_case['original_schema']
            original_data = pd.DataFrame(test_case['original_data']) if isinstance(test_case['original_data'], list) else test_case['original_data']
            
            original_schema_path = save_schema(original_schema, os.path.join(test_case_dir, 'original_schema.json'))
            original_data_path = save_data(original_data, os.path.join(test_case_dir, 'original_data'), file_format)
            
            # Save evolved schema and data
            evolved_schema = test_case['evolved_schema']
            evolved_data = pd.DataFrame(test_case['evolved_data']) if isinstance(test_case['evolved_data'], list) else test_case['evolved_data']
            
            evolved_schema_path = save_schema(evolved_schema, os.path.join(test_case_dir, 'evolved_schema.json'))
            evolved_data_path = save_data(evolved_data, os.path.join(test_case_dir, 'evolved_data'), file_format)
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'original_schema_path': original_schema_path,
                'original_data_path': original_data_path,
                'evolved_schema_path': evolved_schema_path,
                'evolved_data_path': evolved_data_path
            }
        
        elif test_case_type == 'data_quality':
            # Create test case directory
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            # Save schema
            schema = test_case['schema']
            schema_path = save_schema(schema, os.path.join(test_case_dir, 'schema.json'))
            
            # Save clean data
            clean_data = pd.DataFrame(test_case['clean_data']) if isinstance(test_case['clean_data'], list) else test_case['clean_data']
            clean_data_path = save_data(clean_data, os.path.join(test_case_dir, 'clean_data'), file_format)
            
            # Save data with issues
            issue_data_paths = {}
            for idx, issue_case in enumerate(test_case['issue_test_cases']):
                issue_name = issue_case['name']
                issue_data = pd.DataFrame(issue_case['data_with_issues']) if isinstance(issue_case['data_with_issues'], list) else issue_case['data_with_issues']
                issue_path = save_data(
                    issue_data, 
                    os.path.join(test_case_dir, f'issue_{idx+1}_{issue_name}'), 
                    file_format
                )
                issue_data_paths[issue_name] = issue_path
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'schema_path': schema_path,
                'clean_data_path': clean_data_path,
                'issue_data_paths': issue_data_paths
            }
        
        elif test_case_type == 'self_healing':
            # Create test case directory
            test_case_dir = os.path.join(self._output_dir, test_case_name)
            os.makedirs(test_case_dir, exist_ok=True)
            
            # Save schema
            schema = test_case['schema']
            schema_path = save_schema(schema, os.path.join(test_case_dir, 'schema.json'))
            
            # Save clean data
            clean_data = pd.DataFrame(test_case['clean_data']) if isinstance(test_case['clean_data'], list) else test_case['clean_data']
            clean_data_path = save_data(clean_data, os.path.join(test_case_dir, 'clean_data'), file_format)
            
            # Save corrupted data
            corrupted_data = pd.DataFrame(test_case['corrupted_data']) if isinstance(test_case['corrupted_data'], list) else test_case['corrupted_data']
            corrupted_data_path = save_data(corrupted_data, os.path.join(test_case_dir, 'corrupted_data'), file_format)
            
            # Save expected corrections
            corrections_path = os.path.join(test_case_dir, 'expected_corrections.json')
            with open(corrections_path, 'w') as f:
                json.dump(test_case['expected_corrections'], f, indent=2)
            
            test_case['file_paths'] = {
                'test_case_name': test_case_name,
                'schema_path': schema_path,
                'clean_data_path': clean_data_path,
                'corrupted_data_path': corrupted_data_path,
                'corrections_path': corrections_path
            }
        
        return test_case
    
    def load_test_case(self, test_case_name: str, file_format: FileFormat = DEFAULT_FILE_FORMAT) -> Dict:
        """
        Loads a previously saved test case.
        
        Args:
            test_case_name: Name of the test case
            file_format: Format of the data file
            
        Returns:
            Loaded test case
        """
        # Determine test case type based on files
        test_case_dir = os.path.join(self._output_dir, test_case_name)
        
        # Check if this is a schema evolution test case
        if os.path.exists(os.path.join(test_case_dir, 'original_schema.json')):
            # Schema evolution test case
            original_schema = load_schema(os.path.join(test_case_dir, 'original_schema.json'))
            evolved_schema = load_schema(os.path.join(test_case_dir, 'evolved_schema.json'))
            
            original_data = load_data(os.path.join(test_case_dir, 'original_data'), file_format)
            evolved_data = load_data(os.path.join(test_case_dir, 'evolved_data'), file_format)
            
            return {
                'test_case_name': test_case_name,
                'test_case_type': 'schema_evolution',
                'original_schema': original_schema,
                'evolved_schema': evolved_schema,
                'original_data': original_data,
                'evolved_data': evolved_data,
                'file_paths': {
                    'test_case_name': test_case_name,
                    'original_schema_path': os.path.join(test_case_dir, 'original_schema.json'),
                    'original_data_path': os.path.join(test_case_dir, 'original_data'),
                    'evolved_schema_path': os.path.join(test_case_dir, 'evolved_schema.json'),
                    'evolved_data_path': os.path.join(test_case_dir, 'evolved_data')
                }
            }
        
        # Check if this is a self-healing test case
        elif os.path.exists(os.path.join(test_case_dir, 'corrupted_data')):
            # Self-healing test case
            schema = load_schema(os.path.join(test_case_dir, 'schema.json'))
            clean_data = load_data(os.path.join(test_case_dir, 'clean_data'), file_format)
            corrupted_data = load_data(os.path.join(test_case_dir, 'corrupted_data'), file_format)
            
            # Load expected corrections
            corrections_path = os.path.join(test_case_dir, 'expected_corrections.json')
            with open(corrections_path, 'r') as f:
                expected_corrections = json.load(f)
            
            return {
                'test_case_name': test_case_name,
                'test_case_type': 'self_healing',
                'schema': schema,
                'clean_data': clean_data,
                'corrupted_data': corrupted_data,
                'expected_corrections': expected_corrections,
                'file_paths': {
                    'test_case_name': test_case_name,
                    'schema_path': os.path.join(test_case_dir, 'schema.json'),
                    'clean_data_path': os.path.join(test_case_dir, 'clean_data'),
                    'corrupted_data_path': os.path.join(test_case_dir, 'corrupted_data'),
                    'corrections_path': corrections_path
                }
            }
        
        # Check if this is a data quality test case (look for issue data files)
        elif len([f for f in os.listdir(test_case_dir) if f.startswith('issue_')]) > 0:
            # Data quality test case
            schema = load_schema(os.path.join(test_case_dir, 'schema.json'))
            clean_data = load_data(os.path.join(test_case_dir, 'clean_data'), file_format)
            
            # Find and load issue data files
            issue_files = [f for f in os.listdir(test_case_dir) if f.startswith('issue_')]
            issue_test_cases = []
            issue_data_paths = {}
            
            for issue_file in issue_files:
                # Extract issue name from filename (issue_1_missing_values_variation_1 -> missing_values_variation_1)
                parts = issue_file.split('_')
                if len(parts) >= 3:
                    issue_name = '_'.join(parts[2:])
                else:
                    issue_name = issue_file
                
                # Load issue data
                issue_data = load_data(os.path.join(test_case_dir, issue_file), file_format)
                
                # Create issue test case
                issue_test_cases.append({
                    'name': issue_name,
                    'issue_type': parts[2] if len(parts) >= 3 else 'unknown',
                    'data_with_issues': issue_data
                })
                
                issue_data_paths[issue_name] = os.path.join(test_case_dir, issue_file)
            
            return {
                'test_case_name': test_case_name,
                'test_case_type': 'data_quality',
                'schema': schema,
                'clean_data': clean_data,
                'issue_test_cases': issue_test_cases,
                'file_paths': {
                    'test_case_name': test_case_name,
                    'schema_path': os.path.join(test_case_dir, 'schema.json'),
                    'clean_data_path': os.path.join(test_case_dir, 'clean_data'),
                    'issue_data_paths': issue_data_paths
                }
            }
        
        # Otherwise, assume it's a basic test case
        else:
            return load_test_case(test_case_name, self._output_dir, file_format)