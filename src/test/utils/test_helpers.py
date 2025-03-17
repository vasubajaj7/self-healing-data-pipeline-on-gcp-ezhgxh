"""
Provides general utility functions and classes for testing the self-healing data pipeline.

This module contains helpers for test data generation, temporary resource management,
structure comparison, and mock response building that are used across different test modules.
"""

import pytest
from unittest.mock import MagicMock, patch
import typing
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import os
import tempfile
import shutil
import uuid
import json
import datetime
import random
import string
import pandas as pd
import numpy as np

# Global lists to track temporary resources for cleanup
TEMP_FILES = []
TEMP_DIRS = []


def create_temp_file(content: str = "", suffix: str = None, prefix: str = None, dir_path: str = None) -> str:
    """
    Creates a temporary file for testing that will be automatically cleaned up.
    
    Args:
        content: Optional content to write to the file
        suffix: Optional suffix for the filename
        prefix: Optional prefix for the filename
        dir_path: Optional directory path where the file should be created
        
    Returns:
        Path to the created temporary file
    """
    with tempfile.NamedTemporaryFile(mode='w+', suffix=suffix, prefix=prefix, dir=dir_path, delete=False) as temp_file:
        if content:
            temp_file.write(content)
        temp_file_path = temp_file.name
        
    # Add to global list for cleanup
    TEMP_FILES.append(temp_file_path)
    return temp_file_path


def create_temp_directory(suffix: str = None, prefix: str = None, dir_path: str = None) -> str:
    """
    Creates a temporary directory for testing that will be automatically cleaned up.
    
    Args:
        suffix: Optional suffix for the directory name
        prefix: Optional prefix for the directory name
        dir_path: Optional parent directory path
        
    Returns:
        Path to the created temporary directory
    """
    temp_dir = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir_path)
    
    # Add to global list for cleanup
    TEMP_DIRS.append(temp_dir)
    return temp_dir


def cleanup_temp_resources() -> None:
    """
    Cleans up all temporary files and directories created during tests.
    """
    # Clean up files
    for file_path in TEMP_FILES:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error cleaning up temporary file {file_path}: {str(e)}")
    
    # Clean up directories
    for dir_path in TEMP_DIRS:
        try:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        except Exception as e:
            print(f"Error cleaning up temporary directory {dir_path}: {str(e)}")
    
    # Clear the lists
    TEMP_FILES.clear()
    TEMP_DIRS.clear()


def create_test_dataframe(columns_spec: Dict, num_rows: int = 100) -> pd.DataFrame:
    """
    Creates a pandas DataFrame with random test data based on specifications.
    
    Args:
        columns_spec: Dictionary mapping column names to their specifications
            Each specification should be a dictionary with 'type' and other relevant parameters
            Supported types: 'int', 'float', 'str', 'bool', 'datetime', 'category'
        num_rows: Number of rows to generate
        
    Returns:
        DataFrame with generated test data
        
    Example:
        columns_spec = {
            'id': {'type': 'int', 'min': 1, 'max': 1000},
            'name': {'type': 'str', 'length': 10},
            'score': {'type': 'float', 'min': 0, 'max': 100},
            'active': {'type': 'bool'},
            'created_at': {'type': 'datetime', 'start': '2020-01-01', 'end': '2023-01-01'},
            'category': {'type': 'category', 'categories': ['A', 'B', 'C']}
        }
    """
    data = {}
    
    for column_name, spec in columns_spec.items():
        col_type = spec.get('type', 'str')
        
        if col_type == 'int':
            min_val = spec.get('min', 0)
            max_val = spec.get('max', 100)
            data[column_name] = [random.randint(min_val, max_val) for _ in range(num_rows)]
            
        elif col_type == 'float':
            min_val = spec.get('min', 0.0)
            max_val = spec.get('max', 1.0)
            data[column_name] = [random.uniform(min_val, max_val) for _ in range(num_rows)]
            
        elif col_type == 'str':
            length = spec.get('length', 10)
            charset = spec.get('charset', string.ascii_letters + string.digits)
            data[column_name] = [''.join(random.choices(charset, k=length)) for _ in range(num_rows)]
            
        elif col_type == 'bool':
            data[column_name] = [random.choice([True, False]) for _ in range(num_rows)]
            
        elif col_type == 'datetime':
            start_date = spec.get('start', '2020-01-01')
            end_date = spec.get('end', '2023-01-01')
            
            if isinstance(start_date, str):
                start_date = datetime.datetime.fromisoformat(start_date)
            if isinstance(end_date, str):
                end_date = datetime.datetime.fromisoformat(end_date)
                
            start_timestamp = start_date.timestamp()
            end_timestamp = end_date.timestamp()
            
            data[column_name] = [
                datetime.datetime.fromtimestamp(random.uniform(start_timestamp, end_timestamp))
                for _ in range(num_rows)
            ]
            
        elif col_type == 'category':
            categories = spec.get('categories', ['A', 'B', 'C'])
            data[column_name] = [random.choice(categories) for _ in range(num_rows)]
    
    return pd.DataFrame(data)


def create_test_json_data(structure_spec: Dict, as_string: bool = False) -> Union[str, Dict]:
    """
    Creates a JSON string or object with random test data.
    
    Args:
        structure_spec: Dictionary specifying the structure and types of the data
            Each key should map to a type specification or a nested structure
        as_string: Whether to return a JSON string instead of a dictionary
        
    Returns:
        JSON string or dictionary with generated test data
        
    Example:
        structure_spec = {
            'id': {'type': 'int', 'min': 1, 'max': 1000},
            'name': {'type': 'str', 'length': 10},
            'metadata': {
                'created_at': {'type': 'datetime'},
                'tags': {'type': 'list', 'length': 3, 'item_type': {'type': 'str', 'length': 5}}
            }
        }
    """
    def _generate_value(spec):
        if isinstance(spec, dict):
            if 'type' in spec:
                val_type = spec['type']
                
                if val_type == 'int':
                    min_val = spec.get('min', 0)
                    max_val = spec.get('max', 100)
                    return random.randint(min_val, max_val)
                    
                elif val_type == 'float':
                    min_val = spec.get('min', 0.0)
                    max_val = spec.get('max', 1.0)
                    return random.uniform(min_val, max_val)
                    
                elif val_type == 'str':
                    length = spec.get('length', 10)
                    charset = spec.get('charset', string.ascii_letters + string.digits)
                    return ''.join(random.choices(charset, k=length))
                    
                elif val_type == 'bool':
                    return random.choice([True, False])
                    
                elif val_type == 'datetime':
                    start_date = spec.get('start', '2020-01-01')
                    end_date = spec.get('end', '2023-01-01')
                    
                    if isinstance(start_date, str):
                        start_date = datetime.datetime.fromisoformat(start_date)
                    if isinstance(end_date, str):
                        end_date = datetime.datetime.fromisoformat(end_date)
                        
                    start_timestamp = start_date.timestamp()
                    end_timestamp = end_date.timestamp()
                    
                    dt = datetime.datetime.fromtimestamp(random.uniform(start_timestamp, end_timestamp))
                    return dt.isoformat()
                    
                elif val_type == 'list':
                    length = spec.get('length', 3)
                    item_spec = spec.get('item_type', {'type': 'str'})
                    return [_generate_value(item_spec) for _ in range(length)]
                    
                elif val_type == 'dict':
                    inner_spec = spec.get('structure', {})
                    return {k: _generate_value(v) for k, v in inner_spec.items()}
                    
                else:
                    return None
            else:
                # Nested structure
                return {k: _generate_value(v) for k, v in spec.items()}
        else:
            # Simple type specification
            return spec
    
    result = {k: _generate_value(v) for k, v in structure_spec.items()}
    
    if as_string:
        return json.dumps(result)
    return result


def create_test_validation_result(
    rule_id: str = None,
    rule_name: str = None,
    rule_type: str = None,
    dimension: str = None,
    success: bool = True,
    details: Dict = None
) -> Dict:
    """
    Creates a test validation result object for testing.
    
    Args:
        rule_id: ID of the validation rule
        rule_name: Name of the validation rule
        rule_type: Type of the validation rule
        dimension: Quality dimension (completeness, accuracy, etc.)
        success: Whether the validation passed
        details: Additional details about the validation
        
    Returns:
        Validation result dictionary
    """
    if rule_id is None:
        rule_id = f"rule-{uuid.uuid4()}"
    
    if rule_name is None:
        rule_name = f"Test Rule {rule_id[-6:]}"
    
    if rule_type is None:
        rule_type = random.choice(['schema', 'null_check', 'uniqueness', 'referential', 'custom'])
    
    if dimension is None:
        dimension = random.choice(['completeness', 'accuracy', 'consistency', 'validity', 'timeliness'])
    
    if details is None:
        details = {}
    
    validation_result = {
        'rule_id': rule_id,
        'rule_name': rule_name,
        'rule_type': rule_type,
        'dimension': dimension,
        'success': success,
        'timestamp': datetime.datetime.now().isoformat(),
        'details': details
    }
    
    if not success and 'error_message' not in details:
        validation_result['details']['error_message'] = f"Validation failed for {rule_name}"
        validation_result['details']['failed_records'] = random.randint(1, 10)
    
    return validation_result


def create_test_validation_summary(
    total_validations: int = None,
    passed_validations: int = None,
    validation_results: List[Dict] = None,
    quality_score: float = None
) -> Dict:
    """
    Creates a test validation summary object for testing.
    
    Args:
        total_validations: Total number of validations performed
        passed_validations: Number of validations that passed
        validation_results: List of individual validation results
        quality_score: Overall quality score (0.0-1.0)
        
    Returns:
        Validation summary dictionary
    """
    if validation_results is None:
        validation_results = []
    
    if total_validations is None:
        if validation_results:
            total_validations = len(validation_results)
        else:
            total_validations = random.randint(5, 15)
    
    if passed_validations is None:
        if validation_results:
            passed_validations = sum(1 for r in validation_results if r.get('success', True))
        else:
            passed_validations = random.randint(0, total_validations)
    
    failed_validations = total_validations - passed_validations
    
    if quality_score is None:
        if total_validations > 0:
            quality_score = passed_validations / total_validations
        else:
            quality_score = 0.0
    
    # Generate validation results if not provided
    if not validation_results:
        for i in range(passed_validations):
            validation_results.append(create_test_validation_result(
                rule_id=f"pass-rule-{i}",
                success=True
            ))
        
        for i in range(failed_validations):
            validation_results.append(create_test_validation_result(
                rule_id=f"fail-rule-{i}",
                success=False
            ))
        
        # Shuffle to mix passed and failed
        random.shuffle(validation_results)
    
    return {
        'total_validations': total_validations,
        'passed_validations': passed_validations,
        'failed_validations': failed_validations,
        'quality_score': quality_score,
        'validation_time': datetime.datetime.now().isoformat(),
        'validation_results': validation_results
    }


def create_test_pipeline_execution(
    pipeline_id: str = None,
    status: str = None,
    num_tasks: int = None,
    metadata: Dict = None
) -> Dict:
    """
    Creates a test pipeline execution object for testing.
    
    Args:
        pipeline_id: ID of the pipeline
        status: Execution status
        num_tasks: Number of tasks to generate
        metadata: Additional metadata for the execution
        
    Returns:
        Pipeline execution dictionary
    """
    if pipeline_id is None:
        pipeline_id = f"pipeline-{uuid.uuid4()}"
    
    if status is None:
        status = random.choice(['SUCCESS', 'FAILED', 'RUNNING', 'PENDING'])
    
    execution_id = f"exec-{uuid.uuid4()}"
    
    # Generate start and end times
    now = datetime.datetime.now()
    start_time = (now - datetime.timedelta(minutes=random.randint(30, 60))).isoformat()
    
    end_time = None
    if status in ['SUCCESS', 'FAILED']:
        duration_minutes = random.randint(5, 25)
        end_time = (now - datetime.timedelta(minutes=random.randint(5, 29))).isoformat()
    
    # Generate tasks if specified
    tasks = []
    if num_tasks is not None:
        task_statuses = ['SUCCESS', 'FAILED', 'RUNNING', 'PENDING']
        
        # Ensure consistency with overall status
        if status == 'SUCCESS':
            task_statuses = ['SUCCESS']
        elif status == 'FAILED':
            # At least one task should be failed
            tasks.append({
                'task_id': f"task-{uuid.uuid4()}",
                'name': f"Failed Task",
                'status': 'FAILED',
                'start_time': start_time,
                'end_time': end_time,
                'error': "Sample error message for testing"
            })
            num_tasks -= 1
        
        for i in range(num_tasks):
            task_status = random.choice(task_statuses)
            task_end_time = end_time if task_status in ['SUCCESS', 'FAILED'] else None
            
            tasks.append({
                'task_id': f"task-{uuid.uuid4()}",
                'name': f"Task {i+1}",
                'status': task_status,
                'start_time': start_time,
                'end_time': task_end_time
            })
    
    execution = {
        'execution_id': execution_id,
        'pipeline_id': pipeline_id,
        'status': status,
        'start_time': start_time,
        'end_time': end_time,
        'tasks': tasks
    }
    
    if metadata:
        execution['metadata'] = metadata
    
    return execution


def create_test_healing_action(
    action_id: str = None,
    action_type: str = None,
    successful: bool = True,
    parameters: Dict = None,
    result: Dict = None
) -> Dict:
    """
    Creates a test healing action object for testing.
    
    Args:
        action_id: ID of the healing action
        action_type: Type of the healing action
        successful: Whether the action was successful
        parameters: Parameters used for the healing action
        result: Result of the healing action
        
    Returns:
        Healing action dictionary
    """
    if action_id is None:
        action_id = f"action-{uuid.uuid4()}"
    
    if action_type is None:
        action_type = random.choice(['data_correction', 'pipeline_retry', 'resource_adjustment', 'schema_fix'])
    
    if parameters is None:
        parameters = {}
        
        if action_type == 'data_correction':
            parameters = {
                'correction_type': random.choice(['imputation', 'filtering', 'transformation']),
                'target_table': f"dataset.table_{random.randint(1, 10)}",
                'affected_records': random.randint(1, 100)
            }
        elif action_type == 'pipeline_retry':
            parameters = {
                'retry_count': random.randint(1, 3),
                'backoff_seconds': random.randint(10, 60),
                'modified_params': {'memory': f"{random.randint(1, 4)}G"}
            }
        elif action_type == 'resource_adjustment':
            parameters = {
                'resource_type': random.choice(['memory', 'cpu', 'disk']),
                'original_value': f"{random.randint(1, 2)}G",
                'new_value': f"{random.randint(2, 4)}G"
            }
        elif action_type == 'schema_fix':
            parameters = {
                'schema_change': random.choice(['add_column', 'modify_type', 'add_nullable']),
                'field_name': f"field_{random.randint(1, 10)}"
            }
    
    if result is None:
        result = {
            'successful': successful,
            'execution_time': random.uniform(0.1, 5.0),
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        if not successful:
            result['error_message'] = "Sample error message for testing"
    
    return {
        'action_id': action_id,
        'action_type': action_type,
        'parameters': parameters,
        'result': result,
        'confidence_score': random.uniform(0.7, 1.0) if successful else random.uniform(0.3, 0.7),
        'created_at': datetime.datetime.now().isoformat()
    }


def compare_nested_structures(
    actual: Any,
    expected: Any,
    ignore_extra_keys: bool = False,
    ignore_paths: List[str] = None,
    float_tolerance: float = 1e-6
) -> Tuple[bool, str]:
    """
    Compares two nested structures (dicts/lists) for equality with customizable options.
    
    Args:
        actual: The actual value or structure
        expected: The expected value or structure
        ignore_extra_keys: Whether to ignore extra keys in dictionaries
        ignore_paths: List of dot-notation paths to ignore in comparison
        float_tolerance: Tolerance for floating point comparisons
        
    Returns:
        (bool, str) - Match result and difference description
    """
    if ignore_paths is None:
        ignore_paths = []
    
    def _should_ignore(path):
        return path in ignore_paths or any(path.startswith(f"{p}.") for p in ignore_paths)
    
    def _compare(a, e, path=""):
        # Check if path should be ignored
        if _should_ignore(path):
            return True, ""
        
        # Check types
        if type(a) != type(e) and not (isinstance(a, (int, float)) and isinstance(e, (int, float))):
            return False, f"Type mismatch at {path}: {type(a).__name__} != {type(e).__name__}"
        
        # Compare dictionaries
        if isinstance(a, dict):
            # Check for missing keys
            for k in e:
                if k not in a:
                    return False, f"Missing key at {path}.{k}"
            
            # Check for extra keys
            if not ignore_extra_keys:
                for k in a:
                    if k not in e:
                        return False, f"Extra key at {path}.{k}"
            
            # Compare values recursively
            for k in e:
                if k in a:
                    match, msg = _compare(a[k], e[k], f"{path}.{k}" if path else k)
                    if not match:
                        return False, msg
            
            return True, ""
        
        # Compare lists
        elif isinstance(a, list):
            if len(a) != len(e):
                return False, f"List length mismatch at {path}: {len(a)} != {len(e)}"
            
            for i in range(len(a)):
                match, msg = _compare(a[i], e[i], f"{path}[{i}]")
                if not match:
                    return False, msg
            
            return True, ""
        
        # Compare floats with tolerance
        elif isinstance(a, float) and isinstance(e, float):
            if abs(a - e) > float_tolerance:
                return False, f"Float value mismatch at {path}: {a} != {e} (tolerance: {float_tolerance})"
            return True, ""
        
        # Compare other values
        else:
            if a != e:
                return False, f"Value mismatch at {path}: {a} != {e}"
            return True, ""
    
    return _compare(actual, expected)


def generate_unique_id(prefix: str = "") -> str:
    """
    Generates a unique identifier for test resources.
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique identifier string
    """
    unique_id = str(uuid.uuid4())
    if prefix:
        return f"{prefix}-{unique_id}"
    return unique_id


def wait_for_condition(condition_func: Callable[[], bool], timeout: float = 30.0, check_interval: float = 0.5) -> bool:
    """
    Waits for a condition to be true with timeout.
    
    Args:
        condition_func: Function that returns True when the condition is met
        timeout: Maximum time to wait in seconds
        check_interval: Time between condition checks in seconds
        
    Returns:
        True if condition was met, False if timeout occurred
    """
    start_time = datetime.datetime.now()
    
    while (datetime.datetime.now() - start_time).total_seconds() < timeout:
        if condition_func():
            return True
        
        import time
        time.sleep(check_interval)
    
    return False


class MockResponseBuilder:
    """Builder class for creating mock HTTP responses for testing."""
    
    def __init__(self):
        """Initialize the MockResponseBuilder."""
        self._status_codes = {}
        self._responses = {}
        self._headers = {}
    
    def with_status_code(self, url: str, status_code: int):
        """
        Set the status code for a specific request.
        
        Args:
            url: The URL to configure
            status_code: The HTTP status code to return
            
        Returns:
            Self reference for method chaining
        """
        self._status_codes[url] = status_code
        return self
    
    def with_json_response(self, url: str, data: Dict):
        """
        Set a JSON response for a specific request.
        
        Args:
            url: The URL to configure
            data: The JSON data to return
            
        Returns:
            Self reference for method chaining
        """
        self._responses[url] = {'type': 'json', 'data': data}
        return self
    
    def with_text_response(self, url: str, text: str):
        """
        Set a text response for a specific request.
        
        Args:
            url: The URL to configure
            text: The text content to return
            
        Returns:
            Self reference for method chaining
        """
        self._responses[url] = {'type': 'text', 'data': text}
        return self
    
    def with_headers(self, url: str, headers: Dict):
        """
        Set headers for a specific request.
        
        Args:
            url: The URL to configure
            headers: The headers to return
            
        Returns:
            Self reference for method chaining
        """
        self._headers[url] = headers
        return self
    
    def build_mock_response(self, url: str) -> MagicMock:
        """
        Build a mock response for a specific URL.
        
        Args:
            url: The URL to build the response for
            
        Returns:
            Configured mock response
        """
        response = MagicMock()
        
        # Set status code
        response.status_code = self._status_codes.get(url, 200)
        
        # Set response data
        response_data = self._responses.get(url)
        if response_data:
            if response_data['type'] == 'json':
                response.json.return_value = response_data['data']
                response.text = json.dumps(response_data['data'])
            elif response_data['type'] == 'text':
                response.text = response_data['data']
                
                # Also configure json() to raise ValueError if called
                def raise_value_error():
                    raise ValueError("Response content is not JSON")
                response.json.side_effect = raise_value_error
        
        # Set headers
        response.headers = self._headers.get(url, {})
        
        return response
    
    def build_mock_request_function(self) -> Callable:
        """
        Build a mock request function that returns appropriate responses.
        
        Returns:
            Mock request function
        """
        def mock_request(url, *args, **kwargs):
            return self.build_mock_response(url)
        
        return mock_request


class TestDataGenerator:
    """Utility class for generating test data for various scenarios."""
    
    @staticmethod
    def generate_random_string(length: int = 10, charset: str = None) -> str:
        """
        Generate a random string of specified length.
        
        Args:
            length: Length of the string to generate
            charset: Characters to choose from (defaults to letters and digits)
            
        Returns:
            Random string
        """
        if charset is None:
            charset = string.ascii_letters + string.digits
        
        return ''.join(random.choices(charset, k=length))
    
    @staticmethod
    def generate_random_email() -> str:
        """
        Generate a random email address.
        
        Returns:
            Random email address
        """
        username = TestDataGenerator.generate_random_string(random.randint(5, 10)).lower()
        domains = ['example.com', 'test.org', 'fake-email.net', 'sample.co']
        
        return f"{username}@{random.choice(domains)}"
    
    @staticmethod
    def generate_random_date(start_date: datetime.date = None, end_date: datetime.date = None) -> datetime.date:
        """
        Generate a random date within a specified range.
        
        Args:
            start_date: Start of date range (default: 1 year ago)
            end_date: End of date range (default: today)
            
        Returns:
            Random date
        """
        if start_date is None:
            start_date = datetime.date.today() - datetime.timedelta(days=365)
        
        if end_date is None:
            end_date = datetime.date.today()
        
        days_between = (end_date - start_date).days
        if days_between < 0:
            raise ValueError("End date must be after start date")
        
        random_days = random.randint(0, days_between)
        return start_date + datetime.timedelta(days=random_days)
    
    @staticmethod
    def generate_random_timestamp(
        start_datetime: datetime.datetime = None,
        end_datetime: datetime.datetime = None
    ) -> datetime.datetime:
        """
        Generate a random timestamp within a specified range.
        
        Args:
            start_datetime: Start of timestamp range (default: 1 year ago)
            end_datetime: End of timestamp range (default: now)
            
        Returns:
            Random timestamp
        """
        if start_datetime is None:
            start_datetime = datetime.datetime.now() - datetime.timedelta(days=365)
        
        if end_datetime is None:
            end_datetime = datetime.datetime.now()
        
        seconds_between = int((end_datetime - start_datetime).total_seconds())
        if seconds_between < 0:
            raise ValueError("End datetime must be after start datetime")
        
        random_seconds = random.randint(0, seconds_between)
        return start_datetime + datetime.timedelta(seconds=random_seconds)
    
    @staticmethod
    def generate_sample_data_quality_issues(num_issues: int = 5, issue_types: List[str] = None) -> List[Dict]:
        """
        Generate sample data quality issues for testing.
        
        Args:
            num_issues: Number of issues to generate
            issue_types: List of issue types to choose from
            
        Returns:
            List of data quality issue dictionaries
        """
        if issue_types is None:
            issue_types = ['missing_values', 'duplicate_records', 'invalid_format', 'outliers', 'referential_integrity']
        
        issues = []
        
        for _ in range(num_issues):
            issue_type = random.choice(issue_types)
            
            # Generate appropriate details based on issue type
            details = {}
            
            if issue_type == 'missing_values':
                details = {
                    'column': f"column_{random.randint(1, 10)}",
                    'missing_count': random.randint(1, 100),
                    'percentage': round(random.uniform(0.01, 0.5), 4)
                }
            
            elif issue_type == 'duplicate_records':
                details = {
                    'duplicate_count': random.randint(1, 50),
                    'keys_affected': [f"key_{i}" for i in random.sample(range(1, 20), random.randint(1, 5))]
                }
            
            elif issue_type == 'invalid_format':
                details = {
                    'column': f"column_{random.randint(1, 10)}",
                    'expected_format': random.choice(['email', 'date', 'number', 'phone']),
                    'invalid_count': random.randint(1, 30)
                }
            
            elif issue_type == 'outliers':
                details = {
                    'column': f"column_{random.randint(1, 10)}",
                    'outlier_count': random.randint(1, 20),
                    'threshold': round(random.uniform(2.0, 5.0), 2)
                }
            
            elif issue_type == 'referential_integrity':
                details = {
                    'source_table': f"table_{random.randint(1, 10)}",
                    'target_table': f"table_{random.randint(1, 10)}",
                    'unmatched_count': random.randint(1, 40)
                }
            
            # Assign severity based on random thresholds
            severity_value = random.random()
            if severity_value < 0.2:
                severity = 'critical'
            elif severity_value < 0.5:
                severity = 'high'
            elif severity_value < 0.8:
                severity = 'medium'
            else:
                severity = 'low'
            
            issues.append({
                'issue_id': f"issue-{uuid.uuid4()}",
                'type': issue_type,
                'severity': severity,
                'details': details,
                'detected_at': datetime.datetime.now().isoformat()
            })
        
        return issues
    
    @staticmethod
    def generate_sample_pipeline_metrics(pipeline_id: str = None, num_executions: int = 10) -> Dict:
        """
        Generate sample pipeline metrics for testing.
        
        Args:
            pipeline_id: ID of the pipeline
            num_executions: Number of executions to generate metrics for
            
        Returns:
            Dictionary of pipeline metrics
        """
        if pipeline_id is None:
            pipeline_id = f"pipeline-{uuid.uuid4()}"
        
        # Generate execution metrics
        executions = []
        success_count = 0
        total_duration = 0
        total_records = 0
        
        for i in range(num_executions):
            success = random.random() > 0.2  # 80% success rate
            
            if success:
                success_count += 1
                status = 'SUCCESS'
            else:
                status = random.choice(['FAILED', 'ERROR', 'TIMEOUT'])
            
            duration_minutes = random.uniform(5, 60)
            records_processed = random.randint(1000, 100000) if success else random.randint(0, 5000)
            
            total_duration += duration_minutes
            total_records += records_processed
            
            execution_time = datetime.datetime.now() - datetime.timedelta(hours=i*2)
            
            executions.append({
                'execution_id': f"exec-{uuid.uuid4()}",
                'timestamp': execution_time.isoformat(),
                'status': status,
                'duration_minutes': round(duration_minutes, 2),
                'records_processed': records_processed
            })
        
        # Calculate aggregate metrics
        success_rate = success_count / num_executions if num_executions > 0 else 0
        avg_duration = total_duration / num_executions if num_executions > 0 else 0
        avg_records = total_records / success_count if success_count > 0 else 0
        
        # Generate resource utilization metrics
        resource_metrics = {
            'cpu_utilization': {
                'min': round(random.uniform(5, 20), 2),
                'max': round(random.uniform(50, 95), 2),
                'avg': round(random.uniform(30, 70), 2)
            },
            'memory_utilization': {
                'min': round(random.uniform(10, 30), 2),
                'max': round(random.uniform(60, 90), 2),
                'avg': round(random.uniform(40, 75), 2)
            },
            'disk_io': {
                'read_ops': random.randint(1000, 10000),
                'write_ops': random.randint(500, 5000),
                'read_bytes': random.randint(10**6, 10**9),
                'write_bytes': random.randint(10**5, 10**8)
            }
        }
        
        # Generate data volume metrics
        data_metrics = {
            'input_size_bytes': random.randint(10**6, 10**10),
            'output_size_bytes': random.randint(10**5, 10**9),
            'compression_ratio': round(random.uniform(1.5, 10.0), 2),
            'row_count': random.randint(10**3, 10**7)
        }
        
        return {
            'pipeline_id': pipeline_id,
            'metrics_timestamp': datetime.datetime.now().isoformat(),
            'aggregate_metrics': {
                'success_rate': round(success_rate, 4),
                'avg_duration_minutes': round(avg_duration, 2),
                'avg_records_per_execution': int(avg_records),
                'total_executions': num_executions,
                'total_records_processed': total_records
            },
            'recent_executions': executions,
            'resource_utilization': resource_metrics,
            'data_metrics': data_metrics
        }


class TestResourceManager:
    """Context manager for managing test resources and ensuring cleanup."""
    
    def __init__(self):
        """Initialize the TestResourceManager."""
        self._resources = []
        self._cleanup_funcs = []
    
    def __enter__(self):
        """Enter the context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and clean up resources."""
        self.cleanup()
        return None
    
    def add_resource(self, resource, cleanup_func):
        """
        Add a resource to be managed.
        
        Args:
            resource: The resource to be managed
            cleanup_func: Function to call for cleanup
            
        Returns:
            The added resource
        """
        self._resources.append(resource)
        self._cleanup_funcs.append(cleanup_func)
        return resource
    
    def add_temp_file(self, content="", suffix=None, prefix=None, dir_path=None):
        """
        Add a temporary file to be managed.
        
        Args:
            content: Optional content to write to the file
            suffix: Optional suffix for the filename
            prefix: Optional prefix for the filename
            dir_path: Optional directory path
            
        Returns:
            Path to the created temporary file
        """
        file_path = create_temp_file(content, suffix, prefix, dir_path)
        return self.add_resource(file_path, lambda path: os.remove(path) if os.path.exists(path) else None)
    
    def add_temp_directory(self, suffix=None, prefix=None, dir_path=None):
        """
        Add a temporary directory to be managed.
        
        Args:
            suffix: Optional suffix for the directory name
            prefix: Optional prefix for the directory name
            dir_path: Optional parent directory path
            
        Returns:
            Path to the created temporary directory
        """
        dir_path = create_temp_directory(suffix, prefix, dir_path)
        return self.add_resource(dir_path, lambda path: shutil.rmtree(path) if os.path.exists(path) else None)
    
    def cleanup(self):
        """Clean up all managed resources."""
        # Clean up resources in reverse order (LIFO)
        while self._resources and self._cleanup_funcs:
            resource = self._resources.pop()
            cleanup_func = self._cleanup_funcs.pop()
            
            try:
                cleanup_func(resource)
            except Exception as e:
                print(f"Error cleaning up resource {resource}: {str(e)}")
        
        # Clear any remaining items (should not happen if resources and cleanup_funcs stay in sync)
        self._resources.clear()
        self._cleanup_funcs.clear()