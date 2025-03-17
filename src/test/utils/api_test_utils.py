"""
Provides utility functions and classes for testing API endpoints in the self-healing data pipeline.

This module contains tools for creating mock requests, responses, and test data specifically 
for API testing, as well as utilities for validating API responses and simulating API interactions.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import typing
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
import json
import os
import datetime
import httpx

from src.test.utils.test_helpers import (
    create_temp_file, 
    create_temp_directory, 
    MockResponseBuilder, 
    TestDataGenerator
)

# Global constants
API_TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'mock_data', 'api')

def create_test_client(app: FastAPI, base_headers: Dict = None) -> TestClient:
    """
    Creates a FastAPI TestClient for API testing
    
    Args:
        app: FastAPI application instance
        base_headers: Default headers to include in all requests
        
    Returns:
        Configured TestClient instance
    """
    client = TestClient(app)
    
    if base_headers:
        client.headers.update(base_headers)
        
    return client

def create_mock_request(
    headers: Dict = None,
    query_params: Dict = None,
    path_params: Dict = None,
    json_body: Dict = None
) -> MagicMock:
    """
    Creates a mock FastAPI request object for testing
    
    Args:
        headers: HTTP headers
        query_params: URL query parameters
        path_params: URL path parameters
        json_body: JSON request body
        
    Returns:
        Mock request object
    """
    mock_request = MagicMock()
    mock_request.headers = headers or {}
    mock_request.query_params = query_params or {}
    mock_request.path_params = path_params or {}
    
    if json_body:
        mock_request.json.return_value = json_body
        
    return mock_request

def create_mock_response(
    status_code: int = 200,
    json_data: Dict = None,
    text_content: str = None,
    headers: Dict = None
) -> MagicMock:
    """
    Creates a mock HTTP response for testing
    
    Args:
        status_code: HTTP status code
        json_data: JSON response data
        text_content: Text response content
        headers: HTTP response headers
        
    Returns:
        Mock response object
    """
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.headers = headers or {}
    
    if json_data:
        mock_response.json.return_value = json_data
    
    if text_content:
        mock_response.text = text_content
    elif json_data:
        mock_response.text = json.dumps(json_data)
        
    return mock_response

def load_api_test_data(file_name: str) -> Dict:
    """
    Loads API test data from the mock data directory
    
    Args:
        file_name: Name of the test data file
        
    Returns:
        Loaded API test data
    """
    file_path = os.path.join(API_TEST_DATA_PATH, file_name)
    with open(file_path, 'r') as f:
        return json.load(f)

def create_api_url(endpoint_path: str, query_params: Dict = None) -> str:
    """
    Creates a full API URL for a given endpoint path
    
    Args:
        endpoint_path: API endpoint path
        query_params: Optional query parameters
        
    Returns:
        Full API URL
    """
    # Ensure the endpoint path starts with a slash
    if not endpoint_path.startswith('/'):
        endpoint_path = f'/{endpoint_path}'
    
    # Create the base URL with API prefix
    url = f"/api/v1{endpoint_path}"
    
    # Add query parameters if provided
    if query_params:
        query_parts = []
        for key, value in query_params.items():
            if isinstance(value, list):
                for v in value:
                    query_parts.append(f"{key}={v}")
            else:
                query_parts.append(f"{key}={value}")
        
        query_string = "&".join(query_parts)
        url = f"{url}?{query_string}"
    
    return url

def create_pagination_params(page: int = 1, page_size: int = 10) -> Dict:
    """
    Creates pagination parameters for API requests
    
    Args:
        page: Page number (default: 1)
        page_size: Number of items per page (default: 10)
        
    Returns:
        Pagination parameters dictionary
    """
    return {
        "page": page,
        "page_size": page_size
    }

def create_date_range_params(
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None
) -> Dict:
    """
    Creates date range parameters for API requests
    
    Args:
        start_date: Start date (optional)
        end_date: End date (optional)
        
    Returns:
        Date range parameters dictionary
    """
    params = {}
    
    if start_date:
        params["start_date"] = start_date.isoformat()
        
    if end_date:
        params["end_date"] = end_date.isoformat()
        
    return params

def assert_successful_response(
    response: httpx.Response,
    expected_status_code: int = 200
) -> Dict:
    """
    Asserts that an API response is successful
    
    Args:
        response: API response
        expected_status_code: Expected HTTP status code
        
    Returns:
        Response JSON data
    """
    assert response.status_code == expected_status_code, \
        f"Expected status code {expected_status_code}, got {response.status_code}. Response text: {response.text}"
    
    content_type = response.headers.get("content-type", "")
    assert "application/json" in content_type, \
        f"Expected JSON response, got content-type: {content_type}"
    
    data = response.json()
    assert data.get("status") == "success", \
        f"Expected success status, got: {data.get('status')}. Response: {data}"
    
    return data

def assert_error_response(
    response: httpx.Response,
    expected_status_code: int,
    expected_error_type: str = None
) -> Dict:
    """
    Asserts that an API response is an error with expected details
    
    Args:
        response: API response
        expected_status_code: Expected HTTP status code
        expected_error_type: Expected error type
        
    Returns:
        Error details from response
    """
    assert response.status_code == expected_status_code, \
        f"Expected status code {expected_status_code}, got {response.status_code}"
    
    content_type = response.headers.get("content-type", "")
    assert "application/json" in content_type, \
        f"Expected JSON response, got content-type: {content_type}"
    
    data = response.json()
    assert data.get("status") == "error", \
        f"Expected error status, got: {data.get('status')}. Response: {data}"
    
    if expected_error_type:
        assert data.get("error", {}).get("type") == expected_error_type, \
            f"Expected error type {expected_error_type}, got: {data.get('error', {}).get('type')}"
    
    return data.get("error", {})

def assert_pagination_response(
    response_data: Dict,
    expected_page: int = None,
    expected_page_size: int = None,
    expected_total: int = None
) -> Dict:
    """
    Asserts that an API response contains valid pagination metadata
    
    Args:
        response_data: Response data
        expected_page: Expected page number
        expected_page_size: Expected page size
        expected_total: Expected total items
        
    Returns:
        Pagination metadata from response
    """
    assert "metadata" in response_data, "Response missing metadata field"
    metadata = response_data["metadata"]
    
    assert "pagination" in metadata, "Metadata missing pagination information"
    pagination = metadata["pagination"]
    
    assert "page" in pagination, "Pagination missing page field"
    assert "page_size" in pagination, "Pagination missing page_size field"
    assert "total" in pagination, "Pagination missing total field"
    
    if expected_page is not None:
        assert pagination["page"] == expected_page, \
            f"Expected page {expected_page}, got {pagination['page']}"
    
    if expected_page_size is not None:
        assert pagination["page_size"] == expected_page_size, \
            f"Expected page_size {expected_page_size}, got {pagination['page_size']}"
    
    if expected_total is not None:
        assert pagination["total"] == expected_total, \
            f"Expected total {expected_total}, got {pagination['total']}"
    
    return pagination

def create_mock_api_service(endpoint_responses: Dict) -> MagicMock:
    """
    Creates a mock API service with predefined responses
    
    Args:
        endpoint_responses: Dictionary mapping endpoints to response data
        
    Returns:
        Mock API service
    """
    mock_service = MagicMock()
    
    for endpoint, response_data in endpoint_responses.items():
        # Extract method and path
        parts = endpoint.split()
        if len(parts) == 2:
            method, path = parts
        else:
            method = "GET"
            path = endpoint
            
        # Create method name from HTTP method
        method_name = method.lower()
        
        # Configure method response based on data type
        method_mock = getattr(mock_service, method_name)
        
        if isinstance(response_data, dict):
            if "status_code" in response_data:
                # This is a complex response specification
                status_code = response_data.get("status_code", 200)
                response_body = response_data.get("body", {})
                headers = response_data.get("headers", {})
                
                mock_response = create_mock_response(
                    status_code=status_code,
                    json_data=response_body,
                    headers=headers
                )
                method_mock.return_value = mock_response
            else:
                # This is a simple JSON response
                method_mock.return_value = create_mock_response(
                    json_data=response_data
                )
        elif isinstance(response_data, str):
            # Text response
            method_mock.return_value = create_mock_response(
                text_content=response_data
            )
        elif isinstance(response_data, Exception):
            # Exception to be raised
            method_mock.side_effect = response_data
        else:
            # Default response
            method_mock.return_value = create_mock_response(
                json_data={"message": "Mock response"}
            )
    
    return mock_service

class APITestCase:
    """Base class for API test cases with common utilities"""
    
    def __init__(self):
        """Initialize the APITestCase"""
        self.client = None
        self.base_url = None
    
    def setup_method(self):
        """Set up the test case before each test method"""
        # This should be overridden by subclasses to set up the FastAPI TestClient
        # Example implementation:
        # from your_app import app
        # self.client = create_test_client(app)
        # self.base_url = "/api/v1"
        pass
    
    def teardown_method(self):
        """Clean up after each test method"""
        # Override if cleanup is needed
        pass
    
    def get_url(self, path: str) -> str:
        """
        Get a full URL for an endpoint path
        
        Args:
            path: Endpoint path
            
        Returns:
            Full URL
        """
        if not path.startswith('/'):
            path = f'/{path}'
        
        return f"{self.base_url}{path}"
    
    def assert_successful_response(
        self,
        response: httpx.Response,
        expected_status_code: int = 200
    ) -> Dict:
        """
        Assert that a response is successful
        
        Args:
            response: API response
            expected_status_code: Expected HTTP status code
            
        Returns:
            Response data
        """
        return assert_successful_response(response, expected_status_code)
    
    def assert_error_response(
        self,
        response: httpx.Response,
        expected_status_code: int,
        expected_error_type: str = None
    ) -> Dict:
        """
        Assert that a response is an error
        
        Args:
            response: API response
            expected_status_code: Expected HTTP status code
            expected_error_type: Expected error type
            
        Returns:
            Error details
        """
        return assert_error_response(response, expected_status_code, expected_error_type)

class APIResponseValidator:
    """Utility class for validating API responses against expected schemas and values"""
    
    def validate_response_schema(
        self,
        response_data: Dict,
        expected_schema: Dict
    ) -> bool:
        """
        Validate that a response matches an expected schema
        
        Args:
            response_data: Actual response data
            expected_schema: Expected schema structure
            
        Returns:
            True if valid, raises AssertionError otherwise
        """
        def _validate_type(value, expected_type):
            if expected_type == "string":
                return isinstance(value, str)
            elif expected_type == "number":
                return isinstance(value, (int, float))
            elif expected_type == "integer":
                return isinstance(value, int)
            elif expected_type == "boolean":
                return isinstance(value, bool)
            elif expected_type == "array":
                return isinstance(value, list)
            elif expected_type == "object":
                return isinstance(value, dict)
            elif expected_type == "null":
                return value is None
            elif isinstance(expected_type, list):
                # Union type - value must match one of the types
                return any(_validate_type(value, t) for t in expected_type)
            return False
        
        def _validate_schema(data, schema, path=""):
            # Check if data is missing but required
            if data is None:
                if schema.get("required", False):
                    raise AssertionError(f"Required field missing at {path}")
                return True
            
            # Check type
            if "type" in schema:
                if not _validate_type(data, schema["type"]):
                    raise AssertionError(f"Type mismatch at {path}: expected {schema['type']}, got {type(data).__name__}")
            
            # Check properties for objects
            if schema.get("type") == "object" and "properties" in schema:
                # Validate required properties
                for prop_name, prop_schema in schema["properties"].items():
                    prop_path = f"{path}.{prop_name}" if path else prop_name
                    if prop_name not in data:
                        if prop_schema.get("required", False):
                            raise AssertionError(f"Required property {prop_path} missing")
                        continue
                    
                    _validate_schema(data[prop_name], prop_schema, prop_path)
            
            # Check items for arrays
            if schema.get("type") == "array" and "items" in schema:
                for i, item in enumerate(data):
                    item_path = f"{path}[{i}]"
                    _validate_schema(item, schema["items"], item_path)
            
            # Check enum values
            if "enum" in schema and data not in schema["enum"]:
                raise AssertionError(f"Value {data} at {path} not in enum {schema['enum']}")
            
            # Check pattern
            if "pattern" in schema and isinstance(data, str):
                import re
                if not re.match(schema["pattern"], data):
                    raise AssertionError(f"Value {data} at {path} does not match pattern {schema['pattern']}")
            
            # Check minimum/maximum for numbers
            if isinstance(data, (int, float)):
                if "minimum" in schema and data < schema["minimum"]:
                    raise AssertionError(f"Value {data} at {path} less than minimum {schema['minimum']}")
                if "maximum" in schema and data > schema["maximum"]:
                    raise AssertionError(f"Value {data} at {path} greater than maximum {schema['maximum']}")
            
            # Check minLength/maxLength for strings
            if isinstance(data, str):
                if "minLength" in schema and len(data) < schema["minLength"]:
                    raise AssertionError(f"String '{data}' at {path} shorter than minLength {schema['minLength']}")
                if "maxLength" in schema and len(data) > schema["maxLength"]:
                    raise AssertionError(f"String '{data}' at {path} longer than maxLength {schema['maxLength']}")
            
            # Check minItems/maxItems for arrays
            if isinstance(data, list):
                if "minItems" in schema and len(data) < schema["minItems"]:
                    raise AssertionError(f"Array at {path} has fewer than minItems {schema['minItems']}")
                if "maxItems" in schema and len(data) > schema["maxItems"]:
                    raise AssertionError(f"Array at {path} has more than maxItems {schema['maxItems']}")
            
            return True
        
        _validate_schema(response_data, expected_schema)
        return True
    
    def validate_quality_rule_response(
        self,
        response_data: Dict,
        expected_values: Dict = None
    ) -> bool:
        """
        Validate a quality rule response against expected schema and values
        
        Args:
            response_data: Actual response data
            expected_values: Expected values for specific fields
            
        Returns:
            True if valid, raises AssertionError otherwise
        """
        # Required fields for a quality rule response
        required_fields = [
            "rule_id", "rule_name", "rule_type", "dimension", "severity", 
            "created_at", "updated_at", "enabled"
        ]
        
        for field in required_fields:
            assert field in response_data, f"Required field '{field}' missing from quality rule response"
        
        # Validate specific expected values if provided
        if expected_values:
            for field, expected_value in expected_values.items():
                assert field in response_data, f"Expected field '{field}' missing from quality rule response"
                assert response_data[field] == expected_value, \
                    f"Field '{field}' has value '{response_data[field]}', expected '{expected_value}'"
        
        return True
    
    def validate_validation_response(
        self,
        response_data: Dict,
        expected_values: Dict = None
    ) -> bool:
        """
        Validate a quality validation response against expected schema and values
        
        Args:
            response_data: Actual response data
            expected_values: Expected values for specific fields
            
        Returns:
            True if valid, raises AssertionError otherwise
        """
        # Required fields for a validation response
        required_fields = [
            "validation_id", "pipeline_id", "status", "started_at", 
            "completed_at", "total_rules", "passed_rules", "failed_rules"
        ]
        
        for field in required_fields:
            assert field in response_data, f"Required field '{field}' missing from validation response"
        
        # Validate specific expected values if provided
        if expected_values:
            for field, expected_value in expected_values.items():
                assert field in response_data, f"Expected field '{field}' missing from validation response"
                assert response_data[field] == expected_value, \
                    f"Field '{field}' has value '{response_data[field]}', expected '{expected_value}'"
        
        return True
    
    def validate_healing_action_response(
        self,
        response_data: Dict,
        expected_values: Dict = None
    ) -> bool:
        """
        Validate a healing action response against expected schema and values
        
        Args:
            response_data: Actual response data
            expected_values: Expected values for specific fields
            
        Returns:
            True if valid, raises AssertionError otherwise
        """
        # Required fields for a healing action response
        required_fields = [
            "action_id", "action_type", "status", "created_at", 
            "completed_at", "confidence_score", "parameters", "result"
        ]
        
        for field in required_fields:
            assert field in response_data, f"Required field '{field}' missing from healing action response"
        
        # Validate specific expected values if provided
        if expected_values:
            for field, expected_value in expected_values.items():
                assert field in response_data, f"Expected field '{field}' missing from healing action response"
                assert response_data[field] == expected_value, \
                    f"Field '{field}' has value '{response_data[field]}', expected '{expected_value}'"
        
        return True

class MockAPIServer:
    """Mock API server for simulating external API endpoints in tests"""
    
    def __init__(self):
        """Initialize the MockAPIServer"""
        self._endpoints = {}  # Maps endpoint keys to response configurations
        self._request_history = {}  # Tracks requests made to each endpoint
    
    def add_endpoint(
        self,
        method: str,
        path: str,
        status_code: int = 200,
        response_data: Dict = None,
        headers: Dict = None
    ) -> 'MockAPIServer':
        """
        Add an endpoint with a specific response
        
        Args:
            method: HTTP method (GET, POST, etc.)
            path: URL path
            status_code: HTTP status code
            response_data: Response data
            headers: Response headers
            
        Returns:
            Self reference for method chaining
        """
        endpoint_key = f"{method.upper()}:{path}"
        
        self._endpoints[endpoint_key] = {
            "status_code": status_code,
            "response_data": response_data or {},
            "headers": headers or {}
        }
        
        return self
    
    def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict = None,
        json_data: Dict = None
    ) -> Tuple[int, Dict, Dict]:
        """
        Handle a mock request and return the configured response
        
        Args:
            method: HTTP method
            path: URL path
            headers: Request headers
            json_data: Request JSON data
            
        Returns:
            Tuple of (status_code, response_data, headers)
        """
        endpoint_key = f"{method.upper()}:{path}"
        
        # Record the request
        if endpoint_key not in self._request_history:
            self._request_history[endpoint_key] = []
        
        self._request_history[endpoint_key].append({
            "headers": headers or {},
            "json_data": json_data,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
        # Return the configured response
        if endpoint_key in self._endpoints:
            endpoint_config = self._endpoints[endpoint_key]
            return (
                endpoint_config["status_code"],
                endpoint_config["response_data"],
                endpoint_config["headers"]
            )
        else:
            # Endpoint not found
            return (
                404,
                {"status": "error", "message": f"Endpoint not found: {method} {path}"},
                {"Content-Type": "application/json"}
            )
    
    def get_request_history(
        self,
        method: str,
        path: str
    ) -> List[Dict]:
        """
        Get the history of requests made to a specific endpoint
        
        Args:
            method: HTTP method
            path: URL path
            
        Returns:
            List of request details
        """
        endpoint_key = f"{method.upper()}:{path}"
        return self._request_history.get(endpoint_key, [])
    
    def reset(self):
        """Reset the server state, clearing endpoints and request history"""
        self._endpoints.clear()
        self._request_history.clear()