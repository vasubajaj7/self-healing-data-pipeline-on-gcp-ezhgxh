# Data Quality API

## Introduction

The Data Quality API provides endpoints for managing quality rules, executing validations, retrieving quality scores, and handling quality issues in the self-healing data pipeline. These endpoints enable you to define validation criteria, validate datasets against those criteria, and monitor data quality over time.

The API supports both manual validation requests and integration with automated pipeline processes, with the ability to trigger self-healing actions for detected quality issues.

## Base URL

All Data Quality API endpoints are relative to the base URL:

```
https://api.example.com/api/v1/quality
```

For example, the full URL for the quality rules endpoint would be:

```
https://api.example.com/api/v1/quality/rules
```

## API Conventions

The Data Quality API follows standard conventions for all endpoints:

1. **Authentication**: All API requests require a valid OAuth 2.0 JWT token in the Authorization header
2. **Request Format**: Request bodies must be in JSON format
3. **Response Format**: All responses include a standard envelope with status, message, and data fields
4. **Pagination**: List endpoints support page and page_size parameters
5. **Filtering**: Most endpoints support filtering via query parameters
6. **Error Handling**: Errors use standard HTTP status codes with detailed error information in the response body
7. **Versioning**: The API is versioned in the URL path

## Authentication

All API endpoints require authentication using OAuth 2.0 with JWT tokens. Please refer to the [Authentication](authentication.md) documentation for detailed information on obtaining and using access tokens.

In general, you'll need to include an `Authorization` header with a valid token in all API requests:

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

## Quality Rules Endpoints

Quality rules define the validation criteria applied to datasets. Each rule specifies a particular expectation that data should meet, such as schema conformance, value constraints, or statistical properties.

### List Quality Rules

Retrieves a paginated list of quality rules with optional filtering.

**Request:**

```
GET /quality/rules
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| page | integer | No | Page number (default: 1) |
| page_size | integer | No | Number of items per page (default: 20, max: 100) |
| target_dataset | string | No | Filter by target dataset |
| target_table | string | No | Filter by target table |
| rule_type | string | No | Filter by rule type |
| is_active | boolean | No | Filter by active status |

**Response:**

```json
{
  "status": "success",
  "message": "Quality rules retrieved successfully",
  "metadata": {},
  "items": [
    {
      "rule_id": "550e8400-e29b-41d4-a716-446655440000",
      "rule_name": "Customer ID Not Null",
      "target_dataset": "customer_data",
      "target_table": "customers",
      "rule_type": "THRESHOLD",
      "expectation_type": "expect_column_values_to_not_be_null",
      "rule_definition": {
        "column": "customer_id",
        "mostly": 1.0
      },
      "severity": "HIGH",
      "is_active": true,
      "description": "Ensures customer ID is never null",
      "created_at": "2023-06-15T10:30:00Z",
      "updated_at": "2023-06-15T10:30:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com",
      "metadata": {}
    },
    {
      "rule_id": "550e8400-e29b-41d4-a716-446655440001",
      "rule_name": "Valid Email Format",
      "target_dataset": "customer_data",
      "target_table": "customers",
      "rule_type": "PATTERN",
      "expectation_type": "expect_column_values_to_match_regex",
      "rule_definition": {
        "column": "email",
        "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
        "mostly": 0.99
      },
      "severity": "MEDIUM",
      "is_active": true,
      "description": "Validates email format",
      "created_at": "2023-06-15T11:15:00Z",
      "updated_at": "2023-06-15T11:15:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com",
      "metadata": {}
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 2,
    "total_pages": 1
  }
}
```

### Get Quality Rule

Retrieves a specific quality rule by ID.

**Request:**

```
GET /quality/rules/{rule_id}
```

**Path Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| rule_id | string | Yes | ID of the quality rule to retrieve |

**Response:**

```json
{
  "status": "success",
  "message": "Quality rule data retrieved successfully",
  "metadata": {},
  "data": {
    "rule_id": "550e8400-e29b-41d4-a716-446655440000",
    "rule_name": "Customer ID Not Null",
    "target_dataset": "customer_data",
    "target_table": "customers",
    "rule_type": "THRESHOLD",
    "expectation_type": "expect_column_values_to_not_be_null",
    "rule_definition": {
      "column": "customer_id",
      "mostly": 1.0
    },
    "severity": "HIGH",
    "is_active": true,
    "description": "Ensures customer ID is never null",
    "created_at": "2023-06-15T10:30:00Z",
    "updated_at": "2023-06-15T10:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "metadata": {}
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Quality rule with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

### Create Quality Rule

Creates a new quality rule.

**Request:**

```
POST /quality/rules
```

**Request Body:**

```json
{
  "rule_name": "Order Amount Range",
  "target_dataset": "sales_data",
  "target_table": "orders",
  "rule_type": "THRESHOLD",
  "expectation_type": "expect_column_values_to_be_between",
  "rule_definition": {
    "column": "order_amount",
    "min_value": 0.01,
    "max_value": 10000,
    "mostly": 0.99
  },
  "severity": "MEDIUM",
  "is_active": true,
  "description": "Ensures order amounts are within valid range",
  "metadata": {
    "business_owner": "finance@example.com"
  }
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Quality rule created successfully",
  "metadata": {},
  "data": {
    "rule_id": "550e8400-e29b-41d4-a716-446655440002",
    "rule_name": "Order Amount Range",
    "target_dataset": "sales_data",
    "target_table": "orders",
    "rule_type": "THRESHOLD",
    "expectation_type": "expect_column_values_to_be_between",
    "rule_definition": {
      "column": "order_amount",
      "min_value": 0.01,
      "max_value": 10000,
      "mostly": 0.99
    },
    "severity": "MEDIUM",
    "is_active": true,
    "description": "Ensures order amounts are within valid range",
    "created_at": "2023-06-15T14:45:00Z",
    "updated_at": "2023-06-15T14:45:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "metadata": {
      "business_owner": "finance@example.com"
    }
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Invalid request - rule_definition missing required field 'column'"
}
```

### Update Quality Rule

Updates an existing quality rule.

**Request:**

```
PUT /quality/rules/{rule_id}
```

**Path Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| rule_id | string | Yes | ID of the quality rule to update |

**Request Body:**

```json
{
  "rule_name": "Order Amount Valid Range",
  "rule_definition": {
    "column": "order_amount",
    "min_value": 0.01,
    "max_value": 15000,
    "mostly": 0.99
  },
  "severity": "HIGH",
  "description": "Ensures order amounts are within valid range (updated limits)"
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Quality rule updated successfully",
  "metadata": {},
  "data": {
    "rule_id": "550e8400-e29b-41d4-a716-446655440002",
    "rule_name": "Order Amount Valid Range",
    "target_dataset": "sales_data",
    "target_table": "orders",
    "rule_type": "THRESHOLD",
    "expectation_type": "expect_column_values_to_be_between",
    "rule_definition": {
      "column": "order_amount",
      "min_value": 0.01,
      "max_value": 15000,
      "mostly": 0.99
    },
    "severity": "HIGH",
    "is_active": true,
    "description": "Ensures order amounts are within valid range (updated limits)",
    "created_at": "2023-06-15T14:45:00Z",
    "updated_at": "2023-06-15T15:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "metadata": {
      "business_owner": "finance@example.com"
    }
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Quality rule with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

### Delete Quality Rule

Deletes a quality rule.

**Request:**

```
DELETE /quality/rules/{rule_id}
```

**Path Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| rule_id | string | Yes | ID of the quality rule to delete |

**Response:**

```json
{
  "status": "success",
  "message": "Quality rule deleted successfully",
  "metadata": {},
  "data": {
    "success": true,
    "rule_id": "550e8400-e29b-41d4-a716-446655440002"
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Quality rule with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

## Quality Validation Endpoints

Quality validations represent the execution of quality rules against datasets. These endpoints allow you to execute validations and retrieve validation results.

### List Quality Validations

Retrieves a paginated list of quality validations with optional filtering.

**Request:**

```
GET /quality/validations
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| page | integer | No | Page number (default: 1) |
| page_size | integer | No | Number of items per page (default: 20, max: 100) |
| start_date | string (ISO date) | No | Filter by validation time (start) |
| end_date | string (ISO date) | No | Filter by validation time (end) |
| execution_id | string | No | Filter by pipeline execution ID |
| rule_id | string | No | Filter by rule ID |
| status | string | No | Filter by validation status (PASSED, FAILED, WARNING) |

**Response:**

```json
{
  "status": "success",
  "message": "Quality validations retrieved successfully",
  "metadata": {},
  "items": [
    {
      "validation_id": "550e8400-e29b-41d4-a716-446655440100",
      "rule_id": "550e8400-e29b-41d4-a716-446655440000",
      "execution_id": "550e8400-e29b-41d4-a716-446655440200",
      "status": "PASSED",
      "validation_time": "2023-06-15T08:30:00Z",
      "success_percent": 100.0,
      "records_validated": 5000,
      "records_failed": 0,
      "validation_results": {
        "success": true,
        "result": {
          "observed_value": 1.0,
          "element_count": 5000,
          "missing_count": 0,
          "missing_percent": 0.0
        }
      },
      "metadata": {
        "execution_context": "daily_load"
      }
    },
    {
      "validation_id": "550e8400-e29b-41d4-a716-446655440101",
      "rule_id": "550e8400-e29b-41d4-a716-446655440001",
      "execution_id": "550e8400-e29b-41d4-a716-446655440200",
      "status": "FAILED",
      "validation_time": "2023-06-15T08:30:15Z",
      "success_percent": 98.5,
      "records_validated": 5000,
      "records_failed": 75,
      "validation_results": {
        "success": false,
        "result": {
          "observed_value": 0.985,
          "element_count": 5000,
          "unexpected_count": 75,
          "unexpected_percent": 1.5,
          "unexpected_examples": ["invalid.email", "missing@domain", "@incomplete.com"]
        }
      },
      "error_details": {
        "error_type": "VALIDATION_ERROR",
        "message": "Email format validation failed (threshold: 0.99, actual: 0.985)"
      },
      "metadata": {
        "execution_context": "daily_load"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 2,
    "total_pages": 1
  }
}
```

### Get Validation

Retrieves a specific validation result by ID.

**Request:**

```
GET /quality/validations/{validation_id}
```

**Path Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| validation_id | string | Yes | ID of the validation to retrieve |

**Response:**

```json
{
  "status": "success",
  "message": "Quality validation data retrieved successfully",
  "metadata": {},
  "data": {
    "validation_id": "550e8400-e29b-41d4-a716-446655440101",
    "rule_id": "550e8400-e29b-41d4-a716-446655440001",
    "execution_id": "550e8400-e29b-41d4-a716-446655440200",
    "status": "FAILED",
    "validation_time": "2023-06-15T08:30:15Z",
    "success_percent": 98.5,
    "records_validated": 5000,
    "records_failed": 75,
    "validation_results": {
      "success": false,
      "result": {
        "observed_value": 0.985,
        "element_count": 5000,
        "unexpected_count": 75,
        "unexpected_percent": 1.5,
        "unexpected_examples": ["invalid.email", "missing@domain", "@incomplete.com"]
      }
    },
    "error_details": {
      "error_type": "VALIDATION_ERROR",
      "message": "Email format validation failed (threshold: 0.99, actual: 0.985)"
    },
    "metadata": {
      "execution_context": "daily_load"
    }
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Validation with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

### Execute Validation

Executes quality validation for a dataset.

**Request:**

```
POST /quality/validate
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| dataset | string | Yes | Dataset to validate |
| table | string | No | Specific table to validate (if omitted, all tables in dataset) |
| execution_id | string | No | Pipeline execution ID to associate with validation |
| rule_ids | array of strings | No | Specific rule IDs to validate (if omitted, all applicable rules) |

**Response:**

```json
{
  "status": "success",
  "message": "Validation executed successfully",
  "metadata": {},
  "data": {
    "validation_id": "550e8400-e29b-41d4-a716-446655440102",
    "execution_id": "550e8400-e29b-41d4-a716-446655440201",
    "status": "FAILED",
    "validation_time": "2023-06-15T16:45:00Z",
    "success_percent": 95.0,
    "records_validated": 5000,
    "records_failed": 250,
    "validation_results": {
      "rules_passed": 8,
      "rules_failed": 2,
      "rules_warning": 1,
      "total_rules": 11,
      "rule_results": [
        {
          "rule_id": "550e8400-e29b-41d4-a716-446655440000",
          "rule_name": "Customer ID Not Null",
          "status": "PASSED",
          "success_percent": 100.0
        },
        {
          "rule_id": "550e8400-e29b-41d4-a716-446655440001",
          "rule_name": "Valid Email Format",
          "status": "FAILED",
          "success_percent": 98.5
        }
      ]
    },
    "error_details": {
      "error_type": "VALIDATION_ERROR",
      "message": "2 rules failed validation"
    },
    "metadata": {
      "execution_context": "manual_validation",
      "triggered_by": "admin@example.com"
    }
  }
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Dataset 'invalid_dataset' not found"
}
```

### Get Quality Score

Retrieves the quality score for a dataset.

**Request:**

```
GET /quality/score
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| dataset | string | Yes | Dataset to get quality score for |
| table | string | No | Specific table to get quality score for |
| as_of_date | string (ISO date) | No | Get quality score as of specific date |

**Response:**

```json
{
  "status": "success",
  "message": "Quality score calculated successfully",
  "metadata": {},
  "overall_score": 0.95,
  "dimension_scores": {
    "completeness": 0.98,
    "accuracy": 0.96,
    "consistency": 0.94,
    "validity": 0.92,
    "timeliness": 0.99
  },
  "quality_metrics": {
    "total_rules": 25,
    "rules_passed": 22,
    "rules_failed": 3,
    "rules_warning": 0,
    "critical_issues": 1,
    "high_issues": 2,
    "medium_issues": 5,
    "low_issues": 3
  },
  "calculation_time": "2023-06-15T17:00:00Z"
}
```

**Error Responses:**

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Dataset 'invalid_dataset' not found"
}
```

### Get Validation Trend

Retrieves validation pass/fail trend over time.

**Request:**

```
GET /quality/trend
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| dataset | string | Yes | Dataset to get trend for |
| table | string | No | Specific table to get trend for |
| interval | string | Yes | Time interval (hourly, daily, weekly) |
| num_intervals | integer | Yes | Number of intervals to return |

**Response:**

```json
{
  "status": "success",
  "message": "Validation trend data retrieved successfully",
  "metadata": {},
  "data": {
    "dataset": "customer_data",
    "table": "customers",
    "interval": "daily",
    "trend_data": [
      {
        "interval_start": "2023-06-10T00:00:00Z",
        "interval_end": "2023-06-10T23:59:59Z",
        "total_validations": 120,
        "passed_validations": 115,
        "failed_validations": 5,
        "warning_validations": 0,
        "success_rate": 0.958
      },
      {
        "interval_start": "2023-06-11T00:00:00Z",
        "interval_end": "2023-06-11T23:59:59Z",
        "total_validations": 118,
        "passed_validations": 112,
        "failed_validations": 6,
        "warning_validations": 0,
        "success_rate": 0.949
      },
      {
        "interval_start": "2023-06-12T00:00:00Z",
        "interval_end": "2023-06-12T23:59:59Z",
        "total_validations": 122,
        "passed_validations": 120,
        "failed_validations": 2,
        "warning_validations": 0,
        "success_rate": 0.984
      },
      {
        "interval_start": "2023-06-13T00:00:00Z",
        "interval_end": "2023-06-13T23:59:59Z",
        "total_validations": 125,
        "passed_validations": 123,
        "failed_validations": 2,
        "warning_validations": 0,
        "success_rate": 0.984
      },
      {
        "interval_start": "2023-06-14T00:00:00Z",
        "interval_end": "2023-06-14T23:59:59Z",
        "total_validations": 130,
        "passed_validations": 128,
        "failed_validations": 2,
        "warning_validations": 0,
        "success_rate": 0.985
      }
    ]
  }
}
```

## Quality Issues Endpoints

Quality issues represent validation failures that may require attention or remediation.

### Get Quality Issues

Retrieves quality issues for a dataset.

**Request:**

```
GET /quality/issues
```

**Query Parameters:**

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| page | integer | No | Page number (default: 1) |
| page_size | integer | No | Number of items per page (default: 20, max: 100) |
| dataset | string | Yes | Dataset to get issues for |
| table | string | No | Specific table to get issues for |
| severity | string | No | Filter by severity (CRITICAL, HIGH, MEDIUM, LOW) |
| is_resolved | boolean | No | Filter by resolution status |
| start_date | string (ISO date) | No | Filter by issue date (start) |
| end_date | string (ISO date) | No | Filter by issue date (end) |

**Response:**

```json
{
  "status": "success",
  "message": "Quality issues retrieved successfully",
  "metadata": {},
  "data": [
    {
      "issue_id": "550e8400-e29b-41d4-a716-446655440300",
      "validation_id": "550e8400-e29b-41d4-a716-446655440101",
      "rule_id": "550e8400-e29b-41d4-a716-446655440001",
      "dataset": "customer_data",
      "table": "customers",
      "severity": "MEDIUM",
      "issue_type": "FORMAT_ERROR",
      "issue_description": "Email format validation failed (threshold: 0.99, actual: 0.985)",
      "detected_at": "2023-06-15T08:30:15Z",
      "is_resolved": false,
      "affected_records": 75,
      "affected_columns": ["email"],
      "examples": ["invalid.email", "missing@domain", "@incomplete.com"],
      "healing_status": "IN_PROGRESS",
      "healing_confidence": 0.92,
      "healing_id": "550e8400-e29b-41d4-a716-446655440400"
    },
    {
      "issue_id": "550e8400-e29b-41d4-a716-446655440301",
      "validation_id": "550e8400-e29b-41d4-a716-446655440103",
      "rule_id": "550e8400-e29b-41d4-a716-446655440002",
      "dataset": "customer_data",
      "table": "orders",
      "severity": "HIGH",
      "issue_type": "RANGE_VIOLATION",
      "issue_description": "Order amount outside valid range (min: 0.01, max: 15000)",
      "detected_at": "2023-06-15T09:15:30Z",
      "is_resolved": true,
      "resolved_at": "2023-06-15T09:45:00Z",
      "resolution_type": "SELF_HEALING",
      "resolution_description": "Invalid values capped at maximum threshold",
      "affected_records": 12,
      "affected_columns": ["order_amount"],
      "examples": [25000, 18500, 16200],
      "healing_status": "SUCCESS",
      "healing_confidence": 0.98,
      "healing_id": "550e8400-e29b-41d4-a716-446655440401"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 2,
    "total_pages": 1
  }
}
```

## Error Handling

The Quality API uses a consistent error response format across all endpoints. Error responses include an HTTP status code, a machine-readable error code, and a human-readable error message.

Common error codes include:

| Error Code | Description |
| --- | --- |
| VALIDATION_ERROR | Invalid request data |
| RESOURCE_NOT_FOUND | Requested resource doesn't exist |
| EXECUTION_ERROR | Error during execution of an operation |
| PERMISSION_DENIED | User doesn't have permission for the operation |

Example error response:

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Invalid request - Missing required field 'rule_name'"
}
```

## Expectation Types

The Quality API supports the following expectation types for quality rules:

| Expectation Type | Description | Required Parameters |
| --- | --- | --- |
| expect_column_to_exist | Validates that a column exists in the dataset | column |
| expect_table_row_count_to_be_between | Validates that the table has a row count within a range | min_value, max_value |
| expect_column_values_to_not_be_null | Validates that column values are not null | column, mostly |
| expect_column_values_to_be_in_set | Validates that column values are within a set of allowed values | column, value_set, mostly |
| expect_column_values_to_be_between | Validates that column values are within a range | column, min_value, max_value, mostly |
| expect_column_values_to_match_regex | Validates that column values match a regex pattern | column, regex, mostly |
| expect_column_values_to_be_unique | Validates that column values are unique | column, mostly |
| expect_column_values_to_be_of_type | Validates that column values are of a specific type | column, type, mostly |
| expect_column_pair_values_to_be_equal | Validates that values in two columns are equal | column_A, column_B, mostly |
| expect_column_pair_values_A_to_be_greater_than_B | Validates that values in column A are greater than column B | column_A, column_B, mostly |
| expect_column_sum_to_be_between | Validates that the sum of column values is within a range | column, min_value, max_value |
| expect_column_mean_to_be_between | Validates that the mean of column values is within a range | column, min_value, max_value |
| expect_column_median_to_be_between | Validates that the median of column values is within a range | column, min_value, max_value |
| expect_column_stdev_to_be_between | Validates that the standard deviation of column values is within a range | column, min_value, max_value |
| expect_column_distinct_values_to_be_in_set | Validates that distinct column values are within a set | column, value_set |
| expect_column_distinct_values_to_contain_set | Validates that distinct column values contain a set | column, value_set |
| expect_column_distinct_values_to_equal_set | Validates that distinct column values equal a set | column, value_set |

Each expectation type requires specific parameters in the rule_definition object. The 'mostly' parameter (when applicable) is a float between 0 and 1 that specifies the minimum fraction of values that must meet the expectation.

## Code Examples

### Python Example: Creating and Executing Quality Rules

```python
import requests
import json

BASE_URL = "https://api.example.com/api/v1"
TOKEN = "YOUR_ACCESS_TOKEN"

def create_quality_rule(rule_data):
    """Create a new quality rule"""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        f"{BASE_URL}/quality/rules",
        headers=headers,
        json=rule_data
    )
    
    response.raise_for_status()
    return response.json()["data"]

def execute_validation(dataset, table=None, rule_ids=None):
    """Execute quality validation for a dataset"""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    params = {"dataset": dataset}
    if table:
        params["table"] = table
    if rule_ids:
        params["rule_ids"] = rule_ids
    
    response = requests.post(
        f"{BASE_URL}/quality/validate",
        headers=headers,
        params=params
    )
    
    response.raise_for_status()
    return response.json()["data"]

def get_quality_score(dataset, table=None):
    """Get quality score for a dataset"""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    params = {"dataset": dataset}
    if table:
        params["table"] = table
    
    response = requests.get(
        f"{BASE_URL}/quality/score",
        headers=headers,
        params=params
    )
    
    response.raise_for_status()
    return response.json()

# Example usage
try:
    # Create a new quality rule
    rule_data = {
        "rule_name": "Product Price Range",
        "target_dataset": "product_data",
        "target_table": "products",
        "rule_type": "THRESHOLD",
        "expectation_type": "expect_column_values_to_be_between",
        "rule_definition": {
            "column": "price",
            "min_value": 0.01,
            "max_value": 9999.99,
            "mostly": 1.0
        },
        "severity": "HIGH",
        "description": "Ensures product prices are within valid range"
    }
    
    created_rule = create_quality_rule(rule_data)
    print(f"Created rule with ID: {created_rule['rule_id']}")
    
    # Execute validation
    validation_result = execute_validation("product_data", "products")
    print(f"Validation status: {validation_result['status']}")
    print(f"Success percent: {validation_result['success_percent']}%")
    
    # Get quality score
    quality_score = get_quality_score("product_data", "products")
    print(f"Overall quality score: {quality_score['overall_score']}")
    print("Dimension scores:")
    for dimension, score in quality_score['dimension_scores'].items():
        print(f"  {dimension}: {score}")
    
except requests.exceptions.HTTPError as e:
    error_data = e.response.json()
    print(f"API Error: {error_data['error']} - {error_data['message']}")
```

### JavaScript Example: Managing Quality Issues

```javascript
/**
 * Get quality issues for a dataset
 * 
 * @param {string} dataset - Dataset name
 * @param {string|null} table - Optional table name
 * @param {string|null} severity - Optional severity filter
 * @returns {Promise<Object>} Quality issues data
 */
async function getQualityIssues(dataset, table = null, severity = null) {
  const params = new URLSearchParams();
  params.append('dataset', dataset);
  if (table) params.append('table', table);
  if (severity) params.append('severity', severity);
  
  const response = await fetch(
    `https://api.example.com/api/v1/quality/issues?${params.toString()}`,
    {
      headers: {
        'Authorization': `Bearer ${TOKEN}`,
        'Content-Type': 'application/json'
      }
    }
  );
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  const responseData = await response.json();
  return responseData.data;
}

/**
 * Get quality trend for a dataset
 * 
 * @param {string} dataset - Dataset name
 * @param {string|null} table - Optional table name
 * @param {string} interval - Time interval (hourly, daily, weekly)
 * @param {number} numIntervals - Number of intervals to return
 * @returns {Promise<Object>} Trend data
 */
async function getQualityTrend(dataset, table = null, interval = 'daily', numIntervals = 7) {
  const params = new URLSearchParams();
  params.append('dataset', dataset);
  params.append('interval', interval);
  params.append('num_intervals', numIntervals);
  if (table) params.append('table', table);
  
  const response = await fetch(
    `https://api.example.com/api/v1/quality/trend?${params.toString()}`,
    {
      headers: {
        'Authorization': `Bearer ${TOKEN}`,
        'Content-Type': 'application/json'
      }
    }
  );
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  const responseData = await response.json();
  return responseData.data;
}

// Example usage
async function main() {
  try {
    // Get high severity quality issues
    const issues = await getQualityIssues('customer_data', null, 'HIGH');
    console.log(`Found ${issues.length} high severity issues:`);
    issues.forEach(issue => {
      console.log(`- ${issue.issue_description} (${issue.affected_records} records affected)`);
      console.log(`  Detected at: ${new Date(issue.detected_at).toLocaleString()}`);
      console.log(`  Resolved: ${issue.is_resolved ? 'Yes' : 'No'}`);
      if (issue.healing_status) {
        console.log(`  Healing status: ${issue.healing_status} (${issue.healing_confidence * 100}% confidence)`);
      }
      console.log('---');
    });
    
    // Get quality trend
    const trend = await getQualityTrend('customer_data', null, 'daily', 7);
    console.log('Quality trend over the past week:');
    trend.trend_data.forEach(day => {
      const date = new Date(day.interval_start).toLocaleDateString();
      console.log(`- ${date}: ${(day.success_rate * 100).toFixed(1)}% success rate`);
    });
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

main();
```

## Best Practices

### Quality Rule Design

1. **Start with critical data elements**: Focus first on creating rules for the most business-critical data elements.

2. **Use appropriate severity levels**: Assign severity based on business impact, not technical considerations.

3. **Balance strictness and practicality**: Consider using the `mostly` parameter to allow for some flexibility in rules where 100% compliance is not practical.

4. **Layer your validations**: Create multiple rules with different granularity - schema-level, value-level, and relationship-level.

5. **Document your rules**: Use the description field to clearly explain the business purpose of each rule.

### Validation Execution

1. **Schedule regular validations**: Set up automated validation as part of your data pipeline.

2. **Validate early**: Run validations as early as possible in the pipeline to catch issues before they propagate.

3. **Validate after transformations**: Add validation steps after major transformations to ensure data quality is maintained.

4. **Monitor validation trends**: Track validation success rates over time to identify deteriorating data quality.

5. **Integrate with self-healing**: Configure self-healing actions for common quality issues to reduce manual intervention.

### API Usage

1. **Use pagination**: Always use pagination parameters when retrieving collections to avoid performance issues.

2. **Filter effectively**: Use the provided filtering parameters to minimize data transfer and processing.

3. **Handle errors gracefully**: Implement comprehensive error handling that accounts for all possible API error responses.

4. **Implement retry logic**: Use exponential backoff for retrying failed requests, especially for validation execution.

5. **Cache where appropriate**: Cache relatively static data like quality rules to reduce API calls.

## Related Resources

- [Authentication](authentication.md): Authentication and authorization details
- [Self-Healing API](healing-api.md): API for managing self-healing actions
- [Monitoring API](monitoring-api.md): API for monitoring pipeline health and metrics