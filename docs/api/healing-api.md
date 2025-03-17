# Self-Healing API Documentation

## Introduction

The Self-Healing API provides programmatic access to the intelligent self-healing capabilities of the data pipeline. This API allows you to define issue detection patterns, configure automated remediation actions, execute healing operations, and monitor the effectiveness of the self-healing system.

Key features of the Self-Healing API include:

- Creating and managing healing patterns that detect data issues
- Configuring automated actions for common problems
- Monitoring healing executions and success rates
- Adjusting global healing behavior and confidence thresholds
- Manually triggering healing operations when needed
- Retrieving statistics about healing effectiveness

This API enables both fully automated healing workflows and manual intervention when necessary, providing flexibility in how issues are addressed while maintaining full visibility into all healing activities.

## Authentication

The Self-Healing API uses the same authentication mechanisms as other pipeline APIs. All requests require a valid authentication token with appropriate permissions.

For detailed authentication information, refer to the [API Authentication documentation](../authentication.md).

## Base URL

All endpoints in this documentation are relative to the base URL:

```
/healing
```

## Healing Patterns

Healing patterns define how the system identifies issues requiring remediation. Each pattern specifies an issue type and detection criteria.

### List Healing Patterns

Retrieves a paginated list of healing patterns with optional filtering.

**Endpoint:** `GET /patterns`

**Query Parameters:**
- `page` (integer, optional): Page number (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20)
- `issue_type` (string, optional): Filter by issue type (e.g., DATA_QUALITY, PIPELINE_FAILURE)

**Response:**
```json
{
  "status": "success",
  "message": "Healing patterns retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:45Z"
  },
  "items": [
    {
      "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
      "issue_type": "DATA_QUALITY",
      "detection_pattern": {
        "field_patterns": {
          "null_percentage": {
            "operator": "gt",
            "threshold": 10
          }
        },
        "table_name": "customer_data"
      },
      "confidence_threshold": 0.85,
      "description": "Detects high null percentage in customer data table",
      "created_at": "2023-05-10T08:15:30Z",
      "updated_at": "2023-05-10T08:15:30Z",
      "metadata": {
        "created_by": "system",
        "priority": "high"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 42,
    "total_pages": 3
  }
}
```

### Get Healing Pattern by ID

Retrieves a specific healing pattern by its unique identifier.

**Endpoint:** `GET /patterns/{pattern_id}`

**Path Parameters:**
- `pattern_id` (string, required): Unique identifier of the healing pattern

**Response:**
```json
{
  "status": "success",
  "message": "Healing pattern retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:32:15Z"
  },
  "data": {
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "issue_type": "DATA_QUALITY",
    "detection_pattern": {
      "field_patterns": {
        "null_percentage": {
          "operator": "gt",
          "threshold": 10
        }
      },
      "table_name": "customer_data"
    },
    "confidence_threshold": 0.85,
    "description": "Detects high null percentage in customer data table",
    "created_at": "2023-05-10T08:15:30Z",
    "updated_at": "2023-05-10T08:15:30Z",
    "metadata": {
      "created_by": "system",
      "priority": "high"
    }
  }
}
```

### Create Healing Pattern

Creates a new healing pattern in the system.

**Endpoint:** `POST /patterns`

**Request Body:**
```json
{
  "issue_type": "DATA_QUALITY",
  "detection_pattern": {
    "field_patterns": {
      "null_percentage": {
        "operator": "gt",
        "threshold": 10
      }
    },
    "table_name": "customer_data"
  },
  "confidence_threshold": 0.85,
  "description": "Detects high null percentage in customer data table",
  "metadata": {
    "priority": "high"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Healing pattern created successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:40:22Z"
  },
  "data": {
    "pattern_id": "670e8400-e29b-41d4-a716-446655440123",
    "issue_type": "DATA_QUALITY",
    "detection_pattern": {
      "field_patterns": {
        "null_percentage": {
          "operator": "gt",
          "threshold": 10
        }
      },
      "table_name": "customer_data"
    },
    "confidence_threshold": 0.85,
    "description": "Detects high null percentage in customer data table",
    "created_at": "2023-06-15T10:40:22Z",
    "updated_at": "2023-06-15T10:40:22Z",
    "metadata": {
      "priority": "high"
    }
  }
}
```

### Update Healing Pattern

Updates an existing healing pattern.

**Endpoint:** `PUT /patterns/{pattern_id}`

**Path Parameters:**
- `pattern_id` (string, required): Unique identifier of the healing pattern to update

**Request Body:**
```json
{
  "confidence_threshold": 0.9,
  "description": "Updated pattern for null percentage detection",
  "metadata": {
    "priority": "critical"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Healing pattern updated successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:45:12Z"
  },
  "data": {
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "issue_type": "DATA_QUALITY",
    "detection_pattern": {
      "field_patterns": {
        "null_percentage": {
          "operator": "gt",
          "threshold": 10
        }
      },
      "table_name": "customer_data"
    },
    "confidence_threshold": 0.9,
    "description": "Updated pattern for null percentage detection",
    "created_at": "2023-05-10T08:15:30Z",
    "updated_at": "2023-06-15T10:45:12Z",
    "metadata": {
      "created_by": "system",
      "priority": "critical"
    }
  }
}
```

### Delete Healing Pattern

Deletes a healing pattern from the system.

**Endpoint:** `DELETE /patterns/{pattern_id}`

**Path Parameters:**
- `pattern_id` (string, required): Unique identifier of the healing pattern to delete

**Response:**
```json
{
  "status": "success",
  "message": "Healing pattern deleted successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:50:05Z"
  },
  "data": {
    "deleted_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

## Healing Actions

Healing actions define the automated remediation strategies applied when issues are detected. Each action is associated with a specific healing pattern.

### List Healing Actions

Retrieves a paginated list of healing actions with optional filtering.

**Endpoint:** `GET /actions`

**Query Parameters:**
- `page` (integer, optional): Page number (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20)
- `pattern_id` (string, optional): Filter by associated pattern ID
- `action_type` (string, optional): Filter by action type
- `active_only` (boolean, optional): When true, returns only active actions

**Response:**
```json
{
  "status": "success",
  "message": "Healing actions retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:00:30Z"
  },
  "items": [
    {
      "action_id": "650e8400-e29b-41d4-a716-446655440111",
      "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
      "action_type": "DATA_CORRECTION",
      "action_definition": {
        "correction_type": "IMPUTATION",
        "parameters": {
          "method": "mean",
          "columns": ["age", "income"]
        }
      },
      "is_active": true,
      "success_rate": 0.92,
      "description": "Impute missing values with column mean",
      "created_at": "2023-05-15T09:20:10Z",
      "updated_at": "2023-05-15T09:20:10Z",
      "metadata": {
        "created_by": "system"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 35,
    "total_pages": 2
  }
}
```

### Get Healing Action by ID

Retrieves a specific healing action by its unique identifier.

**Endpoint:** `GET /actions/{action_id}`

**Path Parameters:**
- `action_id` (string, required): Unique identifier of the healing action

**Response:**
```json
{
  "status": "success",
  "message": "Healing action retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:05:20Z"
  },
  "data": {
    "action_id": "650e8400-e29b-41d4-a716-446655440111",
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "action_type": "DATA_CORRECTION",
    "action_definition": {
      "correction_type": "IMPUTATION",
      "parameters": {
        "method": "mean",
        "columns": ["age", "income"]
      }
    },
    "is_active": true,
    "success_rate": 0.92,
    "description": "Impute missing values with column mean",
    "created_at": "2023-05-15T09:20:10Z",
    "updated_at": "2023-05-15T09:20:10Z",
    "metadata": {
      "created_by": "system"
    }
  }
}
```

### Create Healing Action

Creates a new healing action in the system.

**Endpoint:** `POST /actions`

**Request Body:**
```json
{
  "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
  "action_type": "DATA_CORRECTION",
  "action_definition": {
    "correction_type": "IMPUTATION",
    "parameters": {
      "method": "mean",
      "columns": ["age", "income"]
    }
  },
  "is_active": true,
  "description": "Impute missing values with column mean",
  "metadata": {
    "priority": "high"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Healing action created successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:10:45Z"
  },
  "data": {
    "action_id": "750e8400-e29b-41d4-a716-446655440222",
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "action_type": "DATA_CORRECTION",
    "action_definition": {
      "correction_type": "IMPUTATION",
      "parameters": {
        "method": "mean",
        "columns": ["age", "income"]
      }
    },
    "is_active": true,
    "success_rate": 0.0,
    "description": "Impute missing values with column mean",
    "created_at": "2023-06-15T11:10:45Z",
    "updated_at": "2023-06-15T11:10:45Z",
    "metadata": {
      "priority": "high"
    }
  }
}
```

### Update Healing Action

Updates an existing healing action.

**Endpoint:** `PUT /actions/{action_id}`

**Path Parameters:**
- `action_id` (string, required): Unique identifier of the healing action to update

**Request Body:**
```json
{
  "action_definition": {
    "correction_type": "IMPUTATION",
    "parameters": {
      "method": "median",
      "columns": ["age", "income", "experience"]
    }
  },
  "is_active": true,
  "description": "Updated: Impute missing values with column median"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Healing action updated successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:15:30Z"
  },
  "data": {
    "action_id": "650e8400-e29b-41d4-a716-446655440111",
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "action_type": "DATA_CORRECTION",
    "action_definition": {
      "correction_type": "IMPUTATION",
      "parameters": {
        "method": "median",
        "columns": ["age", "income", "experience"]
      }
    },
    "is_active": true,
    "success_rate": 0.92,
    "description": "Updated: Impute missing values with column median",
    "created_at": "2023-05-15T09:20:10Z",
    "updated_at": "2023-06-15T11:15:30Z",
    "metadata": {
      "created_by": "system",
      "priority": "critical"
    }
  }
}
```

### Delete Healing Action

Deletes a healing action from the system.

**Endpoint:** `DELETE /actions/{action_id}`

**Path Parameters:**
- `action_id` (string, required): Unique identifier of the healing action to delete

**Response:**
```json
{
  "status": "success",
  "message": "Healing action deleted successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:20:15Z"
  },
  "data": {
    "deleted_id": "650e8400-e29b-41d4-a716-446655440111"
  }
}
```

## Healing Executions

Healing executions represent instances when the system has attempted to apply healing actions to resolve detected issues.

### List Healing Executions

Retrieves a paginated list of healing executions with optional filtering.

**Endpoint:** `GET /executions`

**Query Parameters:**
- `page` (integer, optional): Page number (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20)
- `start_date` (string, optional): Filter by execution date range start (ISO 8601 format)
- `end_date` (string, optional): Filter by execution date range end (ISO 8601 format)
- `execution_id` (string, optional): Filter by pipeline execution ID
- `pattern_id` (string, optional): Filter by pattern ID
- `action_id` (string, optional): Filter by action ID
- `successful_only` (boolean, optional): When true, returns only successful executions

**Response:**
```json
{
  "status": "success",
  "message": "Healing executions retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:30:45Z"
  },
  "items": [
    {
      "healing_id": "850e8400-e29b-41d4-a716-446655440333",
      "execution_id": "pipeline-exec-20230615-001",
      "validation_id": "validation-20230615-001",
      "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
      "action_id": "650e8400-e29b-41d4-a716-446655440111",
      "execution_time": "2023-06-15T10:25:30Z",
      "healing_status": "SUCCESS",
      "successful": true,
      "execution_details": {
        "corrected_records": 152,
        "execution_duration_ms": 1236,
        "confidence_score": 0.95
      },
      "error_message": null,
      "metadata": {
        "pipeline_name": "customer_data_daily"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 128,
    "total_pages": 7
  }
}
```

### Get Healing Execution by ID

Retrieves a specific healing execution by its unique identifier.

**Endpoint:** `GET /executions/{healing_id}`

**Path Parameters:**
- `healing_id` (string, required): Unique identifier of the healing execution

**Response:**
```json
{
  "status": "success",
  "message": "Healing execution retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:35:20Z"
  },
  "data": {
    "healing_id": "850e8400-e29b-41d4-a716-446655440333",
    "execution_id": "pipeline-exec-20230615-001",
    "validation_id": "validation-20230615-001",
    "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
    "action_id": "650e8400-e29b-41d4-a716-446655440111",
    "execution_time": "2023-06-15T10:25:30Z",
    "healing_status": "SUCCESS",
    "successful": true,
    "execution_details": {
      "corrected_records": 152,
      "execution_duration_ms": 1236,
      "confidence_score": 0.95
    },
    "error_message": null,
    "metadata": {
      "pipeline_name": "customer_data_daily"
    }
  }
}
```

## Healing Configuration

The healing configuration controls the global behavior of the self-healing system.

### Get Healing Configuration

Retrieves the current self-healing configuration.

**Endpoint:** `GET /config`

**Response:**
```json
{
  "status": "success",
  "message": "Healing configuration retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:40:30Z"
  },
  "data": {
    "healing_mode": "SEMI_AUTOMATIC",
    "global_confidence_threshold": 0.85,
    "max_retry_attempts": 3,
    "approval_required_high_impact": true,
    "learning_mode_active": true,
    "additional_settings": {
      "notification_channel": "teams",
      "logging_level": "detailed"
    },
    "updated_at": "2023-06-01T09:00:00Z",
    "updated_by": "admin"
  }
}
```

### Update Healing Configuration

Updates the self-healing configuration.

**Endpoint:** `PUT /config`

**Request Body:**
```json
{
  "healing_mode": "AUTOMATIC",
  "global_confidence_threshold": 0.9,
  "max_retry_attempts": 5,
  "approval_required_high_impact": true,
  "learning_mode_active": true,
  "additional_settings": {
    "notification_channel": "teams",
    "logging_level": "detailed",
    "alert_on_failure": true
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Healing configuration updated successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:45:15Z"
  },
  "data": {
    "healing_mode": "AUTOMATIC",
    "global_confidence_threshold": 0.9,
    "max_retry_attempts": 5,
    "approval_required_high_impact": true,
    "learning_mode_active": true,
    "additional_settings": {
      "notification_channel": "teams",
      "logging_level": "detailed",
      "alert_on_failure": true
    },
    "updated_at": "2023-06-15T11:45:15Z",
    "updated_by": "admin"
  }
}
```

## Manual Healing

The manual healing endpoint allows you to trigger healing actions for specific issues on demand.

### Execute Manual Healing

Manually triggers a healing action for a specific issue.

**Endpoint:** `POST /execute`

**Request Body:**
```json
{
  "issue_id": "issue-20230615-001",
  "action_id": "650e8400-e29b-41d4-a716-446655440111",
  "parameters": {
    "threshold": 20,
    "apply_to_fields": ["customer_name", "address"]
  },
  "notes": "Manual healing for customer data quality issue"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Manual healing executed successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:50:30Z"
  },
  "healing_id": "950e8400-e29b-41d4-a716-446655440444",
  "success": true,
  "execution_details": {
    "corrected_records": 45,
    "execution_duration_ms": 872,
    "affected_tables": ["customer_data"]
  },
  "error_message": null
}
```

## Statistics

The statistics endpoint provides aggregate information about healing operations and effectiveness.

### Get Healing Statistics

Retrieves statistics about self-healing operations.

**Endpoint:** `GET /statistics`

**Query Parameters:**
- `start_date` (string, optional): Start date for statistics period (ISO 8601 format)
- `end_date` (string, optional): End date for statistics period (ISO 8601 format)

**Response:**
```json
{
  "status": "success",
  "message": "Healing statistics retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T12:00:45Z",
    "period": {
      "start_date": "2023-05-15T00:00:00Z",
      "end_date": "2023-06-15T23:59:59Z"
    }
  },
  "data": {
    "total_executions": 487,
    "successful_executions": 423,
    "failed_executions": 64,
    "success_rate": 86.9,
    "average_execution_time_ms": 1250,
    "issues_by_type": {
      "DATA_QUALITY": 312,
      "PIPELINE_FAILURE": 98,
      "RESOURCE_CONSTRAINT": 45,
      "SCHEMA_DRIFT": 32
    },
    "actions_by_type": {
      "DATA_CORRECTION": 312,
      "PIPELINE_RETRY": 98,
      "RESOURCE_SCALING": 45,
      "SCHEMA_EVOLUTION": 32
    },
    "top_patterns": [
      {
        "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
        "issue_type": "DATA_QUALITY",
        "occurrences": 145,
        "success_rate": 92.4
      }
    ],
    "healing_trend": [
      {
        "date": "2023-05-15",
        "executions": 15,
        "success_rate": 80.0
      }
    ]
  }
}
```

## Data Models

### Enumerations

#### SelfHealingMode
Operational modes for the self-healing system.

- `DISABLED`: Self-healing is completely disabled
- `RECOMMENDATION_ONLY`: System only recommends actions but doesn't execute them
- `SEMI_AUTOMATIC`: System automatically executes low-impact actions but requires approval for high-impact actions
- `AUTOMATIC`: System automatically executes all healing actions

#### HealingActionType
Types of healing actions that can be performed.

- `DATA_CORRECTION`: Correction of data quality issues
- `PIPELINE_RETRY`: Retry of failed pipeline steps
- `PARAMETER_ADJUSTMENT`: Adjustment of pipeline parameters
- `RESOURCE_SCALING`: Scaling of compute resources
- `SCHEMA_EVOLUTION`: Evolution of data schema to accommodate changes
- `DEPENDENCY_RESOLUTION`: Resolution of dependency issues

#### HealingStatus
Status of a healing execution.

- `PENDING`: Healing is scheduled but not yet started
- `IN_PROGRESS`: Healing is currently in progress
- `SUCCESS`: Healing completed successfully
- `FAILED`: Healing failed
- `APPROVAL_REQUIRED`: Healing requires manual approval
- `APPROVED`: Healing was approved
- `REJECTED`: Healing was rejected

### Request Models

#### HealingPatternCreateRequest
Request model for creating a new healing pattern.

```json
{
  "issue_type": "DATA_QUALITY",
  "detection_pattern": {
    "field_patterns": {
      "null_percentage": {
        "operator": "gt",
        "threshold": 10
      }
    },
    "table_name": "customer_data"
  },
  "confidence_threshold": 0.85,
  "description": "Detects high null percentage in customer data table",
  "metadata": {
    "priority": "high"
  }
}
```

#### HealingPatternUpdateRequest
Request model for updating an existing healing pattern.

```json
{
  "confidence_threshold": 0.9,
  "description": "Updated pattern for null percentage detection",
  "metadata": {
    "priority": "critical"
  }
}
```

#### HealingActionCreateRequest
Request model for creating a new healing action.

```json
{
  "pattern_id": "550e8400-e29b-41d4-a716-446655440000",
  "action_type": "DATA_CORRECTION",
  "action_definition": {
    "correction_type": "IMPUTATION",
    "parameters": {
      "method": "mean",
      "columns": ["age", "income"]
    }
  },
  "is_active": true,
  "description": "Impute missing values with column mean",
  "metadata": {
    "priority": "high"
  }
}
```

#### HealingActionUpdateRequest
Request model for updating an existing healing action.

```json
{
  "action_definition": {
    "correction_type": "IMPUTATION",
    "parameters": {
      "method": "median",
      "columns": ["age", "income", "experience"]
    }
  },
  "is_active": true,
  "description": "Updated: Impute missing values with column median",
  "metadata": {
    "priority": "critical"
  }
}
```

#### HealingConfigUpdateRequest
Request model for updating the global healing configuration.

```json
{
  "healing_mode": "AUTOMATIC",
  "global_confidence_threshold": 0.9,
  "max_retry_attempts": 5,
  "approval_required_high_impact": true,
  "learning_mode_active": true,
  "additional_settings": {
    "notification_channel": "teams",
    "logging_level": "detailed",
    "alert_on_failure": true
  }
}
```

#### ManualHealingRequest
Request model for manually triggering a healing action.

```json
{
  "issue_id": "issue-20230615-001",
  "action_id": "650e8400-e29b-41d4-a716-446655440111",
  "parameters": {
    "threshold": 20,
    "apply_to_fields": ["customer_name", "address"]
  },
  "notes": "Manual healing for customer data quality issue"
}
```

### Response Models

All API responses follow a standard format with the following structure:

```json
{
  "status": "success",
  "message": "Human-readable message",
  "metadata": {
    "timestamp": "2023-06-15T12:00:00Z"
  },
  "data": {}
}
```

For paginated responses, the structure includes items and pagination metadata:

```json
{
  "status": "success",
  "message": "Human-readable message",
  "metadata": {
    "timestamp": "2023-06-15T12:00:00Z"
  },
  "items": [],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 100,
    "total_pages": 5
  }
}
```

## Error Responses

When an error occurs, the API returns an appropriate HTTP status code along with details in the response body:

```json
{
  "status": "error",
  "message": "Error description",
  "metadata": {
    "timestamp": "2023-06-15T12:00:00Z"
  },
  "error": {
    "code": "ERROR_CODE",
    "details": "Additional error details"
  }
}
```

Common error status codes:
- `400 Bad Request`: Invalid request parameters or body
- `401 Unauthorized`: Authentication failure
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Requested resource not found
- `409 Conflict`: Request conflicts with current state
- `422 Unprocessable Entity`: Request validation failed
- `500 Internal Server Error`: Server-side error

## Usage Examples

### Creating a Healing Pattern and Action

```python
import requests

# Authentication (see Authentication documentation)
headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
    'Content-Type': 'application/json'
}

# 1. Create a healing pattern
pattern_data = {
    "issue_type": "DATA_QUALITY",
    "detection_pattern": {
        "field_patterns": {
            "null_percentage": {
                "operator": "gt",
                "threshold": 10
            }
        },
        "table_name": "customer_data"
    },
    "confidence_threshold": 0.85,
    "description": "Detects high null percentage in customer data table"
}

pattern_response = requests.post(
    'https://api.example.com/healing/patterns',
    headers=headers,
    json=pattern_data
)
pattern_id = pattern_response.json()['data']['pattern_id']

# 2. Create a healing action for the pattern
action_data = {
    "pattern_id": pattern_id,
    "action_type": "DATA_CORRECTION",
    "action_definition": {
        "correction_type": "IMPUTATION",
        "parameters": {
            "method": "mean",
            "columns": ["age", "income"]
        }
    },
    "is_active": True,
    "description": "Impute missing values with column mean"
}

action_response = requests.post(
    'https://api.example.com/healing/actions',
    headers=headers,
    json=action_data
)
print(f"Action created: {action_response.json()['data']['action_id']}")
```

### Executing a Manual Healing Action

```python
import requests

# Authentication (see Authentication documentation)
headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN',
    'Content-Type': 'application/json'
}

# Execute manual healing
healing_data = {
    "issue_id": "issue-20230615-001",
    "action_id": "650e8400-e29b-41d4-a716-446655440111",
    "parameters": {
        "threshold": 20,
        "apply_to_fields": ["customer_name", "address"]
    },
    "notes": "Manual healing for customer data quality issue"
}

response = requests.post(
    'https://api.example.com/healing/execute',
    headers=headers,
    json=healing_data
)

if response.status_code == 200:
    result = response.json()
    if result['success']:
        print(f"Healing successful! ID: {result['healing_id']}")
        print(f"Corrected {result['execution_details']['corrected_records']} records")
    else:
        print(f"Healing failed: {result['error_message']}")
else:
    print(f"Request failed: {response.status_code}")
    print(response.text)
```

### Retrieving Self-Healing Statistics

```python
import requests
from datetime import datetime, timedelta

# Authentication (see Authentication documentation)
headers = {
    'Authorization': 'Bearer YOUR_ACCESS_TOKEN'
}

# Get statistics for the last 30 days
end_date = datetime.now().isoformat()
start_date = (datetime.now() - timedelta(days=30)).isoformat()

response = requests.get(
    f'https://api.example.com/healing/statistics?start_date={start_date}&end_date={end_date}',
    headers=headers
)

stats = response.json()['data']
print(f"Total executions: {stats['total_executions']}")
print(f"Success rate: {stats['success_rate']}%")
print(f"Average execution time: {stats['average_execution_time_ms']}ms")

# Display issue type breakdown
print("\nIssues by type:")
for issue_type, count in stats['issues_by_type'].items():
    print(f"  {issue_type}: {count}")

# Display top patterns
print("\nTop healing patterns:")
for pattern in stats['top_patterns'][:3]:
    print(f"  {pattern['issue_type']} (ID: {pattern['pattern_id']}):")
    print(f"    Occurrences: {pattern['occurrences']}")
    print(f"    Success rate: {pattern['success_rate']}%")
```