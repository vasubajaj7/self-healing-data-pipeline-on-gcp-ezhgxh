---
id: admin-api
title: Administration API
sidebar_label: Administration
---

import Authentication from './authentication.md';
import { API_BASE_URL } from './constants';
import { CodeBlock } from '@docusaurus/theme-common';
import APIOverview from './overview.md';

## Introduction

The Administration API provides endpoints for managing users, roles, system settings, system health, and audit logs in the Self-Healing Data Pipeline. These endpoints are typically restricted to users with administrative privileges.

This documentation covers all administrative endpoints, their parameters, request/response formats, and examples of usage.

## Authentication

All Administration API endpoints require authentication with appropriate administrative permissions. Please refer to the [Authentication](authentication.md) documentation for details on obtaining and using access tokens.

Most endpoints in this section require one of the following permissions:

- `admin:read` - For read-only operations
- `admin:create` - For creating new resources
- `admin:update` - For updating existing resources
- `admin:delete` - For deleting resources

These permissions are typically assigned to users with the `admin` role.

## Base URL

All Administration API endpoints are relative to the base URL and prefixed with `/admin`:

```
https://api.example.com/api/v1/admin
```

## User Management

Endpoints for managing user accounts in the system.

### List Users

```
GET /admin/users
```

Retrieves a paginated list of users with optional filtering.

**Query Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `page` | integer | Page number (default: 1) |
| `page_size` | integer | Items per page (default: 20, max: 100) |
| `search` | string | Optional search term for username or email |
| `role` | string | Optional filter by role ID |
| `active_only` | boolean | Optional filter for active users only |

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "john.doe",
      "email": "john.doe@example.com",
      "name": "John Doe",
      "role": "admin",
      "active": true,
      "created_at": "2023-01-15T08:30:00Z",
      "updated_at": "2023-02-20T14:15:30Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "username": "jane.smith",
      "email": "jane.smith@example.com",
      "name": "Jane Smith",
      "role": "data_engineer",
      "active": true,
      "created_at": "2023-01-20T10:45:00Z",
      "updated_at": "2023-01-20T10:45:00Z"
    }
  ],
  "metadata": {
    "page": 1,
    "page_size": 20,
    "total": 45,
    "total_pages": 3
  }
}
```

### Get User

```
GET /admin/users/{user_id}
```

Retrieves a specific user by ID.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `user_id` | string (UUID) | ID of the user to retrieve |

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "username": "john.doe",
    "email": "john.doe@example.com",
    "name": "John Doe",
    "role": "admin",
    "active": true,
    "created_at": "2023-01-15T08:30:00Z",
    "updated_at": "2023-02-20T14:15:30Z"
  }
}
```

### Create User

```
POST /admin/users
```

Creates a new user in the system.

**Request Body:**

```json
{
  "username": "new.user",
  "email": "new.user@example.com",
  "password": "secure-password",
  "name": "New User",
  "role": "data_engineer",
  "active": true
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440002",
    "username": "new.user",
    "email": "new.user@example.com",
    "name": "New User",
    "role": "data_engineer",
    "active": true,
    "created_at": "2023-06-15T09:30:00Z",
    "updated_at": "2023-06-15T09:30:00Z"
  }
}
```

### Update User

```
PUT /admin/users/{user_id}
```

Updates an existing user's information.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `user_id` | string (UUID) | ID of the user to update |

**Request Body:**

```json
{
  "email": "updated.email@example.com",
  "name": "Updated Name",
  "role": "quality_manager",
  "active": true
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "username": "john.doe",
    "email": "updated.email@example.com",
    "name": "Updated Name",
    "role": "quality_manager",
    "active": true,
    "created_at": "2023-01-15T08:30:00Z",
    "updated_at": "2023-06-15T10:45:00Z"
  }
}
```

### Delete User

```
DELETE /admin/users/{user_id}
```

Deletes a user from the system.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `user_id` | string (UUID) | ID of the user to delete |

**Response:**

```json
{
  "status": "success",
  "data": {
    "message": "User successfully deleted"
  }
}
```

## Role Management

Endpoints for managing roles and permissions in the system.

### List Roles

```
GET /admin/roles
```

Retrieves a list of all roles in the system.

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440010",
      "name": "admin",
      "description": "Full system access",
      "permissions": ["admin:read", "admin:create", "admin:update", "admin:delete", "pipeline:read", "pipeline:create", "pipeline:update", "pipeline:delete", "quality:read", "quality:create", "quality:update", "quality:delete", "monitoring:read", "monitoring:update"],
      "created_at": "2023-01-01T00:00:00Z",
      "updated_at": "2023-01-01T00:00:00Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440011",
      "name": "data_engineer",
      "description": "Manage data pipelines and sources",
      "permissions": ["pipeline:read", "pipeline:create", "pipeline:update", "pipeline:delete", "quality:read", "monitoring:read"],
      "created_at": "2023-01-01T00:00:00Z",
      "updated_at": "2023-01-01T00:00:00Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440012",
      "name": "quality_manager",
      "description": "Manage data quality rules and validations",
      "permissions": ["quality:read", "quality:create", "quality:update", "quality:delete", "pipeline:read", "monitoring:read"],
      "created_at": "2023-01-01T00:00:00Z",
      "updated_at": "2023-01-01T00:00:00Z"
    }
  ]
}
```

### Get Role

```
GET /admin/roles/{role_id}
```

Retrieves a specific role by ID.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `role_id` | string (UUID) | ID of the role to retrieve |

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440010",
    "name": "admin",
    "description": "Full system access",
    "permissions": ["admin:read", "admin:create", "admin:update", "admin:delete", "pipeline:read", "pipeline:create", "pipeline:update", "pipeline:delete", "quality:read", "quality:create", "quality:update", "quality:delete", "monitoring:read", "monitoring:update"],
    "created_at": "2023-01-01T00:00:00Z",
    "updated_at": "2023-01-01T00:00:00Z"
  }
}
```

### Create Role

```
POST /admin/roles
```

Creates a new role in the system.

**Request Body:**

```json
{
  "name": "custom_role",
  "description": "Custom role with specific permissions",
  "permissions": ["pipeline:read", "quality:read", "monitoring:read"]
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440013",
    "name": "custom_role",
    "description": "Custom role with specific permissions",
    "permissions": ["pipeline:read", "quality:read", "monitoring:read"],
    "created_at": "2023-06-15T11:30:00Z",
    "updated_at": "2023-06-15T11:30:00Z"
  }
}
```

### Update Role

```
PUT /admin/roles/{role_id}
```

Updates an existing role's information.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `role_id` | string (UUID) | ID of the role to update |

**Request Body:**

```json
{
  "description": "Updated role description",
  "permissions": ["pipeline:read", "quality:read", "quality:create", "monitoring:read"]
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "550e8400-e29b-41d4-a716-446655440013",
    "name": "custom_role",
    "description": "Updated role description",
    "permissions": ["pipeline:read", "quality:read", "quality:create", "monitoring:read"],
    "created_at": "2023-06-15T11:30:00Z",
    "updated_at": "2023-06-15T12:45:00Z"
  }
}
```

### Delete Role

```
DELETE /admin/roles/{role_id}
```

Deletes a role from the system. This will fail if any users are currently assigned this role.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `role_id` | string (UUID) | ID of the role to delete |

**Response:**

```json
{
  "status": "success",
  "data": {
    "message": "Role successfully deleted"
  }
}
```

## System Settings

Endpoints for managing system-wide settings.

### Get System Settings

```
GET /admin/settings
```

Retrieves the current system settings.

**Response:**

```json
{
  "status": "success",
  "data": {
    "general": {
      "system_name": "Self-Healing Data Pipeline",
      "environment": "production",
      "maintenance_mode": false
    },
    "security": {
      "token_expiry_minutes": 60,
      "max_login_attempts": 5,
      "password_policy": {
        "min_length": 10,
        "require_uppercase": true,
        "require_lowercase": true,
        "require_numbers": true,
        "require_special_chars": true
      },
      "mfa_enabled": true
    },
    "pipeline": {
      "default_retry_count": 3,
      "default_timeout_minutes": 60,
      "max_concurrent_pipelines": 10
    },
    "quality": {
      "default_quality_threshold": 0.9,
      "auto_validation_enabled": true
    },
    "self_healing": {
      "enabled": true,
      "confidence_threshold": 0.85,
      "max_auto_correction_attempts": 3
    },
    "monitoring": {
      "alert_retention_days": 90,
      "metrics_retention_days": 30,
      "default_alert_channels": ["email", "teams"]
    },
    "updated_at": "2023-05-10T14:30:00Z"
  }
}
```

### Update System Settings

```
PUT /admin/settings
```

Updates the system settings.

**Request Body:**

```json
{
  "general": {
    "maintenance_mode": true
  },
  "security": {
    "token_expiry_minutes": 30
  },
  "self_healing": {
    "confidence_threshold": 0.9
  }
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "general": {
      "system_name": "Self-Healing Data Pipeline",
      "environment": "production",
      "maintenance_mode": true
    },
    "security": {
      "token_expiry_minutes": 30,
      "max_login_attempts": 5,
      "password_policy": {
        "min_length": 10,
        "require_uppercase": true,
        "require_lowercase": true,
        "require_numbers": true,
        "require_special_chars": true
      },
      "mfa_enabled": true
    },
    "pipeline": {
      "default_retry_count": 3,
      "default_timeout_minutes": 60,
      "max_concurrent_pipelines": 10
    },
    "quality": {
      "default_quality_threshold": 0.9,
      "auto_validation_enabled": true
    },
    "self_healing": {
      "enabled": true,
      "confidence_threshold": 0.9,
      "max_auto_correction_attempts": 3
    },
    "monitoring": {
      "alert_retention_days": 90,
      "metrics_retention_days": 30,
      "default_alert_channels": ["email", "teams"]
    },
    "updated_at": "2023-06-15T13:45:00Z"
  }
}
```

## System Health

Endpoints for monitoring system health and component status.

### Get System Health

```
GET /admin/health
```

Retrieves the current system health status.

**Response:**

```json
{
  "status": "success",
  "data": {
    "status": "healthy",
    "timestamp": "2023-06-15T14:30:00Z",
    "components": {
      "bigquery": {
        "status": "healthy",
        "latency_ms": 120,
        "last_checked": "2023-06-15T14:29:55Z"
      },
      "cloud_storage": {
        "status": "healthy",
        "latency_ms": 85,
        "last_checked": "2023-06-15T14:29:56Z"
      },
      "cloud_composer": {
        "status": "healthy",
        "latency_ms": 210,
        "last_checked": "2023-06-15T14:29:57Z"
      },
      "vertex_ai": {
        "status": "healthy",
        "latency_ms": 180,
        "last_checked": "2023-06-15T14:29:58Z"
      },
      "data_quality": {
        "status": "healthy",
        "latency_ms": 95,
        "last_checked": "2023-06-15T14:29:59Z"
      },
      "self_healing": {
        "status": "healthy",
        "latency_ms": 110,
        "last_checked": "2023-06-15T14:30:00Z"
      }
    },
    "metrics": {
      "cpu_usage": 0.35,
      "memory_usage": 0.42,
      "disk_usage": 0.28,
      "active_pipelines": 3,
      "queued_tasks": 5
    }
  }
}
```

### Check Component Status

```
GET /admin/health/{component_name}
```

Checks the status of a specific system component.

**Path Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `component_name` | string | Name of the component to check (e.g., bigquery, cloud_storage, cloud_composer, vertex_ai, data_quality, self_healing) |

**Response:**

```json
{
  "status": "success",
  "data": {
    "component": "bigquery",
    "status": "healthy",
    "latency_ms": 120,
    "last_checked": "2023-06-15T14:35:00Z",
    "details": {
      "service_status": "available",
      "query_performance": "normal",
      "slot_utilization": 0.45,
      "recent_errors": 0
    }
  }
}
```

## Audit Logs

Endpoints for retrieving system audit logs.

### Get Audit Logs

```
GET /admin/audit-logs
```

Retrieves system audit logs with filtering options.

**Query Parameters:**

| Parameter | Type | Description |
| --- | --- | --- |
| `page` | integer | Page number (default: 1) |
| `page_size` | integer | Items per page (default: 20, max: 100) |
| `start_date` | string (ISO date) | Optional filter for logs after this date |
| `end_date` | string (ISO date) | Optional filter for logs before this date |
| `user_id` | string (UUID) | Optional filter by user ID |
| `action_type` | string | Optional filter by action type (e.g., create, update, delete) |
| `resource_type` | string | Optional filter by resource type (e.g., user, role, pipeline) |

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440100",
      "timestamp": "2023-06-15T13:45:00Z",
      "user_id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "john.doe",
      "action_type": "update",
      "resource_type": "system_settings",
      "resource_id": null,
      "details": {
        "changes": {
          "general.maintenance_mode": [false, true],
          "security.token_expiry_minutes": [60, 30],
          "self_healing.confidence_threshold": [0.85, 0.9]
        }
      },
      "ip_address": "192.168.1.100"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440101",
      "timestamp": "2023-06-15T12:30:00Z",
      "user_id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "john.doe",
      "action_type": "create",
      "resource_type": "user",
      "resource_id": "550e8400-e29b-41d4-a716-446655440002",
      "details": {
        "username": "new.user",
        "email": "new.user@example.com",
        "role": "data_engineer"
      },
      "ip_address": "192.168.1.100"
    }
  ],
  "metadata": {
    "page": 1,
    "page_size": 20,
    "total": 156,
    "total_pages": 8
  }
}
```

## Authentication

Endpoint for user authentication and token generation.

### Login

```
POST /admin/login
```

Authenticates a user and returns an access token.

**Request Body:**

```json
{
  "username_or_email": "john.doe@example.com",
  "password": "secure-password"
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 1800,
    "user": {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "john.doe",
      "email": "john.doe@example.com",
      "name": "John Doe",
      "role": "admin"
    }
  }
}
```

For more details on authentication, token refresh, and logout, please refer to the [Authentication](authentication.md) documentation.

## Error Responses

The Administration API uses standard error responses as described in the [API Overview](overview.md) documentation. Here are some specific error codes you might encounter:

### 400 Bad Request

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Invalid request - Missing required field 'username'"
}
```

### 401 Unauthorized

```json
{
  "status": "error",
  "error": "AUTH_ERROR",
  "message": "Invalid or expired token"
}
```

### 403 Forbidden

```json
{
  "status": "error",
  "error": "PERMISSION_DENIED",
  "message": "User does not have permission to perform this action"
}
```

### 404 Not Found

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "User with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

### 409 Conflict

```json
{
  "status": "error",
  "error": "RESOURCE_IN_USE",
  "message": "Cannot delete role as it is assigned to active users"
}
```

### 422 Unprocessable Entity

```json
{
  "status": "error",
  "error": "CONFIGURATION_ERROR",
  "message": "Cannot apply settings - Invalid configuration value for 'token_expiry_minutes'"
}
```

## Best Practices

When working with the Administration API, follow these best practices:

1. **Limit Administrative Access**: Restrict admin permissions to only those users who absolutely need them.

2. **Audit Regularly**: Regularly review the audit logs to monitor administrative actions.

3. **Use Strong Passwords**: Enforce strong password policies for all users, especially those with administrative access.

4. **Implement MFA**: Enable multi-factor authentication for administrative accounts.

5. **Minimize Sensitive Data**: Avoid including sensitive data in API requests and responses.

6. **Test Changes in Non-Production**: Always test configuration changes in a non-production environment first.

7. **Automate with Caution**: When automating administrative tasks, implement proper error handling and validation.

8. **Regular Health Checks**: Implement regular system health checks to proactively identify issues.

## Code Examples

### User Management with Python
```python
import requests

def get_admin_api_data(endpoint, token, params=None):
    """Generic function to get data from the Admin API
    
    Args:
        endpoint: API endpoint path (without base URL)
        token: Authentication token
        params: Optional query parameters
        
    Returns:
        API response data
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f'https://api.example.com/api/v1/admin{endpoint}',
        headers=headers,
        params=params
    )
    
    response.raise_for_status()  # Raise exception for error status codes
    return response.json()['data']

def post_admin_api_data(endpoint, token, data):
    """Generic function to post data to the Admin API
    
    Args:
        endpoint: API endpoint path (without base URL)
        token: Authentication token
        data: Request body data
        
    Returns:
        API response data
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.post(
        f'https://api.example.com/api/v1/admin{endpoint}',
        headers=headers,
        json=data
    )
    
    response.raise_for_status()  # Raise exception for error status codes
    return response.json()['data']

# Example: Get all users
def get_users(token, page=1, page_size=20, search=None, role=None, active_only=None):
    params = {
        'page': page,
        'page_size': page_size
    }
    
    if search:
        params['search'] = search
    if role:
        params['role'] = role
    if active_only is not None:
        params['active_only'] = str(active_only).lower()
    
    return get_admin_api_data('/users', token, params)

# Example: Create a new user
def create_user(token, user_data):
    return post_admin_api_data('/users', token, user_data)

# Example usage
try:
    token = 'YOUR_ACCESS_TOKEN'
    
    # Get all admin users
    users = get_users(token, role='admin')
    print(f"Found {len(users)} admin users")
    
    # Create a new user
    new_user = {
        "username": "new.user",
        "email": "new.user@example.com",
        "password": "secure-password",
        "name": "New User",
        "role": "data_engineer",
        "active": True
    }
    
    created_user = create_user(token, new_user)
    print(f"Created new user with ID: {created_user['id']}")
    
except requests.exceptions.HTTPError as e:
    print(f'API Error: {e}')
```

### System Health Monitoring with JavaScript