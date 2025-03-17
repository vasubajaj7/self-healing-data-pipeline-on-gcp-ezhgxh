---
id: api-overview
title: API Overview
sidebar_label: Overview
---

import Authentication from './authentication.md';
import IngestionAPI from './ingestion-api.md';
import QualityAPI from './quality-api.md';
import HealingAPI from './healing-api.md';
import MonitoringAPI from './monitoring-api.md';

## Introduction

The Self-Healing Data Pipeline API provides programmatic access to all aspects of the pipeline, including data ingestion, quality validation, self-healing capabilities, monitoring, and optimization. This RESTful API enables integration with external systems, automation of pipeline operations, and custom dashboard development.

This documentation provides comprehensive information about the API endpoints, authentication mechanisms, request/response formats, and best practices for integration.

## API Overview

The API is organized into the following main areas:

- **Authentication**: Secure access to API endpoints using OAuth 2.0 with JWT tokens
- **Data Ingestion**: Endpoints for managing data sources, pipeline definitions, and executions
- **Data Quality**: Endpoints for managing quality rules, validations, and quality issues
- **Self-Healing**: Endpoints for managing healing patterns, actions, and executions
- **Monitoring & Alerting**: Endpoints for metrics, alerts, anomalies, and dashboard data
- **Optimization**: Endpoints for query optimization, schema optimization, and resource management

Each area has its own dedicated documentation page with detailed endpoint specifications, request/response formats, and examples.

## Base URL

All API endpoints are relative to the base URL:

```
https://api.example.com/api/v1
```

For example, the full URL for the metrics endpoint would be:

```
https://api.example.com/api/v1/monitoring/metrics/
```

## Authentication

All API endpoints require authentication using OAuth 2.0 with JWT tokens. Please refer to the [Authentication](authentication.md) documentation for detailed information on obtaining and using access tokens.

In general, you'll need to include an `Authorization` header with a valid token in all API requests:

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

## API Conventions

### Request Format

API requests that include a body should use JSON format with the `Content-Type: application/json` header.

### Response Format

All API responses use a standard JSON format with the following structure:

```json
{
  "status": "success",
  "data": { ... }
}
```

For error responses, the format is:

```json
{
  "status": "error",
  "error": "ERROR_CODE",
  "message": "Human-readable error message"
}
```

### HTTP Status Codes

The API uses standard HTTP status codes to indicate the success or failure of requests:

- `200 OK`: The request was successful
- `201 Created`: The resource was successfully created
- `400 Bad Request`: The request was invalid or malformed
- `401 Unauthorized`: Authentication is required or failed
- `403 Forbidden`: The authenticated user doesn't have permission
- `404 Not Found`: The requested resource doesn't exist
- `409 Conflict`: The request conflicts with the current state
- `422 Unprocessable Entity`: The request was well-formed but cannot be processed
- `500 Internal Server Error`: An unexpected error occurred on the server

### Pagination

Endpoints that return collections support pagination through `page` and `page_size` query parameters. Responses include pagination metadata:

```json
{
  "status": "success",
  "data": [ ... ],
  "metadata": {
    "page": 1,
    "page_size": 20,
    "total": 45,
    "total_pages": 3
  }
}
```

Default page size is typically 20 items, with a maximum of 100 items per page.

## API Endpoints

The API is organized into the following endpoint groups:

### Authentication Endpoints

Endpoints for obtaining and refreshing access tokens. See the [Authentication](authentication.md) documentation for details.

### Data Ingestion Endpoints

Endpoints for managing data sources, pipeline definitions, and pipeline executions. See the [Ingestion API](ingestion-api.md) documentation for details.

### Data Quality Endpoints

Endpoints for managing quality rules, executing validations, and handling quality issues. See the [Quality API](quality-api.md) documentation for details.

### Self-Healing Endpoints

Endpoints for managing healing patterns, actions, and executions. See the [Healing API](healing-api.md) documentation for details.

### Monitoring & Alerting Endpoints

Endpoints for retrieving metrics, managing alerts, detecting anomalies, and accessing dashboard data. See the [Monitoring API](monitoring-api.md) documentation for details.

### Optimization Endpoints

Endpoints for query optimization, schema optimization, and resource management. See the [Optimization API](optimization-api.md) documentation for details.

## Error Handling

The API uses a consistent error response format across all endpoints. Error responses include an HTTP status code, a machine-readable error code, and a human-readable error message.

Example error response:

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Invalid request - Missing required field 'source_name'"
}
```

Common error codes include:

- `AUTH_ERROR`: Authentication or authorization error
- `VALIDATION_ERROR`: Invalid request data
- `RESOURCE_NOT_FOUND`: Requested resource doesn't exist
- `RESOURCE_IN_USE`: Resource cannot be modified or deleted because it's in use
- `EXECUTION_ERROR`: Error during execution of an operation

Each API area may define additional specific error codes, which are documented in the respective API documentation pages.

## Rate Limiting

The API implements rate limiting to ensure fair usage and system stability. Rate limits are applied on a per-user and per-endpoint basis.

When a rate limit is exceeded, the API returns a `429 Too Many Requests` status code with headers indicating the rate limit and when it will reset:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1623766800
```

Clients should implement exponential backoff retry logic when encountering rate limit errors.

## Versioning

The API uses versioning in the URL path to ensure backward compatibility. The current version is `v1`.

When breaking changes are introduced, a new API version will be released. Previous versions will continue to be supported for a deprecation period, which will be announced in advance.

## Security Best Practices

When integrating with the API, follow these security best practices:

1. **Use HTTPS**: Always use HTTPS for all API communications to prevent token interception.

2. **Secure Token Storage**: Store access tokens securely. For web applications, use HttpOnly cookies or secure storage mechanisms.

3. **Implement Token Refresh**: Access tokens have a limited lifetime. Implement proper token refresh logic to maintain user sessions.

4. **Use Minimum Required Permissions**: Request only the permissions your application needs.

5. **Validate Input**: Always validate and sanitize input before sending it to the API.

6. **Handle Sensitive Data**: Be careful when logging API requests and responses to avoid exposing sensitive data.

## Client Libraries

While you can use the API directly with HTTP requests, we provide client libraries for common programming languages to simplify integration:

- **Python**: `pip install selfhealing-pipeline-client`
- **JavaScript/Node.js**: `npm install selfhealing-pipeline-client`
- **Java**: Available through Maven Central
- **Go**: Available through Go modules

These libraries handle authentication, request formatting, error handling, and provide a more convenient interface for working with the API.

## OpenAPI Specification

The complete API is documented using the OpenAPI 3.0 specification. You can access the OpenAPI document at:

```
https://api.example.com/api/v1/openapi.json
```

This specification can be used with tools like Swagger UI, Postman, or code generation tools to create client libraries.

## Support and Feedback

If you encounter any issues or have questions about the API, please contact our support team at api-support@example.com.

We welcome feedback on the API and its documentation. Please submit feature requests or bug reports through our GitHub repository at https://github.com/example/selfhealing-pipeline/issues.

## Code Examples

### Basic API Usage with Python
```python
import requests

def get_api_data(endpoint, token, params=None):
    """Generic function to get data from the API
    
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
        f'https://api.example.com/api/v1{endpoint}',
        headers=headers,
        params=params
    )
    
    response.raise_for_status()  # Raise exception for error status codes
    return response.json()['data']

# Example: Get pipeline metrics
def get_pipeline_metrics(token, component=None):
    params = {}
    if component:
        params['component'] = component
    
    return get_api_data('/monitoring/metrics/', token, params)

# Example: Get active alerts
def get_active_alerts(token, severity=None):
    params = {'status': 'active'}
    if severity:
        params['severity'] = severity
    
    return get_api_data('/monitoring/alerts/', token, params)

# Example usage
try:
    token = 'YOUR_ACCESS_TOKEN'
    
    # Get ingestion component metrics
    metrics = get_pipeline_metrics(token, component='ingestion')
    print(f"Retrieved {len(metrics)} metrics")
    
    # Get critical active alerts
    alerts = get_active_alerts(token, severity='critical')
    print(f"Found {len(alerts)} critical alerts")
    
except requests.exceptions.HTTPError as e:
    print(f'API Error: {e}')
```

### Basic API Usage with JavaScript
```javascript
/**
 * Generic function to get data from the API
 * 
 * @param {string} endpoint - API endpoint path (without base URL)
 * @param {string} token - Authentication token
 * @param {Object} params - Optional query parameters
 * @returns {Promise<Object>} API response data
 */
async function getApiData(endpoint, token, params = {}) {
  // Build query string from params
  const queryParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    queryParams.append(key, value);
  }
  
  const queryString = queryParams.toString() ? `?${queryParams.toString()}` : '';
  
  const response = await fetch(
    `https://api.example.com/api/v1${endpoint}${queryString}`,
    {
      headers: {
        'Authorization': `Bearer ${token}`,
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
 * Get dashboard summary data
 * 
 * @param {string} token - Authentication token
 * @returns {Promise<Object>} Dashboard summary data
 */
async function getDashboardSummary(token) {
  return getApiData('/monitoring/dashboard/summary', token);
}

/**
 * Execute a data quality validation
 * 
 * @param {string} token - Authentication token
 * @param {string} dataset - Dataset to validate
 * @param {string|null} table - Optional specific table
 * @returns {Promise<Object>} Validation results
 */
async function executeValidation(token, dataset, table = null) {
  const params = {};
  if (table) params.table = table;
  
  return getApiData(`/quality/validate/${dataset}`, token, params);
}

// Example usage
async function main() {
  try {
    const token = 'YOUR_ACCESS_TOKEN';
    
    // Get dashboard summary
    const summary = await getDashboardSummary(token);
    console.log('Pipeline health score:', summary.pipeline_health.health_score);
    console.log('Self-healing success rate:', summary.self_healing.success_rate + '%');
    
    // Execute data validation
    const validation = await executeValidation(token, 'customer_data', 'customers');
    console.log('Validation status:', validation.status);
    console.log('Quality score:', validation.quality_score);
    console.log(`Passed ${validation.rules_passed} of ${validation.total_rules} rules`);
    
  } catch (error) {
    console.error('API Error:', error.message);
  }
}

main();
```

### Basic API Usage with cURL
```bash
# Set your access token
TOKEN="YOUR_ACCESS_TOKEN"

# Get pipeline metrics
curl -X GET \
  "https://api.example.com/api/v1/monitoring/metrics/?component=ingestion" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json"

# Create a data source
curl -X POST \
  "https://api.example.com/api/v1/ingestion/sources" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "Sales Data Bucket",
    "source_type": "GCS",
    "connection_details": {
      "bucket_name": "sales-data-bucket",
      "file_pattern": "*.csv"
    },
    "description": "GCS bucket containing sales data CSV files"
  }'

# Execute a pipeline
curl -X POST \
  "https://api.example.com/api/v1/ingestion/pipelines/550e8400-e29b-41d4-a716-446655440000/execute" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "execution_params": {
      "batch_size": 1000,
      "include_deleted": false
    }
  }'

# Get dashboard summary
curl -X GET \
  "https://api.example.com/api/v1/monitoring/dashboard/summary" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json"
```

## API Areas

### Authentication
Endpoints for obtaining and refreshing access tokens
- Documentation: [Authentication](authentication.md)
- Key Endpoints:
    - POST /auth/login - Authenticate user and obtain access token
    - POST /auth/refresh - Refresh an expired access token
    - POST /auth/logout - Invalidate the current token

### Data Ingestion
Endpoints for managing data sources, pipeline definitions, and executions
- Documentation: [Ingestion API](ingestion-api.md)
- Key Endpoints:
    - GET /ingestion/sources - Retrieve a list of data source systems
    - POST /ingestion/sources - Create a new data source system
    - GET /ingestion/pipelines - Retrieve a list of pipeline definitions
    - POST /ingestion/pipelines/{pipeline_id}/execute - Execute a pipeline

### Data Quality
Endpoints for managing quality rules, executing validations, and handling quality issues
- Documentation: [Quality API](quality-api.md)
- Key Endpoints:
    - GET /quality/rules - Retrieve a list of quality rules
    - POST /quality/validate/{dataset} - Execute quality validation for a dataset
    - GET /quality/score/{dataset} - Retrieve the quality score for a dataset
    - GET /quality/issues/{dataset} - Retrieve quality issues for a dataset

### Self-Healing
Endpoints for managing healing patterns, actions, and executions
- Documentation: [Healing API](healing-api.md)
- Key Endpoints:
    - GET /healing/patterns - Retrieve a list of healing patterns
    - GET /healing/actions - Retrieve a list of healing actions
    - GET /healing/executions - Retrieve a list of healing executions
    - POST /healing/execute - Manually trigger a healing action

### Monitoring & Alerting
Endpoints for metrics, alerts, anomalies, and dashboard data
- Documentation: [Monitoring API](monitoring-api.md)
- Key Endpoints:
    - GET /monitoring/metrics - Retrieve pipeline metrics
    - GET /monitoring/alerts - Retrieve alerts
    - GET /monitoring/anomalies - Retrieve detected anomalies
    - GET /monitoring/dashboard/summary - Retrieve dashboard summary data

### Optimization
Endpoints for query optimization, schema optimization, and resource management
- Documentation: [Optimization API](optimization-api.md)
- Key Endpoints:
    - GET /optimization/queries - Retrieve query performance data
    - POST /optimization/queries/analyze - Analyze and optimize a query
    - GET /optimization/schemas - Retrieve schema optimization recommendations
    - GET /optimization/resources - Retrieve resource utilization and recommendations

## Best Practices

### Authentication
Always use secure token storage and implement proper token refresh logic. Never store tokens in client-side code or expose them in URLs.

### Error Handling
Implement comprehensive error handling that accounts for all possible API error responses. Use exponential backoff for retrying failed requests, especially for rate limiting errors.

### Pagination
Always use pagination parameters when retrieving collections to avoid performance issues with large datasets. Process paginated results incrementally rather than attempting to retrieve all pages at once.

### Request Optimization
Minimize the number of API requests by using filtering parameters effectively. Batch operations when possible and implement client-side caching for frequently accessed data that doesn't change often.

### Webhook Integration
For real-time updates, consider using webhook notifications instead of polling the API. This reduces load on both the client and server while providing more timely updates.