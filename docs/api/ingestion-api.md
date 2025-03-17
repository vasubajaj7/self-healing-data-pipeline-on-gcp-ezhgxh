# Data Ingestion API

## Introduction

The Data Ingestion API provides endpoints for managing data sources, pipeline definitions, and pipeline executions in the self-healing data pipeline. This API enables you to programmatically configure and control the data ingestion process, supporting various source types including Google Cloud Storage, Cloud SQL, and external APIs.

This documentation covers all available endpoints, request/response formats, and includes examples for common operations.

## API Conventions

This API follows RESTful conventions with consistent patterns across all endpoints:

- All endpoints return JSON responses with a standard structure
- Authentication is required via OAuth 2.0 with JWT tokens
- Pagination is supported for list operations using page and page_size parameters
- Standard HTTP status codes indicate success or failure
- Error responses include an error code and descriptive message
- Date/time values use ISO 8601 format (YYYY-MM-DDThh:mm:ssZ)
- All API access is over HTTPS

Standard response format:
```json
{
  "status": "success|error",
  "message": "Human-readable message",
  "metadata": {},
  "data": {} or "items": []
}
```

## Base URL

All API endpoints in this document are relative to the base URL:

```
https://api.example.com/api/v1/ingestion
```

For example, the full URL for the source systems endpoint would be:

```
https://api.example.com/api/v1/ingestion/sources
```

## Authentication

All API endpoints require authentication using OAuth 2.0 with JWT tokens. Please refer to the [Authentication](authentication.md) documentation for detailed information on obtaining and using access tokens.

In general, you'll need to include an `Authorization` header with a valid token in all API requests:

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

## Source System Management

These endpoints allow you to manage data source systems, which represent connections to external data sources such as Google Cloud Storage buckets, Cloud SQL databases, or external APIs.

### Get All Data Sources

Retrieves a paginated list of data source systems with optional filtering.

**Endpoint:** `GET /sources`

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `source_type` (string, optional): Filter by source type (GCS, CLOUD_SQL, BIGQUERY, API, SFTP, CUSTOM)
- `status` (string, optional): Filter by connection status (CONNECTED, DISCONNECTED, ERROR, UNKNOWN)

**Response:**
```json
{
  "status": "success",
  "message": "Source systems retrieved successfully",
  "metadata": {},
  "items": [
    {
      "source_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Sales Data Bucket",
      "source_type": "GCS",
      "connection_details": {
        "bucket_name": "sales-data-bucket",
        "file_pattern": "*.csv"
      },
      "schema_version": "1.0",
      "description": "GCS bucket containing sales data CSV files",
      "is_active": true,
      "created_at": "2023-06-15T10:30:00Z",
      "updated_at": "2023-06-15T10:30:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com",
      "status": "CONNECTED"
    },
    {
      "source_id": "550e8400-e29b-41d4-a716-446655440001",
      "name": "Customer Database",
      "source_type": "CLOUD_SQL",
      "connection_details": {
        "instance_name": "customer-db-instance",
        "database": "customers",
        "region": "us-central1"
      },
      "schema_version": "1.0",
      "description": "Cloud SQL database containing customer data",
      "is_active": true,
      "created_at": "2023-06-15T11:45:00Z",
      "updated_at": "2023-06-15T11:45:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com",
      "status": "CONNECTED"
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

### Get Data Source

Retrieves a specific data source system by ID.

**Endpoint:** `GET /sources/{source_id}`

**Path Parameters:**
- `source_id` (string, required): The ID of the source system to retrieve

**Response:**
```json
{
  "status": "success",
  "message": "Source system data retrieved successfully",
  "metadata": {},
  "data": {
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Sales Data Bucket",
    "source_type": "GCS",
    "connection_details": {
      "bucket_name": "sales-data-bucket",
      "file_pattern": "*.csv"
    },
    "schema_version": "1.0",
    "description": "GCS bucket containing sales data CSV files",
    "is_active": true,
    "created_at": "2023-06-15T10:30:00Z",
    "updated_at": "2023-06-15T10:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "status": "CONNECTED"
  }
}
```

### Create Data Source

Creates a new data source system.

**Endpoint:** `POST /sources`

**Request Body:**
```json
{
  "name": "Sales Data Bucket",
  "source_type": "GCS",
  "connection_details": {
    "bucket_name": "sales-data-bucket",
    "file_pattern": "*.csv"
  },
  "schema_version": "1.0",
  "description": "GCS bucket containing sales data CSV files",
  "is_active": true
}
```

**Connection Details by Source Type:**

1. For GCS sources (`source_type: "GCS"`):
```json
{
  "bucket_name": "your-bucket-name",
  "file_pattern": "*.csv"
}
```

2. For Cloud SQL sources (`source_type: "CLOUD_SQL"`):
```json
{
  "instance_name": "your-instance-name",
  "database": "your-database-name",
  "region": "us-central1",
  "credentials": {
    "username": "db-user",
    "password_secret": "projects/your-project/secrets/db-password"
  }
}
```

3. For API sources (`source_type: "API"`):
```json
{
  "endpoint_url": "https://api.example.com/data",
  "auth_method": "oauth2",
  "headers": {
    "Content-Type": "application/json"
  },
  "auth_config": {
    "client_id": "your-client-id",
    "client_secret_secret": "projects/your-project/secrets/api-client-secret",
    "token_url": "https://api.example.com/oauth/token"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Source system created successfully",
  "metadata": {},
  "data": {
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Sales Data Bucket",
    "source_type": "GCS",
    "connection_details": {
      "bucket_name": "sales-data-bucket",
      "file_pattern": "*.csv"
    },
    "schema_version": "1.0",
    "description": "GCS bucket containing sales data CSV files",
    "is_active": true,
    "created_at": "2023-06-15T10:30:00Z",
    "updated_at": "2023-06-15T10:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "status": "UNKNOWN"
  }
}
```

### Update Data Source

Updates an existing data source system.

**Endpoint:** `PUT /sources/{source_id}`

**Path Parameters:**
- `source_id` (string, required): The ID of the source system to update

**Request Body:**
```json
{
  "name": "Updated Sales Data Bucket",
  "connection_details": {
    "bucket_name": "updated-sales-data-bucket",
    "file_pattern": "*.csv"
  },
  "description": "Updated GCS bucket containing sales data CSV files",
  "is_active": true
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Source system updated successfully",
  "metadata": {},
  "data": {
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Updated Sales Data Bucket",
    "source_type": "GCS",
    "connection_details": {
      "bucket_name": "updated-sales-data-bucket",
      "file_pattern": "*.csv"
    },
    "schema_version": "1.0",
    "description": "Updated GCS bucket containing sales data CSV files",
    "is_active": true,
    "created_at": "2023-06-15T10:30:00Z",
    "updated_at": "2023-06-15T11:45:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com",
    "status": "UNKNOWN"
  }
}
```

### Delete Data Source

Deletes a data source system.

**Endpoint:** `DELETE /sources/{source_id}`

**Path Parameters:**
- `source_id` (string, required): The ID of the source system to delete

**Response:**
```json
{
  "status": "success",
  "message": "Source system deleted successfully",
  "metadata": {}
}
```

### Test Source Connection

Tests connection to a data source without creating it.

**Endpoint:** `POST /sources/test-connection`

**Request Body:**
```json
{
  "source_type": "GCS",
  "connection_details": {
    "bucket_name": "sales-data-bucket",
    "file_pattern": "*.csv"
  },
  "test_parameters": {
    "timeout_seconds": 30
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Connection test completed",
  "metadata": {},
  "connection_successful": true,
  "connection_details": {
    "latency_ms": 245,
    "available_objects": 15,
    "total_size_bytes": 1024000
  },
  "test_results": {
    "permissions": ["read", "list"],
    "missing_permissions": []
  }
}
```

### Get Source Schema

Retrieves schema information from a data source.

**Endpoint:** `GET /sources/{source_id}/schema/{object_name}`

**Path Parameters:**
- `source_id` (string, required): The ID of the source system
- `object_name` (string, required): The name of the object to retrieve schema for (table name for databases, file pattern for GCS)

**Response:**
```json
{
  "status": "success",
  "message": "Schema retrieved successfully",
  "metadata": {},
  "data": {
    "object_name": "sales_data",
    "fields": [
      {
        "name": "sale_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Unique sale identifier"
      },
      {
        "name": "product_id",
        "type": "STRING",
        "mode": "REQUIRED",
        "description": "Product identifier"
      },
      {
        "name": "sale_date",
        "type": "TIMESTAMP",
        "mode": "REQUIRED",
        "description": "Date and time of sale"
      },
      {
        "name": "quantity",
        "type": "INTEGER",
        "mode": "REQUIRED",
        "description": "Quantity sold"
      },
      {
        "name": "unit_price",
        "type": "FLOAT",
        "mode": "REQUIRED",
        "description": "Price per unit"
      },
      {
        "name": "customer_id",
        "type": "STRING",
        "mode": "NULLABLE",
        "description": "Customer identifier"
      }
    ],
    "sample_data": [
      {
        "sale_id": "S12345",
        "product_id": "P789",
        "sale_date": "2023-06-01T10:30:00Z",
        "quantity": 5,
        "unit_price": 29.99,
        "customer_id": "C1001"
      }
    ]
  }
}
```

### Get Supported Source Types

Retrieves a list of supported data source types.

**Endpoint:** `GET /sources/types`

**Response:**
```json
{
  "status": "success",
  "message": "Supported source types retrieved successfully",
  "metadata": {},
  "data": {
    "source_types": [
      {
        "type": "GCS",
        "display_name": "Google Cloud Storage",
        "description": "Files stored in Google Cloud Storage buckets",
        "supported_formats": ["CSV", "JSON", "AVRO", "PARQUET", "ORC"]
      },
      {
        "type": "CLOUD_SQL",
        "display_name": "Cloud SQL",
        "description": "Google Cloud SQL databases (MySQL, PostgreSQL)",
        "supported_engines": ["MYSQL", "POSTGRESQL"]
      },
      {
        "type": "BIGQUERY",
        "display_name": "BigQuery",
        "description": "Google BigQuery datasets and tables"
      },
      {
        "type": "API",
        "display_name": "External API",
        "description": "External REST or GraphQL APIs",
        "supported_formats": ["JSON", "XML"]
      },
      {
        "type": "SFTP",
        "display_name": "SFTP Server",
        "description": "Secure File Transfer Protocol servers",
        "supported_formats": ["CSV", "JSON", "XML", "TEXT"]
      },
      {
        "type": "CUSTOM",
        "display_name": "Custom Connector",
        "description": "Custom-developed data source connector"
      }
    ]
  }
}
```

## Pipeline Management

These endpoints allow you to manage pipeline definitions, which define how data is extracted from source systems and loaded into BigQuery.

### Get All Pipelines

Retrieves a paginated list of pipeline definitions with optional filtering.

**Endpoint:** `GET /pipelines`

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `source_id` (string, optional): Filter by source system ID
- `is_active` (boolean, optional): Filter by active status

**Response:**
```json
{
  "status": "success",
  "message": "Pipelines retrieved successfully",
  "metadata": {},
  "items": [
    {
      "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
      "pipeline_name": "Daily Sales Data Import",
      "source_id": "550e8400-e29b-41d4-a716-446655440000",
      "target_dataset": "sales_data",
      "target_table": "daily_sales",
      "dag_id": "gcs_to_bq_daily_sales",
      "configuration": {
        "schedule": "0 2 * * *",
        "extraction_params": {
          "file_pattern": "sales_*.csv",
          "field_delimiter": ",",
          "skip_header_rows": 1
        },
        "transformation_params": {
          "apply_schema": true,
          "convert_timestamps": true
        }
      },
      "description": "Daily import of sales data from GCS to BigQuery",
      "is_active": true,
      "created_at": "2023-06-15T14:30:00Z",
      "updated_at": "2023-06-15T14:30:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com"
    },
    {
      "pipeline_id": "550e8400-e29b-41d4-a716-446655440011",
      "pipeline_name": "Customer Data Sync",
      "source_id": "550e8400-e29b-41d4-a716-446655440001",
      "target_dataset": "customer_data",
      "target_table": "customers",
      "dag_id": "cloudsql_to_bq_customers",
      "configuration": {
        "schedule": "0 1 * * *",
        "extraction_params": {
          "query": "SELECT * FROM customers WHERE updated_at >= '{{ds}}'",
          "incremental_key": "updated_at"
        }
      },
      "description": "Daily sync of customer data from Cloud SQL to BigQuery",
      "is_active": true,
      "created_at": "2023-06-15T15:45:00Z",
      "updated_at": "2023-06-15T15:45:00Z",
      "created_by": "admin@example.com",
      "updated_by": "admin@example.com"
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

### Get Pipeline

Retrieves a specific pipeline definition by ID.

**Endpoint:** `GET /pipelines/{pipeline_id}`

**Path Parameters:**
- `pipeline_id` (string, required): The ID of the pipeline to retrieve

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline data retrieved successfully",
  "metadata": {},
  "data": {
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "pipeline_name": "Daily Sales Data Import",
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "target_dataset": "sales_data",
    "target_table": "daily_sales",
    "dag_id": "gcs_to_bq_daily_sales",
    "configuration": {
      "schedule": "0 2 * * *",
      "extraction_params": {
        "file_pattern": "sales_*.csv",
        "field_delimiter": ",",
        "skip_header_rows": 1
      },
      "transformation_params": {
        "apply_schema": true,
        "convert_timestamps": true
      }
    },
    "description": "Daily import of sales data from GCS to BigQuery",
    "is_active": true,
    "created_at": "2023-06-15T14:30:00Z",
    "updated_at": "2023-06-15T14:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com"
  }
}
```

### Create Pipeline

Creates a new pipeline definition.

**Endpoint:** `POST /pipelines`

**Request Body:**
```json
{
  "pipeline_name": "Daily Sales Data Import",
  "source_id": "550e8400-e29b-41d4-a716-446655440000",
  "target_dataset": "sales_data",
  "target_table": "daily_sales",
  "configuration": {
    "schedule": "0 2 * * *",
    "extraction_params": {
      "file_pattern": "sales_*.csv",
      "field_delimiter": ",",
      "skip_header_rows": 1
    },
    "transformation_params": {
      "apply_schema": true,
      "convert_timestamps": true
    }
  },
  "description": "Daily import of sales data from GCS to BigQuery",
  "is_active": true
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline created successfully",
  "metadata": {},
  "data": {
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "pipeline_name": "Daily Sales Data Import",
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "target_dataset": "sales_data",
    "target_table": "daily_sales",
    "dag_id": "gcs_to_bq_daily_sales",
    "configuration": {
      "schedule": "0 2 * * *",
      "extraction_params": {
        "file_pattern": "sales_*.csv",
        "field_delimiter": ",",
        "skip_header_rows": 1
      },
      "transformation_params": {
        "apply_schema": true,
        "convert_timestamps": true
      }
    },
    "description": "Daily import of sales data from GCS to BigQuery",
    "is_active": true,
    "created_at": "2023-06-15T14:30:00Z",
    "updated_at": "2023-06-15T14:30:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com"
  }
}
```

### Update Pipeline

Updates an existing pipeline definition.

**Endpoint:** `PUT /pipelines/{pipeline_id}`

**Path Parameters:**
- `pipeline_id` (string, required): The ID of the pipeline to update

**Request Body:**
```json
{
  "pipeline_name": "Updated Sales Data Import",
  "target_dataset": "sales_data",
  "target_table": "daily_sales_updated",
  "configuration": {
    "schedule": "0 3 * * *",
    "extraction_params": {
      "file_pattern": "sales_updated_*.csv",
      "field_delimiter": ",",
      "skip_header_rows": 1
    }
  },
  "description": "Updated daily import of sales data from GCS to BigQuery",
  "is_active": true
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline updated successfully",
  "metadata": {},
  "data": {
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "pipeline_name": "Updated Sales Data Import",
    "source_id": "550e8400-e29b-41d4-a716-446655440000",
    "target_dataset": "sales_data",
    "target_table": "daily_sales_updated",
    "dag_id": "gcs_to_bq_daily_sales",
    "configuration": {
      "schedule": "0 3 * * *",
      "extraction_params": {
        "file_pattern": "sales_updated_*.csv",
        "field_delimiter": ",",
        "skip_header_rows": 1
      },
      "transformation_params": {
        "apply_schema": true,
        "convert_timestamps": true
      }
    },
    "description": "Updated daily import of sales data from GCS to BigQuery",
    "is_active": true,
    "created_at": "2023-06-15T14:30:00Z",
    "updated_at": "2023-06-15T16:45:00Z",
    "created_by": "admin@example.com",
    "updated_by": "admin@example.com"
  }
}
```

### Delete Pipeline

Deletes a pipeline definition.

**Endpoint:** `DELETE /pipelines/{pipeline_id}`

**Path Parameters:**
- `pipeline_id` (string, required): The ID of the pipeline to delete

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline deleted successfully",
  "metadata": {}
}
```

## Pipeline Execution

These endpoints allow you to execute pipelines and manage pipeline executions.

### Execute Pipeline

Executes a pipeline.

**Endpoint:** `POST /pipelines/{pipeline_id}/execute`

**Path Parameters:**
- `pipeline_id` (string, required): The ID of the pipeline to execute

**Request Body:**
```json
{
  "execution_params": {
    "batch_size": 1000,
    "include_deleted": false,
    "start_date": "2023-06-01",
    "end_date": "2023-06-15"
  },
  "async_execution": true,
  "force_execution": false
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline execution started successfully",
  "metadata": {},
  "data": {
    "execution_id": "550e8400-e29b-41d4-a716-446655440020",
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "status": "PENDING",
    "start_time": "2023-06-15T17:30:00Z",
    "dag_run_id": "gcs_to_bq_daily_sales_20230615T173000",
    "execution_params": {
      "batch_size": 1000,
      "include_deleted": false,
      "start_date": "2023-06-01",
      "end_date": "2023-06-15"
    }
  }
}
```

### Get Pipeline Executions

Retrieves a paginated list of pipeline executions with optional filtering.

**Endpoint:** `GET /pipelines/{pipeline_id}/executions`

**Path Parameters:**
- `pipeline_id` (string, required): The ID of the pipeline

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `status` (string, optional): Filter by execution status (PENDING, RUNNING, SUCCESS, FAILED, HEALING)
- `start_date` (string, optional): Filter by start date (ISO 8601 format)
- `end_date` (string, optional): Filter by end date (ISO 8601 format)

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline executions retrieved successfully",
  "metadata": {},
  "items": [
    {
      "execution_id": "550e8400-e29b-41d4-a716-446655440020",
      "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
      "status": "SUCCESS",
      "start_time": "2023-06-15T17:30:00Z",
      "end_time": "2023-06-15T17:45:00Z",
      "dag_run_id": "gcs_to_bq_daily_sales_20230615T173000",
      "records_processed": 5280,
      "execution_params": {
        "batch_size": 1000,
        "include_deleted": false,
        "start_date": "2023-06-01",
        "end_date": "2023-06-15"
      },
      "metrics": {
        "extraction_time_seconds": 450,
        "transformation_time_seconds": 120,
        "loading_time_seconds": 180,
        "total_time_seconds": 900
      }
    },
    {
      "execution_id": "550e8400-e29b-41d4-a716-446655440021",
      "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
      "status": "FAILED",
      "start_time": "2023-06-14T17:30:00Z",
      "end_time": "2023-06-14T17:40:00Z",
      "dag_run_id": "gcs_to_bq_daily_sales_20230614T173000",
      "records_processed": 0,
      "execution_params": {
        "batch_size": 1000,
        "include_deleted": false
      },
      "error_details": {
        "error_code": "FILE_NOT_FOUND",
        "error_message": "Source file not found: sales_20230614.csv",
        "task_id": "extract_from_gcs"
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

### Get Pipeline Execution

Retrieves a specific pipeline execution by ID.

**Endpoint:** `GET /executions/{execution_id}`

**Path Parameters:**
- `execution_id` (string, required): The ID of the execution to retrieve

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline execution data retrieved successfully",
  "metadata": {},
  "data": {
    "execution_id": "550e8400-e29b-41d4-a716-446655440020",
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "status": "SUCCESS",
    "start_time": "2023-06-15T17:30:00Z",
    "end_time": "2023-06-15T17:45:00Z",
    "dag_run_id": "gcs_to_bq_daily_sales_20230615T173000",
    "records_processed": 5280,
    "execution_params": {
      "batch_size": 1000,
      "include_deleted": false,
      "start_date": "2023-06-01",
      "end_date": "2023-06-15"
    },
    "metrics": {
      "extraction_time_seconds": 450,
      "transformation_time_seconds": 120,
      "loading_time_seconds": 180,
      "total_time_seconds": 900
    },
    "task_ids": [
      "extract_from_gcs",
      "transform_data",
      "load_to_bigquery",
      "validate_data_quality"
    ]
  }
}
```

### Get Task Executions

Retrieves a paginated list of task executions for a pipeline execution.

**Endpoint:** `GET /executions/{execution_id}/tasks`

**Path Parameters:**
- `execution_id` (string, required): The ID of the pipeline execution

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `status` (string, optional): Filter by task status (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED, UPSTREAM_FAILED)

**Response:**
```json
{
  "status": "success",
  "message": "Task executions retrieved successfully",
  "metadata": {},
  "items": [
    {
      "task_id": "extract_from_gcs",
      "execution_id": "550e8400-e29b-41d4-a716-446655440020",
      "task_name": "Extract from GCS",
      "task_type": "GCSToLocalOperator",
      "status": "SUCCESS",
      "start_time": "2023-06-15T17:30:00Z",
      "end_time": "2023-06-15T17:37:30Z",
      "task_params": {
        "bucket": "sales-data-bucket",
        "object": "sales_20230615.csv",
        "destination": "/tmp/sales_20230615.csv"
      },
      "metrics": {
        "bytes_processed": 1048576,
        "processing_time_seconds": 450
      },
      "log_url": "https://console.cloud.google.com/logs/query?project=your-project-id&query=resource.type%3D%22cloud_composer_environment%22%20AND%20resource.labels.environment_name%3D%22your-composer-environment%22%20AND%20textPayload%3D%22extract_from_gcs%22"
    },
    {
      "task_id": "transform_data",
      "execution_id": "550e8400-e29b-41d4-a716-446655440020",
      "task_name": "Transform Data",
      "task_type": "PythonOperator",
      "status": "SUCCESS",
      "start_time": "2023-06-15T17:37:30Z",
      "end_time": "2023-06-15T17:39:30Z",
      "task_params": {
        "python_callable": "transform_sales_data",
        "op_kwargs": {
          "input_file": "/tmp/sales_20230615.csv",
          "output_file": "/tmp/sales_20230615_transformed.csv"
        }
      },
      "metrics": {
        "records_processed": 5280,
        "processing_time_seconds": 120
      },
      "log_url": "https://console.cloud.google.com/logs/query?project=your-project-id&query=resource.type%3D%22cloud_composer_environment%22%20AND%20resource.labels.environment_name%3D%22your-composer-environment%22%20AND%20textPayload%3D%22transform_data%22"
    },
    {
      "task_id": "load_to_bigquery",
      "execution_id": "550e8400-e29b-41d4-a716-446655440020",
      "task_name": "Load to BigQuery",
      "task_type": "GCSToBigQueryOperator",
      "status": "SUCCESS",
      "start_time": "2023-06-15T17:39:30Z",
      "end_time": "2023-06-15T17:42:30Z",
      "task_params": {
        "bucket": "sales-data-bucket",
        "source_objects": ["sales_20230615_transformed.csv"],
        "destination_project_dataset_table": "your-project-id:sales_data.daily_sales",
        "schema_fields": [...],
        "write_disposition": "WRITE_TRUNCATE"
      },
      "metrics": {
        "rows_loaded": 5280,
        "processing_time_seconds": 180
      },
      "log_url": "https://console.cloud.google.com/logs/query?project=your-project-id&query=resource.type%3D%22cloud_composer_environment%22%20AND%20resource.labels.environment_name%3D%22your-composer-environment%22%20AND%20textPayload%3D%22load_to_bigquery%22"
    },
    {
      "task_id": "validate_data_quality",
      "execution_id": "550e8400-e29b-41d4-a716-446655440020",
      "task_name": "Validate Data Quality",
      "task_type": "PythonOperator",
      "status": "SUCCESS",
      "start_time": "2023-06-15T17:42:30Z",
      "end_time": "2023-06-15T17:45:00Z",
      "task_params": {
        "python_callable": "validate_data_quality",
        "op_kwargs": {
          "dataset": "sales_data",
          "table": "daily_sales"
        }
      },
      "metrics": {
        "quality_checks_passed": 15,
        "quality_checks_failed": 0,
        "processing_time_seconds": 150
      },
      "log_url": "https://console.cloud.google.com/logs/query?project=your-project-id&query=resource.type%3D%22cloud_composer_environment%22%20AND%20resource.labels.environment_name%3D%22your-composer-environment%22%20AND%20textPayload%3D%22validate_data_quality%22"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 4,
    "total_pages": 1
  }
}
```

### Cancel Pipeline Execution

Cancels a running pipeline execution.

**Endpoint:** `POST /executions/{execution_id}/cancel`

**Path Parameters:**
- `execution_id` (string, required): The ID of the execution to cancel

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline execution cancelled successfully",
  "metadata": {},
  "data": {
    "execution_id": "550e8400-e29b-41d4-a716-446655440020",
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "status": "FAILED",
    "start_time": "2023-06-15T17:30:00Z",
    "end_time": "2023-06-15T17:40:00Z",
    "dag_run_id": "gcs_to_bq_daily_sales_20230615T173000",
    "error_details": {
      "error_code": "CANCELLED",
      "error_message": "Execution cancelled by user"
    }
  }
}
```

### Retry Pipeline Execution

Retries a failed pipeline execution.

**Endpoint:** `POST /executions/{execution_id}/retry`

**Path Parameters:**
- `execution_id` (string, required): The ID of the execution to retry

**Request Body:**
```json
{
  "execution_params": {
    "batch_size": 500,
    "include_deleted": true
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline execution retry started successfully",
  "metadata": {},
  "data": {
    "execution_id": "550e8400-e29b-41d4-a716-446655440022",
    "pipeline_id": "550e8400-e29b-41d4-a716-446655440010",
    "status": "PENDING",
    "start_time": "2023-06-15T18:30:00Z",
    "dag_run_id": "gcs_to_bq_daily_sales_20230615T183000",
    "execution_params": {
      "batch_size": 500,
      "include_deleted": true,
      "retry_of_execution_id": "550e8400-e29b-41d4-a716-446655440021"
    }
  }
}
```

## Error Handling

The API uses a consistent error response format across all endpoints. Error responses include an HTTP status code, a machine-readable error code, and a human-readable error message.

Example error response:

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Source system with ID '550e8400-e29b-41d4-a716-446655440999' not found"
}
```

Common error codes for the Ingestion API include:

- `VALIDATION_ERROR`: Invalid request data
- `RESOURCE_NOT_FOUND`: Requested resource doesn't exist
- `RESOURCE_ALREADY_EXISTS`: Resource with the same identifier already exists
- `CONNECTION_ERROR`: Error connecting to data source
- `PERMISSION_DENIED`: Insufficient permissions for the operation
- `EXECUTION_ERROR`: Error during pipeline execution
- `PIPELINE_ALREADY_RUNNING`: Pipeline is already running and cannot be executed again
- `INVALID_CONFIGURATION`: Pipeline configuration is invalid

## Code Examples

### Python Example

```python
import requests
import json

BASE_URL = "https://api.example.com/api/v1/ingestion"
TOKEN = "YOUR_ACCESS_TOKEN"

def get_headers():
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

# Get all data sources
def get_data_sources(source_type=None, status=None, page=1, page_size=20):
    params = {
        "page": page,
        "page_size": page_size
    }
    
    if source_type:
        params["source_type"] = source_type
    
    if status:
        params["status"] = status
    
    response = requests.get(
        f"{BASE_URL}/sources",
        headers=get_headers(),
        params=params
    )
    
    response.raise_for_status()
    return response.json()

# Create a new data source
def create_data_source(name, source_type, connection_details, description=None, is_active=True):
    payload = {
        "name": name,
        "source_type": source_type,
        "connection_details": connection_details,
        "is_active": is_active
    }
    
    if description:
        payload["description"] = description
    
    response = requests.post(
        f"{BASE_URL}/sources",
        headers=get_headers(),
        json=payload
    )
    
    response.raise_for_status()
    return response.json()

# Execute a pipeline
def execute_pipeline(pipeline_id, execution_params=None, async_execution=True):
    payload = {
        "async_execution": async_execution
    }
    
    if execution_params:
        payload["execution_params"] = execution_params
    
    response = requests.post(
        f"{BASE_URL}/pipelines/{pipeline_id}/execute",
        headers=get_headers(),
        json=payload
    )
    
    response.raise_for_status()
    return response.json()

# Example usage
try:
    # Get GCS data sources
    sources = get_data_sources(source_type="GCS")
    print(f"Found {len(sources['items'])} GCS sources")
    
    # Create a new GCS data source
    new_source = create_data_source(
        name="New Sales Data Bucket",
        source_type="GCS",
        connection_details={
            "bucket_name": "new-sales-data-bucket",
            "file_pattern": "*.csv"
        },
        description="New GCS bucket for sales data"
    )
    print(f"Created new source with ID: {new_source['data']['source_id']}")
    
    # Execute a pipeline
    execution = execute_pipeline(
        pipeline_id="550e8400-e29b-41d4-a716-446655440010",
        execution_params={
            "start_date": "2023-06-01",
            "end_date": "2023-06-15"
        }
    )
    print(f"Started pipeline execution with ID: {execution['data']['execution_id']}")
    
except requests.exceptions.HTTPError as e:
    error_message = e.response.json()
    print(f"API Error: {error_message['message']}")
```

### JavaScript Example

```javascript
const BASE_URL = 'https://api.example.com/api/v1/ingestion';
const TOKEN = 'YOUR_ACCESS_TOKEN';

function getHeaders() {
  return {
    'Authorization': `Bearer ${TOKEN}`,
    'Content-Type': 'application/json'
  };
}

// Get all pipelines
async function getPipelines(sourceId = null, isActive = null, page = 1, pageSize = 20) {
  const params = new URLSearchParams({
    page: page.toString(),
    page_size: pageSize.toString()
  });
  
  if (sourceId) {
    params.append('source_id', sourceId);
  }
  
  if (isActive !== null) {
    params.append('is_active', isActive.toString());
  }
  
  const response = await fetch(
    `${BASE_URL}/pipelines?${params.toString()}`,
    {
      method: 'GET',
      headers: getHeaders()
    }
  );
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  return response.json();
}

// Create a new pipeline
async function createPipeline(pipelineData) {
  const response = await fetch(
    `${BASE_URL}/pipelines`,
    {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify(pipelineData)
    }
  );
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  return response.json();
}

// Get pipeline execution details
async function getPipelineExecution(executionId) {
  const response = await fetch(
    `${BASE_URL}/executions/${executionId}`,
    {
      method: 'GET',
      headers: getHeaders()
    }
  );
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  return response.json();
}

// Example usage
async function main() {
  try {
    // Get active pipelines
    const pipelines = await getPipelines(null, true);
    console.log(`Found ${pipelines.items.length} active pipelines`);
    
    // Create a new pipeline
    const newPipeline = await createPipeline({
      pipeline_name: 'New Sales Data Import',
      source_id: '550e8400-e29b-41d4-a716-446655440000',
      target_dataset: 'sales_data',
      target_table: 'new_daily_sales',
      configuration: {
        schedule: '0 4 * * *',
        extraction_params: {
          file_pattern: 'new_sales_*.csv',
          field_delimiter: ',',
          skip_header_rows: 1
        }
      },
      description: 'New daily import of sales data from GCS to BigQuery',
      is_active: true
    });
    console.log(`Created new pipeline with ID: ${newPipeline.data.pipeline_id}`);
    
    // Get execution details
    const execution = await getPipelineExecution('550e8400-e29b-41d4-a716-446655440020');
    console.log(`Execution status: ${execution.data.status}`);
    console.log(`Records processed: ${execution.data.records_processed}`);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

main();
```

### cURL Example

```bash
# Set your access token
TOKEN="YOUR_ACCESS_TOKEN"

# Get all data sources
curl -X GET \
  "https://api.example.com/api/v1/ingestion/sources?source_type=GCS" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json"

# Create a data source
curl -X POST \
  "https://api.example.com/api/v1/ingestion/sources" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Sales Data Bucket",
    "source_type": "GCS",
    "connection_details": {
      "bucket_name": "sales-data-bucket",
      "file_pattern": "*.csv"
    },
    "description": "GCS bucket containing sales data CSV files",
    "is_active": true
  }'

# Execute a pipeline
curl -X POST \
  "https://api.example.com/api/v1/ingestion/pipelines/550e8400-e29b-41d4-a716-446655440010/execute" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "execution_params": {
      "batch_size": 1000,
      "include_deleted": false,
      "start_date": "2023-06-01",
      "end_date": "2023-06-15"
    },
    "async_execution": true
  }'

# Get pipeline execution details
curl -X GET \
  "https://api.example.com/api/v1/ingestion/executions/550e8400-e29b-41d4-a716-446655440020" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json"
```

## Best Practices

When using the Ingestion API, follow these best practices for optimal results:

1. **Use Pagination**: Always use pagination parameters when retrieving collections to avoid performance issues with large datasets.

2. **Implement Error Handling**: Implement comprehensive error handling that accounts for all possible API error responses. Use exponential backoff for retrying failed requests.

3. **Test Connections First**: Before creating a data source, use the test-connection endpoint to verify connectivity and permissions.

4. **Optimize Execution Parameters**: Adjust batch sizes and other execution parameters based on data volume and complexity to optimize performance.

5. **Monitor Executions**: Regularly check the status of pipeline executions and implement automated monitoring for failures.

6. **Use Asynchronous Execution**: For long-running pipelines, use asynchronous execution mode and poll for completion status.

7. **Secure Credentials**: Never include plain-text credentials in connection details. Use Secret Manager references instead.

8. **Implement Idempotency**: Design your integration to handle duplicate requests gracefully, especially for pipeline executions.

9. **Validate Schemas**: Before loading data, validate that the source schema matches your expectations to prevent data quality issues.

10. **Incremental Processing**: Use incremental processing where possible to minimize resource usage and processing time.

## Related Documentation

- [Authentication](authentication.md): Authentication and authorization details
- [Quality API](quality-api.md): Data quality validation endpoints
- [Healing API](healing-api.md): Self-healing capabilities and configuration
- [Monitoring API](monitoring-api.md): Monitoring and alerting endpoints