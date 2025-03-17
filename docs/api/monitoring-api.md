# Monitoring & Alerting API

## Introduction

The Monitoring & Alerting API provides access to pipeline metrics, alerts, anomalies, and dashboard data in the Self-Healing Data Pipeline. These endpoints enable you to monitor the health of your pipelines, receive notifications about issues, and access detailed performance metrics.

This API is a critical component for operational visibility, enabling both automated monitoring systems and custom dashboards to track the pipeline's performance and health.

## Base URL

All Monitoring & Alerting API endpoints are relative to the base URL and prefixed with `/monitoring`:

```
https://api.example.com/api/v1/monitoring
```

For example, the full URL for the metrics endpoint would be:

```
https://api.example.com/api/v1/monitoring/metrics
```

## Authentication

All Monitoring & Alerting API endpoints require authentication. Please refer to the [Authentication](authentication.md) documentation for detailed information on obtaining and using access tokens.

In general, you'll need to include an `Authorization` header with a valid token in all API requests:

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

Different endpoints require different permission levels:
- `monitoring:read` - Required for all GET operations
- `monitoring:update` - Required for acknowledging and resolving alerts
- `monitoring:admin` - Required for updating alert configurations

## Metrics Endpoints

### Get Pipeline Metrics

```
GET /monitoring/metrics
```

Retrieves a paginated list of pipeline metrics with optional filtering.

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `metric_category` (string, optional): Filter by metric category
- `component` (string, optional): Filter by pipeline component
- `pipeline_id` (string, optional): Filter by specific pipeline ID

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline metrics retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "items": [
    {
      "metric_id": "550e8400-e29b-41d4-a716-446655440000",
      "metric_name": "pipeline_execution_time",
      "metric_category": "performance",
      "component": "ingestion",
      "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
      "value": 145.3,
      "unit": "seconds",
      "timestamp": "2023-06-15T10:15:00Z",
      "metadata": {
        "execution_id": "789e0123-e45b-67d8-a901-234567890000"
      }
    },
    {
      "metric_id": "550e8400-e29b-41d4-a716-446655440001",
      "metric_name": "records_processed",
      "metric_category": "volume",
      "component": "ingestion",
      "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
      "value": 15420,
      "unit": "count",
      "timestamp": "2023-06-15T10:15:00Z",
      "metadata": {
        "execution_id": "789e0123-e45b-67d8-a901-234567890000"
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

### Get Metric by ID

```
GET /monitoring/metrics/{metric_id}
```

Retrieves a specific metric by ID.

**Path Parameters:**
- `metric_id` (string, required): The unique identifier of the metric

**Response:**
```json
{
  "status": "success",
  "message": "Pipeline metric data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "metric_id": "550e8400-e29b-41d4-a716-446655440000",
    "metric_name": "pipeline_execution_time",
    "metric_category": "performance",
    "component": "ingestion",
    "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
    "value": 145.3,
    "unit": "seconds",
    "timestamp": "2023-06-15T10:15:00Z",
    "metadata": {
      "execution_id": "789e0123-e45b-67d8-a901-234567890000",
      "task_id": "extract_data",
      "source_system": "sales_database"
    }
  }
}
```

### Get Metric Time Series

```
GET /monitoring/metrics/timeseries/{metric_name}
```

Retrieves time series data for a specific metric.

**Path Parameters:**
- `metric_name` (string, required): The name of the metric to retrieve time series data for

**Query Parameters:**
- `start_date` (string, optional): Start date/time for the time series (ISO 8601 format)
- `end_date` (string, optional): End date/time for the time series (ISO 8601 format)
- `aggregation` (string, optional): Aggregation method (avg, sum, min, max, count)
- `component` (string, optional): Filter by pipeline component
- `pipeline_id` (string, optional): Filter by specific pipeline ID

**Response:**
```json
{
  "status": "success",
  "message": "Metric time series data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "metric_name": "pipeline_execution_time",
  "metric_unit": "seconds",
  "data_points": [
    {
      "timestamp": "2023-06-14T10:00:00Z",
      "value": 142.5
    },
    {
      "timestamp": "2023-06-14T11:00:00Z",
      "value": 138.2
    },
    {
      "timestamp": "2023-06-14T12:00:00Z",
      "value": 145.7
    },
    {
      "timestamp": "2023-06-14T13:00:00Z",
      "value": 150.3
    },
    {
      "timestamp": "2023-06-14T14:00:00Z",
      "value": 147.8
    }
  ],
  "statistics": {
    "min": 138.2,
    "max": 150.3,
    "avg": 144.9,
    "median": 145.7,
    "std_dev": 4.2
  },
  "annotations": {
    "anomalies": [
      {
        "timestamp": "2023-06-14T13:00:00Z",
        "description": "Unusual spike in execution time"
      }
    ]
  }
}
```

## Alert Endpoints

### Get Alerts

```
GET /monitoring/alerts
```

Retrieves a paginated list of alerts with optional filtering.

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `start_date` (string, optional): Filter alerts created after this date/time (ISO 8601 format)
- `end_date` (string, optional): Filter alerts created before this date/time (ISO 8601 format)
- `severity` (string, optional): Filter by severity (critical, high, medium, low)
- `status` (string, optional): Filter by status (new, acknowledged, resolved)
- `component` (string, optional): Filter by pipeline component
- `pipeline_id` (string, optional): Filter by specific pipeline ID

**Response:**
```json
{
  "status": "success",
  "message": "Alerts retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "items": [
    {
      "alert_id": "550e8400-e29b-41d4-a716-446655440010",
      "alert_type": "pipeline_failure",
      "severity": "high",
      "status": "new",
      "description": "Pipeline execution failed: customer_data_load",
      "component": "ingestion",
      "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
      "created_at": "2023-06-15T09:45:00Z",
      "updated_at": "2023-06-15T09:45:00Z",
      "context": {
        "execution_id": "789e0123-e45b-67d8-a901-234567890000",
        "error_message": "BigQuery error: Quota exceeded"
      }
    },
    {
      "alert_id": "550e8400-e29b-41d4-a716-446655440011",
      "alert_type": "data_quality",
      "severity": "medium",
      "status": "acknowledged",
      "description": "Schema drift detected in sales_data table",
      "component": "quality",
      "pipeline_id": "123e4567-e89b-12d3-a456-426614174001",
      "created_at": "2023-06-15T08:30:00Z",
      "updated_at": "2023-06-15T08:45:00Z",
      "acknowledged_by": "user@example.com",
      "acknowledged_at": "2023-06-15T08:45:00Z",
      "context": {
        "execution_id": "789e0123-e45b-67d8-a901-234567890001",
        "quality_rule_id": "abc12345-e89b-12d3-a456-426614174000"
      }
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 5,
    "total_pages": 1
  }
}
```

### Get Alert by ID

```
GET /monitoring/alerts/{alert_id}
```

Retrieves a specific alert by ID.

**Path Parameters:**
- `alert_id` (string, required): The unique identifier of the alert

**Response:**
```json
{
  "status": "success",
  "message": "Alert data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "alert_id": "550e8400-e29b-41d4-a716-446655440010",
    "alert_type": "pipeline_failure",
    "severity": "high",
    "status": "new",
    "description": "Pipeline execution failed: customer_data_load",
    "component": "ingestion",
    "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
    "created_at": "2023-06-15T09:45:00Z",
    "updated_at": "2023-06-15T09:45:00Z",
    "context": {
      "execution_id": "789e0123-e45b-67d8-a901-234567890000",
      "error_message": "BigQuery error: Quota exceeded",
      "job_id": "bq_load_customer_20230615_0945",
      "task_id": "load_to_bigquery"
    },
    "related_alerts": [
      {
        "alert_id": "550e8400-e29b-41d4-a716-446655440012",
        "alert_type": "pipeline_failure",
        "description": "Similar failure in product_load pipeline"
      }
    ],
    "suggested_actions": [
      {
        "action_id": "action_001",
        "description": "Increase BigQuery slot reservation"
      },
      {
        "action_id": "action_002",
        "description": "Implement load job scheduling"
      }
    ],
    "self_healing_status": {
      "attempted": true,
      "successful": false,
      "actions_taken": [
        "Attempted to queue job with lower priority"
      ],
      "reason": "Insufficient slots available in reservation"
    }
  }
}
```

### Acknowledge Alert

```
POST /monitoring/alerts/{alert_id}/acknowledge
```

Acknowledges an alert, updating its status.

**Path Parameters:**
- `alert_id` (string, required): The unique identifier of the alert

**Request Body:**
```json
{
  "notes": "Investigating the BigQuery quota issue",
  "suppress_similar": true
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Alert acknowledged successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:35:00Z"
  },
  "data": {
    "alert_id": "550e8400-e29b-41d4-a716-446655440010",
    "acknowledged": true,
    "acknowledged_by": "user@example.com",
    "acknowledged_at": "2023-06-15T10:35:00Z",
    "similar_suppressed": true,
    "suppressed_count": 2
  }
}
```

### Resolve Alert

```
POST /monitoring/alerts/{alert_id}/resolve
```

Resolves an alert, updating its status.

**Path Parameters:**
- `alert_id` (string, required): The unique identifier of the alert

**Request Body:**
```json
{
  "resolution_notes": "Increased BigQuery slot reservation to resolve quota issue",
  "resolution_type": "manual_fix",
  "prevent_recurrence": true,
  "prevention_action": "Updated slot reservation policy"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Alert resolved successfully",
  "metadata": {
    "timestamp": "2023-06-15T11:15:00Z"
  },
  "data": {
    "alert_id": "550e8400-e29b-41d4-a716-446655440010",
    "resolved": true,
    "resolved_by": "user@example.com",
    "resolved_at": "2023-06-15T11:15:00Z",
    "resolution_type": "manual_fix",
    "time_to_resolution": 5400
  }
}
```

## Anomaly Endpoints

### Get Anomalies

```
GET /monitoring/anomalies
```

Retrieves a paginated list of detected anomalies with optional filtering.

**Query Parameters:**
- `page` (integer, optional): Page number for pagination (default: 1)
- `page_size` (integer, optional): Number of items per page (default: 20, max: 100)
- `start_date` (string, optional): Filter anomalies detected after this date/time (ISO 8601 format)
- `end_date` (string, optional): Filter anomalies detected before this date/time (ISO 8601 format)
- `metric_name` (string, optional): Filter by metric name
- `severity` (string, optional): Filter by severity (critical, high, medium, low)
- `component` (string, optional): Filter by pipeline component
- `min_confidence` (number, optional): Filter by minimum confidence score (0-1)

**Response:**
```json
{
  "status": "success",
  "message": "Data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "items": [
      {
        "anomaly_id": "550e8400-e29b-41d4-a716-446655440020",
        "metric_name": "pipeline_execution_time",
        "component": "ingestion",
        "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
        "detected_at": "2023-06-15T08:15:00Z",
        "severity": "medium",
        "confidence_score": 0.92,
        "anomaly_type": "spike",
        "expected_value": 145.0,
        "actual_value": 230.5,
        "deviation_percentage": 59.0,
        "context": {
          "execution_id": "789e0123-e45b-67d8-a901-234567890000"
        },
        "alert_generated": true,
        "alert_id": "550e8400-e29b-41d4-a716-446655440013"
      },
      {
        "anomaly_id": "550e8400-e29b-41d4-a716-446655440021",
        "metric_name": "error_rate",
        "component": "quality",
        "pipeline_id": "123e4567-e89b-12d3-a456-426614174001",
        "detected_at": "2023-06-15T07:30:00Z",
        "severity": "high",
        "confidence_score": 0.95,
        "anomaly_type": "trend",
        "expected_value": 0.02,
        "actual_value": 0.15,
        "deviation_percentage": 650.0,
        "context": {
          "dataset": "sales_data"
        },
        "alert_generated": true,
        "alert_id": "550e8400-e29b-41d4-a716-446655440014"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total_items": 2,
      "total_pages": 1
    }
  }
}
```

### Get Anomaly by ID

```
GET /monitoring/anomalies/{anomaly_id}
```

Retrieves a specific anomaly by ID.

**Path Parameters:**
- `anomaly_id` (string, required): The unique identifier of the anomaly

**Response:**
```json
{
  "status": "success",
  "message": "Data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "anomaly_id": "550e8400-e29b-41d4-a716-446655440020",
    "metric_name": "pipeline_execution_time",
    "component": "ingestion",
    "pipeline_id": "123e4567-e89b-12d3-a456-426614174000",
    "detected_at": "2023-06-15T08:15:00Z",
    "severity": "medium",
    "confidence_score": 0.92,
    "anomaly_type": "spike",
    "expected_value": 145.0,
    "actual_value": 230.5,
    "deviation_percentage": 59.0,
    "detection_method": "statistical",
    "model_version": "1.2.0",
    "historical_context": {
      "mean": 142.3,
      "median": 145.0,
      "std_dev": 12.5,
      "min": 120.1,
      "max": 170.2,
      "z_score": 7.1
    },
    "context": {
      "execution_id": "789e0123-e45b-67d8-a901-234567890000",
      "task_id": "extract_data",
      "source_system": "sales_database"
    },
    "alert_generated": true,
    "alert_id": "550e8400-e29b-41d4-a716-446655440013",
    "similar_anomalies": [
      {
        "anomaly_id": "550e8400-e29b-41d4-a716-446655440022",
        "detected_at": "2023-06-14T08:30:00Z",
        "metric_name": "pipeline_execution_time",
        "actual_value": 210.3
      }
    ],
    "potential_causes": [
      "Increased data volume",
      "Source system performance degradation",
      "Network latency"
    ],
    "recommended_actions": [
      "Check source system performance",
      "Analyze data volume trends",
      "Review network connectivity"
    ]
  }
}
```

## Configuration Endpoints

### Get Alert Configuration

```
GET /monitoring/config/alerts
```

Retrieves the current alert configuration.

**Response:**
```json
{
  "status": "success",
  "message": "Alert configuration retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "teams_webhook_url": {
      "webhook_url": "https://outlook.office.com/webhook/...",
      "enabled": true
    },
    "email_config": {
      "smtp_server": "smtp.example.com",
      "smtp_port": 587,
      "sender_email": "pipeline-alerts@example.com",
      "recipients": [
        "data-team@example.com",
        "operations@example.com"
      ],
      "enabled": true
    },
    "alert_thresholds": {
      "pipeline_failure": {
        "critical": 1,
        "high": 2,
        "medium": 5,
        "low": 10
      },
      "data_quality": {
        "critical": 0.95,
        "high": 0.9,
        "medium": 0.8,
        "low": 0.7
      },
      "performance": {
        "critical": 100,
        "high": 50,
        "medium": 30,
        "low": 10
      }
    },
    "enabled_channels": {
      "teams": true,
      "email": true,
      "sms": false,
      "webhook": false
    },
    "notification_schedule": {
      "business_hours_only": false,
      "quiet_hours": {
        "enabled": true,
        "start_time": "22:00:00",
        "end_time": "06:00:00",
        "timezone": "America/New_York",
        "override_for_critical": true
      }
    },
    "alert_grouping": {
      "enabled": true,
      "group_by_component": true,
      "group_by_pipeline": true,
      "max_group_size": 10
    }
  }
}
```

### Update Alert Configuration

```
PUT /monitoring/config/alerts
```

Updates the alert configuration.

**Request Body:**
```json
{
  "teams_webhook_url": {
    "webhook_url": "https://outlook.office.com/webhook/...",
    "enabled": true
  },
  "email_config": {
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "sender_email": "pipeline-alerts@example.com",
    "recipients": [
      "data-team@example.com",
      "operations@example.com",
      "manager@example.com"
    ],
    "enabled": true
  },
  "alert_thresholds": {
    "pipeline_failure": {
      "critical": 1,
      "high": 3,
      "medium": 5,
      "low": 10
    },
    "data_quality": {
      "critical": 0.95,
      "high": 0.9,
      "medium": 0.8,
      "low": 0.7
    }
  },
  "enabled_channels": {
    "teams": true,
    "email": true,
    "sms": false,
    "webhook": true
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Alert configuration updated successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:45:00Z"
  },
  "data": {
    "teams_webhook_url": {
      "webhook_url": "https://outlook.office.com/webhook/...",
      "enabled": true
    },
    "email_config": {
      "smtp_server": "smtp.example.com",
      "smtp_port": 587,
      "sender_email": "pipeline-alerts@example.com",
      "recipients": [
        "data-team@example.com",
        "operations@example.com",
        "manager@example.com"
      ],
      "enabled": true
    },
    "alert_thresholds": {
      "pipeline_failure": {
        "critical": 1,
        "high": 3,
        "medium": 5,
        "low": 10
      },
      "data_quality": {
        "critical": 0.95,
        "high": 0.9,
        "medium": 0.8,
        "low": 0.7
      },
      "performance": {
        "critical": 100,
        "high": 50,
        "medium": 30,
        "low": 10
      }
    },
    "enabled_channels": {
      "teams": true,
      "email": true,
      "sms": false,
      "webhook": true
    },
    "notification_schedule": {
      "business_hours_only": false,
      "quiet_hours": {
        "enabled": true,
        "start_time": "22:00:00",
        "end_time": "06:00:00",
        "timezone": "America/New_York",
        "override_for_critical": true
      }
    },
    "alert_grouping": {
      "enabled": true,
      "group_by_component": true,
      "group_by_pipeline": true,
      "max_group_size": 10
    }
  }
}
```

## System Metrics Endpoint

### Get System Metrics

```
GET /monitoring/system
```

Retrieves current system-level metrics for monitoring.

**Response:**
```json
{
  "status": "success",
  "message": "Data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "compute": {
      "composer": {
        "cpu_utilization": 42.5,
        "memory_utilization": 68.3,
        "worker_count": 5,
        "queue_depth": 12
      },
      "functions": {
        "active_instances": 8,
        "execution_count": 1245,
        "error_rate": 0.02,
        "average_execution_time": 1.2
      },
      "dataflow": {
        "active_jobs": 2,
        "worker_count": 10,
        "cpu_utilization": 65.2,
        "memory_utilization": 72.1
      }
    },
    "storage": {
      "bigquery": {
        "slot_utilization": 78.5,
        "active_queries": 15,
        "bytes_processed_today": 1250000000000,
        "storage_used": 5400000000000
      },
      "cloud_storage": {
        "total_size": 8500000000000,
        "object_count": 125000,
        "read_ops": 4500,
        "write_ops": 1200
      }
    },
    "network": {
      "ingress_bytes": 450000000,
      "egress_bytes": 1200000000,
      "request_count": 25000
    },
    "pipeline_stats": {
      "active_pipelines": 8,
      "pipelines_executed_today": 42,
      "success_rate_7d": 98.5,
      "avg_execution_time_7d": 156.2
    },
    "health_status": {
      "overall": "healthy",
      "components": {
        "ingestion": "healthy",
        "quality": "healthy",
        "self_healing": "healthy",
        "monitoring": "healthy",
        "bigquery": "healthy"
      }
    }
  }
}
```

## Dashboard Endpoint

### Get Dashboard Summary

```
GET /monitoring/dashboard
```

Retrieves a summary of monitoring data for the dashboard.

**Response:**
```json
{
  "status": "success",
  "message": "Data retrieved successfully",
  "metadata": {
    "timestamp": "2023-06-15T10:30:00Z"
  },
  "data": {
    "pipeline_health": {
      "health_score": 98,
      "pipelines_total": 12,
      "pipelines_healthy": 11,
      "pipelines_warning": 1,
      "pipelines_error": 0,
      "executions_today": 42,
      "success_rate": 97.6
    },
    "data_quality": {
      "overall_score": 94,
      "rules_total": 245,
      "rules_passing": 231,
      "rules_failing": 14,
      "datasets_with_issues": 2,
      "trend": "stable"
    },
    "self_healing": {
      "issues_detected": 18,
      "issues_auto_resolved": 15,
      "success_rate": 83.3,
      "prevented_failures": 8,
      "learning_progress": 78
    },
    "alerts": {
      "active_total": 5,
      "by_severity": {
        "critical": 0,
        "high": 1,
        "medium": 2,
        "low": 2
      },
      "trend_7d": "decreasing"
    },
    "recent_executions": [
      {
        "pipeline_name": "analytics_daily",
        "status": "completed",
        "execution_time": "2023-06-15T05:30:00Z",
        "duration": 1250,
        "records_processed": 1250000
      },
      {
        "pipeline_name": "customer_load",
        "status": "completed",
        "execution_time": "2023-06-15T04:15:00Z",
        "duration": 950,
        "records_processed": 85000
      },
      {
        "pipeline_name": "product_enrich",
        "status": "failed",
        "execution_time": "2023-06-15T03:45:00Z",
        "duration": 738,
        "error": "Data quality check failed"
      },
      {
        "pipeline_name": "inventory_sync",
        "status": "completed",
        "execution_time": "2023-06-15T02:30:00Z",
        "duration": 620,
        "records_processed": 45000
      }
    ],
    "system_status": {
      "composer": "healthy",
      "bigquery": "healthy",
      "cloud_storage": "healthy",
      "cloud_functions": "healthy",
      "external_apis": "warning"
    },
    "resource_utilization": {
      "bigquery_slots": 78.5,
      "composer_workers": 85.2,
      "storage_growth": 12.5
    },
    "ai_insights": [
      {
        "type": "prediction",
        "message": "Predicted slowdown in sales_metrics pipeline at 14:00",
        "confidence": 0.85
      },
      {
        "type": "pattern",
        "message": "Recurring nulls detected in customer_address field",
        "confidence": 0.92
      },
      {
        "type": "optimization",
        "message": "Query optimization available for orders table",
        "confidence": 0.88
      }
    ]
  }
}
```

## Error Responses

The Monitoring & Alerting API uses standard HTTP status codes and a consistent error response format:

### 400 Bad Request

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Invalid request - 'severity' must be one of: critical, high, medium, low"
}
```

### 401 Unauthorized

```json
{
  "status": "error",
  "error": "AUTH_ERROR",
  "message": "Authentication required"
}
```

### 403 Forbidden

```json
{
  "status": "error",
  "error": "PERMISSION_ERROR",
  "message": "Insufficient permissions - requires monitoring:admin"
}
```

### 404 Not Found

```json
{
  "status": "error",
  "error": "RESOURCE_NOT_FOUND",
  "message": "Alert with ID '550e8400-e29b-41d4-a716-446655440099' not found"
}
```

### 429 Too Many Requests

```json
{
  "status": "error",
  "error": "RATE_LIMIT_EXCEEDED",
  "message": "Rate limit exceeded - try again in 30 seconds"
}
```

### 500 Internal Server Error

```json
{
  "status": "error",
  "error": "INTERNAL_ERROR",
  "message": "An unexpected error occurred"
}
```

## Pagination

List endpoints in the Monitoring & Alerting API support pagination through the `page` and `page_size` query parameters. Responses include pagination metadata in the following format:

```json
{
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 42,
    "total_pages": 3
  }
}
```

The default page size is 20 items, with a maximum of 100 items per page. To navigate through pages, increment the `page` parameter.

## Filtering

Many endpoints support filtering through query parameters. Common filter parameters include:

- **Time-based filters**: `start_date` and `end_date` in ISO 8601 format (e.g., `2023-06-15T10:00:00Z`)
- **Category filters**: `severity`, `status`, `component`, `metric_category`
- **ID filters**: `pipeline_id`, `metric_name`

Filters can be combined to narrow down results. For example:

```
GET /monitoring/alerts?severity=high&status=new&component=ingestion
```

This would return only new, high-severity alerts from the ingestion component.

## Best Practices

### Efficient API Usage

1. **Use filtering**: Always use appropriate filter parameters to limit the data returned, especially for large datasets.

2. **Pagination**: Implement proper pagination when retrieving large collections of data.

3. **Caching**: Cache dashboard and summary data that doesn't change frequently to reduce API calls.

4. **Webhook Integration**: For real-time monitoring, consider setting up webhook notifications instead of polling the API.

### Monitoring Implementation

1. **Alert Prioritization**: Configure alert thresholds appropriately to avoid alert fatigue.

2. **Metric Selection**: Focus on key metrics that provide actionable insights rather than tracking everything.

3. **Correlation**: Use the API to correlate metrics, alerts, and anomalies for better root cause analysis.

4. **Automation**: Implement automated responses to common alerts using the self-healing API.

## Rate Limits

The Monitoring & Alerting API implements rate limiting to ensure fair usage and system stability:

| Endpoint Category | Rate Limit |
| --- | --- |
| Read operations (GET) | 300 requests per minute |
| Write operations (POST, PUT) | 60 requests per minute |
| Dashboard endpoints | 120 requests per minute |

When a rate limit is exceeded, the API returns a `429 Too Many Requests` status code with headers indicating the rate limit and when it will reset:

```
X-RateLimit-Limit: 300
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1623766800
```

Implement exponential backoff retry logic when encountering rate limit errors.

## Webhook Notifications

In addition to polling the API for alerts and anomalies, you can configure webhook notifications to receive real-time updates. This is configured through the Alert Configuration endpoint.

Webhook notifications are sent as HTTP POST requests with the following payload format:

```json
{
  "event_type": "alert_created",
  "timestamp": "2023-06-15T09:45:00Z",
  "data": {
    "alert_id": "550e8400-e29b-41d4-a716-446655440010",
    "alert_type": "pipeline_failure",
    "severity": "high",
    "description": "Pipeline execution failed: customer_data_load",
    "component": "ingestion",
    "pipeline_id": "123e4567-e89b-12d3-a456-426614174000"
  }
}
```

Event types include:
- `alert_created`
- `alert_acknowledged`
- `alert_resolved`
- `anomaly_detected`
- `threshold_breached`

Your webhook endpoint should respond with a 200 OK status code to acknowledge receipt of the notification.

## Client Libraries

While you can use the Monitoring & Alerting API directly with HTTP requests, we provide client libraries for common programming languages to simplify integration:

```python
# Python example using the client library
from selfhealing_pipeline import MonitoringClient

# Initialize client with authentication
client = MonitoringClient(api_key="YOUR_API_KEY")

# Get active critical alerts
alerts = client.get_alerts(status="new", severity="critical")
for alert in alerts:
    print(f"Critical alert: {alert.description}")
    
    # Acknowledge the alert
    client.acknowledge_alert(alert.alert_id, notes="Investigating the issue")

# Get dashboard summary
dashboard = client.get_dashboard_summary()
print(f"Pipeline health score: {dashboard.pipeline_health.health_score}")
```

```javascript
// JavaScript example using the client library
import { MonitoringClient } from 'selfhealing-pipeline-client';

// Initialize client with authentication
const client = new MonitoringClient({ apiKey: 'YOUR_API_KEY' });

// Get system metrics
async function getSystemHealth() {
  try {
    const metrics = await client.getSystemMetrics();
    console.log('System health:', metrics.health_status.overall);
    console.log('BigQuery slot utilization:', metrics.storage.bigquery.slot_utilization);
    
    // Get recent anomalies
    const anomalies = await client.getAnomalies({
      start_date: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      min_confidence: 0.8
    });
    
    console.log(`Detected ${anomalies.items.length} anomalies in the last 24 hours`);
  } catch (error) {
    console.error('Error fetching monitoring data:', error);
  }
}

getSystemHealth();
```

## Related Resources

- API Overview - General API information and conventions
- [Authentication](authentication.md) - Details on authentication and authorization
- Healing API - API for self-healing capabilities
- Quality API - API for data quality validation