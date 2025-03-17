---
id: optimization-api
title: Optimization API
sidebar_label: Optimization
---

import Authentication from './authentication.md';
import { API_BASE_URL } from './overview.md';

## Introduction

The Performance Optimization API provides endpoints for optimizing BigQuery queries, table schemas, and resource utilization in the self-healing data pipeline. These endpoints enable you to analyze and improve performance, reduce costs, and ensure efficient resource usage across your data pipeline operations.

This API is particularly useful for data engineers and administrators who want to optimize their BigQuery workloads, implement best practices for table design, and ensure efficient resource allocation.

## Authentication

All optimization API endpoints require authentication. Please refer to the [Authentication](authentication.md) documentation for details on how to authenticate your requests.

The optimization endpoints require specific permissions:

- `optimization:read` - Required for viewing optimization recommendations and configurations
- `optimization:execute` - Required for applying optimizations
- `optimization:update` - Required for updating optimization configurations

## Base URL

All optimization API endpoints are relative to the base URL and prefixed with `/optimization`:

```
https://api.example.com/api/v1/optimization
```

## Query Optimization Endpoints

These endpoints allow you to analyze and optimize BigQuery SQL queries to improve performance and reduce costs.

### Get Query Optimization Recommendations

```
POST /optimization/query/recommendations
```

Analyzes a SQL query and provides optimization recommendations without modifying the query.

**Request Body:**

```json
{
  "query": "SELECT * FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
  "optimization_techniques": ["predicate_pushdown", "join_reordering", "materialization"]
}
```

**Parameters:**

- `query` (string, required): The SQL query to analyze
- `optimization_techniques` (array of strings, optional): Specific optimization techniques to apply. If not provided, all applicable techniques will be considered.

**Response:**

```json
{
  "status": "success",
  "data": {
    "recommendation_id": "550e8400-e29b-41d4-a716-446655440000",
    "optimization_type": "query",
    "target_resource": "BigQuery Query",
    "recommendations": [
      {
        "technique": "predicate_pushdown",
        "description": "Push WHERE clause into the source table scan",
        "confidence": 0.95,
        "estimated_improvement": "30% reduction in bytes processed"
      },
      {
        "technique": "column_pruning",
        "description": "Select only required columns instead of using SELECT *",
        "confidence": 0.99,
        "estimated_improvement": "60% reduction in bytes processed"
      }
    ],
    "impact_assessment": {
      "cost_reduction": "Approximately 45% reduction in query cost",
      "performance_improvement": "Approximately 35% reduction in execution time",
      "resource_savings": "Approximately 50% reduction in slot usage"
    }
  }
}
```

### Optimize Query

```
POST /optimization/query/optimize
```

Optimizes a SQL query and returns the optimized version with performance comparison.

**Request Body:**

```json
{
  "query": "SELECT * FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
  "optimization_techniques": ["predicate_pushdown", "join_reordering", "materialization"],
  "validate_results": true
}
```

**Parameters:**

- `query` (string, required): The SQL query to optimize
- `optimization_techniques` (array of strings, optional): Specific optimization techniques to apply. If not provided, all applicable techniques will be considered.
- `validate_results` (boolean, optional): Whether to validate that the optimized query returns the same results as the original query. Default is `true`.

**Response:**

```json
{
  "status": "success",
  "data": {
    "original_query": "SELECT * FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
    "optimized_query": "SELECT id, name, value, timestamp FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
    "applied_techniques": [
      "column_pruning",
      "predicate_pushdown"
    ],
    "performance_comparison": {
      "original": {
        "estimated_bytes_processed": 1073741824,
        "estimated_execution_time": "10 seconds",
        "estimated_cost": "$0.005"
      },
      "optimized": {
        "estimated_bytes_processed": 536870912,
        "estimated_execution_time": "6 seconds",
        "estimated_cost": "$0.0025"
      },
      "improvement": {
        "bytes_processed": "50%",
        "execution_time": "40%",
        "cost": "50%"
      }
    },
    "results_validated": true
  }
}
```

### Get Query Optimization History

```
GET /optimization/query/history
```

Retrieves optimization history for similar queries.

**Query Parameters:**

- `query_hash` (string, optional): Hash of the query to find similar optimizations
- `start_date` (string, optional): Start date for filtering (ISO 8601 format)
- `end_date` (string, optional): End date for filtering (ISO 8601 format)
- `page` (integer, optional): Page number for pagination. Default is 1.
- `page_size` (integer, optional): Number of items per page. Default is 20, maximum is 100.

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "timestamp": "2023-06-15T10:30:00Z",
      "query_hash": "a1b2c3d4e5f6",
      "original_query": "SELECT * FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
      "optimized_query": "SELECT id, name, value, timestamp FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
      "improvement": {
        "bytes_processed": "50%",
        "execution_time": "40%",
        "cost": "50%"
      },
      "user": "john.doe@example.com"
    },
    {
      "timestamp": "2023-06-14T15:45:00Z",
      "query_hash": "a1b2c3d4e5f6",
      "original_query": "SELECT * FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
      "optimized_query": "SELECT id, name, value, timestamp FROM `project.dataset.table` WHERE timestamp > '2023-01-01'",
      "improvement": {
        "bytes_processed": "50%",
        "execution_time": "40%",
        "cost": "50%"
      },
      "user": "jane.smith@example.com"
    }
  ],
  "metadata": {
    "page": 1,
    "page_size": 20,
    "total": 2,
    "total_pages": 1
  }
}
```

## Schema Optimization Endpoints

These endpoints allow you to analyze and optimize BigQuery table schemas to improve query performance and reduce costs.

### Get Schema Optimization Recommendations

```
GET /optimization/schema/recommendations/{dataset}/{table}
```

Analyzes a BigQuery table schema and provides optimization recommendations.

**Path Parameters:**

- `dataset` (string, required): The BigQuery dataset name
- `table` (string, required): The BigQuery table name

**Response:**

```json
{
  "status": "success",
  "data": {
    "recommendation_id": "550e8400-e29b-41d4-a716-446655440000",
    "optimization_type": "schema",
    "target_resource": "project.dataset.table",
    "recommendations": [
      {
        "technique": "partitioning",
        "description": "Partition table by date field 'timestamp'",
        "confidence": 0.95,
        "estimated_improvement": "70% reduction in query costs for time-based queries",
        "implementation": {
          "partitioning_field": "timestamp",
          "partitioning_type": "DAY"
        }
      },
      {
        "technique": "clustering",
        "description": "Cluster table by fields 'category', 'region'",
        "confidence": 0.90,
        "estimated_improvement": "40% improvement in query performance for filtered queries",
        "implementation": {
          "clustering_fields": ["category", "region"]
        }
      }
    ],
    "impact_assessment": {
      "cost_reduction": "Approximately 60% reduction in query cost for typical workloads",
      "performance_improvement": "Approximately 45% reduction in execution time for typical queries",
      "implementation_complexity": "Medium - requires table recreation"
    },
    "implementation_script": "CREATE OR REPLACE TABLE `project.dataset.table` PARTITION BY DATE(timestamp) CLUSTER BY category, region AS SELECT * FROM `project.dataset.table_backup`;"
  }
}
```

### Apply Schema Optimizations

```
POST /optimization/schema/optimize/{dataset}/{table}
```

Applies recommended schema optimizations to a BigQuery table.

**Path Parameters:**

- `dataset` (string, required): The BigQuery dataset name
- `table` (string, required): The BigQuery table name

**Request Body:**

```json
{
  "optimizations": {
    "partitioning": {
      "field": "timestamp",
      "type": "DAY"
    },
    "clustering": {
      "fields": ["category", "region"]
    },
    "create_backup": true
  }
}
```

**Parameters:**

- `optimizations` (object, required): The optimizations to apply
  - `partitioning` (object, optional): Partitioning configuration
    - `field` (string, required): The field to partition by
    - `type` (string, required): The partitioning type (DAY, MONTH, YEAR, INTEGER_RANGE)
  - `clustering` (object, optional): Clustering configuration
    - `fields` (array of strings, required): The fields to cluster by (up to 4)
  - `create_backup` (boolean, optional): Whether to create a backup of the original table. Default is `true`.

**Response:**

```json
{
  "status": "success",
  "data": {
    "job_id": "project:us.bquxjob_5678abcd_1234abcd_abcd1234",
    "status": "RUNNING",
    "backup_table": "project.dataset.table_backup_20230615",
    "estimated_completion_time": "2023-06-15T11:30:00Z",
    "monitoring_url": "https://console.cloud.google.com/bigquery?project=project&page=query&j=bquxjob_5678abcd_1234abcd_abcd1234"
  }
}
```

### Get Schema Optimization Candidates

```
GET /optimization/schema/candidates
```

Identifies tables that would benefit from schema optimization.

**Query Parameters:**

- `dataset` (string, optional): Filter by dataset name
- `min_table_size_gb` (number, optional): Minimum table size in GB to consider
- `min_query_count` (integer, optional): Minimum number of queries against the table to consider
- `page` (integer, optional): Page number for pagination. Default is 1.
- `page_size` (integer, optional): Number of items per page. Default is 20, maximum is 100.

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "dataset": "sales_data",
      "table": "transactions",
      "size_gb": 120.5,
      "row_count": 1500000000,
      "query_count_last_30_days": 450,
      "avg_query_cost": "$2.30",
      "optimization_potential": "High",
      "recommended_optimizations": ["partitioning", "clustering"],
      "estimated_savings": "$800/month"
    },
    {
      "dataset": "marketing",
      "table": "campaign_performance",
      "size_gb": 45.2,
      "row_count": 500000000,
      "query_count_last_30_days": 280,
      "avg_query_cost": "$0.85",
      "optimization_potential": "Medium",
      "recommended_optimizations": ["clustering"],
      "estimated_savings": "$200/month"
    }
  ],
  "metadata": {
    "page": 1,
    "page_size": 20,
    "total": 2,
    "total_pages": 1
  }
}
```

## Resource Optimization Endpoints

These endpoints allow you to analyze and optimize resource utilization across the pipeline.

### Get Resource Optimization Recommendations

```
GET /optimization/resource/recommendations
```

Retrieves resource optimization recommendations across the pipeline.

**Query Parameters:**

- `resource_type` (string, optional): Filter by resource type (e.g., "bigquery_slots", "composer_workers", "storage")

**Response:**

```json
{
  "status": "success",
  "data": [
    {
      "recommendation_id": "550e8400-e29b-41d4-a716-446655440000",
      "optimization_type": "resource",
      "target_resource": "bigquery_slots",
      "recommendations": [
        {
          "strategy": "reservation_rightsizing",
          "description": "Reduce BigQuery slot reservation from 500 to 300 slots",
          "confidence": 0.92,
          "estimated_improvement": "$1,200/month cost savings with minimal performance impact",
          "implementation": {
            "current_slots": 500,
            "recommended_slots": 300,
            "peak_utilization": "65%"
          }
        }
      ],
      "impact_assessment": {
        "cost_reduction": "$1,200/month",
        "performance_impact": "Minimal (estimated 5% increase in average query time)",
        "implementation_complexity": "Low"
      }
    },
    {
      "recommendation_id": "660e8400-e29b-41d4-a716-446655440000",
      "optimization_type": "resource",
      "target_resource": "composer_workers",
      "recommendations": [
        {
          "strategy": "worker_rightsizing",
          "description": "Increase Composer worker count from 3 to 5 during peak hours",
          "confidence": 0.85,
          "estimated_improvement": "30% reduction in pipeline execution time during peak hours",
          "implementation": {
            "current_workers": 3,
            "recommended_workers": 5,
            "schedule": "Weekdays 8:00-18:00"
          }
        }
      ],
      "impact_assessment": {
        "cost_increase": "$300/month",
        "performance_improvement": "30% reduction in pipeline execution time during peak hours",
        "implementation_complexity": "Low"
      }
    }
  ]
}
```

### Apply Resource Optimization

```
POST /optimization/resource/optimize
```

Applies a recommended resource optimization.

**Request Body:**

```json
{
  "resource_type": "bigquery_slots",
  "strategy": "reservation_rightsizing",
  "parameters": {
    "current_slots": 500,
    "target_slots": 300,
    "implementation_schedule": "immediate"
  }
}
```

**Parameters:**

- `resource_type` (string, required): The type of resource to optimize
- `strategy` (string, required): The optimization strategy to apply
- `parameters` (object, required): Strategy-specific parameters

**Response:**

```json
{
  "status": "success",
  "data": {
    "job_id": "opt_job_123456",
    "status": "COMPLETED",
    "resource_type": "bigquery_slots",
    "previous_configuration": {
      "slots": 500
    },
    "new_configuration": {
      "slots": 300
    },
    "estimated_savings": "$1,200/month",
    "monitoring_period": "14 days",
    "rollback_available_until": "2023-06-29T10:30:00Z"
  }
}
```

### Get Resource Efficiency Metrics

```
GET /optimization/resource/metrics
```

Retrieves resource efficiency metrics across the pipeline.

**Query Parameters:**

- `days` (integer, optional): Number of days to include in the metrics. Default is 30.
- `resource_type` (string, optional): Filter by resource type

**Response:**

```json
{
  "status": "success",
  "data": {
    "time_period": "2023-05-15 to 2023-06-15",
    "resources": [
      {
        "resource_type": "bigquery_slots",
        "metrics": {
          "average_utilization": "45%",
          "peak_utilization": "78%",
          "idle_periods": "35% of time below 20% utilization",
          "cost_efficiency": "$0.15 per query",
          "trend": "Stable"
        },
        "recommendations": {
          "action": "Consider reducing slot reservation by 100 slots",
          "estimated_savings": "$400/month"
        }
      },
      {
        "resource_type": "composer_workers",
        "metrics": {
          "average_utilization": "75%",
          "peak_utilization": "95%",
          "queue_time": "Average 45 seconds during peak hours",
          "cost_efficiency": "$0.25 per DAG execution",
          "trend": "Increasing"
        },
        "recommendations": {
          "action": "Consider increasing workers during peak hours (8:00-18:00)",
          "estimated_benefit": "30% reduction in queue time"
        }
      }
    ],
    "overall_efficiency_score": 72,
    "historical_comparison": {
      "previous_period": 68,
      "trend": "+4 points"
    }
  }
}
```

## Optimization Configuration Endpoints

These endpoints allow you to manage optimization configuration settings.

### Get Optimization Configuration

```
GET /optimization/config
```

Retrieves the current optimization configuration.

**Response:**

```json
{
  "status": "success",
  "data": {
    "query_optimization_settings": {
      "enabled_techniques": ["predicate_pushdown", "join_reordering", "column_pruning", "materialization"],
      "default_validation": true,
      "max_query_cost_for_auto_optimization": "$10"
    },
    "schema_optimization_settings": {
      "auto_partitioning_enabled": true,
      "auto_clustering_enabled": true,
      "min_table_size_for_recommendations_gb": 1,
      "min_query_count_for_recommendations": 10
    },
    "resource_optimization_settings": {
      "slot_recommendation_enabled": true,
      "worker_recommendation_enabled": true,
      "storage_optimization_enabled": true,
      "recommendation_sensitivity": "medium"
    },
    "auto_implementation_enabled": false
  }
}
```

### Update Optimization Configuration

```
PUT /optimization/config
```

Updates the optimization configuration.

**Request Body:**

```json
{
  "query_optimization_settings": {
    "enabled_techniques": ["predicate_pushdown", "join_reordering", "column_pruning"],
    "default_validation": true,
    "max_query_cost_for_auto_optimization": "$5"
  },
  "schema_optimization_settings": {
    "auto_partitioning_enabled": true,
    "auto_clustering_enabled": true,
    "min_table_size_for_recommendations_gb": 5,
    "min_query_count_for_recommendations": 20
  },
  "resource_optimization_settings": {
    "slot_recommendation_enabled": true,
    "worker_recommendation_enabled": true,
    "storage_optimization_enabled": true,
    "recommendation_sensitivity": "high"
  },
  "auto_implementation_enabled": false
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "query_optimization_settings": {
      "enabled_techniques": ["predicate_pushdown", "join_reordering", "column_pruning"],
      "default_validation": true,
      "max_query_cost_for_auto_optimization": "$5"
    },
    "schema_optimization_settings": {
      "auto_partitioning_enabled": true,
      "auto_clustering_enabled": true,
      "min_table_size_for_recommendations_gb": 5,
      "min_query_count_for_recommendations": 20
    },
    "resource_optimization_settings": {
      "slot_recommendation_enabled": true,
      "worker_recommendation_enabled": true,
      "storage_optimization_enabled": true,
      "recommendation_sensitivity": "high"
    },
    "auto_implementation_enabled": false
  }
}
```

## Dashboard Data Endpoint

This endpoint provides summary data for the optimization dashboard.

### Get Optimization Dashboard Data

```
GET /optimization/dashboard
```

Retrieves summary data for the optimization dashboard.

**Response:**

```json
{
  "status": "success",
  "data": {
    "optimization_stats": {
      "queries_optimized_last_30_days": 245,
      "average_cost_reduction": "45%",
      "average_performance_improvement": "35%",
      "estimated_monthly_savings": "$3,500"
    },
    "recent_recommendations": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "type": "schema",
        "target": "project.dataset.large_table",
        "summary": "Partition by date and cluster by region",
        "estimated_impact": "$1,200/month savings",
        "created_at": "2023-06-14T15:30:00Z"
      },
      {
        "id": "660e8400-e29b-41d4-a716-446655440000",
        "type": "resource",
        "target": "BigQuery slots",
        "summary": "Reduce slot reservation by 100 slots",
        "estimated_impact": "$400/month savings",
        "created_at": "2023-06-13T10:15:00Z"
      }
    ],
    "resource_efficiency": {
      "bigquery_slots": {
        "current_utilization": "45%",
        "trend": "-5% from last month",
        "optimization_potential": "Medium"
      },
      "composer_workers": {
        "current_utilization": "75%",
        "trend": "+10% from last month",
        "optimization_potential": "Low"
      },
      "storage": {
        "current_utilization": "65%",
        "trend": "+15% from last month",
        "optimization_potential": "Medium"
      }
    },
    "top_optimization_candidates": [
      {
        "resource": "project.dataset.large_table",
        "type": "schema",
        "potential_savings": "$1,200/month",
        "recommendation": "Implement partitioning"
      },
      {
        "resource": "Frequently used query pattern",
        "type": "query",
        "potential_savings": "$800/month",
        "recommendation": "Optimize join order and add column pruning"
      }
    ]
  }
}
```

## Error Responses

The optimization API uses standard error responses as described in the [API Overview](overview.md#error-handling).

Specific error codes for the optimization API include:

- `INVALID_QUERY`: The provided SQL query is invalid or cannot be parsed
- `OPTIMIZATION_FAILED`: The optimization process failed
- `INVALID_DATASET_TABLE`: The specified dataset or table does not exist
- `INSUFFICIENT_PERMISSIONS`: The authenticated user doesn't have sufficient permissions on the target resource
- `IMPLEMENTATION_FAILED`: The optimization implementation failed
- `RESOURCE_NOT_FOUND`: The specified resource does not exist
- `INVALID_OPTIMIZATION_CONFIG`: The provided optimization configuration is invalid

## Best Practices

### Query Optimization

1. **Start with recommendations**: Use the `/query/recommendations` endpoint to understand potential optimizations before applying them.

2. **Validate results**: Always set `validate_results` to `true` when optimizing important queries to ensure the optimized query returns the same results.

3. **Batch optimization**: For large query workloads, consider analyzing query patterns and optimizing the most frequently used or expensive queries first.

### Schema Optimization

1. **Analyze before implementing**: Use the `/schema/recommendations` endpoint to understand the potential impact before applying schema changes.

2. **Consider query patterns**: Schema optimizations should be aligned with your query patterns. Partitioning and clustering are most effective when they match your most common filter and join conditions.

3. **Test after implementation**: After applying schema optimizations, test your queries to ensure they're using the optimized schema effectively.

### Resource Optimization

1. **Monitor after changes**: After applying resource optimizations, closely monitor performance to ensure the changes don't negatively impact your workloads.

2. **Consider workload patterns**: Resource optimizations should account for workload patterns, including peak usage times and seasonal variations.

3. **Implement gradually**: For significant resource changes, consider implementing them gradually to minimize risk.

## Rate Limits

The optimization API has the following rate limits:

- Query optimization endpoints: 100 requests per minute per user
- Schema optimization endpoints: 20 requests per minute per user
- Resource optimization endpoints: 20 requests per minute per user
- Configuration endpoints: 10 requests per minute per user

Exceeding these limits will result in a `429 Too Many Requests` response. Please implement appropriate backoff and retry logic in your applications.

## Code Examples

### Optimizing a BigQuery Query

```python
import requests

def optimize_query(token, query):
    """Optimize a BigQuery SQL query
    
    Args:
        token: Authentication token
        query: SQL query to optimize
        
    Returns:
        Optimization result with original and optimized query
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'query': query,
        'validate_results': True
    }
    
    response = requests.post(
        'https://api.example.com/api/v1/optimization/query/optimize',
        headers=headers,
        json=payload
    )
    
    response.raise_for_status()
    return response.json()['data']

# Example usage
try:
    token = 'YOUR_ACCESS_TOKEN'
    
    # Example query to optimize
    query = """
    SELECT * 
    FROM `project.dataset.large_table` 
    WHERE date_column > '2023-01-01' 
    AND region = 'US'
    """
    
    result = optimize_query(token, query)
    
    print("Original query:")
    print(result['original_query'])
    print("\nOptimized query:")
    print(result['optimized_query'])
    print("\nPerformance improvement:")
    print(f"Bytes processed: {result['performance_comparison']['improvement']['bytes_processed']}")
    print(f"Execution time: {result['performance_comparison']['improvement']['execution_time']}")
    print(f"Cost: {result['performance_comparison']['improvement']['cost']}")
    
except requests.exceptions.HTTPError as e:
    print(f'API Error: {e}')
```

### Getting Schema Optimization Recommendations

```javascript
/**
 * Get schema optimization recommendations for a BigQuery table
 * 
 * @param {string} token - Authentication token
 * @param {string} dataset - BigQuery dataset name
 * @param {string} table - BigQuery table name
 * @returns {Promise<Object>} Schema optimization recommendations
 */
async function getSchemaRecommendations(token, dataset, table) {
  const response = await fetch(
    `https://api.example.com/api/v1/optimization/schema/recommendations/${dataset}/${table}`,
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
 * Apply schema optimizations to a BigQuery table
 * 
 * @param {string} token - Authentication token
 * @param {string} dataset - BigQuery dataset name
 * @param {string} table - BigQuery table name
 * @param {Object} optimizations - Optimization configuration
 * @returns {Promise<Object>} Optimization job details
 */
async function applySchemaOptimizations(token, dataset, table, optimizations) {
  const response = await fetch(
    `https://api.example.com/api/v1/optimization/schema/optimize/${dataset}/${table}`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },\n      body: JSON.stringify({ optimizations })\n    }\n  );\n  \n  if (!response.ok) {\n    const errorData = await response.json();\n    throw new Error(`API error: ${errorData.message}`);\n  }\n  \n  const responseData = await response.json();\n  return responseData.data;\n}\n\n// Example usage\nasync function main() {\n  try {\n    const token = 'YOUR_ACCESS_TOKEN';\n    const dataset = 'sales_data';\n    const table = 'transactions';\n    \n    // Get recommendations first\n    const recommendations = await getSchemaRecommendations(token, dataset, table);\n    console.log('Recommendations:', recommendations);\n    \n    // Check if partitioning is recommended\n    const partitioningRec = recommendations.recommendations.find(r => r.technique === 'partitioning');\n    const clusteringRec = recommendations.recommendations.find(r => r.technique === 'clustering');\n    \n    if (partitioningRec || clusteringRec) {\n      // Prepare optimization configuration\n      const optimizations = {};\n      \n      if (partitioningRec) {\n        optimizations.partitioning = {\n          field: partitioningRec.implementation.partitioning_field,\n          type: partitioningRec.implementation.partitioning_type\n        };\n      }\n      \n      if (clusteringRec) {\n        optimizations.clustering = {\n          fields: clusteringRec.implementation.clustering_fields\n        };\n      }\n      \n      optimizations.create_backup = true;\n      \n      // Apply the optimizations\n      const result = await applySchemaOptimizations(token, dataset, table, optimizations);\n      console.log('Optimization job started:', result);\n      console.log('Backup table:', result.backup_table);\n      console.log('Estimated completion time:', result.estimated_completion_time);\n    } else {\n      console.log('No schema optimizations recommended for this table');\n    }\n    \n  } catch (error) {\n    console.error('API Error:', error.message);\n  }\n}\n\nmain();
```

### Resource Optimization with cURL