/**
 * Mock data for testing the self-healing data pipeline web application.
 * This file provides mock objects and generators for various data structures including
 * pipeline definitions, quality metrics, self-healing actions, alerts, and dashboard data.
 * Used by the mock service worker handlers to simulate API responses during testing.
 */

import {
  ID, Timestamp, JSONObject, AlertSeverity, PipelineStatus, HealingStatus, QualityStatus
} from '../../types/global';

import {
  PipelineDefinition, PipelineExecution, TaskExecution, QualityRule, QualityValidation, 
  IssuePattern, HealingAction, HealingExecution, Alert, DataResponse, ListResponse
} from '../../types/api';

import {
  DatasetQualitySummary, QualityIssue, QualityStatistics
} from '../../types/quality';

import {
  HealingIssue, AIModel, HealingSettings, HealingDashboardData
} from '../../types/selfHealing';

import {
  DashboardData, PipelineHealthMetrics, DataQualityMetrics, SelfHealingMetrics, 
  SystemStatus, QuickStats, AlertSummary, AIInsight
} from '../../types/dashboard';

/**
 * Generates a unique ID string for mock data objects
 */
export const generateId = (): ID => {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
};

/**
 * Generates a timestamp string for mock data objects
 * @param daysOffset Optional number of days to offset from current date
 */
export const generateTimestamp = (daysOffset?: number): Timestamp => {
  const date = new Date();
  if (daysOffset !== undefined) {
    date.setDate(date.getDate() + daysOffset);
  }
  return date.toISOString();
};

/**
 * Generates a mock paginated list response for API testing
 * @param items Array of items to include in the response
 * @param page Current page number
 * @param pageSize Number of items per page
 * @param totalItems Total number of items across all pages
 */
export function generateMockListResponse<T>(
  items: T[],
  page: number = 1,
  pageSize: number = 10,
  totalItems: number = items.length
): ListResponse<T> {
  const totalPages = Math.ceil(totalItems / pageSize);
  return {
    status: 'SUCCESS',
    message: 'Success',
    metadata: {
      timestamp: generateTimestamp(),
      requestId: generateId(),
      processingTime: Math.floor(Math.random() * 100)
    },
    items,
    pagination: {
      page,
      pageSize,
      totalItems,
      totalPages,
      nextPage: page < totalPages ? `/api/resource?page=${page + 1}&pageSize=${pageSize}` : undefined,
      previousPage: page > 1 ? `/api/resource?page=${page - 1}&pageSize=${pageSize}` : undefined
    }
  };
}

/**
 * Generates a mock data response for API testing
 * @param data The data to include in the response
 */
export function generateMockDataResponse<T>(data: T): DataResponse<T> {
  return {
    status: 'SUCCESS',
    message: 'Success',
    metadata: {
      timestamp: generateTimestamp(),
      requestId: generateId(),
      processingTime: Math.floor(Math.random() * 100)
    },
    data
  };
}

/**
 * Mock pipeline definitions
 */
export const mockPipelineDefinitions: PipelineDefinition[] = [
  {
    pipelineId: '1',
    pipelineName: 'Customer Data Pipeline',
    sourceId: 'src-001',
    sourceName: 'Customer Database',
    targetDataset: 'customer_data',
    targetTable: 'customer_profiles',
    configuration: {
      schedule: '0 */3 * * *',
      retries: 3,
      timeout: 3600
    },
    description: 'Daily extraction of customer data from operational database',
    isActive: true,
    metadata: {
      owner: 'data-engineering',
      criticality: 'high'
    },
    createdAt: generateTimestamp(-30),
    updatedAt: generateTimestamp(-5),
    lastExecutionStatus: PipelineStatus.HEALTHY,
    lastExecutionTime: generateTimestamp(-1)
  },
  {
    pipelineId: '2',
    pipelineName: 'Sales Metrics Pipeline',
    sourceId: 'src-002',
    sourceName: 'Sales Database',
    targetDataset: 'sales_metrics',
    targetTable: 'daily_sales',
    configuration: {
      schedule: '0 1 * * *',
      retries: 2,
      timeout: 7200
    },
    description: 'Daily aggregation of sales metrics',
    isActive: true,
    metadata: {
      owner: 'data-engineering',
      criticality: 'high'
    },
    createdAt: generateTimestamp(-45),
    updatedAt: generateTimestamp(-10),
    lastExecutionStatus: PipelineStatus.WARNING,
    lastExecutionTime: generateTimestamp(-1)
  },
  {
    pipelineId: '3',
    pipelineName: 'Product Catalog Pipeline',
    sourceId: 'src-003',
    sourceName: 'Product API',
    targetDataset: 'product_catalog',
    targetTable: 'products',
    configuration: {
      schedule: '0 */6 * * *',
      retries: 2,
      timeout: 1800
    },
    description: 'Regular updates of product catalog from API',
    isActive: true,
    metadata: {
      owner: 'product-team',
      criticality: 'medium'
    },
    createdAt: generateTimestamp(-60),
    updatedAt: generateTimestamp(-15),
    lastExecutionStatus: PipelineStatus.ERROR,
    lastExecutionTime: generateTimestamp(-1)
  },
  {
    pipelineId: '4',
    pipelineName: 'Inventory Sync Pipeline',
    sourceId: 'src-004',
    sourceName: 'Inventory System',
    targetDataset: 'inventory',
    targetTable: 'stock_levels',
    configuration: {
      schedule: '0 */2 * * *',
      retries: 3,
      timeout: 1200
    },
    description: 'Bi-hourly synchronization of inventory levels',
    isActive: true,
    metadata: {
      owner: 'operations',
      criticality: 'high'
    },
    createdAt: generateTimestamp(-75),
    updatedAt: generateTimestamp(-3),
    lastExecutionStatus: PipelineStatus.HEALTHY,
    lastExecutionTime: generateTimestamp(0)
  },
  {
    pipelineId: '5',
    pipelineName: 'Vendor Data Pipeline',
    sourceId: 'src-005',
    sourceName: 'Vendor Portal',
    targetDataset: 'vendor_data',
    targetTable: 'vendor_profiles',
    configuration: {
      schedule: '0 0 * * *',
      retries: 2,
      timeout: 3600
    },
    description: 'Daily extraction of vendor data',
    isActive: false,
    metadata: {
      owner: 'procurement',
      criticality: 'medium'
    },
    createdAt: generateTimestamp(-90),
    updatedAt: generateTimestamp(-90),
    lastExecutionStatus: PipelineStatus.INACTIVE,
    lastExecutionTime: generateTimestamp(-30)
  }
];

/**
 * Mock pipeline executions
 */
export const mockPipelineExecutions: PipelineExecution[] = [
  {
    executionId: '1',
    pipelineId: '1',
    pipelineName: 'Customer Data Pipeline',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    recordsProcessed: 15240,
    errorDetails: null,
    executionParams: { fullRefresh: false },
    dagRunId: 'airflow-run-001'
  },
  {
    executionId: '2',
    pipelineId: '2',
    pipelineName: 'Sales Metrics Pipeline',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.WARNING,
    recordsProcessed: 5630,
    errorDetails: 'Some records had missing values, using default values',
    executionParams: { fullRefresh: false },
    dagRunId: 'airflow-run-002'
  },
  {
    executionId: '3',
    pipelineId: '3',
    pipelineName: 'Product Catalog Pipeline',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.ERROR,
    recordsProcessed: 0,
    errorDetails: 'API connection timeout after 3 retry attempts',
    executionParams: { fullRefresh: true },
    dagRunId: 'airflow-run-003'
  },
  {
    executionId: '4',
    pipelineId: '4',
    pipelineName: 'Inventory Sync Pipeline',
    startTime: generateTimestamp(0),
    endTime: generateTimestamp(0),
    status: PipelineStatus.HEALTHY,
    recordsProcessed: 3450,
    errorDetails: null,
    executionParams: { fullRefresh: false },
    dagRunId: 'airflow-run-004'
  },
  {
    executionId: '5',
    pipelineId: '5',
    pipelineName: 'Vendor Data Pipeline',
    startTime: generateTimestamp(-30),
    endTime: generateTimestamp(-30),
    status: PipelineStatus.HEALTHY,
    recordsProcessed: 125,
    errorDetails: null,
    executionParams: { fullRefresh: true },
    dagRunId: 'airflow-run-005'
  }
];

/**
 * Mock task executions
 */
export const mockTaskExecutions: TaskExecution[] = [
  {
    taskExecutionId: '1-1',
    executionId: '1',
    taskId: 'extract_data',
    taskType: 'GCSExtractor',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    errorDetails: null,
    retryCount: 0,
    taskParams: { bucket: 'customer-data', pattern: '*.csv' }
  },
  {
    taskExecutionId: '1-2',
    executionId: '1',
    taskId: 'validate_data',
    taskType: 'DataValidator',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    errorDetails: null,
    retryCount: 0,
    taskParams: { rules: ['schema_match', 'null_check'] }
  },
  {
    taskExecutionId: '1-3',
    executionId: '1',
    taskId: 'load_to_bigquery',
    taskType: 'BigQueryLoader',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    errorDetails: null,
    retryCount: 0,
    taskParams: { dataset: 'customer_data', table: 'customer_profiles' }
  },
  {
    taskExecutionId: '2-1',
    executionId: '2',
    taskId: 'extract_data',
    taskType: 'DatabaseExtractor',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    errorDetails: null,
    retryCount: 0,
    taskParams: { connection: 'sales-db', query: 'SELECT * FROM daily_sales' }
  },
  {
    taskExecutionId: '2-2',
    executionId: '2',
    taskId: 'validate_data',
    taskType: 'DataValidator',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.WARNING,
    errorDetails: 'Missing values in optional fields',
    retryCount: 0,
    taskParams: { rules: ['schema_match', 'null_check', 'range_check'] }
  },
  {
    taskExecutionId: '2-3',
    executionId: '2',
    taskId: 'load_to_bigquery',
    taskType: 'BigQueryLoader',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.HEALTHY,
    errorDetails: null,
    retryCount: 0,
    taskParams: { dataset: 'sales_metrics', table: 'daily_sales' }
  },
  {
    taskExecutionId: '3-1',
    executionId: '3',
    taskId: 'extract_data',
    taskType: 'APIExtractor',
    startTime: generateTimestamp(-1),
    endTime: generateTimestamp(-1),
    status: PipelineStatus.ERROR,
    errorDetails: 'API connection timeout after 3 retry attempts',
    retryCount: 3,
    taskParams: { endpoint: 'https://api.example.com/products', method: 'GET' }
  }
];

/**
 * Mock quality rules
 */
export const mockQualityRules: QualityRule[] = [
  {
    ruleId: '1',
    ruleName: 'Customer ID Not Null',
    targetDataset: 'customer_data',
    targetTable: 'customer_profiles',
    ruleType: 'NULL_CHECK',
    expectationType: 'expect_column_values_to_not_be_null',
    ruleDefinition: {
      column: 'customer_id',
      param: {}
    },
    severity: AlertSeverity.CRITICAL,
    isActive: true,
    description: 'Customer ID must not be null',
    metadata: { category: 'data-integrity' },
    createdAt: generateTimestamp(-90),
    updatedAt: generateTimestamp(-30)
  },
  {
    ruleId: '2',
    ruleName: 'Email Format Check',
    targetDataset: 'customer_data',
    targetTable: 'customer_profiles',
    ruleType: 'PATTERN_MATCH',
    expectationType: 'expect_column_values_to_match_regex',
    ruleDefinition: {
      column: 'email',
      param: {
        regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
      }
    },
    severity: AlertSeverity.HIGH,
    isActive: true,
    description: 'Email must be in valid format',
    metadata: { category: 'data-quality' },
    createdAt: generateTimestamp(-85),
    updatedAt: generateTimestamp(-20)
  },
  {
    ruleId: '3',
    ruleName: 'Sales Amount Range Check',
    targetDataset: 'sales_metrics',
    targetTable: 'daily_sales',
    ruleType: 'VALUE_RANGE',
    expectationType: 'expect_column_values_to_be_between',
    ruleDefinition: {
      column: 'sales_amount',
      param: {
        min_value: 0,
        max_value: 10000
      }
    },
    severity: AlertSeverity.MEDIUM,
    isActive: true,
    description: 'Sales amount must be between 0 and 10000',
    metadata: { category: 'data-quality' },
    createdAt: generateTimestamp(-80),
    updatedAt: generateTimestamp(-15)
  },
  {
    ruleId: '4',
    ruleName: 'Product ID Reference Check',
    targetDataset: 'sales_metrics',
    targetTable: 'daily_sales',
    ruleType: 'REFERENTIAL',
    expectationType: 'expect_column_values_to_exist_in_set',
    ruleDefinition: {
      column: 'product_id',
      param: {
        query: 'SELECT product_id FROM product_catalog.products'
      }
    },
    severity: AlertSeverity.HIGH,
    isActive: true,
    description: 'Product ID must exist in product catalog',
    metadata: { category: 'data-integrity' },
    createdAt: generateTimestamp(-75),
    updatedAt: generateTimestamp(-10)
  },
  {
    ruleId: '5',
    ruleName: 'Inventory Level Non-Negative',
    targetDataset: 'inventory',
    targetTable: 'stock_levels',
    ruleType: 'VALUE_RANGE',
    expectationType: 'expect_column_values_to_be_between',
    ruleDefinition: {
      column: 'quantity',
      param: {
        min_value: 0,
        allow_null: false
      }
    },
    severity: AlertSeverity.CRITICAL,
    isActive: true,
    description: 'Inventory levels must not be negative',
    metadata: { category: 'data-integrity' },
    createdAt: generateTimestamp(-70),
    updatedAt: generateTimestamp(-5)
  }
];

/**
 * Mock dataset quality summaries
 */
export const mockDatasetQualitySummaries: DatasetQualitySummary[] = [
  {
    dataset: 'customer_data',
    tables: [
      { table: 'customer_profiles', qualityScore: 98 },
      { table: 'customer_addresses', qualityScore: 97 },
      { table: 'customer_preferences', qualityScore: 99 }
    ],
    overallScore: 98,
    trend: 'IMPROVING',
    issueCount: 2,
    lastUpdated: generateTimestamp(-1)
  },
  {
    dataset: 'sales_metrics',
    tables: [
      { table: 'daily_sales', qualityScore: 89 },
      { table: 'monthly_aggregates', qualityScore: 92 },
      { table: 'sales_forecasts', qualityScore: 87 }
    ],
    overallScore: 89,
    trend: 'DECLINING',
    issueCount: 15,
    lastUpdated: generateTimestamp(-1)
  },
  {
    dataset: 'product_catalog',
    tables: [
      { table: 'products', qualityScore: 100 },
      { table: 'categories', qualityScore: 100 },
      { table: 'suppliers', qualityScore: 100 }
    ],
    overallScore: 100,
    trend: 'STABLE',
    issueCount: 0,
    lastUpdated: generateTimestamp(-2)
  },
  {
    dataset: 'inventory',
    tables: [
      { table: 'stock_levels', qualityScore: 99 },
      { table: 'warehouses', qualityScore: 100 },
      { table: 'inventory_transactions', qualityScore: 98 }
    ],
    overallScore: 99,
    trend: 'STABLE',
    issueCount: 1,
    lastUpdated: generateTimestamp(0)
  },
  {
    dataset: 'vendor_data',
    tables: [
      { table: 'vendor_profiles', qualityScore: 94 },
      { table: 'vendor_contacts', qualityScore: 95 },
      { table: 'vendor_agreements', qualityScore: 93 }
    ],
    overallScore: 94,
    trend: 'IMPROVING',
    issueCount: 6,
    lastUpdated: generateTimestamp(-5)
  }
];

/**
 * Mock quality issues
 */
export const mockQualityIssues: QualityIssue[] = [
  {
    issueId: '1',
    dataset: 'sales_metrics',
    table: 'daily_sales',
    column: 'sales_amount',
    ruleId: '3',
    validationId: '3-1',
    description: 'Null values in sales_amount field',
    dimension: 'COMPLETENESS',
    severity: AlertSeverity.HIGH,
    status: 'OPEN',
    affectedRows: 15,
    detectedAt: generateTimestamp(-1),
    healingStatus: HealingStatus.IN_PROGRESS,
    healingId: '1',
    details: {
      samples: ['row_id_123', 'row_id_456']
    }
  },
  {
    issueId: '2',
    dataset: 'sales_metrics',
    table: 'daily_sales',
    column: 'discount_percent',
    ruleId: '3',
    validationId: '3-2',
    description: 'Outliers in discount_percent',
    dimension: 'ACCURACY',
    severity: AlertSeverity.MEDIUM,
    status: 'OPEN',
    affectedRows: 3,
    detectedAt: generateTimestamp(-1),
    healingStatus: HealingStatus.IN_PROGRESS,
    healingId: '2',
    details: {
      samples: ['row_id_789'],
      values: [120, 150, 200]
    }
  },
  {
    issueId: '3',
    dataset: 'customer_data',
    table: 'customer_profiles',
    column: 'email',
    ruleId: '2',
    validationId: '2-1',
    description: 'Invalid email format',
    dimension: 'VALIDITY',
    severity: AlertSeverity.MEDIUM,
    status: 'OPEN',
    affectedRows: 5,
    detectedAt: generateTimestamp(-2),
    healingStatus: HealingStatus.COMPLETED,
    healingId: '3',
    details: {
      samples: ['user@example']
    }
  },
  {
    issueId: '4',
    dataset: 'sales_metrics',
    table: 'daily_sales',
    column: 'product_id',
    ruleId: '4',
    validationId: '4-1',
    description: 'Reference integrity violation for product_id',
    dimension: 'CONSISTENCY',
    severity: AlertSeverity.HIGH,
    status: 'OPEN',
    affectedRows: 12,
    detectedAt: generateTimestamp(-2),
    healingStatus: HealingStatus.PENDING,
    healingId: null,
    details: {
      samples: ['PRD-9999']
    }
  },
  {
    issueId: '5',
    dataset: 'inventory',
    table: 'stock_levels',
    column: 'quantity',
    ruleId: '5',
    validationId: '5-1',
    description: 'Negative quantity values',
    dimension: 'VALIDITY',
    severity: AlertSeverity.CRITICAL,
    status: 'OPEN',
    affectedRows: 1,
    detectedAt: generateTimestamp(0),
    healingStatus: HealingStatus.APPROVAL_REQUIRED,
    healingId: null,
    details: {
      samples: ['-5']
    }
  }
];

/**
 * Mock healing issues
 */
export const mockHealingIssues: HealingIssue[] = [
  {
    issueId: '1',
    executionId: '2',
    pipelineId: '2',
    pipelineName: 'Sales Metrics Pipeline',
    issueType: 'DATA_QUALITY',
    component: 'Validation',
    severity: AlertSeverity.HIGH,
    description: 'Null values in sales_amount field',
    detectedAt: generateTimestamp(-1),
    details: {
      dataset: 'sales_metrics',
      table: 'daily_sales',
      column: 'sales_amount',
      affectedRows: 15,
      samples: ['row_id_123', 'row_id_456']
    },
    status: HealingStatus.IN_PROGRESS,
    healingId: '1',
    suggestedActions: [
      {
        actionId: '1',
        patternId: '1',
        name: 'Impute Missing Values',
        actionType: 'DATA_CORRECTION',
        description: 'Fill missing values with average from historical data',
        actionDefinition: {
          method: 'imputation',
          strategy: 'avg',
          lookbackDays: 30
        },
        isActive: true,
        successRate: 95,
        metadata: null,
        createdAt: generateTimestamp(-30),
        updatedAt: generateTimestamp(-10)
      }
    ],
    confidence: 92
  },
  {
    issueId: '2',
    executionId: '2',
    pipelineId: '2',
    pipelineName: 'Sales Metrics Pipeline',
    issueType: 'DATA_QUALITY',
    component: 'Validation',
    severity: AlertSeverity.MEDIUM,
    description: 'Outliers in discount_percent',
    detectedAt: generateTimestamp(-1),
    details: {
      dataset: 'sales_metrics',
      table: 'daily_sales',
      column: 'discount_percent',
      affectedRows: 3,
      samples: ['row_id_789'],
      values: [120, 150, 200]
    },
    status: HealingStatus.IN_PROGRESS,
    healingId: '2',
    suggestedActions: [
      {
        actionId: '2',
        patternId: '2',
        name: 'Fix Outliers',
        actionType: 'DATA_CORRECTION',
        description: 'Cap outlier values to valid maximum',
        actionDefinition: {
          method: 'cap',
          maxValue: 100
        },
        isActive: true,
        successRate: 90,
        metadata: null,
        createdAt: generateTimestamp(-30),
        updatedAt: generateTimestamp(-10)
      }
    ],
    confidence: 85
  },
  {
    issueId: '3',
    executionId: '3',
    pipelineId: '3',
    pipelineName: 'Product Catalog Pipeline',
    issueType: 'SYSTEM_FAILURE',
    component: 'Extraction',
    severity: AlertSeverity.HIGH,
    description: 'API connection timeout after 3 retry attempts',
    detectedAt: generateTimestamp(-1),
    details: {
      endpoint: 'https://api.example.com/products',
      method: 'GET',
      attemptCount: 3,
      errorCode: 'ECONNABORTED'
    },
    status: HealingStatus.FAILED,
    healingId: '3',
    suggestedActions: [
      {
        actionId: '3',
        patternId: '3',
        name: 'API Retry with Backoff',
        actionType: 'RETRY',
        description: 'Retry API connection with exponential backoff',
        actionDefinition: {
          strategy: 'exponential-backoff',
          initialDelayMs: 1000,
          maxDelayMs: 60000,
          maxAttempts: 5
        },
        isActive: true,
        successRate: 80,
        metadata: null,
        createdAt: generateTimestamp(-30),
        updatedAt: generateTimestamp(-10)
      }
    ],
    confidence: 75
  },
  {
    issueId: '4',
    executionId: '2',
    pipelineId: '2',
    pipelineName: 'Sales Metrics Pipeline',
    issueType: 'DATA_QUALITY',
    component: 'Validation',
    severity: AlertSeverity.HIGH,
    description: 'Reference integrity violation for product_id',
    detectedAt: generateTimestamp(-2),
    details: {
      dataset: 'sales_metrics',
      table: 'daily_sales',
      column: 'product_id',
      affectedRows: 12,
      samples: ['PRD-9999']
    },
    status: HealingStatus.PENDING,
    healingId: null,
    suggestedActions: [
      {
        actionId: '4',
        patternId: '4',
        name: 'Reference Lookup',
        actionType: 'DATA_CORRECTION',
        description: 'Replace invalid references with closest valid match',
        actionDefinition: {
          method: 'fuzzy-match',
          referenceTable: 'product_catalog.products',
          referenceColumn: 'product_id',
          threshold: 0.8
        },
        isActive: true,
        successRate: 85,
        metadata: null,
        createdAt: generateTimestamp(-30),
        updatedAt: generateTimestamp(-10)
      }
    ],
    confidence: 70
  },
  {
    issueId: '5',
    executionId: '4',
    pipelineId: '4',
    pipelineName: 'Inventory Sync Pipeline',
    issueType: 'DATA_QUALITY',
    component: 'Validation',
    severity: AlertSeverity.CRITICAL,
    description: 'Negative quantity values',
    detectedAt: generateTimestamp(0),
    details: {
      dataset: 'inventory',
      table: 'stock_levels',
      column: 'quantity',
      affectedRows: 1,
      samples: ['-5']
    },
    status: HealingStatus.APPROVAL_REQUIRED,
    healingId: null,
    suggestedActions: [
      {
        actionId: '5',
        patternId: '5',
        name: 'Fix Negative Values',
        actionType: 'DATA_CORRECTION',
        description: 'Replace negative values with 0',
        actionDefinition: {
          method: 'replace',
          condition: 'value < 0',
          replacement: 0
        },
        isActive: true,
        successRate: 100,
        metadata: null,
        createdAt: generateTimestamp(-30),
        updatedAt: generateTimestamp(-10)
      }
    ],
    confidence: 95
  }
];

/**
 * Mock healing patterns
 */
export const mockHealingPatterns: IssuePattern[] = [
  {
    patternId: '1',
    issueType: 'DATA_QUALITY',
    detectionPattern: {
      ruleType: 'NULL_CHECK',
      dataType: 'numeric'
    },
    confidenceThreshold: 80,
    description: 'Pattern for detecting null values in numeric columns',
    metadata: {
      category: 'completeness'
    },
    createdAt: generateTimestamp(-60),
    updatedAt: generateTimestamp(-10)
  },
  {
    patternId: '2',
    issueType: 'DATA_QUALITY',
    detectionPattern: {
      ruleType: 'VALUE_RANGE',
      dataType: 'numeric',
      condition: 'outlier'
    },
    confidenceThreshold: 75,
    description: 'Pattern for detecting outliers in numeric columns',
    metadata: {
      category: 'accuracy'
    },
    createdAt: generateTimestamp(-55),
    updatedAt: generateTimestamp(-10)
  },
  {
    patternId: '3',
    issueType: 'SYSTEM_FAILURE',
    detectionPattern: {
      component: 'Extraction',
      errorType: 'connection',
      errorPattern: '.*timeout.*'
    },
    confidenceThreshold: 70,
    description: 'Pattern for detecting API connection timeout issues',
    metadata: {
      category: 'connectivity'
    },
    createdAt: generateTimestamp(-50),
    updatedAt: generateTimestamp(-10)
  },
  {
    patternId: '4',
    issueType: 'DATA_QUALITY',
    detectionPattern: {
      ruleType: 'REFERENTIAL',
      dataType: 'string'
    },
    confidenceThreshold: 70,
    description: 'Pattern for detecting reference integrity violations',
    metadata: {
      category: 'consistency'
    },
    createdAt: generateTimestamp(-45),
    updatedAt: generateTimestamp(-10)
  },
  {
    patternId: '5',
    issueType: 'DATA_QUALITY',
    detectionPattern: {
      ruleType: 'VALUE_RANGE',
      dataType: 'numeric',
      condition: 'value < 0'
    },
    confidenceThreshold: 90,
    description: 'Pattern for detecting negative values in non-negative fields',
    metadata: {
      category: 'validity'
    },
    createdAt: generateTimestamp(-40),
    updatedAt: generateTimestamp(-10)
  }
];

/**
 * Mock healing actions
 */
export const mockHealingActions: HealingAction[] = [
  {
    actionId: '1',
    patternId: '1',
    actionType: 'DATA_CORRECTION',
    actionDefinition: {
      method: 'imputation',
      strategy: 'avg',
      lookbackDays: 30
    },
    isActive: true,
    successRate: 95,
    metadata: {
      category: 'completeness'
    },
    createdAt: generateTimestamp(-60),
    updatedAt: generateTimestamp(-10)
  },
  {
    actionId: '2',
    patternId: '2',
    actionType: 'DATA_CORRECTION',
    actionDefinition: {
      method: 'cap',
      maxValue: 100
    },
    isActive: true,
    successRate: 90,
    metadata: {
      category: 'accuracy'
    },
    createdAt: generateTimestamp(-55),
    updatedAt: generateTimestamp(-10)
  },
  {
    actionId: '3',
    patternId: '3',
    actionType: 'RETRY',
    actionDefinition: {
      strategy: 'exponential-backoff',
      initialDelayMs: 1000,
      maxDelayMs: 60000,
      maxAttempts: 5
    },
    isActive: true,
    successRate: 80,
    metadata: {
      category: 'connectivity'
    },
    createdAt: generateTimestamp(-50),
    updatedAt: generateTimestamp(-10)
  },
  {
    actionId: '4',
    patternId: '4',
    actionType: 'DATA_CORRECTION',
    actionDefinition: {
      method: 'fuzzy-match',
      referenceTable: 'product_catalog.products',
      referenceColumn: 'product_id',
      threshold: 0.8
    },
    isActive: true,
    successRate: 85,
    metadata: {
      category: 'consistency'
    },
    createdAt: generateTimestamp(-45),
    updatedAt: generateTimestamp(-10)
  },
  {
    actionId: '5',
    patternId: '5',
    actionType: 'DATA_CORRECTION',
    actionDefinition: {
      method: 'replace',
      condition: 'value < 0',
      replacement: 0
    },
    isActive: true,
    successRate: 100,
    metadata: {
      category: 'validity'
    },
    createdAt: generateTimestamp(-40),
    updatedAt: generateTimestamp(-10)
  }
];

/**
 * Mock healing executions
 */
export const mockHealingExecutions: HealingExecution[] = [
  {
    healingId: '1',
    executionId: '2',
    validationId: '3-1',
    patternId: '1',
    patternName: 'Null Value Detection',
    actionId: '1',
    actionName: 'Impute Missing Values',
    executionTime: generateTimestamp(-1),
    status: HealingStatus.IN_PROGRESS,
    confidence: 92,
    successful: null,
    executionDetails: {
      affectedRows: 15,
      startTime: generateTimestamp(-1),
      estimatedEndTime: generateTimestamp(0)
    },
    errorMessage: null,
    duration: 120
  },
  {
    healingId: '2',
    executionId: '2',
    validationId: '3-2',
    patternId: '2',
    patternName: 'Outlier Detection',
    actionId: '2',
    actionName: 'Fix Outliers',
    executionTime: generateTimestamp(-1),
    status: HealingStatus.IN_PROGRESS,
    confidence: 85,
    successful: null,
    executionDetails: {
      affectedRows: 3,
      startTime: generateTimestamp(-1),
      estimatedEndTime: generateTimestamp(0)
    },
    errorMessage: null,
    duration: 90
  },
  {
    healingId: '3',
    executionId: '3',
    validationId: null,
    patternId: '3',
    patternName: 'API Failure Detection',
    actionId: '3',
    actionName: 'API Retry with Backoff',
    executionTime: generateTimestamp(-1),
    status: HealingStatus.FAILED,
    confidence: 75,
    successful: false,
    executionDetails: {
      attempts: 5,
      startTime: generateTimestamp(-1),
      endTime: generateTimestamp(-1)
    },
    errorMessage: 'Maximum retry attempts exceeded',
    duration: 300
  },
  {
    healingId: '4',
    executionId: '1',
    validationId: '2-1',
    patternId: '5',
    patternName: 'Email Format Fixer',
    actionId: '6',
    actionName: 'Fix Email Format',
    executionTime: generateTimestamp(-2),
    status: HealingStatus.COMPLETED,
    confidence: 95,
    successful: true,
    executionDetails: {
      affectedRows: 5,
      startTime: generateTimestamp(-2),
      endTime: generateTimestamp(-2)
    },
    errorMessage: null,
    duration: 45
  },
  {
    healingId: '5',
    executionId: '4',
    validationId: '5-1',
    patternId: '5',
    patternName: 'Negative Value Detection',
    actionId: '5',
    actionName: 'Fix Negative Values',
    executionTime: generateTimestamp(0),
    status: HealingStatus.APPROVAL_REQUIRED,
    confidence: 95,
    successful: null,
    executionDetails: {
      affectedRows: 1,
      approvalNeeded: true,
      approvalReason: 'Critical table modification'
    },
    errorMessage: null,
    duration: null
  }
];

/**
 * Mock AI models
 */
export const mockAIModels: AIModel[] = [
  {
    modelId: '1',
    name: 'Anomaly Detection Model',
    description: 'Detects anomalies in data patterns',
    modelType: 'DETECTION',
    version: 'v2.4',
    status: 'ACTIVE',
    accuracy: 94.2,
    lastTrainingDate: generateTimestamp(-3),
    trainingDataSize: 1200000,
    modelSize: '45 MB',
    averageInferenceTime: 18,
    metadata: {
      architecture: 'Deep learning neural network with embedding layers',
      implementationDetails: 'TensorFlow 2.8.0'
    },
    createdAt: generateTimestamp(-90),
    updatedAt: generateTimestamp(-3)
  },
  {
    modelId: '2',
    name: 'Value Imputation Model',
    description: 'Predicts missing values based on historical patterns',
    modelType: 'IMPUTATION',
    version: 'v1.8',
    status: 'ACTIVE',
    accuracy: 91.5,
    lastTrainingDate: generateTimestamp(-5),
    trainingDataSize: 850000,
    modelSize: '32 MB',
    averageInferenceTime: 15,
    metadata: {
      architecture: 'Gradient boosting machine',
      implementationDetails: 'XGBoost 1.6.1'
    },
    createdAt: generateTimestamp(-80),
    updatedAt: generateTimestamp(-5)
  },
  {
    modelId: '3',
    name: 'Data Correction Model',
    description: 'Automatically corrects common data issues',
    modelType: 'CORRECTION',
    version: 'v2.1',
    status: 'ACTIVE',
    accuracy: 89.8,
    lastTrainingDate: generateTimestamp(-7),
    trainingDataSize: 1500000,
    modelSize: '50 MB',
    averageInferenceTime: 25,
    metadata: {
      architecture: 'Ensemble of specialized correction models',
      implementationDetails: 'Custom TensorFlow models'
    },
    createdAt: generateTimestamp(-70),
    updatedAt: generateTimestamp(-7)
  },
  {
    modelId: '4',
    name: 'Failure Prediction Model',
    description: 'Predicts pipeline failures before they occur',
    modelType: 'PREDICTION',
    version: 'v1.2',
    status: 'TRAINING',
    accuracy: 85.9,
    lastTrainingDate: generateTimestamp(-1),
    trainingDataSize: 500000,
    modelSize: '28 MB',
    averageInferenceTime: 20,
    metadata: {
      architecture: 'Recurrent neural network with LSTM layers',
      implementationDetails: 'TensorFlow 2.8.0'
    },
    createdAt: generateTimestamp(-60),
    updatedAt: generateTimestamp(-1)
  },
  {
    modelId: '5',
    name: 'Schema Evolution Model',
    description: 'Manages schema changes and compatibility',
    modelType: 'SCHEMA',
    version: 'v0.9',
    status: 'INACTIVE',
    accuracy: 82.5,
    lastTrainingDate: generateTimestamp(-20),
    trainingDataSize: 300000,
    modelSize: '15 MB',
    averageInferenceTime: 12,
    metadata: {
      architecture: 'Graph neural network',
      implementationDetails: 'PyTorch 1.11.0'
    },
    createdAt: generateTimestamp(-50),
    updatedAt: generateTimestamp(-20)
  }
];

/**
 * Mock healing settings
 */
export const mockHealingSettings: HealingSettings = {
  healingMode: 'SEMI_AUTOMATIC',
  globalConfidenceThreshold: 85,
  maxRetryAttempts: 3,
  approvalRequiredHighImpact: true,
  learningModeActive: true,
  additionalSettings: {
    alertThreshold: 80,
    maximumCorrectionPerRun: 100,
    preserveOriginalData: true
  },
  updatedAt: generateTimestamp(-5),
  updatedBy: 'admin@example.com'
};

/**
 * Mock alerts
 */
export const mockAlerts: Alert[] = [
  {
    alertId: '1',
    executionId: '3',
    alertType: 'PIPELINE_FAILURE',
    severity: AlertSeverity.HIGH,
    message: 'BigQuery load failed',
    details: {
      component: 'Load to BigQuery',
      errorCode: 'QUOTA_EXCEEDED',
      errorMessage: 'Quota exceeded for project'
    },
    createdAt: generateTimestamp(0),
    acknowledged: false,
    acknowledgedBy: null,
    acknowledgedAt: null,
    relatedAlerts: []
  },
  {
    alertId: '2',
    executionId: '2',
    alertType: 'DATA_QUALITY',
    severity: AlertSeverity.MEDIUM,
    message: 'Schema drift detected',
    details: {
      component: 'Data Validation',
      dataset: 'sales_metrics',
      table: 'daily_sales',
      changes: [
        { column: 'new_field', action: 'added' }
      ]
    },
    createdAt: generateTimestamp(0),
    acknowledged: false,
    acknowledgedBy: null,
    acknowledgedAt: null,
    relatedAlerts: []
  },
  {
    alertId: '3',
    executionId: '2',
    alertType: 'SYSTEM_PERFORMANCE',
    severity: AlertSeverity.MEDIUM,
    message: 'API response slowdown',
    details: {
      component: 'External API Connector',
      endpoint: 'https://api.example.com/products',
      responseTime: '1.2s',
      baseline: '0.8s'
    },
    createdAt: generateTimestamp(0),
    acknowledged: false,
    acknowledgedBy: null,
    acknowledgedAt: null,
    relatedAlerts: []
  },
  {
    alertId: '4',
    executionId: null,
    alertType: 'RESOURCE_UTILIZATION',
    severity: AlertSeverity.LOW,
    message: 'Storage utilization high',
    details: {
      component: 'Cloud Storage',
      bucket: 'data-pipeline-staging',
      utilization: '85%',
      threshold: '80%'
    },
    createdAt: generateTimestamp(-1),
    acknowledged: true,
    acknowledgedBy: 'admin@example.com',
    acknowledgedAt: generateTimestamp(-1),
    relatedAlerts: []
  },
  {
    alertId: '5',
    executionId: null,
    alertType: 'PERFORMANCE_DEGRADATION',
    severity: AlertSeverity.LOW,
    message: 'Query performance degrading',
    details: {
      component: 'BigQuery',
      dataset: 'sales_metrics',
      table: 'daily_sales',
      queryId: 'bq-2023-06-15-1234',
      executionTime: '45s',
      baseline: '30s'
    },
    createdAt: generateTimestamp(-2),
    acknowledged: false,
    acknowledgedBy: null,
    acknowledgedAt: null,
    relatedAlerts: []
  }
];

/**
 * Mock alert summary
 */
export const mockAlertSummary: Record<AlertSeverity, number> = {
  CRITICAL: 0,
  HIGH: 1,
  MEDIUM: 2,
  LOW: 2
};

/**
 * Mock alert statistics
 */
export const mockAlertStatistics = {
  total: 5,
  acknowledged: 1,
  unacknowledged: 4,
  byType: {
    PIPELINE_FAILURE: 1,
    DATA_QUALITY: 1,
    SYSTEM_PERFORMANCE: 1,
    RESOURCE_UTILIZATION: 1,
    PERFORMANCE_DEGRADATION: 1
  },
  bySeverity: {
    CRITICAL: 0,
    HIGH: 1,
    MEDIUM: 2,
    LOW: 2
  }
};

/**
 * Mock alert trend data
 */
export const mockAlertTrend = {
  timeLabels: [
    '00:00', '06:00', '12:00', '18:00', 'Now'
  ],
  values: [
    4, 8, 6, 3, 5
  ]
};

/**
 * Mock pipeline DAG structure for visualization
 */
export const mockPipelineDAG = {
  nodes: [
    { id: 'source', type: 'source', label: 'GCS', position: { x: 50, y: 200 } },
    { id: 'extract', type: 'task', label: 'Extract', position: { x: 200, y: 200 } },
    { id: 'transform', type: 'task', label: 'Transform', position: { x: 350, y: 150 } },
    { id: 'quality', type: 'task', label: 'Quality Check', position: { x: 350, y: 250 } },
    { id: 'load', type: 'task', label: 'Load', position: { x: 500, y: 200 } }
  ],
  edges: [
    { id: 'e1', source: 'source', target: 'extract' },
    { id: 'e2', source: 'extract', target: 'transform' },
    { id: 'e3', source: 'extract', target: 'quality' },
    { id: 'e4', source: 'transform', target: 'load' },
    { id: 'e5', source: 'quality', target: 'load' }
  ],
  nodeStates: {
    'source': 'HEALTHY',
    'extract': 'HEALTHY',
    'transform': 'HEALTHY',
    'quality': 'WARNING',
    'load': 'HEALTHY'
  }
};

/**
 * Mock healing dashboard data
 */
export const mockHealingDashboardData: HealingDashboardData = {
  totalIssuesDetected: 250,
  issuesResolvedAutomatically: 203,
  activeIssues: 12,
  overallSuccessRate: 87,
  averageResolutionTime: 45,
  issuesByType: {
    DATA_QUALITY: 180,
    SYSTEM_FAILURE: 35,
    PERFORMANCE: 25,
    DATA_FORMAT: 10
  },
  successRateByType: {
    DATA_QUALITY: 90,
    SYSTEM_FAILURE: 75,
    PERFORMANCE: 80,
    DATA_FORMAT: 95
  },
  resolutionTimeByType: {
    DATA_QUALITY: 35,
    SYSTEM_FAILURE: 60,
    PERFORMANCE: 50,
    DATA_FORMAT: 20
  },
  issuesTrend: [
    { date: '2023-06-09', count: 12 },
    { date: '2023-06-10', count: 15 },
    { date: '2023-06-11', count: 8 },
    { date: '2023-06-12', count: 10 },
    { date: '2023-06-13', count: 14 },
    { date: '2023-06-14', count: 9 },
    { date: '2023-06-15', count: 12 }
  ],
  successRateTrend: [
    { date: '2023-06-09', rate: 85 },
    { date: '2023-06-10', rate: 82 },
    { date: '2023-06-11', rate: 88 },
    { date: '2023-06-12', rate: 90 },
    { date: '2023-06-13', rate: 87 },
    { date: '2023-06-14', rate: 89 },
    { date: '2023-06-15', rate: 87 }
  ],
  modelPerformance: {
    'Anomaly Detection Model': 94.2,
    'Value Imputation Model': 91.5,
    'Data Correction Model': 89.8,
    'Failure Prediction Model': 85.9
  },
  recentActivities: [
    {
      activityId: '1',
      timestamp: generateTimestamp(0),
      activityType: 'ISSUE_DETECTED',
      description: 'Detected null values in sales_amount',
      executionId: '2',
      healingId: null,
      modelId: '1',
      userId: null,
      details: {
        dataset: 'sales_metrics',
        table: 'daily_sales',
        confidence: 92
      }
    },
    {
      activityId: '2',
      timestamp: generateTimestamp(0),
      activityType: 'HEALING_STARTED',
      description: 'Started healing process for null values',
      executionId: '2',
      healingId: '1',
      modelId: '2',
      userId: null,
      details: {
        strategy: 'imputation',
        confidence: 91
      }
    },
    {
      activityId: '3',
      timestamp: generateTimestamp(-1),
      activityType: 'HEALING_COMPLETED',
      description: 'Successfully fixed email format issues',
      executionId: '1',
      healingId: '4',
      modelId: '3',
      userId: null,
      details: {
        affectedRows: 5,
        duration: 45
      }
    }
  ]
};

/**
 * Mock quality statistics
 */
export const mockQualityStatistics: QualityStatistics = {
  totalDatasets: 5,
  totalTables: 15,
  totalRules: 245,
  activeRules: 231,
  validationsLast24h: 125,
  validationsLast7d: 820,
  validationsLast30d: 3250,
  successRateLast24h: 94.5,
  successRateLast7d: 95.2,
  successRateLast30d: 94.8,
  openIssues: 24,
  issuesByDimension: {
    COMPLETENESS: 8,
    ACCURACY: 5,
    CONSISTENCY: 6,
    TIMELINESS: 1,
    VALIDITY: 4
  },
  issuesBySeverity: {
    CRITICAL: 3,
    HIGH: 7,
    MEDIUM: 10,
    LOW: 4
  },
  selfHealingSuccessRate: 87.5
};

/**
 * Mock dashboard data for the main dashboard
 */
export const mockDashboardData: DashboardData = {
  pipelineHealth: {
    healthyPercentage: 98,
    totalPipelines: 12,
    healthyCount: 10,
    warningCount: 1,
    errorCount: 0,
    inactiveCount: 1
  },
  dataQuality: {
    passPercentage: 94,
    totalRules: 245,
    passingRules: 231,
    failingRules: 14,
    warningRules: 22
  },
  selfHealing: {
    autoFixPercentage: 87,
    totalIssues: 250,
    autoFixedCount: 203,
    manualFixCount: 30,
    pendingCount: 17
  },
  activeAlerts: [
    {
      id: '1',
      severity: AlertSeverity.HIGH,
      description: 'BigQuery load failed',
      timestamp: generateTimestamp(0),
      pipeline: 'customer_data',
      selfHealingStatus: 'IN_PROGRESS'
    },
    {
      id: '2',
      severity: AlertSeverity.MEDIUM,
      description: 'Schema drift detected',
      timestamp: generateTimestamp(0),
      pipeline: 'sales_metrics',
      selfHealingStatus: 'COMPLETED'
    },
    {
      id: '3',
      severity: AlertSeverity.MEDIUM,
      description: 'API response slowdown',
      timestamp: generateTimestamp(0),
      pipeline: 'product_catalog',
      selfHealingStatus: 'PENDING'
    }
  ],
  systemStatus: {
    gcsConnector: 'OK',
    cloudSql: 'OK',
    externalApis: 'WARN',
    bigQuery: 'OK',
    mlServices: 'OK'
  },
  quickStats: {
    activePipelines: 12,
    pendingJobs: 3,
    alertRateChange: -15,
    alertRatePeriod: '7d'
  },
  recentExecutions: [
    {
      id: '1',
      pipelineName: 'analytics_daily',
      status: 'HEALTHY',
      startTime: generateTimestamp(0),
      endTime: generateTimestamp(0),
      duration: 1800,
      hasWarning: false
    },
    {
      id: '2',
      pipelineName: 'customer_load',
      status: 'HEALTHY',
      startTime: generateTimestamp(-1),
      endTime: generateTimestamp(-1),
      duration: 1200,
      hasWarning: false
    },
    {
      id: '3',
      pipelineName: 'product_enrich',
      status: 'ERROR',
      startTime: generateTimestamp(-1),
      endTime: generateTimestamp(-1),
      duration: 900,
      hasWarning: true
    },
    {
      id: '4',
      pipelineName: 'inventory_sync',
      status: 'HEALTHY',
      startTime: generateTimestamp(-1),
      endTime: generateTimestamp(-1),
      duration: 600,
      hasWarning: false
    }
  ],
  aiInsights: [
    {
      id: '1',
      description: 'Predicted slowdown in sales_metrics pipeline based on historical patterns',
      timestamp: generateTimestamp(0),
      confidence: 85,
      relatedEntity: 'sales_metrics'
    },
    {
      id: '2',
      description: 'Recurring nulls in customer_address field detected, suggest schema constraint',
      timestamp: generateTimestamp(-1),
      confidence: 92,
      relatedEntity: 'customer_data'
    },
    {
      id: '3',
      description: 'Query optimization available for orders table, potential 40% performance improvement',
      timestamp: generateTimestamp(-2),
      confidence: 90,
      relatedEntity: 'order_processing'
    }
  ]
};

export {
  generateId,
  generateTimestamp,
  generateMockListResponse,
  generateMockDataResponse,
  mockPipelineDefinitions,
  mockPipelineExecutions,
  mockTaskExecutions,
  mockQualityRules,
  mockDatasetQualitySummaries,
  mockQualityIssues,
  mockHealingIssues,
  mockHealingPatterns,
  mockHealingActions,
  mockHealingExecutions,
  mockAIModels,
  mockHealingSettings,
  mockAlerts,
  mockAlertSummary,
  mockAlertStatistics,
  mockAlertTrend,
  mockPipelineDAG,
  mockHealingDashboardData,
  mockQualityStatistics,
  mockDashboardData
};