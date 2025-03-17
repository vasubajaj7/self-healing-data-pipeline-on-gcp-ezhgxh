/**
 * Provides reusable API mocking utilities and fixtures for testing the web frontend 
 * of the self-healing data pipeline. This module contains mock implementations of 
 * API services, response generators, and helper functions to facilitate consistent 
 * API mocking across tests.
 */

import { jest } from '@jest/globals'; // @jest/globals ^29.5.0
import apiClient from '../../web/src/services/api/apiClient';
import { 
  DataResponse, 
  ListResponse, 
  PaginationParams 
} from '../../web/src/types/api';
import { 
  PipelineDefinition, 
  PipelineExecution, 
  TaskExecution, 
  PipelineStatus, 
  PipelineHealthMetrics 
} from '../../web/src/types/api';
import { 
  QualityRule, 
  QualityValidation, 
  QualityScore, 
  ValidationStatus, 
  DatasetQualitySummary, 
  QualityIssue 
} from '../../web/src/types/quality';
import { 
  HealingPattern, 
  HealingAction, 
  HealingExecution, 
  HealingIssue, 
  HealingSettings, 
  AIModel, 
  ModelHealth 
} from '../../web/src/types/selfHealing';
import { 
  Alert, 
  NotificationConfig 
} from '../../web/src/types/alerts';
import { 
  User, 
  LoginResponse 
} from '../../web/src/types/user';

/**
 * Interface for mock API client
 */
export interface MockApiClient {
  get: jest.Mock;
  post: jest.Mock;
  put: jest.Mock;
  delete: jest.Mock;
  reset: () => void;
  getCallHistory: () => object;
}

/**
 * Options for creating mock services
 */
export interface MockServiceOptions {
  mockResponses?: Record<string, any>;
  mockErrors?: Record<string, Error>;
  delay?: number;
}

// Mock data for pipeline testing
export const MOCK_PIPELINE_DATA = {
  definitions: [
    {
      pipelineId: 'pipeline-1',
      pipelineName: 'Analytics Daily',
      sourceId: 'source-1',
      sourceName: 'Sales DB',
      targetDataset: 'analytics',
      targetTable: 'daily_sales',
      configuration: { schedule: 'daily', timeout: 3600 },
      description: 'Daily sales analytics pipeline',
      isActive: true,
      metadata: { owner: 'data-team', priority: 'high' },
      createdAt: '2023-01-15T08:00:00Z',
      updatedAt: '2023-06-01T10:30:00Z',
      lastExecutionStatus: 'HEALTHY',
      lastExecutionTime: '2023-06-15T05:30:00Z'
    },
    {
      pipelineId: 'pipeline-2',
      pipelineName: 'Customer Data',
      sourceId: 'source-2',
      sourceName: 'Customer DB',
      targetDataset: 'customer',
      targetTable: 'customer_full',
      configuration: { schedule: 'hourly', timeout: 1800 },
      description: 'Customer data integration pipeline',
      isActive: true,
      metadata: { owner: 'customer-team', priority: 'critical' },
      createdAt: '2023-02-10T14:00:00Z',
      updatedAt: '2023-06-10T16:45:00Z',
      lastExecutionStatus: 'WARNING',
      lastExecutionTime: '2023-06-15T04:15:00Z'
    },
    {
      pipelineId: 'pipeline-3',
      pipelineName: 'Product Enrich',
      sourceId: 'source-3',
      sourceName: 'Product API',
      targetDataset: 'product',
      targetTable: 'product_enriched',
      configuration: { schedule: 'daily', timeout: 2400 },
      description: 'Product data enrichment pipeline',
      isActive: true,
      metadata: { owner: 'product-team', priority: 'medium' },
      createdAt: '2023-03-05T09:30:00Z',
      updatedAt: '2023-06-05T11:20:00Z',
      lastExecutionStatus: 'ERROR',
      lastExecutionTime: '2023-06-15T03:45:00Z'
    }
  ] as PipelineDefinition[],
  
  executions: [
    {
      executionId: 'exec-1',
      pipelineId: 'pipeline-1',
      pipelineName: 'Analytics Daily',
      startTime: '2023-06-15T05:30:00Z',
      endTime: '2023-06-15T05:45:00Z',
      status: 'HEALTHY',
      recordsProcessed: 15420,
      dagRunId: 'run-1234',
      tasks: [
        {
          taskExecutionId: 'task-1',
          executionId: 'exec-1',
          taskId: 'extract',
          taskType: 'DataExtraction',
          startTime: '2023-06-15T05:30:00Z',
          endTime: '2023-06-15T05:35:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { source: 'sales_db' }
        },
        {
          taskExecutionId: 'task-2',
          executionId: 'exec-1',
          taskId: 'transform',
          taskType: 'DataTransformation',
          startTime: '2023-06-15T05:35:00Z',
          endTime: '2023-06-15T05:40:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { transformation: 'standard' }
        },
        {
          taskExecutionId: 'task-3',
          executionId: 'exec-1',
          taskId: 'load',
          taskType: 'DataLoad',
          startTime: '2023-06-15T05:40:00Z',
          endTime: '2023-06-15T05:45:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { destination: 'daily_sales' }
        }
      ]
    },
    {
      executionId: 'exec-2',
      pipelineId: 'pipeline-2',
      pipelineName: 'Customer Data',
      startTime: '2023-06-15T04:15:00Z',
      endTime: '2023-06-15T04:30:00Z',
      status: 'WARNING',
      recordsProcessed: 8750,
      dagRunId: 'run-1235',
      tasks: [
        {
          taskExecutionId: 'task-4',
          executionId: 'exec-2',
          taskId: 'extract',
          taskType: 'DataExtraction',
          startTime: '2023-06-15T04:15:00Z',
          endTime: '2023-06-15T04:20:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { source: 'customer_db' }
        },
        {
          taskExecutionId: 'task-5',
          executionId: 'exec-2',
          taskId: 'validate',
          taskType: 'DataValidation',
          startTime: '2023-06-15T04:20:00Z',
          endTime: '2023-06-15T04:25:00Z',
          status: 'WARNING',
          errorDetails: 'Found 15 records with validation issues',
          retryCount: 0,
          taskParams: { rules: 'customer_validation' }
        },
        {
          taskExecutionId: 'task-6',
          executionId: 'exec-2',
          taskId: 'load',
          taskType: 'DataLoad',
          startTime: '2023-06-15T04:25:00Z',
          endTime: '2023-06-15T04:30:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { destination: 'customer_full' }
        }
      ]
    },
    {
      executionId: 'exec-3',
      pipelineId: 'pipeline-3',
      pipelineName: 'Product Enrich',
      startTime: '2023-06-15T03:45:00Z',
      endTime: null,
      status: 'ERROR',
      recordsProcessed: 3200,
      errorDetails: 'API connection timeout after 3 retries',
      dagRunId: 'run-1236',
      tasks: [
        {
          taskExecutionId: 'task-7',
          executionId: 'exec-3',
          taskId: 'extract',
          taskType: 'DataExtraction',
          startTime: '2023-06-15T03:45:00Z',
          endTime: '2023-06-15T03:50:00Z',
          status: 'HEALTHY',
          retryCount: 0,
          taskParams: { source: 'product_db' }
        },
        {
          taskExecutionId: 'task-8',
          executionId: 'exec-3',
          taskId: 'enrich',
          taskType: 'APIEnrichment',
          startTime: '2023-06-15T03:50:00Z',
          endTime: '2023-06-15T03:55:00Z',
          status: 'ERROR',
          errorDetails: 'API connection timeout after 3 retries',
          retryCount: 3,
          taskParams: { api: 'product_details_api' }
        }
      ]
    }
  ] as PipelineExecution[],
  
  metrics: {
    overallHealth: 75,
    pipelineStatusBreakdown: {
      healthy: 50,
      warning: 30,
      error: 20,
      inactive: 0
    },
    avgExecutionTime: 840,
    successRate: 92.5,
    dataProcessed: 1250000,
    lastUpdated: '2023-06-15T06:00:00Z'
  }
};

// Mock data for quality testing
export const MOCK_QUALITY_DATA = {
  rules: [
    {
      ruleId: 'rule-1',
      ruleName: 'Required Fields Check',
      targetDataset: 'customer',
      targetTable: 'customer_data',
      ruleType: 'NULL_CHECK',
      expectationType: 'not_null',
      ruleDefinition: { columns: ['customer_id', 'email', 'name'] },
      severity: 'HIGH',
      isActive: true,
      description: 'Validate required customer fields are not null',
      createdAt: '2023-01-20T10:00:00Z',
      updatedAt: '2023-05-15T14:30:00Z'
    },
    {
      ruleId: 'rule-2',
      ruleName: 'Email Format Validation',
      targetDataset: 'customer',
      targetTable: 'customer_data',
      ruleType: 'PATTERN_MATCH',
      expectationType: 'match_regex',
      ruleDefinition: { column: 'email', pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' },
      severity: 'MEDIUM',
      isActive: true,
      description: 'Validate email addresses have correct format',
      createdAt: '2023-01-21T11:30:00Z',
      updatedAt: '2023-05-16T09:45:00Z'
    },
    {
      ruleId: 'rule-3',
      ruleName: 'Product Price Range',
      targetDataset: 'product',
      targetTable: 'product_catalog',
      ruleType: 'VALUE_RANGE',
      expectationType: 'between',
      ruleDefinition: { column: 'price', min: 0, max: 10000 },
      severity: 'HIGH',
      isActive: true,
      description: 'Validate product prices are within valid range',
      createdAt: '2023-02-05T15:20:00Z',
      updatedAt: '2023-05-20T13:10:00Z'
    }
  ] as QualityRule[],
  
  validations: [
    {
      validationId: 'val-1',
      executionId: 'exec-1',
      ruleId: 'rule-1',
      validationTime: '2023-06-15T05:35:00Z',
      status: 'PASSED',
    },
    {
      validationId: 'val-2',
      executionId: 'exec-2',
      ruleId: 'rule-1',
      validationTime: '2023-06-15T04:20:00Z',
      status: 'WARNING',
      failedRecords: 15,
      details: { failures: ['Missing email for some customers'] },
      selfHealingStatus: 'IN_PROGRESS'
    },
    {
      validationId: 'val-3',
      executionId: 'exec-3',
      ruleId: 'rule-3',
      validationTime: '2023-06-15T03:50:00Z',
      status: 'FAILED',
      failedRecords: 42,
      details: { failures: ['Multiple products with negative prices'] },
      selfHealingStatus: 'FAILED'
    }
  ] as QualityValidation[],
  
  scores: {
    overallScore: 92.5,
    dimensionScores: {
      completeness: 95,
      accuracy: 90,
      consistency: 94,
      timeliness: 98
    },
    qualityMetrics: {
      nullRate: 0.03,
      duplicateRate: 0.005,
      outOfRangeRate: 0.015
    },
    calculationTime: '2023-06-15T06:00:00Z'
  } as QualityScore,
  
  dataSummaries: [
    {
      dataset: 'customer',
      tables: [
        { table: 'customer_data', qualityScore: 98 },
        { table: 'customer_addresses', qualityScore: 94 }
      ],
      overallScore: 96,
      trend: 'IMPROVING',
      issueCount: 3,
      lastUpdated: '2023-06-15T06:00:00Z'
    },
    {
      dataset: 'sales',
      tables: [
        { table: 'transactions', qualityScore: 97 },
        { table: 'order_items', qualityScore: 95 }
      ],
      overallScore: 96,
      trend: 'STABLE',
      issueCount: 2,
      lastUpdated: '2023-06-15T06:00:00Z'
    },
    {
      dataset: 'product',
      tables: [
        { table: 'product_catalog', qualityScore: 89 },
        { table: 'product_inventory', qualityScore: 92 }
      ],
      overallScore: 90,
      trend: 'DECLINING',
      issueCount: 15,
      lastUpdated: '2023-06-15T06:00:00Z'
    }
  ] as DatasetQualitySummary[],
  
  issues: [
    {
      issueId: 'issue-1',
      dataset: 'customer',
      table: 'customer_data',
      column: 'email',
      ruleId: 'rule-2',
      validationId: 'val-2',
      description: 'Invalid email format for 15 customer records',
      dimension: 'VALIDITY',
      severity: 'MEDIUM',
      status: 'OPEN',
      affectedRows: 15,
      detectedAt: '2023-06-15T04:20:00Z',
      healingStatus: 'IN_PROGRESS',
      healingId: 'healing-1'
    },
    {
      issueId: 'issue-2',
      dataset: 'product',
      table: 'product_catalog',
      column: 'price',
      ruleId: 'rule-3',
      validationId: 'val-3',
      description: 'Negative price values found in 42 products',
      dimension: 'VALIDITY',
      severity: 'HIGH',
      status: 'OPEN',
      affectedRows: 42,
      detectedAt: '2023-06-15T03:50:00Z',
      healingStatus: 'FAILED',
      healingId: 'healing-2'
    }
  ] as QualityIssue[]
};

// Mock data for self-healing testing
export const MOCK_HEALING_DATA = {
  patterns: [
    {
      patternId: 'pattern-1',
      issueType: 'DATA_QUALITY',
      name: 'Missing Email Pattern',
      description: 'Pattern for detecting and fixing missing email addresses',
      detectionPattern: { ruleName: 'Required Fields Check', column: 'email' },
      confidenceThreshold: 0.85,
      isActive: true,
      createdAt: '2023-01-25T09:00:00Z',
      updatedAt: '2023-05-25T16:30:00Z'
    },
    {
      patternId: 'pattern-2',
      issueType: 'DATA_FORMAT',
      name: 'Negative Price Pattern',
      description: 'Pattern for detecting and fixing negative price values',
      detectionPattern: { ruleName: 'Product Price Range', column: 'price', condition: 'value < 0' },
      confidenceThreshold: 0.90,
      isActive: true,
      createdAt: '2023-02-10T11:15:00Z',
      updatedAt: '2023-05-28T10:40:00Z'
    },
    {
      patternId: 'pattern-3',
      issueType: 'SYSTEM_FAILURE',
      name: 'API Timeout Pattern',
      description: 'Pattern for detecting and fixing API connection timeouts',
      detectionPattern: { errorMessage: 'connection timeout', component: 'APIEnrichment' },
      confidenceThreshold: 0.80,
      isActive: true,
      createdAt: '2023-03-05T14:20:00Z',
      updatedAt: '2023-06-02T09:15:00Z'
    }
  ] as HealingPattern[],
  
  actions: [
    {
      actionId: 'action-1',
      patternId: 'pattern-1',
      name: 'Generate Email',
      actionType: 'DATA_CORRECTION',
      description: 'Generate placeholder email using customer name',
      actionDefinition: { template: '{firstName}.{lastName}@example.com', matchCase: true },
      isActive: true,
      successRate: 0.95,
      createdAt: '2023-01-26T10:30:00Z',
      updatedAt: '2023-05-26T14:20:00Z'
    },
    {
      actionId: 'action-2',
      patternId: 'pattern-2',
      name: 'Fix Negative Prices',
      actionType: 'DATA_CORRECTION',
      description: 'Convert negative prices to absolute values',
      actionDefinition: { operation: 'absolute', column: 'price' },
      isActive: true,
      successRate: 0.98,
      createdAt: '2023-02-12T09:45:00Z',
      updatedAt: '2023-05-30T11:20:00Z'
    },
    {
      actionId: 'action-3',
      patternId: 'pattern-3',
      name: 'API Retry with Backoff',
      actionType: 'RETRY',
      description: 'Retry API connection with exponential backoff',
      actionDefinition: { maxRetries: 5, baseDelay: 1000, multiplier: 2 },
      isActive: true,
      successRate: 0.75,
      createdAt: '2023-03-06T15:40:00Z',
      updatedAt: '2023-06-03T10:30:00Z'
    }
  ] as HealingAction[],
  
  executions: [
    {
      healingId: 'healing-1',
      executionId: 'exec-2',
      validationId: 'val-2',
      patternId: 'pattern-1',
      patternName: 'Missing Email Pattern',
      actionId: 'action-1',
      actionName: 'Generate Email',
      executionTime: '2023-06-15T04:25:00Z',
      status: 'IN_PROGRESS',
      confidence: 0.92,
      successful: false,
      duration: 120
    },
    {
      healingId: 'healing-2',
      executionId: 'exec-3',
      validationId: 'val-3',
      patternId: 'pattern-2',
      patternName: 'Negative Price Pattern',
      actionId: 'action-2',
      actionName: 'Fix Negative Prices',
      executionTime: '2023-06-15T03:55:00Z',
      status: 'FAILED',
      confidence: 0.95,
      successful: false,
      errorMessage: 'Unable to access table due to permissions',
      duration: 180
    },
    {
      healingId: 'healing-3',
      executionId: 'exec-3',
      patternId: 'pattern-3',
      patternName: 'API Timeout Pattern',
      actionId: 'action-3',
      actionName: 'API Retry with Backoff',
      executionTime: '2023-06-15T03:57:00Z',
      status: 'COMPLETED',
      confidence: 0.85,
      successful: true,
      executionDetails: { retries: 3, totalDuration: 4500 },
      duration: 300
    }
  ] as HealingExecution[],
  
  issues: [
    {
      issueId: 'healing-issue-1',
      executionId: 'exec-2',
      pipelineId: 'pipeline-2',
      pipelineName: 'Customer Data',
      issueType: 'DATA_QUALITY',
      component: 'DataValidation',
      severity: 'MEDIUM',
      description: 'Missing email addresses in customer data',
      detectedAt: '2023-06-15T04:20:00Z',
      details: { affectedRows: 15, rule: 'Required Fields Check' },
      status: 'IN_PROGRESS',
      healingId: 'healing-1',
      confidence: 0.92
    },
    {
      issueId: 'healing-issue-2',
      executionId: 'exec-3',
      pipelineId: 'pipeline-3',
      pipelineName: 'Product Enrich',
      issueType: 'DATA_FORMAT',
      component: 'DataValidation',
      severity: 'HIGH',
      description: 'Negative price values in product catalog',
      detectedAt: '2023-06-15T03:50:00Z',
      details: { affectedRows: 42, rule: 'Product Price Range' },
      status: 'FAILED',
      healingId: 'healing-2',
      confidence: 0.95
    },
    {
      issueId: 'healing-issue-3',
      executionId: 'exec-3',
      pipelineId: 'pipeline-3',
      pipelineName: 'Product Enrich',
      issueType: 'SYSTEM_FAILURE',
      component: 'APIEnrichment',
      severity: 'HIGH',
      description: 'API connection timeout during product enrichment',
      detectedAt: '2023-06-15T03:55:00Z',
      details: { errorMessage: 'connection timeout', retries: 3 },
      status: 'COMPLETED',
      healingId: 'healing-3',
      confidence: 0.85
    }
  ] as HealingIssue[],
  
  settings: {
    healingMode: 'SEMI_AUTOMATIC',
    globalConfidenceThreshold: 0.85,
    maxRetryAttempts: 3,
    approvalRequiredHighImpact: true,
    learningModeActive: true,
    additionalSettings: { 
      dataQualityThreshold: 0.90,
      systemFailureThreshold: 0.80 
    },
    updatedAt: '2023-06-01T14:30:00Z',
    updatedBy: 'admin'
  } as HealingSettings,
  
  models: [
    {
      modelId: 'model-1',
      name: 'Email Correction Model',
      description: 'Model for detecting and correcting email format issues',
      modelType: 'CORRECTION',
      version: '1.2.0',
      status: 'ACTIVE',
      accuracy: 0.94,
      lastTrainingDate: '2023-05-20T10:00:00Z',
      trainingDataSize: 25000,
      modelSize: '15MB',
      averageInferenceTime: 25,
      createdAt: '2023-01-15T09:30:00Z',
      updatedAt: '2023-05-20T10:00:00Z'
    },
    {
      modelId: 'model-2',
      name: 'Price Anomaly Detector',
      description: 'Model for detecting price anomalies and corrections',
      modelType: 'DETECTION',
      version: '2.1.0',
      status: 'ACTIVE',
      accuracy: 0.97,
      lastTrainingDate: '2023-05-25T11:30:00Z',
      trainingDataSize: 50000,
      modelSize: '22MB',
      averageInferenceTime: 18,
      createdAt: '2023-02-10T14:45:00Z',
      updatedAt: '2023-05-25T11:30:00Z'
    },
    {
      modelId: 'model-3',
      name: 'Failure Prediction Model',
      description: 'Model for predicting pipeline component failures',
      modelType: 'PREDICTION',
      version: '1.0.5',
      status: 'TRAINING',
      accuracy: 0.88,
      lastTrainingDate: '2023-06-10T08:15:00Z',
      trainingDataSize: 15000,
      modelSize: '18MB',
      averageInferenceTime: 35,
      createdAt: '2023-04-05T16:20:00Z',
      updatedAt: '2023-06-10T08:15:00Z'
    }
  ] as AIModel[],
  
  modelHealth: {
    modelId: 'model-1',
    driftStatus: 'NORMAL',
    featureHealth: 'GOOD',
    predictionQuality: 'GOOD',
    inferencePerformance: 'EXCELLENT',
    lastChecked: '2023-06-15T00:00:00Z',
    metrics: {
      driftScore: 0.03,
      featureCorrelation: 0.92,
      predictionAccuracy: 0.94,
      avgInferenceTime: 22
    },
    recommendations: [
      'Consider retraining with more recent data in the next 30 days'
    ]
  } as ModelHealth
};

// Mock data for alert testing
export const MOCK_ALERT_DATA = {
  alerts: [
    {
      alertId: 'alert-1',
      executionId: 'exec-3',
      alertType: 'PIPELINE_FAILURE',
      severity: 'HIGH',
      status: 'ACTIVE',
      message: 'Pipeline execution failed: Product Enrich',
      details: {
        component: 'APIEnrichment',
        errorMessage: 'API connection timeout after 3 retries'
      },
      source: 'pipeline-service',
      component: 'execution-engine',
      createdAt: '2023-06-15T03:55:00Z',
      relatedAlerts: ['alert-4'],
      selfHealingStatus: 'IN_PROGRESS'
    },
    {
      alertId: 'alert-2',
      alertType: 'DATA_QUALITY',
      severity: 'MEDIUM',
      status: 'ACKNOWLEDGED',
      message: 'Data quality validation failed: Missing email addresses',
      details: {
        dataset: 'customer',
        table: 'customer_data',
        rule: 'Required Fields Check',
        failedRecords: 15
      },
      source: 'quality-service',
      component: 'validation-engine',
      createdAt: '2023-06-15T04:20:00Z',
      acknowledgedBy: 'data-engineer',
      acknowledgedAt: '2023-06-15T04:25:00Z',
      selfHealingStatus: 'IN_PROGRESS'
    },
    {
      alertId: 'alert-3',
      alertType: 'PERFORMANCE',
      severity: 'LOW',
      status: 'ACTIVE',
      message: 'Query performance degradation detected',
      details: {
        query: 'daily_analytics',
        executionTime: '45s',
        baseline: '30s',
        degradation: '50%'
      },
      source: 'performance-service',
      component: 'query-analyzer',
      createdAt: '2023-06-15T02:30:00Z'
    },
    {
      alertId: 'alert-4',
      alertType: 'SYSTEM_HEALTH',
      severity: 'HIGH',
      status: 'ACTIVE',
      message: 'External API responsiveness degraded',
      details: {
        api: 'product-details-api',
        avgResponseTime: '2500ms',
        baseline: '800ms',
        availability: '85%'
      },
      source: 'monitoring-service',
      component: 'external-systems',
      createdAt: '2023-06-15T03:40:00Z',
      relatedAlerts: ['alert-1']
    }
  ] as Alert[],
  
  notificationConfig: {
    channels: {
      TEAMS: true,
      EMAIL: true,
      SMS: false
    },
    teamsWebhookUrl: 'https://teams.webhook.example.com/connector',
    emailConfig: {
      recipients: ['data-team@example.com', 'operations@example.com'],
      subjectPrefix: '[DATA-PIPELINE]',
      includeDetails: true
    },
    alertThresholds: {
      CRITICAL: {
        enabled: true,
        minInterval: 5
      },
      HIGH: {
        enabled: true,
        minInterval: 15
      },
      MEDIUM: {
        enabled: true,
        minInterval: 60
      },
      LOW: {
        enabled: true,
        minInterval: 240,
        batchSize: 10
      }
    },
    updatedAt: '2023-05-15T10:00:00Z'
  } as NotificationConfig,
  
  alertStats: {
    critical: 0,
    high: 2,
    medium: 1,
    low: 1,
    total: 4,
    trend: [
      { timestamp: '2023-06-14T06:00:00Z', count: 2 },
      { timestamp: '2023-06-14T12:00:00Z', count: 3 },
      { timestamp: '2023-06-14T18:00:00Z', count: 1 },
      { timestamp: '2023-06-15T00:00:00Z', count: 2 },
      { timestamp: '2023-06-15T06:00:00Z', count: 4 }
    ]
  }
};

// Mock data for user testing
export const MOCK_USER_DATA = {
  users: [
    {
      id: 'user-1',
      username: 'admin',
      email: 'admin@example.com',
      firstName: 'Admin',
      lastName: 'User',
      role: 'ADMIN',
      isActive: true,
      mfaEnabled: true,
      lastLogin: '2023-06-15T08:30:00Z',
      createdAt: '2023-01-01T00:00:00Z',
      updatedAt: '2023-06-15T08:30:00Z'
    },
    {
      id: 'user-2',
      username: 'data-engineer',
      email: 'engineer@example.com',
      firstName: 'Jane',
      lastName: 'Engineer',
      role: 'DATA_ENGINEER',
      isActive: true,
      mfaEnabled: true,
      lastLogin: '2023-06-15T09:15:00Z',
      createdAt: '2023-01-05T00:00:00Z',
      updatedAt: '2023-06-15T09:15:00Z'
    },
    {
      id: 'user-3',
      username: 'analyst',
      email: 'analyst@example.com',
      firstName: 'John',
      lastName: 'Analyst',
      role: 'DATA_ANALYST',
      isActive: true,
      mfaEnabled: false,
      lastLogin: '2023-06-14T16:45:00Z',
      createdAt: '2023-01-10T00:00:00Z',
      updatedAt: '2023-06-14T16:45:00Z'
    }
  ] as User[],
  
  loginResponse: {
    user: {
      id: 'user-2',
      username: 'data-engineer',
      email: 'engineer@example.com',
      firstName: 'Jane',
      lastName: 'Engineer',
      role: 'DATA_ENGINEER',
      isActive: true,
      mfaEnabled: true,
      lastLogin: '2023-06-15T09:15:00Z',
      createdAt: '2023-01-05T00:00:00Z',
      updatedAt: '2023-06-15T09:15:00Z'
    },
    token: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
    refreshToken: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
    expiresAt: Date.now() + 3600000, // 1 hour from now
    requiresMfa: false
  } as LoginResponse
};

// Default mock responses used by the createApiMock function
const DEFAULT_MOCK_RESPONSES: Record<string, any> = {
  // Pipeline endpoints
  'GET /api/v1/pipelines': () => createMockPaginatedResponse(MOCK_PIPELINE_DATA.definitions, 1, 10, MOCK_PIPELINE_DATA.definitions.length),
  'GET /api/v1/pipelines/\\w+': ({ url }: { url: string }) => {
    const id = url.split('/').pop();
    const pipeline = MOCK_PIPELINE_DATA.definitions.find(p => p.pipelineId === id);
    return createMockDataResponse(pipeline);
  },
  'GET /api/v1/pipelines/\\w+/history': ({ url }: { url: string }) => {
    const id = url.split('/')[3];
    const executions = MOCK_PIPELINE_DATA.executions.filter(e => e.pipelineId === id);
    return createMockPaginatedResponse(executions, 1, 10, executions.length);
  },
  'GET /api/v1/pipelines/\\w+/tasks': ({ url }: { url: string }) => {
    const id = url.split('/')[3];
    const execution = MOCK_PIPELINE_DATA.executions.find(e => e.pipelineId === id);
    return createMockPaginatedResponse(execution?.tasks || [], 1, 10, execution?.tasks?.length || 0);
  },

  // Quality endpoints
  'GET /api/v1/quality/rules': () => createMockPaginatedResponse(MOCK_QUALITY_DATA.rules, 1, 10, MOCK_QUALITY_DATA.rules.length),
  'GET /api/v1/quality/metrics': () => createMockDataResponse(MOCK_QUALITY_DATA.scores),
  'GET /api/v1/quality/datasets': () => createMockPaginatedResponse(MOCK_QUALITY_DATA.dataSummaries, 1, 10, MOCK_QUALITY_DATA.dataSummaries.length),
  'GET /api/v1/quality/issues': () => createMockPaginatedResponse(MOCK_QUALITY_DATA.issues, 1, 10, MOCK_QUALITY_DATA.issues.length),

  // Healing endpoints
  'GET /api/v1/healing/patterns': () => createMockPaginatedResponse(MOCK_HEALING_DATA.patterns, 1, 10, MOCK_HEALING_DATA.patterns.length),
  'GET /api/v1/healing/actions': () => createMockPaginatedResponse(MOCK_HEALING_DATA.actions, 1, 10, MOCK_HEALING_DATA.actions.length),
  'GET /api/v1/healing/history': () => createMockPaginatedResponse(MOCK_HEALING_DATA.executions, 1, 10, MOCK_HEALING_DATA.executions.length),
  'GET /api/v1/healing/issues': () => createMockPaginatedResponse(MOCK_HEALING_DATA.issues, 1, 10, MOCK_HEALING_DATA.issues.length),
  'GET /api/v1/healing/settings': () => createMockDataResponse(MOCK_HEALING_DATA.settings),
  'GET /api/v1/healing/models': () => createMockPaginatedResponse(MOCK_HEALING_DATA.models, 1, 10, MOCK_HEALING_DATA.models.length),

  // Alert endpoints
  'GET /api/v1/alerts': () => createMockPaginatedResponse(MOCK_ALERT_DATA.alerts, 1, 10, MOCK_ALERT_DATA.alerts.length),
  'GET /api/v1/alerts/stats': () => createMockDataResponse(MOCK_ALERT_DATA.alertStats),
  'GET /api/v1/alerts/config': () => createMockDataResponse(MOCK_ALERT_DATA.notificationConfig),

  // Auth endpoints
  'POST /api/v1/auth/login': () => createMockDataResponse(MOCK_USER_DATA.loginResponse),
  'GET /api/v1/auth/profile': () => createMockDataResponse(MOCK_USER_DATA.users[1])
};

/**
 * Creates a mock API client with predefined responses for testing
 * @param mockResponses Map of URL patterns to mock response functions
 * @returns Mock API client with get, post, put, and delete methods
 */
export const createApiMock = (mockResponses: Record<string, jest.Mock> = {}): MockApiClient => {
  // Initialize call history tracking
  const callHistory = {
    get: [] as any[],
    post: [] as any[],
    put: [] as any[],
    delete: [] as any[]
  };

  // Create mock implementations
  const get = jest.fn().mockImplementation((url: string, config: any = {}) => {
    // Track call
    callHistory.get.push({ url, config });

    // Find matching mock response
    const mockResponseKey = findMatchingMockResponse('GET', url, mockResponses);
    if (mockResponseKey) {
      return Promise.resolve(mockResponses[mockResponseKey]({ url, config }));
    }

    // Try default responses
    const defaultResponseKey = findMatchingMockResponse('GET', url, DEFAULT_MOCK_RESPONSES);
    if (defaultResponseKey) {
      const response = DEFAULT_MOCK_RESPONSES[defaultResponseKey]({ url, config });
      return Promise.resolve(response);
    }

    // No matching mock found
    return Promise.reject(createMockErrorResponse('Not Found', 404, 'NOT_FOUND'));
  });

  const post = jest.fn().mockImplementation((url: string, data: any, config: any = {}) => {
    // Track call
    callHistory.post.push({ url, data, config });

    // Find matching mock response
    const mockResponseKey = findMatchingMockResponse('POST', url, mockResponses);
    if (mockResponseKey) {
      return Promise.resolve(mockResponses[mockResponseKey]({ url, data, config }));
    }

    // Try default responses
    const defaultResponseKey = findMatchingMockResponse('POST', url, DEFAULT_MOCK_RESPONSES);
    if (defaultResponseKey) {
      const response = DEFAULT_MOCK_RESPONSES[defaultResponseKey]({ url, data, config });
      return Promise.resolve(response);
    }

    // Default success response
    return Promise.resolve(createMockDataResponse({ success: true }));
  });

  const put = jest.fn().mockImplementation((url: string, data: any, config: any = {}) => {
    // Track call
    callHistory.put.push({ url, data, config });

    // Find matching mock response
    const mockResponseKey = findMatchingMockResponse('PUT', url, mockResponses);
    if (mockResponseKey) {
      return Promise.resolve(mockResponses[mockResponseKey]({ url, data, config }));
    }

    // Try default responses
    const defaultResponseKey = findMatchingMockResponse('PUT', url, DEFAULT_MOCK_RESPONSES);
    if (defaultResponseKey) {
      const response = DEFAULT_MOCK_RESPONSES[defaultResponseKey]({ url, data, config });
      return Promise.resolve(response);
    }

    // Default success response
    return Promise.resolve(createMockDataResponse({ success: true }));
  });

  const del = jest.fn().mockImplementation((url: string, config: any = {}) => {
    // Track call
    callHistory.delete.push({ url, config });

    // Find matching mock response
    const mockResponseKey = findMatchingMockResponse('DELETE', url, mockResponses);
    if (mockResponseKey) {
      return Promise.resolve(mockResponses[mockResponseKey]({ url, config }));
    }

    // Try default responses
    const defaultResponseKey = findMatchingMockResponse('DELETE', url, DEFAULT_MOCK_RESPONSES);
    if (defaultResponseKey) {
      const response = DEFAULT_MOCK_RESPONSES[defaultResponseKey]({ url, config });
      return Promise.resolve(response);
    }

    // Default success response
    return Promise.resolve(createMockDataResponse({ success: true }));
  });

  // Function to reset all mocks
  const reset = () => {
    get.mockClear();
    post.mockClear();
    put.mockClear();
    del.mockClear();
    callHistory.get = [];
    callHistory.post = [];
    callHistory.put = [];
    callHistory.delete = [];
  };

  // Function to get call history
  const getCallHistory = () => ({ ...callHistory });

  return {
    get,
    post,
    put,
    delete: del,
    reset,
    getCallHistory
  };
};

/**
 * Helper function to find matching mock response based on method and URL pattern
 * @param method HTTP method (GET, POST, PUT, DELETE)
 * @param url Request URL
 * @param mockResponses Mock response map
 * @returns Matching response key or undefined
 */
const findMatchingMockResponse = (
  method: string,
  url: string,
  mockResponses: Record<string, any>
): string | undefined => {
  return Object.keys(mockResponses).find(key => {
    const [mockMethod, mockPath] = key.split(' ');
    if (mockMethod !== method) return false;
    
    // Exact match
    if (mockPath === url) return true;
    
    // Regex match
    const pathRegex = new RegExp(`^${mockPath}$`);
    return pathRegex.test(url);
  });
};

/**
 * Creates a mock paginated response for list endpoints
 * @param items Array of items to paginate
 * @param page Current page number
 * @param pageSize Number of items per page
 * @param totalItems Total number of items
 * @returns ListResponse with paginated items
 */
export function createMockPaginatedResponse<T>(
  items: T[],
  page: number,
  pageSize: number,
  totalItems: number
): ListResponse<T> {
  const totalPages = Math.ceil(totalItems / pageSize);
  const start = (page - 1) * pageSize;
  const end = Math.min(start + pageSize, totalItems);
  const paginatedItems = items.slice(start, end);

  return {
    status: 'SUCCESS',
    message: 'Data retrieved successfully',
    metadata: {
      timestamp: new Date().toISOString(),
      requestId: `req-${Math.random().toString(36).substring(2, 10)}`,
      processingTime: Math.floor(Math.random() * 100)
    },
    items: paginatedItems,
    pagination: {
      page,
      pageSize,
      totalItems,
      totalPages,
      nextPage: page < totalPages ? `/api/v1/resource?page=${page + 1}&pageSize=${pageSize}` : undefined,
      previousPage: page > 1 ? `/api/v1/resource?page=${page - 1}&pageSize=${pageSize}` : undefined
    }
  };
}

/**
 * Creates a mock data response for single-item endpoints
 * @param data Data to include in the response
 * @returns DataResponse with the provided data
 */
export function createMockDataResponse<T>(data: T): DataResponse<T> {
  return {
    status: 'SUCCESS',
    message: 'Data retrieved successfully',
    metadata: {
      timestamp: new Date().toISOString(),
      requestId: `req-${Math.random().toString(36).substring(2, 10)}`,
      processingTime: Math.floor(Math.random() * 100)
    },
    data
  };
}

/**
 * Creates a mock error response for testing error handling
 * @param message Error message
 * @param statusCode HTTP status code
 * @param errorCode Error code identifier
 * @returns Error response object
 */
export function createMockErrorResponse(
  message: string,
  statusCode: number,
  errorCode: string
): Error {
  const error = new Error(message) as any;
  error.response = {
    data: {
      status: 'ERROR',
      message,
      metadata: {
        timestamp: new Date().toISOString(),
        requestId: `req-${Math.random().toString(36).substring(2, 10)}`
      },
      error: {
        statusCode,
        message,
        errorCode
      }
    },
    status: statusCode
  };
  return error;
}

/**
 * Creates a mock implementation of the pipeline service
 * @param customResponses Custom responses to override defaults
 * @returns Mock pipeline service with predefined responses
 */
export function mockPipelineService(customResponses: Record<string, any> = {}) {
  const apiMock = createApiMock(customResponses);

  return {
    getPipelines: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/pipelines', { params });
    }),
    
    getPipelineById: jest.fn(async (id: string) => {
      return apiMock.get(`/api/v1/pipelines/${id}`);
    }),
    
    getPipelineHistory: jest.fn(async (id: string, params?: PaginationParams) => {
      return apiMock.get(`/api/v1/pipelines/${id}/history`, { params });
    }),
    
    getPipelineTasks: jest.fn(async (id: string, params?: PaginationParams) => {
      return apiMock.get(`/api/v1/pipelines/${id}/tasks`, { params });
    }),
    
    createPipeline: jest.fn(async (pipeline: Partial<PipelineDefinition>) => {
      return apiMock.post('/api/v1/pipelines', pipeline);
    }),
    
    updatePipeline: jest.fn(async (id: string, pipeline: Partial<PipelineDefinition>) => {
      return apiMock.put(`/api/v1/pipelines/${id}`, pipeline);
    }),
    
    deletePipeline: jest.fn(async (id: string) => {
      return apiMock.delete(`/api/v1/pipelines/${id}`);
    }),
    
    executePipeline: jest.fn(async (id: string, params?: any) => {
      return apiMock.post(`/api/v1/pipelines/${id}/execute`, params);
    }),
    
    reset: apiMock.reset,
    
    getCallHistory: apiMock.getCallHistory
  };
}

/**
 * Creates a mock implementation of the quality service
 * @param customResponses Custom responses to override defaults
 * @returns Mock quality service with predefined responses
 */
export function mockQualityService(customResponses: Record<string, any> = {}) {
  const apiMock = createApiMock(customResponses);

  return {
    getQualityRules: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/quality/rules', { params });
    }),
    
    getQualityMetrics: jest.fn(async () => {
      return apiMock.get('/api/v1/quality/metrics');
    }),
    
    getDatasetSummaries: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/quality/datasets', { params });
    }),
    
    getQualityIssues: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/quality/issues', { params });
    }),
    
    createQualityRule: jest.fn(async (rule: Partial<QualityRule>) => {
      return apiMock.post('/api/v1/quality/rules', rule);
    }),
    
    updateQualityRule: jest.fn(async (id: string, rule: Partial<QualityRule>) => {
      return apiMock.put(`/api/v1/quality/rules/${id}`, rule);
    }),
    
    deleteQualityRule: jest.fn(async (id: string) => {
      return apiMock.delete(`/api/v1/quality/rules/${id}`);
    }),
    
    getValidationResults: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/quality/validation-results', { params });
    }),
    
    reset: apiMock.reset,
    
    getCallHistory: apiMock.getCallHistory
  };
}

/**
 * Creates a mock implementation of the healing service
 * @param customResponses Custom responses to override defaults
 * @returns Mock healing service with predefined responses
 */
export function mockHealingService(customResponses: Record<string, any> = {}) {
  const apiMock = createApiMock(customResponses);

  return {
    getPatterns: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/healing/patterns', { params });
    }),
    
    getActions: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/healing/actions', { params });
    }),
    
    getHistory: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/healing/history', { params });
    }),
    
    getIssues: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/healing/issues', { params });
    }),
    
    getSettings: jest.fn(async () => {
      return apiMock.get('/api/v1/healing/settings');
    }),
    
    updateSettings: jest.fn(async (settings: Partial<HealingSettings>) => {
      return apiMock.put('/api/v1/healing/settings', settings);
    }),
    
    getModels: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/healing/models', { params });
    }),
    
    getModelHealth: jest.fn(async (id: string) => {
      return apiMock.get(`/api/v1/healing/models/${id}/health`);
    }),
    
    trainModel: jest.fn(async (modelConfig: any) => {
      return apiMock.post('/api/v1/healing/train', modelConfig);
    }),
    
    createPattern: jest.fn(async (pattern: Partial<HealingPattern>) => {
      return apiMock.post('/api/v1/healing/patterns', pattern);
    }),
    
    updatePattern: jest.fn(async (id: string, pattern: Partial<HealingPattern>) => {
      return apiMock.put(`/api/v1/healing/patterns/${id}`, pattern);
    }),
    
    createAction: jest.fn(async (action: Partial<HealingAction>) => {
      return apiMock.post('/api/v1/healing/actions', action);
    }),
    
    updateAction: jest.fn(async (id: string, action: Partial<HealingAction>) => {
      return apiMock.put(`/api/v1/healing/actions/${id}`, action);
    }),
    
    reset: apiMock.reset,
    
    getCallHistory: apiMock.getCallHistory
  };
}

/**
 * Creates a mock implementation of the alert service
 * @param customResponses Custom responses to override defaults
 * @returns Mock alert service with predefined responses
 */
export function mockAlertService(customResponses: Record<string, any> = {}) {
  const apiMock = createApiMock(customResponses);

  return {
    getAlerts: jest.fn(async (params?: PaginationParams) => {
      return apiMock.get('/api/v1/alerts', { params });
    }),
    
    getAlertById: jest.fn(async (id: string) => {
      return apiMock.get(`/api/v1/alerts/${id}`);
    }),
    
    getAlertStats: jest.fn(async () => {
      return apiMock.get('/api/v1/alerts/stats');
    }),
    
    getNotificationConfig: jest.fn(async () => {
      return apiMock.get('/api/v1/alerts/config');
    }),
    
    updateNotificationConfig: jest.fn(async (config: Partial<NotificationConfig>) => {
      return apiMock.put('/api/v1/alerts/config', config);
    }),
    
    acknowledgeAlert: jest.fn(async (id: string, acknowledgement: any) => {
      return apiMock.post(`/api/v1/alerts/${id}/acknowledge`, acknowledgement);
    }),
    
    resolveAlert: jest.fn(async (id: string, resolution: any) => {
      return apiMock.post(`/api/v1/alerts/${id}/resolve`, resolution);
    }),
    
    reset: apiMock.reset,
    
    getCallHistory: apiMock.getCallHistory
  };
}

/**
 * Creates a mock implementation of the auth service
 * @param customResponses Custom responses to override defaults
 * @returns Mock auth service with predefined responses
 */
export function mockAuthService(customResponses: Record<string, any> = {}) {
  const apiMock = createApiMock(customResponses);

  return {
    login: jest.fn(async (credentials: { username: string; password: string }) => {
      return apiMock.post('/api/v1/auth/login', credentials);
    }),
    
    logout: jest.fn(async () => {
      return apiMock.post('/api/v1/auth/logout', {});
    }),
    
    refreshToken: jest.fn(async (refreshToken: string) => {
      return apiMock.post('/api/v1/auth/refresh', { refreshToken });
    }),
    
    getUserProfile: jest.fn(async () => {
      return apiMock.get('/api/v1/auth/profile');
    }),
    
    updateUserProfile: jest.fn(async (profileData: any) => {
      return apiMock.put('/api/v1/auth/profile', profileData);
    }),
    
    reset: apiMock.reset,
    
    getCallHistory: apiMock.getCallHistory
  };
}