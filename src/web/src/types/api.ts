/**
 * TypeScript type definitions for API interactions in the self-healing data pipeline web application.
 * This file defines interfaces, types, and enums for API requests, responses, and data structures
 * used across the application for communication with backend services.
 */

import {
  ID,
  Timestamp,
  ISO8601Date,
  JSONObject,
  Nullable,
  Optional,
  PipelineStatus,
  AlertSeverity,
  QualityStatus,
  HealingStatus
} from './global';

/**
 * API Error interface for standardized error handling
 */
export interface ApiError {
  statusCode: number;
  message: string;
  errorCode: string;
  details?: Record<string, any>;
}

/**
 * Common request parameters for paginated API requests
 */
export interface PaginationParams {
  page: number;
  pageSize: number;
  sortBy?: string;
  descending?: boolean;
}

/**
 * Common request parameters for date range filtering
 */
export interface DateRangeParams {
  startDate: ISO8601Date;
  endDate: ISO8601Date;
}

/**
 * Metadata for paginated responses
 */
export interface PaginationMetadata {
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
  nextPage?: string;
  previousPage?: string;
}

/**
 * API response status values
 */
export enum ResponseStatus {
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
  WARNING = 'WARNING'
}

/**
 * Common metadata included in all API responses
 */
export interface ResponseMetadata {
  timestamp: Timestamp;
  requestId: string;
  processingTime?: number;
}

/**
 * Base response interface for all API endpoints
 */
export interface ApiResponse {
  status: ResponseStatus;
  message: string;
  metadata: ResponseMetadata;
}

/**
 * Generic response interface for endpoints returning a single data item
 */
export interface DataResponse<T> extends ApiResponse {
  data: T;
}

/**
 * Generic response interface for endpoints returning a list of items
 */
export interface ListResponse<T> extends ApiResponse {
  items: T[];
  pagination: PaginationMetadata;
}

/**
 * Response interface for error responses
 */
export interface ErrorResponse extends ApiResponse {
  error: ApiError;
}

/**
 * Data source system interface
 */
export interface SourceSystem {
  sourceId: ID;
  name: string;
  sourceType: string;
  connectionDetails: JSONObject;
  schemaVersion?: string;
  description?: string;
  isActive: boolean;
  schemaDefinition?: JSONObject;
  extractionSettings?: JSONObject;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Pipeline definition interface
 */
export interface PipelineDefinition {
  pipelineId: ID;
  pipelineName: string;
  sourceId: ID;
  sourceName: string;
  targetDataset: string;
  targetTable: string;
  configuration: JSONObject;
  description?: string;
  isActive: boolean;
  metadata?: JSONObject;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  lastExecutionStatus?: PipelineStatus;
  lastExecutionTime?: Timestamp;
}

/**
 * Pipeline execution interface
 */
export interface PipelineExecution {
  executionId: ID;
  pipelineId: ID;
  pipelineName: string;
  startTime: Timestamp;
  endTime?: Timestamp;
  status: PipelineStatus;
  recordsProcessed?: number;
  errorDetails?: string;
  executionParams?: JSONObject;
  dagRunId: string;
  tasks?: TaskExecution[];
}

/**
 * Task execution interface
 */
export interface TaskExecution {
  taskExecutionId: ID;
  executionId: ID;
  taskId: string;
  taskType: string;
  startTime: Timestamp;
  endTime?: Timestamp;
  status: PipelineStatus;
  errorDetails?: string;
  retryCount: number;
  taskParams?: JSONObject;
}

/**
 * Quality rule interface
 */
export interface QualityRule {
  ruleId: ID;
  ruleName: string;
  targetDataset: string;
  targetTable: string;
  ruleType: string;
  expectationType: string;
  ruleDefinition: JSONObject;
  severity: AlertSeverity;
  isActive: boolean;
  description?: string;
  metadata?: JSONObject;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Validation status enum
 */
export enum ValidationStatus {
  PASSED = 'PASSED',
  FAILED = 'FAILED',
  WARNING = 'WARNING',
  SKIPPED = 'SKIPPED',
  ERROR = 'ERROR'
}

/**
 * Quality validation interface
 */
export interface QualityValidation {
  validationId: ID;
  executionId: ID;
  ruleId: ID;
  validationTime: Timestamp;
  status: ValidationStatus;
  failedRecords?: number;
  details?: JSONObject;
  selfHealingStatus?: HealingStatus;
}

/**
 * Quality score interface
 */
export interface QualityScore {
  overallScore: number;
  dimensionScores: Record<string, number>;
  qualityMetrics: Record<string, any>;
  calculationTime: Timestamp;
}

/**
 * Issue pattern interface for self-healing
 */
export interface IssuePattern {
  patternId: ID;
  issueType: string;
  detectionPattern: JSONObject;
  confidenceThreshold: number;
  description?: string;
  metadata?: JSONObject;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Healing action interface
 */
export interface HealingAction {
  actionId: ID;
  patternId: ID;
  actionType: string;
  actionDefinition: JSONObject;
  isActive: boolean;
  description?: string;
  metadata?: JSONObject;
  successRate: number;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Healing execution interface
 */
export interface HealingExecution {
  healingId: ID;
  executionId: ID;
  validationId?: ID;
  patternId: ID;
  actionId: ID;
  executionTime: Timestamp;
  status: HealingStatus;
  confidence: number;
  successful: boolean;
  executionDetails?: JSONObject;
  errorMessage?: string;
}

/**
 * Healing configuration interface
 */
export interface HealingConfig {
  healingMode: string;
  globalConfidenceThreshold: number;
  maxRetryAttempts: number;
  approvalRequiredHighImpact: boolean;
  learningModeActive: boolean;
  additionalSettings?: JSONObject;
  updatedAt: Timestamp;
}

/**
 * Alert interface
 */
export interface Alert {
  alertId: ID;
  executionId?: ID;
  alertType: string;
  severity: AlertSeverity;
  message: string;
  details?: JSONObject;
  createdAt: Timestamp;
  acknowledged: boolean;
  acknowledgedBy?: string;
  acknowledgedAt?: Timestamp;
  relatedAlerts?: ID[];
}

/**
 * Alert configuration interface
 */
export interface AlertConfig {
  teamsWebhookUrl: JSONObject;
  emailConfig: JSONObject;
  alertThresholds: JSONObject;
  enabledChannels: Record<string, boolean>;
  updatedAt: Timestamp;
}

/**
 * Pipeline metric interface
 */
export interface PipelineMetric {
  metricId: ID;
  executionId?: ID;
  metricCategory: string;
  metricName: string;
  metricValue: number;
  metricUnit?: string;
  collectionTime: Timestamp;
  dimensions?: Record<string, string>;
}

/**
 * Time series metric data interface
 */
export interface MetricTimeSeries {
  metricName: string;
  metricUnit?: string;
  dataPoints: Array<{timestamp: Timestamp, value: number}>;
  statistics?: JSONObject;
  annotations?: JSONObject;
}

/**
 * Optimization configuration interface
 */
export interface OptimizationConfig {
  queryOptimizationSettings: JSONObject;
  schemaOptimizationSettings: JSONObject;
  resourceOptimizationSettings: JSONObject;
  autoImplementationEnabled: boolean;
  updatedAt: Timestamp;
}

/**
 * Optimization recommendation interface
 */
export interface OptimizationRecommendation {
  recommendationId: ID;
  recommendationType: string;
  targetResource: string;
  description: string;
  impact: string;
  estimatedImprovement: JSONObject;
  implementationDetails: JSONObject;
  createdAt: Timestamp;
  implemented: boolean;
  implementedAt?: Timestamp;
}

/**
 * Health check response interface
 */
export interface HealthCheckResponse {
  status: ResponseStatus;
  message: string;
  version: string;
  components: Record<string, {status: string, details?: any}>;
  systemMetrics: JSONObject;
}