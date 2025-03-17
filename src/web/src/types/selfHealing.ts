/**
 * TypeScript type definitions for the self-healing functionality of the data pipeline.
 * This file defines interfaces, types, and enums related to issue detection, pattern recognition,
 * healing actions, AI models, and healing execution data structures used throughout the web application.
 */

import {
  ID,
  Timestamp,
  ISO8601Date,
  JSONObject,
  Nullable,
  Optional,
  HealingStatus,
  AlertSeverity,
  PipelineStatus
} from './global';

import {
  PaginationParams,
  DateRangeParams
} from './api';

/**
 * Enum for self-healing operation modes
 */
export enum HealingMode {
  AUTOMATIC = 'AUTOMATIC',
  SEMI_AUTOMATIC = 'SEMI_AUTOMATIC',
  MANUAL = 'MANUAL'
}

/**
 * Enum for AI model status values
 */
export enum ModelStatus {
  ACTIVE = 'ACTIVE',
  TRAINING = 'TRAINING',
  INACTIVE = 'INACTIVE',
  FAILED = 'FAILED'
}

/**
 * Enum for AI model types used in self-healing
 */
export enum ModelType {
  DETECTION = 'DETECTION',
  IMPUTATION = 'IMPUTATION',
  CORRECTION = 'CORRECTION',
  PREDICTION = 'PREDICTION',
  SCHEMA = 'SCHEMA'
}

/**
 * Enum for self-healing activity types
 */
export enum ActivityType {
  ISSUE_DETECTED = 'ISSUE_DETECTED',
  HEALING_STARTED = 'HEALING_STARTED',
  HEALING_COMPLETED = 'HEALING_COMPLETED',
  HEALING_FAILED = 'HEALING_FAILED',
  MODEL_TRAINING = 'MODEL_TRAINING',
  CONFIG_CHANGED = 'CONFIG_CHANGED',
  MANUAL_INTERVENTION = 'MANUAL_INTERVENTION'
}

/**
 * Enum for issue types that can be healed
 */
export enum IssueType {
  DATA_FORMAT = 'DATA_FORMAT',
  DATA_QUALITY = 'DATA_QUALITY',
  SYSTEM_FAILURE = 'SYSTEM_FAILURE',
  PERFORMANCE = 'PERFORMANCE'
}

/**
 * Enum for healing action types
 */
export enum ActionType {
  DATA_CORRECTION = 'DATA_CORRECTION',
  PARAMETER_ADJUSTMENT = 'PARAMETER_ADJUSTMENT',
  RESOURCE_OPTIMIZATION = 'RESOURCE_OPTIMIZATION',
  RETRY = 'RETRY',
  SCHEMA_CORRECTION = 'SCHEMA_CORRECTION'
}

/**
 * Interface for issue pattern detection information
 */
export interface HealingPattern {
  patternId: ID;
  issueType: IssueType;
  name: string;
  description: string;
  detectionPattern: JSONObject;
  confidenceThreshold: number;
  isActive: boolean;
  metadata: Optional<JSONObject>;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  actions: Optional<HealingAction[]>;
}

/**
 * Interface for healing action information
 */
export interface HealingAction {
  actionId: ID;
  patternId: ID;
  name: string;
  actionType: ActionType;
  description: string;
  actionDefinition: JSONObject;
  isActive: boolean;
  successRate: number;
  metadata: Optional<JSONObject>;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Interface for healing execution information
 */
export interface HealingExecution {
  healingId: ID;
  executionId: ID;
  validationId: Optional<ID>;
  patternId: ID;
  patternName: string;
  actionId: ID;
  actionName: string;
  executionTime: Timestamp;
  status: HealingStatus;
  confidence: number;
  successful: boolean;
  executionDetails: Optional<JSONObject>;
  errorMessage: Optional<string>;
  duration: Optional<number>;
}

/**
 * Interface for issue information that needs healing
 */
export interface HealingIssue {
  issueId: ID;
  executionId: Optional<ID>;
  pipelineId: Optional<ID>;
  pipelineName: Optional<string>;
  issueType: IssueType;
  component: string;
  severity: AlertSeverity;
  description: string;
  detectedAt: Timestamp;
  details: JSONObject;
  status: HealingStatus;
  healingId: Optional<ID>;
  suggestedActions: Optional<HealingAction[]>;
  confidence: Optional<number>;
}

/**
 * Interface for self-healing configuration settings
 */
export interface HealingSettings {
  healingMode: HealingMode;
  globalConfidenceThreshold: number;
  maxRetryAttempts: number;
  approvalRequiredHighImpact: boolean;
  learningModeActive: boolean;
  additionalSettings: Optional<JSONObject>;
  updatedAt: Timestamp;
  updatedBy: Optional<string>;
}

/**
 * Interface for AI model information used in self-healing
 */
export interface AIModel {
  modelId: ID;
  name: string;
  description: string;
  modelType: ModelType;
  version: string;
  status: ModelStatus;
  accuracy: number;
  lastTrainingDate: Timestamp;
  trainingDataSize: number;
  modelSize: string;
  averageInferenceTime: number;
  metadata: Optional<JSONObject>;
  createdAt: Timestamp;
  updatedAt: Timestamp;
}

/**
 * Interface for AI model health metrics
 */
export interface ModelHealth {
  modelId: ID;
  driftStatus: string;
  featureHealth: string;
  predictionQuality: string;
  inferencePerformance: string;
  lastChecked: Timestamp;
  metrics: JSONObject;
  recommendations: Optional<string[]>;
}

/**
 * Interface for model training request parameters
 */
export interface ModelTrainingRequest {
  modelId: Optional<ID>;
  modelType: ModelType;
  name: string;
  description: Optional<string>;
  trainingConfig: JSONObject;
  datasetId: Optional<ID>;
  customDatasetPath: Optional<string>;
}

/**
 * Interface for model evaluation results
 */
export interface ModelEvaluationResult {
  modelId: ID;
  accuracy: number;
  precision: number;
  recall: number;
  f1Score: number;
  rmse: Optional<number>;
  confusionMatrix: Optional<JSONObject>;
  evaluationTime: Timestamp;
  additionalMetrics: Optional<JSONObject>;
}

/**
 * Interface for self-healing activity log entries
 */
export interface HealingActivityLogEntry {
  activityId: ID;
  timestamp: Timestamp;
  activityType: ActivityType;
  description: string;
  executionId: Optional<ID>;
  healingId: Optional<ID>;
  modelId: Optional<ID>;
  userId: Optional<string>;
  details: Optional<JSONObject>;
}

/**
 * Interface for aggregated dashboard data
 */
export interface HealingDashboardData {
  totalIssuesDetected: number;
  issuesResolvedAutomatically: number;
  activeIssues: number;
  overallSuccessRate: number;
  averageResolutionTime: number;
  issuesByType: Record<IssueType, number>;
  successRateByType: Record<IssueType, number>;
  resolutionTimeByType: Record<IssueType, number>;
  issuesTrend: Array<{date: ISO8601Date, count: number}>;
  successRateTrend: Array<{date: ISO8601Date, rate: number}>;
  modelPerformance: Record<string, number>;
  recentActivities: HealingActivityLogEntry[];
}

/**
 * Interface for filtering healing issues
 */
export interface HealingFilters {
  issueType: Optional<IssueType>;
  severity: Optional<AlertSeverity>;
  status: Optional<HealingStatus>;
  pipelineId: Optional<ID>;
  component: Optional<string>;
  dateRange: Optional<DateRangeParams>;
}

/**
 * Interface for healing rule test results
 */
export interface HealingRuleTest {
  patternId: Optional<ID>;
  actionId: Optional<ID>;
  testData: JSONObject;
  detectionResult: boolean;
  confidence: number;
  actionResult: Optional<JSONObject>;
  actionSuccess: Optional<boolean>;
  executionTime: number;
  testTimestamp: Timestamp;
}

/**
 * Interface for manual healing request parameters
 */
export interface ManualHealingRequest {
  issueId: ID;
  actionId: ID;
  parameters: Optional<JSONObject>;
  notes: Optional<string>;
}