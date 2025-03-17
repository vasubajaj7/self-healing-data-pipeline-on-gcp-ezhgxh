/**
 * TypeScript type definitions for data quality functionality in the self-healing data pipeline web application.
 * This file defines interfaces, types, and enums specific to data quality validation, metrics, and reporting.
 */

import {
  ID, Timestamp, ISO8601Date, JSONObject, Nullable, Optional, QualityStatus, AlertSeverity, HealingStatus
} from './global';
import {
  QualityRule, QualityValidation, QualityScore, ValidationStatus
} from './api';

/**
 * Enum for quality dimension categories
 */
export enum QualityDimension {
  COMPLETENESS = 'COMPLETENESS',
  ACCURACY = 'ACCURACY',
  CONSISTENCY = 'CONSISTENCY',
  TIMELINESS = 'TIMELINESS',
  VALIDITY = 'VALIDITY'
}

/**
 * Enum for quality rule types
 */
export enum QualityRuleType {
  SCHEMA = 'SCHEMA',
  NULL_CHECK = 'NULL_CHECK',
  VALUE_RANGE = 'VALUE_RANGE',
  REFERENTIAL = 'REFERENTIAL',
  PATTERN_MATCH = 'PATTERN_MATCH',
  UNIQUENESS = 'UNIQUENESS',
  STATISTICAL = 'STATISTICAL',
  CUSTOM = 'CUSTOM'
}

/**
 * Enum for quality issue status values
 */
export enum QualityIssueStatus {
  OPEN = 'OPEN',
  IN_PROGRESS = 'IN_PROGRESS',
  RESOLVED = 'RESOLVED',
  IGNORED = 'IGNORED'
}

/**
 * Interface for dataset quality summary information
 */
export interface DatasetQualitySummary {
  dataset: string;
  tables: Array<{ table: string; qualityScore: number }>;
  overallScore: number;
  trend: string;
  issueCount: number;
  lastUpdated: Timestamp;
}

/**
 * Interface for quality issue information
 */
export interface QualityIssue {
  issueId: ID;
  dataset: string;
  table: string;
  column: Optional<string>;
  ruleId: Optional<ID>;
  validationId: Optional<ID>;
  description: string;
  dimension: QualityDimension;
  severity: AlertSeverity;
  status: QualityIssueStatus;
  affectedRows: Optional<number>;
  detectedAt: Timestamp;
  healingStatus: Optional<HealingStatus>;
  healingId: Optional<ID>;
  details: Optional<JSONObject>;
}

/**
 * Interface for detailed quality rule information with statistics
 */
export interface QualityRuleDetails {
  rule: QualityRule;
  executionCount: number;
  successRate: number;
  lastExecuted: Optional<Timestamp>;
  avgExecutionTime: Optional<number>;
  failureHistory: Array<{ date: Timestamp; count: number }>;
  relatedIssues: Optional<QualityIssue[]>;
}

/**
 * Interface for quality score time series data
 */
export interface QualityTimeSeries {
  dataset: string;
  table: Optional<string>;
  timeRange: string;
  startDate: ISO8601Date;
  endDate: ISO8601Date;
  overallScores: Array<{ timestamp: Timestamp; score: number }>;
  dimensionScores: Record<QualityDimension, Array<{ timestamp: Timestamp; score: number }>>;
}

/**
 * Interface for detailed quality validation results
 */
export interface QualityValidationResult {
  validation: QualityValidation;
  rule: QualityRule;
  dataset: string;
  table: string;
  executionTime: number;
  recordsProcessed: number;
  failedRecords: number;
  sampleFailures: Array<JSONObject>;
  healingAttempted: boolean;
  healingSuccessful: Optional<boolean>;
  healingDetails: Optional<JSONObject>;
}

/**
 * Interface for detailed column quality information
 */
export interface ColumnQualityDetails {
  dataset: string;
  table: string;
  column: string;
  dataType: string;
  nullPercentage: number;
  uniquePercentage: number;
  statistics: JSONObject;
  topValues: Array<{ value: any; count: number; percentage: number }>;
  qualityIssues: QualityIssue[];
  appliedRules: QualityRule[];
}

/**
 * Interface for overall quality statistics
 */
export interface QualityStatistics {
  totalDatasets: number;
  totalTables: number;
  totalRules: number;
  activeRules: number;
  validationsLast24h: number;
  validationsLast7d: number;
  validationsLast30d: number;
  successRateLast24h: number;
  successRateLast7d: number;
  successRateLast30d: number;
  openIssues: number;
  issuesByDimension: Record<QualityDimension, number>;
  issuesBySeverity: Record<AlertSeverity, number>;
  selfHealingSuccessRate: number;
}

/**
 * Interface for quality dashboard filtering options
 */
export interface QualityDashboardFilters {
  dataset: Optional<string>;
  table: Optional<string>;
  dimension: Optional<QualityDimension>;
  ruleType: Optional<QualityRuleType>;
  severity: Optional<AlertSeverity>;
  status: Optional<QualityIssueStatus | ValidationStatus>;
  timeRange: Optional<string>;
  startDate: Optional<ISO8601Date>;
  endDate: Optional<ISO8601Date>;
  minScore: Optional<number>;
  maxScore: Optional<number>;
  searchTerm: Optional<string>;
}

/**
 * Interface for quality rule template information
 */
export interface QualityRuleTemplate {
  templateId: string;
  name: string;
  description: string;
  ruleType: QualityRuleType;
  dimension: QualityDimension;
  templateDefinition: JSONObject;
  requiredParameters: string[];
  optionalParameters: string[];
  applicableDataTypes: string[];
  exampleUsage: JSONObject;
}

/**
 * Enum for quality trend indicators
 */
export enum QualityTrend {
  IMPROVING = 'IMPROVING',
  STABLE = 'STABLE',
  DECLINING = 'DECLINING',
  UNKNOWN = 'UNKNOWN'
}

/**
 * Enum for predefined quality time range options
 */
export enum QualityTimeRange {
  LAST_24_HOURS = 'LAST_24_HOURS',
  LAST_7_DAYS = 'LAST_7_DAYS',
  LAST_30_DAYS = 'LAST_30_DAYS',
  LAST_90_DAYS = 'LAST_90_DAYS',
  CUSTOM = 'CUSTOM'
}