/**
 * Type definitions for dashboard components and data structures.
 * These types support the self-healing data pipeline web interface
 * and define structures for pipeline health, data quality, alerts,
 * and other dashboard-related data.
 */

/**
 * Enum representing possible pipeline status values
 */
export enum PipelineStatus {
  HEALTHY = 'HEALTHY',
  WARNING = 'WARNING',
  ERROR = 'ERROR',
  INACTIVE = 'INACTIVE'
}

/**
 * Interface for pipeline health metrics displayed on dashboard
 */
export interface PipelineHealthMetrics {
  healthyPercentage: number;
  totalPipelines: number;
  healthyCount: number;
  warningCount: number;
  errorCount: number;
  inactiveCount: number;
}

/**
 * Interface for data quality metrics displayed on dashboard
 */
export interface DataQualityMetrics {
  passPercentage: number;
  totalRules: number;
  passingRules: number;
  failingRules: number;
  warningRules: number;
}

/**
 * Interface for self-healing metrics displayed on dashboard
 */
export interface SelfHealingMetrics {
  autoFixPercentage: number;
  totalIssues: number;
  autoFixedCount: number;
  manualFixCount: number;
  pendingCount: number;
}

/**
 * Enum representing possible alert severity levels
 */
export enum AlertSeverity {
  CRITICAL = 'CRITICAL',
  HIGH = 'HIGH',
  MEDIUM = 'MEDIUM',
  LOW = 'LOW'
}

/**
 * Interface for alert summary information displayed on dashboard
 */
export interface AlertSummary {
  id: string;
  severity: AlertSeverity;
  description: string;
  timestamp: string;
  pipeline: string;
  selfHealingStatus: string;
}

/**
 * Enum representing possible system component status values
 */
export enum SystemComponentStatus {
  OK = 'OK',
  WARN = 'WARN',
  ERROR = 'ERROR'
}

/**
 * Interface for system status information displayed on dashboard
 */
export interface SystemStatus {
  gcsConnector: SystemComponentStatus;
  cloudSql: SystemComponentStatus;
  externalApis: SystemComponentStatus;
  bigQuery: SystemComponentStatus;
  mlServices: SystemComponentStatus;
}

/**
 * Interface for quick statistics displayed on dashboard
 */
export interface QuickStats {
  activePipelines: number;
  pendingJobs: number;
  alertRateChange: number;
  alertRatePeriod: string;
}

/**
 * Interface for pipeline execution information displayed on dashboard
 */
export interface PipelineExecution {
  id: string;
  pipelineName: string;
  status: string;
  startTime: string;
  endTime: string;
  duration: number;
  hasWarning: boolean;
}

/**
 * Interface for AI-generated insights displayed on dashboard
 */
export interface AIInsight {
  id: string;
  description: string;
  timestamp: string;
  confidence: number;
  relatedEntity: string;
}

/**
 * Main interface aggregating all dashboard data
 */
export interface DashboardData {
  pipelineHealth: PipelineHealthMetrics;
  dataQuality: DataQualityMetrics;
  selfHealing: SelfHealingMetrics;
  activeAlerts: AlertSummary[];
  systemStatus: SystemStatus;
  quickStats: QuickStats;
  recentExecutions: PipelineExecution[];
  aiInsights: AIInsight[];
}

/**
 * Type representing time range options for dashboard metrics
 */
export type TimeRange = 
  'LAST_HOUR' | 
  'LAST_24_HOURS' | 
  'LAST_7_DAYS' | 
  'LAST_30_DAYS' | 
  'CUSTOM';

// Named constants for the TimeRange values
export const LAST_HOUR: TimeRange = 'LAST_HOUR';
export const LAST_24_HOURS: TimeRange = 'LAST_24_HOURS';
export const LAST_7_DAYS: TimeRange = 'LAST_7_DAYS';
export const LAST_30_DAYS: TimeRange = 'LAST_30_DAYS';
export const CUSTOM: TimeRange = 'CUSTOM';

/**
 * Interface for dashboard filtering options
 */
export interface DashboardFilters {
  timeRange: TimeRange;
  customStartDate: string | null;
  customEndDate: string | null;
  pipelineFilter: string[];
  statusFilter: PipelineStatus[];
}