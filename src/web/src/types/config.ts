/**
 * TypeScript type definitions for configuration-related interfaces and types used in the
 * self-healing data pipeline web application.
 * 
 * This file defines the structure of configuration objects for data sources, pipelines,
 * quality validation, self-healing, and other configurable aspects of the system.
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
 * Application-level configuration
 */
export interface AppConfig {
  /** Application name */
  name: string;
  /** Application version in semver format */
  version: string;
  /** Deployment environment (dev, staging, production) */
  environment: string;
  /** Default locale for internationalization */
  defaultLocale: string;
  /** Default theme setting */
  defaultTheme: string;
}

/**
 * API-related configuration
 */
export interface ApiConfig {
  /** Base URL for API requests */
  baseUrl: string;
  /** Default request timeout in milliseconds */
  timeout: number;
  /** Map of endpoint names to their paths */
  endpoints: Record<string, string>;
  /** Number of retry attempts for failed requests */
  retryAttempts: number;
  /** Delay between retry attempts in milliseconds */
  retryDelay: number;
}

/**
 * Authentication configuration
 */
export interface AuthConfig {
  /** Whether authentication is enabled */
  enabled: boolean;
  /** URL to redirect for login */
  loginUrl: string;
  /** Local storage key for auth token */
  tokenStorageKey: string;
  /** Token expiry time in seconds */
  tokenExpiryTime: number;
}

/**
 * UI-related configuration
 */
export interface UIConfig {
  /** Default page size for paginated lists */
  defaultPageSize: number;
  /** Available page size options */
  pageSizeOptions: number[];
  /** Default date format */
  dateFormat: string;
  /** Default time format */
  timeFormat: string;
  /** Default datetime format */
  datetimeFormat: string;
  /** Default timezone */
  defaultTimezone: string;
}

/**
 * Feature flag configuration
 */
export interface FeatureFlags {
  /** Enable WebSocket connections for real-time updates */
  enableWebsockets: boolean;
  /** Enable in-app notifications */
  enableNotifications: boolean;
  /** Enable dark mode */
  enableDarkMode: boolean;
  /** Enable usage analytics */
  enableAnalytics: boolean;
  /** Enable advanced chart visualizations */
  enableAdvancedCharts: boolean;
}

/**
 * Monitoring and alerting configuration
 */
export interface MonitoringConfig {
  /** Default refresh interval in milliseconds */
  refreshInterval: number;
  /** Available refresh interval options */
  refreshIntervalOptions: Record<string, number>;
  /** Default time range for monitoring views */
  defaultTimeRange: string;
  /** Color mapping for alert severity levels */
  alertSeverityColors: Record<string, string>;
  /** Color mapping for status indicators */
  statusColors: Record<string, string>;
}

/**
 * Data quality configuration
 */
export interface QualityConfig {
  /** Quality score thresholds for different status levels */
  scoreThresholds: Record<string, number>;
  /** Available quality dimensions */
  dimensions: string[];
  /** Default rule types */
  defaultRuleTypes: string[];
}

/**
 * Self-healing configuration
 */
export interface HealingConfig {
  /** Self-healing mode (automatic, semi-automatic, manual) */
  mode: string;
  /** Confidence thresholds for different actions */
  confidenceThresholds: Record<string, number>;
  /** Maximum number of retry attempts */
  maxRetryAttempts: number;
  /** Whether approval is required for healing actions */
  approvalRequired: boolean;
  /** Whether learning mode is enabled */
  learningMode: boolean;
}

/**
 * Chart and visualization configuration
 */
export interface ChartConfig {
  /** Default chart height in pixels */
  defaultHeight: number;
  /** Animation duration in milliseconds */
  animationDuration: number;
  /** Tooltip display delay in milliseconds */
  tooltipDelay: number;
  /** Default legend position */
  legendPosition: string;
  /** Named colors for specific metrics */
  colors: Record<string, string>;
  /** Default color palette for charts */
  colorPalette: string[];
}

/**
 * Source system configuration
 */
export interface SourceSystem {
  /** Unique identifier */
  sourceId: ID;
  /** Display name */
  name: string;
  /** Source type (GCS, CloudSQL, API, etc.) */
  sourceType: string;
  /** Connection details as JSON object */
  connectionDetails: JSONObject;
  /** Schema version */
  schemaVersion: Optional<string>;
  /** Optional description */
  description: Optional<string>;
  /** Whether the source is active */
  isActive: boolean;
  /** Schema definition, if available */
  schemaDefinition: Optional<JSONObject>;
  /** Extraction settings */
  extractionSettings: Optional<JSONObject>;
  /** Creation timestamp */
  createdAt: Timestamp;
  /** Last update timestamp */
  updatedAt: Timestamp;
  /** Current status */
  status: Optional<string>;
}

/**
 * Pipeline configuration
 */
export interface PipelineConfig {
  /** Unique identifier */
  pipelineId: ID;
  /** Display name */
  name: string;
  /** Source system ID */
  sourceId: ID;
  /** Target BigQuery dataset */
  targetDataset: string;
  /** Target BigQuery table */
  targetTable: string;
  /** Optional description */
  description: Optional<string>;
  /** Cron schedule expression */
  schedule: Optional<string>;
  /** Pipeline-specific configuration */
  configuration: JSONObject;
  /** Whether the pipeline is active */
  isActive: boolean;
  /** Creation timestamp */
  createdAt: Timestamp;
  /** Last update timestamp */
  updatedAt: Timestamp;
  /** Current status */
  status: Optional<PipelineStatus>;
}

/**
 * Validation rule configuration
 */
export interface ValidationRuleConfig {
  /** Unique identifier */
  ruleId: ID;
  /** Rule name */
  name: string;
  /** Target dataset */
  targetDataset: string;
  /** Target table */
  targetTable: string;
  /** Rule type */
  ruleType: string;
  /** Great Expectations expectation type */
  expectationType: string;
  /** Rule definition */
  ruleDefinition: JSONObject;
  /** Alert severity */
  severity: AlertSeverity;
  /** Whether the rule is active */
  isActive: boolean;
  /** Optional description */
  description: Optional<string>;
  /** Creation timestamp */
  createdAt: Timestamp;
  /** Last update timestamp */
  updatedAt: Timestamp;
}

/**
 * Email notification settings
 */
export interface EmailSettings {
  /** Primary recipients */
  recipients: string[];
  /** CC recipients */
  ccRecipients: Optional<string[]>;
  /** Subject line prefix */
  subjectPrefix: Optional<string>;
}

/**
 * Notification configuration
 */
export interface NotificationConfig {
  /** Microsoft Teams webhook URL */
  teamsWebhookUrl: Optional<string>;
  /** Email notification settings */
  emailSettings: Optional<EmailSettings>;
  /** Enabled notification channels */
  enabledChannels: Record<string, boolean>;
  /** Alert thresholds for different metrics */
  alertThresholds: Record<string, AlertSeverity>;
  /** Last update timestamp */
  updatedAt: Timestamp;
}

/**
 * Self-healing action configuration
 */
export interface HealingActionConfig {
  /** Unique identifier */
  actionId: ID;
  /** Related issue pattern ID */
  patternId: ID;
  /** Action type */
  actionType: string;
  /** Action definition */
  actionDefinition: JSONObject;
  /** Whether the action is active */
  isActive: boolean;
  /** Optional description */
  description: Optional<string>;
  /** Historical success rate */
  successRate: number;
  /** Creation timestamp */
  createdAt: Timestamp;
  /** Last update timestamp */
  updatedAt: Timestamp;
}

/**
 * Issue pattern configuration
 */
export interface IssuePatternConfig {
  /** Unique identifier */
  patternId: ID;
  /** Issue type */
  issueType: string;
  /** Detection pattern */
  detectionPattern: JSONObject;
  /** Confidence threshold */
  confidenceThreshold: number;
  /** Optional description */
  description: Optional<string>;
  /** Creation timestamp */
  createdAt: Timestamp;
  /** Last update timestamp */
  updatedAt: Timestamp;
}

/**
 * Performance optimization configuration
 */
export interface OptimizationConfig {
  /** Query optimization settings */
  queryOptimizationSettings: JSONObject;
  /** Schema optimization settings */
  schemaOptimizationSettings: JSONObject;
  /** Resource optimization settings */
  resourceOptimizationSettings: JSONObject;
  /** Whether automatic implementation is enabled */
  autoImplementationEnabled: boolean;
  /** Last update timestamp */
  updatedAt: Timestamp;
}

/**
 * Main configuration interface that combines all configuration sections
 */
export interface Config {
  /** Application configuration */
  app: AppConfig;
  /** API configuration */
  api: ApiConfig;
  /** Authentication configuration */
  auth: AuthConfig;
  /** UI configuration */
  ui: UIConfig;
  /** Feature flags */
  features: FeatureFlags;
  /** Monitoring configuration */
  monitoring: MonitoringConfig;
  /** Quality configuration */
  quality: QualityConfig;
  /** Self-healing configuration */
  healing: HealingConfig;
  /** Chart configuration */
  charts: ChartConfig;
}