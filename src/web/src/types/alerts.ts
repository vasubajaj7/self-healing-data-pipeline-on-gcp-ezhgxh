/**
 * TypeScript type definitions for the alert management system in the self-healing data pipeline web application.
 * This file defines interfaces, types, and enums related to alerts, notifications, and alert management functionality.
 */

import { 
  ID, 
  Timestamp, 
  ISO8601Date, 
  JSONObject, 
  Nullable, 
  Optional, 
  AlertSeverity 
} from './global';

/**
 * Enum defining different alert types based on origin and characteristics
 */
export enum AlertType {
  DATA_QUALITY = 'DATA_QUALITY',     // Alerts related to data quality validation failures
  PIPELINE_FAILURE = 'PIPELINE_FAILURE', // Alerts indicating pipeline component failures
  SYSTEM_HEALTH = 'SYSTEM_HEALTH',   // Alerts about system health and resource usage
  PERFORMANCE = 'PERFORMANCE',       // Alerts about performance degradation
  SECURITY = 'SECURITY'              // Alerts related to security issues
}

/**
 * Enum defining the possible states of an alert during its lifecycle
 */
export enum AlertStatus {
  ACTIVE = 'ACTIVE',                 // New alert that hasn't been acknowledged
  ACKNOWLEDGED = 'ACKNOWLEDGED',     // Alert has been acknowledged but not resolved
  RESOLVED = 'RESOLVED',             // Alert has been resolved
  ESCALATED = 'ESCALATED',           // Alert has been escalated to higher priority
  SUPPRESSED = 'SUPPRESSED'          // Alert is temporarily suppressed
}

/**
 * Enum defining supported notification channels
 */
export enum NotificationChannel {
  TEAMS = 'TEAMS',                   // Microsoft Teams notifications
  EMAIL = 'EMAIL',                   // Email notifications
  SMS = 'SMS'                        // SMS text message notifications
}

/**
 * Interface defining the structure of an alert
 */
export interface Alert {
  alertId: ID;                           // Unique identifier for the alert
  executionId?: Optional<ID>;            // Optional ID of the pipeline execution that triggered the alert
  alertType: AlertType;                  // Type of the alert
  severity: AlertSeverity;               // Severity level of the alert
  status: AlertStatus;                   // Current status of the alert
  message: string;                       // Human-readable alert message
  details?: Optional<JSONObject>;        // Additional structured details about the alert
  source: string;                        // System component that generated the alert
  component: string;                     // Specific component within the source
  createdAt: Timestamp;                  // When the alert was created
  acknowledgedBy?: Optional<string>;     // User who acknowledged the alert
  acknowledgedAt?: Optional<Timestamp>;  // When the alert was acknowledged
  resolvedBy?: Optional<string>;         // User who resolved the alert
  resolvedAt?: Optional<Timestamp>;      // When the alert was resolved
  escalatedBy?: Optional<string>;        // User who escalated the alert
  escalatedAt?: Optional<Timestamp>;     // When the alert was escalated
  escalationLevel?: Optional<string>;    // Current escalation level if escalated
  relatedAlerts?: Optional<ID[]>;        // IDs of related alerts
  selfHealingStatus?: Optional<string>;  // Status of any self-healing attempts
}

/**
 * Interface defining parameters to filter alerts
 */
export interface AlertFilter {
  severity?: Optional<AlertSeverity[]>;   // Filter by severity levels
  status?: Optional<AlertStatus[]>;       // Filter by status
  type?: Optional<AlertType[]>;           // Filter by alert type
  source?: Optional<string>;              // Filter by source
  component?: Optional<string>;           // Filter by component
  startDate?: Optional<ISO8601Date>;      // Filter by start date
  endDate?: Optional<ISO8601Date>;        // Filter by end date
  search?: Optional<string>;              // Search term for text search
}

/**
 * Interface for alert statistics data
 */
export interface AlertStats {
  critical: number;                              // Count of critical alerts
  high: number;                                  // Count of high severity alerts
  medium: number;                                // Count of medium severity alerts
  low: number;                                   // Count of low severity alerts
  total: number;                                 // Total number of alerts
  trend: Array<{                                 // Time series data for alerts
    timestamp: Timestamp;                        // Timestamp for data point
    count: number;                               // Alert count at this time
    severity?: AlertSeverity;                    // Optional severity breakdown
  }>;
}

/**
 * Interface for notification configuration
 */
export interface NotificationConfig {
  channels: Record<NotificationChannel, boolean>;           // Enabled/disabled status for each channel
  teamsWebhookUrl?: Optional<string>;                       // Microsoft Teams webhook URL
  emailConfig?: Optional<EmailConfig>;                      // Email notification configuration
  smsConfig?: Optional<SMSConfig>;                          // SMS notification configuration
  alertThresholds: Record<AlertSeverity, NotificationThreshold>; // Threshold settings by severity
  updatedAt: Timestamp;                                     // Last update timestamp
}

/**
 * Interface for email notification configuration
 */
export interface EmailConfig {
  recipients: string[];                         // List of email recipients
  subjectPrefix?: Optional<string>;             // Optional prefix for email subjects
  includeDetails: boolean;                      // Whether to include full alert details
}

/**
 * Interface for SMS notification configuration
 */
export interface SMSConfig {
  phoneNumbers: string[];                       // List of phone numbers
  criticalOnly: boolean;                        // Whether to send only critical alerts
}

/**
 * Interface for notification threshold configuration
 */
export interface NotificationThreshold {
  enabled: boolean;                             // Whether notifications are enabled
  minInterval: number;                          // Minimum interval between notifications (minutes)
  batchSize?: Optional<number>;                 // Number of alerts to batch (if supported)
}

/**
 * Interface for AI-suggested actions for alert resolution
 */
export interface SuggestedAction {
  actionId: string;                             // Unique identifier for the action
  description: string;                          // Human-readable description
  actionType: string;                           // Type of action
  confidence: number;                           // Confidence score (0-1)
  parameters?: Optional<JSONObject>;            // Optional parameters for the action
  estimatedImpact?: Optional<string>;           // Description of estimated impact
}

/**
 * Interface for alert acknowledgement data
 */
export interface AlertAcknowledgement {
  acknowledgedBy: string;                       // User acknowledging the alert
  comments?: Optional<string>;                  // Optional comments
}

/**
 * Interface for alert escalation data
 */
export interface AlertEscalation {
  escalatedBy: string;                          // User escalating the alert
  escalationReason: string;                     // Reason for escalation
  escalationLevel: string;                      // Target escalation level
}

/**
 * Interface for alert resolution data
 */
export interface AlertResolution {
  resolvedBy: string;                           // User resolving the alert
  resolutionNotes: string;                      // Notes about the resolution
}

/**
 * Interface for alert suppression data
 */
export interface AlertSuppression {
  suppressedBy: string;                         // User suppressing the alert
  durationMinutes: number;                      // Duration of suppression in minutes
  suppressionReason: string;                    // Reason for suppression
}

/**
 * Interface for notification channel status information
 */
export interface NotificationChannelStatus {
  teams: boolean;                               // Teams channel status
  email: boolean;                               // Email channel status
  sms: boolean;                                 // SMS channel status
  configured: string[];                         // List of configured channels
}