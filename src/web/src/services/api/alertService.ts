/**
 * Alert Management Service
 * 
 * This service provides methods for interacting with the alert management API endpoints.
 * It enables retrieving alerts, managing alert status (acknowledge, escalate, resolve),
 * configuring notification settings, and accessing AI-suggested actions.
 */

import apiClient from './apiClient';
import { AxiosError } from 'axios'; // axios ^1.4.0
import { 
  Alert, 
  AlertFilter, 
  NotificationConfig, 
  AlertAcknowledgement,
  AlertEscalation,
  AlertResolution,
  AlertSuppression,
  AlertStats,
  SuggestedAction
} from '../../types/alerts';
import { PaginatedResponse } from '../../types/api';

/**
 * Retrieves a list of alerts based on provided filters
 * @param filter - Filtering criteria for alerts
 * @param page - Page number for pagination
 * @param pageSize - Number of items per page
 * @returns Paginated list of alerts matching the filter criteria
 */
const getAlerts = async (
  filter: AlertFilter = {}, 
  page: number = 1, 
  pageSize: number = 20
): Promise<PaginatedResponse<Alert>> => {
  // Convert filter object to query parameters
  const queryParams = new URLSearchParams();
  
  // Add pagination parameters
  queryParams.append('page', page.toString());
  queryParams.append('pageSize', pageSize.toString());
  
  // Add filter parameters if they exist
  if (filter.severity) {
    filter.severity.forEach(severity => queryParams.append('severity', severity));
  }
  if (filter.status) {
    filter.status.forEach(status => queryParams.append('status', status));
  }
  if (filter.type) {
    filter.type.forEach(type => queryParams.append('type', type));
  }
  if (filter.source) {
    queryParams.append('source', filter.source);
  }
  if (filter.component) {
    queryParams.append('component', filter.component);
  }
  if (filter.startDate) {
    queryParams.append('startDate', filter.startDate);
  }
  if (filter.endDate) {
    queryParams.append('endDate', filter.endDate);
  }
  if (filter.search) {
    queryParams.append('search', filter.search);
  }
  
  // Make the API request
  return apiClient.get<PaginatedResponse<Alert>>(`/alerts?${queryParams.toString()}`);
};

/**
 * Retrieves a specific alert by its ID
 * @param alertId - Unique identifier of the alert
 * @returns The alert details
 */
const getAlertById = async (alertId: string): Promise<Alert> => {
  return apiClient.get<Alert>(`/alerts/${alertId}`);
};

/**
 * Acknowledges an alert to indicate it's being handled
 * @param alertId - Unique identifier of the alert
 * @param acknowledgedBy - User acknowledging the alert
 * @param comments - Optional comments about acknowledgment
 * @returns The updated alert with acknowledged status
 */
const acknowledgeAlert = async (
  alertId: string, 
  acknowledgedBy: string, 
  comments?: string
): Promise<Alert> => {
  const payload: AlertAcknowledgement = {
    acknowledgedBy,
    comments
  };
  
  return apiClient.put<Alert>(`/alerts/${alertId}/acknowledge`, payload);
};

/**
 * Escalates an alert to a higher priority level or team
 * @param alertId - Unique identifier of the alert
 * @param escalatedBy - User escalating the alert
 * @param escalationReason - Reason for escalation
 * @param escalationLevel - Target escalation level
 * @returns The updated alert with escalated status
 */
const escalateAlert = async (
  alertId: string, 
  escalatedBy: string, 
  escalationReason: string, 
  escalationLevel: string
): Promise<Alert> => {
  const payload: AlertEscalation = {
    escalatedBy,
    escalationReason,
    escalationLevel
  };
  
  return apiClient.put<Alert>(`/alerts/${alertId}/escalate`, payload);
};

/**
 * Marks an alert as resolved
 * @param alertId - Unique identifier of the alert
 * @param resolvedBy - User resolving the alert
 * @param resolutionNotes - Notes about how the alert was resolved
 * @returns The updated alert with resolved status
 */
const resolveAlert = async (
  alertId: string, 
  resolvedBy: string, 
  resolutionNotes: string
): Promise<Alert> => {
  const payload: AlertResolution = {
    resolvedBy,
    resolutionNotes
  };
  
  return apiClient.put<Alert>(`/alerts/${alertId}/resolve`, payload);
};

/**
 * Suppresses similar alerts to reduce alert noise
 * @param alertId - Unique identifier of the alert to use as a reference
 * @param suppressedBy - User suppressing the alerts
 * @param durationMinutes - Duration of suppression in minutes
 * @param suppressionReason - Reason for suppression
 * @returns Result of the suppression operation
 */
const suppressSimilarAlerts = async (
  alertId: string, 
  suppressedBy: string, 
  durationMinutes: number, 
  suppressionReason: string
): Promise<{ success: boolean; message: string }> => {
  const payload: AlertSuppression = {
    suppressedBy,
    durationMinutes,
    suppressionReason
  };
  
  return apiClient.post<{ success: boolean; message: string }>(
    `/alerts/${alertId}/suppress-similar`, 
    payload
  );
};

/**
 * Retrieves historical alerts based on provided filters
 * @param filter - Filtering criteria for historical alerts
 * @param page - Page number for pagination
 * @param pageSize - Number of items per page
 * @returns Paginated list of historical alerts
 */
const getAlertHistory = async (
  filter: AlertFilter = {}, 
  page: number = 1, 
  pageSize: number = 20
): Promise<PaginatedResponse<Alert>> => {
  // Convert filter object to query parameters
  const queryParams = new URLSearchParams();
  
  // Add pagination parameters
  queryParams.append('page', page.toString());
  queryParams.append('pageSize', pageSize.toString());
  
  // Add filter parameters if they exist
  if (filter.severity) {
    filter.severity.forEach(severity => queryParams.append('severity', severity));
  }
  if (filter.status) {
    filter.status.forEach(status => queryParams.append('status', status));
  }
  if (filter.type) {
    filter.type.forEach(type => queryParams.append('type', type));
  }
  if (filter.source) {
    queryParams.append('source', filter.source);
  }
  if (filter.component) {
    queryParams.append('component', filter.component);
  }
  if (filter.startDate) {
    queryParams.append('startDate', filter.startDate);
  }
  if (filter.endDate) {
    queryParams.append('endDate', filter.endDate);
  }
  if (filter.search) {
    queryParams.append('search', filter.search);
  }
  
  // Make the API request
  return apiClient.get<PaginatedResponse<Alert>>(`/alerts/history?${queryParams.toString()}`);
};

/**
 * Retrieves alert statistics for dashboards
 * @param timeRange - Time range for statistics (e.g., '24h', '7d', '30d')
 * @returns Alert statistics including counts by severity and trend data
 */
const getAlertStats = async (
  timeRange: string = '24h'
): Promise<AlertStats> => {
  return apiClient.get<AlertStats>(`/alerts/stats?timeRange=${timeRange}`);
};

/**
 * Retrieves configured notification channels
 * @returns Status of notification channels
 */
const getNotificationChannels = async (): Promise<{ 
  teams: boolean; 
  email: boolean; 
  sms: boolean; 
  configured: string[] 
}> => {
  return apiClient.get<{ 
    teams: boolean; 
    email: boolean; 
    sms: boolean; 
    configured: string[] 
  }>('/alerts/notification-channels');
};

/**
 * Updates notification configuration settings
 * @param config - New notification configuration
 * @returns The updated notification configuration
 */
const updateNotificationConfig = async (
  config: NotificationConfig
): Promise<NotificationConfig> => {
  return apiClient.put<NotificationConfig>('/alerts/notification-config', config);
};

/**
 * Retrieves alerts related to a specific alert
 * @param alertId - Unique identifier of the reference alert
 * @returns List of related alerts
 */
const getRelatedAlerts = async (alertId: string): Promise<Alert[]> => {
  return apiClient.get<Alert[]>(`/alerts/${alertId}/related`);
};

/**
 * Retrieves AI-suggested actions for an alert
 * @param alertId - Unique identifier of the alert
 * @returns List of suggested actions
 */
const getSuggestedActions = async (
  alertId: string
): Promise<{ actions: SuggestedAction[] }> => {
  return apiClient.get<{ actions: SuggestedAction[] }>(
    `/alerts/${alertId}/suggested-actions`
  );
};

/**
 * Alert Service - Provides methods for interacting with the alert management API
 */
const alertService = {
  getAlerts,
  getAlertById,
  acknowledgeAlert,
  escalateAlert,
  resolveAlert,
  suppressSimilarAlerts,
  getAlertHistory,
  getAlertStats,
  getNotificationChannels,
  updateNotificationConfig,
  getRelatedAlerts,
  getSuggestedActions
};

export default alertService;