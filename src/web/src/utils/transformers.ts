/**
 * Utility functions for transforming data between different formats in the self-healing data pipeline web application.
 * Primarily handles conversion between API response formats and UI-friendly data structures,
 * ensuring consistent data representation across the application.
 */

import {
  PipelineStatus, AlertSeverity, QualityStatus, HealingStatus, Timestamp, ISO8601Date
} from '../types/global';

import {
  PipelineDefinition, PipelineExecution, TaskExecution, QualityRule, QualityValidation,
  Alert, HealingExecution, HealingAction, IssuePattern
} from '../types/api';

import { formatDate, formatDateTime, formatTime } from '../utils/date';
import { format, parseISO } from 'date-fns'; // v2.30.0

/**
 * Transforms raw pipeline definition data from API to UI-friendly format
 * @param pipeline - The pipeline definition object from API
 * @returns Transformed pipeline data with formatted dates and status indicators
 */
export function transformPipelineData(pipeline: PipelineDefinition) {
  return {
    ...pipeline,
    createdAt: formatDateTime(pipeline.createdAt),
    updatedAt: formatDateTime(pipeline.updatedAt),
    lastExecutionTime: pipeline.lastExecutionTime ? formatDateTime(pipeline.lastExecutionTime) : 'Never',
    
    // Transform status values
    statusLabel: pipeline.lastExecutionStatus ? 
      pipeline.lastExecutionStatus.charAt(0) + pipeline.lastExecutionStatus.slice(1).toLowerCase() : 
      'Not executed',
    statusColor: transformStatusToColor(pipeline.lastExecutionStatus || ''),
    
    // Format for UI display
    isActiveLabel: pipeline.isActive ? 'Active' : 'Inactive',
    sourceName: pipeline.sourceName || 'Unknown source',
    
    // Derived fields
    targetLocation: `${pipeline.targetDataset}.${pipeline.targetTable}`,
    configSummary: summarizeConfig(pipeline.configuration),
  };
}

/**
 * Helper function to summarize configuration object
 * @param config - Configuration object
 * @returns Summarized string representation
 */
function summarizeConfig(config: any): string {
  if (!config) return 'No configuration';
  
  try {
    // Extract key information from configuration
    const keys = Object.keys(config);
    if (keys.length === 0) return 'Empty configuration';
    
    // Return a simplified summary based on key properties
    return `${keys.length} configuration properties`;
  } catch (error) {
    return 'Invalid configuration';
  }
}

/**
 * Transforms pipeline execution data from API to UI-friendly format
 * @param execution - The pipeline execution object from API
 * @returns Transformed execution data with formatted dates, durations, and status indicators
 */
export function transformPipelineExecutionData(execution: PipelineExecution) {
  // Calculate duration if both start and end times are available
  let duration = '';
  if (execution.startTime && execution.endTime) {
    const startDate = new Date(execution.startTime);
    const endDate = new Date(execution.endTime);
    const durationMs = endDate.getTime() - startDate.getTime();
    
    // Format duration based on length
    if (durationMs < 60000) { // Less than a minute
      duration = `${Math.round(durationMs / 1000)}s`;
    } else if (durationMs < 3600000) { // Less than an hour
      duration = `${Math.round(durationMs / 60000)}m`;
    } else { // Hours or more
      const hours = Math.floor(durationMs / 3600000);
      const minutes = Math.floor((durationMs % 3600000) / 60000);
      duration = `${hours}h ${minutes}m`;
    }
  } else if (execution.startTime && !execution.endTime) {
    duration = 'In progress';
  }

  return {
    ...execution,
    startTime: formatDateTime(execution.startTime),
    endTime: execution.endTime ? formatDateTime(execution.endTime) : null,
    duration,
    
    // Transform status values
    statusLabel: execution.status.charAt(0) + execution.status.slice(1).toLowerCase(),
    statusColor: transformStatusToColor(execution.status),
    
    // Format other fields
    recordsProcessedFormatted: execution.recordsProcessed !== undefined ? 
      execution.recordsProcessed.toLocaleString() : 'N/A',
    
    // Derived fields
    hasError: !!execution.errorDetails,
    taskCount: execution.tasks ? execution.tasks.length : 0,
  };
}

/**
 * Transforms task execution data from API to UI-friendly format
 * @param task - The task execution object from API
 * @returns Transformed task data with formatted dates, durations, and status indicators
 */
export function transformTaskExecutionData(task: TaskExecution) {
  // Calculate duration if both start and end times are available
  let duration = '';
  if (task.startTime && task.endTime) {
    const startDate = new Date(task.startTime);
    const endDate = new Date(task.endTime);
    const durationMs = endDate.getTime() - startDate.getTime();
    
    // Format duration based on length
    if (durationMs < 60000) { // Less than a minute
      duration = `${Math.round(durationMs / 1000)}s`;
    } else if (durationMs < 3600000) { // Less than an hour
      duration = `${Math.round(durationMs / 60000)}m`;
    } else { // Hours or more
      const hours = Math.floor(durationMs / 3600000);
      const minutes = Math.floor((durationMs % 3600000) / 60000);
      duration = `${hours}h ${minutes}m`;
    }
  } else if (task.startTime && !task.endTime) {
    duration = 'In progress';
  }

  // Format task type for display
  const taskTypeFormatted = task.taskType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');

  return {
    ...task,
    startTime: formatDateTime(task.startTime),
    endTime: task.endTime ? formatDateTime(task.endTime) : null,
    duration,
    
    // Transform status values
    statusLabel: task.status.charAt(0) + task.status.slice(1).toLowerCase(),
    statusColor: transformStatusToColor(task.status),
    
    // Format other fields
    taskTypeFormatted,
    retryCountFormatted: task.retryCount > 0 ? `${task.retryCount} retries` : 'No retries',
    
    // Derived fields
    hasError: !!task.errorDetails,
    errorSummary: task.errorDetails ? 
      (task.errorDetails.length > 100 ? `${task.errorDetails.substring(0, 100)}...` : task.errorDetails) :
      null,
  };
}

/**
 * Transforms quality rule data from API to UI-friendly format
 * @param rule - The quality rule object from API
 * @returns Transformed rule data with formatted dates and user-friendly descriptions
 */
export function transformQualityRuleData(rule: QualityRule) {
  // Format rule type and expectation type for display
  const ruleTypeFormatted = rule.ruleType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
  
  const expectationTypeFormatted = rule.expectationType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
  
  // Generate human-readable rule description if not provided
  let ruleDescription = rule.description;
  if (!ruleDescription) {
    ruleDescription = `${expectationTypeFormatted} rule for ${rule.targetDataset}.${rule.targetTable}`;
  }

  return {
    ...rule,
    createdAt: formatDateTime(rule.createdAt),
    updatedAt: formatDateTime(rule.updatedAt),
    
    // Format fields for display
    ruleTypeFormatted,
    expectationTypeFormatted,
    severityLabel: rule.severity.charAt(0) + rule.severity.slice(1).toLowerCase(),
    severityColor: transformStatusToColor(rule.severity),
    ruleDescription,
    
    // Derived fields
    isActiveLabel: rule.isActive ? 'Active' : 'Inactive',
    targetFull: `${rule.targetDataset}.${rule.targetTable}`,
  };
}

/**
 * Transforms quality validation data from API to UI-friendly format
 * @param validation - The quality validation object from API
 * @returns Transformed validation data with formatted dates and status indicators
 */
export function transformQualityValidationData(validation: QualityValidation) {
  return {
    ...validation,
    validationTime: formatDateTime(validation.validationTime),
    
    // Transform status values
    statusLabel: validation.status.charAt(0) + validation.status.slice(1).toLowerCase(),
    statusColor: transformStatusToColor(validation.status),
    
    // Format healing status if available
    healingStatusLabel: validation.selfHealingStatus ? 
      validation.selfHealingStatus.split('_').map(word => 
        word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
      ).join(' ') : 
      'Not applicable',
    healingStatusColor: validation.selfHealingStatus ? 
      transformStatusToColor(validation.selfHealingStatus) : 
      '#999999',
    
    // Format other fields
    failedRecordsFormatted: validation.failedRecords !== undefined ? 
      validation.failedRecords.toLocaleString() : 
      'N/A',
    
    // Derived fields
    hasDetails: !!validation.details && Object.keys(validation.details).length > 0,
    detailsSummary: validation.details ? 
      `${Object.keys(validation.details).length} detail items available` : 
      'No details available',
  };
}

/**
 * Transforms alert data from API to UI-friendly format
 * @param alert - The alert object from API
 * @returns Transformed alert data with formatted dates, severity indicators, and status
 */
export function transformAlertData(alert: Alert) {
  return {
    ...alert,
    createdAt: formatDateTime(alert.createdAt),
    acknowledgedAt: alert.acknowledgedAt ? formatDateTime(alert.acknowledgedAt) : null,
    
    // Transform severity level
    severityLabel: alert.severity.charAt(0) + alert.severity.slice(1).toLowerCase(),
    severityColor: transformStatusToColor(alert.severity),
    
    // Format acknowledgement status
    acknowledgedLabel: alert.acknowledged ? 'Acknowledged' : 'Not acknowledged',
    acknowledgedByFormatted: alert.acknowledgedBy || 'N/A',
    
    // Format other fields
    alertTypeFormatted: alert.alertType
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' '),
    
    // Derived fields
    hasDetails: !!alert.details && Object.keys(alert.details).length > 0,
    hasRelatedAlerts: !!alert.relatedAlerts && alert.relatedAlerts.length > 0,
    relatedAlertCount: alert.relatedAlerts ? alert.relatedAlerts.length : 0,
  };
}

/**
 * Transforms healing execution data from API to UI-friendly format
 * @param healing - The healing execution object from API
 * @returns Transformed healing data with formatted dates, confidence scores, and status indicators
 */
export function transformHealingExecutionData(healing: HealingExecution) {
  return {
    ...healing,
    executionTime: formatDateTime(healing.executionTime),
    
    // Format confidence score as percentage
    confidencePercentage: `${Math.round(healing.confidence * 100)}%`,
    
    // Transform healing status
    statusLabel: healing.status.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    ).join(' '),
    statusColor: transformStatusToColor(healing.status),
    
    // Format other fields
    successLabel: healing.successful ? 'Successful' : 'Failed',
    successColor: healing.successful ? '#28a745' : '#dc3545',
    
    // Derived fields
    hasError: !!healing.errorMessage,
    errorSummary: healing.errorMessage ? 
      (healing.errorMessage.length > 100 ? `${healing.errorMessage.substring(0, 100)}...` : healing.errorMessage) : 
      null,
    hasDetails: !!healing.executionDetails && Object.keys(healing.executionDetails).length > 0,
  };
}

/**
 * Transforms healing action data from API to UI-friendly format
 * @param action - The healing action object from API
 * @returns Transformed action data with formatted dates, success rates, and user-friendly descriptions
 */
export function transformHealingActionData(action: HealingAction) {
  // Format action type for display
  const actionTypeFormatted = action.actionType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
  
  // Generate human-readable action description if not provided
  let actionDescription = action.description;
  if (!actionDescription) {
    actionDescription = `${actionTypeFormatted} action for pattern ${action.patternId}`;
  }

  return {
    ...action,
    createdAt: formatDateTime(action.createdAt),
    updatedAt: formatDateTime(action.updatedAt),
    
    // Format success rate as percentage
    successRatePercentage: `${Math.round(action.successRate * 100)}%`,
    
    // Format for UI display
    actionTypeFormatted,
    isActiveLabel: action.isActive ? 'Active' : 'Inactive',
    actionDescription,
    
    // Color coding based on success rate
    successRateColor: getSuccessRateColor(action.successRate),
    
    // Derived fields
    hasMetadata: !!action.metadata && Object.keys(action.metadata).length > 0,
  };
}

/**
 * Helper function to get color based on success rate
 * @param rate - Success rate as a fraction
 * @returns Color code for the success rate
 */
function getSuccessRateColor(rate: number): string {
  if (rate >= 0.9) return '#28a745'; // High - green
  if (rate >= 0.7) return '#ffc107'; // Medium - yellow
  return '#dc3545'; // Low - red
}

/**
 * Transforms issue pattern data from API to UI-friendly format
 * @param pattern - The issue pattern object from API
 * @returns Transformed pattern data with formatted dates, confidence thresholds, and user-friendly descriptions
 */
export function transformIssuePatternData(pattern: IssuePattern) {
  // Format issue type for display
  const issueTypeFormatted = pattern.issueType
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
  
  // Generate human-readable pattern description if not provided
  let patternDescription = pattern.description;
  if (!patternDescription) {
    patternDescription = `Detection pattern for ${issueTypeFormatted} issues`;
  }

  return {
    ...pattern,
    createdAt: formatDateTime(pattern.createdAt),
    updatedAt: formatDateTime(pattern.updatedAt),
    
    // Format confidence threshold as percentage
    confidenceThresholdPercentage: `${Math.round(pattern.confidenceThreshold * 100)}%`,
    
    // Format for UI display
    issueTypeFormatted,
    patternDescription,
    
    // Derived fields
    complexityLevel: getPatternComplexity(pattern.detectionPattern),
    hasMetadata: !!pattern.metadata && Object.keys(pattern.metadata).length > 0,
  };
}

/**
 * Helper function to determine pattern complexity based on the detection pattern structure
 * @param detectionPattern - The pattern definition object
 * @returns String indicating the complexity level
 */
function getPatternComplexity(detectionPattern: any): string {
  if (!detectionPattern) return 'Unknown';
  
  try {
    const keys = Object.keys(detectionPattern);
    const depth = getObjectDepth(detectionPattern);
    
    if (depth <= 1 && keys.length <= 3) return 'Simple';
    if (depth <= 3 && keys.length <= 10) return 'Moderate';
    return 'Complex';
  } catch (error) {
    return 'Unknown';
  }
}

/**
 * Helper function to calculate the depth of a nested object
 * @param obj - The object to analyze
 * @returns The maximum depth of the object
 */
function getObjectDepth(obj: any): number {
  if (typeof obj !== 'object' || obj === null) return 0;
  
  let maxDepth = 1;
  for (const key in obj) {
    if (typeof obj[key] === 'object' && obj[key] !== null) {
      const depth = getObjectDepth(obj[key]) + 1;
      maxDepth = Math.max(maxDepth, depth);
    }
  }
  
  return maxDepth;
}

/**
 * Transforms a date to ISO8601 format for API requests
 * @param date - The date to format
 * @returns Date in ISO8601 format for API consumption
 */
export function transformDateForApi(date: Date | string | number): ISO8601Date {
  if (!date) {
    return '';
  }
  
  // Convert to Date object if it's a string or number
  const dateObj = typeof date === 'object' ? date : new Date(date);
  
  // Format to ISO8601 date (YYYY-MM-DD)
  return format(dateObj, 'yyyy-MM-dd');
}

/**
 * Transforms a date and time to ISO8601 format for API requests
 * @param dateTime - The date and time to format
 * @returns Date and time in ISO8601 format for API consumption
 */
export function transformDateTimeForApi(dateTime: Date | string | number): Timestamp {
  if (!dateTime) {
    return '';
  }
  
  // Convert to Date object if it's a string or number
  const dateObj = typeof dateTime === 'object' ? dateTime : new Date(dateTime);
  
  // Format to ISO8601 datetime with timezone (YYYY-MM-DDTHH:mm:ss.sssZ)
  return format(dateObj, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
}

/**
 * Maps a status value to its corresponding color code for UI display
 * @param status - The status value to map
 * @returns Color code (hex or named color) for the given status
 */
export function transformStatusToColor(
  status: PipelineStatus | QualityStatus | HealingStatus | AlertSeverity | string
): string {
  // Default color if status doesn't match any known value
  let color = '#6c757d'; // gray

  // Pipeline status colors
  if (Object.values(PipelineStatus).includes(status as PipelineStatus)) {
    switch (status) {
      case PipelineStatus.HEALTHY:
        return '#28a745'; // green
      case PipelineStatus.WARNING:
        return '#ffc107'; // yellow
      case PipelineStatus.ERROR:
        return '#dc3545'; // red
      case PipelineStatus.INACTIVE:
        return '#6c757d'; // gray
    }
  }
  
  // Quality status colors
  if (Object.values(QualityStatus).includes(status as QualityStatus)) {
    switch (status) {
      case QualityStatus.PASSED:
        return '#28a745'; // green
      case QualityStatus.WARNING:
        return '#ffc107'; // yellow
      case QualityStatus.FAILED:
        return '#dc3545'; // red
    }
  }
  
  // Healing status colors
  if (Object.values(HealingStatus).includes(status as HealingStatus)) {
    switch (status) {
      case HealingStatus.COMPLETED:
        return '#28a745'; // green
      case HealingStatus.IN_PROGRESS:
        return '#007bff'; // blue
      case HealingStatus.PENDING:
        return '#6c757d'; // gray
      case HealingStatus.FAILED:
        return '#dc3545'; // red
      case HealingStatus.APPROVAL_REQUIRED:
        return '#fd7e14'; // orange
    }
  }
  
  // Alert severity colors
  if (Object.values(AlertSeverity).includes(status as AlertSeverity)) {
    switch (status) {
      case AlertSeverity.CRITICAL:
        return '#dc3545'; // red
      case AlertSeverity.HIGH:
        return '#fd7e14'; // orange
      case AlertSeverity.MEDIUM:
        return '#ffc107'; // yellow
      case AlertSeverity.LOW:
        return '#17a2b8'; // cyan
    }
  }
  
  return color;
}

/**
 * Transforms an array of items into select options format for UI components
 * @param items - The array of items to transform
 * @param labelKey - The property to use as option label
 * @param valueKey - The property to use as option value
 * @returns Array of options with label and value properties
 */
export function transformArrayToOptions<T>(
  items: T[],
  labelKey: keyof T,
  valueKey: keyof T
): Array<{ label: string, value: string | number }> {
  if (!items || !Array.isArray(items)) {
    return [];
  }
  
  return items.map(item => ({
    label: String(item[labelKey] || ''),
    value: String(item[valueKey] || '')
  }));
}

/**
 * Transforms an enum object into select options format for UI components
 * @param enumObject - The enum object to transform
 * @returns Array of options with label and value properties
 */
export function transformEnumToOptions(
  enumObject: object
): Array<{ label: string, value: string | number }> {
  if (!enumObject || typeof enumObject !== 'object') {
    return [];
  }
  
  return Object.entries(enumObject)
    // Filter out numeric keys (TypeScript enum implementation detail)
    .filter(([key]) => isNaN(Number(key)))
    .map(([key, value]) => ({
      label: key
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' '),
      value: value
    }));
}

/**
 * Transforms raw metric data into a format suitable for UI display
 * @param metricData - The raw metric data to transform
 * @returns Transformed metric data with formatted values and labels
 */
export function transformMetricData(metricData: any) {
  if (!metricData) {
    return {};
  }
  
  const result: any = { ...metricData };
  
  // Format metric values based on their types
  Object.keys(result).forEach(key => {
    const value = result[key];
    
    // Skip null or undefined values
    if (value === null || value === undefined) {
      return;
    }
    
    // Format percentage values
    if (key.includes('rate') || key.includes('percentage') || key.includes('ratio')) {
      result[`${key}Formatted`] = `${Math.round(value * 100)}%`;
      // Add color coding based on value
      result[`${key}Color`] = getPercentageColor(value);
    }
    
    // Format duration values
    if (key.includes('duration') || key.includes('time') || key.includes('latency')) {
      if (typeof value === 'number') {
        result[`${key}Formatted`] = formatDuration(value);
      }
    }
    
    // Format count values
    if (key.includes('count') || key.includes('total') || key.includes('number')) {
      if (typeof value === 'number') {
        result[`${key}Formatted`] = value.toLocaleString();
      }
    }
  });
  
  // Transform metric names to user-friendly labels
  Object.keys(result).forEach(key => {
    // Skip already formatted keys
    if (key.endsWith('Formatted') || key.endsWith('Color')) {
      return;
    }
    
    // Create user-friendly label
    result[`${key}Label`] = key
      .split(/(?=[A-Z])/).join(' ') // Split camelCase
      .split('_').join(' ') // Split snake_case
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ');
  });
  
  return result;
}

/**
 * Helper function to get color based on percentage value
 * @param value - Percentage as a fraction
 * @returns Color code for the percentage
 */
function getPercentageColor(value: number): string {
  if (value >= 0.9) return '#28a745'; // High - green
  if (value >= 0.6) return '#ffc107'; // Medium - yellow
  return '#dc3545'; // Low - red
}

/**
 * Helper function to format duration in milliseconds to human-readable string
 * @param durationMs - Duration in milliseconds
 * @returns Formatted duration string
 */
function formatDuration(durationMs: number): string {
  if (durationMs < 1000) {
    return `${durationMs}ms`;
  }
  
  if (durationMs < 60000) {
    return `${Math.round(durationMs / 1000)}s`;
  }
  
  if (durationMs < 3600000) {
    const minutes = Math.floor(durationMs / 60000);
    const seconds = Math.floor((durationMs % 60000) / 1000);
    return `${minutes}m ${seconds}s`;
  }
  
  const hours = Math.floor(durationMs / 3600000);
  const minutes = Math.floor((durationMs % 3600000) / 60000);
  return `${hours}h ${minutes}m`;
}