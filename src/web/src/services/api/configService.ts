/**
 * Configuration Service
 * 
 * This service handles all configuration-related API interactions for the self-healing data pipeline.
 * It provides methods for managing data sources, pipeline configurations, validation rules,
 * notification settings, self-healing parameters, and other configurable aspects of the system.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import { PaginationParams, DataResponse, ListResponse } from '../../types/api';
import { 
  SourceSystem, 
  PipelineConfig, 
  ValidationRuleConfig, 
  NotificationConfig,
  HealingConfig,
  OptimizationConfig,
  HealingActionConfig,
  IssuePatternConfig
} from '../../types/config';
import { logError } from '../../utils/errorHandling';

/**
 * Retrieves a paginated list of data sources with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of data sources
 */
const getDataSources = async (
  params: PaginationParams & { sourceType?: string, isActive?: boolean }
): Promise<ListResponse<SourceSystem>> => {
  try {
    return await apiClient.get<ListResponse<SourceSystem>>(
      `${endpoints.admin.settings}/data-sources`,
      { params }
    );
  } catch (error) {
    logError(error, 'getDataSources');
    throw error;
  }
};

/**
 * Retrieves a single data source by ID
 * @param sourceId - The data source ID
 * @returns Promise resolving to the data source details
 */
const getDataSource = async (sourceId: string): Promise<DataResponse<SourceSystem>> => {
  try {
    return await apiClient.get<DataResponse<SourceSystem>>(
      `${endpoints.admin.settings}/data-sources/${sourceId}`
    );
  } catch (error) {
    logError(error, 'getDataSource');
    throw error;
  }
};

/**
 * Creates a new data source
 * @param sourceData - The data source configuration
 * @returns Promise resolving to the created data source
 */
const createDataSource = async (
  sourceData: Partial<SourceSystem>
): Promise<DataResponse<SourceSystem>> => {
  try {
    return await apiClient.post<DataResponse<SourceSystem>>(
      `${endpoints.admin.settings}/data-sources`,
      sourceData
    );
  } catch (error) {
    logError(error, 'createDataSource');
    throw error;
  }
};

/**
 * Updates an existing data source
 * @param sourceId - The data source ID to update
 * @param sourceData - The updated data source configuration
 * @returns Promise resolving to the updated data source
 */
const updateDataSource = async (
  sourceId: string,
  sourceData: Partial<SourceSystem>
): Promise<DataResponse<SourceSystem>> => {
  try {
    return await apiClient.put<DataResponse<SourceSystem>>(
      `${endpoints.admin.settings}/data-sources/${sourceId}`,
      sourceData
    );
  } catch (error) {
    logError(error, 'updateDataSource');
    throw error;
  }
};

/**
 * Deletes a data source by ID
 * @param sourceId - The data source ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteDataSource = async (
  sourceId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.admin.settings}/data-sources/${sourceId}`
    );
  } catch (error) {
    logError(error, 'deleteDataSource');
    throw error;
  }
};

/**
 * Tests the connection to a data source
 * @param sourceId - The data source ID to test
 * @returns Promise resolving to connection test results
 */
const testDataSourceConnection = async (
  sourceId: string
): Promise<DataResponse<{ success: boolean, message: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ success: boolean, message: string }>>(
      `${endpoints.admin.settings}/data-sources/${sourceId}/test`
    );
  } catch (error) {
    logError(error, 'testDataSourceConnection');
    throw error;
  }
};

/**
 * Retrieves a paginated list of pipeline configurations with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of pipeline configurations
 */
const getPipelineConfigs = async (
  params: PaginationParams & { sourceId?: string, isActive?: boolean }
): Promise<ListResponse<PipelineConfig>> => {
  try {
    return await apiClient.get<ListResponse<PipelineConfig>>(
      `${endpoints.admin.settings}/pipelines`,
      { params }
    );
  } catch (error) {
    logError(error, 'getPipelineConfigs');
    throw error;
  }
};

/**
 * Retrieves a single pipeline configuration by ID
 * @param pipelineId - The pipeline configuration ID
 * @returns Promise resolving to the pipeline configuration details
 */
const getPipelineConfig = async (pipelineId: string): Promise<DataResponse<PipelineConfig>> => {
  try {
    return await apiClient.get<DataResponse<PipelineConfig>>(
      `${endpoints.admin.settings}/pipelines/${pipelineId}`
    );
  } catch (error) {
    logError(error, 'getPipelineConfig');
    throw error;
  }
};

/**
 * Creates a new pipeline configuration
 * @param pipelineData - The pipeline configuration
 * @returns Promise resolving to the created pipeline configuration
 */
const createPipelineConfig = async (
  pipelineData: Partial<PipelineConfig>
): Promise<DataResponse<PipelineConfig>> => {
  try {
    return await apiClient.post<DataResponse<PipelineConfig>>(
      `${endpoints.admin.settings}/pipelines`,
      pipelineData
    );
  } catch (error) {
    logError(error, 'createPipelineConfig');
    throw error;
  }
};

/**
 * Updates an existing pipeline configuration
 * @param pipelineId - The pipeline configuration ID to update
 * @param pipelineData - The updated pipeline configuration
 * @returns Promise resolving to the updated pipeline configuration
 */
const updatePipelineConfig = async (
  pipelineId: string,
  pipelineData: Partial<PipelineConfig>
): Promise<DataResponse<PipelineConfig>> => {
  try {
    return await apiClient.put<DataResponse<PipelineConfig>>(
      `${endpoints.admin.settings}/pipelines/${pipelineId}`,
      pipelineData
    );
  } catch (error) {
    logError(error, 'updatePipelineConfig');
    throw error;
  }
};

/**
 * Deletes a pipeline configuration by ID
 * @param pipelineId - The pipeline configuration ID to delete
 * @returns Promise resolving to a success indicator
 */
const deletePipelineConfig = async (
  pipelineId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.admin.settings}/pipelines/${pipelineId}`
    );
  } catch (error) {
    logError(error, 'deletePipelineConfig');
    throw error;
  }
};

/**
 * Retrieves a paginated list of validation rule configurations with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of validation rule configurations
 */
const getValidationRuleConfigs = async (
  params: PaginationParams & { dataset?: string, table?: string, ruleType?: string, isActive?: boolean }
): Promise<ListResponse<ValidationRuleConfig>> => {
  try {
    return await apiClient.get<ListResponse<ValidationRuleConfig>>(
      `${endpoints.admin.settings}/validation-rules`,
      { params }
    );
  } catch (error) {
    logError(error, 'getValidationRuleConfigs');
    throw error;
  }
};

/**
 * Retrieves a single validation rule configuration by ID
 * @param ruleId - The validation rule ID
 * @returns Promise resolving to the validation rule configuration details
 */
const getValidationRuleConfig = async (ruleId: string): Promise<DataResponse<ValidationRuleConfig>> => {
  try {
    return await apiClient.get<DataResponse<ValidationRuleConfig>>(
      `${endpoints.admin.settings}/validation-rules/${ruleId}`
    );
  } catch (error) {
    logError(error, 'getValidationRuleConfig');
    throw error;
  }
};

/**
 * Creates a new validation rule configuration
 * @param ruleData - The validation rule configuration
 * @returns Promise resolving to the created validation rule configuration
 */
const createValidationRuleConfig = async (
  ruleData: Partial<ValidationRuleConfig>
): Promise<DataResponse<ValidationRuleConfig>> => {
  try {
    return await apiClient.post<DataResponse<ValidationRuleConfig>>(
      `${endpoints.admin.settings}/validation-rules`,
      ruleData
    );
  } catch (error) {
    logError(error, 'createValidationRuleConfig');
    throw error;
  }
};

/**
 * Updates an existing validation rule configuration
 * @param ruleId - The validation rule ID to update
 * @param ruleData - The updated validation rule configuration
 * @returns Promise resolving to the updated validation rule configuration
 */
const updateValidationRuleConfig = async (
  ruleId: string,
  ruleData: Partial<ValidationRuleConfig>
): Promise<DataResponse<ValidationRuleConfig>> => {
  try {
    return await apiClient.put<DataResponse<ValidationRuleConfig>>(
      `${endpoints.admin.settings}/validation-rules/${ruleId}`,
      ruleData
    );
  } catch (error) {
    logError(error, 'updateValidationRuleConfig');
    throw error;
  }
};

/**
 * Deletes a validation rule configuration by ID
 * @param ruleId - The validation rule ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteValidationRuleConfig = async (
  ruleId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.admin.settings}/validation-rules/${ruleId}`
    );
  } catch (error) {
    logError(error, 'deleteValidationRuleConfig');
    throw error;
  }
};

/**
 * Retrieves the current notification configuration
 * @returns Promise resolving to the notification configuration
 */
const getNotificationConfig = async (): Promise<DataResponse<NotificationConfig>> => {
  try {
    return await apiClient.get<DataResponse<NotificationConfig>>(
      `${endpoints.admin.settings}/notifications`
    );
  } catch (error) {
    logError(error, 'getNotificationConfig');
    throw error;
  }
};

/**
 * Updates the notification configuration
 * @param configData - The updated notification configuration
 * @returns Promise resolving to the updated notification configuration
 */
const updateNotificationConfig = async (
  configData: Partial<NotificationConfig>
): Promise<DataResponse<NotificationConfig>> => {
  try {
    return await apiClient.put<DataResponse<NotificationConfig>>(
      `${endpoints.admin.settings}/notifications`,
      configData
    );
  } catch (error) {
    logError(error, 'updateNotificationConfig');
    throw error;
  }
};

/**
 * Tests a notification channel by sending a test notification
 * @param testParams - Channel and configuration to test
 * @returns Promise resolving to notification test results
 */
const testNotificationChannel = async (
  testParams: { channel: string, config: any }
): Promise<DataResponse<{ success: boolean, message: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ success: boolean, message: string }>>(
      `${endpoints.admin.settings}/notifications/test`,
      testParams
    );
  } catch (error) {
    logError(error, 'testNotificationChannel');
    throw error;
  }
};

/**
 * Retrieves the current self-healing configuration
 * @returns Promise resolving to the self-healing configuration
 */
const getHealingConfig = async (): Promise<DataResponse<HealingConfig>> => {
  try {
    return await apiClient.get<DataResponse<HealingConfig>>(
      `${endpoints.admin.settings}/healing`
    );
  } catch (error) {
    logError(error, 'getHealingConfig');
    throw error;
  }
};

/**
 * Updates the self-healing configuration
 * @param configData - The updated self-healing configuration
 * @returns Promise resolving to the updated self-healing configuration
 */
const updateHealingConfig = async (
  configData: Partial<HealingConfig>
): Promise<DataResponse<HealingConfig>> => {
  try {
    return await apiClient.put<DataResponse<HealingConfig>>(
      `${endpoints.admin.settings}/healing`,
      configData
    );
  } catch (error) {
    logError(error, 'updateHealingConfig');
    throw error;
  }
};

/**
 * Retrieves a paginated list of healing actions with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of healing actions
 */
const getHealingActions = async (
  params: PaginationParams & { patternId?: string, actionType?: string, isActive?: boolean }
): Promise<ListResponse<HealingActionConfig>> => {
  try {
    return await apiClient.get<ListResponse<HealingActionConfig>>(
      `${endpoints.admin.settings}/healing/actions`,
      { params }
    );
  } catch (error) {
    logError(error, 'getHealingActions');
    throw error;
  }
};

/**
 * Retrieves a single healing action by ID
 * @param actionId - The healing action ID
 * @returns Promise resolving to the healing action details
 */
const getHealingAction = async (actionId: string): Promise<DataResponse<HealingActionConfig>> => {
  try {
    return await apiClient.get<DataResponse<HealingActionConfig>>(
      `${endpoints.admin.settings}/healing/actions/${actionId}`
    );
  } catch (error) {
    logError(error, 'getHealingAction');
    throw error;
  }
};

/**
 * Creates a new healing action
 * @param actionData - The healing action configuration
 * @returns Promise resolving to the created healing action
 */
const createHealingAction = async (
  actionData: Partial<HealingActionConfig>
): Promise<DataResponse<HealingActionConfig>> => {
  try {
    return await apiClient.post<DataResponse<HealingActionConfig>>(
      `${endpoints.admin.settings}/healing/actions`,
      actionData
    );
  } catch (error) {
    logError(error, 'createHealingAction');
    throw error;
  }
};

/**
 * Updates an existing healing action
 * @param actionId - The healing action ID to update
 * @param actionData - The updated healing action configuration
 * @returns Promise resolving to the updated healing action
 */
const updateHealingAction = async (
  actionId: string,
  actionData: Partial<HealingActionConfig>
): Promise<DataResponse<HealingActionConfig>> => {
  try {
    return await apiClient.put<DataResponse<HealingActionConfig>>(
      `${endpoints.admin.settings}/healing/actions/${actionId}`,
      actionData
    );
  } catch (error) {
    logError(error, 'updateHealingAction');
    throw error;
  }
};

/**
 * Deletes a healing action by ID
 * @param actionId - The healing action ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteHealingAction = async (
  actionId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.admin.settings}/healing/actions/${actionId}`
    );
  } catch (error) {
    logError(error, 'deleteHealingAction');
    throw error;
  }
};

/**
 * Retrieves a paginated list of issue patterns with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of issue patterns
 */
const getIssuePatterns = async (
  params: PaginationParams & { issueType?: string }
): Promise<ListResponse<IssuePatternConfig>> => {
  try {
    return await apiClient.get<ListResponse<IssuePatternConfig>>(
      `${endpoints.admin.settings}/healing/patterns`,
      { params }
    );
  } catch (error) {
    logError(error, 'getIssuePatterns');
    throw error;
  }
};

/**
 * Retrieves a single issue pattern by ID
 * @param patternId - The issue pattern ID
 * @returns Promise resolving to the issue pattern details
 */
const getIssuePattern = async (patternId: string): Promise<DataResponse<IssuePatternConfig>> => {
  try {
    return await apiClient.get<DataResponse<IssuePatternConfig>>(
      `${endpoints.admin.settings}/healing/patterns/${patternId}`
    );
  } catch (error) {
    logError(error, 'getIssuePattern');
    throw error;
  }
};

/**
 * Creates a new issue pattern
 * @param patternData - The issue pattern configuration
 * @returns Promise resolving to the created issue pattern
 */
const createIssuePattern = async (
  patternData: Partial<IssuePatternConfig>
): Promise<DataResponse<IssuePatternConfig>> => {
  try {
    return await apiClient.post<DataResponse<IssuePatternConfig>>(
      `${endpoints.admin.settings}/healing/patterns`,
      patternData
    );
  } catch (error) {
    logError(error, 'createIssuePattern');
    throw error;
  }
};

/**
 * Updates an existing issue pattern
 * @param patternId - The issue pattern ID to update
 * @param patternData - The updated issue pattern configuration
 * @returns Promise resolving to the updated issue pattern
 */
const updateIssuePattern = async (
  patternId: string,
  patternData: Partial<IssuePatternConfig>
): Promise<DataResponse<IssuePatternConfig>> => {
  try {
    return await apiClient.put<DataResponse<IssuePatternConfig>>(
      `${endpoints.admin.settings}/healing/patterns/${patternId}`,
      patternData
    );
  } catch (error) {
    logError(error, 'updateIssuePattern');
    throw error;
  }
};

/**
 * Deletes an issue pattern by ID
 * @param patternId - The issue pattern ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteIssuePattern = async (
  patternId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.admin.settings}/healing/patterns/${patternId}`
    );
  } catch (error) {
    logError(error, 'deleteIssuePattern');
    throw error;
  }
};

/**
 * Retrieves the current optimization configuration
 * @returns Promise resolving to the optimization configuration
 */
const getOptimizationConfig = async (): Promise<DataResponse<OptimizationConfig>> => {
  try {
    return await apiClient.get<DataResponse<OptimizationConfig>>(
      `${endpoints.admin.settings}/optimization`
    );
  } catch (error) {
    logError(error, 'getOptimizationConfig');
    throw error;
  }
};

/**
 * Updates the optimization configuration
 * @param configData - The updated optimization configuration
 * @returns Promise resolving to the updated optimization configuration
 */
const updateOptimizationConfig = async (
  configData: Partial<OptimizationConfig>
): Promise<DataResponse<OptimizationConfig>> => {
  try {
    return await apiClient.put<DataResponse<OptimizationConfig>>(
      `${endpoints.admin.settings}/optimization`,
      configData
    );
  } catch (error) {
    logError(error, 'updateOptimizationConfig');
    throw error;
  }
};

export default {
  // Data source management
  getDataSources,
  getDataSource,
  createDataSource,
  updateDataSource,
  deleteDataSource,
  testDataSourceConnection,
  
  // Pipeline configuration management
  getPipelineConfigs,
  getPipelineConfig,
  createPipelineConfig,
  updatePipelineConfig,
  deletePipelineConfig,
  
  // Validation rule management
  getValidationRuleConfigs,
  getValidationRuleConfig,
  createValidationRuleConfig,
  updateValidationRuleConfig,
  deleteValidationRuleConfig,
  
  // Notification configuration
  getNotificationConfig,
  updateNotificationConfig,
  testNotificationChannel,
  
  // Self-healing configuration
  getHealingConfig,
  updateHealingConfig,
  getHealingActions,
  getHealingAction,
  createHealingAction,
  updateHealingAction,
  deleteHealingAction,
  getIssuePatterns,
  getIssuePattern,
  createIssuePattern,
  updateIssuePattern,
  deleteIssuePattern,
  
  // Optimization configuration
  getOptimizationConfig,
  updateOptimizationConfig
};