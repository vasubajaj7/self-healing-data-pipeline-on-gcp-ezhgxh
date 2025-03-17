/**
 * Service module for interacting with self-healing related API endpoints in the data pipeline application.
 * Provides functions for retrieving healing issues, patterns, actions, executions, and managing
 * self-healing operations like configuring healing settings, triggering manual healing, and
 * monitoring healing activities.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import { PaginationParams, DateRangeParams, DataResponse, ListResponse } from '../../types/api';
import {
  HealingPattern,
  HealingAction,
  HealingExecution,
  HealingIssue,
  HealingSettings,
  AIModel,
  ModelHealth,
  HealingActivityLogEntry,
  HealingDashboardData,
  HealingFilters,
  HealingRuleTest,
  ManualHealingRequest,
  ModelTrainingRequest,
  ModelEvaluationResult,
  IssueType,
  ActionType,
  HealingMode,
  ModelType,
  ModelStatus
} from '../../types/selfHealing';
import { logError } from '../../utils/errorHandling';

/**
 * Retrieves a paginated list of healing issues with optional filtering
 * @param params Pagination and filter parameters
 * @returns Promise resolving to a paginated list of healing issues
 */
const getHealingIssues = async (
  params: PaginationParams & HealingFilters
): Promise<ListResponse<HealingIssue>> => {
  try {
    return await apiClient.get<ListResponse<HealingIssue>>(
      `${endpoints.healing.history}/issues`,
      { params }
    );
  } catch (error) {
    logError(error, 'getHealingIssues');
    throw error;
  }
};

/**
 * Retrieves a single healing issue by ID
 * @param issueId Issue ID
 * @returns Promise resolving to the healing issue data
 */
const getHealingIssue = async (
  issueId: string
): Promise<DataResponse<HealingIssue>> => {
  try {
    return await apiClient.get<DataResponse<HealingIssue>>(
      `${endpoints.healing.history}/issues/${issueId}`
    );
  } catch (error) {
    logError(error, 'getHealingIssue');
    throw error;
  }
};

/**
 * Retrieves a paginated list of healing patterns with optional filtering
 * @param params Pagination and filter parameters
 * @returns Promise resolving to a paginated list of healing patterns
 */
const getHealingPatterns = async (
  params: PaginationParams & { issueType?: IssueType, isActive?: boolean }
): Promise<ListResponse<HealingPattern>> => {
  try {
    return await apiClient.get<ListResponse<HealingPattern>>(
      `${endpoints.healing.actions}/patterns`,
      { params }
    );
  } catch (error) {
    logError(error, 'getHealingPatterns');
    throw error;
  }
};

/**
 * Retrieves a single healing pattern by ID
 * @param patternId Pattern ID
 * @returns Promise resolving to the healing pattern data
 */
const getHealingPattern = async (
  patternId: string
): Promise<DataResponse<HealingPattern>> => {
  try {
    return await apiClient.get<DataResponse<HealingPattern>>(
      `${endpoints.healing.actions}/patterns/${patternId}`
    );
  } catch (error) {
    logError(error, 'getHealingPattern');
    throw error;
  }
};

/**
 * Creates a new healing pattern
 * @param patternData Pattern data to create
 * @returns Promise resolving to the created healing pattern
 */
const createHealingPattern = async (
  patternData: Partial<HealingPattern>
): Promise<DataResponse<HealingPattern>> => {
  try {
    return await apiClient.post<DataResponse<HealingPattern>>(
      `${endpoints.healing.actions}/patterns`,
      patternData
    );
  } catch (error) {
    logError(error, 'createHealingPattern');
    throw error;
  }
};

/**
 * Updates an existing healing pattern
 * @param patternId Pattern ID to update
 * @param patternData Updated pattern data
 * @returns Promise resolving to the updated healing pattern
 */
const updateHealingPattern = async (
  patternId: string,
  patternData: Partial<HealingPattern>
): Promise<DataResponse<HealingPattern>> => {
  try {
    return await apiClient.put<DataResponse<HealingPattern>>(
      `${endpoints.healing.actions}/patterns/${patternId}`,
      patternData
    );
  } catch (error) {
    logError(error, 'updateHealingPattern');
    throw error;
  }
};

/**
 * Deletes a healing pattern by ID
 * @param patternId Pattern ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteHealingPattern = async (
  patternId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      `${endpoints.healing.actions}/patterns/${patternId}`
    );
  } catch (error) {
    logError(error, 'deleteHealingPattern');
    throw error;
  }
};

/**
 * Retrieves a paginated list of healing actions with optional filtering
 * @param params Pagination and filter parameters
 * @returns Promise resolving to a paginated list of healing actions
 */
const getHealingActions = async (
  params: PaginationParams & { patternId?: string, actionType?: ActionType, isActive?: boolean }
): Promise<ListResponse<HealingAction>> => {
  try {
    return await apiClient.get<ListResponse<HealingAction>>(
      endpoints.healing.actions,
      { params }
    );
  } catch (error) {
    logError(error, 'getHealingActions');
    throw error;
  }
};

/**
 * Retrieves a single healing action by ID
 * @param actionId Action ID
 * @returns Promise resolving to the healing action data
 */
const getHealingAction = async (
  actionId: string
): Promise<DataResponse<HealingAction>> => {
  try {
    return await apiClient.get<DataResponse<HealingAction>>(
      endpoints.healing.actionDetails(actionId)
    );
  } catch (error) {
    logError(error, 'getHealingAction');
    throw error;
  }
};

/**
 * Creates a new healing action for a pattern
 * @param actionData Action data to create
 * @returns Promise resolving to the created healing action
 */
const createHealingAction = async (
  actionData: Partial<HealingAction>
): Promise<DataResponse<HealingAction>> => {
  try {
    return await apiClient.post<DataResponse<HealingAction>>(
      endpoints.healing.actions,
      actionData
    );
  } catch (error) {
    logError(error, 'createHealingAction');
    throw error;
  }
};

/**
 * Updates an existing healing action
 * @param actionId Action ID to update
 * @param actionData Updated action data
 * @returns Promise resolving to the updated healing action
 */
const updateHealingAction = async (
  actionId: string,
  actionData: Partial<HealingAction>
): Promise<DataResponse<HealingAction>> => {
  try {
    return await apiClient.put<DataResponse<HealingAction>>(
      endpoints.healing.actionDetails(actionId),
      actionData
    );
  } catch (error) {
    logError(error, 'updateHealingAction');
    throw error;
  }
};

/**
 * Deletes a healing action by ID
 * @param actionId Action ID to delete
 * @returns Promise resolving to a success indicator
 */
const deleteHealingAction = async (
  actionId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      endpoints.healing.actionDetails(actionId)
    );
  } catch (error) {
    logError(error, 'deleteHealingAction');
    throw error;
  }
};

/**
 * Retrieves a paginated list of healing executions with optional filtering
 * @param params Pagination, date range, and filter parameters
 * @returns Promise resolving to a paginated list of healing executions
 */
const getHealingExecutions = async (
  params: PaginationParams & DateRangeParams & { 
    executionId?: string, 
    patternId?: string, 
    actionId?: string, 
    successful?: boolean 
  }
): Promise<ListResponse<HealingExecution>> => {
  try {
    return await apiClient.get<ListResponse<HealingExecution>>(
      endpoints.healing.history,
      { params }
    );
  } catch (error) {
    logError(error, 'getHealingExecutions');
    throw error;
  }
};

/**
 * Retrieves a single healing execution by ID
 * @param healingId Healing execution ID
 * @returns Promise resolving to the healing execution data
 */
const getHealingExecution = async (
  healingId: string
): Promise<DataResponse<HealingExecution>> => {
  try {
    return await apiClient.get<DataResponse<HealingExecution>>(
      `${endpoints.healing.history}/${healingId}`
    );
  } catch (error) {
    logError(error, 'getHealingExecution');
    throw error;
  }
};

/**
 * Retrieves the current self-healing configuration settings
 * @returns Promise resolving to the healing settings data
 */
const getHealingSettings = async (): Promise<DataResponse<HealingSettings>> => {
  try {
    return await apiClient.get<DataResponse<HealingSettings>>(
      endpoints.healing.settings
    );
  } catch (error) {
    logError(error, 'getHealingSettings');
    throw error;
  }
};

/**
 * Updates the self-healing configuration settings
 * @param settingsData Updated settings data
 * @returns Promise resolving to the updated healing settings
 */
const updateHealingSettings = async (
  settingsData: Partial<HealingSettings>
): Promise<DataResponse<HealingSettings>> => {
  try {
    return await apiClient.put<DataResponse<HealingSettings>>(
      endpoints.healing.settings,
      settingsData
    );
  } catch (error) {
    logError(error, 'updateHealingSettings');
    throw error;
  }
};

/**
 * Retrieves a paginated list of AI models with optional filtering
 * @param params Pagination and filter parameters
 * @returns Promise resolving to a paginated list of AI models
 */
const getAIModels = async (
  params: PaginationParams & { modelType?: ModelType, status?: ModelStatus }
): Promise<ListResponse<AIModel>> => {
  try {
    return await apiClient.get<ListResponse<AIModel>>(
      endpoints.healing.models,
      { params }
    );
  } catch (error) {
    logError(error, 'getAIModels');
    throw error;
  }
};

/**
 * Retrieves a single AI model by ID
 * @param modelId Model ID
 * @returns Promise resolving to the AI model data
 */
const getAIModel = async (
  modelId: string
): Promise<DataResponse<AIModel>> => {
  try {
    return await apiClient.get<DataResponse<AIModel>>(
      endpoints.healing.modelDetails(modelId)
    );
  } catch (error) {
    logError(error, 'getAIModel');
    throw error;
  }
};

/**
 * Retrieves health metrics for an AI model
 * @param modelId Model ID
 * @returns Promise resolving to the model health data
 */
const getModelHealth = async (
  modelId: string
): Promise<DataResponse<ModelHealth>> => {
  try {
    return await apiClient.get<DataResponse<ModelHealth>>(
      `${endpoints.healing.modelDetails(modelId)}/health`
    );
  } catch (error) {
    logError(error, 'getModelHealth');
    throw error;
  }
};

/**
 * Initiates training for a new or existing AI model
 * @param trainingRequest Training request parameters
 * @returns Promise resolving to the model ID and training job ID
 */
const trainModel = async (
  trainingRequest: ModelTrainingRequest
): Promise<DataResponse<{ modelId: string, trainingJobId: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ modelId: string, trainingJobId: string }>>(
      endpoints.healing.train,
      trainingRequest
    );
  } catch (error) {
    logError(error, 'trainModel');
    throw error;
  }
};

/**
 * Retrieves evaluation results for an AI model
 * @param modelId Model ID
 * @returns Promise resolving to the model evaluation results
 */
const getModelEvaluation = async (
  modelId: string
): Promise<DataResponse<ModelEvaluationResult>> => {
  try {
    return await apiClient.get<DataResponse<ModelEvaluationResult>>(
      `${endpoints.healing.modelDetails(modelId)}/evaluation`
    );
  } catch (error) {
    logError(error, 'getModelEvaluation');
    throw error;
  }
};

/**
 * Activates an AI model for use in self-healing
 * @param modelId Model ID to activate
 * @returns Promise resolving to a success indicator
 */
const activateModel = async (
  modelId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.post<DataResponse<{ success: boolean }>>(
      `${endpoints.healing.modelDetails(modelId)}/activate`,
      {}
    );
  } catch (error) {
    logError(error, 'activateModel');
    throw error;
  }
};

/**
 * Deactivates an AI model from use in self-healing
 * @param modelId Model ID to deactivate
 * @returns Promise resolving to a success indicator
 */
const deactivateModel = async (
  modelId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.post<DataResponse<{ success: boolean }>>(
      `${endpoints.healing.modelDetails(modelId)}/deactivate`,
      {}
    );
  } catch (error) {
    logError(error, 'deactivateModel');
    throw error;
  }
};

/**
 * Retrieves a paginated list of self-healing activity log entries
 * @param params Pagination and date range parameters
 * @returns Promise resolving to a paginated list of activity log entries
 */
const getActivityLog = async (
  params: PaginationParams & DateRangeParams
): Promise<ListResponse<HealingActivityLogEntry>> => {
  try {
    return await apiClient.get<ListResponse<HealingActivityLogEntry>>(
      `${endpoints.healing.history}/activity`,
      { params }
    );
  } catch (error) {
    logError(error, 'getActivityLog');
    throw error;
  }
};

/**
 * Retrieves aggregated dashboard data for the self-healing system
 * @param params Date range parameters
 * @returns Promise resolving to the dashboard data
 */
const getDashboardData = async (
  params: DateRangeParams
): Promise<DataResponse<HealingDashboardData>> => {
  try {
    return await apiClient.get<DataResponse<HealingDashboardData>>(
      `${endpoints.healing.history}/dashboard`,
      { params }
    );
  } catch (error) {
    logError(error, 'getDashboardData');
    throw error;
  }
};

/**
 * Tests a healing pattern and action against sample data
 * @param testParams Test parameters
 * @returns Promise resolving to the test results
 */
const testHealingRule = async (
  testParams: { patternId?: string, actionId?: string, testData: any }
): Promise<DataResponse<HealingRuleTest>> => {
  try {
    return await apiClient.post<DataResponse<HealingRuleTest>>(
      `${endpoints.healing.predict}/test`,
      testParams
    );
  } catch (error) {
    logError(error, 'testHealingRule');
    throw error;
  }
};

/**
 * Manually triggers a healing action for an issue
 * @param healingRequest Healing request parameters
 * @returns Promise resolving to the healing execution ID
 */
const triggerManualHealing = async (
  healingRequest: ManualHealingRequest
): Promise<DataResponse<{ healingId: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ healingId: string }>>(
      `${endpoints.healing.predict}/manual`,
      healingRequest
    );
  } catch (error) {
    logError(error, 'triggerManualHealing');
    throw error;
  }
};

export default {
  getHealingIssues,
  getHealingIssue,
  getHealingPatterns,
  getHealingPattern,
  createHealingPattern,
  updateHealingPattern,
  deleteHealingPattern,
  getHealingActions,
  getHealingAction,
  createHealingAction,
  updateHealingAction,
  deleteHealingAction,
  getHealingExecutions,
  getHealingExecution,
  getHealingSettings,
  updateHealingSettings,
  getAIModels,
  getAIModel,
  getModelHealth,
  trainModel,
  getModelEvaluation,
  activateModel,
  deactivateModel,
  getActivityLog,
  getDashboardData,
  testHealingRule,
  triggerManualHealing
};