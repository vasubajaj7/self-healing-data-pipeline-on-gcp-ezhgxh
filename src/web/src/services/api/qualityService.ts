/**
 * Quality Service
 * 
 * Service module for interacting with data quality-related API endpoints in the self-healing 
 * data pipeline application. Provides functions for retrieving and managing quality rules,
 * validation results, quality scores, and quality issues.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import {
  QualityRule,
  QualityValidation,
  QualityScore,
  ValidationStatus,
  PaginationParams,
  DateRangeParams,
  DataResponse,
  ListResponse
} from '../../types/api';
import {
  DatasetQualitySummary,
  QualityIssue,
  QualityRuleDetails,
  QualityValidationResult,
  QualityTimeSeries,
  ColumnQualityDetails,
  QualityStatistics,
  QualityDashboardFilters,
  QualityRuleTemplate,
  QualityDimension,
  QualityRuleType,
  QualityIssueStatus
} from '../../types/quality';
import { logError } from '../../utils/errorHandling';

/**
 * Retrieves a paginated list of quality rules with optional filtering
 * @param params - Pagination and filter parameters
 * @returns Promise resolving to a paginated list of quality rules
 */
const getQualityRules = async (
  params: PaginationParams & { 
    dataset?: string, 
    table?: string, 
    ruleType?: QualityRuleType, 
    isActive?: boolean 
  }
): Promise<ListResponse<QualityRule>> => {
  try {
    return await apiClient.get<ListResponse<QualityRule>>(
      endpoints.quality.rules,
      { params }
    );
  } catch (error) {
    logError(error, 'getQualityRules');
    throw error;
  }
};

/**
 * Retrieves a single quality rule by ID
 * @param ruleId - ID of the quality rule to retrieve
 * @returns Promise resolving to the detailed quality rule data
 */
const getQualityRule = async (
  ruleId: string
): Promise<DataResponse<QualityRuleDetails>> => {
  try {
    return await apiClient.get<DataResponse<QualityRuleDetails>>(
      endpoints.quality.ruleDetails(ruleId)
    );
  } catch (error) {
    logError(error, 'getQualityRule');
    throw error;
  }
};

/**
 * Creates a new quality rule
 * @param ruleData - Data for the new quality rule
 * @returns Promise resolving to the created quality rule
 */
const createQualityRule = async (
  ruleData: Partial<QualityRule>
): Promise<DataResponse<QualityRule>> => {
  try {
    return await apiClient.post<DataResponse<QualityRule>>(
      endpoints.quality.rules,
      ruleData
    );
  } catch (error) {
    logError(error, 'createQualityRule');
    throw error;
  }
};

/**
 * Updates an existing quality rule
 * @param ruleId - ID of the quality rule to update
 * @param ruleData - Updated rule data
 * @returns Promise resolving to the updated quality rule
 */
const updateQualityRule = async (
  ruleId: string,
  ruleData: Partial<QualityRule>
): Promise<DataResponse<QualityRule>> => {
  try {
    return await apiClient.put<DataResponse<QualityRule>>(
      endpoints.quality.ruleDetails(ruleId),
      ruleData
    );
  } catch (error) {
    logError(error, 'updateQualityRule');
    throw error;
  }
};

/**
 * Deletes a quality rule by ID
 * @param ruleId - ID of the quality rule to delete
 * @returns Promise resolving to a success indicator
 */
const deleteQualityRule = async (
  ruleId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      endpoints.quality.ruleDetails(ruleId)
    );
  } catch (error) {
    logError(error, 'deleteQualityRule');
    throw error;
  }
};

/**
 * Retrieves a paginated list of quality validations with optional filtering
 * @param params - Pagination, date range, and filter parameters
 * @returns Promise resolving to a paginated list of quality validations
 */
const getQualityValidations = async (
  params: PaginationParams & DateRangeParams & { 
    executionId?: string, 
    ruleId?: string, 
    status?: ValidationStatus 
  }
): Promise<ListResponse<QualityValidation>> => {
  try {
    return await apiClient.get<ListResponse<QualityValidation>>(
      endpoints.quality.validation,
      { params }
    );
  } catch (error) {
    logError(error, 'getQualityValidations');
    throw error;
  }
};

/**
 * Retrieves a single quality validation by ID with detailed results
 * @param validationId - ID of the validation to retrieve
 * @returns Promise resolving to the detailed quality validation result
 */
const getQualityValidation = async (
  validationId: string
): Promise<DataResponse<QualityValidationResult>> => {
  try {
    return await apiClient.get<DataResponse<QualityValidationResult>>(
      endpoints.quality.validationResults(validationId)
    );
  } catch (error) {
    logError(error, 'getQualityValidation');
    throw error;
  }
};

/**
 * Retrieves a paginated list of quality issues with optional filtering
 * @param params - Pagination, date range, and filter parameters
 * @returns Promise resolving to a paginated list of quality issues
 */
const getQualityIssues = async (
  params: PaginationParams & DateRangeParams & { 
    dataset?: string, 
    table?: string, 
    dimension?: QualityDimension, 
    severity?: string,
    status?: QualityIssueStatus 
  }
): Promise<ListResponse<QualityIssue>> => {
  try {
    return await apiClient.get<ListResponse<QualityIssue>>(
      endpoints.quality.issues,
      { params }
    );
  } catch (error) {
    logError(error, 'getQualityIssues');
    throw error;
  }
};

/**
 * Retrieves a single quality issue by ID
 * @param issueId - ID of the quality issue to retrieve
 * @returns Promise resolving to the quality issue data
 */
const getQualityIssue = async (
  issueId: string
): Promise<DataResponse<QualityIssue>> => {
  try {
    return await apiClient.get<DataResponse<QualityIssue>>(
      endpoints.quality.issueDetails(issueId)
    );
  } catch (error) {
    logError(error, 'getQualityIssue');
    throw error;
  }
};

/**
 * Updates the status of a quality issue
 * @param issueId - ID of the quality issue to update
 * @param statusData - Status update data
 * @returns Promise resolving to the updated quality issue
 */
const updateQualityIssueStatus = async (
  issueId: string,
  statusData: { status: QualityIssueStatus, comment?: string }
): Promise<DataResponse<QualityIssue>> => {
  try {
    // Construct the status update endpoint
    const statusEndpoint = `${endpoints.quality.issueDetails(issueId)}/status`;
    return await apiClient.put<DataResponse<QualityIssue>>(
      statusEndpoint,
      statusData
    );
  } catch (error) {
    logError(error, 'updateQualityIssueStatus');
    throw error;
  }
};

/**
 * Retrieves quality summaries for all datasets or filtered by dataset name
 * @param params - Optional dataset filter
 * @returns Promise resolving to a list of dataset quality summaries
 */
const getDatasetQualitySummaries = async (
  params: { dataset?: string }
): Promise<ListResponse<DatasetQualitySummary>> => {
  try {
    return await apiClient.get<ListResponse<DatasetQualitySummary>>(
      `${endpoints.quality.metrics}/datasets`,
      { params }
    );
  } catch (error) {
    logError(error, 'getDatasetQualitySummaries');
    throw error;
  }
};

/**
 * Retrieves the quality score for a specific dataset and table
 * @param dataset - Dataset name
 * @param table - Table name
 * @returns Promise resolving to the quality score data
 */
const getQualityScore = async (
  dataset: string,
  table: string
): Promise<DataResponse<QualityScore>> => {
  try {
    return await apiClient.get<DataResponse<QualityScore>>(
      `${endpoints.quality.metrics}/score/${dataset}/${table}`
    );
  } catch (error) {
    logError(error, 'getQualityScore');
    throw error;
  }
};

/**
 * Retrieves quality score time series data for trend analysis
 * @param params - Dataset, optional table, and date range parameters
 * @returns Promise resolving to quality time series data
 */
const getQualityTimeSeries = async (
  params: { 
    dataset: string, 
    table?: string, 
    startDate: string, 
    endDate: string 
  }
): Promise<DataResponse<QualityTimeSeries>> => {
  try {
    return await apiClient.get<DataResponse<QualityTimeSeries>>(
      `${endpoints.quality.metrics}/timeseries`,
      { params }
    );
  } catch (error) {
    logError(error, 'getQualityTimeSeries');
    throw error;
  }
};

/**
 * Retrieves detailed quality information for a specific column
 * @param dataset - Dataset name
 * @param table - Table name
 * @param column - Column name
 * @returns Promise resolving to column quality details
 */
const getColumnQualityDetails = async (
  dataset: string,
  table: string,
  column: string
): Promise<DataResponse<ColumnQualityDetails>> => {
  try {
    return await apiClient.get<DataResponse<ColumnQualityDetails>>(
      `${endpoints.quality.metrics}/column/${dataset}/${table}/${column}`
    );
  } catch (error) {
    logError(error, 'getColumnQualityDetails');
    throw error;
  }
};

/**
 * Retrieves overall quality statistics for the system
 * @returns Promise resolving to quality statistics data
 */
const getQualityStatistics = async (): Promise<DataResponse<QualityStatistics>> => {
  try {
    return await apiClient.get<DataResponse<QualityStatistics>>(
      `${endpoints.quality.metrics}/statistics`
    );
  } catch (error) {
    logError(error, 'getQualityStatistics');
    throw error;
  }
};

/**
 * Retrieves available quality rule templates
 * @param params - Optional rule type and dimension filters
 * @returns Promise resolving to a list of quality rule templates
 */
const getQualityRuleTemplates = async (
  params: { 
    ruleType?: QualityRuleType, 
    dimension?: QualityDimension 
  }
): Promise<ListResponse<QualityRuleTemplate>> => {
  try {
    return await apiClient.get<ListResponse<QualityRuleTemplate>>(
      `${endpoints.quality.rules}/templates`,
      { params }
    );
  } catch (error) {
    logError(error, 'getQualityRuleTemplates');
    throw error;
  }
};

/**
 * Triggers an on-demand quality validation for a dataset and table
 * @param validationParams - Dataset, table, and optional rule IDs to validate
 * @returns Promise resolving to the validation ID
 */
const runQualityValidation = async (
  validationParams: { 
    dataset: string, 
    table: string, 
    ruleIds?: string[] 
  }
): Promise<DataResponse<{ validationId: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ validationId: string }>>(
      `${endpoints.quality.validation}/run`,
      validationParams
    );
  } catch (error) {
    logError(error, 'runQualityValidation');
    throw error;
  }
};

// Export all functions as a single service object
export default {
  getQualityRules,
  getQualityRule,
  createQualityRule,
  updateQualityRule,
  deleteQualityRule,
  getQualityValidations,
  getQualityValidation,
  getQualityIssues,
  getQualityIssue,
  updateQualityIssueStatus,
  getDatasetQualitySummaries,
  getQualityScore,
  getQualityTimeSeries,
  getColumnQualityDetails,
  getQualityStatistics,
  getQualityRuleTemplates,
  runQualityValidation
};