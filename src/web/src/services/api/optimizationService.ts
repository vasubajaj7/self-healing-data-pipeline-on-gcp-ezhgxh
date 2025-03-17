/**
 * Service module for interacting with the optimization API endpoints of the self-healing data pipeline.
 * Provides methods for retrieving optimization recommendations, applying optimizations, and managing
 * optimization configurations for BigQuery queries, schemas, and resources.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import { DataResponse, ListResponse } from '../../types/api';
import {
  OptimizationType,
  QueryPerformanceMetrics,
  QueryDetails,
  QueryOptimizationRecommendation,
  TableDetails,
  SchemaOptimizationRecommendation,
  ResourceUtilizationMetrics,
  ResourceOptimizationRecommendation,
  CostAnalysisData,
  OptimizationSummary,
  QueryOptimizationParams,
  SchemaOptimizationParams,
  ResourceOptimizationParams,
  ApplyOptimizationRequest,
  RejectOptimizationRequest
} from '../../types/optimization';

/**
 * Retrieves a summary of all optimization recommendations and their status
 * @returns Promise resolving to optimization summary data
 */
const getOptimizationSummary = (): Promise<DataResponse<OptimizationSummary>> => {
  return apiClient.get<DataResponse<OptimizationSummary>>(endpoints.optimization.recommendations + '/summary');
};

/**
 * Retrieves performance metrics for BigQuery queries with pagination support
 * @param params Pagination and filtering parameters
 * @returns Promise resolving to paginated query performance metrics
 */
const getQueryPerformanceMetrics = (params: object): Promise<ListResponse<QueryPerformanceMetrics>> => {
  return apiClient.get<ListResponse<QueryPerformanceMetrics>>(
    endpoints.optimization.recommendations + '/query-performance',
    { params }
  );
};

/**
 * Retrieves detailed information about a specific query including execution plan and statistics
 * @param queryId The ID of the query to retrieve details for
 * @returns Promise resolving to detailed query information
 */
const getQueryDetails = (queryId: string): Promise<DataResponse<QueryDetails>> => {
  return apiClient.get<DataResponse<QueryDetails>>(
    endpoints.optimization.recommendations + '/queries/' + queryId
  );
};

/**
 * Retrieves optimization recommendations for BigQuery queries with filtering and pagination
 * @param params Query optimization filter parameters
 * @returns Promise resolving to paginated query optimization recommendations
 */
const getQueryOptimizationRecommendations = (
  params: QueryOptimizationParams
): Promise<ListResponse<QueryOptimizationRecommendation>> => {
  return apiClient.get<ListResponse<QueryOptimizationRecommendation>>(
    endpoints.optimization.recommendations + '/queries',
    { params }
  );
};

/**
 * Retrieves detailed information about a specific BigQuery table including schema and usage statistics
 * @param datasetId The ID of the dataset
 * @param tableId The ID of the table
 * @returns Promise resolving to detailed table information
 */
const getTableDetails = (datasetId: string, tableId: string): Promise<DataResponse<TableDetails>> => {
  return apiClient.get<DataResponse<TableDetails>>(
    endpoints.optimization.recommendations + '/tables/' + datasetId + '/' + tableId
  );
};

/**
 * Retrieves schema optimization recommendations for BigQuery tables with filtering and pagination
 * @param params Schema optimization filter parameters
 * @returns Promise resolving to paginated schema optimization recommendations
 */
const getSchemaOptimizationRecommendations = (
  params: SchemaOptimizationParams
): Promise<ListResponse<SchemaOptimizationRecommendation>> => {
  return apiClient.get<ListResponse<SchemaOptimizationRecommendation>>(
    endpoints.optimization.recommendations + '/schema',
    { params }
  );
};

/**
 * Retrieves resource utilization metrics with pagination support
 * @param params Pagination and filtering parameters
 * @returns Promise resolving to paginated resource utilization metrics
 */
const getResourceUtilizationMetrics = (params: object): Promise<ListResponse<ResourceUtilizationMetrics>> => {
  return apiClient.get<ListResponse<ResourceUtilizationMetrics>>(
    endpoints.optimization.recommendations + '/resource-metrics',
    { params }
  );
};

/**
 * Retrieves resource optimization recommendations with filtering and pagination
 * @param params Resource optimization filter parameters
 * @returns Promise resolving to paginated resource optimization recommendations
 */
const getResourceOptimizationRecommendations = (
  params: ResourceOptimizationParams
): Promise<ListResponse<ResourceOptimizationRecommendation>> => {
  return apiClient.get<ListResponse<ResourceOptimizationRecommendation>>(
    endpoints.optimization.recommendations + '/resources',
    { params }
  );
};

/**
 * Retrieves cost analysis data for a specified time period
 * @param startDate Start date for cost analysis (ISO format)
 * @param endDate End date for cost analysis (ISO format)
 * @returns Promise resolving to cost analysis data
 */
const getCostAnalysisData = (startDate: string, endDate: string): Promise<DataResponse<CostAnalysisData>> => {
  return apiClient.get<DataResponse<CostAnalysisData>>(
    endpoints.optimization.recommendations + '/cost-analysis',
    { params: { startDate, endDate } }
  );
};

/**
 * Applies a specific optimization recommendation
 * @param request The optimization application request with recommendation details
 * @returns Promise resolving to the application result
 */
const applyOptimization = (request: ApplyOptimizationRequest): Promise<DataResponse<any>> => {
  return apiClient.post<DataResponse<any>>(
    endpoints.optimization.apply,
    request
  );
};

/**
 * Rejects a specific optimization recommendation with a reason
 * @param request The optimization rejection request with reason
 * @returns Promise resolving to the rejection result
 */
const rejectOptimization = (request: RejectOptimizationRequest): Promise<DataResponse<any>> => {
  return apiClient.post<DataResponse<any>>(
    endpoints.optimization.recommendations + '/reject',
    request
  );
};

/**
 * Retrieves the current optimization configuration settings
 * @returns Promise resolving to the optimization configuration
 */
const getOptimizationConfig = (): Promise<DataResponse<any>> => {
  return apiClient.get<DataResponse<any>>(endpoints.optimization.settings);
};

/**
 * Updates the optimization configuration settings
 * @param config The new configuration settings to apply
 * @returns Promise resolving to the updated configuration
 */
const updateOptimizationConfig = (config: object): Promise<DataResponse<any>> => {
  return apiClient.put<DataResponse<any>>(
    endpoints.optimization.settings,
    config
  );
};

export default {
  getOptimizationSummary,
  getQueryPerformanceMetrics,
  getQueryDetails,
  getQueryOptimizationRecommendations,
  getTableDetails,
  getSchemaOptimizationRecommendations,
  getResourceUtilizationMetrics,
  getResourceOptimizationRecommendations,
  getCostAnalysisData,
  applyOptimization,
  rejectOptimization,
  getOptimizationConfig,
  updateOptimizationConfig
};