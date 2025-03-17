/**
 * Service module for interacting with pipeline-related API endpoints in the self-healing data pipeline application.
 * Provides functions for retrieving pipeline definitions, executions, metrics, and performing pipeline operations
 * like starting, stopping, and monitoring pipelines.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import {
  PipelineDefinition, PipelineExecution, TaskExecution,
  PaginationParams, DateRangeParams, DataResponse, ListResponse
} from '../../types/api';
import { PipelineHealthMetrics, PipelineStatus } from '../../types/dashboard';
import { logError } from '../../utils/errorHandling';

/**
 * Retrieves a paginated list of pipeline definitions
 * @param params Pagination parameters for the request
 * @returns Promise resolving to a paginated list of pipeline definitions
 */
const getPipelineDefinitions = async (
  params: PaginationParams
): Promise<ListResponse<PipelineDefinition>> => {
  try {
    return await apiClient.get<ListResponse<PipelineDefinition>>(
      endpoints.pipeline.list,
      { params }
    );
  } catch (error) {
    logError(error, 'getPipelineDefinitions');
    throw error;
  }
};

/**
 * Retrieves a single pipeline definition by ID
 * @param pipelineId ID of the pipeline definition to retrieve
 * @returns Promise resolving to the pipeline definition data
 */
const getPipelineDefinition = async (
  pipelineId: string
): Promise<DataResponse<PipelineDefinition>> => {
  try {
    return await apiClient.get<DataResponse<PipelineDefinition>>(
      endpoints.pipeline.details(pipelineId)
    );
  } catch (error) {
    logError(error, 'getPipelineDefinition');
    throw error;
  }
};

/**
 * Creates a new pipeline definition
 * @param pipelineData Pipeline definition data
 * @returns Promise resolving to the created pipeline definition
 */
const createPipelineDefinition = async (
  pipelineData: Partial<PipelineDefinition>
): Promise<DataResponse<PipelineDefinition>> => {
  try {
    return await apiClient.post<DataResponse<PipelineDefinition>>(
      endpoints.pipeline.create,
      pipelineData
    );
  } catch (error) {
    logError(error, 'createPipelineDefinition');
    throw error;
  }
};

/**
 * Updates an existing pipeline definition
 * @param pipelineId ID of the pipeline to update
 * @param pipelineData Updated pipeline definition data
 * @returns Promise resolving to the updated pipeline definition
 */
const updatePipelineDefinition = async (
  pipelineId: string,
  pipelineData: Partial<PipelineDefinition>
): Promise<DataResponse<PipelineDefinition>> => {
  try {
    return await apiClient.put<DataResponse<PipelineDefinition>>(
      endpoints.pipeline.update(pipelineId),
      pipelineData
    );
  } catch (error) {
    logError(error, 'updatePipelineDefinition');
    throw error;
  }
};

/**
 * Deletes a pipeline definition by ID
 * @param pipelineId ID of the pipeline to delete
 * @returns Promise resolving to a success indicator
 */
const deletePipelineDefinition = async (
  pipelineId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.delete<DataResponse<{ success: boolean }>>(
      endpoints.pipeline.delete(pipelineId)
    );
  } catch (error) {
    logError(error, 'deletePipelineDefinition');
    throw error;
  }
};

/**
 * Retrieves a paginated list of pipeline executions with optional filtering
 * @param params Pagination, date range, and filter parameters
 * @returns Promise resolving to a paginated list of pipeline executions
 */
const getPipelineExecutions = async (
  params: PaginationParams & DateRangeParams & { pipelineId?: string, status?: PipelineStatus }
): Promise<ListResponse<PipelineExecution>> => {
  try {
    // If pipelineId is provided, use the specific history endpoint
    if (params.pipelineId) {
      const { pipelineId, ...queryParams } = params;
      return await apiClient.get<ListResponse<PipelineExecution>>(
        endpoints.pipeline.history(pipelineId),
        { params: queryParams }
      );
    }
    
    // Otherwise use a generic executions endpoint
    return await apiClient.get<ListResponse<PipelineExecution>>(
      '/pipelines/executions',
      { params }
    );
  } catch (error) {
    logError(error, 'getPipelineExecutions');
    throw error;
  }
};

/**
 * Retrieves a single pipeline execution by ID
 * @param executionId ID of the execution to retrieve
 * @returns Promise resolving to the pipeline execution data
 */
const getPipelineExecution = async (
  executionId: string
): Promise<DataResponse<PipelineExecution>> => {
  try {
    return await apiClient.get<DataResponse<PipelineExecution>>(
      `/pipelines/executions/${executionId}`
    );
  } catch (error) {
    logError(error, 'getPipelineExecution');
    throw error;
  }
};

/**
 * Retrieves task executions for a specific pipeline execution
 * @param executionId ID of the pipeline execution
 * @param params Pagination parameters
 * @returns Promise resolving to a paginated list of task executions
 */
const getTaskExecutions = async (
  executionId: string,
  params: PaginationParams
): Promise<ListResponse<TaskExecution>> => {
  try {
    return await apiClient.get<ListResponse<TaskExecution>>(
      `/pipelines/executions/${executionId}/tasks`,
      { params }
    );
  } catch (error) {
    logError(error, 'getTaskExecutions');
    throw error;
  }
};

/**
 * Triggers a pipeline execution with optional parameters
 * @param pipelineId ID of the pipeline to run
 * @param executionParams Optional parameters for the execution
 * @returns Promise resolving to the execution ID of the triggered pipeline
 */
const runPipeline = async (
  pipelineId: string,
  executionParams: Record<string, any> = {}
): Promise<DataResponse<{ executionId: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ executionId: string }>>(
      endpoints.pipeline.execute(pipelineId),
      executionParams
    );
  } catch (error) {
    logError(error, 'runPipeline');
    throw error;
  }
};

/**
 * Stops a running pipeline execution
 * @param executionId ID of the execution to stop
 * @returns Promise resolving to a success indicator
 */
const stopPipeline = async (
  executionId: string
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.post<DataResponse<{ success: boolean }>>(
      `/pipelines/executions/${executionId}/stop`,
      {}
    );
  } catch (error) {
    logError(error, 'stopPipeline');
    throw error;
  }
};

/**
 * Retries a failed pipeline execution
 * @param executionId ID of the failed execution to retry
 * @param retryParams Optional parameters for the retry
 * @returns Promise resolving to the new execution ID
 */
const retryPipeline = async (
  executionId: string,
  retryParams: Record<string, any> = {}
): Promise<DataResponse<{ executionId: string }>> => {
  try {
    return await apiClient.post<DataResponse<{ executionId: string }>>(
      `/pipelines/executions/${executionId}/retry`,
      retryParams
    );
  } catch (error) {
    logError(error, 'retryPipeline');
    throw error;
  }
};

/**
 * Retrieves health metrics for all pipelines or a specific pipeline
 * @param params Optional parameters including pipelineId and timeRange
 * @returns Promise resolving to pipeline health metrics
 */
const getPipelineHealthMetrics = async (
  params: { pipelineId?: string, timeRange?: string } = {}
): Promise<DataResponse<PipelineHealthMetrics>> => {
  try {
    return await apiClient.get<DataResponse<PipelineHealthMetrics>>(
      '/monitoring/pipelines/health',
      { params }
    );
  } catch (error) {
    logError(error, 'getPipelineHealthMetrics');
    throw error;
  }
};

/**
 * Retrieves the schedule information for a pipeline
 * @param pipelineId ID of the pipeline
 * @returns Promise resolving to the pipeline schedule information
 */
const getPipelineSchedule = async (
  pipelineId: string
): Promise<DataResponse<{ schedule: string, nextRun: string }>> => {
  try {
    return await apiClient.get<DataResponse<{ schedule: string, nextRun: string }>>(
      `${endpoints.pipeline.details(pipelineId)}/schedule`
    );
  } catch (error) {
    logError(error, 'getPipelineSchedule');
    throw error;
  }
};

/**
 * Updates the schedule for a pipeline
 * @param pipelineId ID of the pipeline
 * @param scheduleData Schedule data including cron expression
 * @returns Promise resolving to a success indicator
 */
const updatePipelineSchedule = async (
  pipelineId: string,
  scheduleData: { schedule: string }
): Promise<DataResponse<{ success: boolean }>> => {
  try {
    return await apiClient.put<DataResponse<{ success: boolean }>>(
      `${endpoints.pipeline.details(pipelineId)}/schedule`,
      scheduleData
    );
  } catch (error) {
    logError(error, 'updatePipelineSchedule');
    throw error;
  }
};

export default {
  getPipelineDefinitions,
  getPipelineDefinition,
  createPipelineDefinition,
  updatePipelineDefinition,
  deletePipelineDefinition,
  getPipelineExecutions,
  getPipelineExecution,
  getTaskExecutions,
  runPipeline,
  stopPipeline,
  retryPipeline,
  getPipelineHealthMetrics,
  getPipelineSchedule,
  updatePipelineSchedule
};