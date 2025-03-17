import { AxiosError } from 'axios'; // ^1.3.4
import { ERROR_MESSAGES, HTTP_STATUS } from '../utils/constants';
import { ApiError, ErrorResponse } from '../types/api';

/**
 * Parses an error from any source into a standardized ApiError format
 * @param error Any error object or primitive
 * @returns A standardized ApiError object
 */
export const parseApiError = (error: any): ApiError => {
  // Case 1: Already an ApiError
  if (
    error && 
    typeof error === 'object' && 
    'statusCode' in error && 
    'message' in error && 
    'errorCode' in error
  ) {
    return error as ApiError;
  }

  // Case 2: Axios error
  if (error && error.isAxiosError) {
    const axiosError = error as AxiosError;
    
    // Handle axios network errors
    if (!axiosError.response) {
      return {
        statusCode: 0,
        message: ERROR_MESSAGES.NETWORK_ERROR,
        errorCode: 'NETWORK_ERROR',
        details: { originalError: axiosError.message }
      };
    }
    
    // Handle axios response errors
    const statusCode = axiosError.response.status;
    let message = ERROR_MESSAGES.SERVER_ERROR;
    let errorCode = 'SERVER_ERROR';
    
    // Determine appropriate error message based on status code
    if (statusCode === HTTP_STATUS.UNAUTHORIZED) {
      message = ERROR_MESSAGES.AUTHENTICATION_FAILED;
      errorCode = 'AUTHENTICATION_FAILED';
    } else if (statusCode === HTTP_STATUS.FORBIDDEN) {
      message = ERROR_MESSAGES.PERMISSION_DENIED;
      errorCode = 'PERMISSION_DENIED';
    } else if (statusCode === HTTP_STATUS.NOT_FOUND) {
      message = ERROR_MESSAGES.NOT_FOUND;
      errorCode = 'NOT_FOUND';
    } else if (statusCode === HTTP_STATUS.BAD_REQUEST) {
      message = ERROR_MESSAGES.VALIDATION_ERROR;
      errorCode = 'VALIDATION_ERROR';
    }
    
    // Try to extract server-provided error details if available
    const responseData = axiosError.response.data;
    if (responseData && typeof responseData === 'object') {
      if (responseData.message && typeof responseData.message === 'string') {
        message = responseData.message;
      }
      if (responseData.errorCode && typeof responseData.errorCode === 'string') {
        errorCode = responseData.errorCode;
      }
    }
    
    return {
      statusCode,
      message,
      errorCode,
      details: responseData
    };
  }

  // Case 3: ErrorResponse
  if (
    error && 
    typeof error === 'object' && 
    'error' in error &&
    error.error &&
    typeof error.error === 'object'
  ) {
    return error.error as ApiError;
  }

  // Case 4: Standard Error object
  if (error instanceof Error) {
    return {
      statusCode: HTTP_STATUS.INTERNAL_SERVER_ERROR,
      message: error.message || ERROR_MESSAGES.SERVER_ERROR,
      errorCode: 'APPLICATION_ERROR',
      details: { stack: error.stack }
    };
  }

  // Case 5: String error
  if (typeof error === 'string') {
    return {
      statusCode: HTTP_STATUS.INTERNAL_SERVER_ERROR,
      message: error,
      errorCode: 'APPLICATION_ERROR'
    };
  }

  // Case 6: Unknown error type
  return {
    statusCode: HTTP_STATUS.INTERNAL_SERVER_ERROR,
    message: ERROR_MESSAGES.SERVER_ERROR,
    errorCode: 'UNKNOWN_ERROR',
    details: { originalError: error }
  };
};

/**
 * Extracts a user-friendly error message from any error type
 * @param error Any error object or primitive
 * @returns A user-friendly error message string
 */
export const getErrorMessage = (error: any): string => {
  const parsedError = parseApiError(error);
  return parsedError.message || ERROR_MESSAGES.SERVER_ERROR;
};

/**
 * Determines the appropriate severity level for an error based on its type and status code
 * @param error Any error object or primitive
 * @returns Error severity level ('error', 'warning', or 'info')
 */
export const getErrorSeverityLevel = (error: any): 'error' | 'warning' | 'info' => {
  const parsedError = parseApiError(error);
  const statusCode = parsedError.statusCode;
  
  // Critical errors (server errors and authentication issues)
  if (statusCode >= 500 || statusCode === HTTP_STATUS.UNAUTHORIZED) {
    return 'error';
  }
  
  // Warning level errors (most client errors)
  if (statusCode >= 400) {
    return 'warning';
  }
  
  // Informational errors (less severe issues)
  if (parsedError.errorCode === 'INFO') {
    return 'info';
  }
  
  // Default to error for unknown cases
  return 'error';
};

/**
 * Checks if an error is a network connectivity error
 * @param error Any error object or primitive
 * @returns True if the error is a network error
 */
export const isNetworkError = (error: any): boolean => {
  // Check for Axios network errors (no response object)
  if (error && error.isAxiosError) {
    const axiosError = error as AxiosError;
    return !axiosError.response && !!axiosError.request;
  }
  
  // Check for standard network errors
  if (error instanceof Error) {
    return error.message.toLowerCase().includes('network') || 
      error.message.toLowerCase().includes('connection') || 
      error.message.toLowerCase().includes('offline');
  }
  
  // Check error message in parsed error
  const parsedError = parseApiError(error);
  return parsedError.message === ERROR_MESSAGES.NETWORK_ERROR ||
    parsedError.errorCode === 'NETWORK_ERROR';
};

/**
 * Checks if an error is related to authentication or authorization
 * @param error Any error object or primitive
 * @returns True if the error is an authentication error
 */
export const isAuthenticationError = (error: any): boolean => {
  const parsedError = parseApiError(error);
  
  // Check status code for authentication/authorization errors
  if (
    parsedError.statusCode === HTTP_STATUS.UNAUTHORIZED || 
    parsedError.statusCode === HTTP_STATUS.FORBIDDEN
  ) {
    return true;
  }
  
  // Check error codes and messages for authentication-related content
  return parsedError.errorCode === 'AUTHENTICATION_FAILED' ||
    parsedError.errorCode === 'SESSION_EXPIRED' ||
    parsedError.errorCode === 'PERMISSION_DENIED' ||
    parsedError.message === ERROR_MESSAGES.AUTHENTICATION_FAILED ||
    parsedError.message === ERROR_MESSAGES.SESSION_EXPIRED ||
    parsedError.message === ERROR_MESSAGES.PERMISSION_DENIED;
};

/**
 * Logs an error with appropriate formatting and context information
 * @param error Any error object or primitive
 * @param context Additional context information about where the error occurred
 */
export const logError = (error: any, context: string = 'Unknown'): void => {
  const parsedError = parseApiError(error);
  
  // Format the error message with context
  const formattedMessage = `[ERROR] Context: ${context} | Status: ${parsedError.statusCode} | Code: ${parsedError.errorCode} | Message: ${parsedError.message}`;
  
  // Log to console
  console.error(formattedMessage);
  
  // In development, also log detailed information
  if (process.env.NODE_ENV === 'development') {
    console.error('Error Details:', parsedError.details);
    
    // If it's an original Error object with stack trace, log that too
    if (error instanceof Error) {
      console.error('Stack Trace:', error.stack);
    }
  }
};

/**
 * Creates an ApiError from an ErrorResponse object
 * @param response ErrorResponse object from API
 * @returns Standardized ApiError object
 */
export const createErrorFromResponse = (response: ErrorResponse): ApiError => {
  if (response.error) {
    return response.error;
  }
  
  // If the response doesn't have an error object, create one from available data
  return {
    statusCode: HTTP_STATUS.INTERNAL_SERVER_ERROR,
    message: response.message || ERROR_MESSAGES.SERVER_ERROR,
    errorCode: 'API_ERROR',
    details: { responseMetadata: response.metadata }
  };
};