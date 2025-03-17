/**
 * Core API client for the self-healing data pipeline web application.
 * Provides a standardized interface for making HTTP requests to the backend services 
 * with built-in error handling, authentication, request/response interceptors, and retry capabilities.
 */

import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse, AxiosError } from 'axios'; // axios ^1.3.4
import { apiConfig, buildUrl, getAuthHeaders } from '../../config/apiConfig';
import { getToken, isTokenExpired, isTokenAboutToExpire } from '../../utils/auth';
import { parseApiError, getErrorMessage, isNetworkError, isAuthenticationError, logError } from '../../utils/errorHandling';
import { ApiResponse, DataResponse, ListResponse, ErrorResponse, ApiError } from '../../types/api';

/**
 * Creates and configures an Axios instance with interceptors and retry logic
 * @returns Configured Axios instance
 */
const createAxiosInstance = (): AxiosInstance => {
  // Create instance with config
  const instance = axios.create(apiConfig);

  // Request interceptor for authentication
  instance.interceptors.request.use(
    async (config) => {
      // Merge default headers with auth headers
      const authHeaders = getAuthHeaders();
      config.headers = {
        ...config.headers,
        ...authHeaders
      };
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // Request interceptor for token refresh
  instance.interceptors.request.use(
    async (config) => {
      // Check if token is about to expire and refresh it if needed
      // Only if it's not already a token refresh request
      if (
        isTokenAboutToExpire(5) && // 5 minutes before expiry
        !config.url?.includes('/auth/refresh')
      ) {
        try {
          // Refresh token logic would go here
          // This could be implemented with a call to an auth service
          const token = getToken();
          if (token) {
            // For now, we'll continue with the current token
            // In a complete implementation, we would refresh the token here
          }
        } catch (refreshError) {
          // If token refresh fails, continue with the request
          // It will likely fail and be handled by error interceptor
          console.error('Token refresh failed:', refreshError);
        }
      }
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // Response interceptor for standardizing responses
  instance.interceptors.response.use(
    (response: AxiosResponse) => {
      // If the response is already in the expected format, return it
      if (response.data && 
          (response.data.status === 'SUCCESS' || 
           response.data.status === 'ERROR' || 
           response.data.status === 'WARNING')) {
        return response;
      }

      // Otherwise, wrap it in a standard format
      const standardResponse: ApiResponse = {
        status: 'SUCCESS',
        message: 'Request successful',
        metadata: {
          timestamp: new Date().toISOString(),
          requestId: response.headers['x-request-id'] || 'unknown',
          processingTime: response.headers['x-processing-time'] ? 
                          Number(response.headers['x-processing-time']) : 
                          undefined
        }
      };

      // If it's a data response
      if (response.data !== undefined) {
        const dataResponse: DataResponse<any> = {
          ...standardResponse,
          data: response.data
        };
        response.data = dataResponse;
      }

      return response;
    }
  );

  // Error interceptor for handling errors
  instance.interceptors.response.use(
    response => response,
    async (error: AxiosError) => {
      // Get the config and current retry count
      const config = error.config as AxiosRequestConfig & { retryCount?: number };
      
      // Initialize retry count if it doesn't exist
      if (config && !config.retryCount) {
        config.retryCount = 0;
      }

      // Check if we should retry the request
      if (config && shouldRetryRequest(error, config.retryCount || 0)) {
        // Increment the retry count
        config.retryCount = (config.retryCount || 0) + 1;

        // Calculate delay with exponential backoff
        const delay = Math.pow(2, config.retryCount) * (apiConfig.retryDelay || 1000);
        
        // Wait for the delay
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Retry the request
        return instance(config);
      }

      // If we shouldn't retry, or have exceeded retry attempts, handle the error
      return Promise.reject(handleRequestError(error));
    }
  );

  return instance;
};

/**
 * Processes API request errors into a standardized format
 * @param error Any error object
 * @returns Never returns, always throws a standardized error
 */
const handleRequestError = (error: any): never => {
  // Parse the error into a standardized format
  const apiError = parseApiError(error);
  
  // Log the error with context
  logError(apiError, 'API Request');
  
  // Throw the standardized error
  throw apiError;
};

/**
 * Determines if a failed request should be retried
 * @param error Axios error object
 * @param retryCount Current retry count
 * @returns True if the request should be retried
 */
const shouldRetryRequest = (error: AxiosError, retryCount: number): boolean => {
  // Check if we've reached max retries
  if (retryCount >= (apiConfig.maxRetries || 3)) {
    return false;
  }
  
  // Retry on network errors
  if (isNetworkError(error)) {
    return true;
  }
  
  // Retry on specific status codes
  // Usually 408 (Request Timeout), 429 (Too Many Requests), 500, 502, 503, 504 (Server Errors)
  if (error.response) {
    const statusCode = error.response.status;
    return statusCode === 408 || statusCode === 429 || 
           (statusCode >= 500 && statusCode < 600);
  }
  
  // Don't retry other errors
  return false;
};

// Create the Axios instance
const axiosInstance = createAxiosInstance();

/**
 * Makes a GET request to the specified endpoint
 * @param endpoint API endpoint
 * @param config Axios request configuration
 * @returns Promise resolving to the response data
 */
const get = <T>(endpoint: string, config?: AxiosRequestConfig): Promise<T> => {
  const url = buildUrl(endpoint);
  return axiosInstance.get(url, config)
    .then(response => response.data as T)
    .catch(error => {
      throw handleRequestError(error);
    });
};

/**
 * Makes a POST request to the specified endpoint with data
 * @param endpoint API endpoint
 * @param data Request body data
 * @param config Axios request configuration
 * @returns Promise resolving to the response data
 */
const post = <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
  const url = buildUrl(endpoint);
  return axiosInstance.post(url, data, config)
    .then(response => response.data as T)
    .catch(error => {
      throw handleRequestError(error);
    });
};

/**
 * Makes a PUT request to the specified endpoint with data
 * @param endpoint API endpoint
 * @param data Request body data
 * @param config Axios request configuration
 * @returns Promise resolving to the response data
 */
const put = <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
  const url = buildUrl(endpoint);
  return axiosInstance.put(url, data, config)
    .then(response => response.data as T)
    .catch(error => {
      throw handleRequestError(error);
    });
};

/**
 * Makes a PATCH request to the specified endpoint with data
 * @param endpoint API endpoint
 * @param data Request body data
 * @param config Axios request configuration
 * @returns Promise resolving to the response data
 */
const patch = <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
  const url = buildUrl(endpoint);
  return axiosInstance.patch(url, data, config)
    .then(response => response.data as T)
    .catch(error => {
      throw handleRequestError(error);
    });
};

/**
 * Makes a DELETE request to the specified endpoint
 * @param endpoint API endpoint
 * @param config Axios request configuration
 * @returns Promise resolving to the response data
 */
const del = <T>(endpoint: string, config?: AxiosRequestConfig): Promise<T> => {
  const url = buildUrl(endpoint);
  return axiosInstance.delete(url, config)
    .then(response => response.data as T)
    .catch(error => {
      throw handleRequestError(error);
    });
};

// Export the API client
export default {
  get,
  post,
  put,
  patch,
  delete: del // Export 'del' as 'delete'
};

// Export the axios instance for advanced use cases
export { axiosInstance };