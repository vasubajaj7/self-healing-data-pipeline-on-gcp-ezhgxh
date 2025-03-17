/**
 * Custom React hook that provides a standardized interface for making API requests
 * to the backend services of the self-healing data pipeline.
 * 
 * This hook wraps the apiClient with React state management for loading states,
 * error handling, and response data, making it easier to use in React components.
 */

import { useState, useCallback, useRef } from 'react'; // react ^18.2.0
import { AxiosRequestConfig, AxiosError } from 'axios'; // axios ^1.3.4
import apiClient from '../services/api/apiClient';
import { parseApiError, isAuthenticationError } from '../utils/errorHandling';
import { ApiResponse, DataResponse, ListResponse, ErrorResponse } from '../types/api';
import { useAuth } from './useAuth';

/**
 * Custom hook that provides API request methods with loading state and error handling
 * @returns Object containing API methods, loading state, and error information
 */
export const useApi = () => {
  // Initialize states for loading and error
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<ErrorResponse | null>(null);
  
  // Get logout function from the auth hook to handle authentication errors
  const { logout } = useAuth();
  
  /**
   * Wrapper function to handle API requests with loading state and error handling
   * @param requestFn - The API request function to execute
   * @param args - Arguments to pass to the request function
   * @returns Promise resolving to the response data
   */
  const requestWrapper = useCallback(
    async <T>(
      requestFn: (...args: any[]) => Promise<T>,
      ...args: any[]
    ): Promise<T> => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await requestFn(...args);
        return response;
      } catch (error) {
        const parsedError = parseApiError(error);
        
        // Handle authentication errors
        if (isAuthenticationError(error)) {
          // Log the user out if there's an authentication error
          logout();
        }
        
        // Set the error state
        setError({
          status: 'ERROR',
          message: parsedError.message,
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'unknown',
          },
          error: parsedError,
        });
        
        throw parsedError;
      } finally {
        setLoading(false);
      }
    },
    [logout]
  );
  
  /**
   * Makes a GET request to the specified endpoint
   * @param endpoint - API endpoint
   * @param config - Axios request configuration
   * @returns Promise resolving to the response data
   */
  const get = useCallback(
    <T>(endpoint: string, config?: AxiosRequestConfig): Promise<T> => {
      return requestWrapper(apiClient.get, endpoint, config);
    },
    [requestWrapper]
  );
  
  /**
   * Makes a POST request to the specified endpoint with data
   * @param endpoint - API endpoint
   * @param data - Request body data
   * @param config - Axios request configuration
   * @returns Promise resolving to the response data
   */
  const post = useCallback(
    <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
      return requestWrapper(apiClient.post, endpoint, data, config);
    },
    [requestWrapper]
  );
  
  /**
   * Makes a PUT request to the specified endpoint with data
   * @param endpoint - API endpoint
   * @param data - Request body data
   * @param config - Axios request configuration
   * @returns Promise resolving to the response data
   */
  const put = useCallback(
    <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
      return requestWrapper(apiClient.put, endpoint, data, config);
    },
    [requestWrapper]
  );
  
  /**
   * Makes a PATCH request to the specified endpoint with data
   * @param endpoint - API endpoint
   * @param data - Request body data
   * @param config - Axios request configuration
   * @returns Promise resolving to the response data
   */
  const patch = useCallback(
    <T>(endpoint: string, data: any, config?: AxiosRequestConfig): Promise<T> => {
      return requestWrapper(apiClient.patch, endpoint, data, config);
    },
    [requestWrapper]
  );
  
  /**
   * Makes a DELETE request to the specified endpoint
   * @param endpoint - API endpoint
   * @param config - Axios request configuration
   * @returns Promise resolving to the response data
   */
  const del = useCallback(
    <T>(endpoint: string, config?: AxiosRequestConfig): Promise<T> => {
      return requestWrapper(apiClient.delete, endpoint, config);
    },
    [requestWrapper]
  );
  
  // Return the API methods, loading state, and error
  return {
    get,
    post,
    put,
    patch,
    delete: del,
    loading,
    error,
  };
};

/**
 * Hook for making individual API requests with dedicated state management
 * @returns Object containing request executor, data, loading state, error, and reset function
 */
export const useApiRequest = <T>() => {
  // Initialize states for data, loading, and error
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<ErrorResponse | null>(null);
  
  // Get logout function from auth hook
  const { logout } = useAuth();
  
  /**
   * Resets all states to their initial values
   */
  const reset = useCallback(() => {
    setData(null);
    setLoading(false);
    setError(null);
  }, []);
  
  /**
   * Executes an API request with the provided function and arguments
   * @param requestFn - The API request function to execute
   * @param args - Arguments to pass to the request function
   * @returns Promise resolving to the response data
   */
  const executeRequest = useCallback(
    async <R>(
      requestFn: (...args: any[]) => Promise<R>,
      ...args: any[]
    ): Promise<R> => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await requestFn(...args);
        setData(response as unknown as T);
        return response;
      } catch (error) {
        const parsedError = parseApiError(error);
        
        // Handle authentication errors
        if (isAuthenticationError(error)) {
          logout();
        }
        
        // Set the error state
        setError({
          status: 'ERROR',
          message: parsedError.message,
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'unknown',
          },
          error: parsedError,
        });
        
        throw parsedError;
      } finally {
        setLoading(false);
      }
    },
    [logout]
  );
  
  return {
    executeRequest,
    data,
    loading,
    error,
    reset,
  };
};

export default useApi;