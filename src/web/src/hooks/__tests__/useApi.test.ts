import React from 'react';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useApi, useApiRequest } from '../../hooks/useApi';
import apiClient from '../../services/api/apiClient';
import { renderWithProviders } from '../../test/utils/renderWithProviders';
import { isAuthenticationError } from '../../utils/errorHandling';

// Mock the apiClient module
jest.mock('../../services/api/apiClient', () => ({
  get: jest.fn(),
  post: jest.fn(),
  put: jest.fn(),
  patch: jest.fn(),
  delete: jest.fn()
}));

// Mock the error handling utility
jest.mock('../../utils/errorHandling', () => ({
  isAuthenticationError: jest.fn(),
  parseApiError: jest.fn(error => error)
}));

describe('useApi', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    // Set up default mock implementations
    apiClient.get.mockResolvedValue({ data: 'test data' });
    apiClient.post.mockResolvedValue({ data: 'test data' });
    apiClient.put.mockResolvedValue({ data: 'test data' });
    apiClient.patch.mockResolvedValue({ data: 'test data' });
    apiClient.delete.mockResolvedValue({ data: 'test data' });
    isAuthenticationError.mockReturnValue(false);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should return API methods and state', async () => {
    const { result } = renderHook(() => useApi(), { wrapper: renderWithProviders });
    
    expect(result.current.get).toBeInstanceOf(Function);
    expect(result.current.post).toBeInstanceOf(Function);
    expect(result.current.put).toBeInstanceOf(Function);
    expect(result.current.patch).toBeInstanceOf(Function);
    expect(result.current.delete).toBeInstanceOf(Function);
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBe(null);
  });

  it('should handle loading state', async () => {
    // Create a promise that we can control
    let resolvePromise: (value: any) => void;
    const responsePromise = new Promise(resolve => {
      resolvePromise = resolve;
    });

    // Mock apiClient.get to use our controlled promise
    apiClient.get.mockReturnValue(responsePromise);

    const { result } = renderHook(() => useApi(), { wrapper: renderWithProviders });
    
    // Call get method
    const getPromise = result.current.get('/test');
    
    // Check that loading is true during the API call
    expect(result.current.loading).toBe(true);
    
    // Resolve the promise
    resolvePromise!({ data: 'test data' });
    
    // Wait for the promise to resolve
    await getPromise;
    
    // Check that loading is false after the API call completes
    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });
  });

  it('should handle errors', async () => {
    // Mock apiClient.get to reject with an error
    const testError = new Error('Test error');
    apiClient.get.mockRejectedValue(testError);

    const { result } = renderHook(() => useApi(), { wrapper: renderWithProviders });
    
    // Call get method and handle the rejection
    try {
      await result.current.get('/test');
    } catch (error) {
      // Expected to throw
    }
    
    // Check that error state is set and loading is false
    await waitFor(() => {
      expect(result.current.error).not.toBe(null);
      expect(result.current.loading).toBe(false);
    });
  });

  it('should handle authentication errors and logout', async () => {
    // Mock authentication error
    isAuthenticationError.mockReturnValue(true);
    apiClient.get.mockRejectedValue(new Error('Auth error'));
    
    // Create a logout mock function
    const logoutMock = jest.fn();
    
    // Use a custom render function to provide the logout function
    const customRender = (ui: React.ReactElement) => {
      return renderWithProviders(ui, {
        authProviderProps: {
          logout: logoutMock
        }
      });
    };

    const { result } = renderHook(() => useApi(), { wrapper: customRender });
    
    // Call get method and handle the rejection
    try {
      await result.current.get('/test');
    } catch (error) {
      // Expected to throw
    }
    
    // Check that logout was called
    await waitFor(() => {
      expect(logoutMock).toHaveBeenCalled();
    });
  });

  it('should make successful API calls', async () => {
    // Mock successful responses
    apiClient.get.mockResolvedValue({ data: 'get data' });
    apiClient.post.mockResolvedValue({ data: 'post data' });
    apiClient.put.mockResolvedValue({ data: 'put data' });
    apiClient.patch.mockResolvedValue({ data: 'patch data' });
    apiClient.delete.mockResolvedValue({ data: 'delete data' });

    const { result } = renderHook(() => useApi(), { wrapper: renderWithProviders });
    
    // Test GET
    const getResult = await result.current.get('/test');
    expect(getResult).toEqual({ data: 'get data' });
    expect(apiClient.get).toHaveBeenCalledWith('/test', undefined);
    
    // Test POST
    const postResult = await result.current.post('/test', { foo: 'bar' });
    expect(postResult).toEqual({ data: 'post data' });
    expect(apiClient.post).toHaveBeenCalledWith('/test', { foo: 'bar' }, undefined);
    
    // Test PUT
    const putResult = await result.current.put('/test', { foo: 'bar' });
    expect(putResult).toEqual({ data: 'put data' });
    expect(apiClient.put).toHaveBeenCalledWith('/test', { foo: 'bar' }, undefined);
    
    // Test PATCH
    const patchResult = await result.current.patch('/test', { foo: 'bar' });
    expect(patchResult).toEqual({ data: 'patch data' });
    expect(apiClient.patch).toHaveBeenCalledWith('/test', { foo: 'bar' }, undefined);
    
    // Test DELETE
    const deleteResult = await result.current.delete('/test');
    expect(deleteResult).toEqual({ data: 'delete data' });
    expect(apiClient.delete).toHaveBeenCalledWith('/test', undefined);
  });
});

describe('useApiRequest', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    // Set up default mock implementations
    isAuthenticationError.mockReturnValue(false);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should return expected state and functions', () => {
    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: renderWithProviders });
    
    expect(result.current.executeRequest).toBeInstanceOf(Function);
    expect(result.current.data).toBe(null);
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBe(null);
    expect(result.current.reset).toBeInstanceOf(Function);
  });

  it('should handle loading state during request execution', async () => {
    // Create a promise that we can control
    let resolvePromise: (value: any) => void;
    const requestFn = jest.fn().mockImplementation(() => new Promise(resolve => {
      resolvePromise = resolve;
    }));

    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: renderWithProviders });
    
    // Call executeRequest
    const requestPromise = result.current.executeRequest(requestFn);
    
    // Check that loading is true during the request
    expect(result.current.loading).toBe(true);
    expect(result.current.error).toBe(null);
    
    // Resolve the promise
    resolvePromise!({ data: 'test data' });
    
    // Wait for the request to complete
    await requestPromise;
    
    // Check that loading is false after the request completes
    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.data).toEqual({ data: 'test data' });
    });
  });

  it('should handle errors properly', async () => {
    // Mock a function that rejects with an error
    const testError = new Error('Test error');
    const requestFn = jest.fn().mockRejectedValue(testError);

    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: renderWithProviders });
    
    // Call executeRequest and handle the rejection
    try {
      await result.current.executeRequest(requestFn);
    } catch (error) {
      // Expected to throw
    }
    
    // Check that error state is set and loading is false
    await waitFor(() => {
      expect(result.current.error).not.toBe(null);
      expect(result.current.loading).toBe(false);
      expect(result.current.data).toBe(null);
    });
  });

  it('should execute requests and handle data properly', async () => {
    const testData = { id: 1, name: 'Test' };
    const requestFn = jest.fn().mockResolvedValue(testData);

    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: renderWithProviders });
    
    // Call executeRequest
    const response = await result.current.executeRequest(requestFn, 'arg1', 'arg2');
    
    // Check that the request function was called with the correct arguments
    expect(requestFn).toHaveBeenCalledWith('arg1', 'arg2');
    
    // Check that the response is correct
    expect(response).toEqual(testData);
    
    // Check that the data state is updated
    expect(result.current.data).toEqual(testData);
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBe(null);
  });

  it('should reset state properly', async () => {
    const testData = { id: 1, name: 'Test' };
    const requestFn = jest.fn().mockResolvedValue(testData);

    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: renderWithProviders });
    
    // Call executeRequest to set some data
    await result.current.executeRequest(requestFn);
    
    // Check that data is set
    expect(result.current.data).toEqual(testData);
    
    // Reset the state
    act(() => {
      result.current.reset();
    });
    
    // Check that state is reset
    expect(result.current.data).toBe(null);
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBe(null);
  });

  it('should handle authentication errors and logout', async () => {
    // Mock authentication error
    isAuthenticationError.mockReturnValue(true);
    const requestFn = jest.fn().mockRejectedValue(new Error('Auth error'));
    
    // Create a logout mock function
    const logoutMock = jest.fn();
    
    // Use a custom render function to provide the logout function
    const customRender = (ui: React.ReactElement) => {
      return renderWithProviders(ui, {
        authProviderProps: {
          logout: logoutMock
        }
      });
    };

    const { result } = renderHook(() => useApiRequest<any>(), { wrapper: customRender });
    
    // Call executeRequest and handle the rejection
    try {
      await result.current.executeRequest(requestFn);
    } catch (error) {
      // Expected to throw
    }
    
    // Check that logout was called
    await waitFor(() => {
      expect(logoutMock).toHaveBeenCalled();
    });
  });
});