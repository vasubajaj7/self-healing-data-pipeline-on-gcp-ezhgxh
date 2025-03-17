import axios from 'axios'; // axios ^1.3.4
import MockAdapter from 'axios-mock-adapter'; // axios-mock-adapter ^1.21.4
import { rest } from 'msw'; // msw ^1.0.0
import apiClient, { axiosInstance } from '../../apiClient';
import { server } from '../../../test/mocks/server';
import { apiConfig, endpoints, buildUrl } from '../../../config/apiConfig';
import { generateMockDataResponse } from '../../../test/mocks/data';
import { parseApiError, isNetworkError, isAuthenticationError } from '../../../utils/errorHandling';

// Mock the auth module
jest.mock('../../../utils/auth', () => ({
  getToken: jest.fn().mockReturnValue('mock-token'),
  isTokenExpired: jest.fn().mockReturnValue(false),
  isTokenAboutToExpire: jest.fn().mockReturnValue(false)
}));

// Setup MSW server
beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

/**
 * Helper function to set up a mock response for testing
 * @param method HTTP method to mock
 * @param url API endpoint URL
 * @param responseData Response data to return
 * @param statusCode HTTP status code to return
 */
const setupMockResponse = (method: string, url: string, responseData: any, statusCode: number = 200) => {
  server.use(
    rest[method.toLowerCase()](url, (req, res, ctx) => {
      return res(
        ctx.status(statusCode),
        ctx.json(responseData)
      );
    })
  );
};

describe('API Client', () => {
  test('should make successful GET requests', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const mockData = { id: '123', name: 'Test Data' };
    const mockResponse = generateMockDataResponse(mockData);
    const url = buildUrl(testEndpoint);

    // Set up mock response
    setupMockResponse('get', url, mockResponse);

    // Make the request
    const response = await apiClient.get(testEndpoint);

    // Assertions
    expect(response).toEqual(mockResponse);
  });

  test('should make successful POST requests', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const requestData = { name: 'Test Data' };
    const mockData = { id: '123', name: 'Test Data' };
    const mockResponse = generateMockDataResponse(mockData);
    const url = buildUrl(testEndpoint);

    // Set up mock response
    setupMockResponse('post', url, mockResponse);

    // Make the request
    const response = await apiClient.post(testEndpoint, requestData);

    // Assertions
    expect(response).toEqual(mockResponse);
  });

  test('should make successful PUT requests', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint/123';
    const requestData = { name: 'Updated Test Data' };
    const mockData = { id: '123', name: 'Updated Test Data' };
    const mockResponse = generateMockDataResponse(mockData);
    const url = buildUrl(testEndpoint);

    // Set up mock response
    setupMockResponse('put', url, mockResponse);

    // Make the request
    const response = await apiClient.put(testEndpoint, requestData);

    // Assertions
    expect(response).toEqual(mockResponse);
  });

  test('should make successful PATCH requests', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint/123';
    const requestData = { name: 'Partially Updated Data' };
    const mockData = { id: '123', name: 'Partially Updated Data' };
    const mockResponse = generateMockDataResponse(mockData);
    const url = buildUrl(testEndpoint);

    // Set up mock response
    setupMockResponse('patch', url, mockResponse);

    // Make the request
    const response = await apiClient.patch(testEndpoint, requestData);

    // Assertions
    expect(response).toEqual(mockResponse);
  });

  test('should make successful DELETE requests', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint/123';
    const mockResponse = { 
      status: 'SUCCESS',
      message: 'Resource deleted successfully',
      metadata: {
        timestamp: new Date().toISOString(),
        requestId: 'test-request-id',
        processingTime: 42
      }
    };
    const url = buildUrl(testEndpoint);

    // Set up mock response
    setupMockResponse('delete', url, mockResponse);

    // Make the request
    const response = await apiClient.delete(testEndpoint);

    // Assertions
    expect(response).toEqual(mockResponse);
  });
});

describe('Error Handling', () => {
  test('should handle network errors', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Set up mock to simulate a network error
    server.use(
      rest.get(url, (req, res) => {
        return res.networkError('Network error');
      })
    );

    // Execute and verify
    await expect(apiClient.get(testEndpoint)).rejects.toThrow();
    try {
      await apiClient.get(testEndpoint);
    } catch (error) {
      expect(isNetworkError(error)).toBe(true);
      expect(error.statusCode).toBe(0);
      expect(error.errorCode).toBe('NETWORK_ERROR');
    }
  });

  test('should handle authentication errors', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Set up mock to simulate a 401 Unauthorized error
    server.use(
      rest.get(url, (req, res, ctx) => {
        return res(
          ctx.status(401),
          ctx.json({
            status: 'ERROR',
            message: 'Authentication failed',
            metadata: {
              timestamp: new Date().toISOString(),
              requestId: 'test-request-id'
            },
            error: {
              statusCode: 401,
              message: 'Authentication failed',
              errorCode: 'AUTHENTICATION_FAILED'
            }
          })
        );
      })
    );

    // Execute and verify
    await expect(apiClient.get(testEndpoint)).rejects.toThrow();
    try {
      await apiClient.get(testEndpoint);
    } catch (error) {
      expect(isAuthenticationError(error)).toBe(true);
      expect(error.statusCode).toBe(401);
      expect(error.errorCode).toBe('AUTHENTICATION_FAILED');
    }
  });

  test('should handle server errors', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Set up mock to simulate a 500 Internal Server Error
    server.use(
      rest.get(url, (req, res, ctx) => {
        return res(
          ctx.status(500),
          ctx.json({
            status: 'ERROR',
            message: 'Internal server error',
            metadata: {
              timestamp: new Date().toISOString(),
              requestId: 'test-request-id'
            },
            error: {
              statusCode: 500,
              message: 'Internal server error',
              errorCode: 'SERVER_ERROR'
            }
          })
        );
      })
    );

    // Execute and verify
    await expect(apiClient.get(testEndpoint)).rejects.toThrow();
    try {
      await apiClient.get(testEndpoint);
    } catch (error) {
      expect(error.statusCode).toBe(500);
      expect(error.errorCode).toBe('SERVER_ERROR');
    }
  });

  test('should handle validation errors', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const requestData = { invalidField: 'test' };
    const url = buildUrl(testEndpoint);
    
    // Set up mock to simulate a 400 Bad Request with validation errors
    server.use(
      rest.post(url, (req, res, ctx) => {
        return res(
          ctx.status(400),
          ctx.json({
            status: 'ERROR',
            message: 'Validation failed',
            metadata: {
              timestamp: new Date().toISOString(),
              requestId: 'test-request-id'
            },
            error: {
              statusCode: 400,
              message: 'Validation failed',
              errorCode: 'VALIDATION_ERROR',
              details: {
                fields: {
                  invalidField: 'Field is not recognized'
                }
              }
            }
          })
        );
      })
    );

    // Execute and verify
    await expect(apiClient.post(testEndpoint, requestData)).rejects.toThrow();
    try {
      await apiClient.post(testEndpoint, requestData);
    } catch (error) {
      expect(error.statusCode).toBe(400);
      expect(error.errorCode).toBe('VALIDATION_ERROR');
      expect(error.details.fields.invalidField).toBe('Field is not recognized');
    }
  });
});

describe('Retry Logic', () => {
  test('should retry failed requests up to maxRetries times', async () => {
    // Create a mock adapter to track retry attempts
    const mockAxios = new MockAdapter(axios);
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Track how many times the request is attempted
    let attempts = 0;
    
    // Mock axios to fail with a 500 error
    mockAxios.onGet(url).reply(() => {
      attempts++;
      return [500, { error: 'Server error' }];
    });

    // Execute and verify the request fails after retries
    await expect(apiClient.get(testEndpoint)).rejects.toThrow();
    
    // Verify it was attempted maxRetries + 1 times (initial + retries)
    expect(attempts).toBe(apiConfig.maxRetries + 1);
    
    // Clean up
    mockAxios.restore();
  });

  test('should not retry non-retryable errors', async () => {
    // Create a mock adapter to track retry attempts
    const mockAxios = new MockAdapter(axios);
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Track how many times the request is attempted
    let attempts = 0;
    
    // Mock axios to fail with a 400 error (non-retryable)
    mockAxios.onGet(url).reply(() => {
      attempts++;
      return [400, { error: 'Bad request' }];
    });

    // Execute and verify the request fails
    await expect(apiClient.get(testEndpoint)).rejects.toThrow();
    
    // Verify it was attempted only once (no retries)
    expect(attempts).toBe(1);
    
    // Clean up
    mockAxios.restore();
  });

  test('should succeed after a successful retry', async () => {
    // Create a mock adapter
    const mockAxios = new MockAdapter(axios);
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    const expectedData = { success: true };
    
    // Track how many times the request is attempted
    let attempts = 0;
    
    // Mock axios to fail once then succeed
    mockAxios.onGet(url).reply(() => {
      attempts++;
      if (attempts === 1) {
        // First attempt fails with a retryable error
        return [500, { error: 'Server error' }];
      } else {
        // Subsequent attempt succeeds
        return [200, expectedData];
      }
    });

    // Execute and verify the request succeeds after retry
    const response = await axiosInstance.get(url);
    expect(response.data).toEqual(expectedData);
    
    // Verify it was attempted twice (initial failure + successful retry)
    expect(attempts).toBe(2);
    
    // Clean up
    mockAxios.restore();
  });
});

describe('Authentication', () => {
  test('should include authentication headers in requests', async () => {
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Capture the actual headers sent
    let capturedHeaders = null;
    
    server.use(
      rest.get(url, (req, res, ctx) => {
        // Capture the headers
        capturedHeaders = req.headers.all();
        return res(ctx.status(200), ctx.json({ data: 'test' }));
      })
    );

    // Make the request
    await apiClient.get(testEndpoint);
    
    // Verify Authorization header was set correctly
    expect(capturedHeaders).toBeDefined();
    expect(capturedHeaders.authorization).toBe('Bearer mock-token');
  });

  test('should handle token refresh when token is about to expire', async () => {
    // Mock the auth utilities to indicate token is about to expire
    const authUtils = require('../../../utils/auth');
    const originalIsTokenAboutToExpire = authUtils.isTokenAboutToExpire;
    const originalGetToken = authUtils.getToken;
    
    // Override with test implementations
    authUtils.isTokenAboutToExpire = jest.fn().mockReturnValue(true);
    authUtils.getToken = jest.fn().mockReturnValue('refreshed-token');
    
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Capture the actual headers sent
    let capturedHeaders = null;
    
    server.use(
      rest.get(url, (req, res, ctx) => {
        // Capture the headers
        capturedHeaders = req.headers.all();
        return res(ctx.status(200), ctx.json({ data: 'test' }));
      })
    );

    // Make the request
    await apiClient.get(testEndpoint);
    
    // Verify token expiry was checked
    expect(authUtils.isTokenAboutToExpire).toHaveBeenCalledWith(5);
    
    // Restore original implementations
    authUtils.isTokenAboutToExpire = originalIsTokenAboutToExpire;
    authUtils.getToken = originalGetToken;
  });
});

describe('Response Transformation', () => {
  test('should transform successful responses to the expected format', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const nonStandardResponse = { result: 'success', value: 42 };
    const url = buildUrl(testEndpoint);
    
    // Set up mock response with a non-standard format
    server.use(
      rest.get(url, (req, res, ctx) => {
        return res(
          ctx.status(200),
          ctx.json(nonStandardResponse)
        );
      })
    );

    // Make the request
    const response = await apiClient.get(testEndpoint);
    
    // Verify response was transformed to standard format
    expect(response).toHaveProperty('status', 'SUCCESS');
    expect(response).toHaveProperty('message', 'Request successful');
    expect(response).toHaveProperty('metadata');
    expect(response).toHaveProperty('data', nonStandardResponse);
  });

  test('should handle empty responses', async () => {
    // Set up test data
    const testEndpoint = 'test/endpoint';
    const url = buildUrl(testEndpoint);
    
    // Set up mock response with no content
    server.use(
      rest.get(url, (req, res, ctx) => {
        return res(
          ctx.status(204)
        );
      })
    );

    // Make the request and verify it doesn't throw
    await expect(apiClient.get(testEndpoint)).resolves.not.toThrow();
  });
});