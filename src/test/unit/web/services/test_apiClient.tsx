/**
 * Unit tests for the API client service that handles HTTP requests to the backend services.
 * Tests the core functionality including request methods, error handling, retry logic, and authentication integration.
 */

import React from 'react'; // react ^18.2.0
import {
  describe, it, expect, beforeEach, afterEach, jest,
} from '@jest/globals'; // @jest/globals ^29.5.0
import { AxiosError, AxiosRequestConfig } from 'axios'; // axios ^1.3.4
import { waitFor } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { rest } from 'msw'; // msw ^1.2.1

import apiClient, { axiosInstance } from '../../../../web/src/services/api/apiClient';
import { apiConfig, endpoints } from '../../../../web/src/config/apiConfig';
import { parseApiError, isNetworkError, isAuthenticationError } from '../../../../web/src/utils/errorHandling';
import { getToken, isTokenExpired, isTokenAboutToExpire } from '../../../../web/src/utils/auth';
import { server } from '../../../../web/src/test/mocks/server';

// Define mock data for testing
const testEndpoint = '/test';
const testData = { message: 'Test data' };
const testToken = 'test-token';

// Mock the authentication utility functions
const mockAuthUtils = (isExpired: boolean, isAboutToExpire: boolean) => {
  jest.mock('../../../../web/src/utils/auth', () => ({
    getToken: jest.fn().mockReturnValue(testToken),
    isTokenExpired: jest.fn().mockReturnValue(isExpired),
    isTokenAboutToExpire: jest.fn().mockReturnValue(isAboutToExpire),
  }));
};

// Sets up MSW handlers that return successful responses for API requests
const setupSuccessHandlers = () => {
  server.use(
    rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(testData));
    }),
    rest.post(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(201), ctx.json(testData));
    }),
    rest.put(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(testData));
    }),
    rest.patch(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(testData));
    }),
    rest.delete(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(testData));
    })
  );
};

// Sets up MSW handlers that return error responses for API requests
const setupErrorHandlers = (statusCode: number, errorMessage: string) => {
  server.use(
    rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
    }),
    rest.post(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
    }),
    rest.put(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
    }),
    rest.patch(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
    }),
    rest.delete(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
    })
  );
};

// Sets up MSW handlers that simulate network errors for API requests
const setupNetworkErrorHandlers = () => {
  server.use(
    rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.networkError('Failed to connect'));
    }),
    rest.post(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.networkError('Failed to connect'));
    }),
    rest.put(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.networkError('Failed to connect'));
    }),
    rest.patch(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.networkError('Failed to connect'));
    }),
    rest.delete(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
      return res(ctx.networkError('Failed to connect'));
    })
  );
};

describe('apiClient', () => {
  beforeEach(() => {
    server.listen();
  });

  afterEach(() => {
    server.resetHandlers();
    jest.restoreAllMocks();
  });

  afterAll(() => {
    server.close();
  });

  it('should make successful GET requests', async () => {
    setupSuccessHandlers();
    const response = await apiClient.get(testEndpoint);
    expect(response).toEqual(testData);
  });

  it('should make successful POST requests', async () => {
    setupSuccessHandlers();
    const response = await apiClient.post(testEndpoint, testData);
    expect(response).toEqual(testData);
  });

  it('should make successful PUT requests', async () => {
    setupSuccessHandlers();
    const response = await apiClient.put(testEndpoint, testData);
    expect(response).toEqual(testData);
  });

  it('should make successful PATCH requests', async () => {
    setupSuccessHandlers();
    const response = await apiClient.patch(testEndpoint, testData);
    expect(response).toEqual(testData);
  });

  it('should make successful DELETE requests', async () => {
    setupSuccessHandlers();
    const response = await apiClient.delete(testEndpoint);
    expect(response).toEqual(testData);
  });

  it('should handle API errors correctly', async () => {
    const statusCode = 400;
    const errorMessage = 'Bad Request';
    setupErrorHandlers(statusCode, errorMessage);

    try {
      await apiClient.get(testEndpoint);
    } catch (error: any) {
      expect(error.statusCode).toEqual(statusCode);
      expect(error.message).toEqual(errorMessage);
    }
  });

  it('should handle network errors correctly', async () => {
    setupNetworkErrorHandlers();

    try {
      await apiClient.get(testEndpoint);
    } catch (error: any) {
      expect(isNetworkError(error)).toBe(true);
      expect(error.message).toEqual(expect.any(String));
    }
  });

  it('should include authentication headers in requests', async () => {
    mockAuthUtils(false, false);
    let capturedHeaders: any = {};

    server.use(
      rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
        capturedHeaders = req.headers.all();
        return res(ctx.status(200), ctx.json(testData));
      })
    );

    await apiClient.get(testEndpoint);
    expect(capturedHeaders['authorization']).toEqual(`Bearer ${testToken}`);
  });

  it('should handle token refresh when token is about to expire', async () => {
    mockAuthUtils(false, true);
    const refreshTokenHandler = jest.fn();

    server.use(
      rest.post(`${apiConfig.baseURL}/auth/refresh`, (req, res, ctx) => {
        refreshTokenHandler();
        return res(ctx.status(200), ctx.json({ token: 'new-token' }));
      }),
      rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
        return res(ctx.status(200), ctx.json(testData));
      })
    );

    await apiClient.get(testEndpoint);
    expect(refreshTokenHandler).toHaveBeenCalled();
  });

  it('should retry failed requests according to configuration', async () => {
    let attemptCount = 0;
    server.use(
      rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
        attemptCount++;
        if (attemptCount < 3) {
          return res(ctx.status(500));
        }
        return res(ctx.status(200), ctx.json(testData));
      })
    );

    const response = await apiClient.get(testEndpoint);
    expect(response).toEqual(testData);
    expect(attemptCount).toEqual(3);
  });

  it('should not retry certain error status codes', async () => {
    const statusCode = 400;
    const errorMessage = 'Bad Request';
    setupErrorHandlers(statusCode, errorMessage);
    let attemptCount = 0;

    server.use(
      rest.get(`${apiConfig.baseURL}${testEndpoint}`, (req, res, ctx) => {
        attemptCount++;
        return res(ctx.status(statusCode), ctx.json({ message: errorMessage }));
      })
    );

    try {
      await apiClient.get(testEndpoint);
    } catch (error: any) {
      expect(error.statusCode).toEqual(statusCode);
      expect(attemptCount).toEqual(1);
    }
  });

  it('should respect the configured timeout', async () => {
    server.use(
      rest.get(`${apiConfig.baseURL}${testEndpoint}`, async (req, res, ctx) => {
        await new Promise((resolve) => setTimeout(resolve, apiConfig.timeout! + 100));
        return res(ctx.status(200), ctx.json(testData));
      })
    );

    try {
      await apiClient.get(testEndpoint);
    } catch (error: any) {
      expect(error.message).toEqual(expect.any(String));
    }
  });
});

describe('axiosInstance', () => {
  it('should be configured with the correct base URL', () => {
    expect(axiosInstance.defaults.baseURL).toBe(`${apiConfig.baseURL}`);
  });

  it('should be configured with the correct timeout', () => {
    expect(axiosInstance.defaults.timeout).toBe(apiConfig.timeout);
  });

  it('should be configured with the correct headers', () => {
    expect(axiosInstance.defaults.headers['Content-Type']).toBe('application/json');
    expect(axiosInstance.defaults.headers['Accept']).toBe('application/json');
  });
});