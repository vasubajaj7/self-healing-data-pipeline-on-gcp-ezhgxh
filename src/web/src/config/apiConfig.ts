/**
 * API Configuration
 * 
 * This module configures API endpoints, authentication, and request parameters
 * for the self-healing data pipeline web application. It defines the base URL,
 * endpoints, authentication headers, and other API-related configuration settings
 * used by the API client for communicating with backend services.
 */

import { ENV } from './env';
import { MAX_RETRIES, RETRY_DELAY, DEFAULT_TIMEOUT, API_VERSION } from './constants';
import type { AxiosRequestConfig } from 'axios'; // axios ^1.3.4

/**
 * Generates authentication headers for API requests
 * @returns Record<string, string> Authentication headers object
 */
export const getAuthHeaders = (): Record<string, string> => {
  if (!ENV.AUTH_ENABLED) {
    return {};
  }
  
  // Get token from localStorage
  const token = localStorage.getItem('auth_token');
  if (token) {
    return {
      Authorization: `Bearer ${token}`,
    };
  }
  
  return {};
};

/**
 * Builds a complete API URL by combining the base URL, API version, and endpoint path
 * @param endpoint - The API endpoint path
 * @returns string Complete API URL
 */
export const buildUrl = (endpoint: string): string => {
  // Ensure endpoint starts with a forward slash
  const formattedEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  return `${ENV.API_BASE_URL}/${API_VERSION}${formattedEndpoint}`;
};

/**
 * Main API configuration object used by the API client
 */
export const apiConfig: AxiosRequestConfig = {
  baseURL: `${ENV.API_BASE_URL}/${API_VERSION}`,
  timeout: ENV.API_TIMEOUT || DEFAULT_TIMEOUT,
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  },
  withCredentials: true, // Include cookies in cross-origin requests
  maxRetries: MAX_RETRIES,
  retryDelay: RETRY_DELAY,
};

/**
 * API endpoint definitions organized by service area
 */
export const endpoints = {
  // Authentication endpoints
  auth: {
    login: '/auth/login',
    logout: '/auth/logout',
    refreshToken: '/auth/refresh',
    profile: '/auth/profile',
  },
  
  // Pipeline management endpoints
  pipeline: {
    list: '/pipelines',
    details: (id: string) => `/pipelines/${id}`,
    create: '/pipelines',
    update: (id: string) => `/pipelines/${id}`,
    delete: (id: string) => `/pipelines/${id}`,
    execute: (id: string) => `/pipelines/${id}/execute`,
    status: (id: string) => `/pipelines/${id}/status`,
    history: (id: string) => `/pipelines/${id}/history`,
  },
  
  // Data quality endpoints
  quality: {
    metrics: '/quality/metrics',
    rules: '/quality/rules',
    ruleDetails: (id: string) => `/quality/rules/${id}`,
    validation: '/quality/validation',
    validationResults: (id: string) => `/quality/validation/${id}`,
    issues: '/quality/issues',
    issueDetails: (id: string) => `/quality/issues/${id}`,
  },
  
  // Self-healing endpoints
  healing: {
    actions: '/healing/actions',
    actionDetails: (id: string) => `/healing/actions/${id}`,
    history: '/healing/history',
    models: '/healing/models',
    modelDetails: (id: string) => `/healing/models/${id}`,
    train: '/healing/train',
    predict: '/healing/predict',
    settings: '/healing/settings',
  },
  
  // Monitoring and alerting endpoints
  monitoring: {
    metrics: '/monitoring/metrics',
    alerts: '/monitoring/alerts',
    alertDetails: (id: string) => `/monitoring/alerts/${id}`,
    dashboard: '/monitoring/dashboard',
    logs: '/monitoring/logs',
    status: '/monitoring/status',
  },
  
  // Performance optimization endpoints
  optimization: {
    recommendations: '/optimization/recommendations',
    apply: '/optimization/apply',
    history: '/optimization/history',
    settings: '/optimization/settings',
  },
  
  // Administrative endpoints
  admin: {
    users: '/admin/users',
    userDetails: (id: string) => `/admin/users/${id}`,
    roles: '/admin/roles',
    roleDetails: (id: string) => `/admin/roles/${id}`,
    settings: '/admin/settings',
    audit: '/admin/audit',
  },
  
  // Health check endpoint
  health: '/health',
};