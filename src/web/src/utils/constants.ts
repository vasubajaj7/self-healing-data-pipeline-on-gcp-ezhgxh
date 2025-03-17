/**
 * Application-wide constants for the self-healing data pipeline web application.
 * This file defines constants used across the application such as API endpoints,
 * storage keys, error messages, and other utility constants.
 */

// Base API endpoints
export const API_ENDPOINTS = {
  PIPELINE: '/api/pipeline',
  QUALITY: '/api/quality',
  HEALING: '/api/healing',
  ALERTS: '/api/alerts',
  MONITORING: '/api/monitoring',
  OPTIMIZATION: '/api/optimization',
  ADMIN: '/api/admin',
  AUTH: '/api/auth',
};

// Pipeline-specific endpoints
export const PIPELINE_ENDPOINTS = {
  LIST: '/list',
  DETAIL: '/detail',
  EXECUTE: '/execute',
  HISTORY: '/history',
  TASKS: '/tasks',
};

// Data quality-specific endpoints
export const QUALITY_ENDPOINTS = {
  DATASETS: '/datasets',
  VALIDATION_RULES: '/validation-rules',
  VALIDATION_RESULTS: '/validation-results',
  ISSUES: '/issues',
};

// Self-healing specific endpoints
export const HEALING_ENDPOINTS = {
  ISSUES: '/issues',
  ACTIONS: '/actions',
  MODELS: '/models',
  HISTORY: '/history',
  SETTINGS: '/settings',
};

// Alert-specific endpoints
export const ALERT_ENDPOINTS = {
  ACTIVE: '/active',
  HISTORY: '/history',
  ACKNOWLEDGE: '/acknowledge',
  SETTINGS: '/settings',
};

// Monitoring-specific endpoints
export const MONITORING_ENDPOINTS = {
  METRICS: '/metrics',
  DASHBOARD: '/dashboard',
  HEALTH: '/health',
};

// Optimization-specific endpoints
export const OPTIMIZATION_ENDPOINTS = {
  RECOMMENDATIONS: '/recommendations',
  QUERY_PERFORMANCE: '/query-performance',
  SCHEMA: '/schema',
  RESOURCES: '/resources',
};

// Authentication-specific endpoints
export const AUTH_ENDPOINTS = {
  LOGIN: '/login',
  LOGOUT: '/logout',
  REFRESH: '/refresh',
  PROFILE: '/profile',
  RESET_PASSWORD: '/reset-password',
};

// Admin-specific endpoints
export const ADMIN_ENDPOINTS = {
  USERS: '/users',
  ROLES: '/roles',
  SETTINGS: '/settings',
};

// Local storage keys
export const STORAGE_KEYS = {
  AUTH_TOKEN: 'auth_token',
  REFRESH_TOKEN: 'refresh_token',
  TOKEN_EXPIRY: 'token_expiry',
  USER_PREFERENCES: 'user_preferences',
  THEME: 'theme',
  LANGUAGE: 'language',
};

// Individual storage keys for common use
export const AUTH_TOKEN_KEY = STORAGE_KEYS.AUTH_TOKEN;
export const REFRESH_TOKEN_KEY = STORAGE_KEYS.REFRESH_TOKEN;
export const TOKEN_EXPIRY_KEY = STORAGE_KEYS.TOKEN_EXPIRY;

// HTTP Status codes
export const HTTP_STATUS = {
  OK: 200,
  CREATED: 201,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  INTERNAL_SERVER_ERROR: 500,
};

// Standard error messages
export const ERROR_MESSAGES = {
  NETWORK_ERROR: 'Unable to connect to the server. Please check your internet connection.',
  AUTHENTICATION_FAILED: 'Authentication failed. Please check your credentials and try again.',
  SESSION_EXPIRED: 'Your session has expired. Please log in again.',
  PERMISSION_DENIED: 'You do not have permission to perform this action.',
  NOT_FOUND: 'The requested resource was not found.',
  SERVER_ERROR: 'An unexpected error occurred. Please try again later.',
  VALIDATION_ERROR: 'Please check the form for errors and try again.',
};

// Pipeline execution status values
export const PIPELINE_STATUS = {
  RUNNING: 'running',
  SUCCEEDED: 'succeeded',
  FAILED: 'failed',
  PENDING: 'pending',
  SELF_HEALING: 'self_healing',
};

// Data quality validation status
export const QUALITY_STATUS = {
  PASSED: 'passed',
  FAILED: 'failed',
  WARNING: 'warning',
  IN_PROGRESS: 'in_progress',
};

// Alert severity levels
export const ALERT_SEVERITY = {
  CRITICAL: 'critical',
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
};

// Self-healing confidence levels
export const HEALING_CONFIDENCE = {
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
};

// Application routes
export const ROUTES = {
  HOME: '/',
  LOGIN: '/login',
  DASHBOARD: '/dashboard',
  PIPELINE: '/pipeline',
  QUALITY: '/quality',
  HEALING: '/healing',
  ALERTS: '/alerts',
  CONFIGURATION: '/configuration',
  ADMIN: '/admin',
  PROFILE: '/profile',
  NOT_FOUND: '/404',
};

// Date and time format patterns
export const DATE_FORMATS = {
  SHORT_DATE: 'MM/dd/yyyy',
  LONG_DATE: 'MMMM d, yyyy',
  SHORT_TIME: 'HH:mm',
  LONG_TIME: 'HH:mm:ss',
  SHORT_DATETIME: 'MM/dd/yyyy HH:mm',
  LONG_DATETIME: 'MMMM d, yyyy HH:mm:ss',
  ISO_DATE: 'yyyy-MM-dd',
  ISO_DATETIME: 'yyyy-MM-ddTHH:mm:ss',
};

// Pagination defaults
export const PAGINATION = {
  DEFAULT_PAGE: 1,
  DEFAULT_PAGE_SIZE: 20,
  PAGE_SIZE_OPTIONS: [10, 20, 50, 100],
};