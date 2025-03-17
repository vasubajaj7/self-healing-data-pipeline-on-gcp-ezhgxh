/**
 * Environment Configuration
 * 
 * This module provides environment-specific configuration values and utility functions
 * for the self-healing data pipeline web application. It encapsulates all environment
 * variables and provides type-safe access to configuration values.
 * 
 * The configuration is loaded from Vite's import.meta.env system and provides fallbacks
 * for all values to ensure the application works in any environment.
 */

// Type definition for feature flags
interface FeatureFlags {
  ENABLE_SELF_HEALING_UI: boolean;
  ENABLE_ML_MODEL_MANAGEMENT: boolean;
  ENABLE_ADVANCED_ANALYTICS: boolean;
  ENABLE_PIPELINE_BUILDER: boolean;
  ENABLE_QUALITY_DASHBOARD: boolean;
}

/**
 * Determines if the application is running in production mode
 * @returns {boolean} True if running in production, false otherwise
 */
export const isProduction = (): boolean => {
  return import.meta.env.MODE === 'production' || 
         import.meta.env.VITE_NODE_ENV === 'production';
};

/**
 * Determines if the application is running in development mode
 * @returns {boolean} True if running in development, false otherwise
 */
export const isDevelopment = (): boolean => {
  return import.meta.env.MODE === 'development' || 
         import.meta.env.VITE_NODE_ENV === 'development';
};

/**
 * Determines if the application is running in test mode
 * @returns {boolean} True if running in test mode, false otherwise
 */
export const isTest = (): boolean => {
  return import.meta.env.MODE === 'test' || 
         import.meta.env.VITE_NODE_ENV === 'test';
};

/**
 * Safely retrieves an environment variable with a fallback value
 * @param {string} key - The environment variable key
 * @param {any} defaultValue - Default value to return if the key is not found
 * @returns {any} The environment variable value or the default value
 */
export const getEnvVar = <T>(key: string, defaultValue: T): T => {
  const envVar = import.meta.env[key];
  if (envVar === undefined || envVar === null || envVar === '') {
    return defaultValue;
  }
  
  // Handle boolean values
  if (defaultValue === true || defaultValue === false) {
    return (envVar === 'true') as unknown as T;
  }
  
  // Handle numeric values
  if (typeof defaultValue === 'number') {
    return Number(envVar) as unknown as T;
  }
  
  return envVar as unknown as T;
};

/**
 * Main environment configuration object
 * Contains all configuration values for the application
 */
export const ENV = {
  // API Configuration
  API_BASE_URL: getEnvVar<string>(
    'VITE_API_BASE_URL', 
    isDevelopment() ? 'http://localhost:3000/api' : '/api'
  ),
  
  API_TIMEOUT: getEnvVar<number>(
    'VITE_API_TIMEOUT', 
    30000  // 30 seconds default timeout
  ),
  
  // Authentication
  AUTH_ENABLED: getEnvVar<boolean>(
    'VITE_AUTH_ENABLED', 
    !isDevelopment()  // Disabled in dev by default, enabled elsewhere
  ),
  
  // Real-time updates
  WEBSOCKET_URL: getEnvVar<string>(
    'VITE_WEBSOCKET_URL', 
    isDevelopment() ? 'ws://localhost:3000/ws' : `ws://${window.location.host}/ws`
  ),
  
  // Logging configuration
  LOG_LEVEL: getEnvVar<string>(
    'VITE_LOG_LEVEL', 
    isDevelopment() ? 'debug' : 'error'
  ),
  
  // Development aids
  ENABLE_MOCK_API: getEnvVar<boolean>(
    'VITE_ENABLE_MOCK_API', 
    isDevelopment()  // Only enabled in development by default
  ),
  
  // UI refresh interval in milliseconds
  REFRESH_INTERVAL: getEnvVar<number>(
    'VITE_REFRESH_INTERVAL', 
    60000  // 1 minute default refresh
  ),
  
  // Analytics
  GOOGLE_ANALYTICS_ID: getEnvVar<string>(
    'VITE_GOOGLE_ANALYTICS_ID', 
    ''  // Empty by default, must be explicitly set
  ),
  
  // Current environment
  NODE_ENV: getEnvVar<string>(
    'VITE_NODE_ENV', 
    import.meta.env.MODE || 'development'
  ),
  
  // Feature flags to enable/disable functionality
  FEATURE_FLAGS: {
    ENABLE_SELF_HEALING_UI: getEnvVar<boolean>(
      'VITE_FEATURE_SELF_HEALING_UI', 
      true
    ),
    ENABLE_ML_MODEL_MANAGEMENT: getEnvVar<boolean>(
      'VITE_FEATURE_ML_MODEL_MANAGEMENT', 
      !isProduction()  // Only enabled in non-production by default
    ),
    ENABLE_ADVANCED_ANALYTICS: getEnvVar<boolean>(
      'VITE_FEATURE_ADVANCED_ANALYTICS', 
      true
    ),
    ENABLE_PIPELINE_BUILDER: getEnvVar<boolean>(
      'VITE_FEATURE_PIPELINE_BUILDER', 
      true
    ),
    ENABLE_QUALITY_DASHBOARD: getEnvVar<boolean>(
      'VITE_FEATURE_QUALITY_DASHBOARD', 
      true
    )
  } as FeatureFlags
};

// For backwards compatibility if any code uses NODE_ENV directly
export const NODE_ENV = ENV.NODE_ENV;
export const FEATURE_FLAGS = ENV.FEATURE_FLAGS;