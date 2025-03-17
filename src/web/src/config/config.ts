/**
 * Main Configuration
 * 
 * This module centralizes configuration for the self-healing data pipeline web application.
 * It combines environment-specific settings, constants, and feature flags into a unified
 * configuration object that can be imported throughout the application.
 */

import { merge } from 'lodash'; // version ^4.17.21

// Import environment-specific configuration
import { 
  ENV, 
  isProduction, 
  isDevelopment 
} from './env';

// Import API configuration
import { apiConfig } from './apiConfig';

// Import chart configuration
import { 
  chartDefaults,
  chartColorSchemes,
  chartAnimations,
  getChartConfig,
  getChartColorScheme,
  getChartAnimation,
  getResponsiveOptions,
  tooltipDefaults,
  legendDefaults,
  responsiveOptions
} from './chartConfig';

// Import application constants
import {
  MAX_RETRIES,
  RETRY_DELAY,
  DEFAULT_TIMEOUT,
  REFRESH_INTERVALS,
  DEFAULT_LANGUAGE,
  DEFAULT_THEME,
  DEBOUNCE_DELAY,
  THROTTLE_DELAY,
  TOAST_DURATION,
  ANIMATION_DURATION,
  API_VERSION,
  WEBSOCKET_RECONNECT_DELAY,
  WEBSOCKET_MAX_RECONNECT_ATTEMPTS,
  LOCAL_STORAGE_PREFIX,
  DEFAULT_DATE_FORMAT,
  DEFAULT_TIME_FORMAT,
  CHART_ANIMATION_ENABLED,
  MAX_ITEMS_PER_PAGE,
  DEFAULT_ERROR_MESSAGE,
  IDLE_TIMEOUT
} from './constants';

/**
 * Base application configuration object
 * This combines all configuration sources into a single, structured object
 */
const baseConfig = {
  // API-related configuration
  api: {
    baseUrl: ENV.API_BASE_URL,
    timeout: ENV.API_TIMEOUT || DEFAULT_TIMEOUT,
    version: API_VERSION,
    retries: MAX_RETRIES,
    retryDelay: RETRY_DELAY,
    config: apiConfig
  },
  
  // Chart visualization configuration
  charts: {
    defaults: chartDefaults,
    colorSchemes: chartColorSchemes,
    animations: chartAnimations,
    tooltips: tooltipDefaults,
    legends: legendDefaults,
    responsive: responsiveOptions,
    animationEnabled: CHART_ANIMATION_ENABLED,
    getConfig: getChartConfig,
    getColorScheme: getChartColorScheme,
    getAnimation: getChartAnimation,
    getResponsiveOptions: getResponsiveOptions
  },
  
  // Environment-specific settings
  environment: {
    isProd: isProduction(),
    isDev: isDevelopment(),
    env: ENV.NODE_ENV,
    mockApi: ENV.ENABLE_MOCK_API || false,
    analytics: {
      googleAnalyticsId: ENV.GOOGLE_ANALYTICS_ID || ''
    }
  },
  
  // Feature flags for enabling/disabling functionality
  features: {
    ...ENV.FEATURE_FLAGS,
  },
  
  // UI configuration
  ui: {
    theme: DEFAULT_THEME,
    language: DEFAULT_LANGUAGE,
    refreshIntervals: REFRESH_INTERVALS,
    dateFormat: DEFAULT_DATE_FORMAT,
    timeFormat: DEFAULT_TIME_FORMAT,
    maxItemsPerPage: MAX_ITEMS_PER_PAGE,
    defaultErrorMessage: DEFAULT_ERROR_MESSAGE,
    interaction: {
      debounceDelay: DEBOUNCE_DELAY,
      throttleDelay: THROTTLE_DELAY
    },
    toast: TOAST_DURATION,
    animation: ANIMATION_DURATION,
    localStorage: {
      prefix: LOCAL_STORAGE_PREFIX
    }
  },
  
  // Authentication configuration
  auth: {
    enabled: ENV.AUTH_ENABLED,
    tokenKey: 'auth_token',
    refreshTokenKey: 'refresh_token',
    idleTimeout: IDLE_TIMEOUT
  },
  
  // Monitoring and alerting configuration
  monitoring: {
    refreshIntervals: REFRESH_INTERVALS,
    logLevel: ENV.LOG_LEVEL || 'error',
    websocket: {
      url: ENV.WEBSOCKET_URL,
      reconnectDelay: WEBSOCKET_RECONNECT_DELAY,
      maxReconnectAttempts: WEBSOCKET_MAX_RECONNECT_ATTEMPTS
    }
  }
};

/**
 * Get application configuration, optionally merged with custom overrides
 * 
 * @param customConfig - Optional custom configuration to merge with base configuration
 * @returns Combined configuration object
 */
export const getConfig = (customConfig?: Record<string, any>) => {
  if (customConfig) {
    return merge({}, baseConfig, customConfig);
  }
  return baseConfig;
};

// Export the configuration object
export const config = baseConfig;

// Default export for easier importing
export default config;