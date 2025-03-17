/**
 * Application Constants
 * 
 * This file contains application-wide constants used for configuration
 * such as API settings, timing parameters, and default values.
 */

// API Request Configuration
export const MAX_RETRIES = 3;
export const RETRY_DELAY = 1000; // 1 second base delay (will increase with exponential backoff)
export const DEFAULT_TIMEOUT = 30000; // 30 seconds
export const API_VERSION = 'v1';

// Refresh Intervals (milliseconds)
export const REFRESH_INTERVALS = {
  DASHBOARD: 60000, // 1 minute
  PIPELINE_STATUS: 30000, // 30 seconds
  ALERTS: 15000, // 15 seconds
  MONITORING: 10000 // 10 seconds
};

// UI Interaction Delays
export const DEBOUNCE_DELAY = 300; // 300ms for debouncing user input
export const THROTTLE_DELAY = 100; // 100ms for throttling frequent events

// Notification Settings
export const TOAST_DURATION = {
  SHORT: 2000, // 2 seconds
  MEDIUM: 5000, // 5 seconds
  LONG: 10000 // 10 seconds
};

// Animation Settings
export const ANIMATION_DURATION = {
  SHORT: 150, // 150ms
  MEDIUM: 300, // 300ms
  LONG: 500 // 500ms
};

// WebSocket Configuration
export const WEBSOCKET_RECONNECT_DELAY = 2000; // 2 seconds
export const WEBSOCKET_MAX_RECONNECT_ATTEMPTS = 5;

// Storage Settings
export const LOCAL_STORAGE_PREFIX = 'shp_'; // Self-Healing Pipeline prefix

// Default Application Preferences
export const DEFAULT_LANGUAGE = 'en';
export const DEFAULT_THEME = 'light';
export const DEFAULT_DATE_FORMAT = 'MMM dd, yyyy';
export const DEFAULT_TIME_FORMAT = 'HH:mm:ss';

// UI Settings
export const CHART_ANIMATION_ENABLED = true;
export const MAX_ITEMS_PER_PAGE = 25;
export const DEFAULT_ERROR_MESSAGE = 'An unexpected error occurred. Please try again later.';

// Session Settings
export const IDLE_TIMEOUT = 1800000; // 30 minutes