/// <reference types="vite/client" />

/**
 * Interface for Vite's import.meta.env object that contains environment variables
 * These are injected by Vite at build time
 */
interface ImportMetaEnv {
  /**
   * The mode the app is running in (development, production, etc.)
   */
  readonly MODE: string;
  
  /**
   * Whether the app is running in development mode
   */
  readonly DEV: boolean;
  
  /**
   * Whether the app is running in production mode
   */
  readonly PROD: boolean;
  
  /**
   * Whether the app is running in SSR mode
   */
  readonly SSR: boolean;
  
  /**
   * The base URL the app is being served from
   */
  readonly BASE_URL: string;
  
  /**
   * The base URL for API requests
   */
  readonly VITE_API_BASE_URL: string;
  
  /**
   * Timeout for API requests in milliseconds
   */
  readonly VITE_API_TIMEOUT: string;
  
  /**
   * Whether authentication is enabled
   */
  readonly VITE_AUTH_ENABLED: string;
  
  /**
   * WebSocket server URL for real-time updates
   */
  readonly VITE_WEBSOCKET_URL: string;
  
  /**
   * Application logging level
   */
  readonly VITE_LOG_LEVEL: string;
  
  /**
   * Flag to enable mock API responses for development
   */
  readonly VITE_ENABLE_MOCK_API: string;
  
  /**
   * Dashboard refresh interval in milliseconds
   */
  readonly VITE_REFRESH_INTERVAL: string;
  
  /**
   * Google Analytics tracking ID
   */
  readonly VITE_GOOGLE_ANALYTICS_ID: string;
  
  /**
   * Node environment (development, production, test)
   */
  readonly VITE_NODE_ENV: string;
  
  /**
   * Feature flag: Enable WebSocket connections for real-time updates
   */
  readonly VITE_FEATURE_FLAG_ENABLE_WEBSOCKETS: string;
  
  /**
   * Feature flag: Enable notifications system
   */
  readonly VITE_FEATURE_FLAG_ENABLE_NOTIFICATIONS: string;
  
  /**
   * Feature flag: Enable dark mode UI option
   */
  readonly VITE_FEATURE_FLAG_ENABLE_DARK_MODE: string;
  
  /**
   * Feature flag: Enable analytics tracking
   */
  readonly VITE_FEATURE_FLAG_ENABLE_ANALYTICS: string;
  
  /**
   * Feature flag: Enable advanced charting features
   */
  readonly VITE_FEATURE_FLAG_ENABLE_ADVANCED_CHARTS: string;
}

/**
 * Extends the ImportMeta interface to include Vite's env object
 */
interface ImportMeta {
  readonly env: ImportMetaEnv;
}

/**
 * Application version constant injected by Vite's define configuration
 */
declare const __APP_VERSION__: string;