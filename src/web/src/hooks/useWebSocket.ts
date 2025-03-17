import { useState, useEffect, useRef, useCallback } from 'react'; // ^18.2.0
import { WebSocketClient } from '../services/websocket/websocketClient';
import { ENV } from '../config/env';
import { useInterval } from './useInterval';
import { logError } from '../utils/errorHandling';

/**
 * Enum representing possible WebSocket connection states
 */
export enum WebSocketConnectionStatus {
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  DISCONNECTED = 'disconnected',
  RECONNECTING = 'reconnecting'
}

// Type definitions for event handlers
type WebSocketEventHandler = (data: any) => void;
type WebSocketEventMap = Record<string, WebSocketEventHandler[]>;

/**
 * Configuration options for the WebSocket hook
 */
interface WebSocketOptions {
  /**
   * Custom WebSocket server URL (defaults to ENV.WEBSOCKET_URL)
   */
  url?: string;
  
  /**
   * Whether to automatically connect when the component mounts
   * @default true
   */
  autoConnect?: boolean;
  
  /**
   * Whether to automatically reconnect when connection closes unexpectedly
   * @default true
   */
  reconnectOnClose?: boolean;
  
  /**
   * Interval in milliseconds to check connection health
   * @default null (disabled)
   */
  healthCheckInterval?: number;
  
  /**
   * Maximum number of reconnection attempts before giving up
   */
  maxReconnectAttempts?: number;
  
  /**
   * Initial event handlers to register
   */
  eventHandlers?: Record<string, (data: any) => void>;
}

/**
 * Return value of the useWebSocket hook
 */
interface WebSocketHookResult {
  /**
   * Current connection status
   */
  connectionStatus: WebSocketConnectionStatus;
  
  /**
   * Most recently received message
   */
  lastMessage: any | null;
  
  /**
   * Current error state, if any
   */
  error: Error | null;
  
  /**
   * Initiates a WebSocket connection
   * @returns Promise resolving to true if connection succeeded, false otherwise
   */
  connect: () => Promise<boolean>;
  
  /**
   * Closes the WebSocket connection
   */
  disconnect: () => void;
  
  /**
   * Sends data through the WebSocket connection
   * @param data - Data to send (objects will be JSON stringified)
   * @returns true if sent successfully, false if connection is closed or error occurs
   */
  sendMessage: (data: any) => boolean;
  
  /**
   * Subscribes to a specific message type
   * @param eventType - Type of messages to listen for (matches message.type)
   * @param callback - Function to call when message of this type is received
   */
  subscribe: (eventType: string, callback: (data: any) => void) => void;
  
  /**
   * Unsubscribes from a specific message type
   * @param eventType - Type of messages to stop listening for
   * @param callback - Function to remove from listeners
   */
  unsubscribe: (eventType: string, callback: (data: any) => void) => void;
}

/**
 * A custom React hook that provides a declarative way to use WebSocket connections in React components.
 * It manages WebSocket connection lifecycle, handles reconnection logic, and provides methods
 * for sending messages and subscribing to different message types.
 * 
 * @example
 * ```tsx
 * const { 
 *   connectionStatus, 
 *   lastMessage, 
 *   error, 
 *   sendMessage, 
 *   subscribe 
 * } = useWebSocket({
 *   autoConnect: true,
 *   healthCheckInterval: 30000
 * });
 * 
 * // Subscribe to specific message types
 * useEffect(() => {
 *   subscribe('pipeline_status', (data) => {
 *     console.log('Pipeline status update:', data);
 *   });
 * }, [subscribe]);
 * 
 * // Send a message
 * const sendPing = () => {
 *   sendMessage({ type: 'ping', timestamp: Date.now() });
 * };
 * ```
 * 
 * @param options - Configuration options for the WebSocket hook
 * @returns Object containing WebSocket state and methods
 */
export function useWebSocket(options?: WebSocketOptions): WebSocketHookResult {
  // Initialize WebSocketClient instance with URL from options or environment
  const wsClient = useRef<WebSocketClient>(
    new WebSocketClient(options?.url || ENV.WEBSOCKET_URL, {
      reconnectOnClose: options?.reconnectOnClose ?? true,
      reconnectionOptions: {
        maxAttempts: options?.maxReconnectAttempts
      }
    })
  );
  
  // State for connection status, last message, and error
  const [connectionStatus, setConnectionStatus] = useState<WebSocketConnectionStatus>(
    WebSocketConnectionStatus.DISCONNECTED
  );
  const [lastMessage, setLastMessage] = useState<any | null>(null);
  const [error, setError] = useState<Error | null>(null);
  
  // Refs for event handlers to prevent unnecessary re-renders
  const eventHandlersRef = useRef<WebSocketEventMap>({});
  
  // Connect to WebSocket
  const connect = useCallback(async (): Promise<boolean> => {
    try {
      setConnectionStatus(WebSocketConnectionStatus.CONNECTING);
      const connected = await wsClient.current.connect();
      
      if (connected) {
        setConnectionStatus(WebSocketConnectionStatus.CONNECTED);
        setError(null);
      } else {
        setConnectionStatus(WebSocketConnectionStatus.DISCONNECTED);
        setError(new Error('Failed to connect to WebSocket server'));
      }
      
      return connected;
    } catch (err) {
      setConnectionStatus(WebSocketConnectionStatus.DISCONNECTED);
      setError(err instanceof Error ? err : new Error('Unknown error connecting to WebSocket'));
      logError(err, 'useWebSocket.connect');
      return false;
    }
  }, []);
  
  // Disconnect from WebSocket
  const disconnect = useCallback((): void => {
    wsClient.current.disconnect();
    setConnectionStatus(WebSocketConnectionStatus.DISCONNECTED);
  }, []);
  
  // Send message through WebSocket
  const sendMessage = useCallback((data: any): boolean => {
    try {
      return wsClient.current.sendMessage(data);
    } catch (err) {
      logError(err, 'useWebSocket.sendMessage');
      return false;
    }
  }, []);
  
  // Subscribe to an event type
  const subscribe = useCallback((eventType: string, callback: WebSocketEventHandler): void => {
    const handlers = eventHandlersRef.current[eventType] || [];
    if (!handlers.includes(callback)) {
      handlers.push(callback);
      eventHandlersRef.current[eventType] = handlers;
    }
  }, []);
  
  // Unsubscribe from an event type
  const unsubscribe = useCallback((eventType: string, callback: WebSocketEventHandler): void => {
    const handlers = eventHandlersRef.current[eventType] || [];
    const index = handlers.indexOf(callback);
    if (index !== -1) {
      handlers.splice(index, 1);
      eventHandlersRef.current[eventType] = handlers;
    }
  }, []);
  
  // Message handler
  const handleMessage = useCallback((data: any) => {
    setLastMessage(data);
    
    // If data has a type field, trigger type-specific handlers
    if (data && typeof data === 'object' && data.type && typeof data.type === 'string') {
      const eventType = data.type;
      const handlers = eventHandlersRef.current[eventType] || [];
      handlers.forEach(handler => {
        try {
          handler(data);
        } catch (err) {
          logError(err, `useWebSocket.handleMessage.${eventType}`);
        }
      });
    }
  }, []);
  
  // Connected handler
  const handleConnected = useCallback(() => {
    setConnectionStatus(WebSocketConnectionStatus.CONNECTED);
    setError(null);
  }, []);
  
  // Disconnected handler
  const handleDisconnected = useCallback(() => {
    setConnectionStatus(WebSocketConnectionStatus.DISCONNECTED);
  }, []);
  
  // Reconnecting handler
  const handleReconnecting = useCallback(() => {
    setConnectionStatus(WebSocketConnectionStatus.RECONNECTING);
  }, []);
  
  // Error handler
  const handleError = useCallback((err: Error) => {
    setError(err);
    logError(err, 'useWebSocket.handleError');
  }, []);
  
  // Set up WebSocket event listeners on mount and clean up on unmount
  useEffect(() => {
    const client = wsClient.current;
    
    // Add event listeners
    client.addEventListener('message', handleMessage);
    client.addEventListener('connected', handleConnected);
    client.addEventListener('disconnected', handleDisconnected);
    client.addEventListener('reconnecting', handleReconnecting);
    client.addEventListener('error', handleError);
    
    // Set up event handlers from options
    if (options?.eventHandlers) {
      Object.entries(options.eventHandlers).forEach(([eventType, handler]) => {
        subscribe(eventType, handler);
      });
    }
    
    // Connect automatically if specified (or by default)
    if (options?.autoConnect !== false) {
      connect().catch(err => logError(err, 'useWebSocket.autoConnect'));
    }
    
    // Clean up when unmounting
    return () => {
      client.removeEventListener('message', handleMessage);
      client.removeEventListener('connected', handleConnected);
      client.removeEventListener('disconnected', handleDisconnected);
      client.removeEventListener('reconnecting', handleReconnecting);
      client.removeEventListener('error', handleError);
      
      // Disconnect if the connection is still active
      if (client.connected) {
        client.disconnect();
      }
    };
  }, [
    connect, 
    handleMessage, 
    handleConnected, 
    handleDisconnected, 
    handleReconnecting, 
    handleError, 
    options, 
    subscribe
  ]);
  
  // Set up health check interval if specified
  useInterval(() => {
    // Only check health if we think we're connected
    if (connectionStatus === WebSocketConnectionStatus.CONNECTED) {
      // If client reports as not connected but our state thinks we are, try to reconnect
      if (!wsClient.current.connected) {
        setConnectionStatus(WebSocketConnectionStatus.RECONNECTING);
        connect().catch(err => logError(err, 'useWebSocket.healthCheck'));
      }
    }
  }, options?.healthCheckInterval || null);
  
  // Return hook result object
  return {
    connectionStatus,
    lastMessage,
    error,
    connect,
    disconnect,
    sendMessage,
    subscribe,
    unsubscribe
  };
}