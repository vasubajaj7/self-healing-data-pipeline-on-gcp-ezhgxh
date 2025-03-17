import { MessageHandler, type WebSocketMessage } from './messageHandler';
import { ReconnectionStrategy, type ReconnectionOptions } from './reconnectionStrategy';
import { ENV } from '../../config/env';
import { getToken } from '../../utils/auth';
import { logError } from '../../utils/errorHandling';

/**
 * Type definition for WebSocket event types
 */
export type WebSocketEventType = 'connected' | 'disconnected' | 'message' | 'error' | 'reconnecting' | 'reconnect_failed';

/**
 * Type definition for WebSocket event listeners
 */
export type WebSocketEventListener = (data?: any) => void;

/**
 * Configuration options for the WebSocket client
 */
export interface WebSocketClientOptions {
  /**
   * Whether to automatically reconnect when connection closes unexpectedly
   */
  reconnectOnClose?: boolean;
  
  /**
   * Timeout in milliseconds for initial connection attempt
   */
  connectionTimeout?: number;
  
  /**
   * Options for the reconnection strategy
   */
  reconnectionOptions?: ReconnectionOptions;
}

/**
 * Map of WebSocket event types to their data types
 */
export interface WebSocketEventMap {
  connected: void;
  disconnected: void;
  message: any;
  error: Error;
  reconnecting: number;
  reconnect_failed: void;
}

/**
 * Manages WebSocket connections with automatic reconnection, message handling, 
 * and event-based communication for the self-healing data pipeline web application.
 * 
 * @example
 * ```typescript
 * const wsClient = new WebSocketClient();
 * wsClient.addEventListener('connected', () => console.log('Connected!'));
 * wsClient.addEventListener('message', (data) => console.log('Received:', data));
 * wsClient.connect();
 * 
 * // Send a message
 * wsClient.sendMessage({ type: 'ping', payload: { timestamp: Date.now() }});
 * ```
 */
export class WebSocketClient {
  private url: string;
  private socket: WebSocket | null;
  private isConnected: boolean;
  private isConnecting: boolean;
  private reconnectOnClose: boolean;
  private messageHandler: MessageHandler;
  private reconnectionStrategy: ReconnectionStrategy;
  private eventListeners: Map<string, Function[]>;
  private reconnectTimeoutId: number | null;

  /**
   * Initializes a new WebSocketClient instance
   * 
   * @param url - Optional WebSocket server URL (defaults to environment config)
   * @param options - Optional configuration options
   */
  constructor(url?: string, options?: WebSocketClientOptions) {
    this.url = url || ENV.WEBSOCKET_URL;
    this.socket = null;
    this.isConnected = false;
    this.isConnecting = false;
    this.reconnectOnClose = options?.reconnectOnClose ?? true;
    this.messageHandler = new MessageHandler();
    this.reconnectionStrategy = new ReconnectionStrategy(options?.reconnectionOptions);
    this.eventListeners = new Map<string, Function[]>();
    this.reconnectTimeoutId = null;
  }

  /**
   * Establishes a WebSocket connection to the server
   * 
   * @returns Promise resolving to true if connection successful, false otherwise
   */
  public connect(): Promise<boolean> {
    // If already connected, return successful Promise
    if (this.isConnected && this.socket) {
      return Promise.resolve(true);
    }

    // If already connecting, return a Promise that tracks current connection attempt
    if (this.isConnecting) {
      return new Promise<boolean>((resolve) => {
        this.addEventListener('connected', () => resolve(true));
        this.addEventListener('error', () => resolve(false));
      });
    }

    this.isConnecting = true;

    return new Promise<boolean>((resolve, reject) => {
      try {
        // Create a new WebSocket connection
        this.socket = new WebSocket(this.url);

        // Add authentication token if available
        const token = getToken();
        if (token && this.url.startsWith('ws')) {
          // Append token as a query parameter
          const separator = this.url.includes('?') ? '&' : '?';
          this.socket = new WebSocket(`${this.url}${separator}token=${token}`);
        }

        // Set up event handlers
        this.socket.onopen = (event: Event) => {
          this.handleOpen(event);
          resolve(true);
        };

        this.socket.onclose = (event: CloseEvent) => {
          this.handleClose(event);
          if (!this.isConnected) {
            resolve(false);
          }
        };

        this.socket.onmessage = (event: MessageEvent) => {
          this.handleMessage(event);
        };

        this.socket.onerror = (event: Event) => {
          this.handleError(event);
          if (!this.isConnected) {
            reject(new Error('WebSocket connection failed'));
          }
        };

        // Set a connection timeout
        const connectionTimeout = setTimeout(() => {
          if (!this.isConnected) {
            if (this.socket) {
              this.socket.close();
              this.socket = null;
            }
            this.isConnecting = false;
            reject(new Error('WebSocket connection timeout'));
          }
        }, options?.connectionTimeout || 10000);

        // Clear the timeout when connection is established or fails
        this.addEventListener('connected', () => clearTimeout(connectionTimeout));
        this.addEventListener('error', () => clearTimeout(connectionTimeout));
      } catch (error) {
        this.isConnecting = false;
        logError(error, 'WebSocketClient.connect');
        reject(error);
      }
    });
  }

  /**
   * Closes the WebSocket connection
   */
  public disconnect(): void {
    // Clear any pending reconnect timeout
    if (this.reconnectTimeoutId !== null) {
      window.clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }

    // Close the socket if it exists and is connected
    if (this.socket) {
      // Use code 1000 (Normal Closure)
      this.socket.close(1000, 'Client initiated disconnect');
      this.socket = null;
    }

    this.isConnected = false;
    this.isConnecting = false;
    this.emitEvent('disconnected');
  }

  /**
   * Attempts to reconnect to the WebSocket server using the reconnection strategy
   */
  public reconnect(): void {
    // Clear any existing reconnect timeout
    if (this.reconnectTimeoutId !== null) {
      window.clearTimeout(this.reconnectTimeoutId);
      this.reconnectTimeoutId = null;
    }

    // If already connecting, don't try to reconnect again
    if (this.isConnecting) {
      return;
    }

    // Check if we have more reconnection attempts
    if (this.reconnectionStrategy.hasMoreAttempts()) {
      // Get delay from reconnection strategy
      const delay = this.reconnectionStrategy.getDelay();
      const attempt = this.reconnectionStrategy.getCurrentAttempt();
      
      // Emit reconnecting event with attempt information
      this.emitEvent('reconnecting', attempt);
      
      // Schedule reconnection attempt
      this.reconnectTimeoutId = window.setTimeout(() => {
        this.connect().catch(error => {
          logError(error, 'WebSocketClient.reconnect');
        });
      }, delay);
    } else {
      // No more reconnection attempts
      this.emitEvent('reconnect_failed');
      logError(
        `WebSocket reconnection failed after ${this.reconnectionStrategy.getMaxAttempts()} attempts`,
        'WebSocketClient.reconnect'
      );
    }
  }

  /**
   * Sends a message to the WebSocket server
   * 
   * @param data - Message data to send (object will be JSON stringified)
   * @returns True if message was sent successfully, false otherwise
   */
  public sendMessage(data: any): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      logError('Cannot send message - WebSocket is not connected', 'WebSocketClient.sendMessage');
      return false;
    }

    try {
      // Convert data to JSON string if it's an object
      const message = typeof data === 'object' ? JSON.stringify(data) : data;
      this.socket.send(message);
      return true;
    } catch (error) {
      logError(error, 'WebSocketClient.sendMessage');
      return false;
    }
  }

  /**
   * Registers an event listener for a specific event type
   * 
   * @param eventType - Type of event to listen for
   * @param callback - Function to call when event occurs
   */
  public addEventListener(eventType: WebSocketEventType, callback: WebSocketEventListener): void {
    const listeners = this.eventListeners.get(eventType) || [];
    listeners.push(callback);
    this.eventListeners.set(eventType, listeners);
  }

  /**
   * Removes an event listener for a specific event type
   * 
   * @param eventType - Type of event to remove listener from
   * @param callback - Function to remove from listeners
   * @returns True if listener was removed, false if not found
   */
  public removeEventListener(eventType: WebSocketEventType, callback: WebSocketEventListener): boolean {
    const listeners = this.eventListeners.get(eventType);
    
    if (!listeners) {
      return false;
    }
    
    const filteredListeners = listeners.filter(listener => listener !== callback);
    
    if (filteredListeners.length === listeners.length) {
      return false; // Listener not found
    }
    
    this.eventListeners.set(eventType, filteredListeners);
    return true;
  }

  /**
   * Registers a handler for a specific message type
   * 
   * @param messageType - Type of messages this handler should process
   * @param handler - Handler function to process messages of this type
   */
  public registerMessageHandler(messageType: string, handler: Function): void {
    this.messageHandler.registerHandler(messageType, handler);
  }

  /**
   * Removes a handler for a specific message type
   * 
   * @param messageType - Type of messages to unregister the handler for
   * @returns True if handler was removed, false if not found
   */
  public unregisterMessageHandler(messageType: string): boolean {
    return this.messageHandler.unregisterHandler(messageType);
  }

  /**
   * Emits an event to all registered listeners for the event type
   * 
   * @param eventType - Type of event to emit
   * @param data - Data to pass to event listeners
   * @private
   */
  private emitEvent(eventType: WebSocketEventType, data?: any): void {
    const listeners = this.eventListeners.get(eventType);
    
    if (listeners) {
      listeners.forEach(listener => {
        try {
          listener(data);
        } catch (error) {
          logError(error, `WebSocketClient.emitEvent(${eventType})`);
        }
      });
    }
  }

  /**
   * Handles the WebSocket open event
   * 
   * @param event - WebSocket open event
   * @private
   */
  private handleOpen(event: Event): void {
    this.isConnected = true;
    this.isConnecting = false;
    this.reconnectionStrategy.reset();
    this.emitEvent('connected');
    console.log('WebSocket connection established');
  }

  /**
   * Handles the WebSocket close event
   * 
   * @param event - WebSocket close event
   * @private
   */
  private handleClose(event: CloseEvent): void {
    this.isConnected = false;
    this.emitEvent('disconnected', { code: event.code, reason: event.reason });
    
    // Only attempt to reconnect if:
    // 1. reconnectOnClose is true
    // 2. Not a normal closure (code 1000)
    // 3. Not a client-initiated closure (we'll assume any code < 4000 that isn't 1000 is abnormal)
    if (this.reconnectOnClose && event.code !== 1000 && (event.code < 1000 || event.code >= 4000)) {
      this.reconnect();
    }
    
    console.log(`WebSocket connection closed: ${event.code} ${event.reason}`);
  }

  /**
   * Handles incoming WebSocket messages
   * 
   * @param event - WebSocket message event
   * @private
   */
  private handleMessage(event: MessageEvent): void {
    try {
      // Process the message using the message handler
      const processedMessage = this.messageHandler.processMessage(event.data);
      
      // Emit message event with the processed message
      this.emitEvent('message', processedMessage);
    } catch (error) {
      logError(error, 'WebSocketClient.handleMessage');
    }
  }

  /**
   * Handles WebSocket error events
   * 
   * @param event - WebSocket error event
   * @private
   */
  private handleError(event: Event): void {
    logError(event, 'WebSocketClient.handleError');
    this.emitEvent('error', new Error('WebSocket error'));
    
    // Update connected state if socket is closed
    if (this.socket && this.socket.readyState === WebSocket.CLOSED) {
      this.isConnected = false;
    }
  }

  /**
   * Get current connection status
   */
  public get connected(): boolean {
    return this.isConnected;
  }
}