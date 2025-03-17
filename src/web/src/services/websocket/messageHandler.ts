import { logError } from '../../utils/errorHandling';

/**
 * Structure of a WebSocket message with type-based routing
 */
interface WebSocketMessage {
  type: string;
  payload: any;
  id?: string;
  timestamp?: number;
}

/**
 * Type definition for message handler functions
 */
type MessageHandlerFunction = (data: any) => any;

/**
 * Handles WebSocket message processing, routing, and handler registration
 * for the self-healing data pipeline web application
 */
class MessageHandler {
  // Map to store message type-specific handlers
  private handlers: Map<string, Function>;
  // Default handler for messages without a specific handler
  private defaultHandler: Function | null;

  /**
   * Initializes a new MessageHandler instance
   * @param defaultHandler Optional default handler for messages without a specific type handler
   */
  constructor(defaultHandler?: Function | null | undefined) {
    this.handlers = new Map<string, Function>();
    this.defaultHandler = defaultHandler || null;
  }

  /**
   * Processes an incoming WebSocket message and routes it to the appropriate handler
   * @param data Message data to process (can be string JSON or object)
   * @returns Result from the handler or the original data if no handler found
   */
  processMessage(data: any): any {
    try {
      // Parse the message if it's a string
      const message = typeof data === 'string' ? JSON.parse(data) : data;
      
      // Extract the message type
      const messageType = message?.type;
      
      if (!messageType) {
        // If no message type is present and we have a default handler, use it
        return this.defaultHandler ? this.defaultHandler(message) : message;
      }
      
      // Find the handler for this message type
      const handler = this.handlers.get(messageType);
      
      if (handler) {
        // Execute the handler with the message
        return handler(message);
      } else if (this.defaultHandler) {
        // Use the default handler if available
        return this.defaultHandler(message);
      }
      
      // If no handler found, return the original message
      return message;
    } catch (error) {
      // Log any errors during processing
      logError(error, 'MessageHandler.processMessage');
      // Return the original data in case of error
      return data;
    }
  }

  /**
   * Registers a handler function for a specific message type
   * @param messageType Type of messages this handler should process
   * @param handler Handler function to process messages of this type
   */
  registerHandler(messageType: string, handler: Function): void {
    this.handlers.set(messageType, handler);
  }

  /**
   * Removes a handler for a specific message type
   * @param messageType Type of messages to unregister the handler for
   * @returns True if a handler was removed, false if no handler existed for the message type
   */
  unregisterHandler(messageType: string): boolean {
    return this.handlers.delete(messageType);
  }

  /**
   * Sets the default handler for messages without a specific type handler
   * @param handler Handler function or null to remove the default handler
   */
  setDefaultHandler(handler: Function | null): void {
    this.defaultHandler = handler;
  }

  /**
   * Removes all registered message handlers
   */
  clearHandlers(): void {
    this.handlers.clear();
    this.defaultHandler = null;
  }
}

export { MessageHandler };
export type { WebSocketMessage, MessageHandlerFunction };