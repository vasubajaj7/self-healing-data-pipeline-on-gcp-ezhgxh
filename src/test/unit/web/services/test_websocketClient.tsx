import { jest } from 'jest'; // ^29.5.0
import WS from 'jest-websocket-mock'; // ^2.4.0
import { waitFor } from '@testing-library/react'; // ^13.4.0
import { WebSocketClient } from '../../../web/src/services/websocket/websocketClient';
import { MessageHandler } from '../../../web/src/services/websocket/messageHandler';
import { ReconnectionStrategy } from '../../../web/src/services/websocket/reconnectionStrategy';
import { ENV } from '../../../web/src/config/env';

// Setup fake timers before each test
beforeEach(() => {
  jest.useFakeTimers();
});

// Cleanup after each test
afterEach(() => {
  jest.useRealTimers();
  jest.clearAllMocks();
});

/**
 * Creates a mock WebSocket implementation for testing
 * @returns Mock WebSocket implementation
 */
function createMockWebSocket() {
  const mockWebSocket = jest.fn().mockImplementation(() => {
    return {
      readyState: WebSocket.CONNECTING,
      send: jest.fn(),
      close: jest.fn(),
      onopen: null,
      onclose: null,
      onmessage: null,
      onerror: null
    };
  });
  
  return mockWebSocket;
}

describe('WebSocketClient', () => {
  test('should initialize with default values', () => {
    const client = new WebSocketClient();
    
    expect(client['url']).toBe(ENV.WEBSOCKET_URL);
    expect(client['socket']).toBeNull();
    expect(client['isConnected']).toBe(false);
    expect(client['isConnecting']).toBe(false);
    expect(client['reconnectOnClose']).toBe(true);
    expect(client['messageHandler']).toBeInstanceOf(MessageHandler);
    expect(client['reconnectionStrategy']).toBeInstanceOf(ReconnectionStrategy);
    expect(client['eventListeners']).toBeInstanceOf(Map);
    expect(client['eventListeners'].size).toBe(0);
  });

  test('should initialize with custom URL and options', () => {
    const customUrl = 'ws://custom-server.com/ws';
    const options = {
      reconnectOnClose: false,
      connectionTimeout: 5000,
      reconnectionOptions: {
        initialDelay: 2000,
        maxAttempts: 5,
        backoffFactor: 1.5
      }
    };
    
    const client = new WebSocketClient(customUrl, options);
    
    expect(client['url']).toBe(customUrl);
    expect(client['reconnectOnClose']).toBe(options.reconnectOnClose);
    // Verify reconnection strategy was initialized with custom options
    // Note: We can't directly inspect the private properties of reconnectionStrategy
    // but we could test its behavior in other tests
  });

  test('should connect to WebSocket server', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      const connectPromise = client.connect();
      
      // Verify WebSocket was instantiated with correct URL
      expect(mockWs).toHaveBeenCalledWith(ENV.WEBSOCKET_URL);
      expect(client['isConnecting']).toBe(true);
      
      // Simulate connection success
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      
      // Verify connection was successful
      const result = await connectPromise;
      expect(result).toBe(true);
      expect(client['isConnected']).toBe(true);
      expect(client['isConnecting']).toBe(false);
      
      // Verify connected event is emitted
      const connectedListener = jest.fn();
      client.addEventListener('connected', connectedListener);
      
      // Simulate another connection open to trigger the event again
      socketInstance.onopen(new Event('open'));
      expect(connectedListener).toHaveBeenCalled();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should handle connection failure', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Add error event listener
      const errorListener = jest.fn();
      client.addEventListener('error', errorListener);
      
      // Start connection
      const connectPromise = client.connect();
      
      // Simulate connection error
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.onerror(new Event('error'));
      
      // Expect rejection with error
      await expect(connectPromise).rejects.toThrow('WebSocket connection failed');
      expect(client['isConnected']).toBe(false);
      expect(errorListener).toHaveBeenCalled();
      
      // Verify reconnection attempt (default behavior)
      jest.runOnlyPendingTimers(); // Process reconnection timeout
      expect(mockWs).toHaveBeenCalledTimes(2); // Initial + reconnection attempt
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should disconnect from WebSocket server', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Add disconnected event listener
      const disconnectedListener = jest.fn();
      client.addEventListener('disconnected', disconnectedListener);
      
      // Disconnect
      client.disconnect();
      
      // Verify WebSocket.close was called
      expect(socketInstance.close).toHaveBeenCalledWith(1000, expect.any(String));
      expect(client['isConnected']).toBe(false);
      expect(disconnectedListener).toHaveBeenCalled();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should send messages when connected', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Test sending an object message
      const testObjectMessage = { type: 'test', payload: { value: 'test-data' } };
      const objectResult = client.sendMessage(testObjectMessage);
      
      // Verify message was sent and JSON stringified
      expect(objectResult).toBe(true);
      expect(socketInstance.send).toHaveBeenCalledWith(JSON.stringify(testObjectMessage));
      
      // Test sending a string message
      const testStringMessage = 'plain text message';
      const stringResult = client.sendMessage(testStringMessage);
      
      // Verify message was sent directly
      expect(stringResult).toBe(true);
      expect(socketInstance.send).toHaveBeenCalledWith(testStringMessage);
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should not send messages when disconnected', () => {
    const client = new WebSocketClient();
    
    // Prepare test message
    const testMessage = { type: 'test', payload: { value: 'test-data' } };
    
    // Try to send message without connecting
    const result = client.sendMessage(testMessage);
    
    // Verify message was not sent
    expect(result).toBe(false);
  });

  test('should handle incoming messages', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      // Mock MessageHandler.processMessage
      const processMessageSpy = jest.spyOn(MessageHandler.prototype, 'processMessage')
        .mockImplementation((data) => ({
          processed: true,
          originalData: data
        }));
      
      const client = new WebSocketClient();
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Add message event listener
      const messageListener = jest.fn();
      client.addEventListener('message', messageListener);
      
      // Simulate incoming message
      const testData = JSON.stringify({ type: 'test', data: 'test-data' });
      socketInstance.onmessage(new MessageEvent('message', { data: testData }));
      
      // Verify message was processed and event emitted
      expect(processMessageSpy).toHaveBeenCalledWith(testData);
      expect(messageListener).toHaveBeenCalledWith(expect.objectContaining({
        processed: true,
        originalData: testData
      }));
      
      // Cleanup
      processMessageSpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should register and unregister event listeners', () => {
    const client = new WebSocketClient();
    
    // Create listener
    const listener = jest.fn();
    
    // Register listener
    client.addEventListener('connected', listener);
    
    // Emit event
    client['emitEvent']('connected');
    
    // Verify listener was called
    expect(listener).toHaveBeenCalled();
    
    // Unregister listener
    const removeResult = client.removeEventListener('connected', listener);
    
    // Verify listener was removed
    expect(removeResult).toBe(true);
    
    // Reset mock and emit event again
    listener.mockClear();
    client['emitEvent']('connected');
    
    // Verify listener was not called
    expect(listener).not.toHaveBeenCalled();
  });

  test('should register and unregister message handlers', () => {
    const client = new WebSocketClient();
    
    // Mock MessageHandler methods
    const registerHandlerSpy = jest.spyOn(client['messageHandler'], 'registerHandler');
    const unregisterHandlerSpy = jest.spyOn(client['messageHandler'], 'unregisterHandler')
      .mockReturnValue(true);
    
    // Register handler
    const handler = jest.fn();
    client.registerMessageHandler('test-type', handler);
    
    // Verify handler was registered
    expect(registerHandlerSpy).toHaveBeenCalledWith('test-type', handler);
    
    // Unregister handler
    const result = client.unregisterMessageHandler('test-type');
    
    // Verify handler was unregistered
    expect(result).toBe(true);
    expect(unregisterHandlerSpy).toHaveBeenCalledWith('test-type');
    
    // Cleanup
    registerHandlerSpy.mockRestore();
    unregisterHandlerSpy.mockRestore();
  });

  test('should attempt reconnection on close', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Spy on reconnect method
      const reconnectSpy = jest.spyOn(client, 'reconnect');
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Reset mock for reconnection attempt
      mockWs.mockClear();
      
      // Simulate abnormal close (code 1006)
      socketInstance.onclose(new CloseEvent('close', { code: 1006, reason: 'Abnormal closure' }));
      
      // Verify reconnect was called
      expect(reconnectSpy).toHaveBeenCalled();
      
      // Advance timers to trigger reconnection (default initial delay is 1000ms)
      jest.advanceTimersByTime(1000);
      
      // Verify new WebSocket connection was attempted
      expect(mockWs).toHaveBeenCalledTimes(1);
      
      // Cleanup
      reconnectSpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should not attempt reconnection on normal close', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Spy on reconnect method
      const reconnectSpy = jest.spyOn(client, 'reconnect');
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Simulate normal close (code 1000)
      socketInstance.onclose(new CloseEvent('close', { code: 1000, reason: 'Normal closure' }));
      
      // Verify reconnect was not called
      expect(reconnectSpy).not.toHaveBeenCalled();
      
      // Cleanup
      reconnectSpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should not attempt reconnection when reconnectOnClose is false', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient(undefined, { reconnectOnClose: false });
      
      // Spy on reconnect method
      const reconnectSpy = jest.spyOn(client, 'reconnect');
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Simulate abnormal close
      socketInstance.onclose(new CloseEvent('close', { code: 1006, reason: 'Abnormal closure' }));
      
      // Verify reconnect was not called since reconnectOnClose is false
      expect(reconnectSpy).not.toHaveBeenCalled();
      
      // Cleanup
      reconnectSpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should use reconnection strategy for backoff', () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Mock reconnection strategy getDelay method
      const mockDelay = 2000; // Custom delay value in ms
      const getDelaySpy = jest.spyOn(client['reconnectionStrategy'], 'getDelay')
        .mockReturnValue(mockDelay);
      
      // Mock hasMoreAttempts to return true
      jest.spyOn(client['reconnectionStrategy'], 'hasMoreAttempts')
        .mockReturnValue(true);
      
      // Add reconnecting event listener
      const reconnectingListener = jest.fn();
      client.addEventListener('reconnecting', reconnectingListener);
      
      // Trigger reconnect
      client.reconnect();
      
      // Verify getDelay was called
      expect(getDelaySpy).toHaveBeenCalled();
      
      // Verify reconnecting event was emitted
      expect(reconnectingListener).toHaveBeenCalled();
      
      // Clear mocks for second test
      mockWs.mockClear();
      
      // Advance timers by less than the delay - should not trigger reconnect yet
      jest.advanceTimersByTime(mockDelay - 1);
      expect(mockWs).not.toHaveBeenCalled();
      
      // Advance remaining time to trigger reconnect
      jest.advanceTimersByTime(1);
      expect(mockWs).toHaveBeenCalledTimes(1);
      
      // Cleanup spies
      getDelaySpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });

  test('should emit reconnecting event during reconnection', () => {
    const client = new WebSocketClient();
    
    // Mock reconnectionStrategy methods
    jest.spyOn(client['reconnectionStrategy'], 'hasMoreAttempts')
      .mockReturnValue(true);
    
    jest.spyOn(client['reconnectionStrategy'], 'getCurrentAttempt')
      .mockReturnValue(2);
    
    // Add reconnecting event listener
    const reconnectingListener = jest.fn();
    client.addEventListener('reconnecting', reconnectingListener);
    
    // Trigger reconnect
    client.reconnect();
    
    // Verify reconnecting event was emitted with current attempt number
    expect(reconnectingListener).toHaveBeenCalledWith(2);
  });

  test('should emit reconnect_failed when max attempts reached', () => {
    const client = new WebSocketClient();
    
    // Mock hasMoreAttempts to return false
    jest.spyOn(client['reconnectionStrategy'], 'hasMoreAttempts')
      .mockReturnValue(false);
    
    jest.spyOn(client['reconnectionStrategy'], 'getMaxAttempts')
      .mockReturnValue(5);
    
    // Add reconnect_failed event listener
    const reconnectFailedListener = jest.fn();
    client.addEventListener('reconnect_failed', reconnectFailedListener);
    
    // Trigger reconnect
    client.reconnect();
    
    // Verify reconnect_failed event was emitted
    expect(reconnectFailedListener).toHaveBeenCalled();
  });

  test('should reset reconnection strategy on successful connection', async () => {
    // Save original WebSocket constructor
    const originalWebSocket = global.WebSocket;
    const mockWs = createMockWebSocket();
    global.WebSocket = mockWs as any;
    
    try {
      const client = new WebSocketClient();
      
      // Spy on reconnectionStrategy.reset
      const resetSpy = jest.spyOn(client['reconnectionStrategy'], 'reset');
      
      // Connect and simulate successful connection
      const connectPromise = client.connect();
      const socketInstance = mockWs.mock.results[0].value;
      socketInstance.readyState = WebSocket.OPEN;
      socketInstance.onopen(new Event('open'));
      await connectPromise;
      
      // Verify reset was called on successful connection
      expect(resetSpy).toHaveBeenCalled();
      
      // Cleanup
      resetSpy.mockRestore();
    } finally {
      // Restore original WebSocket
      global.WebSocket = originalWebSocket;
    }
  });
});