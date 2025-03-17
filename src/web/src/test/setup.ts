import '@testing-library/jest-dom'; // ^5.16.5
import { server, listen, close, resetHandlers } from './mocks/server';

/**
 * Sets up a mock implementation for localStorage in the test environment
 * This is necessary because Jest uses jsdom which doesn't fully implement localStorage
 */
function setupLocalStorageMock(): void {
  const mockStorage: Record<string, string> = {};
  
  Object.defineProperty(window, 'localStorage', {
    value: {
      getItem: (key: string) => mockStorage[key] || null,
      setItem: (key: string, value: string) => {
        mockStorage[key] = value.toString();
      },
      removeItem: (key: string) => {
        delete mockStorage[key];
      },
      clear: () => {
        Object.keys(mockStorage).forEach(key => {
          delete mockStorage[key];
        });
      }
    },
    writable: true
  });
}

// Set up mock for localStorage
setupLocalStorageMock();

// Start server before all tests
// Using 'error' for onUnhandledRequest to ensure all API requests are properly mocked
global.beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));

// Reset handlers after each test to ensure isolated test environment
global.afterEach(() => server.resetHandlers());

// Clean up after all tests are done, preventing memory leaks
global.afterAll(() => server.close());