import { setupServer } from 'msw/node'; // ^1.2.1
import { handlers } from './handlers';

/**
 * Creates a Mock Service Worker (MSW) server instance for intercepting API requests during tests.
 * This server simulates backend API responses, allowing frontend components to be tested
 * without requiring a real backend server.
 *
 * The server is configured with all handlers defined in the handlers.ts file,
 * which includes mock implementations for all API endpoints used by the application.
 *
 * @example
 * // In test setup file:
 * beforeAll(() => server.listen())
 * afterEach(() => server.resetHandlers())
 * afterAll(() => server.close())
 */
export const server = setupServer(...handlers);