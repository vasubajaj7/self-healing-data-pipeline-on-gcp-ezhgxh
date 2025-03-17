/**
 * Provides authentication-related fixtures and mock implementations for testing
 * React components that require authentication context in the self-healing data pipeline web application.
 */

import { jest } from '@jest/globals'; // ^29.5.0
import React, { ReactNode } from 'react'; // ^18.2.0

// Import authentication context, provider, and hook for mocking
import { 
  AuthContext, 
  AuthProvider, 
  useAuth 
} from '../../web/src/contexts/AuthContext';

// Import user and authentication type definitions
import {
  User, 
  UserRole, 
  UserPermission, 
  LoginCredentials,
  LoginResponse, 
  MfaRequest, 
  AuthState
} from '../../web/src/types/user';

// Import authentication service for mocking
import authService from '../../web/src/services/api/authService';

// Import mock user data for authentication fixtures
import { MOCK_USER_DATA } from './api_fixtures';

/**
 * Props for the MockAuthProvider component
 */
interface MockAuthProviderProps {
  children: ReactNode;
  authState?: Partial<AuthState>;
  mockMethods?: object;
}

/**
 * Options for configuring mock authentication methods
 */
interface MockAuthMethodOptions {
  login?: jest.Mock | { success: boolean, response: LoginResponse, error?: Error };
  logout?: jest.Mock | { success: boolean, error?: Error };
  verifyMfa?: jest.Mock | { success: boolean, response: LoginResponse, error?: Error };
  refreshToken?: jest.Mock | { success: boolean, error?: Error };
  getUserProfile?: jest.Mock | { success: boolean, response: User, error?: Error };
  checkPermission?: jest.Mock | { result: boolean };
  checkRole?: jest.Mock | { result: boolean };
}

/**
 * Collection of mock users for testing authentication
 */
export const MOCK_USERS: User[] = [
  {
    id: 'user-admin',
    username: 'admin',
    email: 'admin@example.com',
    firstName: 'Admin',
    lastName: 'User',
    role: UserRole.ADMIN,
    isActive: true,
    mfaEnabled: true,
    lastLogin: '2023-06-15T08:30:00Z',
    createdAt: '2023-01-01T00:00:00Z',
    updatedAt: '2023-06-15T08:30:00Z'
  },
  {
    id: 'user-engineer',
    username: 'engineer',
    email: 'engineer@example.com',
    firstName: 'Jane',
    lastName: 'Engineer',
    role: UserRole.DATA_ENGINEER,
    isActive: true,
    mfaEnabled: false,
    lastLogin: '2023-06-15T09:15:00Z',
    createdAt: '2023-01-05T00:00:00Z',
    updatedAt: '2023-06-15T09:15:00Z'
  },
  {
    id: 'user-analyst',
    username: 'analyst',
    email: 'analyst@example.com',
    firstName: 'John',
    lastName: 'Analyst',
    role: UserRole.DATA_ANALYST,
    isActive: true,
    mfaEnabled: false,
    lastLogin: '2023-06-14T16:45:00Z',
    createdAt: '2023-01-10T00:00:00Z',
    updatedAt: '2023-06-14T16:45:00Z'
  },
  {
    id: 'user-operator',
    username: 'operator',
    email: 'operator@example.com',
    firstName: 'Sam',
    lastName: 'Operator',
    role: UserRole.PIPELINE_OPERATOR,
    isActive: true,
    mfaEnabled: false,
    lastLogin: '2023-06-14T14:30:00Z',
    createdAt: '2023-02-01T00:00:00Z',
    updatedAt: '2023-06-14T14:30:00Z'
  },
  {
    id: 'user-viewer',
    username: 'viewer',
    email: 'viewer@example.com',
    firstName: 'Alex',
    lastName: 'Viewer',
    role: UserRole.VIEWER,
    isActive: true,
    mfaEnabled: false,
    lastLogin: '2023-06-13T11:20:00Z',
    createdAt: '2023-02-15T00:00:00Z',
    updatedAt: '2023-06-13T11:20:00Z'
  }
];

/**
 * Predefined authentication states for common testing scenarios
 */
export const MOCK_AUTH_STATES: Record<string, AuthState> = {
  authenticated: {
    isAuthenticated: true,
    user: MOCK_USERS[1], // Data Engineer
    permissions: [
      UserPermission.VIEW_DASHBOARD,
      UserPermission.VIEW_PIPELINES,
      UserPermission.MANAGE_PIPELINES,
      UserPermission.VIEW_QUALITY,
      UserPermission.MANAGE_QUALITY,
      UserPermission.VIEW_HEALING,
      UserPermission.VIEW_ALERTS,
      UserPermission.VIEW_CONFIGURATION,
      UserPermission.MANAGE_CONFIGURATION
    ],
    loading: false,
    error: null
  },
  authenticatedAdmin: {
    isAuthenticated: true,
    user: MOCK_USERS[0], // Admin
    permissions: Object.values(UserPermission), // All permissions
    loading: false,
    error: null
  },
  authenticatedViewer: {
    isAuthenticated: true,
    user: MOCK_USERS[4], // Viewer
    permissions: [
      UserPermission.VIEW_DASHBOARD,
      UserPermission.VIEW_PIPELINES,
      UserPermission.VIEW_QUALITY,
      UserPermission.VIEW_ALERTS
    ],
    loading: false,
    error: null
  },
  unauthenticated: {
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: false,
    error: null
  },
  loading: {
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: true,
    error: null
  },
  error: {
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: false,
    error: 'Authentication failed. Please check your credentials and try again.'
  },
  mfaRequired: {
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: false,
    error: null
  }
};

/**
 * Default mock implementations of authentication methods
 */
const DEFAULT_AUTH_METHODS = {
  login: jest.fn().mockImplementation(async () => {
    return {
      user: MOCK_USERS[1],
      token: 'mock-token',
      refreshToken: 'mock-refresh-token',
      expiresAt: Date.now() + 3600000, // 1 hour from now
      requiresMfa: false
    };
  }),
  logout: jest.fn().mockImplementation(async () => {}),
  verifyMfa: jest.fn().mockImplementation(async () => {
    return {
      user: MOCK_USERS[1],
      token: 'mock-token',
      refreshToken: 'mock-refresh-token',
      expiresAt: Date.now() + 3600000, // 1 hour from now
      requiresMfa: false
    };
  }),
  refreshToken: jest.fn().mockImplementation(async () => true),
  getUserProfile: jest.fn().mockImplementation(async () => MOCK_USERS[1]),
  checkPermission: jest.fn().mockImplementation(() => true),
  checkRole: jest.fn().mockImplementation(() => true)
};

/**
 * Creates a mock authentication state for testing
 * 
 * @param overrides - Optional overrides for the default unauthenticated state
 * @returns Mock authentication state with default values and any provided overrides
 */
export function createMockAuthState(overrides: Partial<AuthState> = {}): AuthState {
  const defaultState: AuthState = {
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: false,
    error: null
  };
  
  return {
    ...defaultState,
    ...overrides
  };
}

/**
 * Creates a mock authentication context value for testing
 * 
 * @param overrides - Optional overrides for the auth context state and methods
 * @returns Mock authentication context with state and methods
 */
export function createMockAuthContext(overrides: Partial<{
  authState: Partial<AuthState>,
  methods: Partial<typeof DEFAULT_AUTH_METHODS>
}> = {}) {
  const mockState = createMockAuthState(overrides.authState || {});
  
  return {
    isAuthenticated: mockState.isAuthenticated,
    user: mockState.user,
    permissions: mockState.permissions,
    loading: mockState.loading,
    error: mockState.error,
    login: overrides.methods?.login || DEFAULT_AUTH_METHODS.login,
    logout: overrides.methods?.logout || DEFAULT_AUTH_METHODS.logout,
    verifyMfa: overrides.methods?.verifyMfa || DEFAULT_AUTH_METHODS.verifyMfa,
    refreshToken: overrides.methods?.refreshToken || DEFAULT_AUTH_METHODS.refreshToken,
    getUserProfile: overrides.methods?.getUserProfile || DEFAULT_AUTH_METHODS.getUserProfile,
    checkPermission: overrides.methods?.checkPermission || DEFAULT_AUTH_METHODS.checkPermission,
    checkRole: overrides.methods?.checkRole || DEFAULT_AUTH_METHODS.checkRole
  };
}

/**
 * Creates a mock implementation of the authentication service
 * 
 * @param customResponses - Optional custom responses for auth service methods
 * @returns Mock authentication service with predefined responses
 */
export function mockAuthService(customResponses: MockAuthMethodOptions = {}) {
  // Create a mock implementation of the login method
  const loginMock = customResponses.login 
    ? (customResponses.login instanceof Function 
        ? customResponses.login 
        : jest.fn().mockImplementation(async () => {
            if (customResponses.login?.success) {
              return customResponses.login.response;
            } else {
              throw customResponses.login?.error || new Error('Login failed');
            }
          }))
    : jest.fn().mockImplementation(async () => {
        return {
          user: MOCK_USERS[1],
          token: 'mock-token',
          refreshToken: 'mock-refresh-token',
          expiresAt: Date.now() + 3600000, // 1 hour from now
          requiresMfa: false
        };
      });

  // Create a mock implementation of the logout method
  const logoutMock = customResponses.logout 
    ? (customResponses.logout instanceof Function 
        ? customResponses.logout 
        : jest.fn().mockImplementation(async () => {
            if (!customResponses.logout?.success) {
              throw customResponses.logout?.error || new Error('Logout failed');
            }
          }))
    : jest.fn().mockResolvedValue(undefined);

  // Create a mock implementation of the verifyMfa method
  const verifyMfaMock = customResponses.verifyMfa 
    ? (customResponses.verifyMfa instanceof Function 
        ? customResponses.verifyMfa 
        : jest.fn().mockImplementation(async () => {
            if (customResponses.verifyMfa?.success) {
              return customResponses.verifyMfa.response;
            } else {
              throw customResponses.verifyMfa?.error || new Error('MFA verification failed');
            }
          }))
    : jest.fn().mockImplementation(async () => {
        return {
          user: MOCK_USERS[1],
          token: 'mock-token',
          refreshToken: 'mock-refresh-token',
          expiresAt: Date.now() + 3600000,
          requiresMfa: false
        };
      });

  // Create a mock implementation of the refreshToken method
  const refreshTokenMock = customResponses.refreshToken 
    ? (customResponses.refreshToken instanceof Function 
        ? customResponses.refreshToken 
        : jest.fn().mockImplementation(async () => {
            return customResponses.refreshToken?.success || false;
          }))
    : jest.fn().mockResolvedValue(true);

  // Create a mock implementation of the getCurrentUser method
  const getCurrentUserMock = customResponses.getUserProfile 
    ? (customResponses.getUserProfile instanceof Function 
        ? customResponses.getUserProfile 
        : jest.fn().mockImplementation(async () => {
            if (customResponses.getUserProfile?.success) {
              return customResponses.getUserProfile.response;
            } else {
              throw customResponses.getUserProfile?.error || new Error('Failed to get user profile');
            }
          }))
    : jest.fn().mockResolvedValue(MOCK_USERS[1]);

  return {
    login: loginMock,
    logout: logoutMock,
    refreshToken: refreshTokenMock,
    verifyMfa: verifyMfaMock,
    getCurrentUser: getCurrentUserMock,
    requestPasswordReset: jest.fn().mockResolvedValue(undefined),
    resetPassword: jest.fn().mockResolvedValue(undefined),
    updatePassword: jest.fn().mockResolvedValue(undefined),
    validateSession: jest.fn().mockResolvedValue(true),
    setupMfa: jest.fn().mockResolvedValue({ qrCodeUrl: 'mock-qr-code-url', secret: 'mock-secret' }),
    verifyMfaSetup: jest.fn().mockResolvedValue(true),
    disableMfa: jest.fn().mockResolvedValue(true)
  };
}

/**
 * Mock implementation of the AuthProvider component for testing
 */
export const MockAuthProvider: React.FC<MockAuthProviderProps> = ({ 
  children, 
  authState = {}, 
  mockMethods = {} 
}) => {
  // Create mock auth context with overrides
  const mockContextValue = createMockAuthContext({
    authState,
    methods: mockMethods
  });

  return (
    <AuthContext.Provider value={mockContextValue}>
      {children}
    </AuthContext.Provider>
  );
};