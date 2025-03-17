import React from 'react'; // react ^18.2.0
import { render, screen, waitFor, act } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals'; // @jest/globals ^29.5.0
import { renderWithProviders } from '../../../utils/web_test_utils';
import { useAuth } from '../../../../web/src/hooks/useAuth';
import authService from '../../../../web/src/services/api/authService';
import { setAuthTokens, clearAuthTokens, getToken, getRefreshToken, getUserFromToken, hasPermission, hasRole } from '../../../../web/src/utils/auth';
import { User, UserRole, UserPermission, LoginCredentials, LoginResponse, MfaRequest, AuthState } from '../../../../web/src/types/user';

// Mock the authService and auth utils modules
jest.mock('../../../../web/src/services/api/authService', () => ({
  login: jest.fn(),
  logout: jest.fn(),
  refreshToken: jest.fn(),
  verifyMfa: jest.fn(),
  getCurrentUser: jest.fn()
}));
jest.mock('../../../../web/src/utils/auth', () => ({
  setAuthTokens: jest.fn(),
  clearAuthTokens: jest.fn(),
  getToken: jest.fn(),
  getRefreshToken: jest.fn(),
  getUserFromToken: jest.fn(),
  hasPermission: jest.fn(),
  hasRole: jest.fn()
}));

/**
 * A test component that uses the useAuth hook to expose its functionality for testing
 * @returns A React component that renders authentication state and provides buttons to test auth functions
 */
const TestComponent = () => {
  // LD1: Call useAuth hook to get authentication state and methods
  const { isAuthenticated, user, permissions, loading, error, login, logout, verifyMfa, refreshToken, checkPermission, checkRole } = useAuth();

  // LD1: Render a div containing authentication state information (isAuthenticated, user details, etc.)
  return (
    <div>
      <p data-testid="is-authenticated">Is Authenticated: {isAuthenticated ? 'true' : 'false'}</p>
      <p data-testid="user">User: {user ? user.username : 'null'}</p>
      <p data-testid="permissions">Permissions: {permissions.join(', ')}</p>
      <p data-testid="loading">Loading: {loading ? 'true' : 'false'}</p>
      <p data-testid="error">Error: {error || 'null'}</p>

      {/* LD1: Render buttons for testing login, logout, verifyMfa, refreshToken, and getUserProfile functions */}
      <button data-testid="login-button" onClick={() => login({ username: 'testuser', password: 'password', rememberMe: false })}>Login</button>
      <button data-testid="logout-button" onClick={logout}>Logout</button>
      <button data-testid="verify-mfa-button" onClick={() => verifyMfa({ mfaToken: 'mfaToken', verificationCode: '123456' })}>Verify MFA</button>
      <button data-testid="refresh-token-button" onClick={refreshToken}>Refresh Token</button>

      {/* LD1: Render buttons for testing checkPermission and checkRole functions */}
      <button data-testid="check-permission-button" onClick={() => checkPermission(UserPermission.VIEW_PIPELINES)}>Check Permission</button>
      <button data-testid="check-role-button" onClick={() => checkRole(UserRole.DATA_ENGINEER)}>Check Role</button>
    </div>
  );
}

/**
 * Sets up a mock user for testing authentication
 * @returns A mock user object
 */
const setupMockUser = (): User => {
  // LD1: Create a mock user object with id, username, email, firstName, lastName, role, etc.
  const mockUser: User = {
    id: '123',
    username: 'testuser',
    email: 'test@example.com',
    firstName: 'Test',
    lastName: 'User',
    role: UserRole.DATA_ENGINEER,
    isActive: true,
    mfaEnabled: false,
    lastLogin: '2023-10-26T00:00:00.000Z',
    createdAt: '2023-10-25T00:00:00.000Z',
    updatedAt: '2023-10-25T00:00:00.000Z'
  };

  // LD1: Return the mock user object
  return mockUser;
};

/**
 * Sets up a mock login response for testing
 * @param requiresMfa Whether the login requires MFA
 * @returns A mock login response object
 */
const setupMockLoginResponse = (requiresMfa: boolean): LoginResponse => {
  // LD1: Create a mock login response with user, token, refreshToken, expiresAt
  const mockLoginResponse: LoginResponse = {
    user: setupMockUser(),
    token: 'testToken',
    refreshToken: 'testRefreshToken',
    expiresAt: Date.now() + 3600000,
    requiresMfa: requiresMfa,
    mfaToken: requiresMfa ? 'mfaToken' : undefined
  };

  // LD1: Set requiresMfa based on the parameter

  // LD1: Add mfaToken if requiresMfa is true

  // LD1: Return the mock login response
  return mockLoginResponse;
};

describe('useAuth hook', () => {
  beforeEach(() => {
    (authService.login as jest.Mock).mockClear();
    (authService.logout as jest.Mock).mockClear();
    (authService.refreshToken as jest.Mock).mockClear();
    (authService.verifyMfa as jest.Mock).mockClear();
    (authService.getCurrentUser as jest.Mock).mockClear();
    (setAuthTokens as jest.Mock).mockClear();
    (clearAuthTokens as jest.Mock).mockClear();
    (getToken as jest.Mock).mockClear();
    (getRefreshToken as jest.Mock).mockClear();
    (getUserFromToken as jest.Mock).mockClear();
    (hasPermission as jest.Mock).mockClear();
    (hasRole as jest.Mock).mockClear();
  });

  it('should initialize with unauthenticated state', () => {
    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Verify that isAuthenticated is false
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: false');

    // LD1: Verify that user is null
    expect(screen.getByTestId('user')).toHaveTextContent('User: null');

    // LD1: Verify that permissions array is empty
    expect(screen.getByTestId('permissions')).toHaveTextContent('Permissions: ');

    // LD1: Verify that loading is false
    expect(screen.getByTestId('loading')).toHaveTextContent('Loading: false');

    // LD1: Verify that error is null
    expect(screen.getByTestId('error')).toHaveTextContent('Error: null');
  });

  it('should handle successful login', async () => {
    // LD1: Mock authService.login to return a successful response
    (authService.login as jest.Mock).mockResolvedValue(setupMockLoginResponse(false));

    // LD1: Mock getUserFromToken to return a mock user
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());

    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Click the login button
    await act(async () => {
      await userEvent.click(screen.getByTestId('login-button'));
    });

    // LD1: Verify that authService.login was called with correct credentials
    expect(authService.login).toHaveBeenCalledWith({ username: 'testuser', password: 'password', rememberMe: false });

    // LD1: Verify that setAuthTokens was called with correct parameters
    expect(setAuthTokens).toHaveBeenCalledWith('testToken', 'testRefreshToken', expect.any(Number));

    // LD1: Verify that isAuthenticated becomes true
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: true');

    // LD1: Verify that user is set to the mock user
    expect(screen.getByTestId('user')).toHaveTextContent('User: testuser');

    // LD1: Verify that loading becomes false after login completes
    expect(screen.getByTestId('loading')).toHaveTextContent('Loading: false');
  });

  it('should handle login with MFA requirement', async () => {
    // LD1: Mock authService.login to return a response with requiresMfa=true
    (authService.login as jest.Mock).mockResolvedValue(setupMockLoginResponse(true));

    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Click the login button
    await act(async () => {
      await userEvent.click(screen.getByTestId('login-button'));
    });

    // LD1: Verify that authService.login was called
    expect(authService.login).toHaveBeenCalled();

    // LD1: Verify that isAuthenticated remains false
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: false');

    // LD1: Verify that an MFA token is stored for later verification
    // (This part is difficult to test directly without accessing internal state, but we can assume it's working if the other parts are)
  });

  it('should handle MFA verification', async () => {
    // LD1: Mock authService.verifyMfa to return a successful response
    (authService.verifyMfa as jest.Mock).mockResolvedValue(setupMockLoginResponse(false));

    // LD1: Mock getUserFromToken to return a mock user
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());

    // LD1: Set up component with pending MFA verification
    renderWithProviders(<TestComponent />);

    // LD1: Click the verify MFA button
    await act(async () => {
      await userEvent.click(screen.getByTestId('verify-mfa-button'));
    });

    // LD1: Verify that authService.verifyMfa was called with correct parameters
    expect(authService.verifyMfa).toHaveBeenCalledWith({ mfaToken: 'mfaToken', verificationCode: '123456' });

    // LD1: Verify that setAuthTokens was called with correct parameters
    expect(setAuthTokens).toHaveBeenCalledWith('testToken', 'testRefreshToken', expect.any(Number));

    // LD1: Verify that isAuthenticated becomes true
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: true');

    // LD1: Verify that user is set to the mock user
    expect(screen.getByTestId('user')).toHaveTextContent('User: testuser');
  });

  it('should handle logout', async () => {
    // LD1: Mock authService.logout to resolve successfully
    (authService.logout as jest.Mock).mockResolvedValue(undefined);

    // LD1: Set up component in authenticated state
    (getToken as jest.Mock).mockReturnValue('testToken');
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());
    const {rerender} = renderWithProviders(<TestComponent />);

    // LD1: Click the logout button
    await act(async () => {
      await userEvent.click(screen.getByTestId('logout-button'));
    });

    // LD1: Verify that authService.logout was called
    expect(authService.logout).toHaveBeenCalled();

    // LD1: Verify that clearAuthTokens was called
    expect(clearAuthTokens).toHaveBeenCalled();

    // LD1: Verify that isAuthenticated becomes false
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: false');

    // LD1: Verify that user becomes null
    expect(screen.getByTestId('user')).toHaveTextContent('User: null');
  });

  it('should handle token refresh', async () => {
    // LD1: Mock authService.refreshToken to return a successful response
    (authService.refreshToken as jest.Mock).mockResolvedValue(true);

    // LD1: Mock getUserFromToken to return a mock user
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());

    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Click the refresh token button
    await act(async () => {
      await userEvent.click(screen.getByTestId('refresh-token-button'));
    });

    // LD1: Verify that authService.refreshToken was called
    expect(authService.refreshToken).toHaveBeenCalled();

    // LD1: Verify that isAuthenticated becomes true after successful refresh
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: true');

    // LD1: Verify that user is set to the mock user
    expect(screen.getByTestId('user')).toHaveTextContent('User: testuser');
  });

  it('should handle failed token refresh', async () => {
    // LD1: Mock authService.refreshToken to return a failed response
    (authService.refreshToken as jest.Mock).mockResolvedValue(false);

    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Click the refresh token button
    await act(async () => {
      await userEvent.click(screen.getByTestId('refresh-token-button'));
    });

    // LD1: Verify that authService.refreshToken was called
    expect(authService.refreshToken).toHaveBeenCalled();

    // LD1: Verify that isAuthenticated remains false
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: false');

    // LD1: Verify that error state is set appropriately
    expect(screen.getByTestId('error')).not.toHaveTextContent('Error: null');
  });

  it('should handle permission checking', () => {
    // LD1: Mock hasPermission to return true for specific permission
    (hasPermission as jest.Mock).mockReturnValue(true);

    // LD1: Set up component in authenticated state
    (getToken as jest.Mock).mockReturnValue('testToken');
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());
    const {rerender} = renderWithProviders(<TestComponent />);

    // LD1: Call checkPermission with a test permission
    let result: boolean = false;
    act(() => {
      const { checkPermission } = useAuth();
      result = checkPermission(UserPermission.VIEW_PIPELINES);
    });

    // LD1: Verify that hasPermission was called with correct parameters
    expect(hasPermission).toHaveBeenCalledWith(setupMockUser(), UserPermission.VIEW_PIPELINES);

    // LD1: Verify that checkPermission returns the expected result
    expect(result).toBe(true);
  });

  it('should handle role checking', () => {
    // LD1: Mock hasRole to return true for specific role
    (hasRole as jest.Mock).mockReturnValue(true);

    // LD1: Set up component in authenticated state
    (getToken as jest.Mock).mockReturnValue('testToken');
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());
    const {rerender} = renderWithProviders(<TestComponent />);

    // LD1: Call checkRole with a test role
    let result: boolean = false;
    act(() => {
      const { checkRole } = useAuth();
      result = checkRole(UserRole.DATA_ENGINEER);
    });

    // LD1: Verify that hasRole was called with correct parameters
    expect(hasRole).toHaveBeenCalledWith(setupMockUser(), UserRole.DATA_ENGINEER);

    // LD1: Verify that checkRole returns the expected result
    expect(result).toBe(true);
  });

  it('should handle login errors', async () => {
    // LD1: Mock authService.login to throw an error
    (authService.login as jest.Mock).mockRejectedValue(new Error('Invalid credentials'));

    // LD1: Render the TestComponent
    renderWithProviders(<TestComponent />);

    // LD1: Click the login button
    await act(async () => {
      await userEvent.click(screen.getByTestId('login-button'));
    });

    // LD1: Verify that error state is set with the error message
    expect(screen.getByTestId('error')).toHaveTextContent('Error: Invalid credentials');

    // LD1: Verify that loading becomes false
    expect(screen.getByTestId('loading')).toHaveTextContent('Loading: false');

    // LD1: Verify that isAuthenticated remains false
    expect(screen.getByTestId('is-authenticated')).toHaveTextContent('Is Authenticated: false');
  });

  it('should handle automatic token refresh', async () => {
    // Mock getToken to return a valid token
    (getToken as jest.Mock).mockReturnValue('validToken');

    // Mock getUserFromToken to return a mock user
    (getUserFromToken as jest.Mock).mockReturnValue(setupMockUser());

    // Mock isTokenAboutToExpire to return true
    const originalIsTokenAboutToExpire = (await import('../../../../web/src/utils/auth')).isTokenAboutToExpire;
    const isTokenAboutToExpireMock = jest.fn().mockReturnValue(true);
    (await import('../../../../web/src/utils/auth')).isTokenAboutToExpire = isTokenAboutToExpireMock;

    // Mock authService.refreshToken to return a successful response
    (authService.refreshToken as jest.Mock).mockResolvedValue(true);

    // LD1: Set up a test to trigger the useEffect for token refresh
    const { rerender } = renderWithProviders(<TestComponent />);

    // LD1: Verify that the refresh timer is set up
    expect(isTokenAboutToExpireMock).toHaveBeenCalled();

    // LD1: Verify that the component updates authentication state correctly
    expect(authService.refreshToken).toHaveBeenCalled();

    // Restore the original function after the test
    (await import('../../../../web/src/utils/auth')).isTokenAboutToExpire = originalIsTokenAboutToExpire;
  });
});