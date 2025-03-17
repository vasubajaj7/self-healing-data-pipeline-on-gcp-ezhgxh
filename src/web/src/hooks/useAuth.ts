/**
 * Custom React hook that provides authentication functionality for the self-healing data pipeline web application.
 * Manages user authentication state, handles login/logout operations, token refresh, and provides utilities for 
 * permission and role checking.
 * 
 * @version 1.0.0
 */

import { useState, useEffect, useCallback, useRef } from 'react'; // react ^18.2.0
import { 
  User, UserRole, UserPermission, LoginCredentials, 
  LoginResponse, MfaRequest, AuthState 
} from '../types/user';
import authService from '../services/api/authService';
import { 
  getToken, getRefreshToken, setAuthTokens, clearAuthTokens,
  isTokenExpired, getUserFromToken, hasPermission, hasRole,
  isAdmin, getPermissionsForRole, isTokenAboutToExpire 
} from '../utils/auth';
import { ENV } from '../config/env';

/**
 * Custom hook that provides authentication functionality throughout the application
 * @returns Authentication state and methods for authentication operations
 */
export const useAuth = () => {
  // Initialize authentication state
  const [authState, setAuthState] = useState<AuthState>({
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: true,
    error: null
  });

  // Reference for refresh timer
  const refreshTimerRef = useRef<NodeJS.Timeout | null>(null);

  /**
   * Initialize authentication state from stored token
   */
  const initializeAuth = useCallback(async () => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));

      // If authentication is disabled in environment
      if (!ENV.AUTH_ENABLED) {
        setAuthState({
          isAuthenticated: true,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Check for existing token
      const token = getToken();
      if (!token) {
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Check if token is expired
      if (isTokenExpired()) {
        // Try to refresh the token
        const refreshSuccess = await authService.refreshToken();
        if (!refreshSuccess) {
          clearAuthTokens();
          setAuthState({
            isAuthenticated: false,
            user: null,
            permissions: [],
            loading: false,
            error: null
          });
          return;
        }
      }

      // Get user from token
      const user = getUserFromToken();
      if (!user) {
        clearAuthTokens();
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);

      // Set authenticated state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });

      // Start refresh timer
      setupRefreshTimer();
    } catch (error) {
      console.error('Error initializing authentication:', error);
      clearAuthTokens();
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: 'Failed to initialize authentication'
      });
    }
  }, []);

  /**
   * Log in a user with credentials
   * @param credentials Login credentials containing username and password
   * @returns Promise resolving to login response with tokens and user data
   */
  const login = async (credentials: LoginCredentials): Promise<LoginResponse> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call login API
      const loginResponse = await authService.login(credentials);
      
      // Check if MFA is required
      if (loginResponse.requiresMfa) {
        // Return response with MFA token to be used with verifyMfa
        return loginResponse;
      }
      
      // Get user from token or from response
      const user = loginResponse.user || getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found in response');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer
      setupRefreshTimer();
      
      return loginResponse;
    } catch (error) {
      console.error('Login error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to login. Please check your credentials.';
      
      setAuthState(prevState => ({
        ...prevState,
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Log out the current user
   */
  const logout = async (): Promise<void> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true }));
      
      // Call logout API
      await authService.logout();
      
      // Clear refresh timer
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      
      // Clear auth state
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: null
      });
    } catch (error) {
      console.error('Logout error:', error);
      
      // Even if logout API fails, we should clear local state
      clearAuthTokens();
      
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: null
      });
    }
  };

  /**
   * Verify a multi-factor authentication code
   * @param mfaRequest Object containing MFA token and verification code
   * @returns Promise resolving to login response with tokens and user data
   */
  const verifyMfa = async (mfaRequest: MfaRequest): Promise<LoginResponse> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call verify MFA API
      const loginResponse = await authService.verifyMfa(mfaRequest);
      
      // Get user from response or token
      const user = loginResponse.user || getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found after MFA verification');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer
      setupRefreshTimer();
      
      return loginResponse;
    } catch (error) {
      console.error('MFA verification error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to verify MFA. Please try again.';
      
      setAuthState(prevState => ({
        ...prevState,
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Refresh the authentication token
   * @returns Promise resolving to boolean indicating success
   */
  const refreshToken = async (): Promise<boolean> => {
    try {
      // Call refresh token API
      const refreshSuccess = await authService.refreshToken();
      
      if (!refreshSuccess) {
        // If refresh failed, logout
        clearAuthTokens();
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: 'Session expired. Please log in again.'
        });
        return false;
      }
      
      // Get user from new token
      const user = getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found after token refresh');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer again
      setupRefreshTimer();
      
      return true;
    } catch (error) {
      console.error('Token refresh error:', error);
      
      // Clear auth on refresh error
      clearAuthTokens();
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: 'Session expired. Please log in again.'
      });
      
      return false;
    }
  };

  /**
   * Fetch the current user's profile from the API
   * @returns Promise resolving to user object
   */
  const getUserProfile = async (): Promise<User> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call get current user API
      const user = await authService.getCurrentUser();
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      return user;
    } catch (error) {
      console.error('Get user profile error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to fetch user profile.';
      
      setAuthState(prevState => ({
        ...prevState,
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Check if the current user has a specific permission
   * @param permission The permission to check for
   * @returns Boolean indicating if user has the permission
   */
  const checkPermission = (permission: UserPermission): boolean => {
    if (!authState.isAuthenticated || !authState.user) {
      return false;
    }
    
    return hasPermission(authState.user, permission);
  };

  /**
   * Check if the current user has a specific role
   * @param role The role to check for
   * @returns Boolean indicating if user has the role
   */
  const checkRole = (role: UserRole): boolean => {
    if (!authState.isAuthenticated || !authState.user) {
      return false;
    }
    
    return hasRole(authState.user, role);
  };

  /**
   * Setup a timer to refresh the token before it expires
   */
  const setupRefreshTimer = useCallback(() => {
    // Clear existing timer if any
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
    
    // Don't set up timer if auth is disabled
    if (!ENV.AUTH_ENABLED) {
      return;
    }
    
    // Get token expiry
    const tokenExpiry = getTokenExpiry();
    if (!tokenExpiry) {
      return;
    }
    
    // Calculate time until refresh (5 minutes before expiry)
    const currentTime = Date.now();
    const timeToRefresh = tokenExpiry - currentTime - (5 * 60 * 1000);
    
    // If token is already expired or about to expire, refresh now
    if (timeToRefresh <= 0) {
      refreshToken();
      return;
    }
    
    // Set timer to refresh token
    refreshTimerRef.current = setTimeout(() => {
      refreshToken();
    }, timeToRefresh);
  }, []);

  // Load initial auth state on mount
  useEffect(() => {
    initializeAuth();
    
    // Cleanup on unmount
    return () => {
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
    };
  }, [initializeAuth]);

  // Return auth state and methods
  return {
    isAuthenticated: authState.isAuthenticated,
    user: authState.user,
    permissions: authState.permissions,
    loading: authState.loading,
    error: authState.error,
    login,
    logout,
    verifyMfa,
    refreshToken,
    getUserProfile,
    checkPermission,
    checkRole
  };
};

/**
 * Hook that implements the authentication provider functionality
 * @returns Authentication state and methods for the AuthProvider component
 */
export const useAuthProvider = () => {
  // Initialize authentication state with default values
  const [authState, setAuthState] = useState<AuthState>({
    isAuthenticated: false,
    user: null,
    permissions: [],
    loading: true,
    error: null
  });

  // Reference for refresh timer
  const refreshTimerRef = useRef<NodeJS.Timeout | null>(null);

  /**
   * Initialize authentication state from stored token
   */
  const initializeAuth = useCallback(async () => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));

      // If authentication is disabled in environment
      if (!ENV.AUTH_ENABLED) {
        setAuthState({
          isAuthenticated: true,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Check for existing token
      const token = getToken();
      if (!token) {
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Check if token is expired
      if (isTokenExpired()) {
        // Try to refresh the token
        const refreshSuccess = await authService.refreshToken();
        if (!refreshSuccess) {
          clearAuthTokens();
          setAuthState({
            isAuthenticated: false,
            user: null,
            permissions: [],
            loading: false,
            error: null
          });
          return;
        }
      }

      // Get user from token
      const user = getUserFromToken();
      if (!user) {
        clearAuthTokens();
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: null
        });
        return;
      }

      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);

      // Set authenticated state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });

      // Start refresh timer
      setupRefreshTimer();
    } catch (error) {
      console.error('Error initializing authentication:', error);
      clearAuthTokens();
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: 'Failed to initialize authentication'
      });
    }
  }, []);

  /**
   * Log in a user with credentials
   * @param credentials Login credentials containing username and password
   * @returns Promise resolving to login response with tokens and user data
   */
  const login = async (credentials: LoginCredentials): Promise<LoginResponse> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call login API
      const loginResponse = await authService.login(credentials);
      
      // Check if MFA is required
      if (loginResponse.requiresMfa) {
        // Return response with MFA token to be used with verifyMfa
        return loginResponse;
      }
      
      // Get user from token or from response
      const user = loginResponse.user || getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found in response');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer
      setupRefreshTimer();
      
      return loginResponse;
    } catch (error) {
      console.error('Login error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to login. Please check your credentials.';
      
      setAuthState(prevState => ({
        ...prevState,
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Log out the current user
   */
  const logout = async (): Promise<void> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true }));
      
      // Call logout API
      await authService.logout();
      
      // Clear refresh timer
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      
      // Clear auth state
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: null
      });
    } catch (error) {
      console.error('Logout error:', error);
      
      // Even if logout API fails, we should clear local state
      clearAuthTokens();
      
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: null
      });
    }
  };

  /**
   * Verify a multi-factor authentication code
   * @param mfaRequest Object containing MFA token and verification code
   * @returns Promise resolving to login response with tokens and user data
   */
  const verifyMfa = async (mfaRequest: MfaRequest): Promise<LoginResponse> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call verify MFA API
      const loginResponse = await authService.verifyMfa(mfaRequest);
      
      // Get user from response or token
      const user = loginResponse.user || getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found after MFA verification');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer
      setupRefreshTimer();
      
      return loginResponse;
    } catch (error) {
      console.error('MFA verification error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to verify MFA. Please try again.';
      
      setAuthState(prevState => ({
        ...prevState,
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Refresh the authentication token
   * @returns Promise resolving to boolean indicating success
   */
  const refreshToken = async (): Promise<boolean> => {
    try {
      // Call refresh token API
      const refreshSuccess = await authService.refreshToken();
      
      if (!refreshSuccess) {
        // If refresh failed, logout
        clearAuthTokens();
        setAuthState({
          isAuthenticated: false,
          user: null,
          permissions: [],
          loading: false,
          error: 'Session expired. Please log in again.'
        });
        return false;
      }
      
      // Get user from new token
      const user = getUserFromToken();
      
      if (!user) {
        throw new Error('User information not found after token refresh');
      }
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      // Setup refresh timer again
      setupRefreshTimer();
      
      return true;
    } catch (error) {
      console.error('Token refresh error:', error);
      
      // Clear auth on refresh error
      clearAuthTokens();
      setAuthState({
        isAuthenticated: false,
        user: null,
        permissions: [],
        loading: false,
        error: 'Session expired. Please log in again.'
      });
      
      return false;
    }
  };

  /**
   * Fetch the current user's profile from the API
   * @returns Promise resolving to user object
   */
  const getUserProfile = async (): Promise<User> => {
    try {
      setAuthState(prevState => ({ ...prevState, loading: true, error: null }));
      
      // Call get current user API
      const user = await authService.getCurrentUser();
      
      // Get permissions based on user role
      const permissions = getPermissionsForRole(user.role);
      
      // Update auth state
      setAuthState({
        isAuthenticated: true,
        user,
        permissions,
        loading: false,
        error: null
      });
      
      return user;
    } catch (error) {
      console.error('Get user profile error:', error);
      const errorMessage = error instanceof Error 
        ? error.message 
        : 'Failed to fetch user profile.';
      
      setAuthState(prevState => ({
        ...prevState,
        loading: false,
        error: errorMessage
      }));
      
      throw error;
    }
  };

  /**
   * Check if the current user has a specific permission
   * @param permission The permission to check for
   * @returns Boolean indicating if user has the permission
   */
  const checkPermission = (permission: UserPermission): boolean => {
    if (!authState.isAuthenticated || !authState.user) {
      return false;
    }
    
    return hasPermission(authState.user, permission);
  };

  /**
   * Check if the current user has a specific role
   * @param role The role to check for
   * @returns Boolean indicating if user has the role
   */
  const checkRole = (role: UserRole): boolean => {
    if (!authState.isAuthenticated || !authState.user) {
      return false;
    }
    
    return hasRole(authState.user, role);
  };

  /**
   * Setup a timer to refresh the token before it expires
   */
  const setupRefreshTimer = useCallback(() => {
    // Clear existing timer if any
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
    
    // Don't set up timer if auth is disabled
    if (!ENV.AUTH_ENABLED) {
      return;
    }
    
    // Get token expiry
    const tokenExpiry = getTokenExpiry();
    if (!tokenExpiry) {
      return;
    }
    
    // Calculate time until refresh (5 minutes before expiry)
    const currentTime = Date.now();
    const timeToRefresh = tokenExpiry - currentTime - (5 * 60 * 1000);
    
    // If token is already expired or about to expire, refresh now
    if (timeToRefresh <= 0) {
      refreshToken();
      return;
    }
    
    // Set timer to refresh token
    refreshTimerRef.current = setTimeout(() => {
      refreshToken();
    }, timeToRefresh);
  }, []);

  // Load initial auth state on mount
  useEffect(() => {
    initializeAuth();
    
    // Cleanup on unmount
    return () => {
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
    };
  }, [initializeAuth]);

  // Return auth state and methods for context provider
  return {
    authState,
    login,
    logout,
    verifyMfa,
    refreshToken,
    getUserProfile,
    checkPermission,
    checkRole
  };
};

export default useAuth;