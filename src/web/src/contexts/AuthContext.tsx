import React, { createContext, useContext, ReactNode } from 'react';
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
import { useAuthProvider } from '../hooks/useAuth';

/**
 * Type definition for the authentication context value
 */
interface AuthContextType {
  isAuthenticated: boolean;
  user: User | null;
  permissions: UserPermission[];
  loading: boolean;
  error: string | null;
  login: (credentials: LoginCredentials) => Promise<LoginResponse>;
  logout: () => Promise<void>;
  verifyMfa: (mfaRequest: MfaRequest) => Promise<LoginResponse>;
  refreshToken: () => Promise<boolean>;
  getUserProfile: () => Promise<User>;
  checkPermission: (permission: UserPermission) => boolean;
  checkRole: (role: UserRole) => boolean;
}

/**
 * Props for the AuthProvider component
 */
interface AuthProviderProps {
  children: ReactNode;
}

/**
 * Create the authentication context with a default undefined value
 */
const AuthContext = createContext<AuthContextType | undefined>(undefined);

/**
 * React context provider component for authentication
 * Manages authentication state and provides login/logout functionality
 * throughout the application
 */
export const AuthProvider = ({ children }: AuthProviderProps) => {
  // Use the auth provider hook to get authentication state and methods
  const {
    authState,
    login,
    logout,
    verifyMfa,
    refreshToken,
    getUserProfile,
    checkPermission,
    checkRole
  } = useAuthProvider();

  // Combine state and methods into context value
  const authContextValue: AuthContextType = {
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

  return (
    <AuthContext.Provider value={authContextValue}>
      {children}
    </AuthContext.Provider>
  );
};

/**
 * Custom hook that provides access to the authentication context
 * @returns Authentication context value containing state and methods
 */
export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export { AuthContext };