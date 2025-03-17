/**
 * Authentication utility functions for the self-healing data pipeline web application.
 * This module provides functions for token management, user authentication,
 * permission verification, and role-based access control.
 */

import jwtDecode from 'jwt-decode'; // jwt-decode@^3.1.2
import { AUTH_TOKEN_KEY, REFRESH_TOKEN_KEY, TOKEN_EXPIRY_KEY } from './constants';
import { ENV } from '../config/env';
import { User, UserRole, UserPermission } from '../types/user';
import { 
  setLocalStorageItem, 
  getLocalStorageItem, 
  removeLocalStorageItem 
} from './storage';

/**
 * Stores authentication tokens in local storage
 * @param token - The JWT authentication token
 * @param refreshToken - The refresh token for obtaining new authentication tokens
 * @param expiresAt - The expiration timestamp for the token
 * @returns True if tokens were successfully stored
 */
export function setAuthTokens(token: string, refreshToken?: string, expiresAt?: number): boolean {
  const tokenStored = setLocalStorageItem(AUTH_TOKEN_KEY, token);
  let refreshTokenStored = true;
  let expiryStored = true;
  
  if (refreshToken) {
    refreshTokenStored = setLocalStorageItem(REFRESH_TOKEN_KEY, refreshToken);
  }
  
  if (expiresAt) {
    expiryStored = setLocalStorageItem(TOKEN_EXPIRY_KEY, expiresAt);
  }
  
  return tokenStored && refreshTokenStored && expiryStored;
}

/**
 * Removes authentication tokens from local storage
 * @returns True if tokens were successfully removed
 */
export function clearAuthTokens(): boolean {
  const tokenRemoved = removeLocalStorageItem(AUTH_TOKEN_KEY);
  const refreshTokenRemoved = removeLocalStorageItem(REFRESH_TOKEN_KEY);
  const expiryRemoved = removeLocalStorageItem(TOKEN_EXPIRY_KEY);
  
  return tokenRemoved && refreshTokenRemoved && expiryRemoved;
}

/**
 * Retrieves the authentication token from local storage
 * @returns The authentication token or null if not found
 */
export function getToken(): string | null {
  return getLocalStorageItem<string | null>(AUTH_TOKEN_KEY, null);
}

/**
 * Retrieves the refresh token from local storage
 * @returns The refresh token or null if not found
 */
export function getRefreshToken(): string | null {
  return getLocalStorageItem<string | null>(REFRESH_TOKEN_KEY, null);
}

/**
 * Retrieves the token expiration timestamp from local storage
 * @returns The token expiration timestamp or null if not found
 */
export function getTokenExpiry(): number | null {
  const expiry = getLocalStorageItem<number | null>(TOKEN_EXPIRY_KEY, null);
  return typeof expiry === 'number' ? expiry : null;
}

/**
 * Checks if the authentication token is expired
 * @returns True if the token is expired or not found, false otherwise
 */
export function isTokenExpired(): boolean {
  const expiryTime = getTokenExpiry();
  
  if (!expiryTime) {
    return true;
  }
  
  const currentTime = Date.now();
  return currentTime > expiryTime;
}

/**
 * Checks if the token is about to expire within the specified threshold
 * @param thresholdMinutes - The threshold time in minutes before expiration
 * @returns True if the token is about to expire, false otherwise
 */
export function isTokenAboutToExpire(thresholdMinutes: number): boolean {
  const expiryTime = getTokenExpiry();
  
  if (!expiryTime) {
    return true;
  }
  
  const currentTime = Date.now();
  const thresholdMs = thresholdMinutes * 60 * 1000;
  
  return (currentTime + thresholdMs) > expiryTime;
}

/**
 * Checks if the user is authenticated
 * @returns True if the user is authenticated, false otherwise
 */
export function isAuthenticated(): boolean {
  // If authentication is disabled in the environment, return true
  if (!ENV.AUTH_ENABLED) {
    return true;
  }
  
  const token = getToken();
  
  // Check if token exists and is not expired
  return !!token && !isTokenExpired();
}

/**
 * Extracts user information from the JWT token
 * @returns User object extracted from the token or null if token is invalid
 */
export function getUserFromToken(): User | null {
  const token = getToken();
  
  if (!token) {
    return null;
  }
  
  try {
    // Decode the token and extract user information
    const decodedToken: any = jwtDecode(token);
    
    // Map the token payload to User object
    const user: User = {
      id: decodedToken.sub || decodedToken.id,
      username: decodedToken.username,
      email: decodedToken.email,
      firstName: decodedToken.firstName,
      lastName: decodedToken.lastName,
      role: decodedToken.role,
      isActive: decodedToken.isActive,
      mfaEnabled: decodedToken.mfaEnabled,
      lastLogin: decodedToken.lastLogin,
      createdAt: decodedToken.createdAt,
      updatedAt: decodedToken.updatedAt
    };
    
    return user;
  } catch (error) {
    console.error('Error decoding token:', error);
    return null;
  }
}

/**
 * Gets the list of permissions associated with a user role
 * @param role - The user role to get permissions for
 * @returns Array of permissions for the specified role
 */
export function getPermissionsForRole(role: UserRole): UserPermission[] {
  // Define permission mappings for each role
  const rolePermissions: Record<UserRole, UserPermission[]> = {
    [UserRole.ADMIN]: [
      // Admins have all permissions
      ...Object.values(UserPermission)
    ],
    [UserRole.DATA_ENGINEER]: [
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
    [UserRole.DATA_ANALYST]: [
      UserPermission.VIEW_DASHBOARD,
      UserPermission.VIEW_PIPELINES,
      UserPermission.VIEW_QUALITY,
      UserPermission.VIEW_HEALING,
      UserPermission.VIEW_ALERTS
    ],
    [UserRole.PIPELINE_OPERATOR]: [
      UserPermission.VIEW_DASHBOARD,
      UserPermission.VIEW_PIPELINES,
      UserPermission.VIEW_QUALITY,
      UserPermission.VIEW_HEALING,
      UserPermission.VIEW_ALERTS,
      UserPermission.MANAGE_ALERTS,
      UserPermission.MANAGE_HEALING
    ],
    [UserRole.VIEWER]: [
      UserPermission.VIEW_DASHBOARD,
      UserPermission.VIEW_PIPELINES,
      UserPermission.VIEW_QUALITY,
      UserPermission.VIEW_ALERTS
    ]
  };
  
  return rolePermissions[role] || [];
}

/**
 * Checks if the user has a specific permission
 * @param user - The user object
 * @param permission - The permission to check
 * @returns True if the user has the permission, false otherwise
 */
export function hasPermission(user: User | null, permission: UserPermission): boolean {
  if (!user) {
    return false;
  }
  
  // Admins have all permissions
  if (user.role === UserRole.ADMIN) {
    return true;
  }
  
  // Get the permissions for the user's role
  const permissions = getPermissionsForRole(user.role);
  
  // Check if the permission is included in the user's permissions
  return permissions.includes(permission);
}

/**
 * Checks if the user has a specific role
 * @param user - The user object
 * @param role - The role to check
 * @returns True if the user has the role, false otherwise
 */
export function hasRole(user: User | null, role: UserRole): boolean {
  if (!user) {
    return false;
  }
  
  return user.role === role;
}

/**
 * Checks if the user is an administrator
 * @param user - The user object
 * @returns True if the user is an admin, false otherwise
 */
export function isAdmin(user: User | null): boolean {
  if (!user) {
    return false;
  }
  
  return user.role === UserRole.ADMIN;
}