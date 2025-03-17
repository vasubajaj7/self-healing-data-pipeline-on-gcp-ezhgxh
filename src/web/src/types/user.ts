/**
 * TypeScript type definitions for user-related data structures in the self-healing data pipeline web application.
 * Defines interfaces and types for user authentication, authorization, profile management, and session handling.
 */

import { ID, Timestamp, JSONObject, Nullable, Optional } from './global';

/**
 * Enum defining the possible user roles in the application
 */
export enum UserRole {
  ADMIN = 'ADMIN',                   // Full system administration access
  DATA_ENGINEER = 'DATA_ENGINEER',   // Create/modify pipeline definitions
  DATA_ANALYST = 'DATA_ANALYST',     // Query data, view metadata
  PIPELINE_OPERATOR = 'PIPELINE_OPERATOR', // Monitor and troubleshoot pipelines
  VIEWER = 'VIEWER'                  // Read-only access to dashboards and reports
}

/**
 * Enum defining the possible user permissions in the application
 */
export enum UserPermission {
  // Dashboard permissions
  VIEW_DASHBOARD = 'VIEW_DASHBOARD',

  // Pipeline permissions
  VIEW_PIPELINES = 'VIEW_PIPELINES',
  MANAGE_PIPELINES = 'MANAGE_PIPELINES',

  // Data quality permissions
  VIEW_QUALITY = 'VIEW_QUALITY',
  MANAGE_QUALITY = 'MANAGE_QUALITY',
  
  // Self-healing permissions
  VIEW_HEALING = 'VIEW_HEALING',
  MANAGE_HEALING = 'MANAGE_HEALING',
  
  // Alerting permissions
  VIEW_ALERTS = 'VIEW_ALERTS',
  MANAGE_ALERTS = 'MANAGE_ALERTS',
  
  // Configuration permissions
  VIEW_CONFIGURATION = 'VIEW_CONFIGURATION',
  MANAGE_CONFIGURATION = 'MANAGE_CONFIGURATION',
  
  // Administration permissions
  VIEW_ADMIN = 'VIEW_ADMIN',
  MANAGE_USERS = 'MANAGE_USERS',
  MANAGE_ROLES = 'MANAGE_ROLES',
  MANAGE_SYSTEM = 'MANAGE_SYSTEM'
}

/**
 * Interface defining the user object structure
 */
export interface User {
  id: ID;                        // Unique identifier
  username: string;              // Username for login
  email: string;                 // Email address
  firstName: string;             // First name
  lastName: string;              // Last name
  role: UserRole;                // User's role
  isActive: boolean;             // User account status
  mfaEnabled: boolean;           // Multi-factor authentication status
  lastLogin: Optional<Timestamp>; // Last login timestamp
  createdAt: Timestamp;          // Account creation timestamp
  updatedAt: Timestamp;          // Account last update timestamp
}

/**
 * Interface defining the extended user profile information
 */
export interface UserProfile {
  id: ID;                         // Unique identifier
  username: string;               // Username for login
  email: string;                  // Email address
  firstName: string;              // First name
  lastName: string;               // Last name
  jobTitle: Optional<string>;     // User's job title
  department: Optional<string>;   // User's department
  avatarUrl: Optional<string>;    // Profile picture URL
  role: UserRole;                 // User's role
  mfaEnabled: boolean;            // Multi-factor authentication status
  preferences: UserPreferences;   // User preferences
  lastLogin: Optional<Timestamp>; // Last login timestamp
  createdAt: Timestamp;           // Account creation timestamp
  updatedAt: Timestamp;           // Account last update timestamp
}

/**
 * Interface defining user preferences for application settings
 */
export interface UserPreferences {
  theme: string;                          // UI theme preference (light/dark)
  dashboardLayout: Optional<JSONObject>;  // Custom dashboard layout configuration
  notifications: UserNotificationPreferences; // Notification preferences
  defaultView: Optional<string>;          // Default landing page
  timezone: string;                       // User's preferred timezone
  dateFormat: string;                     // Preferred date format
}

/**
 * Interface defining user notification preferences
 */
export interface UserNotificationPreferences {
  email: boolean;                       // Email notifications enabled
  teams: boolean;                       // Microsoft Teams notifications enabled
  inApp: boolean;                       // In-app notifications enabled
  alertLevels: Record<string, boolean>; // Alert levels to receive (by severity)
}

/**
 * Interface defining login request credentials
 */
export interface LoginCredentials {
  username: string;             // Username or email
  password: string;             // Password
  rememberMe: Optional<boolean>; // Remember login session
}

/**
 * Interface defining login response data
 */
export interface LoginResponse {
  user: User;                     // User information
  token: string;                  // JWT access token
  refreshToken: Optional<string>; // JWT refresh token
  expiresAt: number;              // Token expiration timestamp
  requiresMfa: boolean;           // Whether MFA is required
  mfaToken: Optional<string>;     // Token for MFA verification
}

/**
 * Interface defining MFA verification request
 */
export interface MfaRequest {
  mfaToken: string;          // MFA session token
  verificationCode: string;  // Verification code entered by user
}

/**
 * Interface defining password reset request
 */
export interface PasswordResetRequest {
  email: string;  // User email for password reset
}

/**
 * Interface defining password update request
 */
export interface PasswordUpdateRequest {
  currentPassword: string;  // Current password
  newPassword: string;      // New password
}

/**
 * Interface defining user profile update request
 */
export interface UserPreferencesUpdateRequest {
  firstName: Optional<string>;            // Updated first name
  lastName: Optional<string>;             // Updated last name
  jobTitle: Optional<string>;             // Updated job title
  department: Optional<string>;           // Updated department
  preferences: Optional<UserPreferences>; // Updated preferences
}

/**
 * Interface defining the authentication state
 */
export interface AuthState {
  isAuthenticated: boolean;      // Whether user is authenticated
  user: User | null;             // Current user information
  permissions: UserPermission[]; // User permissions
  loading: boolean;              // Authentication loading state
  error: string | null;          // Authentication error message
}