/**
 * Authentication service for the self-healing data pipeline web application.
 * Provides functions for user login, logout, token refresh, password management, and session verification.
 * Handles communication with the backend authentication API endpoints.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import { 
  setAuthTokens, 
  clearAuthTokens, 
  getToken, 
  getRefreshToken, 
  isTokenExpired 
} from '../../utils/auth';
import { parseApiError, logError } from '../../utils/errorHandling';
import { 
  LoginCredentials, 
  LoginResponse, 
  MfaRequest, 
  PasswordResetRequest, 
  PasswordUpdateRequest, 
  User 
} from '../../types/user';
import { DataResponse } from '../../types/api';

// Additional auth endpoints not explicitly defined in apiConfig
const authEndpoints = {
  ...endpoints.auth,
  mfaVerify: '/auth/mfa/verify',
  resetRequest: '/auth/password/reset-request',
  resetPassword: '/auth/password/reset',
  updatePassword: '/auth/password/update',
  validateSession: '/auth/validate-session',
  mfaSetup: '/auth/mfa/setup',
  mfaVerifySetup: '/auth/mfa/verify-setup',
  mfaDisable: '/auth/mfa/disable'
};

/**
 * Authenticates a user with username and password credentials
 * @param credentials Login credentials containing username and password
 * @returns Promise resolving to login response with user info and tokens
 */
const login = async (credentials: LoginCredentials): Promise<LoginResponse> => {
  try {
    const response = await apiClient.post<DataResponse<LoginResponse>>(
      authEndpoints.login,
      credentials
    );

    // Extract auth data from the response
    const loginData = response.data;

    // Store tokens if authentication was successful and tokens were provided
    if (loginData.token) {
      setAuthTokens(
        loginData.token,
        loginData.refreshToken,
        loginData.expiresAt
      );
    }

    return loginData;
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'Login');
    throw apiError;
  }
};

/**
 * Logs out the current user by invalidating their session
 * @returns Promise that resolves when logout is complete
 */
const logout = async (): Promise<void> => {
  try {
    // Get the current token
    const token = getToken();

    // Only call the logout endpoint if a token exists
    if (token) {
      await apiClient.post<void>(authEndpoints.logout, {});
    }

    // Clear the stored tokens regardless of API call result
    clearAuthTokens();
  } catch (error) {
    // Parse and log the error
    const apiError = parseApiError(error);
    logError(apiError, 'Logout');
    
    // Clear tokens even if the API call fails
    clearAuthTokens();
  }
};

/**
 * Refreshes the authentication token using the refresh token
 * @returns Promise resolving to true if token refresh was successful
 */
const refreshToken = async (): Promise<boolean> => {
  try {
    // Get the current refresh token
    const refreshToken = getRefreshToken();
    
    // If no refresh token exists, return false
    if (!refreshToken) {
      return false;
    }
    
    // Request a new token using the refresh token
    const response = await apiClient.post<DataResponse<LoginResponse>>(
      authEndpoints.refreshToken,
      { refreshToken }
    );
    
    // Extract auth data from the response
    const refreshData = response.data;
    
    // Store the new tokens
    if (refreshData.token) {
      setAuthTokens(
        refreshData.token,
        refreshData.refreshToken,
        refreshData.expiresAt
      );
      return true;
    }
    
    return false;
  } catch (error) {
    // Parse and log the error, return false to indicate failure
    const apiError = parseApiError(error);
    logError(apiError, 'Token Refresh');
    return false;
  }
};

/**
 * Verifies a multi-factor authentication code
 * @param mfaRequest Object containing MFA token and verification code
 * @returns Promise resolving to login response with user info and tokens
 */
const verifyMfa = async (mfaRequest: MfaRequest): Promise<LoginResponse> => {
  try {
    const response = await apiClient.post<DataResponse<LoginResponse>>(
      authEndpoints.mfaVerify,
      mfaRequest
    );
    
    // Extract auth data from the response
    const loginData = response.data;
    
    // Store tokens
    if (loginData.token) {
      setAuthTokens(
        loginData.token,
        loginData.refreshToken,
        loginData.expiresAt
      );
    }
    
    return loginData;
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'MFA Verification');
    throw apiError;
  }
};

/**
 * Requests a password reset for a user account
 * @param resetRequest Object containing the email address
 * @returns Promise that resolves when request is processed
 */
const requestPasswordReset = async (resetRequest: PasswordResetRequest): Promise<void> => {
  try {
    await apiClient.post<void>(
      authEndpoints.resetRequest,
      resetRequest
    );
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'Password Reset Request');
    throw apiError;
  }
};

/**
 * Resets a user's password using a reset token
 * @param token Password reset token
 * @param newPassword New password
 * @returns Promise that resolves when password is reset
 */
const resetPassword = async (token: string, newPassword: string): Promise<void> => {
  try {
    await apiClient.post<void>(
      authEndpoints.resetPassword,
      { token, newPassword }
    );
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'Password Reset');
    throw apiError;
  }
};

/**
 * Updates the current user's password
 * @param updateRequest Object containing current and new password
 * @returns Promise that resolves when password is updated
 */
const updatePassword = async (updateRequest: PasswordUpdateRequest): Promise<void> => {
  try {
    await apiClient.post<void>(
      authEndpoints.updatePassword,
      updateRequest
    );
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'Password Update');
    throw apiError;
  }
};

/**
 * Retrieves the current authenticated user's profile
 * @returns Promise resolving to current user information
 */
const getCurrentUser = async (): Promise<User> => {
  try {
    const response = await apiClient.get<DataResponse<User>>(
      authEndpoints.profile
    );
    
    return response.data;
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'Get Current User');
    throw apiError;
  }
};

/**
 * Validates the current user session
 * @returns Promise resolving to true if the session is valid
 */
const validateSession = async (): Promise<boolean> => {
  try {
    // Check if the current token is expired
    if (isTokenExpired()) {
      // Try to refresh the token
      const refreshSuccess = await refreshToken();
      
      // If refresh fails, return false
      if (!refreshSuccess) {
        return false;
      }
    }
    
    // Make a request to validate the session
    await apiClient.get<void>(
      authEndpoints.validateSession
    );
    
    // If the request succeeds, the session is valid
    return true;
  } catch (error) {
    // Parse and log the error, return false to indicate invalid session
    const apiError = parseApiError(error);
    logError(apiError, 'Session Validation');
    return false;
  }
};

/**
 * Initiates multi-factor authentication setup for a user
 * @returns Promise resolving to MFA setup information
 */
const setupMfa = async (): Promise<{ qrCodeUrl: string, secret: string }> => {
  try {
    const response = await apiClient.post<DataResponse<{ qrCodeUrl: string, secret: string }>>(
      authEndpoints.mfaSetup,
      {}
    );
    
    return response.data;
  } catch (error) {
    // Parse and log the error, then rethrow
    const apiError = parseApiError(error);
    logError(apiError, 'MFA Setup');
    throw apiError;
  }
};

/**
 * Verifies and completes MFA setup with a verification code
 * @param verificationCode Code entered by the user from their authenticator app
 * @returns Promise resolving to true if MFA setup was successfully verified
 */
const verifyMfaSetup = async (verificationCode: string): Promise<boolean> => {
  try {
    await apiClient.post<void>(
      authEndpoints.mfaVerifySetup,
      { verificationCode }
    );
    
    return true;
  } catch (error) {
    // Parse and log the error, return false to indicate verification failure
    const apiError = parseApiError(error);
    logError(apiError, 'MFA Setup Verification');
    return false;
  }
};

/**
 * Disables multi-factor authentication for the current user
 * @returns Promise resolving to true if MFA was successfully disabled
 */
const disableMfa = async (): Promise<boolean> => {
  try {
    await apiClient.post<void>(
      authEndpoints.mfaDisable,
      {}
    );
    
    return true;
  } catch (error) {
    // Parse and log the error, return false to indicate failure
    const apiError = parseApiError(error);
    logError(apiError, 'MFA Disable');
    return false;
  }
};

// Export the authentication service
export default {
  login,
  logout,
  refreshToken,
  verifyMfa,
  requestPasswordReset,
  resetPassword,
  updatePassword,
  getCurrentUser,
  validateSession,
  setupMfa,
  verifyMfaSetup,
  disableMfa
};