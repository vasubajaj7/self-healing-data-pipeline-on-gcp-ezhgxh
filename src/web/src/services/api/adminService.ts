/**
 * Administrative Service
 * 
 * This service module provides functions for administrative operations in the self-healing data pipeline
 * web application, including user management, role management, system settings, system health monitoring,
 * and audit logging.
 */

import apiClient from './apiClient';
import { endpoints } from '../../config/apiConfig';
import { User, UserRole, UserCreateRequest, UserUpdateRequest } from '../../types/user';
import { DataResponse, PaginatedResponse, HealthCheckResponse } from '../../types/api';

// User Management Functions

/**
 * Retrieves a paginated list of users with optional filtering
 * @param page Page number to retrieve (1-based)
 * @param pageSize Number of users per page
 * @param search Optional search term to filter users
 * @param role Optional role filter
 * @param activeOnly If true, only return active users
 * @returns Promise resolving to paginated list of users
 */
const getUsers = async (
  page: number,
  pageSize: number,
  search?: string,
  role?: string,
  activeOnly?: boolean
): Promise<PaginatedResponse<User>> => {
  // Construct query parameters
  const queryParams: Record<string, any> = {
    page,
    pageSize
  };

  // Add optional filters if provided
  if (search) queryParams.search = search;
  if (role) queryParams.role = role;
  if (activeOnly !== undefined) queryParams.activeOnly = activeOnly;

  // Make API request
  return apiClient.get(endpoints.admin.users, { params: queryParams });
};

/**
 * Retrieves a specific user by ID
 * @param userId User ID to retrieve
 * @returns Promise resolving to user details
 */
const getUserById = async (userId: string): Promise<DataResponse<User>> => {
  return apiClient.get(endpoints.admin.userDetails(userId));
};

/**
 * Creates a new user in the system
 * @param userData User data for creation
 * @returns Promise resolving to created user details
 */
const createUser = async (userData: UserCreateRequest): Promise<DataResponse<User>> => {
  return apiClient.post(endpoints.admin.users, userData);
};

/**
 * Updates an existing user's information
 * @param userId ID of the user to update
 * @param userData User data to update
 * @returns Promise resolving to updated user details
 */
const updateUser = async (
  userId: string,
  userData: UserUpdateRequest
): Promise<DataResponse<User>> => {
  return apiClient.put(endpoints.admin.userDetails(userId), userData);
};

/**
 * Deletes a user from the system
 * @param userId ID of the user to delete
 * @returns Promise resolving to deletion confirmation
 */
const deleteUser = async (
  userId: string
): Promise<DataResponse<{ success: boolean; message: string }>> => {
  return apiClient.delete(endpoints.admin.userDetails(userId));
};

// Role Management Functions

/**
 * Retrieves a list of all roles in the system
 * @returns Promise resolving to list of roles
 */
const getRoles = async (): Promise<DataResponse<UserRole[]>> => {
  return apiClient.get(endpoints.admin.roles);
};

/**
 * Retrieves a specific role by ID
 * @param roleId Role ID to retrieve
 * @returns Promise resolving to role details
 */
const getRoleById = async (roleId: string): Promise<DataResponse<UserRole>> => {
  return apiClient.get(endpoints.admin.roleDetails(roleId));
};

/**
 * Creates a new role in the system
 * @param roleData Role data for creation
 * @returns Promise resolving to created role details
 */
const createRole = async (roleData: object): Promise<DataResponse<UserRole>> => {
  return apiClient.post(endpoints.admin.roles, roleData);
};

/**
 * Updates an existing role's information
 * @param roleId ID of the role to update
 * @param roleData Role data to update
 * @returns Promise resolving to updated role details
 */
const updateRole = async (
  roleId: string,
  roleData: object
): Promise<DataResponse<UserRole>> => {
  return apiClient.put(endpoints.admin.roleDetails(roleId), roleData);
};

/**
 * Deletes a role from the system
 * @param roleId ID of the role to delete
 * @returns Promise resolving to deletion confirmation
 */
const deleteRole = async (
  roleId: string
): Promise<DataResponse<{ success: boolean; message: string }>> => {
  return apiClient.delete(endpoints.admin.roleDetails(roleId));
};

// System Settings Functions

/**
 * Retrieves the current system settings
 * @returns Promise resolving to system settings
 */
const getSystemSettings = async (): Promise<DataResponse<any>> => {
  return apiClient.get(endpoints.admin.settings);
};

/**
 * Updates the system settings
 * @param settingsData Settings data to update
 * @returns Promise resolving to updated system settings
 */
const updateSystemSettings = async (settingsData: object): Promise<DataResponse<any>> => {
  return apiClient.put(endpoints.admin.settings, settingsData);
};

// System Health Functions

/**
 * Retrieves the current system health status
 * @returns Promise resolving to system health information
 */
const getSystemHealth = async (): Promise<HealthCheckResponse> => {
  return apiClient.get(endpoints.admin.health);
};

/**
 * Checks the status of a specific system component
 * @param componentName Name of the component to check
 * @returns Promise resolving to component status information
 */
const checkComponentStatus = async (componentName: string): Promise<DataResponse<any>> => {
  return apiClient.get(`${endpoints.admin.health}/${componentName}`);
};

// Audit Logging Functions

/**
 * Retrieves system audit logs with filtering options
 * @param page Page number to retrieve (1-based)
 * @param pageSize Number of logs per page
 * @param startDate Optional start date filter
 * @param endDate Optional end date filter
 * @param userId Optional user ID filter
 * @param actionType Optional action type filter
 * @param resourceType Optional resource type filter
 * @returns Promise resolving to paginated list of audit logs
 */
const getAuditLogs = async (
  page: number,
  pageSize: number,
  startDate?: Date,
  endDate?: Date,
  userId?: string,
  actionType?: string,
  resourceType?: string
): Promise<PaginatedResponse<any>> => {
  // Construct query parameters
  const queryParams: Record<string, any> = {
    page,
    pageSize
  };

  // Add optional filters if provided
  if (startDate) queryParams.startDate = startDate.toISOString();
  if (endDate) queryParams.endDate = endDate.toISOString();
  if (userId) queryParams.userId = userId;
  if (actionType) queryParams.actionType = actionType;
  if (resourceType) queryParams.resourceType = resourceType;

  // Make API request
  return apiClient.get(endpoints.admin.audit, { params: queryParams });
};

// Authentication Functions

/**
 * Authenticates a user with username/email and password
 * @param usernameOrEmail Username or email address
 * @param password User password
 * @returns Promise resolving to authentication result with token
 */
const login = async (
  usernameOrEmail: string,
  password: string
): Promise<DataResponse<{ token: string; user: User }>> => {
  return apiClient.post(endpoints.admin.login, { usernameOrEmail, password });
};

// Export all functions
export default {
  // User management
  getUsers,
  getUserById,
  createUser,
  updateUser,
  deleteUser,
  
  // Role management
  getRoles,
  getRoleById,
  createRole,
  updateRole,
  deleteRole,
  
  // System settings
  getSystemSettings,
  updateSystemSettings,
  
  // System health
  getSystemHealth,
  checkComponentStatus,
  
  // Audit logging
  getAuditLogs,
  
  // Authentication
  login
};