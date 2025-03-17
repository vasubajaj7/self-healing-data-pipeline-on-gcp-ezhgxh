/**
 * Defines the route configuration for the self-healing data pipeline web application.
 * This file contains route path constants and a route configuration array that specifies
 * the structure, access control, and component mapping for all application routes.
 */

import { UserRole, UserPermission } from '../types/user';

/**
 * Interface defining the structure of a route configuration object
 */
export interface RouteConfig {
  path: string;             // Route path, referencing a constant from ROUTES
  component: string;        // Component name to render for this route
  layout: boolean;          // Whether to wrap the component in the main layout
  auth: boolean;            // Whether the route requires authentication
  roles?: string[];         // Optional array of roles allowed to access this route
  permissions?: string[];   // Optional array of permissions required to access this route
  children?: RouteConfig[]; // Optional array of child routes
}

/**
 * Object containing route path constants for the application
 */
export const ROUTES = {
  LOGIN: '/login',
  DASHBOARD: '/',
  PIPELINE_MANAGEMENT: '/pipelines',
  DATA_QUALITY: '/quality',
  SELF_HEALING: '/self-healing',
  ALERTING: '/alerts',
  CONFIGURATION: '/configuration',
  ADMINISTRATION: '/admin',
  PROFILE: '/profile',
  NOT_FOUND: '*'
};

/**
 * Object containing sub-route path constants for self-healing section
 */
const SELF_HEALING_ROUTES = {
  ACTIVITY_LOG: '/activity-log',
  MODEL_MANAGEMENT: '/models',
  CONFIGURATION: '/config'
};

/**
 * Object containing sub-route path constants for alerting section
 */
const ALERTING_ROUTES = {
  DASHBOARD: '/dashboard',
  NOTIFICATION_CONFIG: '/notification-config',
  HISTORY: '/history'
};

/**
 * Object containing sub-route path constants for optimization section
 */
const OPTIMIZATION_ROUTES = {
  QUERY: '/query',
  SCHEMA: '/schema',
  RESOURCE: '/resource'
};

/**
 * Array defining the route configuration for the application
 */
export const routes: RouteConfig[] = [
  {
    path: 'LOGIN',
    component: 'Login',
    layout: false,
    auth: false
  },
  {
    path: 'DASHBOARD',
    component: 'Dashboard',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER', 'DATA_ANALYST', 'PIPELINE_OPERATOR'],
    permissions: ['VIEW_DASHBOARD']
  },
  {
    path: 'PIPELINE_MANAGEMENT',
    component: 'PipelineManagement',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER', 'PIPELINE_OPERATOR'],
    permissions: ['VIEW_PIPELINES']
  },
  {
    path: 'DATA_QUALITY',
    component: 'DataQuality',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER', 'DATA_ANALYST'],
    permissions: ['VIEW_QUALITY']
  },
  {
    path: 'SELF_HEALING',
    component: 'SelfHealing',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER'],
    permissions: ['VIEW_HEALING'],
    children: [
      {
        path: 'SELF_HEALING_ROUTES.ACTIVITY_LOG',
        component: 'ActivityLog'
      },
      {
        path: 'SELF_HEALING_ROUTES.MODEL_MANAGEMENT',
        component: 'ModelManagement',
        permissions: ['MANAGE_HEALING']
      },
      {
        path: 'SELF_HEALING_ROUTES.CONFIGURATION',
        component: 'HealingConfiguration',
        permissions: ['MANAGE_HEALING']
      }
    ]
  },
  {
    path: 'ALERTING',
    component: 'Alerting',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER', 'PIPELINE_OPERATOR'],
    permissions: ['VIEW_ALERTS'],
    children: [
      {
        path: 'ALERTING_ROUTES.DASHBOARD',
        component: 'AlertDashboard'
      },
      {
        path: 'ALERTING_ROUTES.NOTIFICATION_CONFIG',
        component: 'NotificationConfig',
        permissions: ['MANAGE_ALERTS']
      },
      {
        path: 'ALERTING_ROUTES.HISTORY',
        component: 'AlertHistory'
      }
    ]
  },
  {
    path: 'CONFIGURATION',
    component: 'Configuration',
    layout: true,
    auth: true,
    roles: ['ADMIN', 'DATA_ENGINEER'],
    permissions: ['VIEW_CONFIGURATION']
  },
  {
    path: 'ADMINISTRATION',
    component: 'Administration',
    layout: true,
    auth: true,
    roles: ['ADMIN'],
    permissions: ['VIEW_ADMIN', 'MANAGE_USERS']
  },
  {
    path: 'PROFILE',
    component: 'Profile',
    layout: true,
    auth: true
  },
  {
    path: 'NOT_FOUND',
    component: 'NotFound',
    layout: true,
    auth: false
  }
];