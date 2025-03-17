/**
 * Test data fixtures for Playwright end-to-end tests of the self-healing data pipeline application.
 * Provides mock user data, login responses, and other test data needed for consistent and reliable end-to-end testing.
 */

import { 
  UserRole, 
  UserPermission, 
  User, 
  LoginResponse 
} from '../../../web/src/types/user';

import { 
  ID, 
  Timestamp, 
  PipelineStatus, 
  AlertSeverity, 
  QualityStatus, 
  HealingStatus 
} from '../../../web/src/types/global';

// Type definitions
export type UserCredentials = { username: string; password: string };
export type MockApiResponse<T> = { statusCode: number; data: T | null; error: object | null };

// Interfaces
export interface TestUser {
  username: string;
  email: string;
  password: string;
  firstName: string;
  lastName: string;
  role: UserRole;
  isActive: boolean;
  mfaEnabled: boolean;
}

export interface MockLoginResponse {
  statusCode: number;
  data: LoginResponse | null;
  error: object | null;
}

export interface MockMfaResponse {
  statusCode: number;
  data: object | null;
  error: object | null;
}

export interface MockPipeline {
  id: ID;
  name: string;
  description: string;
  status: PipelineStatus;
  lastRun: Timestamp;
  successRate: number;
}

export interface MockDataset {
  id: ID;
  name: string;
  qualityScore: number;
  status: QualityStatus;
  trend: string;
  issueCount: number;
}

export interface MockHealingIssue {
  id: ID;
  description: string;
  severity: AlertSeverity;
  status: HealingStatus;
  confidence: number;
  detectedAt: Timestamp;
}

export interface MockAlert {
  id: ID;
  description: string;
  severity: AlertSeverity;
  acknowledged: boolean;
  createdAt: Timestamp;
}

// Mock user data for different roles
export const users = {
  admin: {
    username: 'admin',
    email: 'admin@example.com',
    password: 'Admin123!',
    firstName: 'Admin',
    lastName: 'User',
    role: UserRole.ADMIN,
    isActive: true,
    mfaEnabled: false
  } as TestUser,
  
  engineer: {
    username: 'engineer',
    email: 'engineer@example.com',
    password: 'Engineer123!',
    firstName: 'Data',
    lastName: 'Engineer',
    role: UserRole.DATA_ENGINEER,
    isActive: true,
    mfaEnabled: false
  } as TestUser,
  
  analyst: {
    username: 'analyst',
    email: 'analyst@example.com',
    password: 'Analyst123!',
    firstName: 'Data',
    lastName: 'Analyst',
    role: UserRole.DATA_ANALYST,
    isActive: true,
    mfaEnabled: false
  } as TestUser,
  
  operator: {
    username: 'operator',
    email: 'operator@example.com',
    password: 'Operator123!',
    firstName: 'Pipeline',
    lastName: 'Operator',
    role: UserRole.PIPELINE_OPERATOR,
    isActive: true,
    mfaEnabled: false
  } as TestUser,
  
  viewer: {
    username: 'viewer',
    email: 'viewer@example.com',
    password: 'Viewer123!',
    firstName: 'Viewer',
    lastName: 'User',
    role: UserRole.VIEWER,
    isActive: true,
    mfaEnabled: false
  } as TestUser,
  
  inactive: {
    username: 'inactive',
    email: 'inactive@example.com',
    password: 'Inactive123!',
    firstName: 'Inactive',
    lastName: 'User',
    role: UserRole.VIEWER,
    isActive: false,
    mfaEnabled: false
  } as TestUser,
  
  mfaUser: {
    username: 'mfauser',
    email: 'mfauser@example.com',
    password: 'MfaUser123!',
    firstName: 'MFA',
    lastName: 'User',
    role: UserRole.DATA_ANALYST,
    isActive: true,
    mfaEnabled: true
  } as TestUser
};

// Mock login response data for different scenarios
export const loginResponses = {
  admin: {
    statusCode: 200,
    data: {
      user: {
        id: '1',
        username: users.admin.username,
        email: users.admin.email,
        firstName: users.admin.firstName,
        lastName: users.admin.lastName,
        role: users.admin.role,
        isActive: users.admin.isActive,
        mfaEnabled: users.admin.mfaEnabled,
        lastLogin: '2023-06-15T10:30:00Z',
        createdAt: '2023-01-01T00:00:00Z',
        updatedAt: '2023-06-15T10:30:00Z'
      },
      token: 'mock-jwt-token-admin',
      refreshToken: 'mock-refresh-token-admin',
      expiresAt: Date.now() + 3600000, // 1 hour from now
      requiresMfa: false,
      mfaToken: null
    },
    error: null
  } as MockLoginResponse,
  
  engineer: {
    statusCode: 200,
    data: {
      user: {
        id: '2',
        username: users.engineer.username,
        email: users.engineer.email,
        firstName: users.engineer.firstName,
        lastName: users.engineer.lastName,
        role: users.engineer.role,
        isActive: users.engineer.isActive,
        mfaEnabled: users.engineer.mfaEnabled,
        lastLogin: '2023-06-15T09:45:00Z',
        createdAt: '2023-01-15T00:00:00Z',
        updatedAt: '2023-06-15T09:45:00Z'
      },
      token: 'mock-jwt-token-engineer',
      refreshToken: 'mock-refresh-token-engineer',
      expiresAt: Date.now() + 3600000,
      requiresMfa: false,
      mfaToken: null
    },
    error: null
  } as MockLoginResponse,
  
  analyst: {
    statusCode: 200,
    data: {
      user: {
        id: '3',
        username: users.analyst.username,
        email: users.analyst.email,
        firstName: users.analyst.firstName,
        lastName: users.analyst.lastName,
        role: users.analyst.role,
        isActive: users.analyst.isActive,
        mfaEnabled: users.analyst.mfaEnabled,
        lastLogin: '2023-06-14T16:20:00Z',
        createdAt: '2023-02-10T00:00:00Z',
        updatedAt: '2023-06-14T16:20:00Z'
      },
      token: 'mock-jwt-token-analyst',
      refreshToken: 'mock-refresh-token-analyst',
      expiresAt: Date.now() + 3600000,
      requiresMfa: false,
      mfaToken: null
    },
    error: null
  } as MockLoginResponse,
  
  operator: {
    statusCode: 200,
    data: {
      user: {
        id: '4',
        username: users.operator.username,
        email: users.operator.email,
        firstName: users.operator.firstName,
        lastName: users.operator.lastName,
        role: users.operator.role,
        isActive: users.operator.isActive,
        mfaEnabled: users.operator.mfaEnabled,
        lastLogin: '2023-06-15T08:15:00Z',
        createdAt: '2023-01-20T00:00:00Z',
        updatedAt: '2023-06-15T08:15:00Z'
      },
      token: 'mock-jwt-token-operator',
      refreshToken: 'mock-refresh-token-operator',
      expiresAt: Date.now() + 3600000,
      requiresMfa: false,
      mfaToken: null
    },
    error: null
  } as MockLoginResponse,
  
  viewer: {
    statusCode: 200,
    data: {
      user: {
        id: '5',
        username: users.viewer.username,
        email: users.viewer.email,
        firstName: users.viewer.firstName,
        lastName: users.viewer.lastName,
        role: users.viewer.role,
        isActive: users.viewer.isActive,
        mfaEnabled: users.viewer.mfaEnabled,
        lastLogin: '2023-06-10T14:30:00Z',
        createdAt: '2023-03-05T00:00:00Z',
        updatedAt: '2023-06-10T14:30:00Z'
      },
      token: 'mock-jwt-token-viewer',
      refreshToken: 'mock-refresh-token-viewer',
      expiresAt: Date.now() + 3600000,
      requiresMfa: false,
      mfaToken: null
    },
    error: null
  } as MockLoginResponse,
  
  mfaRequired: {
    statusCode: 200,
    data: {
      user: null,
      token: null,
      refreshToken: null,
      expiresAt: 0,
      requiresMfa: true,
      mfaToken: 'mock-mfa-token'
    },
    error: null
  } as MockLoginResponse,
  
  invalidCredentials: {
    statusCode: 401,
    data: null,
    error: {
      message: 'Invalid username or password',
      code: 'INVALID_CREDENTIALS'
    }
  } as MockLoginResponse,
  
  inactiveAccount: {
    statusCode: 403,
    data: null,
    error: {
      message: 'Account is inactive',
      code: 'INACTIVE_ACCOUNT'
    }
  } as MockLoginResponse
};

// Mock MFA verification response data
export const mfaResponses = {
  valid: {
    statusCode: 200,
    data: {
      user: {
        id: '6',
        username: users.mfaUser.username,
        email: users.mfaUser.email,
        firstName: users.mfaUser.firstName,
        lastName: users.mfaUser.lastName,
        role: users.mfaUser.role,
        isActive: users.mfaUser.isActive,
        mfaEnabled: users.mfaUser.mfaEnabled,
        lastLogin: '2023-06-15T11:00:00Z',
        createdAt: '2023-02-15T00:00:00Z',
        updatedAt: '2023-06-15T11:00:00Z'
      },
      token: 'mock-jwt-token-mfa-user',
      refreshToken: 'mock-refresh-token-mfa-user',
      expiresAt: Date.now() + 3600000
    },
    error: null
  } as MockMfaResponse,
  
  invalid: {
    statusCode: 401,
    data: null,
    error: {
      message: 'Invalid verification code',
      code: 'INVALID_MFA_CODE'
    }
  } as MockMfaResponse,
  
  expired: {
    statusCode: 401,
    data: null,
    error: {
      message: 'MFA session has expired',
      code: 'EXPIRED_MFA_SESSION'
    }
  } as MockMfaResponse
};

// Mock pipeline data
export const pipelineData = {
  pipelines: [
    {
      id: 'pipe-001',
      name: 'analytics_daily',
      description: 'Daily analytics data processing',
      status: PipelineStatus.HEALTHY,
      lastRun: '2023-06-15T05:30:00Z',
      successRate: 98.5
    },
    {
      id: 'pipe-002',
      name: 'customer_load',
      description: 'Customer data integration',
      status: PipelineStatus.HEALTHY,
      lastRun: '2023-06-15T04:15:00Z',
      successRate: 97.2
    },
    {
      id: 'pipe-003',
      name: 'product_enrich',
      description: 'Product data enrichment',
      status: PipelineStatus.ERROR,
      lastRun: '2023-06-15T03:45:00Z',
      successRate: 89.4
    },
    {
      id: 'pipe-004',
      name: 'inventory_sync',
      description: 'Inventory synchronization',
      status: PipelineStatus.HEALTHY,
      lastRun: '2023-06-15T02:30:00Z',
      successRate: 99.1
    },
    {
      id: 'pipe-005',
      name: 'sales_metrics',
      description: 'Sales metrics calculation',
      status: PipelineStatus.WARNING,
      lastRun: '2023-06-15T01:15:00Z',
      successRate: 92.7
    }
  ] as MockPipeline[],
  
  executions: [
    {
      id: 'exec-001',
      pipelineId: 'pipe-001',
      startTime: '2023-06-15T05:30:00Z',
      endTime: '2023-06-15T05:45:23Z',
      status: 'COMPLETED',
      recordsProcessed: 15420,
      errorDetails: null
    },
    {
      id: 'exec-002',
      pipelineId: 'pipe-002',
      startTime: '2023-06-15T04:15:00Z',
      endTime: '2023-06-15T04:28:12Z',
      status: 'COMPLETED',
      recordsProcessed: 8730,
      errorDetails: null
    },
    {
      id: 'exec-003',
      pipelineId: 'pipe-003',
      startTime: '2023-06-15T03:45:00Z',
      endTime: '2023-06-15T03:57:18Z',
      status: 'FAILED',
      recordsProcessed: 4510,
      errorDetails: 'Data quality check failed: Null values in required fields'
    },
    {
      id: 'exec-004',
      pipelineId: 'pipe-004',
      startTime: '2023-06-15T02:30:00Z',
      endTime: '2023-06-15T02:42:45Z',
      status: 'COMPLETED',
      recordsProcessed: 6280,
      errorDetails: null
    },
    {
      id: 'exec-005',
      pipelineId: 'pipe-005',
      startTime: '2023-06-15T01:15:00Z',
      endTime: '2023-06-15T01:32:38Z',
      status: 'COMPLETED',
      recordsProcessed: 12340,
      errorDetails: 'Warning: 15 records with outlier values detected'
    }
  ],
  
  tasks: [
    {
      id: 'task-001',
      executionId: 'exec-003',
      taskId: 'extract',
      taskType: 'EXTRACT',
      startTime: '2023-06-15T03:45:00Z',
      endTime: '2023-06-15T03:48:32Z',
      status: 'COMPLETED'
    },
    {
      id: 'task-002',
      executionId: 'exec-003',
      taskId: 'transform',
      taskType: 'TRANSFORM',
      startTime: '2023-06-15T03:48:35Z',
      endTime: '2023-06-15T03:52:18Z',
      status: 'COMPLETED'
    },
    {
      id: 'task-003',
      executionId: 'exec-003',
      taskId: 'quality_check',
      taskType: 'VALIDATION',
      startTime: '2023-06-15T03:52:20Z',
      endTime: '2023-06-15T03:57:18Z',
      status: 'FAILED'
    }
  ]
};

// Mock data quality data
export const qualityData = {
  datasets: [
    {
      id: 'ds-001',
      name: 'customer_data',
      qualityScore: 98,
      status: QualityStatus.PASSED,
      trend: 'up',
      issueCount: 2
    },
    {
      id: 'ds-002',
      name: 'sales_metrics',
      qualityScore: 89,
      status: QualityStatus.WARNING,
      trend: 'down',
      issueCount: 15
    },
    {
      id: 'ds-003',
      name: 'product_catalog',
      qualityScore: 100,
      status: QualityStatus.PASSED,
      trend: 'stable',
      issueCount: 0
    },
    {
      id: 'ds-004',
      name: 'inventory',
      qualityScore: 99,
      status: QualityStatus.PASSED,
      trend: 'stable',
      issueCount: 1
    },
    {
      id: 'ds-005',
      name: 'vendor_data',
      qualityScore: 94,
      status: QualityStatus.PASSED,
      trend: 'up',
      issueCount: 6
    }
  ] as MockDataset[],
  
  validationResults: [
    {
      id: 'val-001',
      datasetId: 'ds-002',
      ruleId: 'rule-001',
      executionId: 'exec-003',
      timestamp: '2023-06-15T03:55:12Z',
      status: QualityStatus.FAILED,
      details: 'Found 15 records with null values in sales_amount column'
    },
    {
      id: 'val-002',
      datasetId: 'ds-002',
      ruleId: 'rule-005',
      executionId: 'exec-005',
      timestamp: '2023-06-15T01:28:45Z',
      status: QualityStatus.WARNING,
      details: 'Found 3 records with outlier values in discount_percent column'
    },
    {
      id: 'val-003',
      datasetId: 'ds-001',
      ruleId: 'rule-002',
      executionId: 'exec-002',
      timestamp: '2023-06-15T04:25:18Z',
      status: QualityStatus.WARNING,
      details: 'Found 2 records with potentially invalid email format'
    }
  ],
  
  rules: [
    {
      id: 'rule-001',
      name: 'non_null_check',
      datasetId: 'ds-002',
      description: 'Check for null values in required fields',
      ruleType: 'NULLABILITY',
      severity: AlertSeverity.HIGH
    },
    {
      id: 'rule-002',
      name: 'email_format_check',
      datasetId: 'ds-001',
      description: 'Validate email format',
      ruleType: 'FORMAT',
      severity: AlertSeverity.MEDIUM
    },
    {
      id: 'rule-003',
      name: 'date_range_check',
      datasetId: 'ds-004',
      description: 'Validate date is within acceptable range',
      ruleType: 'RANGE',
      severity: AlertSeverity.MEDIUM
    },
    {
      id: 'rule-004',
      name: 'referential_integrity',
      datasetId: 'ds-003',
      description: 'Check referential integrity with related tables',
      ruleType: 'REFERENTIAL',
      severity: AlertSeverity.HIGH
    },
    {
      id: 'rule-005',
      name: 'value_range_check',
      datasetId: 'ds-002',
      description: 'Check numeric values are within acceptable ranges',
      ruleType: 'RANGE',
      severity: AlertSeverity.MEDIUM
    }
  ]
};

// Mock self-healing data
export const healingData = {
  issues: [
    {
      id: 'issue-001',
      description: 'Null values in sales_amount',
      severity: AlertSeverity.HIGH,
      status: HealingStatus.IN_PROGRESS,
      confidence: 87,
      detectedAt: '2023-06-15T03:55:12Z'
    },
    {
      id: 'issue-002',
      description: 'Outliers in discount_percent',
      severity: AlertSeverity.MEDIUM,
      status: HealingStatus.IN_PROGRESS,
      confidence: 92,
      detectedAt: '2023-06-15T01:28:45Z'
    },
    {
      id: 'issue-003',
      description: 'Schema drift in vendor_api',
      severity: AlertSeverity.HIGH,
      status: HealingStatus.COMPLETED,
      confidence: 95,
      detectedAt: '2023-06-14T22:15:30Z'
    },
    {
      id: 'issue-004',
      description: 'Query performance degradation',
      severity: AlertSeverity.MEDIUM,
      status: HealingStatus.PENDING,
      confidence: 78,
      detectedAt: '2023-06-15T08:40:15Z'
    },
    {
      id: 'issue-005',
      description: 'Data type mismatch in new records',
      severity: AlertSeverity.HIGH,
      status: HealingStatus.APPROVAL_REQUIRED,
      confidence: 64,
      detectedAt: '2023-06-15T10:12:45Z'
    }
  ] as MockHealingIssue[],
  
  actions: [
    {
      id: 'action-001',
      issueId: 'issue-001',
      type: 'IMPUTATION',
      description: 'Impute null values with column median',
      confidenceScore: 87,
      status: HealingStatus.IN_PROGRESS
    },
    {
      id: 'action-002',
      issueId: 'issue-002',
      type: 'FILTER',
      description: 'Filter outlier values based on IQR',
      confidenceScore: 92,
      status: HealingStatus.IN_PROGRESS
    },
    {
      id: 'action-003',
      issueId: 'issue-003',
      type: 'SCHEMA_ADAPTATION',
      description: 'Adapt pipeline to handle new schema',
      confidenceScore: 95,
      status: HealingStatus.COMPLETED
    },
    {
      id: 'action-004',
      issueId: 'issue-004',
      type: 'QUERY_OPTIMIZATION',
      description: 'Apply query optimization recommendations',
      confidenceScore: 78,
      status: HealingStatus.PENDING
    },
    {
      id: 'action-005',
      issueId: 'issue-005',
      type: 'TYPE_CONVERSION',
      description: 'Apply data type conversions to new records',
      confidenceScore: 64,
      status: HealingStatus.APPROVAL_REQUIRED
    }
  ],
  
  executions: [
    {
      id: 'heal-001',
      actionId: 'action-001',
      startTime: '2023-06-15T03:58:00Z',
      endTime: null,
      status: HealingStatus.IN_PROGRESS,
      impactedRecords: 15,
      details: 'Applying median imputation to null values'
    },
    {
      id: 'heal-002',
      actionId: 'action-002',
      startTime: '2023-06-15T01:35:00Z',
      endTime: null,
      status: HealingStatus.IN_PROGRESS,
      impactedRecords: 3,
      details: 'Filtering outlier values based on 1.5 IQR'
    },
    {
      id: 'heal-003',
      actionId: 'action-003',
      startTime: '2023-06-14T22:18:00Z',
      endTime: '2023-06-14T22:25:45Z',
      status: HealingStatus.COMPLETED,
      impactedRecords: 0,
      details: 'Successfully adapted pipeline for new schema'
    }
  ]
};

// Mock alert data
export const alertData = {
  alerts: [
    {
      id: 'alert-001',
      description: 'BigQuery load failed',
      severity: AlertSeverity.HIGH,
      acknowledged: false,
      createdAt: '2023-06-15T09:45:00Z'
    },
    {
      id: 'alert-002',
      description: 'Schema drift detected',
      severity: AlertSeverity.MEDIUM,
      acknowledged: false,
      createdAt: '2023-06-15T09:30:00Z'
    },
    {
      id: 'alert-003',
      description: 'API response slowdown',
      severity: AlertSeverity.MEDIUM,
      acknowledged: false,
      createdAt: '2023-06-15T09:13:00Z'
    },
    {
      id: 'alert-004',
      description: 'Storage utilization high',
      severity: AlertSeverity.LOW,
      acknowledged: false,
      createdAt: '2023-06-15T08:45:00Z'
    },
    {
      id: 'alert-005',
      description: 'Query performance degrading',
      severity: AlertSeverity.LOW,
      acknowledged: false,
      createdAt: '2023-06-15T07:30:00Z'
    }
  ] as MockAlert[],
  
  notifications: [
    {
      id: 'notif-001',
      alertId: 'alert-001',
      channel: 'TEAMS',
      recipient: 'data-pipeline-alerts',
      sentAt: '2023-06-15T09:45:30Z',
      status: 'DELIVERED'
    },
    {
      id: 'notif-002',
      alertId: 'alert-001',
      channel: 'EMAIL',
      recipient: 'oncall@example.com',
      sentAt: '2023-06-15T09:45:35Z',
      status: 'DELIVERED'
    },
    {
      id: 'notif-003',
      alertId: 'alert-002',
      channel: 'TEAMS',
      recipient: 'data-pipeline-alerts',
      sentAt: '2023-06-15T09:30:15Z',
      status: 'DELIVERED'
    },
    {
      id: 'notif-004',
      alertId: 'alert-003',
      channel: 'TEAMS',
      recipient: 'data-pipeline-alerts',
      sentAt: '2023-06-15T09:13:20Z',
      status: 'DELIVERED'
    }
  ]
};

// Mock configuration data
export const configData = {
  dataSources: [
    {
      id: 'src-001',
      name: 'sales_gcs',
      type: 'GCS',
      status: 'ACTIVE',
      lastUsed: '2023-06-15T09:45:00Z'
    },
    {
      id: 'src-002',
      name: 'customer_db',
      type: 'CLOUD_SQL',
      status: 'ACTIVE',
      lastUsed: '2023-06-15T09:30:00Z'
    },
    {
      id: 'src-003',
      name: 'product_api',
      type: 'REST_API',
      status: 'WARNING',
      lastUsed: '2023-06-15T09:13:00Z'
    },
    {
      id: 'src-004',
      name: 'vendor_sftp',
      type: 'SFTP',
      status: 'ACTIVE',
      lastUsed: '2023-06-15T07:45:00Z'
    },
    {
      id: 'src-005',
      name: 'finance_db',
      type: 'CLOUD_SQL',
      status: 'ACTIVE',
      lastUsed: '2023-06-14T15:30:00Z'
    }
  ],
  
  connections: [
    {
      id: 'conn-001',
      sourceId: 'src-001',
      details: {
        bucketName: 'example-data-lake',
        path: '/sales/daily/',
        filePattern: '*.parquet'
      }
    },
    {
      id: 'conn-002',
      sourceId: 'src-002',
      details: {
        instanceName: 'customer-db-prod',
        database: 'customers',
        table: 'customer_profiles'
      }
    },
    {
      id: 'conn-003',
      sourceId: 'src-003',
      details: {
        url: 'https://api.example.com/v2/products',
        authType: 'OAUTH2',
        rateLimits: {
          requestsPerMinute: 60,
          concurrentRequests: 5
        }
      }
    }
  ]
};

// Mock API responses for various endpoints
export const mockApiResponses = {
  dashboard: {
    summary: {
      statusCode: 200,
      data: {
        pipelineHealth: {
          healthy: 12,
          warning: 2,
          error: 1,
          inactive: 3,
          healthyPercentage: 98
        },
        dataQuality: {
          passed: 25,
          warning: 3,
          failed: 2,
          passedPercentage: 94
        },
        selfHealing: {
          attempted: 18,
          successful: 15,
          pending: 2,
          failed: 1,
          successPercentage: 87
        },
        alerts: {
          critical: 1,
          high: 1,
          medium: 2,
          low: 2,
          total: 6
        }
      },
      error: null
    },
    recentExecutions: {
      statusCode: 200,
      data: [
        {
          id: 'exec-001',
          pipelineId: 'pipe-001',
          pipelineName: 'analytics_daily',
          status: 'COMPLETED',
          startTime: '2023-06-15T05:30:00Z',
          endTime: '2023-06-15T05:45:23Z'
        },
        {
          id: 'exec-002',
          pipelineId: 'pipe-002',
          pipelineName: 'customer_load',
          status: 'COMPLETED',
          startTime: '2023-06-15T04:15:00Z',
          endTime: '2023-06-15T04:28:12Z'
        },
        {
          id: 'exec-003',
          pipelineId: 'pipe-003',
          pipelineName: 'product_enrich',
          status: 'FAILED',
          startTime: '2023-06-15T03:45:00Z',
          endTime: '2023-06-15T03:57:18Z'
        },
        {
          id: 'exec-004',
          pipelineId: 'pipe-004',
          pipelineName: 'inventory_sync',
          status: 'COMPLETED',
          startTime: '2023-06-15T02:30:00Z',
          endTime: '2023-06-15T02:42:45Z'
        }
      ],
      error: null
    }
  },
  
  pipeline: {
    list: {
      statusCode: 200,
      data: {
        pipelines: pipelineData.pipelines,
        pagination: {
          page: 1,
          pageSize: 10,
          totalItems: 5,
          totalPages: 1
        }
      },
      error: null
    },
    details: {
      statusCode: 200,
      data: {
        pipeline: pipelineData.pipelines[2],
        executions: [pipelineData.executions[2]],
        tasks: pipelineData.tasks
      },
      error: null
    }
  },
  
  quality: {
    list: {
      statusCode: 200,
      data: {
        datasets: qualityData.datasets,
        pagination: {
          page: 1,
          pageSize: 10,
          totalItems: 5,
          totalPages: 1
        }
      },
      error: null
    },
    details: {
      statusCode: 200,
      data: {
        dataset: qualityData.datasets[1],
        validationResults: [qualityData.validationResults[0], qualityData.validationResults[1]],
        rules: [qualityData.rules[0], qualityData.rules[4]]
      },
      error: null
    }
  },
  
  healing: {
    list: {
      statusCode: 200,
      data: {
        issues: healingData.issues,
        pagination: {
          page: 1,
          pageSize: 10,
          totalItems: 5,
          totalPages: 1
        }
      },
      error: null
    },
    details: {
      statusCode: 200,
      data: {
        issue: healingData.issues[0],
        action: healingData.actions[0],
        execution: healingData.executions[0]
      },
      error: null
    }
  },
  
  alerts: {
    list: {
      statusCode: 200,
      data: {
        alerts: alertData.alerts,
        pagination: {
          page: 1,
          pageSize: 10,
          totalItems: 5,
          totalPages: 1
        }
      },
      error: null
    },
    details: {
      statusCode: 200,
      data: {
        alert: alertData.alerts[0],
        notifications: [alertData.notifications[0], alertData.notifications[1]],
        relatedAlerts: [alertData.alerts[1]],
        suggestedActions: [
          'Increase BigQuery slot reservation',
          'Implement load job scheduling',
          'Review query optimization opportunities'
        ]
      },
      error: null
    }
  },
  
  config: {
    dataSources: {
      statusCode: 200,
      data: {
        dataSources: configData.dataSources,
        pagination: {
          page: 1,
          pageSize: 10,
          totalItems: 5,
          totalPages: 1
        }
      },
      error: null
    },
    dataSourceDetails: {
      statusCode: 200,
      data: {
        dataSource: configData.dataSources[2],
        connection: configData.connections[2],
        warningDetails: 'Response time degradation detected - Avg response: 1.2s (up from 0.8s)'
      },
      error: null
    }
  }
};