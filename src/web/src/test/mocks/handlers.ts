import { rest } from 'msw'; // ^1.2.1
import { 
  mockPipelineDefinitions, 
  mockPipelineExecutions, 
  mockTaskExecutions,
  mockQualityRules,
  mockDatasetQualitySummaries,
  mockQualityIssues,
  mockHealingIssues,
  mockHealingPatterns,
  mockHealingActions,
  mockHealingExecutions,
  mockAIModels,
  mockHealingSettings,
  mockAlerts,
  mockDashboardData,
  mockHealingDashboardData,
  generateMockListResponse,
  generateMockDataResponse
} from './data';
import { endpoints } from '../../config/apiConfig';

/**
 * Creates mock handlers for authentication-related API endpoints
 */
const createAuthHandlers = () => [
  // Login handler
  rest.post(endpoints.auth.login, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        token: 'mock-auth-token',
        refreshToken: 'mock-refresh-token',
        expiresIn: 3600,
        user: {
          id: 'user-1',
          username: 'testuser',
          email: 'test@example.com',
          roles: ['admin']
        }
      }))
    );
  }),
  
  // Logout handler
  rest.post(endpoints.auth.logout, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json({
        status: 'SUCCESS',
        message: 'Logged out successfully',
        metadata: {
          timestamp: new Date().toISOString(),
          requestId: 'mock-request-id',
          processingTime: 25
        }
      })
    );
  }),
  
  // Refresh token handler
  rest.post(endpoints.auth.refreshToken, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        token: 'mock-new-auth-token',
        refreshToken: 'mock-new-refresh-token',
        expiresIn: 3600
      }))
    );
  }),
  
  // User profile handler
  rest.get(endpoints.auth.profile, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        id: 'user-1',
        username: 'testuser',
        email: 'test@example.com',
        firstName: 'Test',
        lastName: 'User',
        roles: ['admin'],
        preferences: {
          theme: 'light',
          language: 'en'
        },
        lastLogin: new Date().toISOString()
      }))
    );
  })
];

/**
 * Creates mock handlers for pipeline management API endpoints
 */
const createPipelineHandlers = () => [
  // Get pipeline list
  rest.get(endpoints.pipeline.list, (req, res, ctx) => {
    // Extract query parameters for pagination
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockPipelineDefinitions,
        page,
        pageSize,
        mockPipelineDefinitions.length
      ))
    );
  }),
  
  // Get pipeline details by ID
  rest.get(endpoints.pipeline.details(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const pipeline = mockPipelineDefinitions.find(p => p.pipelineId === id);
    
    if (!pipeline) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Pipeline not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 15
          },
          error: {
            statusCode: 404,
            message: 'Pipeline not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(pipeline))
    );
  }),
  
  // Create a new pipeline
  rest.post(endpoints.pipeline.create, async (req, res, ctx) => {
    // Create a new pipeline with mock ID
    const newPipeline = {
      ...await req.json(),
      pipelineId: `new-pipeline-${Date.now()}`,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      lastExecutionStatus: undefined,
      lastExecutionTime: undefined
    };
    
    return res(
      ctx.status(201),
      ctx.json(generateMockDataResponse(newPipeline))
    );
  }),
  
  // Update a pipeline
  rest.put(endpoints.pipeline.update(':id'), async (req, res, ctx) => {
    const { id } = req.params;
    const pipeline = mockPipelineDefinitions.find(p => p.pipelineId === id);
    
    if (!pipeline) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Pipeline not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 18
          },
          error: {
            statusCode: 404,
            message: 'Pipeline not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    const updatedPipeline = {
      ...pipeline,
      ...await req.json(),
      updatedAt: new Date().toISOString()
    };
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(updatedPipeline))
    );
  }),
  
  // Delete a pipeline
  rest.delete(endpoints.pipeline.delete(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const pipeline = mockPipelineDefinitions.find(p => p.pipelineId === id);
    
    if (!pipeline) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Pipeline not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 12
          },
          error: {
            statusCode: 404,
            message: 'Pipeline not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json({
        status: 'SUCCESS',
        message: 'Pipeline deleted successfully',
        metadata: {
          timestamp: new Date().toISOString(),
          requestId: 'mock-request-id',
          processingTime: 35
        }
      })
    );
  }),
  
  // Execute a pipeline
  rest.post(endpoints.pipeline.execute(':id'), (req, res, ctx) => {
    const { id } = req.params;
    
    return res(
      ctx.status(202),
      ctx.json({
        status: 'SUCCESS',
        message: 'Pipeline execution started',
        metadata: {
          timestamp: new Date().toISOString(),
          requestId: 'mock-request-id',
          processingTime: 40
        },
        data: {
          executionId: `exec-${Date.now()}`,
          pipelineId: id,
          status: 'RUNNING',
          startTime: new Date().toISOString()
        }
      })
    );
  }),
  
  // Get pipeline executions
  rest.get(endpoints.pipeline.history(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    // Filter executions by pipeline ID
    const executions = mockPipelineExecutions.filter(e => e.pipelineId === id);
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        executions,
        page,
        pageSize,
        executions.length
      ))
    );
  }),
  
  // Get task executions for a specific pipeline execution
  rest.get('/api/v1/pipelines/:pipelineId/executions/:executionId/tasks', (req, res, ctx) => {
    const { executionId } = req.params;
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    // Filter task executions by execution ID
    const tasks = mockTaskExecutions.filter(t => t.executionId === executionId);
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        tasks,
        page,
        pageSize,
        tasks.length
      ))
    );
  })
];

/**
 * Creates mock handlers for data quality API endpoints
 */
const createQualityHandlers = () => [
  // Get quality rules
  rest.get(endpoints.quality.rules, (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockQualityRules,
        page,
        pageSize,
        mockQualityRules.length
      ))
    );
  }),
  
  // Get quality rule details
  rest.get(endpoints.quality.ruleDetails(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const rule = mockQualityRules.find(r => r.ruleId === id);
    
    if (!rule) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Quality rule not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 10
          },
          error: {
            statusCode: 404,
            message: 'Quality rule not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(rule))
    );
  }),
  
  // Get dataset quality summaries
  rest.get('/api/v1/quality/datasets', (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockDatasetQualitySummaries,
        page,
        pageSize,
        mockDatasetQualitySummaries.length
      ))
    );
  }),
  
  // Get quality issues
  rest.get(endpoints.quality.issues, (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockQualityIssues,
        page,
        pageSize,
        mockQualityIssues.length
      ))
    );
  }),
  
  // Get quality issue details
  rest.get(endpoints.quality.issueDetails(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const issue = mockQualityIssues.find(i => i.issueId === id);
    
    if (!issue) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Quality issue not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 11
          },
          error: {
            statusCode: 404,
            message: 'Quality issue not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(issue))
    );
  }),
  
  // Get validation results
  rest.get(endpoints.quality.validationResults(':id'), (req, res, ctx) => {
    const { id } = req.params;
    
    // Create mock validation result
    const validationResult = {
      validationId: id,
      ruleId: mockQualityRules[0].ruleId,
      rule: mockQualityRules[0],
      dataset: 'sales_metrics',
      table: 'daily_sales',
      executionTime: 1250, // ms
      recordsProcessed: 5000,
      failedRecords: 15,
      sampleFailures: [
        { row_id: 'row_123', value: null },
        { row_id: 'row_456', value: null }
      ],
      healingAttempted: true,
      healingSuccessful: true,
      healingDetails: {
        healingId: '1',
        actionType: 'DATA_CORRECTION',
        confidence: 92
      }
    };
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(validationResult))
    );
  }),
  
  // Create/update quality rule
  rest.post(endpoints.quality.rules, async (req, res, ctx) => {
    const newRule = {
      ...await req.json(),
      ruleId: `rule-${Date.now()}`,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    
    return res(
      ctx.status(201),
      ctx.json(generateMockDataResponse(newRule))
    );
  }),
  
  // Get quality metrics
  rest.get(endpoints.quality.metrics, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        totalDatasets: 5,
        totalTables: 15,
        totalRules: 245,
        activeRules: 231,
        validationsLast24h: 125,
        validationsLast7d: 820,
        validationsLast30d: 3250,
        successRateLast24h: 94.5,
        successRateLast7d: 95.2,
        successRateLast30d: 94.8,
        openIssues: 24,
        issuesByDimension: {
          COMPLETENESS: 8,
          ACCURACY: 5,
          CONSISTENCY: 6,
          TIMELINESS: 1,
          VALIDITY: 4
        },
        issuesBySeverity: {
          CRITICAL: 3,
          HIGH: 7,
          MEDIUM: 10,
          LOW: 4
        },
        selfHealingSuccessRate: 87.5
      }))
    );
  })
];

/**
 * Creates mock handlers for self-healing API endpoints
 */
const createHealingHandlers = () => [
  // Get healing issues
  rest.get('/api/v1/healing/issues', (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockHealingIssues,
        page,
        pageSize,
        mockHealingIssues.length
      ))
    );
  }),
  
  // Get healing patterns
  rest.get('/api/v1/healing/patterns', (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockHealingPatterns,
        page,
        pageSize,
        mockHealingPatterns.length
      ))
    );
  }),
  
  // Get healing actions
  rest.get(endpoints.healing.actions, (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockHealingActions,
        page,
        pageSize,
        mockHealingActions.length
      ))
    );
  }),
  
  // Get healing executions
  rest.get('/api/v1/healing/history', (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockHealingExecutions,
        page,
        pageSize,
        mockHealingExecutions.length
      ))
    );
  }),
  
  // Get AI models
  rest.get(endpoints.healing.models, (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockAIModels,
        page,
        pageSize,
        mockAIModels.length
      ))
    );
  }),
  
  // Get AI model details
  rest.get(endpoints.healing.modelDetails(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const model = mockAIModels.find(m => m.modelId === id);
    
    if (!model) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'AI model not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 14
          },
          error: {
            statusCode: 404,
            message: 'AI model not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(model))
    );
  }),
  
  // Get healing settings
  rest.get(endpoints.healing.settings, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(mockHealingSettings))
    );
  }),
  
  // Update healing settings
  rest.put(endpoints.healing.settings, async (req, res, ctx) => {
    const updatedSettings = {
      ...mockHealingSettings,
      ...await req.json(),
      updatedAt: new Date().toISOString(),
      updatedBy: 'test@example.com'
    };
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(updatedSettings))
    );
  }),
  
  // Get healing dashboard data
  rest.get('/api/v1/healing/dashboard', (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(mockHealingDashboardData))
    );
  })
];

/**
 * Creates mock handlers for monitoring and alerting API endpoints
 */
const createMonitoringHandlers = () => [
  // Get alerts
  rest.get(endpoints.monitoring.alerts, (req, res, ctx) => {
    const page = Number(req.url.searchParams.get('page')) || 1;
    const pageSize = Number(req.url.searchParams.get('pageSize')) || 10;
    
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockAlerts,
        page,
        pageSize,
        mockAlerts.length
      ))
    );
  }),
  
  // Get alert details
  rest.get(endpoints.monitoring.alertDetails(':id'), (req, res, ctx) => {
    const { id } = req.params;
    const alert = mockAlerts.find(a => a.alertId === id);
    
    if (!alert) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Alert not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 9
          },
          error: {
            statusCode: 404,
            message: 'Alert not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(alert))
    );
  }),
  
  // Acknowledge alert
  rest.post(`/api/v1/monitoring/alerts/:id/acknowledge`, async (req, res, ctx) => {
    const { id } = req.params;
    const alert = mockAlerts.find(a => a.alertId === id);
    
    if (!alert) {
      return res(
        ctx.status(404),
        ctx.json({
          status: 'ERROR',
          message: 'Alert not found',
          metadata: {
            timestamp: new Date().toISOString(),
            requestId: 'mock-request-id',
            processingTime: 8
          },
          error: {
            statusCode: 404,
            message: 'Alert not found',
            errorCode: 'RESOURCE_NOT_FOUND'
          }
        })
      );
    }
    
    const acknowledgedAlert = {
      ...alert,
      acknowledged: true,
      acknowledgedBy: 'test@example.com',
      acknowledgedAt: new Date().toISOString()
    };
    
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(acknowledgedAlert))
    );
  }),
  
  // Get alert statistics
  rest.get('/api/v1/monitoring/alerts/statistics', (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        total: 5,
        acknowledged: 1,
        unacknowledged: 4,
        byType: {
          PIPELINE_FAILURE: 1,
          DATA_QUALITY: 1,
          SYSTEM_PERFORMANCE: 1,
          RESOURCE_UTILIZATION: 1,
          PERFORMANCE_DEGRADATION: 1
        },
        bySeverity: {
          CRITICAL: 0,
          HIGH: 1,
          MEDIUM: 2,
          LOW: 2
        }
      }))
    );
  }),
  
  // Get alert trends
  rest.get('/api/v1/monitoring/alerts/trends', (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse({
        timeLabels: [
          '00:00', '06:00', '12:00', '18:00', 'Now'
        ],
        values: [
          4, 8, 6, 3, 5
        ]
      }))
    );
  })
];

/**
 * Creates mock handlers for dashboard API endpoints
 */
const createDashboardHandlers = () => [
  // Get dashboard data
  rest.get(endpoints.monitoring.dashboard, (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(mockDashboardData))
    );
  }),
  
  // Get recent executions
  rest.get('/api/v1/dashboard/executions', (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockListResponse(
        mockDashboardData.recentExecutions,
        1,
        10,
        mockDashboardData.recentExecutions.length
      ))
    );
  }),
  
  // Get system status
  rest.get('/api/v1/dashboard/system-status', (req, res, ctx) => {
    return res(
      ctx.status(200),
      ctx.json(generateMockDataResponse(mockDashboardData.systemStatus))
    );
  })
];

/**
 * Array of all MSW request handlers for intercepting API requests during tests
 */
export const handlers = [
  ...createAuthHandlers(),
  ...createPipelineHandlers(),
  ...createQualityHandlers(),
  ...createHealingHandlers(),
  ...createMonitoringHandlers(),
  ...createDashboardHandlers()
];