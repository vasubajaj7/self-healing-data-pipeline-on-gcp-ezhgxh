import http from 'k6/http'; // k6/http@latest
import { check, group } from 'k6'; // k6@latest
import { sleep } from 'k6'; // k6@latest
import { Counter, Trend, Rate } from 'k6/metrics'; // k6/metrics@latest
import { getAuthToken, makeAuthenticatedRequest, generateTestData, setupTestEnvironment, teardownTestEnvironment, checkResponse, randomSleep, logMetrics, BASE_URL, DEFAULT_HEADERS, CUSTOM_METRICS } from './common';

// Test configuration
export const options = {
  scenarios: {
    constant_load: {
      executor: 'constant-vus',
      vus: 20,
      duration: '1m',
      gracefulStop: '30s'
    },
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '1m', target: 20 },
        { duration: '30s', target: 40 },
        { duration: '1m', target: 20 },
        { duration: '30s', target: 0 }
      ],
      gracefulRampDown: '30s'
    },
    stress_test: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { duration: '1m', target: 20 },
        { duration: '3m', target: 20 },
        { duration: '1m', target: 40 },
        { duration: '3m', target: 40 },
        { duration: '1m', target: 0 }
      ]
    }
  },
  thresholds: {
    'http_req_duration': ['p(95)<500', 'p(99)<1000'],
    'http_req_duration{endpoint:quality}': ['p(95)<600'],
    'http_req_duration{endpoint:healing}': ['p(95)<800'],
    'http_req_duration{endpoint:monitoring}': ['p(95)<400'],
    'http_req_duration{endpoint:optimization}': ['p(95)<700'],
    'http_reqs': ['rate>100'],
    'checks': ['rate>0.95']
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)']
};

// Shared test data storage
let testData;

// Authentication token
let authToken;

// Custom metrics for API categories
const apiMetrics = {
  quality: {
    requests: new Counter('quality_requests'),
    failures: new Counter('quality_failures'),
    duration: new Trend('quality_duration')
  },
  healing: {
    requests: new Counter('healing_requests'),
    failures: new Counter('healing_failures'),
    duration: new Trend('healing_duration')
  },
  monitoring: {
    requests: new Counter('monitoring_requests'),
    failures: new Counter('monitoring_failures'),
    duration: new Trend('monitoring_duration')
  },
  optimization: {
    requests: new Counter('optimization_requests'),
    failures: new Counter('optimization_failures'),
    duration: new Trend('optimization_duration')
  }
};

/**
 * Setup function that runs once before the load test begins
 * 
 * @returns {Object} Test data and configuration for the load test
 */
export function setup() {
  console.log('Setting up load test environment');
  
  // Initialize test environment
  const env = setupTestEnvironment();
  
  // Authenticate with the API
  const credentials = {
    username: __ENV.TEST_USERNAME || 'testuser',
    password: __ENV.TEST_PASSWORD || 'TestPassword123!'
  };
  
  authToken = getAuthToken(credentials);
  
  if (!authToken) {
    console.error('Failed to authenticate. Tests may fail.');
  }
  
  // Generate test data for different endpoints
  const data = {
    quality: generateQualityTestData(),
    healing: generateHealingTestData(),
    monitoring: generateMonitoringTestData(),
    optimization: generateOptimizationTestData(),
    environment: env
  };
  
  console.log('Test setup completed successfully');
  return data;
}

/**
 * Teardown function that runs once after the load test completes
 * 
 * @param {Object} data - Test data from setup
 */
export function teardown(data) {
  console.log('Starting test environment teardown');
  
  // Clean up test environment
  teardownTestEnvironment(data.environment);
  
  // Log final metrics
  logMetrics({
    quality: {
      requests: apiMetrics.quality.requests.name,
      failures: apiMetrics.quality.failures.name,
      avgDuration: apiMetrics.quality.duration.name
    },
    healing: {
      requests: apiMetrics.healing.requests.name,
      failures: apiMetrics.healing.failures.name,
      avgDuration: apiMetrics.healing.duration.name
    },
    monitoring: {
      requests: apiMetrics.monitoring.requests.name,
      failures: apiMetrics.monitoring.failures.name,
      avgDuration: apiMetrics.monitoring.duration.name
    },
    optimization: {
      requests: apiMetrics.optimization.requests.name,
      failures: apiMetrics.optimization.failures.name,
      avgDuration: apiMetrics.optimization.duration.name
    }
  }, 'Final API Test Metrics');
  
  console.log('Test environment teardown completed');
}

/**
 * Tests data quality API endpoints under load
 * 
 * @param {Object} testData - Test data for quality endpoints
 * @returns {boolean} True if all tests passed
 */
function testQualityEndpoints(testData) {
  let success = true;
  const startTime = new Date().getTime();
  
  group('Quality API Endpoints', () => {
    // GET quality rules with pagination
    const rulesResponse = makeAuthenticatedRequest(
      'GET',
      '/quality/rules',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(rulesResponse, 200, 'Get quality rules');
    
    // If rules exist, test getting a specific rule
    if (rulesResponse.json('data') && rulesResponse.json('data').length > 0) {
      const ruleId = rulesResponse.json('data')[0].id;
      
      const ruleDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/quality/rules/${ruleId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(ruleDetailResponse, 200, 'Get quality rule details');
    }
    
    // Create a new quality rule
    const newRuleResponse = makeAuthenticatedRequest(
      'POST',
      '/quality/rules',
      testData.rules[0],
      null,
      authToken
    );
    
    success = success && checkResponse(newRuleResponse, 201, 'Create quality rule');
    
    // If rule was created, update it
    if (newRuleResponse.status === 201) {
      const createdRuleId = newRuleResponse.json('id');
      const updatedRule = { ...testData.rules[0], name: `${testData.rules[0].name}_updated` };
      
      const updateResponse = makeAuthenticatedRequest(
        'PUT',
        `/quality/rules/${createdRuleId}`,
        updatedRule,
        null,
        authToken
      );
      
      success = success && checkResponse(updateResponse, 200, 'Update quality rule');
    }
    
    // Get validation history
    const validationsResponse = makeAuthenticatedRequest(
      'GET',
      '/quality/validations',
      null,
      { page: 1, pageSize: 10, status: 'all' },
      authToken
    );
    
    success = success && checkResponse(validationsResponse, 200, 'Get quality validations');
    
    // If validations exist, test getting a specific validation
    if (validationsResponse.json('data') && validationsResponse.json('data').length > 0) {
      const validationId = validationsResponse.json('data')[0].id;
      
      const validationDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/quality/validations/${validationId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(validationDetailResponse, 200, 'Get validation details');
    }
    
    // Trigger a new validation
    const triggerValidationResponse = makeAuthenticatedRequest(
      'POST',
      '/quality/validate',
      {
        datasetId: testData.datasets[0].id,
        rules: testData.rules.map(r => r.id).slice(0, 3)
      },
      null,
      authToken
    );
    
    success = success && checkResponse(triggerValidationResponse, 202, 'Trigger validation');
    
    // Get datasets list
    const datasetsResponse = makeAuthenticatedRequest(
      'GET',
      '/quality/datasets',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(datasetsResponse, 200, 'Get datasets');
    
    // Get dataset metrics
    if (datasetsResponse.json('data') && datasetsResponse.json('data').length > 0) {
      const datasetId = datasetsResponse.json('data')[0].id;
      
      const datasetMetricsResponse = makeAuthenticatedRequest(
        'GET',
        `/quality/datasets/${datasetId}/metrics`,
        null,
        { timeRange: '30d' },
        authToken
      );
      
      success = success && checkResponse(datasetMetricsResponse, 200, 'Get dataset metrics');
    }
  });
  
  // Update metrics
  const duration = new Date().getTime() - startTime;
  apiMetrics.quality.requests.add(1);
  apiMetrics.quality.duration.add(duration);
  if (!success) {
    apiMetrics.quality.failures.add(1);
  }
  
  return success;
}

/**
 * Tests self-healing API endpoints under load
 * 
 * @param {Object} testData - Test data for healing endpoints
 * @returns {boolean} True if all tests passed
 */
function testHealingEndpoints(testData) {
  let success = true;
  const startTime = new Date().getTime();
  
  group('Self-Healing API Endpoints', () => {
    // GET healing issues with pagination
    const issuesResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/issues',
      null,
      { page: 1, pageSize: 10, status: 'all' },
      authToken
    );
    
    success = success && checkResponse(issuesResponse, 200, 'Get healing issues');
    
    // If issues exist, test getting a specific issue
    if (issuesResponse.json('data') && issuesResponse.json('data').length > 0) {
      const issueId = issuesResponse.json('data')[0].id;
      
      const issueDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/issues/${issueId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(issueDetailResponse, 200, 'Get issue details');
      
      // Trigger healing action for this issue
      const triggerHealingResponse = makeAuthenticatedRequest(
        'POST',
        `/healing/issues/${issueId}/fix`,
        { 
          confidence: 95,
          applyFix: true
        },
        null,
        authToken
      );
      
      success = success && checkResponse(triggerHealingResponse, 202, 'Trigger healing');
    }
    
    // Get healing actions
    const actionsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/actions',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(actionsResponse, 200, 'Get healing actions');
    
    // If actions exist, test getting a specific action
    if (actionsResponse.json('data') && actionsResponse.json('data').length > 0) {
      const actionId = actionsResponse.json('data')[0].id;
      
      const actionDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/actions/${actionId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(actionDetailResponse, 200, 'Get action details');
    }
    
    // Create a new healing action
    const newActionResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/actions',
      testData.actions[0],
      null,
      authToken
    );
    
    success = success && checkResponse(newActionResponse, 201, 'Create healing action');
    
    // If action was created, update it
    if (newActionResponse.status === 201) {
      const createdActionId = newActionResponse.json('id');
      const updatedAction = { ...testData.actions[0], name: `${testData.actions[0].name}_updated` };
      
      const updateResponse = makeAuthenticatedRequest(
        'PUT',
        `/healing/actions/${createdActionId}`,
        updatedAction,
        null,
        authToken
      );
      
      success = success && checkResponse(updateResponse, 200, 'Update healing action');
    }
    
    // Get ML models
    const modelsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/models',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(modelsResponse, 200, 'Get ML models');
    
    // If models exist, test getting a specific model and its metrics
    if (modelsResponse.json('data') && modelsResponse.json('data').length > 0) {
      const modelId = modelsResponse.json('data')[0].id;
      
      const modelDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/models/${modelId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(modelDetailResponse, 200, 'Get model details');
      
      const modelMetricsResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/models/${modelId}/metrics`,
        null,
        { timeRange: '30d' },
        authToken
      );
      
      success = success && checkResponse(modelMetricsResponse, 200, 'Get model metrics');
    }
  });
  
  // Update metrics
  const duration = new Date().getTime() - startTime;
  apiMetrics.healing.requests.add(1);
  apiMetrics.healing.duration.add(duration);
  if (!success) {
    apiMetrics.healing.failures.add(1);
  }
  
  return success;
}

/**
 * Tests monitoring and alerting API endpoints under load
 * 
 * @param {Object} testData - Test data for monitoring endpoints
 * @returns {boolean} True if all tests passed
 */
function testMonitoringEndpoints(testData) {
  let success = true;
  const startTime = new Date().getTime();
  
  group('Monitoring API Endpoints', () => {
    // GET alerts with filtering
    const alertsResponse = makeAuthenticatedRequest(
      'GET',
      '/monitoring/alerts',
      null,
      { 
        page: 1, 
        pageSize: 10, 
        severity: 'all', 
        acknowledged: false, 
        timeRange: '24h' 
      },
      authToken
    );
    
    success = success && checkResponse(alertsResponse, 200, 'Get alerts');
    
    // If alerts exist, test getting a specific alert and acknowledging it
    if (alertsResponse.json('data') && alertsResponse.json('data').length > 0) {
      const alertId = alertsResponse.json('data')[0].id;
      
      const alertDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/monitoring/alerts/${alertId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(alertDetailResponse, 200, 'Get alert details');
      
      // Acknowledge the alert
      const acknowledgeResponse = makeAuthenticatedRequest(
        'PUT',
        `/monitoring/alerts/${alertId}`,
        {
          acknowledged: true,
          comment: 'Acknowledged during load testing'
        },
        null,
        authToken
      );
      
      success = success && checkResponse(acknowledgeResponse, 200, 'Acknowledge alert');
    }
    
    // Get metrics with filters
    const metricsResponse = makeAuthenticatedRequest(
      'GET',
      '/monitoring/metrics',
      null,
      { 
        page: 1, 
        pageSize: 10, 
        category: 'pipeline', 
        timeRange: '24h' 
      },
      authToken
    );
    
    success = success && checkResponse(metricsResponse, 200, 'Get metrics');
    
    // If metrics exist, test getting a specific metric
    if (metricsResponse.json('data') && metricsResponse.json('data').length > 0) {
      const metricId = metricsResponse.json('data')[0].id;
      
      const metricDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/monitoring/metrics/${metricId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(metricDetailResponse, 200, 'Get metric details');
    }
    
    // Get dashboards
    const dashboardsResponse = makeAuthenticatedRequest(
      'GET',
      '/monitoring/dashboards',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(dashboardsResponse, 200, 'Get dashboards');
    
    // If dashboards exist, test getting a specific dashboard
    if (dashboardsResponse.json('data') && dashboardsResponse.json('data').length > 0) {
      const dashboardId = dashboardsResponse.json('data')[0].id;
      
      const dashboardDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/monitoring/dashboards/${dashboardId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(dashboardDetailResponse, 200, 'Get dashboard details');
    }
    
    // Get notification settings
    const notificationSettingsResponse = makeAuthenticatedRequest(
      'GET',
      '/monitoring/notifications/settings',
      null,
      null,
      authToken
    );
    
    success = success && checkResponse(notificationSettingsResponse, 200, 'Get notification settings');
    
    // Update notification settings
    const updateSettingsResponse = makeAuthenticatedRequest(
      'PUT',
      '/monitoring/notifications/settings',
      {
        email: {
          enabled: true,
          recipients: ['test@example.com'],
          minSeverity: 'medium'
        },
        teams: {
          enabled: true,
          webhookUrl: 'https://example.com/webhook',
          minSeverity: 'high'
        }
      },
      null,
      authToken
    );
    
    success = success && checkResponse(updateSettingsResponse, 200, 'Update notification settings');
    
    // Test notification
    const testNotificationResponse = makeAuthenticatedRequest(
      'POST',
      '/monitoring/notifications/test',
      {
        channel: 'email',
        recipient: 'test@example.com'
      },
      null,
      authToken
    );
    
    success = success && checkResponse(testNotificationResponse, 202, 'Test notification');
  });
  
  // Update metrics
  const duration = new Date().getTime() - startTime;
  apiMetrics.monitoring.requests.add(1);
  apiMetrics.monitoring.duration.add(duration);
  if (!success) {
    apiMetrics.monitoring.failures.add(1);
  }
  
  return success;
}

/**
 * Tests performance optimization API endpoints under load
 * 
 * @param {Object} testData - Test data for optimization endpoints
 * @returns {boolean} True if all tests passed
 */
function testOptimizationEndpoints(testData) {
  let success = true;
  const startTime = new Date().getTime();
  
  group('Optimization API Endpoints', () => {
    // GET optimization recommendations with filters
    const recommendationsResponse = makeAuthenticatedRequest(
      'GET',
      '/optimization/recommendations',
      null,
      { 
        page: 1, 
        pageSize: 10, 
        category: 'all', 
        status: 'pending' 
      },
      authToken
    );
    
    success = success && checkResponse(recommendationsResponse, 200, 'Get recommendations');
    
    // If recommendations exist, test getting a specific recommendation and applying it
    if (recommendationsResponse.json('data') && recommendationsResponse.json('data').length > 0) {
      const recId = recommendationsResponse.json('data')[0].id;
      
      const recDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/optimization/recommendations/${recId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(recDetailResponse, 200, 'Get recommendation details');
      
      // Apply the recommendation
      const applyResponse = makeAuthenticatedRequest(
        'POST',
        `/optimization/recommendations/${recId}/apply`,
        {
          applyNow: true,
          schedule: null
        },
        null,
        authToken
      );
      
      success = success && checkResponse(applyResponse, 202, 'Apply recommendation');
    }
    
    // Get queries with filters
    const queriesResponse = makeAuthenticatedRequest(
      'GET',
      '/optimization/queries',
      null,
      { 
        page: 1, 
        pageSize: 10, 
        status: 'all', 
        timeRange: '7d' 
      },
      authToken
    );
    
    success = success && checkResponse(queriesResponse, 200, 'Get queries');
    
    // If queries exist, test getting a specific query and analyzing it
    if (queriesResponse.json('data') && queriesResponse.json('data').length > 0) {
      const queryId = queriesResponse.json('data')[0].id;
      
      const queryDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/optimization/queries/${queryId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(queryDetailResponse, 200, 'Get query details');
      
      // Analyze the query
      const analyzeResponse = makeAuthenticatedRequest(
        'POST',
        `/optimization/queries/${queryId}/analyze`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(analyzeResponse, 202, 'Analyze query');
    }
    
    // Get schemas
    const schemasResponse = makeAuthenticatedRequest(
      'GET',
      '/optimization/schemas',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(schemasResponse, 200, 'Get schemas');
    
    // If schemas exist, test getting a specific schema and optimizing it
    if (schemasResponse.json('data') && schemasResponse.json('data').length > 0) {
      const schemaId = schemasResponse.json('data')[0].id;
      
      const schemaDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/optimization/schemas/${schemaId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(schemaDetailResponse, 200, 'Get schema details');
      
      // Optimize the schema
      const optimizeResponse = makeAuthenticatedRequest(
        'POST',
        `/optimization/schemas/${schemaId}/optimize`,
        {
          optimizationType: 'partitioning',
          options: {
            field: 'timestamp',
            type: 'time',
            granularity: 'day'
          }
        },
        null,
        authToken
      );
      
      success = success && checkResponse(optimizeResponse, 202, 'Optimize schema');
    }
    
    // Get resources
    const resourcesResponse = makeAuthenticatedRequest(
      'GET',
      '/optimization/resources',
      null,
      { page: 1, pageSize: 10 },
      authToken
    );
    
    success = success && checkResponse(resourcesResponse, 200, 'Get resources');
    
    // If resources exist, test getting a specific resource
    if (resourcesResponse.json('data') && resourcesResponse.json('data').length > 0) {
      const resourceId = resourcesResponse.json('data')[0].id;
      
      const resourceDetailResponse = makeAuthenticatedRequest(
        'GET',
        `/optimization/resources/${resourceId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(resourceDetailResponse, 200, 'Get resource details');
    }
  });
  
  // Update metrics
  const duration = new Date().getTime() - startTime;
  apiMetrics.optimization.requests.add(1);
  apiMetrics.optimization.duration.add(duration);
  if (!success) {
    apiMetrics.optimization.failures.add(1);
  }
  
  return success;
}

/**
 * Generates test data for quality API tests
 * 
 * @returns {Object} Quality test data
 */
function generateQualityTestData() {
  // Generate validation rule definitions
  const rules = [
    {
      id: `rule_${Date.now()}_1`,
      name: 'Customer ID Not Null',
      type: 'nullCheck',
      dataset: 'customers',
      table: 'customer_data',
      column: 'customer_id',
      description: 'Customer ID must not be null',
      severity: 'critical'
    },
    {
      id: `rule_${Date.now()}_2`,
      name: 'Valid Order Total',
      type: 'rangeCheck',
      dataset: 'sales',
      table: 'orders',
      column: 'order_total',
      min: 0,
      max: 100000,
      description: 'Order total must be between 0 and 100,000',
      severity: 'high'
    },
    {
      id: `rule_${Date.now()}_3`,
      name: 'Email Format Check',
      type: 'patternCheck',
      dataset: 'customers',
      table: 'customer_data',
      column: 'email',
      pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
      description: 'Email must be in valid format',
      severity: 'medium'
    },
    {
      id: `rule_${Date.now()}_4`,
      name: 'Product ID Reference',
      type: 'referenceCheck',
      dataset: 'sales',
      table: 'order_items',
      column: 'product_id',
      referenceDataset: 'products',
      referenceTable: 'product_catalog',
      referenceColumn: 'product_id',
      description: 'Product ID must exist in product catalog',
      severity: 'high'
    }
  ];
  
  // Create test datasets
  const datasets = [
    {
      id: `dataset_${Date.now()}_1`,
      name: 'customers',
      description: 'Customer master data',
      tables: ['customer_data', 'customer_addresses'],
      owner: 'data_team'
    },
    {
      id: `dataset_${Date.now()}_2`,
      name: 'sales',
      description: 'Sales transaction data',
      tables: ['orders', 'order_items'],
      owner: 'sales_team'
    },
    {
      id: `dataset_${Date.now()}_3`,
      name: 'products',
      description: 'Product catalog data',
      tables: ['product_catalog', 'product_inventory'],
      owner: 'product_team'
    }
  ];
  
  // Generate validation parameters
  const validationParams = {
    datasetId: datasets[0].id,
    rules: rules.map(r => r.id).slice(0, 2),
    validateNow: true
  };
  
  return {
    rules,
    datasets,
    validationParams
  };
}

/**
 * Generates test data for self-healing API tests
 * 
 * @returns {Object} Self-healing test data
 */
function generateHealingTestData() {
  // Generate issues
  const issues = [
    {
      id: `issue_${Date.now()}_1`,
      type: 'dataQuality',
      title: 'Null Values in Required Field',
      description: 'Customer IDs contain null values in recent import',
      source: 'quality_validation',
      sourceId: `validation_${Date.now()}_1`,
      severity: 'high',
      status: 'detected',
      detectedAt: new Date().toISOString(),
      affectedResource: 'customers.customer_data',
      metadata: {
        column: 'customer_id',
        affectedRows: 42,
        totalRows: 1250
      }
    },
    {
      id: `issue_${Date.now()}_2`,
      type: 'pipeline',
      title: 'Pipeline Execution Timeout',
      description: 'Sales data pipeline consistently timing out',
      source: 'pipeline_execution',
      sourceId: `execution_${Date.now()}_1`,
      severity: 'critical',
      status: 'detected',
      detectedAt: new Date().toISOString(),
      affectedResource: 'sales_pipeline',
      metadata: {
        executionId: `exec_${Date.now()}`,
        duration: 3600,
        timeout: 1800,
        failedTask: 'data_transform'
      }
    },
    {
      id: `issue_${Date.now()}_3`,
      type: 'performance',
      title: 'Slow Query Performance',
      description: 'Customer analytics query showing degraded performance',
      source: 'query_monitoring',
      sourceId: `query_${Date.now()}_1`,
      severity: 'medium',
      status: 'detected',
      detectedAt: new Date().toISOString(),
      affectedResource: 'analytics.customer_query',
      metadata: {
        queryId: `q_${Date.now()}`,
        avgDuration: 120,
        currentDuration: 450,
        bytesProcessed: 2500000000
      }
    }
  ];
  
  // Generate healing actions
  const actions = [
    {
      id: `action_${Date.now()}_1`,
      name: 'Null Value Imputation',
      description: 'Impute null values based on statistically derived patterns',
      type: 'dataCorrection',
      issueTypes: ['dataQuality'],
      parameters: {
        strategy: 'statistical',
        methods: ['mean', 'median', 'mode', 'constant'],
        confidence: 90
      },
      requiresApproval: true,
      enabled: true
    },
    {
      id: `action_${Date.now()}_2`,
      name: 'Pipeline Parameter Optimization',
      description: 'Adjust pipeline parameters to optimize performance',
      type: 'pipelineOptimization',
      issueTypes: ['pipeline', 'performance'],
      parameters: {
        targetParameters: ['batchSize', 'timeout', 'workerCount', 'memoryLimit'],
        optimizationStrategy: 'reinforcementLearning',
        safetyFactor: 1.5
      },
      requiresApproval: true,
      enabled: true
    },
    {
      id: `action_${Date.now()}_3`,
      name: 'Query Optimization',
      description: 'Rewrite and optimize poorly performing queries',
      type: 'queryOptimization',
      issueTypes: ['performance'],
      parameters: {
        strategies: ['partitioning', 'clustering', 'materialization', 'rewrite'],
        maxCostIncrease: 0.1,
        minPerformanceImprovement: 0.3
      },
      requiresApproval: false,
      enabled: true
    }
  ];
  
  // Generate model data
  const models = [
    {
      id: `model_${Date.now()}_1`,
      name: 'NullValuePredictor',
      version: '1.0.2',
      type: 'imputation',
      description: 'Predicts missing values based on other columns and historical patterns',
      createdAt: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(),
      updatedAt: new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString(),
      metrics: {
        accuracy: 0.92,
        precision: 0.94,
        recall: 0.89,
        f1Score: 0.91,
        inferenceTime: 45 // ms
      },
      status: 'active'
    },
    {
      id: `model_${Date.now()}_2`,
      name: 'PipelineOptimizer',
      version: '2.1.0',
      type: 'optimization',
      description: 'Optimizes pipeline parameters based on historical performance',
      createdAt: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString(),
      updatedAt: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      metrics: {
        meanAbsoluteError: 0.08,
        rootMeanSquaredError: 0.12,
        r2Score: 0.87,
        inferenceTime: 120 // ms
      },
      status: 'active'
    },
    {
      id: `model_${Date.now()}_3`,
      name: 'AnomalyDetector',
      version: '1.3.5',
      type: 'detection',
      description: 'Detects anomalies in pipeline execution metrics',
      createdAt: new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString(),
      updatedAt: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(),
      metrics: {
        precision: 0.92,
        recall: 0.85,
        f1Score: 0.88,
        falsePositiveRate: 0.05,
        inferenceTime: 85 // ms
      },
      status: 'active'
    }
  ];
  
  return {
    issues,
    actions,
    models
  };
}

/**
 * Generates test data for monitoring API tests
 * 
 * @returns {Object} Monitoring test data
 */
function generateMonitoringTestData() {
  // Generate alerts
  const alerts = [
    {
      id: `alert_${Date.now()}_1`,
      title: 'Pipeline Execution Failed',
      description: 'Customer data pipeline failed 3 times in succession',
      type: 'pipeline',
      severity: 'critical',
      timestamp: new Date().toISOString(),
      source: 'pipeline_monitor',
      sourceId: 'customer_pipeline',
      acknowledged: false,
      metadata: {
        pipelineId: 'customer_daily_load',
        failureCount: 3,
        lastErrorMessage: 'Connection timeout after 30s'
      }
    },
    {
      id: `alert_${Date.now()}_2`,
      title: 'Data Quality Score Decreased',
      description: 'Products dataset quality score dropped below threshold',
      type: 'quality',
      severity: 'high',
      timestamp: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
      source: 'quality_monitor',
      sourceId: 'products_dataset',
      acknowledged: false,
      metadata: {
        datasetId: 'products',
        previousScore: 95,
        currentScore: 82,
        threshold: 90
      }
    },
    {
      id: `alert_${Date.now()}_3`,
      title: 'Unusual Query Pattern Detected',
      description: 'Significant increase in large analytical queries',
      type: 'performance',
      severity: 'medium',
      timestamp: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      source: 'query_monitor',
      sourceId: 'query_analytics',
      acknowledged: true,
      metadata: {
        queryType: 'analytical',
        normalRate: 5,
        currentRate: 25,
        bytesProcessed: 5000000000
      }
    }
  ];
  
  // Generate metrics
  const metrics = [
    {
      id: `metric_${Date.now()}_1`,
      name: 'pipeline_success_rate',
      category: 'pipeline',
      description: 'Percentage of successful pipeline executions',
      currentValue: 94.5,
      previousValue: 98.2,
      changePercentage: -3.77,
      timestamp: new Date().toISOString(),
      metadata: {
        totalExecutions: 200,
        successfulExecutions: 189,
        timeRange: '24h'
      }
    },
    {
      id: `metric_${Date.now()}_2`,
      name: 'data_quality_score',
      category: 'quality',
      description: 'Average data quality score across all datasets',
      currentValue: 92.3,
      previousValue: 91.8,
      changePercentage: 0.54,
      timestamp: new Date().toISOString(),
      metadata: {
        datasets: 12,
        highestScore: 99.8,
        lowestScore: 85.2,
        timeRange: '24h'
      }
    },
    {
      id: `metric_${Date.now()}_3`,
      name: 'query_performance',
      category: 'performance',
      description: 'Average query execution time in seconds',
      currentValue: 3.2,
      previousValue: 2.8,
      changePercentage: 14.29,
      timestamp: new Date().toISOString(),
      metadata: {
        totalQueries: 5280,
        p95Duration: 8.5,
        p99Duration: 15.2,
        timeRange: '24h'
      }
    }
  ];
  
  // Generate dashboards
  const dashboards = [
    {
      id: `dashboard_${Date.now()}_1`,
      name: 'Pipeline Health Overview',
      description: 'Overview of all data pipeline health metrics',
      created: new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString(),
      updated: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
      owner: 'system_admin',
      panels: [
        {
          id: 'panel_1',
          title: 'Pipeline Success Rate',
          type: 'line',
          metric: 'pipeline_success_rate',
          timeRange: '30d'
        },
        {
          id: 'panel_2',
          title: 'Failed Pipelines',
          type: 'table',
          metric: 'pipeline_failures',
          timeRange: '7d'
        }
      ]
    },
    {
      id: `dashboard_${Date.now()}_2`,
      name: 'Data Quality Metrics',
      description: 'Data quality metrics across all datasets',
      created: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString(),
      updated: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
      owner: 'data_quality_team',
      panels: [
        {
          id: 'panel_1',
          title: 'Overall Quality Score',
          type: 'gauge',
          metric: 'data_quality_score',
          timeRange: 'current'
        },
        {
          id: 'panel_2',
          title: 'Quality Score Trend',
          type: 'line',
          metric: 'data_quality_score',
          timeRange: '90d'
        }
      ]
    }
  ];
  
  // Generate notification settings
  const notificationSettings = {
    email: {
      enabled: true,
      recipients: ['alerts@example.com', 'admin@example.com'],
      minSeverity: 'high'
    },
    teams: {
      enabled: true,
      webhookUrl: 'https://example.com/webhook/teams',
      minSeverity: 'medium'
    },
    sms: {
      enabled: false,
      recipients: [],
      minSeverity: 'critical'
    }
  };
  
  return {
    alerts,
    metrics,
    dashboards,
    notificationSettings
  };
}

/**
 * Generates test data for optimization API tests
 * 
 * @returns {Object} Optimization test data
 */
function generateOptimizationTestData() {
  // Generate recommendations
  const recommendations = [
    {
      id: `rec_${Date.now()}_1`,
      title: 'Partition Large Customer Table',
      description: 'Implement time-based partitioning on customer_events table',
      category: 'schema',
      impact: 'high',
      estimatedImprovement: {
        performanceGain: 45, // percentage
        costReduction: 30,    // percentage
      },
      status: 'pending',
      created: new Date().toISOString(),
      details: {
        table: 'customer_events',
        currentSize: '1.2TB',
        queryFrequency: 'high',
        partitionField: 'event_timestamp',
        partitionType: 'time',
        partitionGranularity: 'day'
      }
    },
    {
      id: `rec_${Date.now()}_2`,
      title: 'Materialize Common Joins',
      description: 'Create materialized view for frequently joined tables',
      category: 'query',
      impact: 'medium',
      estimatedImprovement: {
        performanceGain: 65, // percentage
        costReduction: 50,    // percentage
      },
      status: 'pending',
      created: new Date().toISOString(),
      details: {
        baseTables: ['orders', 'customers', 'products'],
        queryPattern: 'analytics_daily',
        refreshFrequency: '6h',
        estimatedSize: '150GB'
      }
    },
    {
      id: `rec_${Date.now()}_3`,
      title: 'Increase BigQuery Slot Reservation',
      description: 'Increase reserved slots to handle peak workloads',
      category: 'resource',
      impact: 'medium',
      estimatedImprovement: {
        performanceGain: 30, // percentage
        costReduction: -10,   // negative = cost increase
      },
      status: 'pending',
      created: new Date().toISOString(),
      details: {
        currentSlots: 100,
        recommendedSlots: 200,
        peakConcurrency: 180,
        estimatedCostChange: '+$500/month'
      }
    }
  ];
  
  // Generate query examples
  const queries = [
    {
      id: `query_${Date.now()}_1`,
      name: 'Daily Customer Analytics',
      sql: 'SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count, SUM(o.total) AS total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY c.customer_id, c.name ORDER BY total_spent DESC LIMIT 1000',
      lastExecuted: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      avgDuration: 45.2, // seconds
      avgBytesProcessed: 2500000000, // bytes
      executionCount: 45,
      status: 'active'
    },
    {
      id: `query_${Date.now()}_2`,
      name: 'Product Performance Report',
      sql: 'SELECT p.product_id, p.name, p.category, SUM(oi.quantity) AS units_sold, SUM(oi.quantity * oi.unit_price) AS revenue FROM products p JOIN order_items oi ON p.product_id = oi.product_id JOIN orders o ON oi.order_id = o.order_id WHERE o.order_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE() GROUP BY p.product_id, p.name, p.category ORDER BY revenue DESC',
      lastExecuted: new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString(),
      avgDuration: 120.5, // seconds
      avgBytesProcessed: 8500000000, // bytes
      executionCount: 12,
      status: 'active'
    },
    {
      id: `query_${Date.now()}_3`,
      name: 'Slow Moving Inventory',
      sql: 'SELECT p.product_id, p.name, p.category, i.quantity_on_hand, COALESCE(SUM(oi.quantity), 0) AS units_sold_90days FROM products p JOIN inventory i ON p.product_id = i.product_id LEFT JOIN order_items oi ON p.product_id = oi.product_id LEFT JOIN orders o ON oi.order_id = o.order_id AND o.order_date > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) GROUP BY p.product_id, p.name, p.category, i.quantity_on_hand HAVING units_sold_90days < 10 AND i.quantity_on_hand > 50 ORDER BY units_sold_90days, i.quantity_on_hand DESC',
      lastExecuted: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      avgDuration: 85.3, // seconds
      avgBytesProcessed: 3200000000, // bytes
      executionCount: 8,
      status: 'active'
    }
  ];
  
  // Generate schema optimization configurations
  const schemas = [
    {
      id: `schema_${Date.now()}_1`,
      dataset: 'sales',
      table: 'orders',
      rowCount: 15000000,
      sizeBytes: 4500000000,
      partitioned: false,
      clustered: false,
      lastModified: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString(),
      columns: [
        { name: 'order_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'customer_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'order_date', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'status', type: 'STRING', mode: 'REQUIRED' },
        { name: 'total', type: 'NUMERIC', mode: 'REQUIRED' }
      ],
      optimizationStatus: 'recommended'
    },
    {
      id: `schema_${Date.now()}_2`,
      dataset: 'analytics',
      table: 'customer_events',
      rowCount: 250000000,
      sizeBytes: 120000000000,
      partitioned: false,
      clustered: false,
      lastModified: new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString(),
      columns: [
        { name: 'event_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'customer_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'event_type', type: 'STRING', mode: 'REQUIRED' },
        { name: 'event_timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'event_data', type: 'JSON', mode: 'NULLABLE' }
      ],
      optimizationStatus: 'recommended'
    }
  ];
  
  // Generate resource optimization parameters
  const resources = [
    {
      id: `resource_${Date.now()}_1`,
      name: 'BigQuery Slots',
      category: 'compute',
      currentAllocation: 100,
      recommendedAllocation: 200,
      peakUsage: 180,
      avgUsage: 75,
      reservationType: 'monthly',
      costImpact: {
        current: 2000, // dollars per month
        recommended: 3500, // dollars per month
        savings: -1500 // negative = cost increase
      }
    },
    {
      id: `resource_${Date.now()}_2`,
      name: 'Cloud Composer Workers',
      category: 'orchestration',
      currentAllocation: 3,
      recommendedAllocation: 5,
      peakUsage: 4.8,
      avgUsage: 2.1,
      reservationType: 'none',
      costImpact: {
        current: 450, // dollars per month
        recommended: 750, // dollars per month
        savings: -300 // negative = cost increase
      }
    },
    {
      id: `resource_${Date.now()}_3`,
      name: 'Cloud Storage Archive',
      category: 'storage',
      currentAllocation: 'Standard',
      recommendedAllocation: 'Nearline',
      accessFrequency: 'monthly',
      sizeGB: 5000,
      costImpact: {
        current: 115, // dollars per month
        recommended: 65, // dollars per month
        savings: 50
      }
    }
  ];
  
  return {
    recommendations,
    queries,
    schemas,
    resources
  };
}

/**
 * Main function that executes during the load test
 */
export default function() {
  // Get test data from shared context if not already loaded
  if (!testData) {
    testData = __ITER === 0 ? setup() : {};
  }
  
  // Randomly select which API category to test
  const categories = ['quality', 'healing', 'monitoring', 'optimization'];
  const category = categories[Math.floor(Math.random() * categories.length)];
  
  let testResult = false;
  
  // Execute the selected test function
  switch(category) {
    case 'quality':
      testResult = testQualityEndpoints(testData.quality);
      break;
    case 'healing':
      testResult = testHealingEndpoints(testData.healing);
      break;
    case 'monitoring':
      testResult = testMonitoringEndpoints(testData.monitoring);
      break;
    case 'optimization':
      testResult = testOptimizationEndpoints(testData.optimization);
      break;
  }
  
  // Add random sleep between requests to simulate real user behavior
  randomSleep(1, 3);
  
  // Update custom metrics
  if (testResult) {
    CUSTOM_METRICS.successful_checks.add(1);
  } else {
    CUSTOM_METRICS.error_rate.add(1);
  }
}