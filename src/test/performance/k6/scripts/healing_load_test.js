import http from 'k6/http';
import { check, group } from 'k6';
import { sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import { getAuthToken, makeAuthenticatedRequest, generateTestData, setupTestEnvironment, teardownTestEnvironment, checkResponse, randomSleep, logMetrics, BASE_URL, DEFAULT_HEADERS, CUSTOM_METRICS } from './common';

// K6 options defining load test scenarios
export const options = {
  scenarios: {
    // Constant load scenario with 20 virtual users for 1 minute
    constant_load: {
      executor: 'constant-vus',
      vus: 20,
      duration: '1m',
      gracefulStop: '30s'
    },
    // Ramping load scenario that gradually increases and decreases users
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
    // Stress test scenario with increasing request rate
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
  // Performance thresholds that determine test success/failure
  thresholds: {
    'http_req_duration': ['p(95)<500', 'p(99)<1000'],
    'http_req_duration{endpoint:issue_detection}': ['p(95)<600'],
    'http_req_duration{endpoint:root_cause}': ['p(95)<800'],
    'http_req_duration{endpoint:correction}': ['p(95)<700'],
    'http_req_duration{endpoint:model_serving}': ['p(95)<300'],
    'http_reqs': ['rate>100'],
    'checks': ['rate>0.95'],
    'healing_success_rate': ['value>0.85']
  },
  // Statistics to include in the summary report
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)']
};

// Test data to be generated in setup and used throughout the test
let testData = null;

// Auth token for API requests
let authToken = null;

// Custom metrics for self-healing performance measurement
const healingMetrics = {
  issue_detection: {
    requests: new Counter('issue_detection_requests'),
    failures: new Counter('issue_detection_failures'),
    duration: new Trend('issue_detection_duration')
  },
  root_cause: {
    requests: new Counter('root_cause_requests'),
    failures: new Counter('root_cause_failures'),
    duration: new Trend('root_cause_duration')
  },
  correction: {
    requests: new Counter('correction_requests'),
    failures: new Counter('correction_failures'),
    duration: new Trend('correction_duration')
  },
  model_serving: {
    requests: new Counter('model_serving_requests'),
    failures: new Counter('model_serving_failures'),
    duration: new Trend('model_serving_duration')
  },
  healing_success_rate: new Rate('healing_success_rate'),
  correction_throughput: new Counter('correction_throughput')
};

/**
 * Setup function that runs once before the load test begins
 * 
 * @returns {Object} Test data and configuration for the load test
 */
export function setup() {
  // Initialize test environment
  const environment = setupTestEnvironment();
  
  // Get authentication token
  authToken = getAuthToken({ 
    username: 'test_admin', 
    password: 'TestPassword123' 
  });
  
  if (!authToken) {
    throw new Error('Failed to obtain authentication token for tests');
  }
  
  // Generate test data for all test scenarios
  const data = {
    issue_detection: generateIssueDetectionTestData(),
    root_cause: generateRootCauseTestData(),
    correction: generateCorrectionTestData(),
    model_serving: generateModelServingTestData(),
    environment: environment,
    createdIssueIds: []
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
  // Clean up test environment
  teardownTestEnvironment(data.environment);
  
  // Log final metrics
  logMetrics({
    issue_detection_requests: healingMetrics.issue_detection.requests.name,
    issue_detection_failures: healingMetrics.issue_detection.failures.name,
    root_cause_requests: healingMetrics.root_cause.requests.name,
    root_cause_failures: healingMetrics.root_cause.failures.name,
    correction_requests: healingMetrics.correction.requests.name,
    correction_failures: healingMetrics.correction.failures.name,
    model_serving_requests: healingMetrics.model_serving.requests.name,
    model_serving_failures: healingMetrics.model_serving.failures.name,
    healing_success_rate: healingMetrics.healing_success_rate.name,
    correction_throughput: healingMetrics.correction_throughput.name
  }, 'Final Healing Test Metrics');
  
  // Clean up any test issues or models that were created
  if (data.createdIssueIds && data.createdIssueIds.length > 0) {
    data.createdIssueIds.forEach(issueId => {
      makeAuthenticatedRequest('DELETE', `/healing/issues/${issueId}`, null, null, authToken);
    });
  }
  
  console.log('Test teardown completed successfully');
}

/**
 * Tests issue detection API endpoints under load
 * 
 * @param {Object} testData - Test data for issue detection
 * @returns {boolean} True if all tests passed
 */
function testIssueDetectionEndpoints(testData) {
  return group('Issue Detection Endpoints', () => {
    let success = true;
    const startTime = new Date().getTime();
    
    // Test GET /api/healing/issues endpoint
    const listResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/issues',
      null,
      { limit: 20, offset: 0, status: 'open' },
      authToken
    );
    
    success = success && checkResponse(listResponse, 200, 'List issues');
    
    // Test GET /api/healing/issues/{issue_id} endpoint
    if (listResponse.json('data') && listResponse.json('data').length > 0) {
      const issueId = listResponse.json('data')[0].id;
      const getResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/issues/${issueId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getResponse, 200, 'Get issue details');
    }
    
    // Test POST /api/healing/detect endpoint
    const detectionPayload = testData.detectionParams;
    const detectResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/detect',
      detectionPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(detectResponse, 201, 'Detect issue');
    
    // If issue was created successfully, store the ID for cleanup
    if (detectResponse.status === 201 && detectResponse.json('id')) {
      if (!testData.createdIssueIds) {
        testData.createdIssueIds = [];
      }
      testData.createdIssueIds.push(detectResponse.json('id'));
    }
    
    // Test PUT /api/healing/issues/{issue_id} endpoint
    if (testData.createdIssueIds && testData.createdIssueIds.length > 0) {
      const updateIssueId = testData.createdIssueIds[0];
      const updatePayload = {
        status: 'acknowledged',
        assignedTo: 'test_engineer',
        priority: 'high'
      };
      
      const updateResponse = makeAuthenticatedRequest(
        'PUT',
        `/healing/issues/${updateIssueId}`,
        updatePayload,
        null,
        authToken
      );
      
      success = success && checkResponse(updateResponse, 200, 'Update issue');
    }
    
    // Test GET /api/healing/issues/statistics endpoint
    const statsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/issues/statistics',
      null,
      { timeRange: '7d' },
      authToken
    );
    
    success = success && checkResponse(statsResponse, 200, 'Get issue statistics');
    
    // Calculate and record metrics
    const duration = new Date().getTime() - startTime;
    healingMetrics.issue_detection.duration.add(duration);
    healingMetrics.issue_detection.requests.add(1);
    
    if (!success) {
      healingMetrics.issue_detection.failures.add(1);
    }
    
    return success;
  });
}

/**
 * Tests root cause analysis API endpoints under load
 * 
 * @param {Object} testData - Test data for root cause analysis
 * @returns {boolean} True if all tests passed
 */
function testRootCauseAnalysisEndpoints(testData) {
  return group('Root Cause Analysis Endpoints', () => {
    let success = true;
    const startTime = new Date().getTime();
    
    // Test POST /api/healing/issues/{issue_id}/analyze endpoint
    if (testData.createdIssueIds && testData.createdIssueIds.length > 0) {
      const issueId = testData.createdIssueIds[0];
      const analysisPayload = testData.analysisParams;
      
      const analyzeResponse = makeAuthenticatedRequest(
        'POST',
        `/healing/issues/${issueId}/analyze`,
        analysisPayload,
        null,
        authToken
      );
      
      success = success && checkResponse(analyzeResponse, 200, 'Analyze root cause');
      
      // Test GET /api/healing/issues/{issue_id}/analysis endpoint
      const getAnalysisResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/issues/${issueId}/analysis`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getAnalysisResponse, 200, 'Get analysis results');
    }
    
    // Test GET /api/healing/patterns endpoint
    const listPatternsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/patterns',
      null,
      { limit: 20, offset: 0, category: 'data_quality' },
      authToken
    );
    
    success = success && checkResponse(listPatternsResponse, 200, 'List patterns');
    
    // Test GET /api/healing/patterns/{pattern_id} endpoint
    if (listPatternsResponse.json('data') && listPatternsResponse.json('data').length > 0) {
      const patternId = listPatternsResponse.json('data')[0].id;
      const getPatternResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/patterns/${patternId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getPatternResponse, 200, 'Get pattern details');
    }
    
    // Test POST /api/healing/patterns endpoint
    const patternPayload = testData.patternDef;
    const createPatternResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/patterns',
      patternPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(createPatternResponse, 201, 'Create pattern');
    
    // Test PUT /api/healing/patterns/{pattern_id} endpoint
    if (createPatternResponse.status === 201 && createPatternResponse.json('id')) {
      const patternId = createPatternResponse.json('id');
      const updatePatternPayload = {
        ...patternPayload,
        confidence: patternPayload.confidence + 0.05,
        description: patternPayload.description + ' (updated)',
      };
      
      const updatePatternResponse = makeAuthenticatedRequest(
        'PUT',
        `/healing/patterns/${patternId}`,
        updatePatternPayload,
        null,
        authToken
      );
      
      success = success && checkResponse(updatePatternResponse, 200, 'Update pattern');
    }
    
    // Calculate and record metrics
    const duration = new Date().getTime() - startTime;
    healingMetrics.root_cause.duration.add(duration);
    healingMetrics.root_cause.requests.add(1);
    
    if (!success) {
      healingMetrics.root_cause.failures.add(1);
    }
    
    return success;
  });
}

/**
 * Tests correction strategy API endpoints under load
 * 
 * @param {Object} testData - Test data for correction strategies
 * @returns {boolean} True if all tests passed
 */
function testCorrectionEndpoints(testData) {
  return group('Correction Endpoints', () => {
    let success = true;
    const startTime = new Date().getTime();
    
    // Test POST /api/healing/issues/{issue_id}/fix endpoint
    if (testData.createdIssueIds && testData.createdIssueIds.length > 0) {
      const issueId = testData.createdIssueIds[0];
      const fixPayload = testData.fixParams;
      
      const fixResponse = makeAuthenticatedRequest(
        'POST',
        `/healing/issues/${issueId}/fix`,
        fixPayload,
        null,
        authToken
      );
      
      success = success && checkResponse(fixResponse, 200, 'Apply fix');
      
      // If successful, increment correction throughput
      if (fixResponse.status === 200) {
        healingMetrics.correction_throughput.add(1);
      }
    }
    
    // Test GET /api/healing/actions endpoint
    const listActionsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/actions',
      null,
      { limit: 20, offset: 0, category: 'data_quality' },
      authToken
    );
    
    success = success && checkResponse(listActionsResponse, 200, 'List actions');
    
    // Test GET /api/healing/actions/{action_id} endpoint
    if (listActionsResponse.json('data') && listActionsResponse.json('data').length > 0) {
      const actionId = listActionsResponse.json('data')[0].id;
      const getActionResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/actions/${actionId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getActionResponse, 200, 'Get action details');
    }
    
    // Test POST /api/healing/actions endpoint
    const actionPayload = testData.actionDef;
    const createActionResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/actions',
      actionPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(createActionResponse, 201, 'Create action');
    
    // Test PUT /api/healing/actions/{action_id} endpoint
    if (createActionResponse.status === 201 && createActionResponse.json('id')) {
      const actionId = createActionResponse.json('id');
      const updateActionPayload = {
        ...actionPayload,
        successRate: actionPayload.successRate + 0.05,
        description: actionPayload.description + ' (updated)',
      };
      
      const updateActionResponse = makeAuthenticatedRequest(
        'PUT',
        `/healing/actions/${actionId}`,
        updateActionPayload,
        null,
        authToken
      );
      
      success = success && checkResponse(updateActionResponse, 200, 'Update action');
    }
    
    // Test GET /api/healing/executions endpoint
    const listExecutionsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/executions',
      null,
      { limit: 20, offset: 0 },
      authToken
    );
    
    success = success && checkResponse(listExecutionsResponse, 200, 'List executions');
    
    // Test GET /api/healing/executions/{execution_id} endpoint
    if (listExecutionsResponse.json('data') && listExecutionsResponse.json('data').length > 0) {
      const executionId = listExecutionsResponse.json('data')[0].id;
      const getExecutionResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/executions/${executionId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getExecutionResponse, 200, 'Get execution details');
    }
    
    // Calculate and record metrics
    const duration = new Date().getTime() - startTime;
    healingMetrics.correction.duration.add(duration);
    healingMetrics.correction.requests.add(1);
    
    if (!success) {
      healingMetrics.correction.failures.add(1);
    }
    
    return success;
  });
}

/**
 * Tests ML model serving API endpoints under load
 * 
 * @param {Object} testData - Test data for model serving
 * @returns {boolean} True if all tests passed
 */
function testModelServingEndpoints(testData) {
  return group('Model Serving Endpoints', () => {
    let success = true;
    const startTime = new Date().getTime();
    
    // Test GET /api/healing/models endpoint
    const listModelsResponse = makeAuthenticatedRequest(
      'GET',
      '/healing/models',
      null,
      { limit: 20, offset: 0, type: 'correction' },
      authToken
    );
    
    success = success && checkResponse(listModelsResponse, 200, 'List models');
    
    let modelId = null;
    
    // Test GET /api/healing/models/{model_id} endpoint
    if (listModelsResponse.json('data') && listModelsResponse.json('data').length > 0) {
      modelId = listModelsResponse.json('data')[0].id;
      const getModelResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/models/${modelId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(getModelResponse, 200, 'Get model details');
    } else {
      // If no models are available, use a default test model ID
      modelId = 'test_correction_model';
    }
    
    // Test POST /api/healing/models/{model_id}/predict endpoint
    const predictPayload = testData.predictParams;
    const predictResponse = makeAuthenticatedRequest(
      'POST',
      `/healing/models/${modelId}/predict`,
      predictPayload,
      null,
      authToken
    );
    
    // Model prediction might return 200 or 404 if model doesn't exist
    success = success && checkResponse(predictResponse, (r) => {
      return r.status === 200 || r.status === 404;
    }, 'Model prediction');
    
    // Test GET /api/healing/models/{model_id}/metrics endpoint
    const modelMetricsResponse = makeAuthenticatedRequest(
      'GET',
      `/healing/models/${modelId}/metrics`,
      null,
      { timeRange: '7d' },
      authToken
    );
    
    success = success && checkResponse(modelMetricsResponse, (r) => {
      return r.status === 200 || r.status === 404;
    }, 'Get model metrics');
    
    // Test POST /api/healing/feedback endpoint
    const feedbackPayload = testData.feedbackData;
    const feedbackResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/feedback',
      feedbackPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(feedbackResponse, 201, 'Submit feedback');
    
    // Calculate and record metrics
    const duration = new Date().getTime() - startTime;
    healingMetrics.model_serving.duration.add(duration);
    healingMetrics.model_serving.requests.add(1);
    
    if (!success) {
      healingMetrics.model_serving.failures.add(1);
    }
    
    return success;
  });
}

/**
 * Generates test data for issue detection API tests
 * 
 * @returns {Object} Issue detection test data
 */
function generateIssueDetectionTestData() {
  // Generate data quality issue examples
  const dataQualityIssues = [
    {
      type: 'data_quality',
      subtype: 'schema_mismatch',
      source: 'sales_pipeline',
      details: {
        table: 'customer_data',
        expected: { 'customer_id': 'STRING', 'email': 'STRING', 'signup_date': 'DATE' },
        actual: { 'customer_id': 'STRING', 'email': 'STRING', 'signup_date': 'TIMESTAMP' }
      },
      severity: 'high'
    },
    {
      type: 'data_quality',
      subtype: 'null_values',
      source: 'marketing_pipeline',
      details: {
        table: 'campaign_results',
        column: 'conversion_rate',
        null_percentage: 12.5,
        sample_ids: ['row_123', 'row_456', 'row_789']
      },
      severity: 'medium'
    },
    {
      type: 'data_quality',
      subtype: 'out_of_range',
      source: 'finance_pipeline',
      details: {
        table: 'transaction_data',
        column: 'amount',
        min_allowed: 0,
        max_allowed: 10000,
        actual_value: -250,
        sample_ids: ['tx_789']
      },
      severity: 'high'
    }
  ];
  
  // Generate pipeline failure issue examples
  const pipelineFailureIssues = [
    {
      type: 'pipeline_failure',
      subtype: 'task_timeout',
      source: 'etl_pipeline',
      details: {
        task_id: 'load_customer_data',
        execution_id: 'run_20230615_123',
        timeout_after: '30m',
        last_status: 'running'
      },
      severity: 'critical'
    },
    {
      type: 'pipeline_failure',
      subtype: 'data_source_unavailable',
      source: 'reporting_pipeline',
      details: {
        source_id: 'mysql_production',
        error_message: 'Connection refused',
        last_success: '2023-06-14T10:30:00Z'
      },
      severity: 'critical'
    }
  ];
  
  // Generate resource constraint issue examples
  const resourceIssues = [
    {
      type: 'resource_constraint',
      subtype: 'quota_exceeded',
      source: 'analytics_pipeline',
      details: {
        resource: 'bigquery_slots',
        quota: 100,
        requested: 120,
        project_id: 'analytics-prod-123'
      },
      severity: 'high'
    },
    {
      type: 'resource_constraint',
      subtype: 'memory_pressure',
      source: 'ml_pipeline',
      details: {
        resource: 'vertex_training_job',
        allocated_memory: '64GB',
        peak_usage: '62GB',
        duration: '45m'
      },
      severity: 'medium'
    }
  ];
  
  // Generate external dependency issue examples
  const dependencyIssues = [
    {
      type: 'external_dependency',
      subtype: 'api_error',
      source: 'integration_pipeline',
      details: {
        api_name: 'payment_gateway',
        endpoint: '/transactions/process',
        status_code: 503,
        error_message: 'Service temporarily unavailable'
      },
      severity: 'high'
    }
  ];
  
  // Create detection parameters for API testing
  const detectionParams = {
    source: 'test_pipeline',
    check_type: 'data_quality',
    data: {
      table_name: 'sales_data',
      row_count: 10000,
      sample_size: 1000,
      validation_rules: [
        { column: 'order_id', rule: 'not_null' },
        { column: 'customer_id', rule: 'foreign_key', ref_table: 'customers', ref_column: 'id' },
        { column: 'amount', rule: 'range', min: 0, max: 10000 }
      ]
    }
  };
  
  return {
    dataQualityIssues,
    pipelineFailureIssues,
    resourceIssues,
    dependencyIssues,
    detectionParams,
    // Sample ids for using in the tests
    sampleIssueIds: ['issue_123', 'issue_456', 'issue_789'],
    createdIssueIds: []
  };
}

/**
 * Generates test data for root cause analysis API tests
 * 
 * @returns {Object} Root cause analysis test data
 */
function generateRootCauseTestData() {
  // Analysis parameters for different issue types
  const analysisParams = {
    analysis_depth: 'deep',
    include_historical: true,
    time_window: '7d',
    related_systems: ['bigquery', 'cloud_composer', 'cloud_storage']
  };
  
  // Test pattern definitions
  const patternDef = {
    name: 'Null values after ETL transformation',
    category: 'data_quality',
    detection_pattern: {
      conditions: [
        { field: 'issue.type', operator: 'equals', value: 'data_quality' },
        { field: 'issue.subtype', operator: 'equals', value: 'null_values' },
        { field: 'issue.details.table', operator: 'matches', value: '.*_transformed$' }
      ],
      aggregation: 'all'
    },
    root_causes: [
      {
        cause: 'ETL transformation error handling',
        probability: 0.75,
        description: 'The transformation process is not properly handling empty strings or special characters'
      },
      {
        cause: 'Source data missing values',
        probability: 0.20,
        description: 'The source data already contains null values that aren\'t being detected during extraction'
      },
      {
        cause: 'Database constraint issues',
        probability: 0.05,
        description: 'Database constraints causing some inserts to fail'
      }
    ],
    confidence: 0.85,
    created_by: 'test_user',
    description: 'Pattern for identifying null values introduced during ETL transformations'
  };
  
  // Pattern matching parameters
  const matchParams = {
    issue_data: {
      type: 'data_quality',
      subtype: 'null_values',
      details: {
        table: 'customers_transformed',
        column: 'email',
        null_percentage: 5.2
      }
    },
    match_threshold: 0.7,
    max_matches: 3
  };
  
  return {
    analysisParams,
    patternDef,
    matchParams,
    // Sample ids for testing
    samplePatternIds: ['pattern_123', 'pattern_456', 'pattern_789']
  };
}

/**
 * Generates test data for correction API tests
 * 
 * @returns {Object} Correction test data
 */
function generateCorrectionTestData() {
  // Correction action definitions
  const actionDef = {
    name: 'Null value imputation',
    type: 'data_correction',
    category: 'data_quality',
    applicable_to: [
      { issue_type: 'data_quality', issue_subtype: 'null_values' }
    ],
    action_definition: {
      steps: [
        {
          type: 'sql',
          target: 'bigquery',
          description: 'Update null values with default or average',
          template: "UPDATE {{ table }} SET {{ column }} = COALESCE({{ column }}, {{ default_value }}) WHERE {{ condition }}"
        },
        {
          type: 'notification',
          target: 'data_owner',
          description: 'Notify data owner about the correction',
          template: "Null values in {{ table }}.{{ column }} were automatically corrected"
        }
      ],
      parameters: [
        { name: 'table', type: 'string', required: true },
        { name: 'column', type: 'string', required: true },
        { name: 'default_value', type: 'string', required: true },
        { name: 'condition', type: 'string', required: false, default: '1=1' }
      ]
    },
    approval_required: false,
    successRate: 0.95,
    created_by: 'test_user',
    description: 'Automatically impute null values with defaults or calculated values'
  };
  
  // Fix parameters for testing corrections
  const fixParams = {
    action_id: 'action_123', // Can be overridden with actual ID during tests
    parameters: {
      table: 'customers_transformed',
      column: 'email',
      default_value: "'unknown@example.com'",
      condition: "email IS NULL"
    },
    execution_options: {
      dry_run: false,
      priority: 'high',
      timeout: '5m'
    }
  };
  
  // Execution parameters
  const executionParams = {
    issue_id: 'issue_123', // Can be overridden with actual ID during tests
    action_id: 'action_123', // Can be overridden with actual ID during tests
    parameters: {
      table: 'products',
      column: 'price',
      default_value: '0',
      condition: "price IS NULL OR price < 0"
    },
    requested_by: 'test_user'
  };
  
  // Approval workflow data
  const approvalData = {
    execution_id: 'exec_123', // Can be overridden with actual ID during tests
    approved: true,
    approved_by: 'test_approver',
    comments: 'Approved for automated correction',
    approval_time: new Date().toISOString()
  };
  
  return {
    actionDef,
    fixParams,
    executionParams,
    approvalData,
    // Sample ids for testing
    sampleActionIds: ['action_123', 'action_456', 'action_789'],
    sampleExecutionIds: ['exec_123', 'exec_456', 'exec_789']
  };
}

/**
 * Generates test data for model serving API tests
 * 
 * @returns {Object} Model serving test data
 */
function generateModelServingTestData() {
  // Model input data
  const predictParams = {
    features: {
      issue_type: 'data_quality',
      issue_subtype: 'null_values',
      table_name: 'customers',
      column_name: 'email',
      null_percentage: 5.2,
      data_volume: 10000,
      pipeline: 'etl_pipeline',
      environment: 'production'
    },
    options: {
      confidence_threshold: 0.7,
      max_results: 3,
      include_explanation: true
    }
  };
  
  // Feedback data
  const feedbackData = {
    model_id: 'model_123', // Can be overridden with actual ID during tests
    prediction_id: 'pred_123',
    actual_outcome: 'success',
    feedback_type: 'correction_success',
    details: {
      execution_time: 42.5,
      applied_fix: 'null_value_imputation',
      verification_result: 'passed'
    },
    submitted_by: 'test_user'
  };
  
  return {
    predictParams,
    feedbackData,
    // Sample ids for testing
    sampleModelIds: ['model_123', 'model_456', 'model_789'],
    modelTypes: ['detection', 'root_cause_analysis', 'correction', 'prediction']
  };
}

/**
 * Simulates a complete self-healing process from detection to resolution
 * 
 * @param {Object} testData - Test data for the simulation
 * @returns {boolean} True if simulation completed successfully
 */
function simulateHealingProcess(testData) {
  return group('End-to-End Healing Simulation', () => {
    let success = true;
    const startTime = new Date().getTime();
    
    // Step 1: Create a test issue through detection API
    const detectionPayload = testData.issue_detection.detectionParams;
    const detectResponse = makeAuthenticatedRequest(
      'POST',
      '/healing/detect',
      detectionPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(detectResponse, 201, 'Detect issue');
    
    // If issue creation failed, stop the simulation
    if (detectResponse.status !== 201 || !detectResponse.json('id')) {
      healingMetrics.healing_success_rate.add(0);
      return false;
    }
    
    const issueId = detectResponse.json('id');
    
    // Store the created issue ID for cleanup
    if (!testData.createdIssueIds) {
      testData.createdIssueIds = [];
    }
    testData.createdIssueIds.push(issueId);
    
    // Step 2: Trigger root cause analysis
    const analysisPayload = testData.root_cause.analysisParams;
    const analyzeResponse = makeAuthenticatedRequest(
      'POST',
      `/healing/issues/${issueId}/analyze`,
      analysisPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(analyzeResponse, 200, 'Analyze root cause');
    
    // If analysis failed, mark as partially successful
    if (analyzeResponse.status !== 200) {
      healingMetrics.healing_success_rate.add(0.5);
      return success;
    }
    
    // Step 3: Evaluate and apply correction
    const fixPayload = testData.correction.fixParams;
    const fixResponse = makeAuthenticatedRequest(
      'POST',
      `/healing/issues/${issueId}/fix`,
      fixPayload,
      null,
      authToken
    );
    
    success = success && checkResponse(fixResponse, 200, 'Apply fix');
    
    // If correction failed, mark as partially successful
    if (fixResponse.status !== 200) {
      healingMetrics.healing_success_rate.add(0.5);
      return success;
    }
    
    // If we reached this point, the execution ID should be available
    const executionId = fixResponse.json('execution_id');
    
    // Step 4: Verify the outcome
    if (executionId) {
      // Wait a moment for the execution to complete
      sleep(1);
      
      // Check execution status
      const executionResponse = makeAuthenticatedRequest(
        'GET',
        `/healing/executions/${executionId}`,
        null,
        null,
        authToken
      );
      
      success = success && checkResponse(executionResponse, 200, 'Check execution status');
      
      // Record throughput
      healingMetrics.correction_throughput.add(1);
      
      // Step 5: Submit feedback
      const feedbackPayload = {
        ...testData.model_serving.feedbackData,
        execution_id: executionId,
        prediction_id: executionId
      };
      
      const feedbackResponse = makeAuthenticatedRequest(
        'POST',
        '/healing/feedback',
        feedbackPayload,
        null,
        authToken
      );
      
      success = success && checkResponse(feedbackResponse, 201, 'Submit feedback');
    }
    
    // If all steps completed successfully, mark as full success
    if (success) {
      healingMetrics.healing_success_rate.add(1);
    } else {
      healingMetrics.healing_success_rate.add(0.3);
    }
    
    return success;
  });
}

/**
 * Main function that executes during the load test
 */
export default function() {
  // Get test data from the shared context
  testData = testData || __ENV.testData;
  
  // Randomly select which component to test based on test iteration
  const testComponents = [
    { weight: 0.3, func: () => testIssueDetectionEndpoints(testData.issue_detection) },
    { weight: 0.2, func: () => testRootCauseAnalysisEndpoints(testData.root_cause) },
    { weight: 0.2, func: () => testCorrectionEndpoints(testData.correction) },
    { weight: 0.15, func: () => testModelServingEndpoints(testData.model_serving) },
    { weight: 0.15, func: () => simulateHealingProcess(testData) }
  ];
  
  // Weighted random selection of test component
  let random = Math.random();
  let cumulativeWeight = 0;
  
  for (const component of testComponents) {
    cumulativeWeight += component.weight;
    if (random <= cumulativeWeight) {
      component.func();
      break;
    }
  }
  
  // Occasionally run the full end-to-end healing simulation
  if (__ITER % 10 === 0) {
    simulateHealingProcess(testData);
  }
  
  // Add some randomization to requests timing to simulate real users
  randomSleep(1, 3);
}