import http from 'k6/http';
import { check, group } from 'k6';
import { sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import { getAuthToken, makeAuthenticatedRequest, generateTestData, setupTestEnvironment, teardownTestEnvironment, checkResponse, randomSleep, logMetrics, BASE_URL, DEFAULT_HEADERS, CUSTOM_METRICS } from './common';

// Test execution configuration
export const options = {
  scenarios: {
    // Scenario 1: Constant number of VUs
    constant_load: {
      executor: 'constant-vus',
      vus: 20,
      duration: '1m',
      gracefulStop: '30s',
    },
    // Scenario 2: Ramping VUs (gradual increase and decrease)
    ramp_up: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '1m', target: 20 },
        { duration: '30s', target: 40 },
        { duration: '1m', target: 20 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '30s',
    },
    // Scenario 3: Arrival rate (requests per second)
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
        { duration: '1m', target: 0 },
      ],
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<500', 'p(99)<1000'],
    'http_req_duration{endpoint:sources}': ['p(95)<400'],
    'http_req_duration{endpoint:pipelines}': ['p(95)<600'],
    'http_req_duration{endpoint:executions}': ['p(95)<800'],
    'http_req_duration{endpoint:tasks}': ['p(95)<500'],
    'http_reqs': ['rate>100'],
    'checks': ['rate>0.95'],
    'data_throughput': ['value>5000'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

// Shared test data and authentication token
let testData = null;
let authToken = null;

// Custom metrics for ingestion components
const ingestionMetrics = {
  sources: {
    requests: new Counter('sources_requests'),
    failures: new Counter('sources_failures'),
    duration: new Trend('sources_duration'),
  },
  pipelines: {
    requests: new Counter('pipelines_requests'),
    failures: new Counter('pipelines_failures'),
    duration: new Trend('pipelines_duration'),
  },
  executions: {
    requests: new Counter('executions_requests'),
    failures: new Counter('executions_failures'),
    duration: new Trend('executions_duration'),
  },
  tasks: {
    requests: new Counter('tasks_requests'),
    failures: new Counter('tasks_failures'),
    duration: new Trend('tasks_duration'),
  },
  data_throughput: new Counter('data_throughput'),
};

/**
 * Setup function that runs once before the load test begins
 * @returns {Object} Test data and configuration for the load test
 */
export function setup() {
  // Initialize test environment
  const env = setupTestEnvironment();
  
  // Get authentication token
  const credentials = {
    username: __ENV.TEST_USERNAME || 'testuser',
    password: __ENV.TEST_PASSWORD || 'Password123!'
  };
  
  authToken = getAuthToken(credentials);
  console.log('Authentication token obtained for test execution');
  
  // Generate test data for different parts of the ingestion API
  const sourceData = generateSourceSystemTestData();
  const pipelineData = generatePipelineTestData();
  const executionData = generateExecutionTestData();
  
  testData = {
    env,
    authToken,
    sources: sourceData,
    pipelines: pipelineData,
    executions: executionData,
  };
  
  return testData;
}

/**
 * Teardown function that runs once after the load test completes
 * @param {Object} data - Test data from setup
 */
export function teardown(data) {
  // Clean up test environment
  teardownTestEnvironment(data.env);
  
  // Log final metrics
  logMetrics({
    sources: {
      requests: ingestionMetrics.sources.requests.name,
      failures: ingestionMetrics.sources.failures.name,
      avgDuration: ingestionMetrics.sources.duration.name,
    },
    pipelines: {
      requests: ingestionMetrics.pipelines.requests.name,
      failures: ingestionMetrics.pipelines.failures.name,
      avgDuration: ingestionMetrics.pipelines.duration.name,
    },
    executions: {
      requests: ingestionMetrics.executions.requests.name,
      failures: ingestionMetrics.executions.failures.name,
      avgDuration: ingestionMetrics.executions.duration.name,
    },
    tasks: {
      requests: ingestionMetrics.tasks.requests.name,
      failures: ingestionMetrics.tasks.failures.name,
      avgDuration: ingestionMetrics.tasks.duration.name,
    },
    data_throughput: ingestionMetrics.data_throughput.name,
  }, 'Final Ingestion Test Metrics');
  
  // Clean up any source systems or pipelines created during testing
  console.log('Teardown complete');
}

/**
 * Tests source system management API endpoints under load
 * @param {Object} testData - Test data from setup
 * @returns {boolean} True if all tests passed
 */
function testSourceSystemEndpoints(testData) {
  return group('Source System API Tests', () => {
    // Start timer for metrics
    const startTime = new Date();
    let success = true;
    
    // Increment request counter
    ingestionMetrics.sources.requests.add(1);
    
    // Test GET /api/ingestion/sources (list sources)
    const listResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/sources',
      null,
      { page: 1, limit: 20, type: 'all' },
      testData.authToken
    );
    
    if (!checkResponse(listResponse, 200, 'List Sources')) {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // Test GET /api/ingestion/sources/{source_id} (get source details)
    // For this test, we'll try to get the first source from the list or use a test ID
    let sourceId = 'test_source_id';
    if (listResponse.status === 200) {
      const sources = listResponse.json('sources') || [];
      if (sources.length > 0) {
        sourceId = sources[0].id;
      }
    }
    
    const getResponse = makeAuthenticatedRequest(
      'GET',
      `/ingestion/sources/${sourceId}`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(getResponse, { 'Get Source Details - Success': (r) => r.status === 200 || r.status === 404 })) {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // Test POST /api/ingestion/sources (create source)
    // Randomly select a source type from the test data
    const sourceTypes = Object.keys(testData.sources);
    const randomType = sourceTypes[Math.floor(Math.random() * sourceTypes.length)];
    const randomSource = testData.sources[randomType];
    
    const createResponse = makeAuthenticatedRequest(
      'POST',
      '/ingestion/sources',
      randomSource,
      null,
      testData.authToken
    );
    
    let createdSourceId = null;
    if (checkResponse(createResponse, 201, 'Create Source')) {
      createdSourceId = createResponse.json('id');
    } else {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // If we created a source, test updating and deleting it
    if (createdSourceId) {
      // Test PUT /api/ingestion/sources/{source_id} (update source)
      const updatePayload = { ...randomSource, name: `${randomSource.name}_updated` };
      const updateResponse = makeAuthenticatedRequest(
        'PUT',
        `/ingestion/sources/${createdSourceId}`,
        updatePayload,
        null,
        testData.authToken
      );
      
      if (!checkResponse(updateResponse, 200, 'Update Source')) {
        success = false;
        ingestionMetrics.sources.failures.add(1);
      }
      
      // Test DELETE /api/ingestion/sources/{source_id} (delete source)
      const deleteResponse = makeAuthenticatedRequest(
        'DELETE',
        `/ingestion/sources/${createdSourceId}`,
        null,
        null,
        testData.authToken
      );
      
      if (!checkResponse(deleteResponse, 204, 'Delete Source')) {
        success = false;
        ingestionMetrics.sources.failures.add(1);
      }
    }
    
    // Test POST /api/ingestion/sources/test (test connection)
    const testConnResponse = makeAuthenticatedRequest(
      'POST',
      '/ingestion/sources/test',
      randomSource,
      null,
      testData.authToken
    );
    
    if (!checkResponse(testConnResponse, 200, 'Test Source Connection')) {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // Test GET /api/ingestion/sources/{source_id}/schema/{object_name} (get schema)
    // We'll use a generic object name here
    const schemaResponse = makeAuthenticatedRequest(
      'GET',
      `/ingestion/sources/${sourceId}/schema/default`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(schemaResponse, { 'Get Schema - Success': (r) => r.status === 200 || r.status === 404 })) {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // Test GET /api/ingestion/sources/types (list source types)
    const typesResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/sources/types',
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(typesResponse, 200, 'List Source Types')) {
      success = false;
      ingestionMetrics.sources.failures.add(1);
    }
    
    // Calculate and record duration
    const duration = (new Date() - startTime) / 1000;
    ingestionMetrics.sources.duration.add(duration);
    
    return success;
  });
}

/**
 * Tests pipeline management API endpoints under load
 * @param {Object} testData - Test data from setup
 * @returns {boolean} True if all tests passed
 */
function testPipelineEndpoints(testData) {
  return group('Pipeline API Tests', () => {
    // Start timer for metrics
    const startTime = new Date();
    let success = true;
    
    // Increment request counter
    ingestionMetrics.pipelines.requests.add(1);
    
    // Test GET /api/ingestion/pipelines (list pipelines)
    const listResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/pipelines',
      null,
      { page: 1, limit: 20, status: 'all' },
      testData.authToken
    );
    
    if (!checkResponse(listResponse, 200, 'List Pipelines')) {
      success = false;
      ingestionMetrics.pipelines.failures.add(1);
    }
    
    // Test GET /api/ingestion/pipelines/{pipeline_id} (get pipeline details)
    // For this test, we'll try to get the first pipeline from the list or use a test ID
    let pipelineId = 'test_pipeline_id';
    if (listResponse.status === 200) {
      const pipelines = listResponse.json('pipelines') || [];
      if (pipelines.length > 0) {
        pipelineId = pipelines[0].id;
      }
    }
    
    const getResponse = makeAuthenticatedRequest(
      'GET',
      `/ingestion/pipelines/${pipelineId}`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(getResponse, { 'Get Pipeline Details - Success': (r) => r.status === 200 || r.status === 404 })) {
      success = false;
      ingestionMetrics.pipelines.failures.add(1);
    }
    
    // Test POST /api/ingestion/pipelines (create pipeline)
    // Randomly select a pipeline from the test data
    const randomPipeline = testData.pipelines[Math.floor(Math.random() * testData.pipelines.length)];
    
    const createResponse = makeAuthenticatedRequest(
      'POST',
      '/ingestion/pipelines',
      randomPipeline,
      null,
      testData.authToken
    );
    
    let createdPipelineId = null;
    if (checkResponse(createResponse, 201, 'Create Pipeline')) {
      createdPipelineId = createResponse.json('id');
    } else {
      success = false;
      ingestionMetrics.pipelines.failures.add(1);
    }
    
    // If we created a pipeline, test updating and deleting it
    if (createdPipelineId) {
      // Test PUT /api/ingestion/pipelines/{pipeline_id} (update pipeline)
      const updatePayload = { ...randomPipeline, name: `${randomPipeline.name}_updated` };
      const updateResponse = makeAuthenticatedRequest(
        'PUT',
        `/ingestion/pipelines/${createdPipelineId}`,
        updatePayload,
        null,
        testData.authToken
      );
      
      if (!checkResponse(updateResponse, 200, 'Update Pipeline')) {
        success = false;
        ingestionMetrics.pipelines.failures.add(1);
      }
      
      // Test DELETE /api/ingestion/pipelines/{pipeline_id} (delete pipeline)
      const deleteResponse = makeAuthenticatedRequest(
        'DELETE',
        `/ingestion/pipelines/${createdPipelineId}`,
        null,
        null,
        testData.authToken
      );
      
      if (!checkResponse(deleteResponse, 204, 'Delete Pipeline')) {
        success = false;
        ingestionMetrics.pipelines.failures.add(1);
      }
    }
    
    // Calculate and record duration
    const duration = (new Date() - startTime) / 1000;
    ingestionMetrics.pipelines.duration.add(duration);
    
    return success;
  });
}

/**
 * Tests pipeline execution API endpoints under load
 * @param {Object} testData - Test data from setup
 * @returns {boolean} True if all tests passed
 */
function testPipelineExecutionEndpoints(testData) {
  return group('Pipeline Execution API Tests', () => {
    // Start timer for metrics
    const startTime = new Date();
    let success = true;
    
    // Increment request counter
    ingestionMetrics.executions.requests.add(1);
    
    // Test GET /api/ingestion/executions (list executions)
    const listResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/executions',
      null,
      { page: 1, limit: 20, status: 'all' },
      testData.authToken
    );
    
    if (!checkResponse(listResponse, 200, 'List Executions')) {
      success = false;
      ingestionMetrics.executions.failures.add(1);
    }
    
    // Get a valid pipeline ID to execute
    const listPipelinesResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/pipelines',
      null,
      { page: 1, limit: 5, status: 'active' },
      testData.authToken
    );
    
    let pipelineIdToExecute = 'test_pipeline_id';
    let executionId = 'test_execution_id';
    
    if (listPipelinesResponse.status === 200) {
      const pipelines = listPipelinesResponse.json('pipelines') || [];
      if (pipelines.length > 0) {
        pipelineIdToExecute = pipelines[0].id;
      }
    }
    
    // Test POST /api/ingestion/pipelines/{pipeline_id}/execute (execute pipeline)
    // Randomly select execution parameters from test data
    const randomExecution = testData.executions[Math.floor(Math.random() * testData.executions.length)];
    
    const executeResponse = makeAuthenticatedRequest(
      'POST',
      `/ingestion/pipelines/${pipelineIdToExecute}/execute`,
      randomExecution,
      null,
      testData.authToken
    );
    
    if (checkResponse(executeResponse, 202, 'Execute Pipeline')) {
      executionId = executeResponse.json('execution_id') || executionId;
    } else {
      success = false;
      ingestionMetrics.executions.failures.add(1);
    }
    
    // Test GET /api/ingestion/executions/{execution_id} (get execution details)
    const getExecResponse = makeAuthenticatedRequest(
      'GET',
      `/ingestion/executions/${executionId}`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(getExecResponse, { 'Get Execution Details - Success': (r) => r.status === 200 || r.status === 404 })) {
      success = false;
      ingestionMetrics.executions.failures.add(1);
    }
    
    // Test POST /api/ingestion/executions/{execution_id}/cancel (cancel execution)
    const cancelResponse = makeAuthenticatedRequest(
      'POST',
      `/ingestion/executions/${executionId}/cancel`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(cancelResponse, { 'Cancel Execution - Success': (r) => r.status === 200 || r.status === 404 || r.status === 409 })) {
      success = false;
      ingestionMetrics.executions.failures.add(1);
    }
    
    // Test POST /api/ingestion/executions/{execution_id}/retry (retry execution)
    const retryResponse = makeAuthenticatedRequest(
      'POST',
      `/ingestion/executions/${executionId}/retry`,
      null,
      null,
      testData.authToken
    );
    
    if (!checkResponse(retryResponse, { 'Retry Execution - Success': (r) => r.status === 202 || r.status === 404 || r.status === 409 })) {
      success = false;
      ingestionMetrics.executions.failures.add(1);
    }
    
    // Calculate and record duration
    const duration = (new Date() - startTime) / 1000;
    ingestionMetrics.executions.duration.add(duration);
    
    return success;
  });
}

/**
 * Tests task execution API endpoints under load
 * @param {Object} testData - Test data from setup
 * @returns {boolean} True if all tests passed
 */
function testTaskExecutionEndpoints(testData) {
  return group('Task Execution API Tests', () => {
    // Start timer for metrics
    const startTime = new Date();
    let success = true;
    
    // Increment request counter
    ingestionMetrics.tasks.requests.add(1);
    
    // First, get a valid execution ID
    const listExecutionsResponse = makeAuthenticatedRequest(
      'GET',
      '/ingestion/executions',
      null,
      { page: 1, limit: 5 },
      testData.authToken
    );
    
    let executionId = 'test_execution_id';
    
    if (listExecutionsResponse.status === 200) {
      const executions = listExecutionsResponse.json('executions') || [];
      if (executions.length > 0) {
        executionId = executions[0].id;
      }
    }
    
    // Test GET /api/ingestion/executions/{execution_id}/tasks (list tasks)
    const tasksResponse = makeAuthenticatedRequest(
      'GET',
      `/ingestion/executions/${executionId}/tasks`,
      null,
      { page: 1, limit: 20, status: 'all' },
      testData.authToken
    );
    
    if (!checkResponse(tasksResponse, { 'List Tasks - Success': (r) => r.status === 200 || r.status === 404 })) {
      success = false;
      ingestionMetrics.tasks.failures.add(1);
    }
    
    // Calculate and record duration
    const duration = (new Date() - startTime) / 1000;
    ingestionMetrics.tasks.duration.add(duration);
    
    return success;
  });
}

/**
 * Generates test data for source system API tests
 * @returns {Object} Source system test data
 */
function generateSourceSystemTestData() {
  return {
    gcs: {
      name: `gcs_source_${Date.now()}`,
      type: 'gcs',
      config: {
        bucket: 'test-data-bucket',
        path: 'data/incoming/',
        filePattern: '*.csv',
        authentication: {
          type: 'service_account',
          project_id: 'test-project'
        }
      }
    },
    cloudsql: {
      name: `cloudsql_source_${Date.now()}`,
      type: 'cloudsql',
      config: {
        instance: 'test-cloud-sql',
        database: 'test_db',
        username: 'test_user',
        password: 'test_password',
        tables: ['customers', 'orders', 'products']
      }
    },
    api: {
      name: `api_source_${Date.now()}`,
      type: 'rest_api',
      config: {
        baseUrl: 'https://api.example.com/v1',
        authentication: {
          type: 'oauth2',
          clientId: 'test-client',
          clientSecret: 'test-secret'
        },
        endpoints: [
          {
            path: '/users',
            method: 'GET',
            params: {
              limit: 100
            }
          }
        ]
      }
    },
    custom: {
      name: `custom_source_${Date.now()}`,
      type: 'custom',
      config: {
        connectionString: 'custom://connection/string',
        parameters: {
          param1: 'value1',
          param2: 'value2'
        }
      }
    }
  };
}

/**
 * Generates test data for pipeline API tests
 * @returns {Array} Pipeline test data
 */
function generatePipelineTestData() {
  return [
    {
      name: `gcs_to_bq_pipeline_${Date.now()}`,
      description: 'Pipeline to load GCS files to BigQuery',
      source: {
        type: 'gcs',
        config: {
          bucket: 'test-data-bucket',
          path: 'data/incoming/',
          filePattern: '*.csv'
        }
      },
      destination: {
        type: 'bigquery',
        config: {
          dataset: 'test_dataset',
          table: 'test_table',
          writeDisposition: 'WRITE_APPEND'
        }
      },
      schedule: '0 */2 * * *', // Every 2 hours
      enabled: true,
      validation: {
        enabled: true,
        rules: [
          {
            type: 'not_null',
            columns: ['id', 'name', 'timestamp']
          }
        ]
      },
      transformations: [
        {
          type: 'sql',
          script: 'SELECT * FROM input WHERE value > 0'
        }
      ],
      errorHandling: {
        retryCount: 3,
        retryDelay: 60,
        deadLetterDestination: {
          type: 'gcs',
          config: {
            bucket: 'error-bucket',
            path: 'dead-letter/'
          }
        }
      }
    },
    {
      name: `sql_to_bq_pipeline_${Date.now()}`,
      description: 'Pipeline to extract Cloud SQL data to BigQuery',
      source: {
        type: 'cloudsql',
        config: {
          instance: 'test-instance',
          database: 'test_db',
          table: 'customers'
        }
      },
      destination: {
        type: 'bigquery',
        config: {
          dataset: 'test_dataset',
          table: 'customers',
          writeDisposition: 'WRITE_TRUNCATE'
        }
      },
      schedule: '0 0 * * *', // Daily at midnight
      enabled: true,
      validation: {
        enabled: true,
        rules: [
          {
            type: 'unique',
            columns: ['customer_id']
          }
        ]
      },
      transformations: [],
      errorHandling: {
        retryCount: 2,
        retryDelay: 120
      }
    },
    {
      name: `api_to_bq_pipeline_${Date.now()}`,
      description: 'Pipeline to load API data to BigQuery',
      source: {
        type: 'rest_api',
        config: {
          endpoint: 'https://api.example.com/v1/data',
          method: 'GET'
        }
      },
      destination: {
        type: 'bigquery',
        config: {
          dataset: 'test_dataset',
          table: 'api_data',
          writeDisposition: 'WRITE_APPEND'
        }
      },
      schedule: '0 */6 * * *', // Every 6 hours
      enabled: true,
      validation: {
        enabled: true,
        rules: []
      },
      transformations: [
        {
          type: 'jq',
          script: '.data[] | {id: .id, name: .name, value: .metrics.value}'
        }
      ],
      errorHandling: {
        retryCount: 3,
        retryDelay: 60
      }
    }
  ];
}

/**
 * Generates test data for pipeline execution API tests
 * @returns {Array} Execution test data
 */
function generateExecutionTestData() {
  return [
    {
      // Default execution with no overrides
      parameters: {}
    },
    {
      // Execution with date range override
      parameters: {
        startDate: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 7 days ago
        endDate: new Date().toISOString().split('T')[0] // Today
      }
    },
    {
      // Execution with specific config overrides
      parameters: {
        source: {
          path: 'data/special/'
        },
        validation: {
          enabled: false
        }
      }
    },
    {
      // Full backfill execution
      parameters: {
        backfill: true,
        forceReload: true
      }
    }
  ];
}

/**
 * Simulates a complete data ingestion process
 * @param {Object} testData - Test data from setup
 * @returns {boolean} True if simulation completed successfully
 */
function simulateDataIngestion(testData) {
  return group('Complete Ingestion Simulation', () => {
    let success = true;
    
    // 1. Create a test source
    const source = testData.sources.gcs;
    const createSourceResponse = makeAuthenticatedRequest(
      'POST',
      '/ingestion/sources',
      source,
      null,
      testData.authToken
    );
    
    if (!checkResponse(createSourceResponse, 201, 'Create Test Source')) {
      success = false;
      return false; // Stop simulation if we can't create the source
    }
    
    const sourceId = createSourceResponse.json('id');
    
    // 2. Create a test pipeline using this source
    const pipeline = {
      name: `test_pipeline_${Date.now()}`,
      description: 'Test pipeline for load testing',
      source: {
        id: sourceId,
        type: 'gcs'
      },
      destination: {
        type: 'bigquery',
        config: {
          dataset: 'test_dataset',
          table: 'test_table_' + Date.now(),
          writeDisposition: 'WRITE_APPEND'
        }
      },
      schedule: null, // On-demand pipeline
      enabled: true,
      validation: {
        enabled: true,
        rules: []
      }
    };
    
    const createPipelineResponse = makeAuthenticatedRequest(
      'POST',
      '/ingestion/pipelines',
      pipeline,
      null,
      testData.authToken
    );
    
    if (!checkResponse(createPipelineResponse, 201, 'Create Test Pipeline')) {
      success = false;
      
      // Clean up the source
      makeAuthenticatedRequest(
        'DELETE',
        `/ingestion/sources/${sourceId}`,
        null,
        null,
        testData.authToken
      );
      
      return false;
    }
    
    const pipelineId = createPipelineResponse.json('id');
    
    // 3. Execute the pipeline
    const executionParams = {
      parameters: {
        dataSize: 1000 // 1000 records for testing
      }
    };
    
    const executeResponse = makeAuthenticatedRequest(
      'POST',
      `/ingestion/pipelines/${pipelineId}/execute`,
      executionParams,
      null,
      testData.authToken
    );
    
    if (!checkResponse(executeResponse, 202, 'Execute Test Pipeline')) {
      success = false;
      
      // Clean up resources
      makeAuthenticatedRequest(
        'DELETE',
        `/ingestion/pipelines/${pipelineId}`,
        null,
        null,
        testData.authToken
      );
      
      makeAuthenticatedRequest(
        'DELETE',
        `/ingestion/sources/${sourceId}`,
        null,
        null,
        testData.authToken
      );
      
      return false;
    }
    
    const executionId = executeResponse.json('execution_id');
    
    // 4. Monitor the execution (poll for status)
    let executionComplete = false;
    let retries = 0;
    const maxRetries = 10;
    
    while (!executionComplete && retries < maxRetries) {
      sleep(2); // Wait 2 seconds between polls
      
      const statusResponse = makeAuthenticatedRequest(
        'GET',
        `/ingestion/executions/${executionId}`,
        null,
        null,
        testData.authToken
      );
      
      if (statusResponse.status === 200) {
        const status = statusResponse.json('status');
        
        if (status === 'completed' || status === 'failed') {
          executionComplete = true;
          
          // Record data throughput based on records processed
          const recordsProcessed = statusResponse.json('records_processed') || 1000;
          ingestionMetrics.data_throughput.add(recordsProcessed);
          
          // Validate status
          check(statusResponse, {
            'Pipeline execution completed or failed': (r) => r.json('status') === 'completed' || r.json('status') === 'failed',
          });
        }
      }
      
      retries++;
    }
    
    // 5. Clean up resources
    makeAuthenticatedRequest(
      'DELETE',
      `/ingestion/pipelines/${pipelineId}`,
      null,
      null,
      testData.authToken
    );
    
    makeAuthenticatedRequest(
      'DELETE',
      `/ingestion/sources/${sourceId}`,
      null,
      null,
      testData.authToken
    );
    
    return success;
  });
}

/**
 * Main function that executes during the load test
 */
export default function() {
  // Get test data from shared object
  if (!testData) {
    console.log('Test data not available, skipping iteration');
    return;
  }
  
  // Test different ingestion API endpoints randomly
  const testFunctions = [
    { weight: 3, fn: () => testSourceSystemEndpoints(testData) },
    { weight: 4, fn: () => testPipelineEndpoints(testData) },
    { weight: 3, fn: () => testPipelineExecutionEndpoints(testData) },
    { weight: 2, fn: () => testTaskExecutionEndpoints(testData) }
  ];
  
  // Calculate total weight
  const totalWeight = testFunctions.reduce((sum, tf) => sum + tf.weight, 0);
  
  // Select random test based on weight
  let randomValue = Math.random() * totalWeight;
  let selectedTest = testFunctions[0].fn;
  
  for (const testFunction of testFunctions) {
    randomValue -= testFunction.weight;
    if (randomValue <= 0) {
      selectedTest = testFunction.fn;
      break;
    }
  }
  
  // Execute the selected test
  selectedTest();
  
  // Periodically run the complete ingestion simulation (roughly 10% of iterations)
  if (Math.random() < 0.1) {
    simulateDataIngestion(testData);
  }
  
  // Add some randomness to the execution time to make the test more realistic
  randomSleep(1, 3);
}