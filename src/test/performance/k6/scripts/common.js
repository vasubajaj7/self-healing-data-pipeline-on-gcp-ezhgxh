import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import encoding from 'k6/encoding';

// Global configuration for all performance tests
export const BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8000/api';
export const DEFAULT_HEADERS = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
};

// Custom metrics for detailed performance analysis
export const CUSTOM_METRICS = {
  successful_requests: new Counter('successful_requests'),
  failed_requests: new Counter('failed_requests'),
  request_duration: new Trend('request_duration'),
  successful_checks: new Rate('successful_checks'),
  data_throughput: new Counter('data_throughput'),
  error_rate: new Rate('error_rate')
};

/**
 * Authenticates with the API and returns an authentication token
 * 
 * @param {Object} credentials - Object containing authentication credentials
 * @returns {string} Authentication token for API requests
 */
export function getAuthToken(credentials) {
  const authUrl = `${BASE_URL}/auth/token`;
  const payload = JSON.stringify(credentials);
  
  const response = http.post(authUrl, payload, {
    headers: DEFAULT_HEADERS
  });
  
  // Check if authentication was successful
  const success = check(response, {
    'Authentication successful': (r) => r.status === 200,
    'Token received': (r) => r.json('token') !== undefined
  });
  
  if (!success) {
    console.error(`Authentication failed: ${response.status} ${response.body}`);
    CUSTOM_METRICS.failed_requests.add(1);
    return null;
  }
  
  CUSTOM_METRICS.successful_requests.add(1);
  return response.json('token');
}

/**
 * Makes an authenticated HTTP request to the API
 * 
 * @param {string} method - HTTP method (GET, POST, PUT, DELETE, etc.)
 * @param {string} url - API endpoint URL (will be appended to BASE_URL)
 * @param {Object} payload - Request payload/body (optional)
 * @param {Object} params - URL parameters (optional)
 * @param {string} token - Authentication token (optional)
 * @returns {Object} HTTP response object
 */
export function makeAuthenticatedRequest(method, url, payload = null, params = null, token = null) {
  // Prepare headers
  const headers = { ...DEFAULT_HEADERS };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  // Prepare URL with parameters
  let fullUrl = `${BASE_URL}${url}`;
  if (params) {
    const searchParams = new URLSearchParams();
    for (const key in params) {
      searchParams.append(key, params[key]);
    }
    fullUrl += '?' + searchParams.toString();
  }
  
  // Format payload if provided
  const body = payload ? JSON.stringify(payload) : null;
  
  // Record start time for duration calculation
  const startTime = new Date().getTime();
  
  // Make request
  const response = method.toUpperCase() === 'GET' ? 
    http.get(fullUrl, { headers }) :
    http.request(method, fullUrl, body, { headers });
  
  // Calculate request duration
  const duration = new Date().getTime() - startTime;
  CUSTOM_METRICS.request_duration.add(duration);
  
  // Track metrics based on response
  if (response.status >= 200 && response.status < 300) {
    CUSTOM_METRICS.successful_requests.add(1);
    if (response.body) {
      CUSTOM_METRICS.data_throughput.add(response.body.length);
    }
  } else {
    CUSTOM_METRICS.failed_requests.add(1);
    CUSTOM_METRICS.error_rate.add(1);
  }
  
  return response;
}

/**
 * Generates test data for performance tests
 * 
 * @param {string} dataType - Type of test data to generate (e.g., 'user', 'pipeline', 'job')
 * @param {number} size - Size/amount of test data to generate
 * @returns {Object} Generated test data object
 */
export function generateTestData(dataType, size = 1) {
  let result;
  
  switch(dataType.toLowerCase()) {
    case 'user':
      result = generateUserData(size);
      break;
    case 'pipeline':
      result = generatePipelineData(size);
      break;
    case 'job':
      result = generateJobData(size);
      break;
    case 'quality':
      result = generateQualityData(size);
      break;
    case 'alert':
      result = generateAlertData(size);
      break;
    default:
      result = generateGenericData(size);
  }
  
  return result;
  
  // Helper functions for specific data types
  function generateUserData(count) {
    const users = [];
    for (let i = 0; i < count; i++) {
      users.push({
        username: `testuser_${Date.now()}_${i}`,
        email: `test_${Date.now()}_${i}@example.com`,
        firstName: `Test${i}`,
        lastName: `User${i}`,
        role: i % 3 === 0 ? 'admin' : 'user'
      });
    }
    return count === 1 ? users[0] : users;
  }
  
  function generatePipelineData(count) {
    const pipelines = [];
    for (let i = 0; i < count; i++) {
      pipelines.push({
        name: `test_pipeline_${Date.now()}_${i}`,
        source: `source_${i}`,
        destination: `destination_${i}`,
        schedule: '0 */2 * * *', // Every 2 hours
        enabled: true,
        config: {
          extractType: i % 2 === 0 ? 'full' : 'incremental',
          validateSchema: true,
          retryCount: 3
        }
      });
    }
    return count === 1 ? pipelines[0] : pipelines;
  }
  
  function generateJobData(count) {
    const jobs = [];
    const statuses = ['pending', 'running', 'completed', 'failed'];
    for (let i = 0; i < count; i++) {
      jobs.push({
        id: `job_${Date.now()}_${i}`,
        pipelineId: `pipeline_${i % 5}`,
        status: statuses[i % statuses.length],
        startTime: new Date(Date.now() - Math.floor(Math.random() * 3600000)).toISOString(),
        endTime: i % statuses.length === 2 ? new Date().toISOString() : null,
        records: Math.floor(Math.random() * 10000),
        errors: i % statuses.length === 3 ? Math.floor(Math.random() * 100) : 0
      });
    }
    return count === 1 ? jobs[0] : jobs;
  }
  
  function generateQualityData(count) {
    const qualityChecks = [];
    const statuses = ['passed', 'warning', 'failed'];
    for (let i = 0; i < count; i++) {
      qualityChecks.push({
        id: `quality_${Date.now()}_${i}`,
        jobId: `job_${i % 5}`,
        status: statuses[i % statuses.length],
        score: Math.floor(Math.random() * 100),
        rules: {
          total: 10,
          passed: Math.floor(Math.random() * 10),
          warning: Math.floor(Math.random() * 5),
          failed: Math.floor(Math.random() * 3)
        },
        timestamp: new Date().toISOString()
      });
    }
    return count === 1 ? qualityChecks[0] : qualityChecks;
  }
  
  function generateAlertData(count) {
    const alerts = [];
    const severities = ['critical', 'high', 'medium', 'low'];
    const types = ['quality', 'performance', 'availability', 'security'];
    for (let i = 0; i < count; i++) {
      alerts.push({
        id: `alert_${Date.now()}_${i}`,
        type: types[i % types.length],
        severity: severities[i % severities.length],
        message: `Test alert message ${i}`,
        source: `test_source_${i % 3}`,
        timestamp: new Date().toISOString(),
        acknowledged: false
      });
    }
    return count === 1 ? alerts[0] : alerts;
  }
  
  function generateGenericData(count) {
    const data = [];
    for (let i = 0; i < count; i++) {
      data.push({
        id: `data_${Date.now()}_${i}`,
        name: `Test Data ${i}`,
        value: Math.random() * 100,
        timestamp: new Date().toISOString(),
        attributes: {
          attr1: `value_${i}`,
          attr2: i % 2 === 0,
          attr3: Math.floor(Math.random() * 100)
        }
      });
    }
    return count === 1 ? data[0] : data;
  }
}

/**
 * Sets up the test environment before performance tests
 * 
 * @returns {Object} Environment configuration
 */
export function setupTestEnvironment() {
  // Initialize environment configuration
  const config = {
    startTime: new Date(),
    resources: {},
    cleanupTasks: []
  };
  
  console.log(`Test environment setup started at ${config.startTime.toISOString()}`);
  
  // Add cleanup task for tracking purposes
  const addCleanupTask = (task) => {
    config.cleanupTasks.push(task);
  };
  
  // Set up any required test data
  try {
    // For example, create test user if needed
    const testUser = generateTestData('user');
    const credentials = {
      username: testUser.username,
      password: 'TestPassword123'
    };
    
    const token = getAuthToken(credentials);
    if (token) {
      config.resources.authToken = token;
      console.log('Authentication token acquired for test environment');
    }
    
    // Create test pipelines if needed
    const testPipeline = generateTestData('pipeline');
    const response = makeAuthenticatedRequest(
      'POST',
      '/pipelines',
      testPipeline,
      null,
      config.resources.authToken
    );
    
    if (response.status === 201) {
      config.resources.testPipelineId = response.json('id');
      console.log(`Test pipeline created with ID: ${config.resources.testPipelineId}`);
      
      // Add cleanup task to remove test pipeline
      addCleanupTask(() => {
        console.log(`Cleaning up test pipeline: ${config.resources.testPipelineId}`);
        makeAuthenticatedRequest(
          'DELETE',
          `/pipelines/${config.resources.testPipelineId}`,
          null,
          null,
          config.resources.authToken
        );
      });
    }
  } catch (error) {
    console.error(`Error setting up test environment: ${error}`);
  }
  
  console.log('Test environment setup completed');
  return config;
}

/**
 * Cleans up the test environment after performance tests
 * 
 * @param {Object} environmentConfig - Environment configuration from setupTestEnvironment
 * @returns {boolean} Success status of teardown
 */
export function teardownTestEnvironment(environmentConfig) {
  console.log(`Test environment teardown started at ${new Date().toISOString()}`);
  console.log(`Test duration: ${(new Date() - environmentConfig.startTime) / 1000} seconds`);
  
  let success = true;
  
  // Execute cleanup tasks in reverse order
  for (let i = environmentConfig.cleanupTasks.length - 1; i >= 0; i--) {
    const task = environmentConfig.cleanupTasks[i];
    try {
      task();
    } catch (error) {
      console.error(`Error during cleanup task: ${error}`);
      success = false;
    }
  }
  
  console.log('Test environment teardown completed');
  return success;
}

/**
 * Validates an HTTP response against expected criteria
 * 
 * @param {Object} response - HTTP response object
 * @param {Object|number} expectedStatus - Expected status code or object with check criteria
 * @param {string} checkName - Name for the check group
 * @returns {boolean} Whether the response meets the expected criteria
 */
export function checkResponse(response, expectedStatus = 200, checkName = 'Response validation') {
  let checkResult;
  
  if (typeof expectedStatus === 'number') {
    // Simple status code check
    checkResult = check(response, {
      [`${checkName} - Status is ${expectedStatus}`]: (r) => r.status === expectedStatus
    });
  } else {
    // Use provided check object
    checkResult = check(response, expectedStatus);
  }
  
  // Update metrics based on check results
  if (checkResult) {
    CUSTOM_METRICS.successful_checks.add(1);
  } else {
    CUSTOM_METRICS.error_rate.add(1);
  }
  
  return checkResult;
}

/**
 * Adds a random sleep interval to simulate realistic user behavior
 * 
 * @param {number} minSeconds - Minimum sleep duration in seconds
 * @param {number} maxSeconds - Maximum sleep duration in seconds
 */
export function randomSleep(minSeconds = 1, maxSeconds = 5) {
  const sleepTime = minSeconds + Math.random() * (maxSeconds - minSeconds);
  sleep(sleepTime);
}

/**
 * Logs performance metrics for analysis
 * 
 * @param {Object} metrics - Metrics object to log
 * @param {string} label - Label for the metrics
 */
export function logMetrics(metrics, label = 'Performance metrics') {
  const timestamp = new Date().toISOString();
  const formattedMetrics = {
    timestamp,
    label,
    ...metrics
  };
  
  console.log(JSON.stringify(formattedMetrics, null, 2));
}

/**
 * Formats a payload object for API requests
 * 
 * @param {Object} data - Data to format
 * @returns {string} Formatted payload string
 */
export function formatPayload(data) {
  if (!data) return null;
  
  if (typeof data === 'string') {
    return data; // Already a string
  }
  
  try {
    return JSON.stringify(data);
  } catch (error) {
    console.error(`Error formatting payload: ${error}`);
    return null;
  }
}

/**
 * Parses an API response into a usable format
 * 
 * @param {Object} response - HTTP response object
 * @returns {Object} Parsed response data
 */
export function parseResponse(response) {
  if (!response) return null;
  
  try {
    // Check if response has valid body
    if (!response.body) {
      return { success: false, message: 'Empty response body' };
    }
    
    // Try to parse as JSON
    try {
      return response.json();
    } catch (e) {
      // If not JSON, return text body
      return { 
        success: response.status >= 200 && response.status < 300,
        message: response.body,
        status: response.status
      };
    }
  } catch (error) {
    console.error(`Error parsing response: ${error}`);
    return { success: false, error: error.message };
  }
}

/**
 * Calculates performance metrics from test results
 * 
 * @param {Array} results - Array of test results
 * @returns {Object} Calculated metrics
 */
export function calculateMetrics(results) {
  if (!results || !results.length) {
    return {
      count: 0,
      empty: true
    };
  }
  
  // Extract numeric values from results for statistical calculations
  const extractNumericValue = (result, key) => {
    const value = result[key];
    return typeof value === 'number' ? value : null;
  };
  
  const getNumericValues = (key) => {
    return results
      .map(r => extractNumericValue(r, key))
      .filter(v => v !== null);
  };
  
  // Calculate basic statistics for a set of values
  const calculateStats = (values) => {
    if (!values || !values.length) return null;
    
    values.sort((a, b) => a - b);
    
    const sum = values.reduce((acc, val) => acc + val, 0);
    const count = values.length;
    const min = values[0];
    const max = values[count - 1];
    const avg = sum / count;
    
    // Calculate percentiles
    const p50 = values[Math.floor(count * 0.5)];
    const p90 = values[Math.floor(count * 0.9)];
    const p95 = values[Math.floor(count * 0.95)];
    const p99 = values[Math.floor(count * 0.99)];
    
    return {
      count,
      min,
      max,
      avg,
      p50,
      p90,
      p95,
      p99
    };
  };
  
  // Calculate metrics for response times
  const responseTimes = getNumericValues('responseTime');
  const responseTimeStats = calculateStats(responseTimes);
  
  // Calculate success/failure rates
  const totalRequests = results.length;
  const successfulRequests = results.filter(r => r.status >= 200 && r.status < 300).length;
  const failedRequests = totalRequests - successfulRequests;
  const successRate = totalRequests > 0 ? (successfulRequests / totalRequests) * 100 : 0;
  
  // Calculate throughput
  const timeRange = results.length > 1 ? 
    (results[results.length - 1].timestamp - results[0].timestamp) / 1000 : // in seconds
    1; // default to 1 second if only one result
  
  const requestsPerSecond = totalRequests / timeRange;
  
  // Calculate data throughput if available
  const dataVolumes = getNumericValues('dataSize');
  const totalDataVolume = dataVolumes.reduce((acc, val) => acc + val, 0);
  const dataVolumeStats = calculateStats(dataVolumes);
  
  return {
    timestamp: new Date().toISOString(),
    duration: {
      totalTimeSeconds: timeRange,
      rps: requestsPerSecond
    },
    requests: {
      total: totalRequests,
      successful: successfulRequests,
      failed: failedRequests,
      successRate: successRate
    },
    responseTimes: responseTimeStats,
    dataVolume: dataVolumeStats ? {
      ...dataVolumeStats,
      total: totalDataVolume,
      bytesPerSecond: timeRange > 0 ? totalDataVolume / timeRange : 0
    } : null
  };
}