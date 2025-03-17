import { defineConfig, devices } from '@playwright/test'; // v1.33.0
import path from 'path'; // built-in
import dotenv from 'dotenv'; // v16.0.3

// Load environment-specific configuration
const env = process.env.NODE_ENV || 'development';
const envFiles = [
  '.env.local',
  `.env.${env}.local`,
  `.env.${env}`,
  '.env'
];

// Load environment variables from .env files
envFiles.forEach(file => {
  dotenv.config({ path: path.resolve(process.cwd(), file), override: false });
});

// Global variables
const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';
const API_URL = process.env.API_URL || 'http://localhost:8080/api';
const CI = process.env.CI === 'true';

/**
 * Playwright configuration for end-to-end testing of the self-healing data pipeline
 * @see https://playwright.dev/docs/test-configuration
 */
export default defineConfig({
  // Test directory and output paths
  testDir: './playwright/tests',
  outputDir: '../../reports/e2e/playwright',
  
  // Timeout settings
  timeout: 30000,
  expect: {
    timeout: 5000,
    toHaveScreenshot: {
      maxDiffPixels: 100,
    },
    toMatchSnapshot: {
      maxDiffPixelRatio: 0.05,
    },
  },
  
  // CI vs local environment specific settings
  forbidOnly: CI, // Fail if test.only is used in CI
  retries: CI ? 3 : 1, // Different retry strategy based on environment
  workers: CI ? 2 : '50%', // Limited workers in CI environment
  
  // Reporting configuration - different for CI vs local
  reporter: CI 
    ? [
        ['github'],
        ['html', { outputFolder: '../../reports/e2e/playwright/html-report' }],
        ['json', { outputFile: '../../reports/e2e/playwright/results.json' }],
      ] 
    : [
        ['html', { outputFolder: '../../reports/e2e/playwright/html-report' }],
        ['json', { outputFile: '../../reports/e2e/playwright/results.json' }],
        ['list'],
      ],
  
  // Global test settings
  use: {
    baseURL: BASE_URL,
    actionTimeout: 15000,
    navigationTimeout: 30000,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'on-first-retry',
  },
  
  // Browser projects configuration for cross-browser testing
  projects: [
    {
      name: 'chromium',
      use: {
        browserName: 'chromium',
        viewport: { width: 1280, height: 720 },
        ignoreHTTPSErrors: true,
        permissions: ['geolocation'],
      },
    },
    {
      name: 'firefox',
      use: {
        browserName: 'firefox',
        viewport: { width: 1280, height: 720 },
      },
    },
    {
      name: 'webkit',
      use: {
        browserName: 'webkit',
        viewport: { width: 1280, height: 720 },
      },
    },
    {
      name: 'mobile-chrome',
      use: {
        browserName: 'chromium',
        ...devices['Pixel 5'],
      },
    },
    {
      name: 'mobile-safari',
      use: {
        browserName: 'webkit',
        ...devices['iPhone 12'],
      },
    },
  ],
  
  // Local web server configuration for testing
  webServer: {
    command: 'cd ../../web && npm run preview',
    url: BASE_URL,
    reuseExistingServer: true,
    timeout: 120000,
  },
});