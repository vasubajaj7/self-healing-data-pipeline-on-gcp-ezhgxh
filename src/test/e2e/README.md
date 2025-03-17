# End-to-End Testing Framework

## Overview

This directory contains the end-to-end (E2E) testing framework for the Self-Healing Data Pipeline. These tests validate the complete pipeline functionality from UI operations through to data processing and recovery mechanisms, ensuring the system behaves as expected from a user's perspective.

Our E2E testing approach focuses on:

- Validating complete pipeline flows from end to end
- Testing self-healing capabilities through error injection
- Verifying UI components and user workflows
- Ensuring data quality validation processes work correctly
- Confirming alert and notification systems function properly

## Test Frameworks

We utilize two complementary testing frameworks to ensure comprehensive coverage:

### Cypress (v10.11.0)

Cypress is primarily used for testing the web interface components including:
- Dashboard visualizations
- Configuration screens
- Alert management interfaces
- Data quality validation UI

Cypress provides excellent developer experience with time-travel debugging, real-time reloads, and automatic waiting, making it ideal for UI-focused tests.

### Playwright (v1.32.3)

Playwright is used for testing complex scenarios that require:
- Multi-tab or multi-browser testing
- Advanced network interception and mocking
- Testing across different browser engines (Chromium, Firefox, WebKit)
- Complex authentication flows
- Performance and resource monitoring

We use Playwright for scenarios that validate end-to-end data processing pipelines where we need to simulate data inputs and verify downstream effects across multiple systems.

## Directory Structure

```
src/test/e2e/
├── cypress/
│   ├── fixtures/            # Test data for Cypress tests
│   ├── integration/         # Cypress test specifications
│   │   ├── dashboard/       # Tests for dashboard functionality
│   │   ├── data-quality/    # Tests for data quality features
│   │   ├── self-healing/    # Tests for self-healing capabilities
│   │   └── alerts/          # Tests for alerting functionality
│   ├── plugins/             # Cypress plugins
│   ├── support/             # Helper functions and commands
│   │   ├── commands.ts      # Custom Cypress commands
│   │   └── page-objects/    # Page object implementations
│   └── tsconfig.json        # TypeScript configuration for Cypress
├── playwright/
│   ├── fixtures/            # Test data and page objects for Playwright
│   │   ├── test-data.ts     # Test data utilities
│   │   └── page-objects/    # Page object implementations
│   ├── tests/               # Playwright test specifications
│   │   ├── pipeline-flows/  # End-to-end pipeline flow tests
│   │   ├── error-recovery/  # Error injection and recovery tests
│   │   └── integration/     # System integration tests
│   ├── utils/               # Helper utilities
│   └── playwright.config.ts # Playwright configuration
├── shared/                  # Shared utilities between frameworks
│   ├── auth/                # Authentication utilities
│   ├── data-generators/     # Test data generation utilities
│   └── constants.ts         # Shared constants
├── cypress.json             # Cypress configuration
├── package.json             # Dependencies and scripts
└── README.md                # This documentation file
```

## Setup Instructions

### Prerequisites

- Node.js 16.x or higher
- npm 8.x or higher
- Access to the development or test environment

### Installation Steps

1. Clone the repository (if not already done):
   ```bash
   git clone https://github.com/your-org/self-healing-data-pipeline.git
   cd self-healing-data-pipeline
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Set up environment configuration:
   ```bash
   cp .env.example .env.test
   ```

4. Update the `.env.test` file with appropriate test environment values:
   ```
   BASE_URL=https://test-environment-url
   TEST_USERNAME=test-user
   TEST_PASSWORD=test-password
   TEST_API_KEY=your-api-key
   ```

5. Install browser dependencies for Playwright:
   ```bash
   npx playwright install
   ```

## Running Tests

### Running Cypress Tests

```bash
# Run all Cypress tests
npm run cypress:run

# Run specific test file
npm run cypress:run -- --spec "cypress/integration/dashboard.spec.ts"

# Run tests with UI
npm run cypress:open

# Run tests with a specific browser
npm run cypress:run -- --browser chrome

# Run tests with a specific configuration
npm run cypress:run -- --config baseUrl=https://staging-environment
```

### Running Playwright Tests

```bash
# Run all Playwright tests
npm run playwright:test

# Run specific test file
npm run playwright:test tests/pipeline-flows/data-ingestion.spec.ts

# Run tests with UI
npm run playwright:ui

# Run tests with a specific browser
npm run playwright:test --project=chromium

# Run tests with specific tag(s)
npm run playwright:test --grep @smoke
```

### Running in CI/CD

Tests are automatically executed in the CI/CD pipeline on pull requests and before deployment to staging and production environments. See the [CI/CD Integration](#cicd-integration) section for more details.

## Test Scenarios

Our E2E test suite covers the following key scenarios:

### Happy Path Scenarios

- **Data Ingestion Flow**: Verifies the complete data ingestion process from source to BigQuery
- **Pipeline Creation and Execution**: Tests creating a new pipeline and executing it successfully
- **Data Quality Validation**: Ensures data quality rules are applied correctly
- **Monitoring Dashboard**: Validates pipeline health metrics display correctly
- **Alert Configuration**: Tests creation and management of alert rules

### Error Recovery Scenarios

- **Source Connectivity Failures**: Injects connection failures and verifies self-healing
- **Data Quality Issues**: Introduces data quality problems and validates correction
- **Pipeline Execution Failures**: Triggers pipeline failures and verifies recovery
- **Resource Constraint Handling**: Tests behavior under resource limitations
- **Schema Drift Detection**: Validates schema changes are detected and handled

### Security and Authentication Scenarios

- **User Authentication Flows**: Tests login, session management, and permissions
- **API Authentication**: Verifies API key authentication and token refreshing
- **Access Control**: Tests role-based access controls function properly

## Page Object Pattern

We implement the Page Object Pattern to improve test maintainability and readability. Page objects encapsulate page behaviors and selectors, allowing tests to focus on workflows rather than implementation details.

### Example Page Object (Playwright)

```typescript
// Example from playwright/fixtures/page-objects/dashboardPage.ts
export class DashboardPage {
  constructor(private page: Page) {}

  async navigate() {
    await this.page.goto('/dashboard');
  }

  async getPipelineHealthStatus() {
    return this.page.locator('[data-testid="pipeline-health"]').textContent();
  }

  async getDataQualityScore() {
    return this.page.locator('[data-testid="quality-score"]').textContent();
  }

  async clickPipelineDetails(pipelineId: string) {
    await this.page.click(`[data-pipeline-id="${pipelineId}"]`);
  }

  async waitForRefresh() {
    await this.page.waitForSelector('[data-testid="last-updated"]', { state: 'visible' });
  }
}
```

### Using Page Objects in Tests

```typescript
// Example test using the page object
test('dashboard shows correct pipeline health', async ({ page }) => {
  const dashboardPage = new DashboardPage(page);
  
  await dashboardPage.navigate();
  
  // Verify health indicators
  const healthStatus = await dashboardPage.getPipelineHealthStatus();
  expect(healthStatus).toContain('98% Healthy');
  
  const qualityScore = await dashboardPage.getDataQualityScore();
  expect(qualityScore).toContain('94%');
});
```

## Test Data Management

We follow these principles for test data management:

1. **Isolated Test Data**: Each test creates or uses its own isolated test data to prevent interference between tests.

2. **Fixtures**: Static test data is stored in fixture files:
   - JSON fixtures for Cypress (`cypress/fixtures/`)
   - TypeScript fixtures for Playwright (`playwright/fixtures/test-data.ts`)

3. **Data Generators**: Dynamic test data is created using data generators in `shared/data-generators/`.

4. **Test Data Cleanup**: Tests are responsible for cleaning up test data they create, typically in `afterEach` or `afterAll` hooks.

5. **Environment-Specific Data**: Environment-specific test data is configured through environment variables.

### Example Data Generator

```typescript
// shared/data-generators/pipeline-generator.ts
export function generateTestPipeline(overrides = {}) {
  return {
    id: `test-pipeline-${Date.now()}`,
    name: `Test Pipeline ${Date.now()}`,
    source: 'gcs',
    sourceConfig: {
      bucket: 'test-data-bucket',
      path: 'test-data/sales/',
      filePattern: '*.csv'
    },
    destination: 'bigquery',
    destinationConfig: {
      dataset: 'test_dataset',
      table: 'test_sales_data'
    },
    schedule: '0 */4 * * *', // Every 4 hours
    owner: 'test-user@example.com',
    ...overrides
  };
}
```

## Authentication Handling

Authentication is handled differently in each framework:

### Cypress Authentication

We use Cypress' `cy.session` to cache authentication between tests:

```typescript
// cypress/support/commands.ts
Cypress.Commands.add('login', (username, password) => {
  cy.session([username, password], () => {
    cy.visit('/login');
    cy.get('#username').type(username);
    cy.get('#password').type(password);
    cy.get('button[type="submit"]').click();
    cy.url().should('include', '/dashboard');
  });
});
```

### Playwright Authentication

We use Playwright's storage state feature to preserve authentication:

```typescript
// playwright/utils/auth.ts
async function login(page: Page, username: string, password: string) {
  await page.goto('/login');
  await page.fill('#username', username);
  await page.fill('#password', password);
  await page.click('button[type="submit"]');
  await page.waitForURL('**/dashboard');
  
  // Store authentication state
  const storageState = await page.context().storageState();
  await fs.writeFile('./playwright/.auth/user.json', JSON.stringify(storageState));
}

// In playwright.config.ts
const config: PlaywrightTestConfig = {
  use: {
    baseURL: process.env.BASE_URL,
    storageState: './playwright/.auth/user.json',
  },
  // ...
};
```

## CI/CD Integration

Our E2E tests are integrated into the CI/CD pipeline to ensure quality at each deployment stage:

### GitHub Actions Workflow

```yaml
# .github/workflows/e2e-tests.yml (simplified)
name: E2E Tests

on:
  pull_request:
    branches: [ main, develop ]
  push:
    branches: [ main, develop ]

jobs:
  cypress-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: 16
      - name: Install dependencies
        run: npm ci
      - name: Run Cypress tests
        run: npm run cypress:run

  playwright-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: 16
      - name: Install dependencies
        run: npm ci
      - name: Install Playwright browsers
        run: npx playwright install --with-deps
      - name: Run Playwright tests
        run: npm run playwright:test
      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: playwright-results
          path: playwright-report/
```

### Deployment Gates

E2E tests serve as quality gates in the deployment process:

1. **PR Checks**: Basic smoke tests run on pull requests
2. **Development Deployment**: Full suite runs after deployment to development
3. **Staging Verification**: Critical path tests run before promoting to staging
4. **Production Validation**: Smoke tests run before final production deployment

### Reporting

Test results are captured and reported in multiple ways:

- GitHub Actions summary and artifacts
- Detailed HTML reports stored as build artifacts
- Integration with test management system (TestRail)
- Slack notifications for test failures

## Best Practices

Follow these guidelines when writing and maintaining E2E tests:

### Test Structure

1. **Arrange-Act-Assert**: Structure tests with clear setup, action, and verification phases
2. **Single Responsibility**: Each test should verify one specific behavior
3. **Independence**: Tests should not depend on other tests or specific test execution order
4. **Readability**: Use descriptive test names and comments to clarify intent

### Selectors

1. **Data Attributes**: Use `data-testid` attributes for test selectors
2. **Resilient Selectors**: Avoid brittle selectors like CSS classes that may change
3. **Abstractions**: Encapsulate selectors in page objects

### Performance

1. **Test Isolation**: Reset application state between tests
2. **Parallel Execution**: Design tests to run in parallel
3. **Minimal Setup**: Perform only necessary setup for each test
4. **Targeted Scope**: Use component or API tests for detailed behavior, E2E for critical paths

### Maintenance

1. **Regular Updates**: Update tests when application behavior changes
2. **Flaky Test Management**: Tag and prioritize fixing flaky tests
3. **Documentation**: Document complex test scenarios and custom utilities
4. **Review Changes**: Include test modifications in code reviews

## Troubleshooting

### Common Issues and Solutions

#### Tests Fail When Running in CI But Pass Locally

- **Problem**: Environment differences between local and CI environments
- **Solution**: Ensure environment variables are set correctly in CI. Use the same browser versions.

#### Flaky Tests (Intermittently Failing)

- **Problem**: Race conditions, timing issues, or external dependencies
- **Solution**: Add explicit waits, improve selector specificity, or implement retry logic.

#### Authentication Issues

- **Problem**: Session expiration or authentication failure
- **Solution**: Check token expiration times, implement proper auth state handling.

#### Slow Test Execution

- **Problem**: Tests take too long to execute
- **Solution**: Run tests in parallel, optimize setup/teardown, use targeted selectors.

### Debugging Tips

1. **Visual Debugging**:
   - Use `cypress open` for Cypress visual debugging
   - Use `playwright:ui` for Playwright visual debugging

2. **Logging**:
   - Add `cy.log()` statements in Cypress
   - Use `console.log()` with `DEBUG=pw:api` environment variable in Playwright

3. **Screenshots and Videos**:
   - Review failure screenshots in CI artifacts
   - Check video recordings of test runs

4. **Network Monitoring**:
   - Use network interception to debug API issues
   - Monitor network requests during test execution

## References

- [Cypress Documentation](https://docs.cypress.io/)
- [Playwright Documentation](https://playwright.dev/docs/intro)
- [Page Object Pattern](https://martinfowler.com/bliki/PageObject.html)
- [Internal Testing Guide](https://internal-docs.example.com/testing-standards)