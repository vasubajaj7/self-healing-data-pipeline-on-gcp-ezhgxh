import { test, expect } from '@playwright/test';
import QualityPage from '../fixtures/page-objects/qualityPage';
import { mockApiResponses } from '../fixtures/test-data';

// Test suite for the main data quality dashboard functionality
test.describe('Data Quality Dashboard', () => {
  test('should display the quality dashboard with correct title', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    await expect(authenticatedPage.locator('h1:has-text("Data Quality")')).toBeVisible();
  });

  test('should display all required quality dashboard components', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Verify all key dashboard components are visible
    await expect(authenticatedPage.locator(qualityPage.qualityScoreChart)).toBeVisible();
    await expect(authenticatedPage.locator(qualityPage.datasetQualityTable)).toBeVisible();
    await expect(authenticatedPage.locator(qualityPage.qualityDimensionsCard)).toBeVisible();
    await expect(authenticatedPage.locator(qualityPage.validationRulesTable)).toBeVisible({ timeout: 15000 });
    await expect(authenticatedPage.locator(qualityPage.validationIssuesTable)).toBeVisible({ timeout: 15000 });
    await expect(authenticatedPage.locator(qualityPage.qualityTrendChart)).toBeVisible({ timeout: 15000 });
    await expect(authenticatedPage.locator(qualityPage.failingRulesCard)).toBeVisible();
  });

  test('should display correct quality score', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    
    // Mock quality API response with specific score
    await authenticatedPage.route('**/api/quality/list', (route) => {
      const mockResponse = mockApiResponses.quality.list;
      // Ensure first dataset has a known quality score
      mockResponse.data.datasets[0].qualityScore = 98;
      route.fulfill({ json: mockResponse });
    });
    
    await qualityPage.waitForQualityPageLoaded();
    
    // Get quality score from the UI
    const qualityScore = await qualityPage.getQualityScore();
    
    // Verify it matches the expected value
    expect(qualityScore).toContain('98');
  });

  test('should display correct quality dimensions breakdown', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    
    // Mock quality API with specific dimensions data
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const url = route.request().url();
      if (url.includes('/api/quality/details')) {
        const mockResponse = { ...mockApiResponses.quality.details };
        mockResponse.data.dataset = {
          ...mockResponse.data.dataset,
          qualityDimensions: {
            completeness: 96,
            accuracy: 92,
            consistency: 94,
            timeliness: 99
          }
        };
        route.fulfill({ json: mockResponse });
      } else {
        route.fulfill({ json: mockApiResponses.quality.list });
      }
    });
    
    await qualityPage.waitForQualityPageLoaded();
    
    // Get dimensions breakdown
    const dimensions = await qualityPage.getQualityDimensions();
    
    // Verify dimensions match expected values
    expect(dimensions['Completeness']).toContain('96');
    expect(dimensions['Accuracy']).toContain('92');
    expect(dimensions['Consistency']).toContain('94');
    expect(dimensions['Timeliness']).toContain('99');
  });

  test('should display dataset quality summaries correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Get dataset quality summaries
    const summaries = await qualityPage.getDatasetQualitySummaries();
    
    // Verify count and content
    expect(summaries.length).toBeGreaterThan(0);
    expect(summaries[0].dataset).toBeTruthy();
    expect(summaries[0].qualityScore).toBeTruthy();
    expect(summaries[0].trend).toBeTruthy();
    expect(summaries[0].issueCount).toBeTruthy();
  });

  test('should update dashboard when dataset is selected', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Get initial quality score
    const initialScore = await qualityPage.getQualityScore();
    
    // Mock updated response for new dataset selection
    await authenticatedPage.route('**/api/quality/details**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.details };
      mockResponse.data.dataset = {
        ...mockResponse.data.dataset,
        name: 'product_catalog',
        qualityScore: 100 // Different score than initial
      };
      route.fulfill({ json: mockResponse });
    });
    
    // Select a different dataset
    await qualityPage.selectDataset('product_catalog');
    
    // Get updated quality score
    const updatedScore = await qualityPage.getQualityScore();
    
    // Verify score has been updated
    expect(updatedScore).not.toEqual(initialScore);
    expect(updatedScore).toContain('100');
  });

  test('should update dashboard when table is selected', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Get initial quality metrics
    const initialScore = await qualityPage.getQualityScore();
    
    // Mock updated response for new table selection
    await authenticatedPage.route('**/api/quality/details**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.details };
      mockResponse.data.dataset = {
        ...mockResponse.data.dataset,
        tableName: 'orders',
        qualityScore: 95 // Different score
      };
      route.fulfill({ json: mockResponse });
    });
    
    // Select a different table
    await qualityPage.selectTable('orders');
    
    // Get updated quality score
    const updatedScore = await qualityPage.getQualityScore();
    
    // Verify score has been updated
    expect(updatedScore).not.toEqual(initialScore);
    expect(updatedScore).toContain('95');
  });

  test('should update dashboard when time range is changed', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Get initial dataset summaries
    const initialSummaries = await qualityPage.getDatasetQualitySummaries();
    
    // Mock updated response for new time range
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Modify dataset count to verify change
      mockResponse.data.datasets = mockResponse.data.datasets.slice(0, 3);
      route.fulfill({ json: mockResponse });
    });
    
    // Select a different time range
    await qualityPage.selectTimeRange('7 days');
    
    // Get updated dataset summaries
    const updatedSummaries = await qualityPage.getDatasetQualitySummaries();
    
    // Verify data has been updated
    expect(updatedSummaries.length).not.toEqual(initialSummaries.length);
    expect(updatedSummaries.length).toEqual(3);
  });

  test('should refresh dashboard when refresh button is clicked', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Get initial last refreshed time
    const initialRefreshedTime = await qualityPage.getLastRefreshedTime();
    
    // Mock updated response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      route.fulfill({ json: mockApiResponses.quality.list });
    });
    
    // Let time pass to ensure new refresh time
    await authenticatedPage.waitForTimeout(1000);
    
    // Click refresh button
    await qualityPage.clickRefresh();
    
    // Get updated last refreshed time
    const updatedRefreshedTime = await qualityPage.getLastRefreshedTime();
    
    // Verify refresh time has been updated
    expect(updatedRefreshedTime).not.toEqual(initialRefreshedTime);
  });
});

// Test suite for the validation rules functionality
test.describe('Quality Validation Rules', () => {
  test('should display validation rules correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Get validation rules
    const rules = await qualityPage.getValidationRules();
    
    // Verify rules are displayed correctly
    expect(rules.length).toBeGreaterThan(0);
    expect(rules[0].name).toBeTruthy();
    expect(rules[0].type).toBeTruthy();
    expect(rules[0].dimension).toBeTruthy();
    expect(rules[0].successRate).toBeTruthy();
  });

  test('should filter rules by type correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Get initial rule count
    const initialRules = await qualityPage.getValidationRules();
    
    // Mock filtered response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter rules to only include schema type
      const filteredRules = mockResponse.data.rules?.filter(rule => rule.ruleType === 'SCHEMA') || [];
      mockResponse.data.rules = filteredRules;
      route.fulfill({ json: mockResponse });
    });
    
    // Filter by schema type
    await qualityPage.filterRulesByType('Schema');
    
    // Get filtered rules
    const filteredRules = await qualityPage.getValidationRules();
    
    // Verify filtering worked
    expect(filteredRules.length).toBeLessThan(initialRules.length);
    filteredRules.forEach(rule => {
      expect(rule.type.toLowerCase()).toContain('schema');
    });
  });

  test('should filter rules by dimension correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Get initial rule count
    const initialRules = await qualityPage.getValidationRules();
    
    // Mock filtered response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter rules to only include Completeness dimension
      const filteredRules = mockResponse.data.rules?.filter(rule => 
        rule.dimension?.toLowerCase() === 'completeness'
      ) || [];
      mockResponse.data.rules = filteredRules;
      route.fulfill({ json: mockResponse });
    });
    
    // Filter by Completeness dimension
    await qualityPage.filterByDimension('Completeness');
    
    // Get filtered rules
    const filteredRules = await qualityPage.getValidationRules();
    
    // Verify filtering worked
    expect(filteredRules.length).toBeLessThan(initialRules.length);
    filteredRules.forEach(rule => {
      expect(rule.dimension.toLowerCase()).toContain('completeness');
    });
  });

  test('should search rules correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Get initial rule count
    const initialRules = await qualityPage.getValidationRules();
    
    // Determine a search term from the first rule
    const searchTerm = initialRules[0].name.split('_')[0];
    
    // Mock search response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter rules to only include rules with search term in name
      const filteredRules = mockResponse.data.rules?.filter(rule => 
        rule.name.toLowerCase().includes(searchTerm.toLowerCase())
      ) || [];
      mockResponse.data.rules = filteredRules;
      route.fulfill({ json: mockResponse });
    });
    
    // Search for the term
    await qualityPage.search(searchTerm);
    
    // Get search results
    const searchResults = await qualityPage.getValidationRules();
    
    // Verify search worked
    expect(searchResults.length).toBeLessThan(initialRules.length);
    searchResults.forEach(rule => {
      expect(rule.name.toLowerCase()).toContain(searchTerm.toLowerCase());
    });
  });

  test('should open rule editor when Add Rule button is clicked', async ({ adminPage }) => {
    const qualityPage = new QualityPage(adminPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Click Add Rule button
    await qualityPage.clickAddRule();
    
    // Verify rule editor modal is displayed
    await expect(adminPage.locator(qualityPage.ruleEditorModal)).toBeVisible();
  });

  test('should open rule editor with rule data when Edit Rule is clicked', async ({ adminPage }) => {
    const qualityPage = new QualityPage(adminPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Click edit button for first rule
    await qualityPage.clickEditRule(0);
    
    // Verify rule editor modal is displayed with rule data
    await expect(adminPage.locator(qualityPage.ruleEditorModal)).toBeVisible();
    // Verify rule data is loaded in the modal
    await expect(adminPage.locator(`${qualityPage.ruleEditorModal} input[type="text"]`).first()).toHaveValue();
  });
});

// Test suite for the quality issues functionality
test.describe('Quality Issues', () => {
  test('should display quality issues correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to issues tab
    await qualityPage.clickTab('Issues');
    
    // Get quality issues
    const issues = await qualityPage.getQualityIssues();
    
    // Verify issues are displayed correctly
    expect(issues.length).toBeGreaterThan(0);
    expect(issues[0].id).toBeTruthy();
    expect(issues[0].description).toBeTruthy();
    expect(issues[0].severity).toBeTruthy();
    expect(issues[0].status).toBeTruthy();
  });

  test('should filter issues by severity correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to issues tab
    await qualityPage.clickTab('Issues');
    
    // Get initial issue count
    const initialIssues = await qualityPage.getQualityIssues();
    
    // Mock filtered response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter issues to only include High severity
      const filteredIssues = mockResponse.data.issues?.filter(issue => 
        issue.severity === 'HIGH'
      ) || [];
      mockResponse.data.issues = filteredIssues;
      route.fulfill({ json: mockResponse });
    });
    
    // Filter by High severity
    await qualityPage.filterIssuesBySeverity('High');
    
    // Get filtered issues
    const filteredIssues = await qualityPage.getQualityIssues();
    
    // Verify filtering worked
    expect(filteredIssues.length).toBeLessThan(initialIssues.length);
    filteredIssues.forEach(issue => {
      expect(issue.severity.toLowerCase()).toBe('high');
    });
  });

  test('should filter issues by status correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to issues tab
    await qualityPage.clickTab('Issues');
    
    // Get initial issue count
    const initialIssues = await qualityPage.getQualityIssues();
    
    // Mock filtered response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter issues to only include Open status
      const filteredIssues = mockResponse.data.issues?.filter(issue => 
        issue.status === 'OPEN'
      ) || [];
      mockResponse.data.issues = filteredIssues;
      route.fulfill({ json: mockResponse });
    });
    
    // Filter by Open status
    await qualityPage.filterByStatus('Open');
    
    // Get filtered issues
    const filteredIssues = await qualityPage.getQualityIssues();
    
    // Verify filtering worked
    expect(filteredIssues.length).toBeLessThan(initialIssues.length);
    filteredIssues.forEach(issue => {
      expect(issue.status.toLowerCase()).toBe('open');
    });
  });

  test('should search issues correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to issues tab
    await qualityPage.clickTab('Issues');
    
    // Get initial issue count
    const initialIssues = await qualityPage.getQualityIssues();
    
    // Determine a search term from the first issue
    const searchTerm = initialIssues[0].description.split(' ')[0];
    
    // Mock search response
    await authenticatedPage.route('**/api/quality/**', (route) => {
      const mockResponse = { ...mockApiResponses.quality.list };
      // Filter issues to only include issues with search term in description
      const filteredIssues = mockResponse.data.issues?.filter(issue => 
        issue.description.toLowerCase().includes(searchTerm.toLowerCase())
      ) || [];
      mockResponse.data.issues = filteredIssues;
      route.fulfill({ json: mockResponse });
    });
    
    // Search for the term
    await qualityPage.search(searchTerm);
    
    // Get search results
    const searchResults = await qualityPage.getQualityIssues();
    
    // Verify search worked
    expect(searchResults.length).toBeLessThan(initialIssues.length);
    searchResults.forEach(issue => {
      expect(issue.description.toLowerCase()).toContain(searchTerm.toLowerCase());
    });
  });

  test('should open issue details when issue is clicked', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to issues tab
    await qualityPage.clickTab('Issues');
    
    // Click on first issue
    await qualityPage.clickIssue(0);
    
    // Verify issue detail modal is displayed
    await expect(authenticatedPage.locator(qualityPage.issueDetailModal)).toBeVisible();
  });
});

// Test suite for the quality trends functionality
test.describe('Quality Trends', () => {
  test('should display quality trend chart correctly', async ({ authenticatedPage }) => {
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to trends tab
    await qualityPage.clickTab('Trends');
    
    // Verify trend chart is visible
    await expect(authenticatedPage.locator(qualityPage.qualityTrendChart)).toBeVisible();
    
    // Get trend data
    const trendData = await qualityPage.getQualityTrendData();
    
    // Verify trend data exists
    expect(trendData.length).toBeGreaterThan(0);
  });
});

// Test suite for quality dashboard access with different user roles
test.describe('Quality Dashboard Access Control', () => {
  test('should show all quality components for admin users', async ({ adminPage }) => {
    const qualityPage = new QualityPage(adminPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Verify add button is visible for admin
    const addButtonVisible = await qualityPage.isAddRuleButtonVisible();
    expect(addButtonVisible).toBe(true);
    
    // Verify edit buttons are visible for admin
    const editButtonsVisible = await qualityPage.areEditRuleButtonsVisible();
    expect(editButtonsVisible).toBe(true);
  });

  test('should show appropriate quality components for engineer users', async ({ engineerPage }) => {
    const qualityPage = new QualityPage(engineerPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Verify add button is visible for engineers
    const addButtonVisible = await qualityPage.isAddRuleButtonVisible();
    expect(addButtonVisible).toBe(true);
    
    // Verify edit buttons are visible for engineers
    const editButtonsVisible = await qualityPage.areEditRuleButtonsVisible();
    expect(editButtonsVisible).toBe(true);
  });

  test('should show appropriate quality components for analyst users', async ({ analystPage }) => {
    const qualityPage = new QualityPage(analystPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Verify add button is not visible for analysts
    const addButtonVisible = await qualityPage.isAddRuleButtonVisible();
    expect(addButtonVisible).toBe(false);
    
    // Verify edit buttons are not visible for analysts
    const editButtonsVisible = await qualityPage.areEditRuleButtonsVisible();
    expect(editButtonsVisible).toBe(false);
  });

  test('should show appropriate quality components for operator users', async ({ operatorPage }) => {
    const qualityPage = new QualityPage(operatorPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Navigate to rules tab
    await qualityPage.clickTab('Rules');
    
    // Verify add button is not visible for operators
    const addButtonVisible = await qualityPage.isAddRuleButtonVisible();
    expect(addButtonVisible).toBe(false);
    
    // Verify edit buttons are not visible for operators
    const editButtonsVisible = await qualityPage.areEditRuleButtonsVisible();
    expect(editButtonsVisible).toBe(false);
  });
});

// Test suite for quality dashboard responsiveness to different viewport sizes
test.describe('Quality Dashboard Responsiveness', () => {
  test('should adapt layout for tablet viewport', async ({ authenticatedPage }) => {
    // Set viewport size to tablet dimensions
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Verify quality dashboard is visible in tablet view
    await expect(authenticatedPage.locator(qualityPage.qualityScoreChart)).toBeVisible();
    await expect(authenticatedPage.locator(qualityPage.datasetQualityTable)).toBeVisible();
    
    // Check for responsive layout elements specific to tablet
    const isMobileNavVisible = await authenticatedPage.isVisible('[data-testid="mobile-nav-menu"]');
    const isDesktopNavVisible = await authenticatedPage.isVisible('[data-testid="desktop-nav-menu"]');
    
    // Tablet should show desktop navigation
    expect(isDesktopNavVisible).toBe(true);
    expect(isMobileNavVisible).toBe(false);
  });

  test('should adapt layout for mobile viewport', async ({ authenticatedPage }) => {
    // Set viewport size to mobile dimensions
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    
    const qualityPage = new QualityPage(authenticatedPage);
    await qualityPage.goto();
    await qualityPage.mockQualityData();
    await qualityPage.waitForQualityPageLoaded();
    
    // Verify quality dashboard critical elements are visible in mobile view
    await expect(authenticatedPage.locator(qualityPage.qualityScoreChart)).toBeVisible();
    
    // Check for responsive layout elements specific to mobile
    const isMobileNavVisible = await authenticatedPage.isVisible('[data-testid="mobile-nav-menu"]');
    const isDesktopNavVisible = await authenticatedPage.isVisible('[data-testid="desktop-nav-menu"]');
    
    // Mobile should show mobile navigation
    expect(isMobileNavVisible).toBe(true);
    expect(isDesktopNavVisible).toBe(false);
    
    // Verify mobile navigation works
    await authenticatedPage.click('[data-testid="mobile-nav-toggle"]');
    await expect(authenticatedPage.locator('[data-testid="mobile-nav-dropdown"]')).toBeVisible();
  });
});