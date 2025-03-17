import { test, expect } from '@playwright/test'; // @playwright/test ^1.32.0
import DashboardPage from '../fixtures/page-objects/dashboardPage';
import { authenticatedPage, adminPage, engineerPage, analystPage, operatorPage } from '../fixtures/auth.setup';
import { mockApiResponses } from '../fixtures/test-data';

test.describe('Dashboard Page', () => {
  test('should display the dashboard with correct title', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    const title = await authenticatedPage.textContent('[data-testid="dashboard-title"]');
    expect(title).toContain('Dashboard');
  });

  test('should display all required dashboard cards', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify all required dashboard cards are visible
    await expect(authenticatedPage.locator(dashboardPage.pipelineHealthCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.dataQualityCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.selfHealingStatusCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.alertSummaryCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.systemStatusCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.quickStatsCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.aiInsightsCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.recentExecutionsTable)).toBeVisible();
  });

  test('should display correct pipeline health metrics', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    
    // Mock the API response with specific pipeline health data
    await authenticatedPage.route('**/api/dashboard/summary', async (route) => {
      const mockResponse = mockApiResponses.dashboard.summary;
      mockResponse.data.pipelineHealth.healthyPercentage = 98;
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockResponse) 
      });
    });
    
    await dashboardPage.waitForDashboardLoaded();
    
    const healthPercentage = await dashboardPage.getPipelineHealthPercentage();
    expect(healthPercentage).toContain('98');
  });

  test('should display correct data quality metrics', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    
    // Mock the API response with specific data quality metrics
    await authenticatedPage.route('**/api/dashboard/summary', async (route) => {
      const mockResponse = mockApiResponses.dashboard.summary;
      mockResponse.data.dataQuality.passedPercentage = 94;
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockResponse) 
      });
    });
    
    await dashboardPage.waitForDashboardLoaded();
    
    const qualityPercentage = await dashboardPage.getDataQualityPercentage();
    expect(qualityPercentage).toContain('94');
  });

  test('should display correct self-healing metrics', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    
    // Mock the API response with specific self-healing metrics
    await authenticatedPage.route('**/api/dashboard/summary', async (route) => {
      const mockResponse = mockApiResponses.dashboard.summary;
      mockResponse.data.selfHealing.successPercentage = 87;
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockResponse) 
      });
    });
    
    await dashboardPage.waitForDashboardLoaded();
    
    const healingPercentage = await dashboardPage.getSelfHealingPercentage();
    expect(healingPercentage).toContain('87');
  });

  test('should update dashboard when time range is changed', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Get initial metrics
    const initialHealthPercentage = await dashboardPage.getPipelineHealthPercentage();
    
    // Mock updated data for new time range
    await authenticatedPage.route('**/api/dashboard/summary*', async (route) => {
      const mockResponse = mockApiResponses.dashboard.summary;
      mockResponse.data.pipelineHealth.healthyPercentage = 92; // Different value
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockResponse) 
      });
    });
    
    // Select a different time range
    await dashboardPage.selectTimeRange('7d');
    
    // Get updated metrics
    const updatedHealthPercentage = await dashboardPage.getPipelineHealthPercentage();
    
    // Verify the metrics have been updated
    expect(updatedHealthPercentage).not.toEqual(initialHealthPercentage);
    expect(updatedHealthPercentage).toContain('92');
  });

  test('should refresh dashboard when refresh button is clicked', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Get initial refresh time
    const initialRefreshTime = await dashboardPage.getLastRefreshedTime();
    
    // Wait a moment to ensure time difference
    await authenticatedPage.waitForTimeout(1000);
    
    // Mock updated data for refresh
    await authenticatedPage.route('**/api/dashboard/summary*', async (route) => {
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockApiResponses.dashboard.summary) 
      });
    });
    
    // Click refresh button
    await dashboardPage.clickRefresh();
    
    // Get updated refresh time
    const updatedRefreshTime = await dashboardPage.getLastRefreshedTime();
    
    // Verify refresh time has been updated
    expect(updatedRefreshTime).not.toEqual(initialRefreshTime);
  });

  test('should display correct system status indicators', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    
    // Mock the API response with specific system status data
    await authenticatedPage.route('**/api/dashboard/summary', async (route) => {
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockApiResponses.dashboard.summary) 
      });
    });
    
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify statuses for each component
    const gcsStatus = await dashboardPage.getSystemStatusForComponent('GCS');
    expect(gcsStatus).toBe('OK');
    
    const sqlStatus = await dashboardPage.getSystemStatusForComponent('CloudSQL');
    expect(sqlStatus).toBe('OK');
    
    const apiStatus = await dashboardPage.getSystemStatusForComponent('ExternalAPIs');
    expect(apiStatus).toBe('WARN');
    
    const bqStatus = await dashboardPage.getSystemStatusForComponent('BigQuery');
    expect(bqStatus).toBe('OK');
    
    const mlStatus = await dashboardPage.getSystemStatusForComponent('MLServices');
    expect(mlStatus).toBe('OK');
  });

  test('should display correct number of active alerts', async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    
    // Mock the API response with specific alert data
    await authenticatedPage.route('**/api/dashboard/summary', async (route) => {
      const mockResponse = mockApiResponses.dashboard.summary;
      mockResponse.data.alerts.total = 6;
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(mockResponse) 
      });
    });
    
    await dashboardPage.waitForDashboardLoaded();
    
    const alertsCount = await dashboardPage.getActiveAlertsCount();
    expect(alertsCount).toBe(2); // Assuming 2 alert items are displayed in the UI
  });

  test("should navigate to pipeline management when 'View All' is clicked", async ({ authenticatedPage }) => {
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Click the View All button
    await dashboardPage.clickViewAllExecutions();
    
    // Verify navigation to pipeline management page
    expect(authenticatedPage.url()).toContain('/pipelines');
  });
});

test.describe('Dashboard Access Control', () => {
  test('should show all dashboard components for admin users', async ({ adminPage }) => {
    const dashboardPage = new DashboardPage(adminPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify all dashboard components are visible for admin
    const isDashboardLoaded = await dashboardPage.isDashboardLoaded();
    expect(isDashboardLoaded).toBe(true);
    
    // Additional admin-specific elements
    await expect(adminPage.locator('[data-testid="admin-controls"]')).toBeVisible();
  });

  test('should show appropriate dashboard components for engineer users', async ({ engineerPage }) => {
    const dashboardPage = new DashboardPage(engineerPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify engineer-accessible components are visible
    await expect(engineerPage.locator(dashboardPage.pipelineHealthCard)).toBeVisible();
    await expect(engineerPage.locator(dashboardPage.dataQualityCard)).toBeVisible();
    await expect(engineerPage.locator(dashboardPage.selfHealingStatusCard)).toBeVisible();
    
    // Verify restricted components based on engineer role permissions
    // For example, engineers might not have access to certain admin features
    await expect(engineerPage.locator('[data-testid="admin-controls"]')).not.toBeVisible();
  });

  test('should show appropriate dashboard components for analyst users', async ({ analystPage }) => {
    const dashboardPage = new DashboardPage(analystPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify analyst-accessible components are visible
    await expect(analystPage.locator(dashboardPage.dataQualityCard)).toBeVisible();
    await expect(analystPage.locator(dashboardPage.recentExecutionsTable)).toBeVisible();
    
    // Verify restricted components based on analyst role permissions
    await expect(analystPage.locator('[data-testid="healing-controls"]')).not.toBeVisible();
    await expect(analystPage.locator('[data-testid="admin-controls"]')).not.toBeVisible();
  });

  test('should show appropriate dashboard components for operator users', async ({ operatorPage }) => {
    const dashboardPage = new DashboardPage(operatorPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify operator-accessible components are visible
    await expect(operatorPage.locator(dashboardPage.pipelineHealthCard)).toBeVisible();
    await expect(operatorPage.locator(dashboardPage.alertSummaryCard)).toBeVisible();
    await expect(operatorPage.locator(dashboardPage.systemStatusCard)).toBeVisible();
    
    // Verify restricted components based on operator role permissions
    await expect(operatorPage.locator('[data-testid="configuration-controls"]')).not.toBeVisible();
    await expect(operatorPage.locator('[data-testid="admin-controls"]')).not.toBeVisible();
  });
});

test.describe('Dashboard Responsiveness', () => {
  test('should adapt layout for tablet viewport', async ({ authenticatedPage }) => {
    // Set viewport to tablet size
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify dashboard adaptation for tablet view
    // In tablet view, we expect certain layout changes, such as a collapsed sidebar
    await expect(authenticatedPage.locator('[data-testid="sidebar-collapsed"]')).toBeVisible();
    
    // Still expect all critical information to be visible
    await expect(authenticatedPage.locator(dashboardPage.pipelineHealthCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.dataQualityCard)).toBeVisible();
    await expect(authenticatedPage.locator(dashboardPage.selfHealingStatusCard)).toBeVisible();
  });

  test('should adapt layout for mobile viewport', async ({ authenticatedPage }) => {
    // Set viewport to mobile size
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    
    const dashboardPage = new DashboardPage(authenticatedPage);
    await dashboardPage.goto();
    await dashboardPage.mockDashboardData();
    await dashboardPage.waitForDashboardLoaded();
    
    // Verify dashboard adaptation for mobile view
    // In mobile view, we expect a different layout with stacked cards and a mobile menu
    await expect(authenticatedPage.locator('[data-testid="mobile-menu-button"]')).toBeVisible();
    
    // Still expect critical information to be visible, possibly in a different arrangement
    await expect(authenticatedPage.locator(dashboardPage.pipelineHealthCard)).toBeVisible();
    
    // Verify navigation elements are accessible in mobile view
    await authenticatedPage.click('[data-testid="mobile-menu-button"]');
    await expect(authenticatedPage.locator('[data-testid="mobile-menu"]')).toBeVisible();
  });
});