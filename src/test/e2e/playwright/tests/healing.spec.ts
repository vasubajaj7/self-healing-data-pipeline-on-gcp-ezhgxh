import { expect } from '@playwright/test';
import HealingPage from '../fixtures/page-objects/healingPage';
import { test } from '../fixtures/auth.setup';
import { mockApiResponses } from '../fixtures/test-data';
import { IssueType, HealingMode, HealingStatus, AlertSeverity } from '../../../web/src/types/selfHealing';

/**
 * End-to-end tests for the self-healing functionality of the data pipeline application.
 * 
 * These tests verify:
 * - The self-healing dashboard displays correct metrics and components
 * - Issue management functionality works as expected
 * - Configuration settings can be properly managed
 * - Self-healing features respect access control for different user roles
 * - UI is responsive on different device sizes
 */

// Test suite for the main self-healing dashboard functionality
test.describe('Self-Healing Dashboard', () => {
  test('should display the self-healing dashboard with correct title', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await expect(authenticatedPage.locator('h1')).toContainText('Self-Healing');
  });

  test('should display all required dashboard components', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await expect(authenticatedPage.locator(healingPage.activeIssuesTable)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.healingActionsTable)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.successRateChart)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.modelHealthCard)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.modelPerformanceCard)).toBeVisible();
  });

  test('should display correct number of active issues', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const issuesCount = await healingPage.getActiveIssuesCount();
    expect(issuesCount).toBe(mockApiResponses.healing.list.data.issues.length);
  });

  test('should display correct success rate percentage', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const successRate = await healingPage.getSuccessRate();
    expect(successRate).toContain(mockApiResponses.healing.list.data.overallSuccessRate || '87%');
  });

  test('should display correct model health status', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const modelHealth = await healingPage.getModelHealthStatus();
    expect(modelHealth).toMatchObject({
      driftStatus: 'OK',
      featureHealth: 'OK',
      predictions: 'OK',
      inferenceTime: 'OK'
    });
  });

  test('should filter active issues by type', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const initialCount = await healingPage.getActiveIssuesCount();
    await healingPage.filterIssuesByType(IssueType.DATA_QUALITY);
    const filteredCount = await healingPage.getActiveIssuesCount();
    
    expect(filteredCount).toBeLessThanOrEqual(initialCount);
    
    await healingPage.clearFilters();
    const resetCount = await healingPage.getActiveIssuesCount();
    expect(resetCount).toBe(initialCount);
  });

  test('should filter active issues by status', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const initialCount = await healingPage.getActiveIssuesCount();
    await healingPage.filterIssuesByStatus(HealingStatus.IN_PROGRESS);
    const filteredCount = await healingPage.getActiveIssuesCount();
    
    expect(filteredCount).toBeLessThanOrEqual(initialCount);
    
    await healingPage.clearFilters();
    const resetCount = await healingPage.getActiveIssuesCount();
    expect(resetCount).toBe(initialCount);
  });

  test('should filter active issues by severity', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const initialCount = await healingPage.getActiveIssuesCount();
    await healingPage.filterIssuesBySeverity(AlertSeverity.HIGH);
    const filteredCount = await healingPage.getActiveIssuesCount();
    
    expect(filteredCount).toBeLessThanOrEqual(initialCount);
    
    await healingPage.clearFilters();
    const resetCount = await healingPage.getActiveIssuesCount();
    expect(resetCount).toBe(initialCount);
  });

  test('should refresh dashboard when refresh button is clicked', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    const initialUpdateTime = await healingPage.getLastUpdatedTime();
    
    // Mock updated healing API responses
    await authenticatedPage.route('**/api/healing/issues*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(mockApiResponses.healing.list.data)
      });
    });
    
    await healingPage.clickRefresh();
    
    const newUpdateTime = await healingPage.getLastUpdatedTime();
    expect(newUpdateTime).not.toBe(initialUpdateTime);
  });
});

// Tests for issue details and manual fix functionality
test.describe('Issue Management', () => {
  test('should display issue details when clicking on an issue', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    
    await expect(authenticatedPage.locator(healingPage.issueDetailsModal)).toBeVisible();
    
    const issueDetails = await healingPage.getIssueDetails();
    expect(issueDetails).toHaveProperty('description');
    expect(issueDetails).toHaveProperty('severity');
    expect(issueDetails).toHaveProperty('status');
  });

  test('should allow manual fix for an issue', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    await expect(adminPage.locator(healingPage.manualFixButton)).toBeVisible();
    
    await healingPage.triggerManualFix();
    await healingPage.confirmManualFix();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
  });

  test('should allow canceling a manual fix', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    await expect(adminPage.locator(healingPage.manualFixButton)).toBeVisible();
    
    await healingPage.triggerManualFix();
    await healingPage.cancelManualFix();
    
    // Verify confirmation dialog disappears
    await expect(adminPage.locator("dialog:has-text('Confirm Action')")).not.toBeVisible();
  });

  test('should close issue details modal', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    await expect(authenticatedPage.locator(healingPage.issueDetailsModal)).toBeVisible();
    
    await healingPage.closeIssueDetails();
    await expect(authenticatedPage.locator(healingPage.issueDetailsModal)).not.toBeVisible();
  });
});

// Tests for self-healing configuration settings
test.describe('Configuration Management', () => {
  test('should display configuration settings', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    
    const settings = await healingPage.getConfigurationSettings();
    expect(settings).toHaveProperty('autonomousMode');
    expect(settings).toHaveProperty('confidenceThreshold');
    expect(settings).toHaveProperty('maxRetryAttempts');
    expect(settings).toHaveProperty('approvalRequired');
    expect(settings).toHaveProperty('learningMode');
  });

  test('should update autonomous mode setting', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.setAutonomousMode('SEMI_AUTOMATIC');
    await healingPage.saveConfigurationSettings();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
    
    const settings = await healingPage.getConfigurationSettings();
    expect(settings.autonomousMode).toBe('SEMI_AUTOMATIC');
  });

  test('should update confidence threshold setting', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.setConfidenceThreshold(75);
    await healingPage.saveConfigurationSettings();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
    
    const settings = await healingPage.getConfigurationSettings();
    expect(Number(settings.confidenceThreshold)).toBe(75);
  });

  test('should update max retry attempts setting', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.setMaxRetryAttempts(5);
    await healingPage.saveConfigurationSettings();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
    
    const settings = await healingPage.getConfigurationSettings();
    expect(Number(settings.maxRetryAttempts)).toBe(5);
  });

  test('should update approval required setting', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.setApprovalRequired('High Impact Only');
    await healingPage.saveConfigurationSettings();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
    
    const settings = await healingPage.getConfigurationSettings();
    expect(settings.approvalRequired).toBe('High Impact Only');
  });

  test('should update learning mode setting', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.setLearningMode('Active');
    await healingPage.saveConfigurationSettings();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
    
    const settings = await healingPage.getConfigurationSettings();
    expect(settings.learningMode).toBe('Active');
  });
});

// Tests for managing correction rules
test.describe('Correction Rules Management', () => {
  test('should display correction rules list', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    
    const rules = await healingPage.getCorrectionRules();
    expect(rules.length).toBeGreaterThan(0);
  });

  test('should open rule editor for a specific rule', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.openRuleEditor(0);
    
    await expect(adminPage.locator(healingPage.ruleEditor)).toBeVisible();
  });

  test('should toggle auto-apply setting for a rule', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    await healingPage.openRuleEditor(0);
    await healingPage.toggleAutoApply();
    await healingPage.saveRule();
    
    await expect(adminPage.locator(healingPage.successNotification)).toBeVisible();
  });
});

// Tests for AI model management functionality
test.describe('Model Management', () => {
  test('should display AI models list', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Model Management');
    
    const models = await healingPage.getModels();
    expect(models.length).toBeGreaterThan(0);
  });

  test('should display model details when clicking on a model', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Model Management');
    await healingPage.openModelDetails(0);
    
    await expect(adminPage.locator(healingPage.modelDetailsPanel)).toBeVisible();
    
    const modelDetails = await healingPage.getModelDetails();
    expect(modelDetails).toHaveProperty('name');
    expect(modelDetails).toHaveProperty('type');
    expect(modelDetails).toHaveProperty('version');
    expect(modelDetails).toHaveProperty('accuracy');
  });
});

// Tests for activity log functionality
test.describe('Activity Log', () => {
  test('should display activity log entries', async ({ authenticatedPage }) => {
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Activity Log');
    
    const activities = await healingPage.getActivityLog();
    expect(activities.length).toBeGreaterThan(0);
  });
});

// Tests for access control with different user roles
test.describe('Access Control', () => {
  test('should allow admin users to access all tabs', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    expect(await healingPage.isTabAccessible('Dashboard')).toBe(true);
    expect(await healingPage.isTabAccessible('Activity Log')).toBe(true);
    expect(await healingPage.isTabAccessible('Model Management')).toBe(true);
    expect(await healingPage.isTabAccessible('Configuration')).toBe(true);
  });

  test('should allow engineer users to access appropriate tabs', async ({ engineerPage }) => {
    const healingPage = new HealingPage(engineerPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    expect(await healingPage.isTabAccessible('Dashboard')).toBe(true);
    expect(await healingPage.isTabAccessible('Activity Log')).toBe(true);
    expect(await healingPage.isTabAccessible('Model Management')).toBe(true);
    expect(await healingPage.isTabAccessible('Configuration')).toBe(false);
  });

  test('should allow analyst users to access appropriate tabs', async ({ analystPage }) => {
    const healingPage = new HealingPage(analystPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    expect(await healingPage.isTabAccessible('Dashboard')).toBe(true);
    expect(await healingPage.isTabAccessible('Activity Log')).toBe(true);
    expect(await healingPage.isTabAccessible('Model Management')).toBe(false);
    expect(await healingPage.isTabAccessible('Configuration')).toBe(false);
  });

  test('should allow operator users to access appropriate tabs', async ({ operatorPage }) => {
    const healingPage = new HealingPage(operatorPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    expect(await healingPage.isTabAccessible('Dashboard')).toBe(true);
    expect(await healingPage.isTabAccessible('Activity Log')).toBe(true);
    expect(await healingPage.isTabAccessible('Model Management')).toBe(false);
    expect(await healingPage.isTabAccessible('Configuration')).toBe(false);
  });

  test('should allow admin users to perform manual fixes', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    expect(await healingPage.isManualFixButtonVisible()).toBe(true);
  });

  test('should allow engineer users to perform manual fixes', async ({ engineerPage }) => {
    const healingPage = new HealingPage(engineerPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    expect(await healingPage.isManualFixButtonVisible()).toBe(true);
  });

  test('should not allow analyst users to perform manual fixes', async ({ analystPage }) => {
    const healingPage = new HealingPage(analystPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.openIssueDetails(0);
    expect(await healingPage.isManualFixButtonVisible()).toBe(false);
  });

  test('should allow admin users to edit configuration settings', async ({ adminPage }) => {
    const healingPage = new HealingPage(adminPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    await healingPage.switchToTab('Configuration');
    expect(await healingPage.areSettingsEditable()).toBe(true);
  });
});

// Tests for responsive design on different viewport sizes
test.describe('Responsiveness', () => {
  test('should adapt layout for tablet viewport', async ({ authenticatedPage }) => {
    // Set viewport size to tablet dimensions
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    // Verify dashboard layout has adapted for tablet view
    await expect(authenticatedPage.locator(healingPage.activeIssuesTable)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.successRateChart)).toBeVisible();
    await expect(authenticatedPage.locator(healingPage.modelHealthCard)).toBeVisible();
  });

  test('should adapt layout for mobile viewport', async ({ authenticatedPage }) => {
    // Set viewport size to mobile dimensions
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    
    const healingPage = new HealingPage(authenticatedPage);
    await healingPage.goto();
    await healingPage.mockHealingData();
    await healingPage.waitForPageLoaded();
    
    // Verify dashboard layout has adapted for mobile view
    await expect(authenticatedPage.locator(healingPage.activeIssuesTable)).toBeVisible();
    
    // Verify navigation elements are accessible in mobile view
    await expect(authenticatedPage.locator('button[aria-label="Open navigation menu"]')).toBeVisible();
  });
});