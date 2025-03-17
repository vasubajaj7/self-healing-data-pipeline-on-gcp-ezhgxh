import { test, expect } from '@playwright/test';
import AlertsPage from '../fixtures/page-objects/alertsPage';
import { test as authTest } from '../fixtures/auth.setup';
import { alertData } from '../fixtures/test-data';

authTest.describe('Alerts Page', () => {
  authTest('should display the alerts page with correct title', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Verify page title
    await expect(authenticatedPage.locator('h1')).toContainText(/Alerts|Alerting/);
  });

  authTest('should display all required alert components', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Verify all required components are visible
    await expect(authenticatedPage.locator('[data-testid="active-alerts-table"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="alert-stats-card"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="alert-trend-chart"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="notification-channels-card"]')).toBeVisible();
    
    // Verify filter controls are visible
    await expect(authenticatedPage.locator('[data-testid="time-range-filter"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="severity-filter"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="status-filter"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="alert-search-input"]')).toBeVisible();
  });

  authTest('should display correct number of active alerts', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get count of alerts in table
    const alertCount = await alertsPage.getActiveAlertsCount();
    
    // Verify alert count matches mock data
    expect(alertCount).toBe(alertData.alerts.length);
  });

  authTest('should display correct alert statistics', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get alert statistics
    const stats = await alertsPage.getAlertStats();
    
    // Verify statistics match expected values from mock data
    expect(stats).toEqual(expect.objectContaining({
      high: 1,
      medium: 2,
      low: 2,
      total: 5
    }));
  });

  authTest('should filter alerts by severity', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get initial count
    const initialCount = await alertsPage.getActiveAlertsCount();
    
    // Mock filtered response
    await authenticatedPage.route('**/api/alerts*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alerts: [alertData.alerts[0]],  // Only the HIGH severity alert
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: 1,
            totalPages: 1
          }
        })
      });
    });
    
    // Apply filter
    await alertsPage.filterBySeverity('High');
    
    // Get new count
    const filteredCount = await alertsPage.getActiveAlertsCount();
    
    // Verify filtered results
    expect(filteredCount).toBe(1);
    expect(filteredCount).toBeLessThan(initialCount);
  });

  authTest('should filter alerts by status', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get initial count
    const initialCount = await alertsPage.getActiveAlertsCount();
    
    // Mock filtered response for acknowledged alerts
    await authenticatedPage.route('**/api/alerts*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alerts: alertData.alerts.filter(a => a.acknowledged),
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: alertData.alerts.filter(a => a.acknowledged).length,
            totalPages: 1
          }
        })
      });
    });
    
    // Apply filter
    await alertsPage.filterByStatus('Acknowledged');
    
    // Get new count
    const filteredCount = await alertsPage.getActiveAlertsCount();
    
    // Verify filtered results
    expect(filteredCount).toBeLessThanOrEqual(initialCount);
  });

  authTest('should filter alerts by time range', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get initial count
    const initialCount = await alertsPage.getActiveAlertsCount();
    
    // Mock filtered response for 24 hour time range
    await authenticatedPage.route('**/api/alerts*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alerts: alertData.alerts.slice(0, 3),  // Fewer alerts for the time range
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: 3,
            totalPages: 1
          }
        })
      });
    });
    
    // Apply filter
    await alertsPage.selectTimeRange('24 hours');
    
    // Get new count
    const filteredCount = await alertsPage.getActiveAlertsCount();
    
    // Verify filtered results
    expect(filteredCount).toBe(3);
    expect(filteredCount).toBeLessThan(initialCount);
  });

  authTest('should search alerts by keyword', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get initial count
    const initialCount = await alertsPage.getActiveAlertsCount();
    
    // Mock search results
    await authenticatedPage.route('**/api/alerts*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alerts: [alertData.alerts[0]],  // Only one result for the search
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: 1,
            totalPages: 1
          }
        })
      });
    });
    
    // Perform search
    await alertsPage.searchAlerts('BigQuery');
    
    // Get new count
    const searchCount = await alertsPage.getActiveAlertsCount();
    
    // Verify search results
    expect(searchCount).toBe(1);
    expect(searchCount).toBeLessThan(initialCount);
  });

  authTest('should display alert details when alert is selected', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details API response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: [
            'Increase BigQuery slot reservation',
            'Implement load job scheduling',
            'Review query optimization opportunities'
          ]
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify details panel is visible
    await expect(authenticatedPage.locator('[data-testid="alert-details-card"]')).toBeVisible();
    
    // Get alert details
    const details = await alertsPage.getAlertDetails();
    
    // Verify details match mock data
    expect(details).toEqual(expect.objectContaining({
      id: alertData.alerts[0].id,
      description: alertData.alerts[0].description,
      severity: expect.stringContaining(alertData.alerts[0].severity)
    }));
  });

  authTest('should display related alerts for selected alert', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details API response with related alerts
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: [
            'Increase BigQuery slot reservation',
            'Implement load job scheduling',
            'Review query optimization opportunities'
          ]
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify related alerts section is visible
    await expect(authenticatedPage.locator('[data-testid="related-alerts-card"]')).toBeVisible();
    
    // Get related alerts count
    const relatedCount = await alertsPage.getRelatedAlertsCount();
    
    // Verify count matches mock data
    expect(relatedCount).toBe(1);
  });

  authTest('should display suggested actions for selected alert', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details API response with suggested actions
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: [
            'Increase BigQuery slot reservation',
            'Implement load job scheduling',
            'Review query optimization opportunities'
          ]
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify suggested actions section is visible
    await expect(authenticatedPage.locator('[data-testid="suggested-actions-card"]')).toBeVisible();
    
    // Get suggested actions count
    const actionsCount = await alertsPage.getSuggestedActionsCount();
    
    // Verify count matches mock data
    expect(actionsCount).toBe(3);
  });

  authTest('should acknowledge an alert successfully', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify acknowledge button is enabled
    expect(await alertsPage.isAlertActionEnabled('acknowledge')).toBe(true);
    
    // Mock acknowledgment response
    await authenticatedPage.route('**/api/alerts/*/acknowledge', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          success: true,
          message: 'Alert acknowledged successfully',
          alert: {
            ...alertData.alerts[0],
            acknowledged: true,
            status: 'ACKNOWLEDGED'
          }
        })
      });
    });
    
    // Acknowledge the alert
    await alertsPage.acknowledgeAlert('Test acknowledgment');
    
    // Verify success feedback message
    const feedback = await alertsPage.waitForFeedbackMessage();
    expect(feedback).toContain('acknowledged');
  });

  authTest('should escalate an alert successfully', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify escalate button is enabled
    expect(await alertsPage.isAlertActionEnabled('escalate')).toBe(true);
    
    // Mock escalation response
    await authenticatedPage.route('**/api/alerts/*/escalate', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          success: true,
          message: 'Alert escalated successfully',
          alert: {
            ...alertData.alerts[0],
            status: 'ESCALATED'
          }
        })
      });
    });
    
    // Escalate the alert
    await alertsPage.escalateAlert('Test escalation', 'Manager');
    
    // Verify success feedback message
    const feedback = await alertsPage.waitForFeedbackMessage();
    expect(feedback).toContain('escalated');
  });

  authTest('should resolve an alert successfully', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify resolve button is enabled
    expect(await alertsPage.isAlertActionEnabled('resolve')).toBe(true);
    
    // Mock resolution response
    await authenticatedPage.route('**/api/alerts/*/resolve', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          success: true,
          message: 'Alert resolved successfully',
          alert: {
            ...alertData.alerts[0],
            status: 'RESOLVED'
          }
        })
      });
    });
    
    // Resolve the alert
    await alertsPage.resolveAlert('Test resolution');
    
    // Verify success feedback message
    const feedback = await alertsPage.waitForFeedbackMessage();
    expect(feedback).toContain('resolved');
  });

  authTest('should suppress an alert successfully', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify suppress button is enabled
    expect(await alertsPage.isAlertActionEnabled('suppress')).toBe(true);
    
    // Mock suppression response
    await authenticatedPage.route('**/api/alerts/*/suppress', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          success: true,
          message: 'Alert suppressed successfully',
          alert: {
            ...alertData.alerts[0],
            status: 'SUPPRESSED'
          }
        })
      });
    });
    
    // Suppress the alert
    await alertsPage.suppressAlert('Test suppression', 60);
    
    // Verify success feedback message
    const feedback = await alertsPage.waitForFeedbackMessage();
    expect(feedback).toContain('suppressed');
  });

  authTest('should refresh alerts when refresh button is clicked', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get initial alert data for comparison
    const initialAlertCount = await alertsPage.getActiveAlertsCount();
    
    // Mock updated alert data with fewer alerts
    await authenticatedPage.route('**/api/alerts', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alerts: alertData.alerts.slice(0, 3),  // Fewer alerts
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: 3,
            totalPages: 1
          }
        })
      });
    });
    
    // Click refresh button
    await authenticatedPage.click('[data-testid="refresh-button"]');
    
    // Wait for refresh to complete
    await authenticatedPage.waitForResponse(
      response => response.url().includes('/api/alerts') && response.status() === 200
    );
    
    // Get updated alert count
    const updatedAlertCount = await alertsPage.getActiveAlertsCount();
    
    // Verify count has changed
    expect(updatedAlertCount).toBe(3);
    expect(updatedAlertCount).toBeLessThan(initialAlertCount);
  });
});

authTest.describe('Alert Access Control', () => {
  authTest('should show all alert actions for admin users', async ({ adminPage }) => {
    const alertsPage = new AlertsPage(adminPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await adminPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Verify all action buttons are enabled for admin
    expect(await alertsPage.isAlertActionEnabled('acknowledge')).toBe(true);
    expect(await alertsPage.isAlertActionEnabled('escalate')).toBe(true);
    expect(await alertsPage.isAlertActionEnabled('resolve')).toBe(true);
    expect(await alertsPage.isAlertActionEnabled('suppress')).toBe(true);
  });

  authTest('should show appropriate alert actions for engineer users', async ({ engineerPage }) => {
    const alertsPage = new AlertsPage(engineerPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await engineerPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Check which actions are enabled for engineers
    // These assertions are based on assumed permissions for engineers
    await expect(engineerPage.locator('[data-testid="acknowledge-button"]')).toBeVisible();
    await expect(engineerPage.locator('[data-testid="resolve-button"]')).toBeVisible();
  });

  authTest('should show appropriate alert actions for analyst users', async ({ analystPage }) => {
    const alertsPage = new AlertsPage(analystPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await analystPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Check which actions are enabled for analysts
    // These assertions are based on assumed permissions for analysts
    await expect(analystPage.locator('[data-testid="acknowledge-button"]')).toBeVisible();
  });

  authTest('should show appropriate alert actions for operator users', async ({ operatorPage }) => {
    const alertsPage = new AlertsPage(operatorPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Mock alert details response
    await operatorPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select the first alert
    await alertsPage.selectAlert(0);
    
    // Check which actions are enabled for operators
    // These assertions are based on assumed permissions for operators
    await expect(operatorPage.locator('[data-testid="acknowledge-button"]')).toBeVisible();
    await expect(operatorPage.locator('[data-testid="escalate-button"]')).toBeVisible();
    await expect(operatorPage.locator('[data-testid="resolve-button"]')).toBeVisible();
  });
});

authTest.describe('Alert Notification Channels', () => {
  authTest('should display correct notification channel status', async ({ authenticatedPage }) => {
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Get notification channel status
    const channelStatus = await alertsPage.getNotificationChannelStatus();
    
    // Verify status matches expected mock data
    expect(channelStatus).toEqual(expect.objectContaining({
      teams: 4,
      email: 5,
      sms: 0,
      pending: 0
    }));
  });
});

authTest.describe('Alert Responsiveness', () => {
  authTest('should adapt layout for tablet viewport', async ({ authenticatedPage }) => {
    // Set viewport to tablet size
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Check that the page is loaded in tablet view
    expect(await alertsPage.isAlertsPageLoaded()).toBe(true);
    
    // Check that all critical components are visible
    await expect(authenticatedPage.locator('[data-testid="active-alerts-table"]')).toBeVisible();
    await expect(authenticatedPage.locator('[data-testid="alert-stats-card"]')).toBeVisible();
  });

  authTest('should adapt layout for mobile viewport', async ({ authenticatedPage }) => {
    // Set viewport to mobile size
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    
    const alertsPage = new AlertsPage(authenticatedPage);
    await alertsPage.goto();
    await alertsPage.mockAlertData();
    await alertsPage.waitForAlertsLoaded();
    
    // Check that the page is loaded in mobile view
    expect(await alertsPage.isAlertsPageLoaded()).toBe(true);
    
    // Check that all critical components are accessible in mobile view
    await expect(authenticatedPage.locator('[data-testid="active-alerts-table"]')).toBeVisible();
    
    // Mock alert details response
    await authenticatedPage.route('**/api/alerts/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          alert: alertData.alerts[0],
          notifications: alertData.notifications.slice(0, 2),
          relatedAlerts: [alertData.alerts[1]],
          suggestedActions: ['Increase BigQuery slot reservation']
        })
      });
    });
    
    // Select an alert and check if actions are accessible
    await alertsPage.selectAlert(0);
    await expect(authenticatedPage.locator('[data-testid="alert-details-card"]')).toBeVisible();
  });
});