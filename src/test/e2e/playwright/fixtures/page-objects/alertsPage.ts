import { Page } from '@playwright/test'; // v1.32.0
import { alertData } from '../test-data';

/**
 * Page object representing the alerts page of the application
 */
export default class AlertsPage {
  private page: Page;
  private url: string;
  private pageTitle: string;
  private activeAlertsTable: string;
  private alertStatsCard: string;
  private alertTrendChart: string;
  private notificationChannelsCard: string;
  private timeRangeFilter: string;
  private severityFilter: string;
  private statusFilter: string;
  private typeFilter: string;
  private searchInput: string;
  private refreshButton: string;
  private alertDetailsCard: string;
  private relatedAlertsCard: string;
  private suggestedActionsCard: string;
  private acknowledgeButton: string;
  private escalateButton: string;
  private resolveButton: string;
  private suppressButton: string;

  /**
   * Initialize the AlertsPage object with selectors
   * @param page - Playwright page object
   */
  constructor(page: Page) {
    this.page = page;
    this.url = '/alerting';
    this.pageTitle = 'h1:has-text("Alerting")';
    this.activeAlertsTable = '[data-testid="active-alerts-table"]';
    this.alertStatsCard = '[data-testid="alert-stats-card"]';
    this.alertTrendChart = '[data-testid="alert-trend-chart"]';
    this.notificationChannelsCard = '[data-testid="notification-channels-card"]';
    this.timeRangeFilter = '[data-testid="time-range-filter"]';
    this.severityFilter = '[data-testid="severity-filter"]';
    this.statusFilter = '[data-testid="status-filter"]';
    this.typeFilter = '[data-testid="type-filter"]';
    this.searchInput = '[data-testid="alert-search-input"]';
    this.refreshButton = '[data-testid="refresh-button"]';
    this.alertDetailsCard = '[data-testid="alert-details-card"]';
    this.relatedAlertsCard = '[data-testid="related-alerts-card"]';
    this.suggestedActionsCard = '[data-testid="suggested-actions-card"]';
    this.acknowledgeButton = '[data-testid="acknowledge-button"]';
    this.escalateButton = '[data-testid="escalate-button"]';
    this.resolveButton = '[data-testid="resolve-button"]';
    this.suppressButton = '[data-testid="suppress-button"]';
  }

  /**
   * Navigate to the alerts page
   * @returns Promise that resolves when navigation is complete
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.pageTitle);
    await this.page.waitForSelector(this.activeAlertsTable);
  }

  /**
   * Mock the alert API responses
   * @returns Promise that resolves when mocking is complete
   */
  async mockAlertData(): Promise<void> {
    await this.page.route('**/api/alerts', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(alertData.alerts)
      });
    });

    await this.page.route('**/api/alerts/stats', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          high: 1,
          medium: 2,
          low: 2,
          total: 5
        })
      });
    });

    await this.page.route('**/api/notifications', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(alertData.notifications)
      });
    });
  }

  /**
   * Wait for the alerts page to fully load
   * @returns Promise that resolves when alerts page is loaded
   */
  async waitForAlertsLoaded(): Promise<void> {
    await this.page.waitForSelector(this.activeAlertsTable);
    await this.page.waitForSelector(this.alertStatsCard);
    await this.page.waitForSelector(this.alertTrendChart);
  }

  /**
   * Select a time range from the filter dropdown
   * @param timeRange - Time range value to select
   * @returns Promise that resolves when time range is selected
   */
  async selectTimeRange(timeRange: string): Promise<void> {
    await this.page.click(this.timeRangeFilter);
    await this.page.click(`text="${timeRange}"`);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Filter alerts by severity
   * @param severity - Severity level to filter by
   * @returns Promise that resolves when filter is applied
   */
  async filterBySeverity(severity: string): Promise<void> {
    await this.page.click(this.severityFilter);
    await this.page.click(`text="${severity}"`);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Filter alerts by status
   * @param status - Status to filter by
   * @returns Promise that resolves when filter is applied
   */
  async filterByStatus(status: string): Promise<void> {
    await this.page.click(this.statusFilter);
    await this.page.click(`text="${status}"`);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Filter alerts by type
   * @param type - Alert type to filter by
   * @returns Promise that resolves when filter is applied
   */
  async filterByType(type: string): Promise<void> {
    await this.page.click(this.typeFilter);
    await this.page.click(`text="${type}"`);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Search alerts by keyword
   * @param searchTerm - Search term to filter alerts
   * @returns Promise that resolves when search is applied
   */
  async searchAlerts(searchTerm: string): Promise<void> {
    await this.page.fill(this.searchInput, '');
    await this.page.fill(this.searchInput, searchTerm);
    await this.page.press(this.searchInput, 'Enter');
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Click the refresh button to manually refresh alerts
   * @returns Promise that resolves when refresh is complete
   */
  async clickRefresh(): Promise<void> {
    await this.page.click(this.refreshButton);
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Get the count of active alerts in the table
   * @returns Promise that resolves with the number of active alerts
   */
  async getActiveAlertsCount(): Promise<number> {
    const rows = await this.page.$$(`${this.activeAlertsTable} tbody tr`);
    return rows.length;
  }

  /**
   * Get the alert statistics by severity
   * @returns Promise that resolves with alert statistics
   */
  async getAlertStats(): Promise<object> {
    const criticalCount = await this.page.textContent(`${this.alertStatsCard} [data-testid="alert-count-critical"]`);
    const highCount = await this.page.textContent(`${this.alertStatsCard} [data-testid="alert-count-high"]`);
    const mediumCount = await this.page.textContent(`${this.alertStatsCard} [data-testid="alert-count-medium"]`);
    const lowCount = await this.page.textContent(`${this.alertStatsCard} [data-testid="alert-count-low"]`);
    const totalCount = await this.page.textContent(`${this.alertStatsCard} [data-testid="alert-count-total"]`);

    return {
      critical: parseInt(criticalCount || '0'),
      high: parseInt(highCount || '0'),
      medium: parseInt(mediumCount || '0'),
      low: parseInt(lowCount || '0'),
      total: parseInt(totalCount || '0')
    };
  }

  /**
   * Select an alert from the table by index
   * @param index - Zero-based index of the alert to select
   * @returns Promise that resolves when alert is selected
   */
  async selectAlert(index: number): Promise<void> {
    await this.page.click(`${this.activeAlertsTable} tbody tr:nth-child(${index + 1})`);
    await this.page.waitForSelector(this.alertDetailsCard);
  }

  /**
   * Get the details of the currently selected alert
   * @returns Promise that resolves with alert details
   */
  async getAlertDetails(): Promise<object> {
    const id = await this.page.getAttribute(`${this.alertDetailsCard}`, 'data-alert-id');
    const description = await this.page.textContent(`${this.alertDetailsCard} [data-testid="alert-description"]`);
    const severity = await this.page.textContent(`${this.alertDetailsCard} [data-testid="alert-severity"]`);
    const createdAt = await this.page.textContent(`${this.alertDetailsCard} [data-testid="alert-created-at"]`);
    const status = await this.page.textContent(`${this.alertDetailsCard} [data-testid="alert-status"]`);
    const details = await this.page.textContent(`${this.alertDetailsCard} [data-testid="alert-details"]`);

    return {
      id,
      description,
      severity,
      createdAt,
      status,
      details
    };
  }

  /**
   * Get the count of related alerts for the selected alert
   * @returns Promise that resolves with the number of related alerts
   */
  async getRelatedAlertsCount(): Promise<number> {
    const rows = await this.page.$$(`${this.relatedAlertsCard} [data-testid="related-alert-item"]`);
    return rows.length;
  }

  /**
   * Get the count of suggested actions for the selected alert
   * @returns Promise that resolves with the number of suggested actions
   */
  async getSuggestedActionsCount(): Promise<number> {
    const actions = await this.page.$$(`${this.suggestedActionsCard} [data-testid="suggested-action-item"]`);
    return actions.length;
  }

  /**
   * Acknowledge the selected alert
   * @param comment - Optional comment for acknowledgment
   * @returns Promise that resolves when alert is acknowledged
   */
  async acknowledgeAlert(comment?: string): Promise<void> {
    await this.page.click(this.acknowledgeButton);
    
    if (comment) {
      await this.page.fill('[data-testid="acknowledgment-comment"]', comment);
    }
    
    await this.page.click('[data-testid="confirm-acknowledge-button"]');
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Escalate the selected alert
   * @param reason - Reason for escalation
   * @param level - Escalation level
   * @returns Promise that resolves when alert is escalated
   */
  async escalateAlert(reason: string, level: string): Promise<void> {
    await this.page.click(this.escalateButton);
    
    await this.page.selectOption('[data-testid="escalation-level"]', level);
    await this.page.fill('[data-testid="escalation-reason"]', reason);
    
    await this.page.click('[data-testid="confirm-escalation-button"]');
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Resolve the selected alert
   * @param resolutionNotes - Notes on how the alert was resolved
   * @returns Promise that resolves when alert is resolved
   */
  async resolveAlert(resolutionNotes: string): Promise<void> {
    await this.page.click(this.resolveButton);
    
    await this.page.fill('[data-testid="resolution-notes"]', resolutionNotes);
    
    await this.page.click('[data-testid="confirm-resolution-button"]');
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Suppress the selected alert
   * @param reason - Reason for suppression
   * @param durationMinutes - Duration in minutes to suppress
   * @returns Promise that resolves when alert is suppressed
   */
  async suppressAlert(reason: string, durationMinutes: number): Promise<void> {
    await this.page.click(this.suppressButton);
    
    await this.page.fill('[data-testid="suppression-reason"]', reason);
    await this.page.fill('[data-testid="suppression-duration"]', durationMinutes.toString());
    
    await this.page.click('[data-testid="confirm-suppression-button"]');
    await this.page.waitForResponse(response => 
      response.url().includes('/api/alerts') && response.status() === 200
    );
  }

  /**
   * Get the status of notification channels
   * @returns Promise that resolves with notification channel status
   */
  async getNotificationChannelStatus(): Promise<object> {
    const teamsSent = await this.page.textContent(`${this.notificationChannelsCard} [data-testid="teams-sent"]`);
    const emailSent = await this.page.textContent(`${this.notificationChannelsCard} [data-testid="email-sent"]`);
    const smsSent = await this.page.textContent(`${this.notificationChannelsCard} [data-testid="sms-sent"]`);
    const pending = await this.page.textContent(`${this.notificationChannelsCard} [data-testid="pending"]`);

    return {
      teams: parseInt(teamsSent || '0'),
      email: parseInt(emailSent || '0'),
      sms: parseInt(smsSent || '0'),
      pending: parseInt(pending || '0')
    };
  }

  /**
   * Check if a specific alert action is enabled for the selected alert
   * @param actionType - Type of action to check (acknowledge, escalate, resolve, suppress)
   * @returns Promise that resolves with true if action is enabled, false otherwise
   */
  async isAlertActionEnabled(actionType: string): Promise<boolean> {
    let selector: string;
    
    switch (actionType.toLowerCase()) {
      case 'acknowledge':
        selector = this.acknowledgeButton;
        break;
      case 'escalate':
        selector = this.escalateButton;
        break;
      case 'resolve':
        selector = this.resolveButton;
        break;
      case 'suppress':
        selector = this.suppressButton;
        break;
      default:
        throw new Error(`Unknown action type: ${actionType}`);
    }
    
    const isDisabled = await this.page.getAttribute(selector, 'disabled');
    return isDisabled !== 'true' && isDisabled !== '';
  }

  /**
   * Wait for a feedback message to appear after an action
   * @returns Promise that resolves with the feedback message text
   */
  async waitForFeedbackMessage(): Promise<string> {
    const snackbar = await this.page.waitForSelector('[data-testid="feedback-snackbar"]');
    return snackbar.textContent() || '';
  }

  /**
   * Check if the alerts page is fully loaded
   * @returns Promise that resolves with true if page is loaded, false otherwise
   */
  async isAlertsPageLoaded(): Promise<boolean> {
    const alertsTableVisible = await this.page.isVisible(this.activeAlertsTable);
    const alertStatsVisible = await this.page.isVisible(this.alertStatsCard);
    
    return alertsTableVisible && alertStatsVisible;
  }
}