import { Page } from '@playwright/test'; // @playwright/test ^1.32.0
import { dashboard } from '../test-data';

/**
 * Page object representing the dashboard page of the application
 */
class DashboardPage {
  readonly page: Page;
  readonly url: string;
  
  // Selectors for dashboard elements
  readonly dashboardTitle: string;
  readonly timeRangeFilter: string;
  readonly refreshButton: string;
  readonly lastRefreshedText: string;
  
  // Dashboard card selectors
  readonly pipelineHealthCard: string;
  readonly dataQualityCard: string;
  readonly selfHealingStatusCard: string;
  readonly alertSummaryCard: string;
  readonly systemStatusCard: string;
  readonly quickStatsCard: string;
  readonly aiInsightsCard: string;
  readonly recentExecutionsTable: string;
  readonly viewAllExecutionsButton: string;

  /**
   * Initialize the DashboardPage object with selectors
   * @param page - Playwright page object
   */
  constructor(page: Page) {
    this.page = page;
    this.url = '/dashboard';
    
    // Define selectors for all dashboard page elements
    this.dashboardTitle = '[data-testid="dashboard-title"]';
    this.timeRangeFilter = '[data-testid="time-range-filter"]';
    this.refreshButton = '[data-testid="refresh-button"]';
    this.lastRefreshedText = '[data-testid="last-refreshed"]';
    
    this.pipelineHealthCard = '[data-testid="pipeline-health-card"]';
    this.dataQualityCard = '[data-testid="data-quality-card"]';
    this.selfHealingStatusCard = '[data-testid="self-healing-card"]';
    this.alertSummaryCard = '[data-testid="alert-summary-card"]';
    this.systemStatusCard = '[data-testid="system-status-card"]';
    this.quickStatsCard = '[data-testid="quick-stats-card"]';
    this.aiInsightsCard = '[data-testid="ai-insights-card"]';
    this.recentExecutionsTable = '[data-testid="recent-executions-table"]';
    this.viewAllExecutionsButton = '[data-testid="view-all-executions"]';
  }

  /**
   * Navigate to the dashboard page
   * @returns Promise that resolves when navigation is complete
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.dashboardTitle, { state: 'visible' });
  }

  /**
   * Mock the dashboard API responses
   * @returns Promise that resolves when mocking is complete
   */
  async mockDashboardData(): Promise<void> {
    await this.page.route('**/api/dashboard/summary', async (route) => {
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(dashboard.summary) 
      });
    });
    
    await this.page.route('**/api/dashboard/recentExecutions', async (route) => {
      await route.fulfill({ 
        status: 200, 
        body: JSON.stringify(dashboard.recentExecutions) 
      });
    });
  }

  /**
   * Wait for the dashboard to fully load
   * @returns Promise that resolves when dashboard is loaded
   */
  async waitForDashboardLoaded(): Promise<void> {
    await this.page.waitForSelector(this.pipelineHealthCard, { state: 'visible' });
    await this.page.waitForSelector(this.dataQualityCard, { state: 'visible' });
    await this.page.waitForSelector(this.selfHealingStatusCard, { state: 'visible' });
    await this.page.waitForSelector(this.recentExecutionsTable, { state: 'visible' });
  }

  /**
   * Select a time range from the filter dropdown
   * @param timeRange - The time range option to select (e.g., '24h', '7d', '30d')
   * @returns Promise that resolves when time range is selected
   */
  async selectTimeRange(timeRange: string): Promise<void> {
    await this.page.click(this.timeRangeFilter);
    await this.page.click(`[data-testid="time-range-option-${timeRange}"]`);
    await this.page.waitForResponse('**/api/dashboard/summary*');
  }

  /**
   * Click the refresh button to manually refresh dashboard data
   * @returns Promise that resolves when refresh is complete
   */
  async clickRefresh(): Promise<void> {
    await this.page.click(this.refreshButton);
    await this.page.waitForResponse('**/api/dashboard/summary*');
  }

  /**
   * Get the last refreshed time text
   * @returns Promise that resolves with the last refreshed time text
   */
  async getLastRefreshedTime(): Promise<string> {
    return this.page.textContent(this.lastRefreshedText) || '';
  }

  /**
   * Get the pipeline health percentage
   * @returns Promise that resolves with the pipeline health percentage
   */
  async getPipelineHealthPercentage(): Promise<string> {
    const selector = `${this.pipelineHealthCard} [data-testid="health-percentage"]`;
    return this.page.textContent(selector) || '';
  }

  /**
   * Get the data quality percentage
   * @returns Promise that resolves with the data quality percentage
   */
  async getDataQualityPercentage(): Promise<string> {
    const selector = `${this.dataQualityCard} [data-testid="quality-percentage"]`;
    return this.page.textContent(selector) || '';
  }

  /**
   * Get the self-healing auto-fix percentage
   * @returns Promise that resolves with the self-healing percentage
   */
  async getSelfHealingPercentage(): Promise<string> {
    const selector = `${this.selfHealingStatusCard} [data-testid="healing-percentage"]`;
    return this.page.textContent(selector) || '';
  }

  /**
   * Get the count of active alerts
   * @returns Promise that resolves with the number of active alerts
   */
  async getActiveAlertsCount(): Promise<number> {
    const alertItems = await this.page.$$(`${this.alertSummaryCard} [data-testid="alert-item"]`);
    return alertItems.length;
  }

  /**
   * Get the status of a specific system component
   * @param componentName The name of the system component
   * @returns Promise that resolves with the component status (OK, WARN, ERROR)
   */
  async getSystemStatusForComponent(componentName: string): Promise<string> {
    const componentSelector = `${this.systemStatusCard} [data-testid="component-${componentName}"]`;
    const statusSelector = `${componentSelector} [data-testid="component-status"]`;
    return this.page.getAttribute(statusSelector, 'data-status') || '';
  }

  /**
   * Get a specific quick stats value by label
   * @param statLabel The label of the stat to retrieve
   * @returns Promise that resolves with the quick stat value
   */
  async getQuickStatsValue(statLabel: string): Promise<string> {
    const statSelector = `${this.quickStatsCard} [data-testid="stat-label-${statLabel}"] + [data-testid="stat-value"]`;
    return this.page.textContent(statSelector) || '';
  }

  /**
   * Get the count of recent executions in the table
   * @returns Promise that resolves with the number of recent executions
   */
  async getRecentExecutionsCount(): Promise<number> {
    const rows = await this.page.$$(`${this.recentExecutionsTable} tbody tr`);
    return rows.length;
  }

  /**
   * Click the 'View All' button for recent executions
   * @returns Promise that resolves when navigation is complete
   */
  async clickViewAllExecutions(): Promise<void> {
    await this.page.click(this.viewAllExecutionsButton);
    await this.page.waitForURL('**/pipelines*');
  }

  /**
   * Click on a specific alert to view details
   * @param index The index of the alert to click (0-based)
   * @returns Promise that resolves when alert details are displayed
   */
  async clickAlertDetails(index: number): Promise<void> {
    const alertSelector = `${this.alertSummaryCard} [data-testid="alert-item"]:nth-child(${index + 1})`;
    await this.page.click(alertSelector);
    await this.page.waitForSelector('[data-testid="alert-details"]', { state: 'visible' });
  }

  /**
   * Get the count of AI insights displayed
   * @returns Promise that resolves with the number of AI insights
   */
  async getAIInsightsCount(): Promise<number> {
    const insights = await this.page.$$(`${this.aiInsightsCard} [data-testid="insight-item"]`);
    return insights.length;
  }

  /**
   * Check if the dashboard is fully loaded
   * @returns Promise that resolves with true if dashboard is loaded, false otherwise
   */
  async isDashboardLoaded(): Promise<boolean> {
    const isHealthCardVisible = await this.page.isVisible(this.pipelineHealthCard);
    const isQualityCardVisible = await this.page.isVisible(this.dataQualityCard);
    const isHealingCardVisible = await this.page.isVisible(this.selfHealingStatusCard);
    const isExecutionsTableVisible = await this.page.isVisible(this.recentExecutionsTable);
    
    return isHealthCardVisible && isQualityCardVisible && isHealingCardVisible && isExecutionsTableVisible;
  }

  /**
   * Get the title of a specific dashboard card
   * @param cardSelector The selector for the card
   * @returns Promise that resolves with the card title
   */
  async getCardTitle(cardSelector: string): Promise<string> {
    const titleSelector = `${cardSelector} [data-testid="card-title"]`;
    return this.page.textContent(titleSelector) || '';
  }
}

export default DashboardPage;