import { Page } from '@playwright/test';
import { mockApiResponses } from '../test-data';

export default class QualityPage {
  readonly page: Page;
  readonly url: string;
  readonly pageTitle: string;
  readonly refreshButton: string;
  readonly lastRefreshedText: string;
  readonly datasetSelector: string;
  readonly tableSelector: string;
  readonly timeRangeFilter: string;
  readonly overviewTab: string;
  readonly datasetDetailTab: string;
  readonly tableDetailTab: string;
  readonly rulesTab: string;
  readonly issuesTab: string;
  readonly trendsTab: string;
  readonly qualityScoreChart: string;
  readonly qualityDimensionsCard: string;
  readonly datasetQualityTable: string;
  readonly validationRulesTable: string;
  readonly validationIssuesTable: string;
  readonly qualityTrendChart: string;
  readonly failingRulesCard: string;
  readonly addRuleButton: string;
  readonly ruleEditorModal: string;
  readonly issueDetailModal: string;
  readonly severityFilter: string;
  readonly ruleTypeFilter: string;
  readonly dimensionFilter: string;
  readonly statusFilter: string;
  readonly searchInput: string;

  constructor(page: Page) {
    this.page = page;
    this.url = '/quality';
    this.pageTitle = 'h1:has-text("Data Quality")';
    this.refreshButton = 'button[aria-label="Refresh"]';
    this.lastRefreshedText = '[data-testid="last-refreshed-text"]';
    this.datasetSelector = '[data-testid="dataset-selector"]';
    this.tableSelector = '[data-testid="table-selector"]';
    this.timeRangeFilter = '[data-testid="time-range-filter"]';
    this.overviewTab = '[data-testid="tab-overview"]';
    this.datasetDetailTab = '[data-testid="tab-dataset-detail"]';
    this.tableDetailTab = '[data-testid="tab-table-detail"]';
    this.rulesTab = '[data-testid="tab-rules"]';
    this.issuesTab = '[data-testid="tab-issues"]';
    this.trendsTab = '[data-testid="tab-trends"]';
    this.qualityScoreChart = '[data-testid="quality-score-chart"]';
    this.qualityDimensionsCard = '[data-testid="quality-dimensions-card"]';
    this.datasetQualityTable = '[data-testid="dataset-quality-table"]';
    this.validationRulesTable = '[data-testid="validation-rules-table"]';
    this.validationIssuesTable = '[data-testid="validation-issues-table"]';
    this.qualityTrendChart = '[data-testid="quality-trend-chart"]';
    this.failingRulesCard = '[data-testid="failing-rules-card"]';
    this.addRuleButton = '[data-testid="add-rule-button"]';
    this.ruleEditorModal = '[data-testid="rule-editor-modal"]';
    this.issueDetailModal = '[data-testid="issue-detail-modal"]';
    this.severityFilter = '[data-testid="severity-filter"]';
    this.ruleTypeFilter = '[data-testid="rule-type-filter"]';
    this.dimensionFilter = '[data-testid="dimension-filter"]';
    this.statusFilter = '[data-testid="status-filter"]';
    this.searchInput = '[data-testid="search-input"]';
  }

  /**
   * Navigate to the quality page
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.pageTitle);
  }

  /**
   * Mock the quality API responses
   */
  async mockQualityData(): Promise<void> {
    await this.page.route('**/api/quality/**', (route) => {
      const url = route.request().url();
      if (url.includes('/api/quality/list')) {
        route.fulfill({ json: mockApiResponses.quality.list });
      } else if (url.includes('/api/quality/details')) {
        route.fulfill({ json: mockApiResponses.quality.details });
      }
    });
  }

  /**
   * Wait for the quality page to fully load
   */
  async waitForQualityPageLoaded(): Promise<void> {
    await this.page.waitForSelector(this.qualityScoreChart);
    await this.page.waitForSelector(this.datasetQualityTable);
    await this.page.waitForSelector(this.qualityDimensionsCard);
  }

  /**
   * Click the refresh button to manually refresh quality data
   */
  async clickRefresh(): Promise<void> {
    await this.page.click(this.refreshButton);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Get the last refreshed time text
   */
  async getLastRefreshedTime(): Promise<string> {
    return await this.page.textContent(this.lastRefreshedText) || '';
  }

  /**
   * Select a dataset from the dataset selector
   */
  async selectDataset(datasetName: string): Promise<void> {
    await this.page.click(this.datasetSelector);
    await this.page.click(`text="${datasetName}"`);
    await this.page.waitForResponse('**/api/quality/details**');
  }

  /**
   * Select a table from the table selector
   */
  async selectTable(tableName: string): Promise<void> {
    await this.page.click(this.tableSelector);
    await this.page.click(`text="${tableName}"`);
    await this.page.waitForResponse('**/api/quality/details**');
  }

  /**
   * Select a time range from the filter dropdown
   */
  async selectTimeRange(timeRange: string): Promise<void> {
    await this.page.click(this.timeRangeFilter);
    await this.page.click(`text="${timeRange}"`);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Click on a specific tab
   */
  async clickTab(tabName: string): Promise<void> {
    let tabSelector;
    
    switch (tabName.toLowerCase()) {
      case 'overview':
        tabSelector = this.overviewTab;
        break;
      case 'dataset detail':
        tabSelector = this.datasetDetailTab;
        break;
      case 'table detail':
        tabSelector = this.tableDetailTab;
        break;
      case 'rules':
        tabSelector = this.rulesTab;
        break;
      case 'issues':
        tabSelector = this.issuesTab;
        break;
      case 'trends':
        tabSelector = this.trendsTab;
        break;
      default:
        throw new Error(`Tab "${tabName}" not recognized`);
    }
    
    await this.page.click(tabSelector);
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Get the overall quality score
   */
  async getQualityScore(): Promise<string> {
    return await this.page.textContent(`${this.qualityScoreChart} [data-testid="score-value"]`) || '';
  }

  /**
   * Get the quality dimensions breakdown
   */
  async getQualityDimensions(): Promise<Record<string, string>> {
    const dimensions: Record<string, string> = {};
    const dimensionElements = await this.page.$$(`${this.qualityDimensionsCard} [data-testid="dimension-item"]`);
    
    for (const element of dimensionElements) {
      const name = await element.$eval('[data-testid="dimension-name"]', el => el.textContent || '');
      const score = await element.$eval('[data-testid="dimension-score"]', el => el.textContent || '');
      dimensions[name.trim()] = score.trim();
    }
    
    return dimensions;
  }

  /**
   * Get the dataset quality summaries from the table
   */
  async getDatasetQualitySummaries(): Promise<Array<{dataset: string, qualityScore: string, trend: string, issueCount: string}>> {
    const rows = await this.page.$$(`${this.datasetQualityTable} tbody tr`);
    const summaries = [];
    
    for (const row of rows) {
      const cells = await row.$$('td');
      if (cells.length >= 4) {
        const dataset = await cells[0].textContent() || '';
        const qualityScore = await cells[1].textContent() || '';
        const trend = await cells[2].textContent() || '';
        const issueCount = await cells[3].textContent() || '';
        
        summaries.push({
          dataset: dataset.trim(),
          qualityScore: qualityScore.trim(),
          trend: trend.trim(),
          issueCount: issueCount.trim()
        });
      }
    }
    
    return summaries;
  }

  /**
   * Get the validation rules from the table
   */
  async getValidationRules(): Promise<Array<{name: string, type: string, dimension: string, successRate: string}>> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    await this.page.waitForSelector(this.validationRulesTable);
    const rows = await this.page.$$(`${this.validationRulesTable} tbody tr`);
    const rules = [];
    
    for (const row of rows) {
      const cells = await row.$$('td');
      if (cells.length >= 4) {
        const name = await cells[0].textContent() || '';
        const type = await cells[1].textContent() || '';
        const dimension = await cells[2].textContent() || '';
        const successRate = await cells[3].textContent() || '';
        
        rules.push({
          name: name.trim(),
          type: type.trim(),
          dimension: dimension.trim(),
          successRate: successRate.trim()
        });
      }
    }
    
    return rules;
  }

  /**
   * Get the quality issues from the table
   */
  async getQualityIssues(): Promise<Array<{id: string, description: string, severity: string, status: string}>> {
    // Click on the Issues tab if not already active
    if (!(await this.page.isVisible(this.validationIssuesTable))) {
      await this.clickTab('Issues');
    }
    
    await this.page.waitForSelector(this.validationIssuesTable);
    const rows = await this.page.$$(`${this.validationIssuesTable} tbody tr`);
    const issues = [];
    
    for (const row of rows) {
      const cells = await row.$$('td');
      if (cells.length >= 4) {
        const id = await cells[0].textContent() || '';
        const description = await cells[1].textContent() || '';
        const severity = await cells[2].textContent() || '';
        const status = await cells[3].textContent() || '';
        
        issues.push({
          id: id.trim(),
          description: description.trim(),
          severity: severity.trim(),
          status: status.trim()
        });
      }
    }
    
    return issues;
  }

  /**
   * Click the Add Rule button
   */
  async clickAddRule(): Promise<void> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    await this.page.click(this.addRuleButton);
    await this.page.waitForSelector(this.ruleEditorModal);
  }

  /**
   * Click the edit button for a specific rule
   */
  async clickEditRule(index: number): Promise<void> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    const editButtons = await this.page.$$(`${this.validationRulesTable} [data-testid="edit-rule-button"]`);
    if (index < editButtons.length) {
      await editButtons[index].click();
      await this.page.waitForSelector(this.ruleEditorModal);
    } else {
      throw new Error(`Edit button at index ${index} not found`);
    }
  }

  /**
   * Click on a specific issue to view details
   */
  async clickIssue(index: number): Promise<void> {
    // Click on the Issues tab if not already active
    if (!(await this.page.isVisible(this.validationIssuesTable))) {
      await this.clickTab('Issues');
    }
    
    const rows = await this.page.$$(`${this.validationIssuesTable} tbody tr`);
    if (index < rows.length) {
      await rows[index].click();
      await this.page.waitForSelector(this.issueDetailModal);
    } else {
      throw new Error(`Issue at index ${index} not found`);
    }
  }

  /**
   * Filter issues by severity
   */
  async filterIssuesBySeverity(severity: string): Promise<void> {
    // Click on the Issues tab if not already active
    if (!(await this.page.isVisible(this.validationIssuesTable))) {
      await this.clickTab('Issues');
    }
    
    await this.page.click(this.severityFilter);
    await this.page.click(`text="${severity}"`);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Filter rules by type
   */
  async filterRulesByType(ruleType: string): Promise<void> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    await this.page.click(this.ruleTypeFilter);
    await this.page.click(`text="${ruleType}"`);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Filter by quality dimension
   */
  async filterByDimension(dimension: string): Promise<void> {
    await this.page.click(this.dimensionFilter);
    await this.page.click(`text="${dimension}"`);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Filter by status
   */
  async filterByStatus(status: string): Promise<void> {
    await this.page.click(this.statusFilter);
    await this.page.click(`text="${status}"`);
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Search for items using the search input
   */
  async search(searchTerm: string): Promise<void> {
    await this.page.fill(this.searchInput, '');
    await this.page.fill(this.searchInput, searchTerm);
    await this.page.press(this.searchInput, 'Enter');
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Clear all active filters
   */
  async clearFilters(): Promise<void> {
    await this.page.click('[data-testid="clear-filters-button"]');
    await this.page.waitForResponse('**/api/quality/**');
  }

  /**
   * Get the quality trend data from the chart
   */
  async getQualityTrendData(): Promise<Array<{date: string, score: string}>> {
    // Click on the Trends tab if not already active
    if (!(await this.page.isVisible(this.qualityTrendChart))) {
      await this.clickTab('Trends');
    }
    
    await this.page.waitForSelector(this.qualityTrendChart);
    
    // This is a simplified approach to get chart data as it's difficult to extract actual chart data
    // In a real implementation, you might need a more specific approach based on the chart library
    const dataPoints = await this.page.$$(`${this.qualityTrendChart} [data-testid="chart-data-point"]`);
    const trendData = [];
    
    for (const point of dataPoints) {
      const date = await point.getAttribute('data-date') || '';
      const score = await point.getAttribute('data-score') || '';
      
      trendData.push({
        date,
        score
      });
    }
    
    return trendData;
  }

  /**
   * Get the failing rules from the failing rules card
   */
  async getFailingRules(): Promise<Array<{name: string, failureCount: string}>> {
    const ruleItems = await this.page.$$(`${this.failingRulesCard} [data-testid="failing-rule-item"]`);
    const failingRules = [];
    
    for (const item of ruleItems) {
      const name = await item.$eval('[data-testid="rule-name"]', el => el.textContent || '');
      const failureCount = await item.$eval('[data-testid="failure-count"]', el => el.textContent || '');
      
      failingRules.push({
        name: name.trim(),
        failureCount: failureCount.trim()
      });
    }
    
    return failingRules;
  }

  /**
   * Check if the Add Rule button is visible
   */
  async isAddRuleButtonVisible(): Promise<boolean> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    return this.page.isVisible(this.addRuleButton);
  }

  /**
   * Check if Edit Rule buttons are visible
   */
  async areEditRuleButtonsVisible(): Promise<boolean> {
    // Click on the Rules tab if not already active
    if (!(await this.page.isVisible(this.validationRulesTable))) {
      await this.clickTab('Rules');
    }
    
    const editButtons = await this.page.$$(`${this.validationRulesTable} [data-testid="edit-rule-button"]`);
    return editButtons.length > 0;
  }

  /**
   * Close any open modal
   */
  async closeModal(): Promise<void> {
    const closeButton = await this.page.$('[data-testid="close-modal-button"]');
    if (closeButton) {
      await closeButton.click();
      // Wait for modal to disappear
      await this.page.waitForSelector('[data-testid="close-modal-button"]', { state: 'detached' });
    }
  }

  /**
   * Check if the quality page is fully loaded
   */
  async isQualityPageLoaded(): Promise<boolean> {
    const scoreChartVisible = await this.page.isVisible(this.qualityScoreChart);
    const tableVisible = await this.page.isVisible(this.datasetQualityTable);
    const dimensionsCardVisible = await this.page.isVisible(this.qualityDimensionsCard);
    
    return scoreChartVisible && tableVisible && dimensionsCardVisible;
  }
}