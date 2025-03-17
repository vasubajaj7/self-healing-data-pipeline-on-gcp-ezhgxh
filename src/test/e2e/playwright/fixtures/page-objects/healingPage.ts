import { Page } from "@playwright/test"; // ^1.32.0
import { healing } from "../test-data";

/**
 * Page object representing the self-healing page of the application
 */
class HealingPage {
  // The page instance
  private page: Page;
  
  // URL and selectors
  readonly url: string;
  readonly pageTitle: string;
  
  // Tab selectors
  readonly tabList: string;
  readonly dashboardTab: string;
  readonly activityLogTab: string;
  readonly modelManagementTab: string;
  readonly configurationTab: string;
  
  // Dashboard selectors
  readonly activeIssuesTable: string;
  readonly healingActionsTable: string;
  readonly successRateChart: string;
  readonly modelHealthCard: string;
  readonly modelPerformanceCard: string;
  
  // Filter selectors
  readonly issueTypeFilter: string;
  readonly issueStatusFilter: string;
  readonly issueSeverityFilter: string;
  readonly refreshButton: string;
  readonly lastUpdatedText: string;
  
  // Modal selectors
  readonly issueDetailsModal: string;
  readonly manualFixButton: string;
  readonly confirmFixButton: string;
  readonly cancelFixButton: string;
  readonly successNotification: string;
  
  // Configuration selectors
  readonly configurationForm: string;
  readonly autonomousModeSelect: string;
  readonly confidenceThresholdSlider: string;
  readonly maxRetryAttemptsInput: string;
  readonly approvalRequiredSelect: string;
  readonly learningModeSelect: string;
  readonly saveSettingsButton: string;
  
  // Rule selectors
  readonly correctionRulesList: string;
  readonly ruleEditor: string;
  readonly autoApplyToggle: string;
  readonly saveRuleButton: string;
  
  // Model management selectors
  readonly modelsList: string;
  readonly modelDetailsPanel: string;
  
  // Activity log selectors
  readonly activityLogTable: string;
  
  /**
   * Initialize the HealingPage object with selectors
   * @param page - Playwright page object
   */
  constructor(page: Page) {
    this.page = page;
    this.url = "/self-healing";
    this.pageTitle = "h1:has-text('Self-Healing Dashboard')";
    
    // Tab selectors
    this.tabList = "[role='tablist']";
    this.dashboardTab = "button[role='tab']:has-text('Dashboard')";
    this.activityLogTab = "button[role='tab']:has-text('Activity Log')";
    this.modelManagementTab = "button[role='tab']:has-text('ML Models')";
    this.configurationTab = "button[role='tab']:has-text('Configuration')";
    
    // Dashboard selectors
    this.activeIssuesTable = "table:has-text('Active Issues')";
    this.healingActionsTable = "table:has-text('Healing Actions')";
    this.successRateChart = "[data-testid='success-rate-chart']";
    this.modelHealthCard = "[data-testid='model-health-card']";
    this.modelPerformanceCard = "[data-testid='model-performance-card']";
    
    // Filter selectors
    this.issueTypeFilter = "[data-testid='issue-type-filter']";
    this.issueStatusFilter = "[data-testid='issue-status-filter']";
    this.issueSeverityFilter = "[data-testid='issue-severity-filter']";
    this.refreshButton = "button:has-text('Refresh')";
    this.lastUpdatedText = "[data-testid='last-updated']";
    
    // Modal selectors
    this.issueDetailsModal = "[role='dialog']:has-text('Issue Details')";
    this.manualFixButton = "button:has-text('Manual Fix')";
    this.confirmFixButton = "button:has-text('Confirm Fix')";
    this.cancelFixButton = "button:has-text('Cancel')";
    this.successNotification = "[role='alert']:has-text('Success')";
    
    // Configuration selectors
    this.configurationForm = "form:has-text('Self-Healing Settings')";
    this.autonomousModeSelect = "select[name='autonomousMode']";
    this.confidenceThresholdSlider = "input[type='range'][name='confidenceThreshold']";
    this.maxRetryAttemptsInput = "input[name='maxRetryAttempts']";
    this.approvalRequiredSelect = "select[name='approvalRequired']";
    this.learningModeSelect = "select[name='learningMode']";
    this.saveSettingsButton = "button:has-text('Save Changes')";
    
    // Rule selectors
    this.correctionRulesList = "table:has-text('Correction Rules')";
    this.ruleEditor = "[data-testid='rule-editor']";
    this.autoApplyToggle = "label:has-text('Auto-Apply') input[type='checkbox']";
    this.saveRuleButton = "button:has-text('Save Rule')";
    
    // Model management selectors
    this.modelsList = "table:has-text('AI Models')";
    this.modelDetailsPanel = "[data-testid='model-details-panel']";
    
    // Activity log selectors
    this.activityLogTable = "table:has-text('Activity Log')";
  }
  
  /**
   * Navigate to the self-healing page
   * @returns Promise that resolves when navigation is complete
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.pageTitle);
  }
  
  /**
   * Mock the healing API responses
   * @returns Promise that resolves when mocking is complete
   */
  async mockHealingData(): Promise<void> {
    await this.page.route('**/api/healing/issues*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(healing.list.data)
      });
    });
    
    await this.page.route('**/api/healing/issue/*', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(healing.details.data)
      });
    });
  }
  
  /**
   * Wait for the healing page to fully load
   * @returns Promise that resolves when page is loaded
   */
  async waitForPageLoaded(): Promise<void> {
    await this.page.waitForSelector(this.activeIssuesTable);
    await this.page.waitForSelector(this.successRateChart);
    await this.page.waitForSelector(this.modelHealthCard);
  }
  
  /**
   * Switch to a specific tab on the healing page
   * @param tabName - The name of the tab to switch to
   * @returns Promise that resolves when tab switch is complete
   */
  async switchToTab(tabName: string): Promise<void> {
    let tabSelector: string;
    
    switch (tabName.toLowerCase()) {
      case 'dashboard':
        tabSelector = this.dashboardTab;
        break;
      case 'activity log':
        tabSelector = this.activityLogTab;
        break;
      case 'ml models':
      case 'model management':
        tabSelector = this.modelManagementTab;
        break;
      case 'configuration':
        tabSelector = this.configurationTab;
        break;
      default:
        throw new Error(`Unknown tab name: ${tabName}`);
    }
    
    await this.page.click(tabSelector);
    
    // Wait for the content to be visible based on the selected tab
    switch (tabName.toLowerCase()) {
      case 'dashboard':
        await this.page.waitForSelector(this.activeIssuesTable);
        break;
      case 'activity log':
        await this.page.waitForSelector(this.activityLogTable);
        break;
      case 'ml models':
      case 'model management':
        await this.page.waitForSelector(this.modelsList);
        break;
      case 'configuration':
        await this.page.waitForSelector(this.configurationForm);
        break;
    }
  }
  
  /**
   * Get the count of active issues in the table
   * @returns Promise that resolves with the number of active issues
   */
  async getActiveIssuesCount(): Promise<number> {
    const rows = await this.page.$$(`${this.activeIssuesTable} tbody tr`);
    return rows.length;
  }
  
  /**
   * Get the success rate percentage from the chart
   * @returns Promise that resolves with the success rate percentage
   */
  async getSuccessRate(): Promise<string> {
    const successRateText = await this.page.textContent(`${this.successRateChart} .percentage-value`);
    return successRateText || '';
  }
  
  /**
   * Get the model health status indicators
   * @returns Promise that resolves with the model health status object
   */
  async getModelHealthStatus(): Promise<object> {
    const modelHealth = {
      driftStatus: await this.page.textContent(`${this.modelHealthCard} [data-testid='drift-status']`),
      featureHealth: await this.page.textContent(`${this.modelHealthCard} [data-testid='feature-health']`),
      predictions: await this.page.textContent(`${this.modelHealthCard} [data-testid='predictions-status']`),
      inferenceTime: await this.page.textContent(`${this.modelHealthCard} [data-testid='inference-time-status']`)
    };
    
    return modelHealth;
  }
  
  /**
   * Get the model performance metrics
   * @returns Promise that resolves with the performance metrics object
   */
  async getModelPerformanceMetrics(): Promise<object> {
    const modelPerformance = {
      accuracy: await this.page.textContent(`${this.modelPerformanceCard} [data-testid='metric-accuracy']`),
      confidence: await this.page.textContent(`${this.modelPerformanceCard} [data-testid='metric-confidence']`),
      precision: await this.page.textContent(`${this.modelPerformanceCard} [data-testid='metric-precision']`),
      recall: await this.page.textContent(`${this.modelPerformanceCard} [data-testid='metric-recall']`)
    };
    
    return modelPerformance;
  }
  
  /**
   * Filter active issues by type
   * @param issueType - The type of issue to filter by
   * @returns Promise that resolves when filtering is complete
   */
  async filterIssuesByType(issueType: string): Promise<void> {
    await this.page.click(this.issueTypeFilter);
    await this.page.click(`li:has-text('${issueType}')`);
    // Wait for the table to update with filtered results
    await this.page.waitForTimeout(500);
  }
  
  /**
   * Filter active issues by status
   * @param status - The status to filter by
   * @returns Promise that resolves when filtering is complete
   */
  async filterIssuesByStatus(status: string): Promise<void> {
    await this.page.click(this.issueStatusFilter);
    await this.page.click(`li:has-text('${status}')`);
    // Wait for the table to update with filtered results
    await this.page.waitForTimeout(500);
  }
  
  /**
   * Filter active issues by severity
   * @param severity - The severity to filter by
   * @returns Promise that resolves when filtering is complete
   */
  async filterIssuesBySeverity(severity: string): Promise<void> {
    await this.page.click(this.issueSeverityFilter);
    await this.page.click(`li:has-text('${severity}')`);
    // Wait for the table to update with filtered results
    await this.page.waitForTimeout(500);
  }
  
  /**
   * Clear all active filters
   * @returns Promise that resolves when filters are cleared
   */
  async clearFilters(): Promise<void> {
    await this.page.click("button:has-text('Clear Filters')");
    // Wait for the table to update with unfiltered results
    await this.page.waitForTimeout(500);
  }
  
  /**
   * Click the refresh button to update the data
   * @returns Promise that resolves when refresh is complete
   */
  async clickRefresh(): Promise<void> {
    const beforeText = await this.getLastUpdatedTime();
    await this.page.click(this.refreshButton);
    
    // Wait for loading indicator to disappear
    await this.page.waitForSelector("[data-testid='loading-indicator']", { state: "hidden" });
    
    // Wait for last updated text to change
    const waitForUpdate = async () => {
      const currentText = await this.getLastUpdatedTime();
      return currentText !== beforeText;
    };
    
    await this.page.waitForFunction(waitForUpdate);
  }
  
  /**
   * Get the last updated time text
   * @returns Promise that resolves with the last updated time text
   */
  async getLastUpdatedTime(): Promise<string> {
    return await this.page.textContent(this.lastUpdatedText) || '';
  }
  
  /**
   * Open the details modal for a specific issue
   * @param index - The zero-based index of the issue in the table
   * @returns Promise that resolves when the modal is open
   */
  async openIssueDetails(index: number): Promise<void> {
    await this.page.click(`${this.activeIssuesTable} tbody tr:nth-child(${index + 1})`);
    await this.page.waitForSelector(this.issueDetailsModal);
  }
  
  /**
   * Get the details of the currently open issue
   * @returns Promise that resolves with the issue details object
   */
  async getIssueDetails(): Promise<object> {
    const detailsModal = this.issueDetailsModal;
    
    const issueDetails = {
      description: await this.page.textContent(`${detailsModal} [data-testid='issue-description']`),
      severity: await this.page.textContent(`${detailsModal} [data-testid='issue-severity']`),
      status: await this.page.textContent(`${detailsModal} [data-testid='issue-status']`),
      confidence: await this.page.textContent(`${detailsModal} [data-testid='issue-confidence']`),
      detectedAt: await this.page.textContent(`${detailsModal} [data-testid='issue-detected-at']`),
      errorDetails: await this.page.textContent(`${detailsModal} [data-testid='issue-error-details']`),
      rootCause: await this.page.textContent(`${detailsModal} [data-testid='issue-root-cause']`),
      healingAction: await this.page.textContent(`${detailsModal} [data-testid='issue-healing-action']`)
    };
    
    return issueDetails;
  }
  
  /**
   * Trigger a manual fix for the currently open issue
   * @returns Promise that resolves when the fix is triggered
   */
  async triggerManualFix(): Promise<void> {
    await this.page.click(this.manualFixButton);
    await this.page.waitForSelector("dialog:has-text('Confirm Action')");
  }
  
  /**
   * Confirm the manual fix in the confirmation dialog
   * @returns Promise that resolves when the fix is confirmed
   */
  async confirmManualFix(): Promise<void> {
    await this.page.click(this.confirmFixButton);
    await this.page.waitForSelector(this.successNotification);
  }
  
  /**
   * Cancel the manual fix in the confirmation dialog
   * @returns Promise that resolves when the fix is canceled
   */
  async cancelManualFix(): Promise<void> {
    await this.page.click(this.cancelFixButton);
    await this.page.waitForSelector("dialog:has-text('Confirm Action')", { state: "hidden" });
  }
  
  /**
   * Close the issue details modal
   * @returns Promise that resolves when the modal is closed
   */
  async closeIssueDetails(): Promise<void> {
    await this.page.click(`${this.issueDetailsModal} button[aria-label='Close']`);
    await this.page.waitForSelector(this.issueDetailsModal, { state: "hidden" });
  }
  
  /**
   * Get the current configuration settings
   * @returns Promise that resolves with the configuration settings object
   */
  async getConfigurationSettings(): Promise<object> {
    await this.switchToTab('Configuration');
    
    const autonomousMode = await this.page.$eval(this.autonomousModeSelect, el => (el as HTMLSelectElement).value);
    const confidenceThreshold = await this.page.$eval(this.confidenceThresholdSlider, el => (el as HTMLInputElement).value);
    const maxRetryAttempts = await this.page.$eval(this.maxRetryAttemptsInput, el => (el as HTMLInputElement).value);
    const approvalRequired = await this.page.$eval(this.approvalRequiredSelect, el => (el as HTMLSelectElement).value);
    const learningMode = await this.page.$eval(this.learningModeSelect, el => (el as HTMLSelectElement).value);
    
    return {
      autonomousMode,
      confidenceThreshold,
      maxRetryAttempts,
      approvalRequired,
      learningMode
    };
  }
  
  /**
   * Set the autonomous mode setting
   * @param mode - The mode to set (e.g., 'Automatic', 'Semi-Automatic', 'Manual')
   * @returns Promise that resolves when the setting is changed
   */
  async setAutonomousMode(mode: string): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.selectOption(this.autonomousModeSelect, mode);
  }
  
  /**
   * Set the confidence threshold setting
   * @param threshold - The threshold percentage (0-100)
   * @returns Promise that resolves when the setting is changed
   */
  async setConfidenceThreshold(threshold: number): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.fill(this.confidenceThresholdSlider, threshold.toString());
  }
  
  /**
   * Set the maximum retry attempts setting
   * @param attempts - The number of retry attempts
   * @returns Promise that resolves when the setting is changed
   */
  async setMaxRetryAttempts(attempts: number): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.fill(this.maxRetryAttemptsInput, attempts.toString());
  }
  
  /**
   * Set the approval required setting
   * @param approvalOption - The approval option (e.g., 'Always', 'High Impact Only', 'Never')
   * @returns Promise that resolves when the setting is changed
   */
  async setApprovalRequired(approvalOption: string): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.selectOption(this.approvalRequiredSelect, approvalOption);
  }
  
  /**
   * Set the learning mode setting
   * @param mode - The learning mode (e.g., 'Active', 'Passive', 'Disabled')
   * @returns Promise that resolves when the setting is changed
   */
  async setLearningMode(mode: string): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.selectOption(this.learningModeSelect, mode);
  }
  
  /**
   * Save the current configuration settings
   * @returns Promise that resolves when settings are saved
   */
  async saveConfigurationSettings(): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.click(this.saveSettingsButton);
    await this.page.waitForSelector(this.successNotification);
  }
  
  /**
   * Get the list of correction rules
   * @returns Promise that resolves with an array of correction rule objects
   */
  async getCorrectionRules(): Promise<Array<object>> {
    await this.switchToTab('Configuration');
    
    const rows = await this.page.$$(`${this.correctionRulesList} tbody tr`);
    const rules = [];
    
    for (const row of rows) {
      const name = await row.$eval('td:nth-child(1)', el => el.textContent);
      const type = await row.$eval('td:nth-child(2)', el => el.textContent);
      const autoApply = await row.$eval('td:nth-child(3)', el => el.textContent);
      
      rules.push({
        name,
        type,
        autoApply: autoApply === 'Yes'
      });
    }
    
    return rules;
  }
  
  /**
   * Open the editor for a specific correction rule
   * @param index - The zero-based index of the rule in the table
   * @returns Promise that resolves when the editor is open
   */
  async openRuleEditor(index: number): Promise<void> {
    await this.switchToTab('Configuration');
    await this.page.click(`${this.correctionRulesList} tbody tr:nth-child(${index + 1})`);
    await this.page.waitForSelector(this.ruleEditor);
  }
  
  /**
   * Toggle the auto-apply setting for the current rule
   * @returns Promise that resolves when the setting is toggled
   */
  async toggleAutoApply(): Promise<void> {
    await this.page.click(this.autoApplyToggle);
    // Wait for the toggle to change state
    await this.page.waitForTimeout(300);
  }
  
  /**
   * Save the current rule settings
   * @returns Promise that resolves when the rule is saved
   */
  async saveRule(): Promise<void> {
    await this.page.click(this.saveRuleButton);
    await this.page.waitForSelector(this.successNotification);
    await this.page.waitForSelector(this.ruleEditor, { state: "hidden" });
  }
  
  /**
   * Get the list of AI models
   * @returns Promise that resolves with an array of model objects
   */
  async getModels(): Promise<Array<object>> {
    await this.switchToTab('Model Management');
    
    const rows = await this.page.$$(`${this.modelsList} tbody tr`);
    const models = [];
    
    for (const row of rows) {
      const name = await row.$eval('td:nth-child(1)', el => el.textContent);
      const type = await row.$eval('td:nth-child(2)', el => el.textContent);
      const status = await row.$eval('td:nth-child(3)', el => el.textContent);
      
      models.push({
        name,
        type,
        status
      });
    }
    
    return models;
  }
  
  /**
   * Open the details panel for a specific model
   * @param index - The zero-based index of the model in the table
   * @returns Promise that resolves when the details panel is open
   */
  async openModelDetails(index: number): Promise<void> {
    await this.switchToTab('Model Management');
    await this.page.click(`${this.modelsList} tbody tr:nth-child(${index + 1})`);
    await this.page.waitForSelector(this.modelDetailsPanel);
  }
  
  /**
   * Get the details of the currently selected model
   * @returns Promise that resolves with the model details object
   */
  async getModelDetails(): Promise<object> {
    const detailsPanel = this.modelDetailsPanel;
    
    const modelDetails = {
      name: await this.page.textContent(`${detailsPanel} [data-testid='model-name']`),
      type: await this.page.textContent(`${detailsPanel} [data-testid='model-type']`),
      version: await this.page.textContent(`${detailsPanel} [data-testid='model-version']`),
      lastUpdated: await this.page.textContent(`${detailsPanel} [data-testid='model-last-updated']`),
      trainingData: await this.page.textContent(`${detailsPanel} [data-testid='model-training-data']`),
      accuracy: await this.page.textContent(`${detailsPanel} [data-testid='model-accuracy']`),
      rmse: await this.page.textContent(`${detailsPanel} [data-testid='model-rmse']`),
      inferenceTime: await this.page.textContent(`${detailsPanel} [data-testid='model-inference-time']`)
    };
    
    return modelDetails;
  }
  
  /**
   * Get the activity log entries
   * @returns Promise that resolves with an array of activity log objects
   */
  async getActivityLog(): Promise<Array<object>> {
    await this.switchToTab('Activity Log');
    
    const rows = await this.page.$$(`${this.activityLogTable} tbody tr`);
    const activities = [];
    
    for (const row of rows) {
      const timestamp = await row.$eval('td:nth-child(1)', el => el.textContent);
      const action = await row.$eval('td:nth-child(2)', el => el.textContent);
      const status = await row.$eval('td:nth-child(3)', el => el.textContent);
      const details = await row.$eval('td:nth-child(4)', el => el.textContent);
      
      activities.push({
        timestamp,
        action,
        status,
        details
      });
    }
    
    return activities;
  }
  
  /**
   * Check if the manual fix button is visible for the current issue
   * @returns Promise that resolves with true if the button is visible, false otherwise
   */
  async isManualFixButtonVisible(): Promise<boolean> {
    const button = await this.page.$(this.manualFixButton);
    return button !== null;
  }
  
  /**
   * Check if the configuration settings are editable
   * @returns Promise that resolves with true if settings are editable, false otherwise
   */
  async areSettingsEditable(): Promise<boolean> {
    await this.switchToTab('Configuration');
    
    const autonomousModeDisabled = await this.page.$eval(
      this.autonomousModeSelect, 
      el => (el as HTMLSelectElement).disabled
    );
    
    const confidenceThresholdDisabled = await this.page.$eval(
      this.confidenceThresholdSlider, 
      el => (el as HTMLInputElement).disabled
    );
    
    return !autonomousModeDisabled && !confidenceThresholdDisabled;
  }
  
  /**
   * Check if a specific tab is accessible
   * @param tabName - The name of the tab to check
   * @returns Promise that resolves with true if the tab is accessible, false otherwise
   */
  async isTabAccessible(tabName: string): Promise<boolean> {
    let tabSelector: string;
    
    switch (tabName.toLowerCase()) {
      case 'dashboard':
        tabSelector = this.dashboardTab;
        break;
      case 'activity log':
        tabSelector = this.activityLogTab;
        break;
      case 'ml models':
      case 'model management':
        tabSelector = this.modelManagementTab;
        break;
      case 'configuration':
        tabSelector = this.configurationTab;
        break;
      default:
        throw new Error(`Unknown tab name: ${tabName}`);
    }
    
    const tab = await this.page.$(tabSelector);
    return tab !== null;
  }
}

export default HealingPage;