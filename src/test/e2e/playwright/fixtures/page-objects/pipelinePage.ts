import { Page } from '@playwright/test'; // v1.32.0
import { pipelines, executions } from '../test-data';

/**
 * Page object representing the pipeline management page of the application
 */
class PipelinePage {
  private page: Page;
  private url: string;
  private pipelineTable: string;
  private pipelineNameColumn: string;
  private pipelineStatusColumn: string;
  private pipelineSourceTypeColumn: string;
  private pipelineTargetColumn: string;
  private pipelineLastRunColumn: string;
  private createPipelineButton: string;
  private refreshButton: string;
  private searchInput: string;
  private statusFilterDropdown: string;
  private sourceTypeFilterDropdown: string;
  private pipelineDetailsCard: string;
  private pipelineNameHeader: string;
  private pipelineStatusBadge: string;
  private pipelineDescription: string;
  private pipelineDagVisualization: string;
  private editPipelineButton: string;
  private runPipelineButton: string;
  private disablePipelineButton: string;
  private enablePipelineButton: string;
  private deletePipelineButton: string;
  private viewHistoryButton: string;
  private backToListButton: string;
  private pipelineForm: string;
  private pipelineNameInput: string;
  private pipelineDescriptionInput: string;
  private pipelineSourceTypeSelect: string;
  private pipelineSourceConfigSection: string;
  private pipelineTargetConfigSection: string;
  private saveButton: string;
  private cancelButton: string;
  private confirmationDialog: string;
  private confirmButton: string;
  private cancelDialogButton: string;
  private executionHistoryTable: string;
  private executionDetailsCard: string;
  private executionStatusBadge: string;
  private executionStartTime: string;
  private executionEndTime: string;
  private executionDuration: string;
  private taskExecutionTable: string;
  private errorDetailsSection: string;
  private errorMessage: string;
  private selfHealingSection: string;
  private backToDetailsButton: string;
  private validationErrorMessages: string;
  private notificationToast: string;

  /**
   * Initialize the PipelinePage object with selectors
   * @param page - Playwright Page object
   */
  constructor(page: Page) {
    this.page = page;
    this.url = '/pipeline-management';
    
    // Table selectors
    this.pipelineTable = '[data-testid="pipeline-table"]';
    this.pipelineNameColumn = '[data-testid="pipeline-name-column"]';
    this.pipelineStatusColumn = '[data-testid="pipeline-status-column"]';
    this.pipelineSourceTypeColumn = '[data-testid="pipeline-source-type-column"]';
    this.pipelineTargetColumn = '[data-testid="pipeline-target-column"]';
    this.pipelineLastRunColumn = '[data-testid="pipeline-last-run-column"]';
    
    // Action buttons
    this.createPipelineButton = '[data-testid="create-pipeline-button"]';
    this.refreshButton = '[data-testid="refresh-button"]';
    
    // Filter controls
    this.searchInput = '[data-testid="search-input"]';
    this.statusFilterDropdown = '[data-testid="status-filter-dropdown"]';
    this.sourceTypeFilterDropdown = '[data-testid="source-type-filter-dropdown"]';
    
    // Pipeline details
    this.pipelineDetailsCard = '[data-testid="pipeline-details-card"]';
    this.pipelineNameHeader = '[data-testid="pipeline-name-header"]';
    this.pipelineStatusBadge = '[data-testid="pipeline-status-badge"]';
    this.pipelineDescription = '[data-testid="pipeline-description"]';
    this.pipelineDagVisualization = '[data-testid="pipeline-dag-visualization"]';
    
    // Detail actions
    this.editPipelineButton = '[data-testid="edit-pipeline-button"]';
    this.runPipelineButton = '[data-testid="run-pipeline-button"]';
    this.disablePipelineButton = '[data-testid="disable-pipeline-button"]';
    this.enablePipelineButton = '[data-testid="enable-pipeline-button"]';
    this.deletePipelineButton = '[data-testid="delete-pipeline-button"]';
    this.viewHistoryButton = '[data-testid="view-history-button"]';
    this.backToListButton = '[data-testid="back-to-list-button"]';
    
    // Form elements
    this.pipelineForm = '[data-testid="pipeline-form"]';
    this.pipelineNameInput = '[data-testid="pipeline-name-input"]';
    this.pipelineDescriptionInput = '[data-testid="pipeline-description-input"]';
    this.pipelineSourceTypeSelect = '[data-testid="pipeline-source-type-select"]';
    this.pipelineSourceConfigSection = '[data-testid="pipeline-source-config-section"]';
    this.pipelineTargetConfigSection = '[data-testid="pipeline-target-config-section"]';
    this.saveButton = '[data-testid="save-button"]';
    this.cancelButton = '[data-testid="cancel-button"]';
    
    // Dialog elements
    this.confirmationDialog = '[data-testid="confirmation-dialog"]';
    this.confirmButton = '[data-testid="confirm-button"]';
    this.cancelDialogButton = '[data-testid="cancel-dialog-button"]';
    
    // Execution history
    this.executionHistoryTable = '[data-testid="execution-history-table"]';
    this.executionDetailsCard = '[data-testid="execution-details-card"]';
    this.executionStatusBadge = '[data-testid="execution-status-badge"]';
    this.executionStartTime = '[data-testid="execution-start-time"]';
    this.executionEndTime = '[data-testid="execution-end-time"]';
    this.executionDuration = '[data-testid="execution-duration"]';
    this.taskExecutionTable = '[data-testid="task-execution-table"]';
    
    // Error and healing sections
    this.errorDetailsSection = '[data-testid="error-details-section"]';
    this.errorMessage = '[data-testid="error-message"]';
    this.selfHealingSection = '[data-testid="self-healing-section"]';
    this.backToDetailsButton = '[data-testid="back-to-details-button"]';
    
    // Validation and notification
    this.validationErrorMessages = '[data-testid="validation-error-message"]';
    this.notificationToast = '[data-testid="notification-toast"]';
  }

  /**
   * Navigate to the pipeline management page
   * @returns Promise that resolves when navigation is complete
   */
  async goto(): Promise<void> {
    await this.page.goto(this.url);
    await this.page.waitForSelector(this.pipelineTable);
  }

  /**
   * Wait for the pipeline page to fully load
   * @returns Promise that resolves when page is loaded
   */
  async waitForPageLoad(): Promise<void> {
    await this.page.waitForSelector(this.pipelineTable);
    await this.page.waitForSelector(this.createPipelineButton);
    await this.page.waitForSelector(this.searchInput);
  }

  /**
   * Get the number of pipelines in the table
   * @returns Promise that resolves with the number of pipelines
   */
  async getPipelineCount(): Promise<number> {
    const rows = await this.page.locator(`${this.pipelineTable} > tbody > tr`).count();
    return rows;
  }

  /**
   * Search for pipelines by name
   * @param searchTerm - The search term to use
   * @returns Promise that resolves when search is complete
   */
  async searchPipeline(searchTerm: string): Promise<void> {
    await this.page.fill(this.searchInput, '');
    await this.page.fill(this.searchInput, searchTerm);
    await this.page.press(this.searchInput, 'Enter');
    // Wait for the table to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Filter pipelines by status
   * @param status - The status to filter by
   * @returns Promise that resolves when filter is applied
   */
  async filterByStatus(status: string): Promise<void> {
    await this.page.click(this.statusFilterDropdown);
    await this.page.click(`[data-value="${status}"]`);
    // Wait for the table to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Filter pipelines by source type
   * @param sourceType - The source type to filter by
   * @returns Promise that resolves when filter is applied
   */
  async filterBySourceType(sourceType: string): Promise<void> {
    await this.page.click(this.sourceTypeFilterDropdown);
    await this.page.click(`[data-value="${sourceType}"]`);
    // Wait for the table to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Select a pipeline from the table by name
   * @param pipelineName - The name of the pipeline to select
   * @returns Promise that resolves when pipeline is selected
   */
  async selectPipeline(pipelineName: string): Promise<void> {
    await this.page.click(`${this.pipelineTable} >> text="${pipelineName}"`);
    await this.page.waitForSelector(this.pipelineDetailsCard);
  }

  /**
   * Click the create pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickCreatePipeline(): Promise<void> {
    await this.page.click(this.createPipelineButton);
    await this.page.waitForSelector(this.pipelineForm);
  }

  /**
   * Click the refresh button
   * @returns Promise that resolves when button is clicked
   */
  async clickRefresh(): Promise<void> {
    await this.page.click(this.refreshButton);
    // Wait for the table to update
    await this.page.waitForTimeout(500);
  }

  /**
   * Click the edit pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickEditPipeline(): Promise<void> {
    await this.page.click(this.editPipelineButton);
    await this.page.waitForSelector(this.pipelineForm);
  }

  /**
   * Click the run pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickRunPipeline(): Promise<void> {
    await this.page.click(this.runPipelineButton);
    await this.page.waitForSelector(this.confirmationDialog);
  }

  /**
   * Click the disable pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickDisablePipeline(): Promise<void> {
    await this.page.click(this.disablePipelineButton);
    await this.page.waitForSelector(this.confirmationDialog);
  }

  /**
   * Click the enable pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickEnablePipeline(): Promise<void> {
    await this.page.click(this.enablePipelineButton);
    await this.page.waitForSelector(this.confirmationDialog);
  }

  /**
   * Click the delete pipeline button
   * @returns Promise that resolves when button is clicked
   */
  async clickDeletePipeline(): Promise<void> {
    await this.page.click(this.deletePipelineButton);
    await this.page.waitForSelector(this.confirmationDialog);
  }

  /**
   * Click the view history button
   * @returns Promise that resolves when button is clicked
   */
  async clickViewHistory(): Promise<void> {
    await this.page.click(this.viewHistoryButton);
    await this.page.waitForSelector(this.executionHistoryTable);
  }

  /**
   * Click the back to list button
   * @returns Promise that resolves when button is clicked
   */
  async clickBackToList(): Promise<void> {
    await this.page.click(this.backToListButton);
    await this.page.waitForSelector(this.pipelineTable);
  }

  /**
   * Click the back to details button
   * @returns Promise that resolves when button is clicked
   */
  async clickBackToDetails(): Promise<void> {
    await this.page.click(this.backToDetailsButton);
    await this.page.waitForSelector(this.pipelineDetailsCard);
  }

  /**
   * Confirm the current dialog
   * @returns Promise that resolves when dialog is confirmed
   */
  async confirmDialog(): Promise<void> {
    await this.page.click(this.confirmButton);
    await this.page.waitForSelector(this.confirmationDialog, { state: 'hidden' });
  }

  /**
   * Cancel the current dialog
   * @returns Promise that resolves when dialog is canceled
   */
  async cancelDialog(): Promise<void> {
    await this.page.click(this.cancelDialogButton);
    await this.page.waitForSelector(this.confirmationDialog, { state: 'hidden' });
  }

  /**
   * Fill the pipeline creation/edit form
   * @param pipelineData - The data to fill in the form
   * @returns Promise that resolves when form is filled
   */
  async fillPipelineForm(pipelineData: object): Promise<void> {
    // Clear and fill basic fields
    await this.page.fill(this.pipelineNameInput, '');
    await this.page.fill(this.pipelineNameInput, pipelineData['name']);
    
    await this.page.fill(this.pipelineDescriptionInput, '');
    await this.page.fill(this.pipelineDescriptionInput, pipelineData['description']);
    
    // Select source type
    await this.page.selectOption(this.pipelineSourceTypeSelect, pipelineData['sourceType']);
    
    // Fill source configuration fields based on selected source type
    // This would depend on the specific fields for each source type
    
    // Fill target configuration
    // This would depend on the specific fields for the target
  }

  /**
   * Save the pipeline form
   * @returns Promise that resolves when form is saved
   */
  async savePipelineForm(): Promise<void> {
    await this.page.click(this.saveButton);
    // Wait for either notification or return to list/details
    await Promise.race([
      this.page.waitForSelector(this.notificationToast),
      this.page.waitForSelector(this.pipelineTable),
      this.page.waitForSelector(this.pipelineDetailsCard)
    ]);
  }

  /**
   * Cancel the pipeline form
   * @returns Promise that resolves when form is canceled
   */
  async cancelPipelineForm(): Promise<void> {
    await this.page.click(this.cancelButton);
    await this.page.waitForSelector(this.pipelineForm, { state: 'hidden' });
  }

  /**
   * Select an execution from the history table by index
   * @param index - The index of the execution to select
   * @returns Promise that resolves when execution is selected
   */
  async selectExecution(index: number): Promise<void> {
    await this.page.click(`${this.executionHistoryTable} > tbody > tr:nth-child(${index + 1})`);
    await this.page.waitForSelector(this.executionDetailsCard);
  }

  /**
   * Get all validation error messages
   * @returns Promise that resolves with array of error messages
   */
  async getValidationErrors(): Promise<string[]> {
    const errors = await this.page.locator(this.validationErrorMessages).allTextContents();
    return errors;
  }

  /**
   * Get the text of the current notification toast
   * @returns Promise that resolves with notification text or null if not present
   */
  async getNotificationText(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.notificationToast);
    if (isVisible) {
      return this.page.textContent(this.notificationToast);
    }
    return null;
  }

  /**
   * Wait for a notification toast to appear
   * @returns Promise that resolves when notification appears
   */
  async waitForNotification(): Promise<void> {
    await this.page.waitForSelector(this.notificationToast);
  }

  /**
   * Get the status of the currently selected pipeline
   * @returns Promise that resolves with the pipeline status text
   */
  async getPipelineStatus(): Promise<string> {
    return this.page.textContent(this.pipelineStatusBadge);
  }

  /**
   * Check if a pipeline with the given name exists in the table
   * @param pipelineName - The name of the pipeline to check for
   * @returns Promise that resolves with true if pipeline exists, false otherwise
   */
  async isPipelineInTable(pipelineName: string): Promise<boolean> {
    const count = await this.page.locator(`${this.pipelineTable} >> text="${pipelineName}"`).count();
    return count > 0;
  }

  /**
   * Get the number of executions in the history table
   * @returns Promise that resolves with the number of executions
   */
  async getExecutionCount(): Promise<number> {
    const rows = await this.page.locator(`${this.executionHistoryTable} > tbody > tr`).count();
    return rows;
  }

  /**
   * Get the status of the currently selected execution
   * @returns Promise that resolves with the execution status text
   */
  async getExecutionStatus(): Promise<string> {
    return this.page.textContent(this.executionStatusBadge);
  }

  /**
   * Get the number of tasks in the task execution table
   * @returns Promise that resolves with the number of tasks
   */
  async getTaskCount(): Promise<number> {
    const rows = await this.page.locator(`${this.taskExecutionTable} > tbody > tr`).count();
    return rows;
  }

  /**
   * Get the error message for a failed execution
   * @returns Promise that resolves with error message or null if not present
   */
  async getErrorMessage(): Promise<string | null> {
    const isVisible = await this.page.isVisible(this.errorDetailsSection);
    if (isVisible) {
      return this.page.textContent(this.errorMessage);
    }
    return null;
  }

  /**
   * Check if self-healing information is displayed
   * @returns Promise that resolves with true if self-healing info is present, false otherwise
   */
  async hasSelfHealingInformation(): Promise<boolean> {
    return this.page.isVisible(this.selfHealingSection);
  }

  /**
   * Create a new pipeline with the given data
   * @param pipelineData - The data to use for the new pipeline
   * @returns Promise that resolves when pipeline is created
   */
  async createNewPipeline(pipelineData: object): Promise<void> {
    await this.clickCreatePipeline();
    await this.fillPipelineForm(pipelineData);
    await this.savePipelineForm();
  }

  /**
   * Edit the currently selected pipeline with the given data
   * @param pipelineData - The data to update the pipeline with
   * @returns Promise that resolves when pipeline is edited
   */
  async editCurrentPipeline(pipelineData: object): Promise<void> {
    await this.clickEditPipeline();
    await this.fillPipelineForm(pipelineData);
    await this.savePipelineForm();
  }

  /**
   * Run the currently selected pipeline
   * @returns Promise that resolves when pipeline is triggered
   */
  async runCurrentPipeline(): Promise<void> {
    await this.clickRunPipeline();
    await this.confirmDialog();
    await this.waitForNotification();
  }

  /**
   * Disable the currently selected pipeline
   * @returns Promise that resolves when pipeline is disabled
   */
  async disableCurrentPipeline(): Promise<void> {
    await this.clickDisablePipeline();
    await this.confirmDialog();
    await this.waitForNotification();
    
    // Verify the pipeline is now disabled
    const status = await this.getPipelineStatus();
    if (status !== 'Disabled') {
      throw new Error(`Expected pipeline status to be 'Disabled', but was '${status}'`);
    }
  }

  /**
   * Enable the currently selected pipeline
   * @returns Promise that resolves when pipeline is enabled
   */
  async enableCurrentPipeline(): Promise<void> {
    await this.clickEnablePipeline();
    await this.confirmDialog();
    await this.waitForNotification();
    
    // Verify the pipeline is now active
    const status = await this.getPipelineStatus();
    if (status !== 'Active') {
      throw new Error(`Expected pipeline status to be 'Active', but was '${status}'`);
    }
  }

  /**
   * Delete the currently selected pipeline
   * @returns Promise that resolves when pipeline is deleted
   */
  async deleteCurrentPipeline(): Promise<void> {
    await this.clickDeletePipeline();
    await this.confirmDialog();
    await this.waitForNotification();
    
    // Verify we're back at the pipeline list
    await this.page.waitForSelector(this.pipelineTable);
  }

  /**
   * View the execution history of the currently selected pipeline
   * @returns Promise that resolves when history is displayed
   */
  async viewCurrentPipelineHistory(): Promise<void> {
    await this.clickViewHistory();
    await this.page.waitForSelector(this.executionHistoryTable);
  }

  /**
   * Check if pipeline details are currently visible
   * @returns Promise that resolves with true if details are visible, false otherwise
   */
  async isPipelineDetailsVisible(): Promise<boolean> {
    return this.page.isVisible(this.pipelineDetailsCard);
  }

  /**
   * Check if pipeline form is currently visible
   * @returns Promise that resolves with true if form is visible, false otherwise
   */
  async isPipelineFormVisible(): Promise<boolean> {
    return this.page.isVisible(this.pipelineForm);
  }

  /**
   * Check if execution history is currently visible
   * @returns Promise that resolves with true if history is visible, false otherwise
   */
  async isExecutionHistoryVisible(): Promise<boolean> {
    return this.page.isVisible(this.executionHistoryTable);
  }

  /**
   * Check if execution details are currently visible
   * @returns Promise that resolves with true if details are visible, false otherwise
   */
  async isExecutionDetailsVisible(): Promise<boolean> {
    return this.page.isVisible(this.executionDetailsCard);
  }

  /**
   * Check if DAG visualization is currently visible
   * @returns Promise that resolves with true if DAG is visible, false otherwise
   */
  async isDagVisualizationVisible(): Promise<boolean> {
    return this.page.isVisible(this.pipelineDagVisualization);
  }
}

export default PipelinePage;