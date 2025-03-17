import { test, expect } from '@playwright/test'; // v1.32.0
import PipelinePage from '../fixtures/page-objects/pipelinePage';
import { test as authTest } from '../fixtures/auth.setup';
import { pipelineData } from '../fixtures/test-data';

// Mock API responses for pipeline management endpoints
async function setupApiMocks(page) {
  // Mock pipeline list endpoint
  await page.route('**/api/pipelines', (route) => {
    if (route.request().method() === 'GET') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          pipelines: pipelineData.pipelines,
          pagination: {
            page: 1,
            pageSize: 10,
            totalItems: pipelineData.pipelines.length,
            totalPages: 1
          }
        }),
      });
    }
    
    if (route.request().method() === 'POST') {
      const requestBody = route.request().postDataJSON();
      return route.fulfill({
        status: 201,
        contentType: 'application/json',
        body: JSON.stringify({ id: 'new-pipeline-id', ...requestBody }),
      });
    }
    
    return route.continue();
  });

  // Mock pipeline detail, update, and delete endpoints
  await page.route('**/api/pipelines/:id', (route) => {
    const url = route.request().url();
    const id = url.split('/').pop();
    
    if (route.request().method() === 'GET') {
      const pipeline = pipelineData.pipelines.find(p => p.id === id);
      if (pipeline) {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify(pipeline),
        });
      }
      return route.fulfill({
        status: 404,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Pipeline not found' }),
      });
    }
    
    if (route.request().method() === 'PUT') {
      const requestBody = route.request().postDataJSON();
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ id, ...requestBody }),
      });
    }
    
    if (route.request().method() === 'DELETE') {
      return route.fulfill({ status: 204 });
    }
    
    return route.continue();
  });

  // Mock pipeline executions endpoint
  await page.route('**/api/pipelines/:id/executions', (route) => {
    const url = route.request().url();
    const id = url.split('/').slice(-2)[0];
    const executions = pipelineData.executions.filter(e => e.pipelineId === id);
    
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(executions),
    });
  });

  // Mock pipeline run endpoint
  await page.route('**/api/pipelines/:id/run', (route) => {
    if (route.request().method() === 'POST') {
      return route.fulfill({
        status: 202,
        contentType: 'application/json',
        body: JSON.stringify({ executionId: 'new-execution-id', status: 'Running' }),
      });
    }
    return route.continue();
  });

  // Mock pipeline disable endpoint
  await page.route('**/api/pipelines/:id/disable', (route) => {
    if (route.request().method() === 'POST') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ...pipelineData.pipelines[0], status: 'Disabled' }),
      });
    }
    return route.continue();
  });

  // Mock pipeline enable endpoint
  await page.route('**/api/pipelines/:id/enable', (route) => {
    if (route.request().method() === 'POST') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ...pipelineData.pipelines[0], status: 'Active' }),
      });
    }
    return route.continue();
  });

  // Mock execution details endpoint
  await page.route('**/api/executions/:id', (route) => {
    const url = route.request().url();
    const id = url.split('/').pop();
    const execution = pipelineData.executions.find(e => e.id === id);
    
    if (execution) {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(execution),
      });
    }
    
    return route.fulfill({
      status: 404,
      contentType: 'application/json',
      body: JSON.stringify({ error: 'Execution not found' }),
    });
  });
}

// Setup and teardown hooks
authTest.beforeEach(async ({ page }) => {
  // Setup API mocks for pipeline management endpoints
  await setupApiMocks(page);
});

authTest.afterEach(async () => {
  // Cleanup any created test data
  // Reset API mocks if needed
});

authTest.describe('Pipeline Management Page', () => {
  authTest('should display the pipeline list with correct columns', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify the pipeline table columns are visible
    await expect(authenticatedPage.locator(pipelinePage.pipelineNameColumn)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineStatusColumn)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineSourceTypeColumn)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineTargetColumn)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineLastRunColumn)).toBeVisible();
  });

  authTest('should display pipeline details when a pipeline is selected', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select the first pipeline in the list
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify the pipeline details card is visible
    await expect(authenticatedPage.locator(pipelinePage.pipelineDetailsCard)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineNameHeader)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineStatusBadge)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineDescription)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.pipelineDagVisualization)).toBeVisible();
  });

  authTest('should filter pipelines by name using search', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Get the initial pipeline count
    const initialCount = await pipelinePage.getPipelineCount();
    
    // Search for a specific pipeline name
    const searchTerm = 'analytics';
    await pipelinePage.searchPipeline(searchTerm);
    
    // Verify the pipeline count has decreased
    const filteredCount = await pipelinePage.getPipelineCount();
    expect(filteredCount).toBeLessThan(initialCount);
    
    // Verify the visible pipelines match the search term
    const pipelineTable = authenticatedPage.locator(`${pipelinePage.pipelineTable} > tbody > tr`);
    const rowCount = await pipelineTable.count();
    for (let i = 0; i < rowCount; i++) {
      const pipelineNameText = await pipelineTable.nth(i).textContent();
      expect(pipelineNameText.toLowerCase()).toContain(searchTerm.toLowerCase());
    }
  });

  authTest('should filter pipelines by status', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Get the initial pipeline count
    const initialCount = await pipelinePage.getPipelineCount();
    
    // Filter pipelines by 'Active' status
    await pipelinePage.filterByStatus('Active');
    
    // Verify only pipelines with 'Active' status are displayed
    // Check for decreased count as not all pipelines are active
    const activeCount = await pipelinePage.getPipelineCount();
    expect(activeCount).toBeLessThan(initialCount);
    
    // Filter pipelines by 'Failed' status
    await pipelinePage.filterByStatus('Failed');
    
    // Verify only pipelines with 'Failed' status are displayed
    const failedCount = await pipelinePage.getPipelineCount();
    expect(failedCount).toBeLessThan(initialCount);
    
    // Filter pipelines by 'All' status
    await pipelinePage.filterByStatus('All');
    
    // Verify the pipeline count returns to the initial count
    const resetCount = await pipelinePage.getPipelineCount();
    expect(resetCount).toEqual(initialCount);
  });

  authTest('should filter pipelines by source type', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Get the initial pipeline count
    const initialCount = await pipelinePage.getPipelineCount();
    
    // Filter pipelines by 'GCS' source type
    await pipelinePage.filterBySourceType('GCS');
    
    // Verify only pipelines with 'GCS' source type are displayed
    const gcsCount = await pipelinePage.getPipelineCount();
    expect(gcsCount).toBeLessThan(initialCount);
    
    // Filter pipelines by 'Cloud SQL' source type
    await pipelinePage.filterBySourceType('Cloud SQL');
    
    // Verify only pipelines with 'Cloud SQL' source type are displayed
    const sqlCount = await pipelinePage.getPipelineCount();
    expect(sqlCount).toBeLessThan(initialCount);
    
    // Filter pipelines by 'All' source types
    await pipelinePage.filterBySourceType('All');
    
    // Verify the pipeline count returns to the initial count
    const resetCount = await pipelinePage.getPipelineCount();
    expect(resetCount).toEqual(initialCount);
  });

  authTest('should create a new pipeline successfully', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Get the initial pipeline count
    const initialCount = await pipelinePage.getPipelineCount();
    
    // Click the create pipeline button
    await pipelinePage.clickCreatePipeline();
    
    // Verify the pipeline form is displayed
    await expect(engineerPage.locator(pipelinePage.pipelineForm)).toBeVisible();
    
    // Fill the pipeline form with test data
    const testPipeline = {
      name: 'Test Pipeline',
      description: 'Test pipeline description',
      sourceType: 'GCS'
    };
    await pipelinePage.fillPipelineForm(testPipeline);
    
    // Save the pipeline form
    await pipelinePage.savePipelineForm();
    
    // Verify a success notification is displayed
    const notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline count has increased by 1
    const newCount = await pipelinePage.getPipelineCount();
    expect(newCount).toEqual(initialCount + 1);
    
    // Verify the new pipeline appears in the list
    const isPipelineInList = await pipelinePage.isPipelineInTable(testPipeline.name);
    expect(isPipelineInList).toBeTruthy();
  });

  authTest('should show validation errors for invalid pipeline form data', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Click the create pipeline button
    await pipelinePage.clickCreatePipeline();
    
    // Verify the pipeline form is displayed
    await expect(engineerPage.locator(pipelinePage.pipelineForm)).toBeVisible();
    
    // Submit the form without filling required fields
    await pipelinePage.savePipelineForm();
    
    // Verify validation error messages are displayed
    const errors = await pipelinePage.getValidationErrors();
    expect(errors.length).toBeGreaterThan(0);
    
    // Verify the form was not submitted
    await expect(engineerPage.locator(pipelinePage.pipelineForm)).toBeVisible();
  });

  authTest('should edit an existing pipeline successfully', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select the first pipeline in the list
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the edit pipeline button
    await pipelinePage.clickEditPipeline();
    
    // Verify the pipeline form is displayed with pre-filled data
    await expect(engineerPage.locator(pipelinePage.pipelineForm)).toBeVisible();
    
    // Modify the pipeline description
    const updatedDescription = 'Updated pipeline description';
    await engineerPage.fill(pipelinePage.pipelineDescriptionInput, updatedDescription);
    
    // Save the pipeline form
    await pipelinePage.savePipelineForm();
    
    // Verify a success notification is displayed
    const notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline details show the updated description
    const description = await engineerPage.textContent(pipelinePage.pipelineDescription);
    expect(description).toContain(updatedDescription);
  });

  authTest('should run a pipeline successfully', async ({ operatorPage }) => {
    const pipelinePage = new PipelinePage(operatorPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select a pipeline with 'Active' status
    // Using first pipeline as an example, in real test would find one with appropriate status
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the run pipeline button
    await pipelinePage.clickRunPipeline();
    
    // Confirm the run pipeline dialog
    await pipelinePage.confirmDialog();
    
    // Verify a success notification is displayed
    const notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline status changes to 'Running' or shows a recent execution
    // This depends on how your UI displays this information
    // For now we'll check that the pipeline details are still visible
    await expect(operatorPage.locator(pipelinePage.pipelineDetailsCard)).toBeVisible();
  });

  authTest('should disable and enable a pipeline', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select a pipeline with 'Active' status
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the disable pipeline button
    await pipelinePage.clickDisablePipeline();
    
    // Confirm the disable pipeline dialog
    await pipelinePage.confirmDialog();
    
    // Verify a success notification is displayed
    let notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline status changes to 'Disabled'
    let status = await pipelinePage.getPipelineStatus();
    expect(status).toContain('Disabled');
    
    // Click the enable pipeline button
    await pipelinePage.clickEnablePipeline();
    
    // Confirm the enable pipeline dialog
    await pipelinePage.confirmDialog();
    
    // Verify a success notification is displayed
    notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline status changes to 'Active'
    status = await pipelinePage.getPipelineStatus();
    expect(status).toContain('Active');
  });

  authTest('should delete a pipeline', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Get the initial pipeline count
    const initialCount = await pipelinePage.getPipelineCount();
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the delete pipeline button
    await pipelinePage.clickDeletePipeline();
    
    // Confirm the delete pipeline dialog
    await pipelinePage.confirmDialog();
    
    // Verify a success notification is displayed
    const notification = await pipelinePage.getNotificationText();
    expect(notification).toContain('success');
    
    // Verify the pipeline count has decreased by 1
    const newCount = await pipelinePage.getPipelineCount();
    expect(newCount).toEqual(initialCount - 1);
    
    // Verify the deleted pipeline no longer appears in the list
    const isPipelineInList = await pipelinePage.isPipelineInTable(pipelineData.pipelines[0].name);
    expect(isPipelineInList).toBeFalsy();
  });

  authTest('should view pipeline execution history', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select a pipeline with previous executions
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the view history button
    await pipelinePage.clickViewHistory();
    
    // Verify the execution history table is displayed
    await expect(authenticatedPage.locator(pipelinePage.executionHistoryTable)).toBeVisible();
    
    // Verify the execution history contains entries
    const executionCount = await pipelinePage.getExecutionCount();
    expect(executionCount).toBeGreaterThan(0);
  });

  authTest('should view execution details', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select a pipeline with previous executions
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Click the view history button
    await pipelinePage.clickViewHistory();
    
    // Verify the execution history table is displayed
    await expect(authenticatedPage.locator(pipelinePage.executionHistoryTable)).toBeVisible();
    
    // Select the first execution in the history
    await pipelinePage.selectExecution(0);
    
    // Verify the execution details card is displayed
    await expect(authenticatedPage.locator(pipelinePage.executionDetailsCard)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.executionStatusBadge)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.executionStartTime)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.executionEndTime)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.executionDuration)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.taskExecutionTable)).toBeVisible();
  });

  authTest('should show self-healing information for failed executions', async ({ authenticatedPage }) => {
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Select a pipeline with failed executions
    await pipelinePage.selectPipeline(pipelineData.pipelines[2].name); // product_enrich has failed status
    
    // Click the view history button
    await pipelinePage.clickViewHistory();
    
    // Verify the execution history table is displayed
    await expect(authenticatedPage.locator(pipelinePage.executionHistoryTable)).toBeVisible();
    
    // Select a failed execution in the history
    await pipelinePage.selectExecution(0);
    
    // Verify the execution details card is displayed
    await expect(authenticatedPage.locator(pipelinePage.executionDetailsCard)).toBeVisible();
    
    // Verify the execution status badge shows 'Failed'
    const status = await authenticatedPage.textContent(pipelinePage.executionStatusBadge);
    expect(status).toContain('Failed');
    
    // Verify the error details section is visible
    await expect(authenticatedPage.locator(pipelinePage.errorDetailsSection)).toBeVisible();
    
    // Verify the self-healing section is visible
    await expect(authenticatedPage.locator(pipelinePage.selfHealingSection)).toBeVisible();
    
    // Verify self-healing information is displayed
    const hasSelfHealingInfo = await pipelinePage.hasSelfHealingInformation();
    expect(hasSelfHealingInfo).toBeTruthy();
  });
});

authTest.describe('Pipeline Management Access Control', () => {
  authTest('should show all pipeline management features for admin users', async ({ adminPage }) => {
    const pipelinePage = new PipelinePage(adminPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify create pipeline button is enabled
    await expect(adminPage.locator(pipelinePage.createPipelineButton)).toBeEnabled();
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify edit pipeline button is enabled
    await expect(adminPage.locator(pipelinePage.editPipelineButton)).toBeEnabled();
    
    // Verify run pipeline button is enabled
    await expect(adminPage.locator(pipelinePage.runPipelineButton)).toBeEnabled();
    
    // Verify disable/enable pipeline button is enabled
    await expect(adminPage.locator(pipelinePage.disablePipelineButton)).toBeEnabled();
    
    // Verify delete pipeline button is enabled
    await expect(adminPage.locator(pipelinePage.deletePipelineButton)).toBeEnabled();
  });

  authTest('should show appropriate pipeline management features for engineer users', async ({ engineerPage }) => {
    const pipelinePage = new PipelinePage(engineerPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify create pipeline button is enabled
    await expect(engineerPage.locator(pipelinePage.createPipelineButton)).toBeEnabled();
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify edit pipeline button is enabled
    await expect(engineerPage.locator(pipelinePage.editPipelineButton)).toBeEnabled();
    
    // Verify run pipeline button is enabled
    await expect(engineerPage.locator(pipelinePage.runPipelineButton)).toBeEnabled();
    
    // Verify disable/enable pipeline button is enabled
    await expect(engineerPage.locator(pipelinePage.disablePipelineButton)).toBeEnabled();
    
    // Verify delete pipeline button is enabled
    await expect(engineerPage.locator(pipelinePage.deletePipelineButton)).toBeEnabled();
  });

  authTest('should show appropriate pipeline management features for operator users', async ({ operatorPage }) => {
    const pipelinePage = new PipelinePage(operatorPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify create pipeline button is disabled or not visible
    const createButtonVisible = await operatorPage.isVisible(pipelinePage.createPipelineButton);
    if (createButtonVisible) {
      await expect(operatorPage.locator(pipelinePage.createPipelineButton)).toBeDisabled();
    }
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify edit pipeline button is disabled or not visible
    const editButtonVisible = await operatorPage.isVisible(pipelinePage.editPipelineButton);
    if (editButtonVisible) {
      await expect(operatorPage.locator(pipelinePage.editPipelineButton)).toBeDisabled();
    }
    
    // Verify run pipeline button is enabled
    await expect(operatorPage.locator(pipelinePage.runPipelineButton)).toBeEnabled();
    
    // Verify disable/enable pipeline button is disabled or not visible
    const disableButtonVisible = await operatorPage.isVisible(pipelinePage.disablePipelineButton);
    if (disableButtonVisible) {
      await expect(operatorPage.locator(pipelinePage.disablePipelineButton)).toBeDisabled();
    }
    
    // Verify delete pipeline button is disabled or not visible
    const deleteButtonVisible = await operatorPage.isVisible(pipelinePage.deletePipelineButton);
    if (deleteButtonVisible) {
      await expect(operatorPage.locator(pipelinePage.deletePipelineButton)).toBeDisabled();
    }
  });
});

authTest.describe('Pipeline Management Responsiveness', () => {
  authTest('should adapt layout for tablet viewport', async ({ authenticatedPage }) => {
    // Set viewport size to tablet dimensions (e.g., 768x1024)
    await authenticatedPage.setViewportSize({ width: 768, height: 1024 });
    
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify pipeline table is visible and properly formatted
    await expect(authenticatedPage.locator(pipelinePage.pipelineTable)).toBeVisible();
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify pipeline details are visible and properly formatted
    await expect(authenticatedPage.locator(pipelinePage.pipelineDetailsCard)).toBeVisible();
    
    // Verify all action buttons are accessible
    await expect(authenticatedPage.locator(pipelinePage.editPipelineButton)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.runPipelineButton)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.viewHistoryButton)).toBeVisible();
  });

  authTest('should adapt layout for mobile viewport', async ({ authenticatedPage }) => {
    // Set viewport size to mobile dimensions (e.g., 375x667)
    await authenticatedPage.setViewportSize({ width: 375, height: 667 });
    
    const pipelinePage = new PipelinePage(authenticatedPage);
    await pipelinePage.goto();
    await pipelinePage.waitForPageLoad();
    
    // Verify pipeline table is visible and properly formatted
    await expect(authenticatedPage.locator(pipelinePage.pipelineTable)).toBeVisible();
    
    // Verify table is scrollable or reformatted for mobile
    // In mobile view, the table should still be accessible
    
    // Select a pipeline
    await pipelinePage.selectPipeline(pipelineData.pipelines[0].name);
    
    // Verify pipeline details are visible and properly formatted
    await expect(authenticatedPage.locator(pipelinePage.pipelineDetailsCard)).toBeVisible();
    
    // Verify all action buttons are accessible
    await expect(authenticatedPage.locator(pipelinePage.editPipelineButton)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.runPipelineButton)).toBeVisible();
    await expect(authenticatedPage.locator(pipelinePage.viewHistoryButton)).toBeVisible();
  });
});