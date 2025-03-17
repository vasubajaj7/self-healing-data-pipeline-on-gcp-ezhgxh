/**
 * This file contains end-to-end tests for the self-healing functionality of the data pipeline application.
 * It tests the dashboard visualization, issue management, action approval/rejection,
 * model performance monitoring, and configuration settings.
 * 
 * The tests use custom Cypress commands defined in support/commands.ts
 * and mock data from fixtures/healing_data.json.
 */

import '../support/index.d.ts';

describe('Self-Healing', () => {
  before(() => {
    // Login as a data engineer using custom command
    cy.loginAsEngineer();
    
    // Mock healing data API responses
    cy.mockHealingData();
    
    // Navigate to the self-healing page
    cy.navigateTo('Self-Healing');
  });

  it('should display the self-healing dashboard with key metrics', () => {
    // Verify page title is visible
    cy.get('h1').should('contain', 'Self-Healing Dashboard');
    
    // Verify success rate metric is displayed with correct value
    cy.get('[data-testid="success-rate-metric"]').should('be.visible')
      .and('contain', '87%');
    
    // Verify issues detected metric is displayed with correct value
    cy.get('[data-testid="issues-detected-metric"]').should('be.visible')
      .and('contain', '142');
    
    // Verify active issues metric is displayed with correct value
    cy.get('[data-testid="active-issues-metric"]').should('be.visible')
      .and('contain', '23');
    
    // Verify resolution time metric is displayed with correct value
    cy.get('[data-testid="resolution-time-metric"]').should('be.visible')
      .and('contain', '5.2 min');
    
    // Verify success rate chart is visible
    cy.get('[data-testid="success-rate-chart"]').should('be.visible');
    
    // Verify model health indicators are displayed
    cy.get('[data-testid="model-health-indicator"]').should('be.visible')
      .and('have.length.at.least', 1);
  });

  it('should display active issues table with correct data', () => {
    // Click on the Issues tab if not already active
    cy.get('[data-testid="issues-tab"]').click();
    
    // Verify the active issues table is visible
    cy.get('[data-testid="active-issues-table"]').should('be.visible');
    
    // Verify the table contains the expected number of rows
    cy.get('[data-testid="active-issues-table"] tbody tr').should('have.length', 5);
    
    // Verify the table contains the expected issue types
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Data Quality');
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Pipeline Failure');
    
    // Verify the table contains the expected severity levels
    cy.get('[data-testid="active-issues-table"]').should('contain', 'High');
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Medium');
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Low');
    
    // Verify the table contains the expected status values
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Open');
    cy.get('[data-testid="active-issues-table"]').should('contain', 'In Progress');
    cy.get('[data-testid="active-issues-table"]').should('contain', 'Resolved');
  });

  it('should display issue details when clicking on an issue', () => {
    // Click on the Issues tab if not already active
    cy.get('[data-testid="issues-tab"]').click();
    
    // Click on the first issue in the table
    cy.get('[data-testid="active-issues-table"] tbody tr').first().click();
    
    // Verify the issue details modal is displayed
    cy.get('[data-testid="issue-details-modal"]').should('be.visible');
    
    // Verify the issue ID is correct
    cy.get('[data-testid="issue-id"]').should('contain', 'ISSUE-');
    
    // Verify the issue type is displayed
    cy.get('[data-testid="issue-type"]').should('be.visible');
    
    // Verify the issue description is displayed
    cy.get('[data-testid="issue-description"]').should('be.visible');
    
    // Verify the issue details contain expected information
    cy.get('[data-testid="issue-details"]').should('be.visible');
    
    // Verify potential actions are listed
    cy.get('[data-testid="potential-actions"]').should('be.visible')
      .find('li').should('have.length.at.least', 1);
    
    // Close the modal
    cy.get('[data-testid="close-modal-button"]').click();
    cy.get('[data-testid="issue-details-modal"]').should('not.exist');
  });

  it('should display healing actions table with correct data', () => {
    // Click on the Actions tab
    cy.get('[data-testid="actions-tab"]').click();
    
    // Verify the healing actions table is visible
    cy.get('[data-testid="healing-actions-table"]').should('be.visible');
    
    // Verify the table contains the expected number of rows
    cy.get('[data-testid="healing-actions-table"] tbody tr').should('have.length.at.least', 3);
    
    // Verify the table contains the expected action types
    cy.get('[data-testid="healing-actions-table"]').should('contain', 'Data Correction');
    cy.get('[data-testid="healing-actions-table"]').should('contain', 'Job Retry');
    
    // Verify the table contains the expected confidence scores
    cy.get('[data-testid="healing-actions-table"]').should('contain', '%');
    
    // Verify the table contains the expected status values
    cy.get('[data-testid="healing-actions-table"]').should('contain', 'Pending Approval');
    cy.get('[data-testid="healing-actions-table"]').should('contain', 'Approved');
    cy.get('[data-testid="healing-actions-table"]').should('contain', 'Completed');
  });

  it('should display action details when clicking on an action', () => {
    // Click on the Actions tab
    cy.get('[data-testid="actions-tab"]').click();
    
    // Click on the first action in the table
    cy.get('[data-testid="healing-actions-table"] tbody tr').first().click();
    
    // Verify the action details modal is displayed
    cy.get('[data-testid="action-details-modal"]').should('be.visible');
    
    // Verify the action ID is correct
    cy.get('[data-testid="action-id"]').should('contain', 'ACTION-');
    
    // Verify the correction strategy is displayed
    cy.get('[data-testid="correction-strategy"]').should('be.visible');
    
    // Verify the confidence score is displayed
    cy.get('[data-testid="confidence-score"]').should('contain', '%');
    
    // Verify the correction details contain expected information
    cy.get('[data-testid="correction-details"]').should('be.visible');
    
    // Verify validation results are displayed
    cy.get('[data-testid="validation-results"]').should('be.visible');
    
    // Close the modal
    cy.get('[data-testid="close-modal-button"]').click();
    cy.get('[data-testid="action-details-modal"]').should('not.exist');
  });

  it('should allow approving a pending healing action', () => {
    // Click on the Actions tab
    cy.get('[data-testid="actions-tab"]').click();
    
    // Find an action with PENDING_APPROVAL status
    cy.get('[data-testid="healing-actions-table"] tbody tr')
      .contains('Pending Approval')
      .parents('tr')
      .find('[data-testid="approve-action-button"]')
      .click();
    
    // Verify the confirmation dialog is displayed
    cy.get('[data-testid="confirmation-dialog"]').should('be.visible');
    
    // Confirm the approval
    cy.get('[data-testid="confirm-button"]').click();
    
    // Mock the API response for approval
    cy.mockApiResponse('POST', '/api/self-healing/actions/*/approve', {
      success: true,
      message: 'Action approved successfully'
    });
    
    // Verify the success notification is displayed
    cy.checkToastNotification('Action approved successfully');
    
    // Verify the action status changes to APPROVED or IN_PROGRESS
    cy.get('[data-testid="healing-actions-table"]')
      .should('contain', 'Approved')
      .or('contain', 'In Progress');
  });

  it('should allow rejecting a pending healing action with a reason', () => {
    // Click on the Actions tab
    cy.get('[data-testid="actions-tab"]').click();
    
    // Find an action with PENDING_APPROVAL status
    cy.get('[data-testid="healing-actions-table"] tbody tr')
      .contains('Pending Approval')
      .parents('tr')
      .find('[data-testid="reject-action-button"]')
      .click();
    
    // Verify the rejection dialog is displayed
    cy.get('[data-testid="rejection-dialog"]').should('be.visible');
    
    // Enter a rejection reason
    cy.get('[data-testid="rejection-reason"]')
      .type('Requires manual verification and adjustment');
    
    // Confirm the rejection
    cy.get('[data-testid="confirm-rejection-button"]').click();
    
    // Mock the API response for rejection
    cy.mockApiResponse('POST', '/api/self-healing/actions/*/reject', {
      success: true,
      message: 'Action rejected successfully'
    });
    
    // Verify the success notification is displayed
    cy.checkToastNotification('Action rejected successfully');
    
    // Verify the action status changes to REJECTED
    cy.get('[data-testid="healing-actions-table"]')
      .should('contain', 'Rejected');
  });

  it('should display model performance metrics correctly', () => {
    // Click on the Models tab
    cy.get('[data-testid="models-tab"]').click();
    
    // Verify the model performance card is displayed
    cy.get('[data-testid="model-performance-card"]').should('be.visible');
    
    // Verify the model list contains the expected number of models
    cy.get('[data-testid="model-list"] .model-item').should('have.length.at.least', 3);
    
    // Verify each model displays its type, version, and accuracy
    cy.get('[data-testid="model-list"] .model-item').first().within(() => {
      cy.get('.model-type').should('be.visible');
      cy.get('.model-version').should('be.visible');
      cy.get('.model-accuracy').should('contain', '%');
    });
    
    // Verify the model health indicators are displayed
    cy.get('[data-testid="model-health-indicator"]').should('have.length.at.least', 1);
    
    // Click on a model to view details
    cy.get('[data-testid="model-list"] .model-item').first().click();
    
    // Verify the model details modal shows performance metrics
    cy.get('[data-testid="model-details-modal"]').should('be.visible');
    cy.get('[data-testid="performance-metrics"]').should('be.visible');
    
    // Close the modal
    cy.get('[data-testid="close-modal-button"]').click();
  });

  it('should navigate to model management page', () => {
    // Click on the Models tab
    cy.get('[data-testid="models-tab"]').click();
    
    // Click on the 'Manage Models' button
    cy.get('[data-testid="manage-models-button"]').click();
    
    // Verify navigation to the model management page
    cy.url().should('include', '/model-management');
    
    // Verify the model management interface is displayed
    cy.get('[data-testid="model-management-interface"]').should('be.visible');
    
    // Verify the model list is displayed with training status
    cy.get('[data-testid="model-management-list"]').should('be.visible');
    cy.get('[data-testid="model-training-status"]').should('be.visible');
  });

  it('should navigate to activity log page', () => {
    // Click on the Activity Log tab
    cy.get('[data-testid="activity-log-tab"]').click();
    
    // Verify navigation to the activity log page
    cy.url().should('include', '/activity-log');
    
    // Verify the activity log table is displayed
    cy.get('[data-testid="activity-log-table"]').should('be.visible');
    
    // Verify the log entries contain timestamps, activity types, and status
    cy.get('[data-testid="activity-log-table"] tbody tr').first().within(() => {
      cy.get('.timestamp').should('be.visible');
      cy.get('.activity-type').should('be.visible');
      cy.get('.status').should('be.visible');
    });
    
    // Verify filtering controls are available
    cy.get('[data-testid="log-filter-controls"]').should('be.visible');
    
    // Apply a filter and verify filtered results
    cy.get('[data-testid="activity-type-filter"]').select('Data Correction');
    cy.get('[data-testid="apply-filter-button"]').click();
    
    // Verify filtered results
    cy.get('[data-testid="activity-log-table"] tbody tr').each(($row) => {
      cy.wrap($row).should('contain', 'Data Correction');
    });
  });

  it('should navigate to configuration page and display settings', () => {
    // Click on the Configuration tab
    cy.get('[data-testid="configuration-tab"]').click();
    
    // Verify navigation to the configuration page
    cy.url().should('include', '/configuration');
    
    // Verify the settings form is displayed
    cy.get('[data-testid="self-healing-settings-form"]').should('be.visible');
    
    // Verify autonomous mode setting is displayed
    cy.get('[data-testid="autonomous-mode-setting"]').should('be.visible');
    
    // Verify confidence threshold setting is displayed
    cy.get('[data-testid="confidence-threshold-setting"]').should('be.visible');
    
    // Verify max retry attempts setting is displayed
    cy.get('[data-testid="max-retry-attempts-setting"]').should('be.visible');
    
    // Verify approval required setting is displayed
    cy.get('[data-testid="approval-required-setting"]').should('be.visible');
    
    // Verify learning mode setting is displayed
    cy.get('[data-testid="learning-mode-setting"]').should('be.visible');
  });

  it('should update self-healing settings', () => {
    // Click on the Configuration tab
    cy.get('[data-testid="configuration-tab"]').click();
    
    // Change autonomous mode setting
    cy.get('[data-testid="autonomous-mode-setting"]').select('Semi-Automatic');
    
    // Change confidence threshold setting
    cy.get('[data-testid="confidence-threshold-setting"]').clear().type('85');
    
    // Change max retry attempts setting
    cy.get('[data-testid="max-retry-attempts-setting"]').clear().type('3');
    
    // Change approval required setting
    cy.get('[data-testid="approval-required-setting"]').select('High Impact Only');
    
    // Click the Save button
    cy.get('[data-testid="save-settings-button"]').click();
    
    // Mock the API response for settings update
    cy.mockApiResponse('POST', '/api/self-healing/settings', {
      success: true,
      message: 'Settings updated successfully'
    });
    
    // Verify the success notification is displayed
    cy.checkToastNotification('Settings updated successfully');
    
    // Verify the settings are updated in the UI
    cy.get('[data-testid="autonomous-mode-setting"]').should('have.value', 'Semi-Automatic');
    cy.get('[data-testid="confidence-threshold-setting"]').should('have.value', '85');
    cy.get('[data-testid="max-retry-attempts-setting"]').should('have.value', '3');
    cy.get('[data-testid="approval-required-setting"]').should('have.value', 'High Impact Only');
  });

  it('should handle API errors gracefully', () => {
    // Mock API error response for healing data
    cy.mockApiResponse('GET', '/api/self-healing/dashboard', {
      error: true,
      message: 'Failed to fetch healing data',
      status: 500
    });
    
    // Reload the page
    cy.reload();
    
    // Verify error message is displayed
    cy.get('[data-testid="error-message"]').should('be.visible')
      .and('contain', 'Failed to fetch healing data');
    
    // Verify retry button is available
    cy.get('[data-testid="retry-button"]').should('be.visible');
    
    // Click retry button
    cy.get('[data-testid="retry-button"]').click();
    
    // Mock successful API response
    cy.mockHealingData();
    
    // Verify dashboard loads correctly after retry
    cy.get('[data-testid="success-rate-metric"]').should('be.visible');
    cy.get('[data-testid="issues-detected-metric"]').should('be.visible');
    cy.get('[data-testid="active-issues-metric"]').should('be.visible');
    cy.get('[data-testid="resolution-time-metric"]').should('be.visible');
  });
});