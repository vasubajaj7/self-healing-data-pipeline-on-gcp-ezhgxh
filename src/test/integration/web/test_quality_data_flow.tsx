import React from 'react'; // react ^18.2.0
import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals'; // @jest/globals ^29.5.0
import { screen, waitFor, within, fireEvent } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import {
  renderComponent,
  waitForLoadingToComplete,
  findTableRowByText,
  createUserEvent,
  fillFormFields,
  selectDropdownOption,
} from '../../utils/web_test_utils';
import { mockQualityService, MOCK_QUALITY_DATA } from '../../fixtures/web/api_fixtures';
import QualityDashboard from '../../../web/src/components/quality/QualityDashboard';
import { QualityContextProvider } from '../../../web/src/contexts/QualityContext';
import qualityService from '../../../web/src/services/api/qualityService';
import sampleValidationResults from '../../mock_data/quality/sample_validation_results.json';

/**
 * Sets up the quality service mock with predefined responses
 * @returns Mocked quality service instance
 */
const setupQualityServiceMock = () => {
  // LD1: Create a mock quality service using mockQualityService
  const mockedService = mockQualityService();

  // LD1: Configure mock responses for getQualityStatistics, getDatasetQualitySummaries, getQualityRules, getQualityIssues, getQualityValidation
  mockedService.getQualityStatistics.mockResolvedValue(MOCK_QUALITY_DATA.scores);
  mockedService.getDatasetSummaries.mockResolvedValue({ items: MOCK_QUALITY_DATA.dataSummaries });
  mockedService.getQualityRules.mockResolvedValue({ items: MOCK_QUALITY_DATA.rules });
  mockedService.getQualityIssues.mockResolvedValue({ items: MOCK_QUALITY_DATA.issues });
  mockedService.getQualityValidation.mockResolvedValue(sampleValidationResults);

  // LD1: Return the mocked service
  return mockedService;
};

/**
 * Renders the QualityDashboard component with necessary providers and mocks
 * @param options 
 * @returns Render result with additional helper methods
 */
const renderQualityDashboard = (options: any = {}) => {
  // LD1: Set up quality service mock using setupQualityServiceMock
  const mockedService = setupQualityServiceMock();

  // LD1: Mock the actual qualityService implementation with the mock
  jest.spyOn(qualityService, 'getQualityRules').mockImplementation(mockedService.getQualityRules);
  jest.spyOn(qualityService, 'getQualityStatistics').mockImplementation(mockedService.getQualityStatistics);
  jest.spyOn(qualityService, 'getDatasetQualitySummaries').mockImplementation(mockedService.getDatasetSummaries);
  jest.spyOn(qualityService, 'getQualityIssues').mockImplementation(mockedService.getQualityIssues);
  jest.spyOn(qualityService, 'getQualityValidation').mockImplementation(mockedService.getQualityValidation);

  // LD1: Wrap QualityDashboard with QualityContextProvider
  const component = (
    <QualityContextProvider>
      <QualityDashboard />
    </QualityContextProvider>
  );

  // LD1: Render the wrapped component using renderComponent
  const renderResult = renderComponent(component, options);

  // LD1: Wait for initial loading to complete
  waitForLoadingToComplete();

  // LD1: Return the render result
  return renderResult;
};

describe('Quality Dashboard Integration', () => {
  it('should load and display quality statistics', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Wait for loading to complete
    await waitForLoadingToComplete();

    // LD1: Verify that quality score is displayed
    expect(screen.getByText('Overall Quality Score')).toBeInTheDocument();

    // LD1: Verify that quality dimensions are displayed
    expect(screen.getByText('Completeness')).toBeInTheDocument();
    expect(screen.getByText('Accuracy')).toBeInTheDocument();

    // LD1: Verify that dataset quality table is displayed with correct data
    const datasetTable = screen.getByRole('table', { name: 'Dataset Quality' });
    expect(findTableRowByText('customer', datasetTable)).toBeInTheDocument();
    expect(findTableRowByText('product', datasetTable)).toBeInTheDocument();
  });

  it('should navigate between tabs correctly', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Click on the 'Rules' tab
    const rulesTab = screen.getByRole('tab', { name: 'Rules' });
    await userEvent.click(rulesTab);

    // LD1: Verify that validation rules table is displayed
    expect(screen.getByRole('table', { name: 'Validation Rules' })).toBeInTheDocument();

    // LD1: Click on the 'Issues' tab
    const issuesTab = screen.getByRole('tab', { name: 'Issues' });
    await userEvent.click(issuesTab);

    // LD1: Verify that validation issues table is displayed
    expect(screen.getByRole('table', { name: 'Validation Issues' })).toBeInTheDocument();
  });

  it('should filter datasets correctly', async () => {
    // LD1: Render the QualityDashboard component
    const { rerender } = renderQualityDashboard();

    // LD1: Find and click on a dataset row in the dataset quality table
    const datasetTable = screen.getByRole('table', { name: 'Dataset Quality' });
    const customerRow = findTableRowByText('customer', datasetTable);
    await userEvent.click(customerRow);

    // LD1: Verify that the dashboard switches to Dataset Detail tab
    const datasetDetailHeader = await screen.findByText('customer Quality Score');
    expect(datasetDetailHeader).toBeInTheDocument();

    // LD1: Verify that the selected dataset's details are displayed
    expect(screen.getByText('Completeness')).toBeInTheDocument();

    // LD1: Verify that the quality service was called with correct dataset filter
    expect(qualityService.getDatasetSummaries).toHaveBeenCalledWith({ dataset: 'customer' });
  });
});

describe('Quality Validation Flow', () => {
  it('should display validation details when selecting a validation', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Rules' tab
    const rulesTab = screen.getByRole('tab', { name: 'Rules' });
    await userEvent.click(rulesTab);

    // LD1: Find and click on a validation rule
    const validationRulesTable = screen.getByRole('table', { name: 'Validation Rules' });
    const ruleRow = findTableRowByText('Required Fields Check', validationRulesTable);
    await userEvent.click(ruleRow);

    // LD1: Verify that validation details are displayed
    expect(screen.getByText('Rule Name')).toBeInTheDocument();
    expect(screen.getByText('Target Dataset')).toBeInTheDocument();

    // LD1: Verify that validation results match the expected data
    expect(screen.getByText('Required Fields Check')).toBeInTheDocument();
  });

  it('should handle quality issue resolution flow', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Issues' tab
    const issuesTab = screen.getByRole('tab', { name: 'Issues' });
    await userEvent.click(issuesTab);

    // LD1: Find and click on a quality issue
    const validationIssuesTable = screen.getByRole('table', { name: 'Validation Issues' });
    const issueRow = findTableRowByText('Invalid email format', validationIssuesTable);
    await userEvent.click(issueRow);

    // LD1: Verify that issue details are displayed
    expect(screen.getByText('Description')).toBeInTheDocument();
    expect(screen.getByText('Severity')).toBeInTheDocument();

    // LD1: Click on 'Resolve Issue' button
    const resolveButton = screen.getByRole('button', { name: 'Resolve Issue' });
    await userEvent.click(resolveButton);

    // LD1: Fill in resolution details
    await fillFormFields({
      'Resolved By': 'testuser',
      'Resolution Notes': 'Fixed data issue',
    });

    // LD1: Submit the resolution
    const submitButton = screen.getByRole('button', { name: 'Submit' });
    await userEvent.click(submitButton);

    // LD1: Verify that the quality service was called with correct resolution data
    expect(qualityService.updateQualityIssueStatus).toHaveBeenCalledWith(
      'issue-1',
      { status: 'RESOLVED', comment: 'Fixed data issue' }
    );

    // LD1: Verify that the issues list is updated
    await waitFor(() => {
      expect(screen.queryByText('Invalid email format')).toBeNull();
    });
  });
});

describe('Self-Healing Integration', () => {
  it('should display self-healing recommendations for quality issues', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Issues' tab
    const issuesTab = screen.getByRole('tab', { name: 'Issues' });
    await userEvent.click(issuesTab);

    // LD1: Find an issue with self-healing recommendations
    const validationIssuesTable = screen.getByRole('table', { name: 'Validation Issues' });
    const issueRow = findTableRowByText('Invalid email format', validationIssuesTable);

    // LD1: Verify that self-healing recommendation is displayed
    expect(within(issueRow).getByText('Generate placeholder email')).toBeVisible();

    // LD1: Verify that recommendation details match the expected data
    expect(within(issueRow).getByText('Generate placeholder email')).toBeInTheDocument();
  });

  it('should allow applying self-healing recommendations', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Issues' tab
    const issuesTab = screen.getByRole('tab', { name: 'Issues' });
    await userEvent.click(issuesTab);

    // LD1: Find an issue with self-healing recommendations
    const validationIssuesTable = screen.getByRole('table', { name: 'Validation Issues' });
    const issueRow = findTableRowByText('Invalid email format', validationIssuesTable);

    // LD1: Click on 'Apply Recommendation' button
    const applyButton = within(issueRow).getByRole('button', { name: 'Apply Recommendation' });
    await userEvent.click(applyButton);

    // LD1: Verify confirmation dialog is displayed
    expect(screen.getByText('Confirm Action')).toBeInTheDocument();

    // LD1: Confirm the application
    const confirmButton = screen.getByRole('button', { name: 'Confirm' });
    await userEvent.click(confirmButton);

    // LD1: Verify that the healing service was called with correct parameters
    expect(qualityService.updateQualityIssueStatus).toHaveBeenCalledWith(
      'issue-1',
      { status: 'IN_PROGRESS', comment: 'Applying self-healing recommendation' }
    );

    // LD1: Verify that the issue status is updated
    await waitFor(() => {
      expect(within(issueRow).getByText('In Progress')).toBeVisible();
    });
  });
});

describe('Quality Rule Management', () => {
  it('should open rule editor when adding a new rule', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Rules' tab
    const rulesTab = screen.getByRole('tab', { name: 'Rules' });
    await userEvent.click(rulesTab);

    // LD1: Click on 'Add Rule' button
    const addButton = screen.getByRole('button', { name: 'Add Rule' });
    await userEvent.click(addButton);

    // LD1: Verify that rule editor modal is displayed
    expect(screen.getByRole('dialog', { name: 'Create Validation Rule' })).toBeInTheDocument();

    // LD1: Verify that the form is empty for a new rule
    expect(screen.getByLabelText('Rule Name')).toHaveValue('');
  });

  it('should populate rule editor when editing an existing rule', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Rules' tab
    const rulesTab = screen.getByRole('tab', { name: 'Rules' });
    await userEvent.click(rulesTab);

    // LD1: Find and click on 'Edit' button for a rule
    const validationRulesTable = screen.getByRole('table', { name: 'Validation Rules' });
    const ruleRow = findTableRowByText('Required Fields Check', validationRulesTable);
    const editButton = within(ruleRow).getByRole('button', { name: 'Edit Rule' });
    await userEvent.click(editButton);

    // LD1: Verify that rule editor modal is displayed
    expect(screen.getByRole('dialog', { name: 'Edit Validation Rule' })).toBeInTheDocument();

    // LD1: Verify that the form is populated with the rule's data
    expect(screen.getByLabelText('Rule Name')).toHaveValue('Required Fields Check');
  });

  it('should save a new rule correctly', async () => {
    // LD1: Render the QualityDashboard component
    renderQualityDashboard();

    // LD1: Navigate to the 'Rules' tab
    const rulesTab = screen.getByRole('tab', { name: 'Rules' });
    await userEvent.click(rulesTab);

    // LD1: Click on 'Add Rule' button
    const addButton = screen.getByRole('button', { name: 'Add Rule' });
    await userEvent.click(addButton);

    // LD1: Fill in rule details
    await fillFormFields({
      'Rule Name': 'New Rule',
      'Target Dataset': 'customer',
      'Target Table': 'customer_data',
    });

    // LD1: Click on 'Save' button
    const saveButton = screen.getByRole('button', { name: 'Save' });
    await userEvent.click(saveButton);

    // LD1: Verify that the quality service was called with correct rule data
    expect(qualityService.createQualityRule).toHaveBeenCalledWith(
      expect.objectContaining({
        ruleName: 'New Rule',
        targetDataset: 'customer',
        targetTable: 'customer_data',
      })
    );

    // LD1: Verify that the rules list is updated
    await waitFor(() => {
      expect(screen.getByRole('table', { name: 'Validation Rules' })).toBeInTheDocument();
    });
  });
});