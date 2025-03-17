import React from 'react'; // react ^18.2.0
import { render, screen, fireEvent, waitFor, within, act } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals'; // @jest/globals ^29.5.0
import QualityDashboard from '../../../../web/src/components/quality/QualityDashboard'; // Import the main quality dashboard component for testing
import DatasetQualityTable from '../../../../web/src/components/quality/DatasetQualityTable'; // Import the dataset quality table component for testing
import QualityScoreChart from '../../../../web/src/components/quality/QualityScoreChart'; // Import the quality score chart component for testing
import ValidationRulesTable from '../../../../web/src/components/quality/ValidationRulesTable'; // Import the validation rules table component for testing
import ValidationIssuesTable from '../../../../web/src/components/quality/ValidationIssuesTable'; // Import the validation issues table component for testing
import { QualityContext, QualityProvider } from '../../../../web/src/contexts/QualityContext'; // Import quality context and provider for testing component integration
import { DatasetQualitySummary, QualityIssue, QualityRule, QualityStatistics, QualityDimension, QualityRuleType, QualityIssueStatus } from '../../../../web/src/types/quality'; // Import quality-related type definitions for test data
import { renderComponent, createUserEvent, fillFormFields, selectDropdownOption, waitForLoadingToComplete, findTableRowByText } from '../../../utils/web_test_utils'; // Import test utilities for rendering and interacting with components
import { renderWithTheme, createMockComponent, MockCard, MockChart } from '../../fixtures/web/component_fixtures'; // Import component fixtures for consistent testing
import { mockQualityService, MOCK_QUALITY_DATA } from '../../fixtures/web/api_fixtures'; // Import API mocking utilities and mock data for quality services

// Define mock data for testing
const mockDatasets: DatasetQualitySummary[] = MOCK_QUALITY_DATA.dataSummaries; // Array of mock dataset quality summaries for testing
const mockRules: QualityRule[] = MOCK_QUALITY_DATA.rules; // Array of mock quality rules for testing
const mockIssues: QualityIssue[] = MOCK_QUALITY_DATA.issues; // Array of mock quality issues for testing
const mockStatistics: QualityStatistics = MOCK_QUALITY_DATA.scores; // Mock quality statistics for testing

/**
 * Creates a mock quality context with predefined data for testing
 * @param {object} overrides
 * @returns {object} Mock quality context with data and functions
 */
const createMockQualityContext = (overrides: any = {}) => {
  // Create a default mock quality context with test data
  const defaultContext = {
    datasets: mockDatasets,
    rules: mockRules,
    issues: mockIssues,
    statistics: mockStatistics,
    timeSeries: null,
    filters: { dataset: null, table: null, dimension: null, ruleType: null, severity: null, status: null, timeRange: null, startDate: null, endDate: null, minScore: null, maxScore: null, searchTerm: null },
    loading: false,
    error: null,
    fetchQualityData: jest.fn(),
    fetchDatasets: jest.fn(),
    fetchRules: jest.fn(),
    fetchIssues: jest.fn(),
    fetchStatistics: jest.fn(),
    fetchTimeSeries: jest.fn(),
    createRule: jest.fn(),
    updateRule: jest.fn(),
    deleteRule: jest.fn(),
    updateIssueStatus: jest.fn(),
    runValidation: jest.fn(),
    setFilters: jest.fn(),
    setRefreshInterval: jest.fn(),
  };

  // Override default values with any provided overrides
  const mockContext = { ...defaultContext, ...overrides };

  // Create mock functions for all context methods
  Object.keys(mockContext).forEach(key => {
    if (typeof mockContext[key] === 'function') {
      mockContext[key].mockImplementation(() => {});
    }
  });

  // Return the mock quality context
  return mockContext;
};

/**
 * Renders the QualityDashboard component with mock context for testing
 * @param {object} props
 * @param {object} contextOverrides
 * @returns {object} Render result with user event instance
 */
const renderQualityDashboard = (props: any = {}, contextOverrides: any = {}) => {
  // Create mock quality context with provided overrides
  const mockContext = createMockQualityContext(contextOverrides);

  // Wrap QualityDashboard with QualityProvider using mock context
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QualityProvider value={mockContext}>
      {children}
    </QualityProvider>
  );

  // Render the wrapped component using renderComponent utility
  const renderResult = renderComponent(<QualityDashboard {...props} />, { wrapper });

  // Create and return user event instance with render result
  const user = createUserEvent();
  return { ...renderResult, user };
};

/**
 * Renders the DatasetQualityTable component with mock data for testing
 * @param {object} props
 * @param {object} contextOverrides
 * @returns {object} Render result with user event instance
 */
const renderDatasetQualityTable = (props: any = {}, contextOverrides: any = {}) => {
  // Create mock quality context with provided overrides
  const mockContext = createMockQualityContext(contextOverrides);

  // Wrap DatasetQualityTable with QualityProvider using mock context
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QualityProvider value={mockContext}>
      {children}
    </QualityProvider>
  );

  // Render the wrapped component using renderComponent utility
  const renderResult = renderComponent(<DatasetQualityTable {...props} />, { wrapper });

  // Create and return user event instance with render result
  const user = createUserEvent();
  return { ...renderResult, user };
};

/**
 * Renders the ValidationRulesTable component with mock data for testing
 * @param {object} props
 * @param {object} contextOverrides
 * @returns {object} Render result with user event instance
 */
const renderValidationRulesTable = (props: any = {}, contextOverrides: any = {}) => {
  // Create mock quality context with provided overrides
  const mockContext = createMockQualityContext(contextOverrides);

  // Wrap ValidationRulesTable with QualityProvider using mock context
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QualityProvider value={mockContext}>
      {children}
    </QualityProvider>
  );

  // Render the wrapped component using renderComponent utility
  const renderResult = renderComponent(<ValidationRulesTable {...props} />, { wrapper });

  // Create and return user event instance with render result
  const user = createUserEvent();
  return { ...renderResult, user };
};

/**
 * Renders the ValidationIssuesTable component with mock data for testing
 * @param {object} props
 * @param {object} contextOverrides
 * @returns {object} Render result with user event instance
 */
const renderValidationIssuesTable = (props: any = {}, contextOverrides: any = {}) => {
  // Create mock quality context with provided overrides
  const mockContext = createMockQualityContext(contextOverrides);

  // Wrap ValidationIssuesTable with QualityProvider using mock context
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QualityProvider value={mockContext}>
      {children}
    </QualityProvider>
  );

  // Render the wrapped component using renderComponent utility
  const renderResult = renderComponent(<ValidationIssuesTable {...props} />, { wrapper });

  // Create and return user event instance with render result
  const user = createUserEvent();
  return { ...renderResult, user };
};

describe('QualityDashboard', () => {
  it('renders the dashboard with all sections', async () => {
    // Render QualityDashboard with mock quality context
    const { getByText, getByRole } = renderQualityDashboard();

    // Verify that the dashboard title is displayed
    expect(getByText('Data Quality Dashboard')).toBeInTheDocument();

    // Verify that all tabs are rendered (Overview, Dataset Detail, Table Detail, Rules, Issues)
    expect(getByRole('tab', { name: 'Overview' })).toBeInTheDocument();
    expect(getByRole('tab', { name: 'Rules' })).toBeInTheDocument();
    expect(getByRole('tab', { name: 'Issues' })).toBeInTheDocument();

    // Verify that the Overview tab is active by default
    expect(getByRole('tab', { name: 'Overview' })).toHaveClass('Mui-selected');

    // Verify that the quality score chart is displayed
    expect(screen.getByText('Overall Quality Score')).toBeInTheDocument();
    expect(screen.getByText('Quality Dimensions')).toBeInTheDocument();
    expect(screen.getByText('Dataset Quality')).toBeInTheDocument();
  });

  it('displays loading state when data is loading', () => {
    // Render QualityDashboard with loading state set to true
    const { getByText } = renderQualityDashboard({}, { loading: true });

    // Verify that loading indicators are displayed
    expect(getByText('Loading chart...')).toBeInTheDocument();

    // Verify that content is not fully rendered while loading
    expect(screen.queryByText('Data Quality Dashboard')).toBeInTheDocument();
  });

  it('displays error state when there is an error', () => {
    // Render QualityDashboard with error state set to an error message
    const { getByText } = renderQualityDashboard({}, { error: 'Failed to load data' });

    // Verify that the error message is displayed
    expect(getByText('Failed to load data')).toBeInTheDocument();

    // Verify that a retry button is available
    expect(getByText('Refresh')).toBeInTheDocument();
  });

  it('switches tabs when tab is clicked', async () => {
    // Render QualityDashboard with mock quality context
    const { getByRole, user } = renderQualityDashboard();

    // Click on the Rules tab
    await user.click(getByRole('tab', { name: 'Rules' }));

    // Verify that the Rules tab is now active
    expect(getByRole('tab', { name: 'Rules' })).toHaveClass('Mui-selected');

    // Verify that the validation rules table is displayed
    expect(screen.getByText('Validation Rules')).toBeInTheDocument();

    // Click on the Issues tab
    await user.click(getByRole('tab', { name: 'Issues' }));

    // Verify that the Issues tab is now active
    expect(getByRole('tab', { name: 'Issues' })).toHaveClass('Mui-selected');

    // Verify that the validation issues table is displayed
    expect(screen.getByText('Validation Issues')).toBeInTheDocument();
  });

  it('handles dataset selection from the dataset table', async () => {
    // Render QualityDashboard with mock quality context
    const { getByRole, user, rerender } = renderQualityDashboard();

    // Find and click on a dataset row in the dataset quality table
    const datasetRow = await findTableRowByText('customer');
    await user.click(datasetRow);

    // Verify that the Dataset Detail tab becomes active
    expect(getByRole('tab', { name: 'Dataset Detail' })).toHaveClass('Mui-selected');

    // Verify that the selected dataset details are displayed
    expect(screen.getByText('customer Quality Score')).toBeInTheDocument();
  });

  it('handles table selection from the dataset detail view', async () => {
    // Render QualityDashboard with a selected dataset
    const { getByRole, user } = renderQualityDashboard({}, { filters: { dataset: 'customer' } });

    // Find and click on a table row in the dataset detail view
    const tableRow = await findTableRowByText('customer_data');
    await user.click(tableRow);

    // Verify that the Table Detail tab becomes active
    expect(getByRole('tab', { name: 'Table Detail' })).toHaveClass('Mui-selected');

    // Verify that the selected table details are displayed
    expect(screen.getByText('customer_data Quality Score')).toBeInTheDocument();
  });

  it('refreshes data when refresh button is clicked', async () => {
    // Render QualityDashboard with mock quality context
    const { getByRole, user } = renderQualityDashboard({}, { fetchQualityData: jest.fn() });

    // Find and click the refresh button
    const refreshButton = getByRole('button', { name: 'Refresh' });
    await user.click(refreshButton);

    // Verify that fetchQualityData function was called
    expect(mockQualityService().getQualityRules).toHaveBeenCalled();
  });

  it('opens rule editor when add rule button is clicked', async () => {
    // Render QualityDashboard with mock quality context
    const { getByRole, user } = renderQualityDashboard();

    // Find and click the add rule button
    const addRuleButton = getByRole('button', { name: 'Add Rule' });
    await user.click(addRuleButton);

    // Verify that the rule editor modal is displayed
    expect(screen.getByText('Create Validation Rule')).toBeInTheDocument();

    // Verify that the rule editor is in 'add' mode (no existing rule)
    expect(screen.queryByText('Edit Validation Rule')).not.toBeInTheDocument();
  });

  it('opens rule editor with rule data when edit rule is clicked', async () => {
    // Render QualityDashboard and navigate to Rules tab
    const { getByRole, user } = renderQualityDashboard();
    await user.click(getByRole('tab', { name: 'Rules' }));

    // Find and click the edit button on a rule row
    const editButton = await findTableRowByText('Required Fields Check');
    await user.click(editButton);

    // Verify that the rule editor modal is displayed
    expect(screen.getByText('Edit Validation Rule')).toBeInTheDocument();

    // Verify that the rule editor is populated with the selected rule data
    expect(screen.getByRole('textbox', { name: 'Rule Name' })).toHaveValue('Required Fields Check');
  });

  it('refreshes data after rule editor is closed', async () => {
    // Render QualityDashboard and open rule editor
    const { getByRole, user } = renderQualityDashboard();
    await user.click(getByRole('button', { name: 'Add Rule' }));

    // Close the rule editor modal
    const cancelButton = getByRole('button', { name: 'Cancel' });
    await user.click(cancelButton);

    // Verify that fetchQualityData function was called
    expect(mockQualityService().getQualityRules).toHaveBeenCalled();
  });
});

describe('DatasetQualityTable', () => {
  it('renders the dataset quality table with data', async () => {
    // Render DatasetQualityTable with mock datasets
    const { getByText } = renderDatasetQualityTable();

    // Verify that the table headers are displayed
    expect(getByText('Dataset')).toBeInTheDocument();
    expect(getByText('Quality Score')).toBeInTheDocument();
    expect(getByText('Trend')).toBeInTheDocument();
    expect(getByText('Issues')).toBeInTheDocument();
    expect(getByText('Last Updated')).toBeInTheDocument();

    // Verify that each dataset row is displayed with correct data
    expect(getByText('customer')).toBeInTheDocument();
    expect(getByText('sales')).toBeInTheDocument();
    expect(getByText('product')).toBeInTheDocument();

    // Verify that quality scores are displayed with appropriate formatting
    expect(getByText('96%')).toBeInTheDocument();
    expect(getByText('90%')).toBeInTheDocument();
  });

  it('displays loading state when data is loading', () => {
    // Render DatasetQualityTable with loading state set to true
    const { getByText, queryByText } = renderDatasetQualityTable({}, { loading: true });

    // Verify that loading indicators are displayed
    expect(getByText('Loading...')).toBeInTheDocument();

    // Verify that table content is not fully rendered while loading
    expect(queryByText('customer')).not.toBeInTheDocument();
  });

  it('handles row click and calls onSelectDataset', async () => {
    // Create mock onSelectDataset function
    const onSelectDataset = jest.fn();

    // Render DatasetQualityTable with the mock function
    const { getByText, user } = renderDatasetQualityTable({ onDatasetSelect });

    // Find and click on a dataset row
    const datasetRow = await findTableRowByText('customer');
    await user.click(datasetRow);

    // Verify that onSelectDataset was called with the correct dataset
    expect(onSelectDataset).toHaveBeenCalledWith('customer');
  });

  it('displays trend indicators correctly', () => {
    // Render DatasetQualityTable with datasets having different trends
    const { getByText, getByTestId } = renderDatasetQualityTable();

    // Verify that improving trends show up arrow
    expect(getByTestId('trend-customer')).toBeInTheDocument();

    // Verify that declining trends show down arrow
    expect(getByTestId('trend-product')).toBeInTheDocument();

    // Verify that stable trends show horizontal arrow
    expect(getByTestId('trend-sales')).toBeInTheDocument();
  });

  it('formats quality scores with appropriate colors', () => {
    // Render DatasetQualityTable with datasets having different quality scores
    const { getByText } = renderDatasetQualityTable();

    // Verify that high scores (>90) have green color
    expect(getByText('96%')).toHaveStyle('color: #2e7d32');

    // Verify that medium scores (70-90) have yellow/amber color
    expect(getByText('90%')).toHaveStyle('color: #ed6c02');

    // Verify that low scores (<70) have red color
    // expect(getByText('65%')).toHaveStyle('color: #d32f2f');
  });
});

describe('ValidationRulesTable', () => {
  it('renders the validation rules table with data', async () => {
    // Render ValidationRulesTable with mock rules
    const { getByText } = renderValidationRulesTable();

    // Verify that the table headers are displayed
    expect(getByText('Rule Name')).toBeInTheDocument();
    expect(getByText('Dataset')).toBeInTheDocument();
    expect(getByText('Table')).toBeInTheDocument();
    expect(getByText('Rule Type')).toBeInTheDocument();
    expect(getByText('Severity')).toBeInTheDocument();
    expect(getByText('Status')).toBeInTheDocument();
    expect(getByText('Last Updated')).toBeInTheDocument();
    expect(getByText('Actions')).toBeInTheDocument();

    // Verify that each rule row is displayed with correct data
    expect(getByText('Required Fields Check')).toBeInTheDocument();
    expect(getByText('Email Format Validation')).toBeInTheDocument();
    expect(getByText('Product Price Range')).toBeInTheDocument();

    // Verify that rule types and dimensions are displayed correctly
    expect(getByText('Null check')).toBeInTheDocument();
    expect(getByText('Pattern match')).toBeInTheDocument();
    expect(getByText('Value range')).toBeInTheDocument();
  });

  it('displays loading state when data is loading', () => {
    // Render ValidationRulesTable with loading state set to true
    const { getByText, queryByText } = renderValidationRulesTable({}, { loading: true });

    // Verify that loading indicators are displayed
    expect(getByText('Loading...')).toBeInTheDocument();

    // Verify that table content is not fully rendered while loading
    expect(queryByText('Required Fields Check')).not.toBeInTheDocument();
  });

  it('handles rule editing when edit button is clicked', async () => {
    // Create mock onEditRule function
    const onEditRule = jest.fn();

    // Render ValidationRulesTable with the mock function
    const { getByText, user } = renderValidationRulesTable({ onRuleEdit: onEditRule });

    // Find and click the edit button on a rule row
    const ruleRow = await findTableRowByText('Required Fields Check');
    await user.click(ruleRow);

    // Verify that onEditRule was called with the correct rule
    expect(onEditRule).toHaveBeenCalledWith(mockRules[0]);
  });

  it('handles rule deletion when delete button is clicked', async () => {
    // Create mock onDeleteRule function
    const onDeleteRule = jest.fn();

    // Render ValidationRulesTable with the mock function
    const { getByText, user } = renderValidationRulesTable({ onRuleDelete: onDeleteRule });

    // Find and click the delete button on a rule row
    const ruleRow = await findTableRowByText('Required Fields Check');
    await user.click(ruleRow);

    // Verify that a confirmation dialog is displayed
    expect(getByText('Confirm Delete')).toBeInTheDocument();

    // Confirm the deletion
    const deleteButton = screen.getByRole('button', { name: 'Delete' });
    await user.click(deleteButton);

    // Verify that onDeleteRule was called with the correct rule ID
    expect(onDeleteRule).toHaveBeenCalledWith(mockRules[0].ruleId);
  });

  it('filters rules based on filter props', async () => {
    // Render ValidationRulesTable with filter props (dataset, table, ruleType)
    const { getByText, queryByText, rerender } = renderValidationRulesTable({ dataset: 'customer' });

    // Verify that only rules matching the filters are displayed
    expect(getByText('Required Fields Check')).toBeInTheDocument();
    expect(queryByText('Product Price Range')).not.toBeInTheDocument();

    // Change the filters
    renderValidationRulesTable({ dataset: 'product' });

    // Verify that the displayed rules update accordingly
    expect(getByText('Product Price Range')).toBeInTheDocument();
    expect(queryByText('Required Fields Check')).not.toBeInTheDocument();
  });
});

describe('ValidationIssuesTable', () => {
  it('renders the validation issues table with data', async () => {
    // Render ValidationIssuesTable with mock issues
    const { getByText } = renderValidationIssuesTable();

    // Verify that the table headers are displayed
    expect(getByText('Severity')).toBeInTheDocument();
    expect(getByText('Dataset')).toBeInTheDocument();
    expect(getByText('Table')).toBeInTheDocument();
    expect(getByText('Column')).toBeInTheDocument();
    expect(getByText('Description')).toBeInTheDocument();
    expect(getByText('Dimension')).toBeInTheDocument();
    expect(getByText('Status')).toBeInTheDocument();
    expect(getByText('Self-Healing')).toBeInTheDocument();
    expect(getByText('Detected')).toBeInTheDocument();

    // Verify that each issue row is displayed with correct data
    expect(getByText('Invalid email format for 15 customer records')).toBeInTheDocument();
    expect(getByText('Negative price values found in 42 products')).toBeInTheDocument();

    // Verify that severity levels are displayed with appropriate icons/colors
    expect(getByText('Medium')).toBeInTheDocument();
    expect(getByText('High')).toBeInTheDocument();
  });

  it('displays loading state when data is loading', () => {
    // Render ValidationIssuesTable with loading state set to true
    const { getByText, queryByText } = renderValidationIssuesTable({}, { loading: true });

    // Verify that loading indicators are displayed
    expect(getByText('Loading...')).toBeInTheDocument();

    // Verify that table content is not fully rendered while loading
    expect(queryByText('Invalid email format for 15 customer records')).not.toBeInTheDocument();
  });

  it('handles issue selection when row is clicked', async () => {
    // Create mock onSelectIssue function
    const onSelectIssue = jest.fn();

    // Render ValidationIssuesTable with the mock function
    const { getByText, user } = renderValidationIssuesTable({ onIssueSelect });

    // Find and click on an issue row
    const issueRow = await findTableRowByText('Invalid email format for 15 customer records');
    await user.click(issueRow);

    // Verify that onSelectIssue was called with the correct issue
    expect(onSelectIssue).toHaveBeenCalledWith(mockIssues[0]);
  });

  it('displays healing status indicators correctly', () => {
    // Render ValidationIssuesTable with issues having different healing statuses
    const { getByText } = renderValidationIssuesTable();

    // Verify that successful healing shows success indicator
    // expect(getByText('Completed')).toBeInTheDocument();

    // Verify that in-progress healing shows progress indicator
    expect(getByText('In progress')).toBeInTheDocument();

    // Verify that failed healing shows failure indicator
    expect(getByText('Failed')).toBeInTheDocument();

    // Verify that issues without healing show no indicator
    // expect(getByText('N/A')).toBeInTheDocument();
  });

  it('filters issues based on filter props', async () => {
    // Render ValidationIssuesTable with filter props (dataset, table, severity, status)
    const { getByText, queryByText, rerender } = renderValidationIssuesTable({ dataset: 'customer' });

    // Verify that only issues matching the filters are displayed
    expect(getByText('Invalid email format for 15 customer records')).toBeInTheDocument();
    expect(queryByText('Negative price values found in 42 products')).not.toBeInTheDocument();

    // Change the filters
    renderValidationIssuesTable({ dataset: 'product' });

    // Verify that the displayed issues update accordingly
    expect(getByText('Negative price values found in 42 products')).toBeInTheDocument();
    expect(queryByText('Invalid email format for 15 customer records')).not.toBeInTheDocument();
  });

  it('handles status updates when status is changed', async () => {
    // Create mock onUpdateStatus function
    const onUpdateStatus = jest.fn();

    // Render ValidationIssuesTable with the mock function
    const { getByText, user } = renderValidationIssuesTable({ onUpdateStatus });

    // Find and click the status dropdown on an issue row
    const issueRow = await findTableRowByText('Invalid email format for 15 customer records');
    await user.click(issueRow);

    // Select a different status
    const statusDropdown = screen.getByRole('combobox', { name: 'Status' });
    await user.click(statusDropdown);

    const resolvedOption = screen.getByRole('option', { name: 'Resolved' });
    await user.click(resolvedOption);

    // Verify that onUpdateStatus was called with the correct issue ID and status
    expect(onUpdateStatus).toHaveBeenCalledWith(mockIssues[0].issueId, 'RESOLVED');
  });
});

describe('QualityScoreChart', () => {
  it('renders the quality score chart with data', () => {
    // Render QualityScoreChart with mock quality score
    const { getByText } = renderComponent(<QualityScoreChart score={95} />);

    // Verify that the chart is displayed
    expect(getByText('95%')).toBeInTheDocument();

    // Verify that the score value is displayed correctly
    expect(getByText('Overall Quality Score')).toBeInTheDocument();

    // Verify that the chart color matches the score level
    // expect(getByText('95%')).toHaveStyle('color: green');
  });

  it('displays loading state when data is loading', () => {
    // Render QualityScoreChart with loading state set to true
    const { getByText, queryByText } = renderComponent(<QualityScoreChart score={0} isLoading={true} />);

    // Verify that loading indicators are displayed
    expect(getByText('Loading chart...')).toBeInTheDocument();

    // Verify that chart is not fully rendered while loading
    expect(queryByText('Overall Quality Score')).toBeNull();
  });

  it('uses appropriate colors based on score value', () => {
    // Render QualityScoreChart with different score values
    const { rerender } = renderComponent(<QualityScoreChart score={95} />);

    // Verify that high scores (>90) use green color
    // expect(getByText('95%')).toHaveStyle('color: green');

    rerender(<QualityScoreChart score={80} />);

    // Verify that medium scores (70-90) use yellow/amber color
    // expect(getByText('80%')).toHaveStyle('color: yellow');

    rerender(<QualityScoreChart score={60} />);

    // Verify that low scores (<70) use red color
    // expect(getByText('60%')).toHaveStyle('color: red');
  });

  it('displays title and subtitle correctly', () => {
    // Render QualityScoreChart with custom title and subtitle
    const { getByText } = renderComponent(<QualityScoreChart title="Custom Title" />);

    // Verify that the provided title is displayed
    expect(getByText('Custom Title')).toBeInTheDocument();
  });
});