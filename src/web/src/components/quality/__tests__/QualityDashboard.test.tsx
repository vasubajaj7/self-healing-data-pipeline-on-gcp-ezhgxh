import React from 'react'; // React ^18.2.0
import { screen, waitFor, fireEvent, within } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import { renderWithProviders } from '../../../test/utils/renderWithProviders';
import QualityDashboard from '../QualityDashboard';
import { QualityContext } from '../../../contexts/QualityContext';
import { mockQualityData } from '../../../test/mocks/data';
import { QualityDimension, QualityRuleType, QualityIssueStatus } from '../../../types/quality';

// Mock the react-router-dom module to provide navigation functionality
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => jest.fn(),
}));

/**
 * Sets up the test environment with mocked context values
 * @param {object} customProps - Custom properties to override the default context values
 * @returns {object} Rendered component and utility functions
 */
const setup = (customProps = {}) => {
  // Create mock quality context value with datasets, rules, issues, statistics, loading state, and functions
  const mockQualityContextValue = {
    ...mockQualityData,
    ...customProps,
  };

  // Create mock navigation function
  const mockNavigate = jest.fn();

  // Render QualityDashboard with renderWithProviders utility
  const renderResult = renderWithProviders(
    <QualityDashboard />,
    {
      preloadedState: {},
      overrides: [
        {
          provide: QualityContext,
          useValue: mockQualityContextValue,
        },
      ],
    }
  );

  // Return rendered component and utility functions
  return {
    ...renderResult,
    mockNavigate,
  };
};

describe('QualityDashboard Component', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('renders without crashing', () => {
    // Render the QualityDashboard component with mock data
    const { getByText } = setup();

    // Verify that the component is in the document
    expect(screen.getByText('Data Quality Dashboard')).toBeInTheDocument();
  });

  it('displays loading state correctly', () => {
    // Render the QualityDashboard component with loading state set to true
    setup({ loading: true });

    // Verify that loading indicators are displayed
    expect(screen.getByRole('progressbar')).toBeVisible();
  });

  it('displays quality data correctly', () => {
    // Render the QualityDashboard component with mock quality data
    const { getByText } = setup();

    // Verify that quality score is displayed
    expect(getByText('98%')).toBeInTheDocument();

    // Verify that quality dimensions are displayed
    expect(getByText('Completeness')).toBeInTheDocument();

    // Verify that dataset quality table is displayed
    expect(getByText('customer_data')).toBeInTheDocument();

    // Verify that quality trend chart is visible
    expect(screen.getByText('Quality Score Trend')).toBeInTheDocument();
  });

  it('handles tab navigation correctly', async () => {
    // Render the QualityDashboard component
    const { getByRole } = setup();

    // Click on different tabs
    const rulesTab = getByRole('tab', { name: 'Rules' });
    fireEvent.click(rulesTab);

    // Verify that the correct content is displayed for each tab
    await waitFor(() => {
      expect(screen.getByText('Validation Rules')).toBeVisible();
    });

    const issuesTab = getByRole('tab', { name: 'Issues' });
    fireEvent.click(issuesTab);

    await waitFor(() => {
      expect(screen.getByText('Validation Issues')).toBeVisible();
    });
  });

  it('handles dataset selection correctly', async () => {
    // Render the QualityDashboard component
    const { getByText, mockNavigate } = setup();

    // Find a dataset in the dataset quality table
    const datasetLink = getByText('customer_data');

    // Click on the dataset
    fireEvent.click(datasetLink);

    // Verify that the selected dataset is displayed in detail view
    await waitFor(() => {
      expect(screen.getByText('customer_data Quality Score')).toBeVisible();
    });
  });

  it('handles table selection correctly', async () => {
    // Render the QualityDashboard component
    const { getByText, mockNavigate } = setup();

    // Find a dataset in the dataset quality table
    const datasetLink = getByText('customer_data');

    // Click on the dataset
    fireEvent.click(datasetLink);

    // Verify that the selected dataset is displayed in detail view
    await waitFor(() => {
      expect(screen.getByText('customer_data Quality Score')).toBeVisible();
    });

    // Find a table in the dataset detail view
    const tableLink = getByText('customer_profiles');

    // Click on the table
    fireEvent.click(tableLink);

    // Verify that the selected table is displayed in detail view
    await waitFor(() => {
      expect(screen.getByText('customer_profiles Quality Score')).toBeVisible();
    });
  });

  it('handles refresh button click', () => {
    // Render the QualityDashboard component
    const { getByText } = setup({
      fetchQualityData: jest.fn(),
    });

    // Find the refresh button
    const refreshButton = getByText('Refresh');

    // Click the refresh button
    fireEvent.click(refreshButton);

    // Verify that fetchQualityData was called
    expect(mockQualityData.fetchQualityData).toHaveBeenCalledTimes(1);
  });

  it('handles rule editing correctly', async () => {
    // Render the QualityDashboard component
    const { getByRole, getByText } = setup();

    // Navigate to the Rules tab
    const rulesTab = getByRole('tab', { name: 'Rules' });
    fireEvent.click(rulesTab);

    // Find the edit button for a rule
    await waitFor(() => {
      expect(screen.getByText('Validation Rules')).toBeVisible();
    });

    const editButton = screen.getAllByRole('button', { name: 'Edit Rule' })[0];

    // Click the edit button
    fireEvent.click(editButton);

    // Verify that the rule editor modal is displayed
    await waitFor(() => {
      expect(screen.getByText('Edit Validation Rule')).toBeVisible();
    });
  });

  it('handles rule addition correctly', async () => {
    // Render the QualityDashboard component
    const { getByRole, getByText } = setup();

    // Find the add rule button
    const addRuleButton = screen.getByRole('button', { name: 'Add Rule' });

    // Click the add rule button
    fireEvent.click(addRuleButton);

    // Verify that the rule editor modal is displayed
    await waitFor(() => {
      expect(screen.getByText('Create Validation Rule')).toBeVisible();
    });
  });

  it('handles rule editor close correctly', async () => {
    // Render the QualityDashboard component
    const { getByRole, getByText } = setup({
      fetchQualityData: jest.fn(),
    });

    // Find the add rule button
    const addRuleButton = screen.getByRole('button', { name: 'Add Rule' });

    // Click the add rule button
    fireEvent.click(addRuleButton);

    // Verify that the rule editor modal is displayed
    await waitFor(() => {
      expect(screen.getByText('Create Validation Rule')).toBeVisible();
    });

    // Find the close button
    const cancelButton = screen.getByRole('button', { name: 'Cancel' });

    // Click the close button
    fireEvent.click(cancelButton);

    // Verify that the rule editor is closed
    await waitFor(() => {
      expect(screen.queryByText('Create Validation Rule')).not.toBeInTheDocument();
    });

    // Verify that quality data is refreshed
    expect(mockQualityData.fetchQualityData).toHaveBeenCalledTimes(1);
  });

  it('displays error state correctly', () => {
    // Render the QualityDashboard component with error state
    setup({ error: 'Failed to load data' });

    // Verify that error messages are displayed
    expect(screen.getByText('Failed to load data')).toBeVisible();
  });

  it('initializes with correct tab when initialTab prop is provided', () => {
    // Render the QualityDashboard component with initialTab prop set to Rules tab index
    const { getByText } = setup({ initialTab: 3 });

    // Verify that the Rules tab is active
    expect(screen.getByText('Validation Rules')).toBeVisible();
  });
});