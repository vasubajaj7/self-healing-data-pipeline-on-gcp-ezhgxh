import React from 'react'; // React ^18.2.0
import { screen, waitFor, fireEvent } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import { renderWithProviders } from '../../test/utils/renderWithProviders';
import DashboardOverview from '../DashboardOverview';
import { mockDashboardData } from '../../test/mocks/data';
import { DashboardContext } from '../../contexts/DashboardContext';
import { TimeRange } from '../../types/dashboard';
import jest from 'jest'; // jest ^29.5.0

// LD1: Mock the react-router-dom module to provide navigation functionality
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => jest.fn(),
}));

// LD1: Mock the dashboard context for controlling component state
jest.mock('../../contexts/DashboardContext', () => ({
  useDashboard: () => ({
    dashboardData: mockDashboardData,
    loading: false,
    error: null,
    filters: { timeRange: 'LAST_24_HOURS' },
    setFilters: jest.fn(),
    fetchDashboardData: jest.fn(),
    refreshInterval: 60000,
    setRefreshInterval: jest.fn(),
  }),
}));

interface SetupOptions {
  customProps?: any;
}

// LD1: Sets up the test environment with mocked context values
const setup = (customProps: SetupOptions = {}) => {
  // LD1: Create mock dashboard context value with loading state, data, and functions
  const mockContextValue = {
    dashboardData: mockDashboardData,
    loading: false,
    error: null,
    filters: { timeRange: 'LAST_24_HOURS' },
    setFilters: jest.fn(),
    fetchDashboardData: jest.fn(),
    refreshInterval: 60000,
    setRefreshInterval: jest.fn(),
    ...customProps,
  };

  // LD1: Create mock navigation function
  const navigate = jest.fn();

  // LD1: Render DashboardOverview with renderWithProviders utility
  const renderResult = renderWithProviders(<DashboardOverview />, {
    dashboardProviderProps: mockContextValue,
  });

  // LD1: Return rendered component and utility functions
  return {
    ...renderResult,
    mockContextValue,
    navigate,
  };
};

// LD1: Tests for the DashboardOverview component
describe('DashboardOverview Component', () => {
  // LD1: Reset all mocks before each test
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // LD1: Clean up any resources after each test
  afterEach(() => {
    jest.restoreAllMocks();
  });

  // LD1: Verifies that the component renders without errors
  it('renders without crashing', () => {
    // LD1: Render the DashboardOverview component with mock data
    const { getByText } = setup();

    // LD1: Verify that the component is in the document
    expect(getByText('Dashboard Overview')).toBeInTheDocument();
  });

  // LD1: Verifies that loading indicators are shown when data is loading
  it('displays loading state correctly', () => {
    // LD1: Render the DashboardOverview component with loading state set to true
    const { getByText } = setup({ loading: true });

    // LD1: Verify that loading indicators are displayed
    expect(getByText('Loading alerts...')).toBeInTheDocument();
  });

  // LD1: Verifies that dashboard data is displayed correctly when loaded
  it('displays dashboard data correctly', () => {
    // LD1: Render the DashboardOverview component with mock dashboard data
    const { getByText } = setup();

    // LD1: Verify that pipeline health metrics are displayed
    expect(getByText('Pipeline Health')).toBeInTheDocument();

    // LD1: Verify that data quality metrics are displayed
    expect(getByText('Data Quality')).toBeInTheDocument();

    // LD1: Verify that self-healing metrics are displayed
    expect(getByText('Self-Healing')).toBeInTheDocument();

    // LD1: Verify that recent executions are displayed
    expect(getByText('Recent Executions')).toBeInTheDocument();

    // LD1: Verify that system status is displayed
    expect(getByText('System Status')).toBeInTheDocument();

    // LD1: Verify that AI insights are displayed
    expect(getByText('AI Insights')).toBeInTheDocument();
  });

  // LD1: Verifies that changing the time range filter updates the context
  it('handles time range filter changes', async () => {
    // LD1: Render the DashboardOverview component
    const { getByLabelText, mockContextValue } = setup();

    // LD1: Find the time range select element
    const timeRangeSelect = getByLabelText('Time Range');

    // LD1: Change the selection to a different time range
    userEvent.selectOptions(timeRangeSelect, ['LAST_7_DAYS']);

    // LD1: Verify that setFilters was called with the new time range
    await waitFor(() => {
      expect(mockContextValue.setFilters).toHaveBeenCalledWith({
        timeRange: 'LAST_7_DAYS',
      });
    });
  });

  // LD1: Verifies that clicking the refresh button triggers data refresh
  it('handles refresh button click', async () => {
    // LD1: Render the DashboardOverview component
    const { getByRole, mockContextValue } = setup();

    // LD1: Find the refresh button
    const refreshButton = getByRole('button', { name: 'refresh' });

    // LD1: Click the refresh button
    fireEvent.click(refreshButton);

    // LD1: Verify that fetchDashboardData was called
    await waitFor(() => {
      expect(mockContextValue.fetchDashboardData).toHaveBeenCalledTimes(1);
    });
  });

  // LD1: Verifies that clicking view all executions navigates to the pipeline management page
  it('navigates to pipeline management when view all is clicked', async () => {
    // LD1: Render the DashboardOverview component
    const { getByText, navigate } = setup();

    // LD1: Find the view all executions button
    const viewAllButton = getByText('View All');

    // LD1: Click the view all executions button
    fireEvent.click(viewAllButton);

    // LD1: Verify that navigate was called with the correct path
    await waitFor(() => {
      expect(navigate).toHaveBeenCalledWith('/pipeline');
    });
  });

  // LD1: Verifies that error messages are displayed when data loading fails
  it('displays error state correctly', () => {
    // LD1: Render the DashboardOverview component with error state
    const { getByText } = setup({ error: 'Failed to load data' });

    // LD1: Verify that error messages are displayed
    expect(getByText('Error: Failed to load data')).toBeInTheDocument();
  });
});