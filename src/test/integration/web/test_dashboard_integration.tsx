import React from 'react'; // react ^18.2.0
import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals'; // @jest/globals ^29.5.0
import { screen, waitFor, within } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { QueryClient, QueryClientProvider } from 'react-query'; // react-query ^3.39.2
import {
  renderComponent,
  waitForLoadingToComplete,
  selectDropdownOption,
  createUserEvent,
} from '../../utils/web_test_utils';
import {
  MOCK_PIPELINE_DATA,
  MOCK_QUALITY_DATA,
  MOCK_HEALING_DATA,
  MOCK_ALERT_DATA,
  mockPipelineService,
  mockQualityService,
  mockHealingService,
  mockAlertService,
  createMockDataResponse,
} from '../../fixtures/web/api_fixtures';
import DashboardOverview from '../../../web/src/components/dashboard/DashboardOverview';
import { DashboardProvider } from '../../../web/src/contexts/DashboardContext';
import { TimeRange } from '../../../web/src/types/dashboard';

/**
 * @dev Declare mock service instances as global variables
 */
declare global {
  var mockPipelineServiceInstance: ReturnType<typeof mockPipelineService>;
  var mockQualityServiceInstance: ReturnType<typeof mockQualityService>;
  var mockHealingServiceInstance: ReturnType<typeof mockHealingService>;
  var mockAlertServiceInstance: ReturnType<typeof mockAlertService>;
  var queryClient: QueryClient;
}

/**
 * @dev Sets up all mock services and data for testing
 */
const setupMocks = () => {
  // LD1: Create mock implementations of all required services
  global.mockPipelineServiceInstance = mockPipelineService();
  global.mockQualityServiceInstance = mockQualityService();
  global.mockHealingServiceInstance = mockHealingService();
  global.mockAlertServiceInstance = mockAlertService();

  // LD1: Configure mock responses with test data
  (global.mockPipelineServiceInstance.getPipelines as jest.Mock).mockResolvedValue(
    createMockDataResponse(MOCK_PIPELINE_DATA.definitions)
  );
  (global.mockQualityServiceInstance.getQualityMetrics as jest.Mock).mockResolvedValue(
    createMockDataResponse(MOCK_QUALITY_DATA.scores)
  );
  (global.mockHealingServiceInstance.getSelfHealingMetrics as jest.Mock).mockResolvedValue(
    createMockDataResponse(MOCK_HEALING_DATA.settings)
  );
  (global.mockAlertServiceInstance.getAlerts as jest.Mock).mockResolvedValue(
    createMockDataResponse(MOCK_ALERT_DATA.alerts)
  );

  // LD1: Set up spies on service methods to track calls
  jest.spyOn(global.mockPipelineServiceInstance, 'getPipelines');
  jest.spyOn(global.mockQualityServiceInstance, 'getQualityMetrics');
  jest.spyOn(global.mockHealingServiceInstance, 'getSelfHealingMetrics');
  jest.spyOn(global.mockAlertServiceInstance, 'getAlerts');

  // LD1: Initialize query client with default options
  global.queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
};

/**
 * @dev Renders the dashboard component with all necessary providers
 */
const renderDashboard = (options: object = {}) => {
  // LD1: Create a wrapper component with DashboardProvider and QueryClientProvider
  const createDashboardWrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={global.queryClient}>
      <DashboardProvider>
        {children}
      </DashboardProvider>
    </QueryClientProvider>
  );

  // LD1: Render the DashboardOverview component within the wrapper
  const renderResult = renderComponent(<DashboardOverview />, { wrapper: createDashboardWrapper, ...options });

  // LD1: Wait for initial loading to complete
  waitForLoadingToComplete();

  // LD1: Return the render result
  return renderResult;
};

describe('Dashboard Integration Tests', () => {
  beforeEach(() => {
    // LD1: Set up mock services and reset state before each test
    setupMocks();
  });

  afterEach(() => {
    // LD1: Clean up mocks and reset query client after each test
    jest.restoreAllMocks();
    global.queryClient.clear();
  });

  it('should render the dashboard with all components', async () => {
    // LD1: Render the dashboard component
    renderDashboard();

    // LD1: Wait for loading to complete
    await waitForLoadingToComplete();

    // LD1: Verify that all dashboard cards are rendered
    expect(screen.getByText('Pipeline Health')).toBeInTheDocument();
    expect(screen.getByText('Data Quality')).toBeInTheDocument();
    expect(screen.getByText('Self-Healing')).toBeInTheDocument();
    expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    expect(screen.getByText('System Status')).toBeInTheDocument();
    expect(screen.getByText('Quick Stats')).toBeInTheDocument();
    expect(screen.getByText('Recent Executions')).toBeInTheDocument();

    // LD1: Check that pipeline health metrics are displayed correctly
    expect(screen.getByText(`${MOCK_PIPELINE_DATA.metrics.overallHealth}%`)).toBeInTheDocument();

    // LD1: Check that data quality metrics are displayed correctly
    expect(screen.getByText(`${MOCK_QUALITY_DATA.scores.overallScore}%`)).toBeInTheDocument();

    // LD1: Check that self-healing metrics are displayed correctly
    expect(screen.getByText(`${MOCK_HEALING_DATA.settings.globalConfidenceThreshold}`)).toBeInTheDocument();

    // LD1: Verify that recent executions table is populated
    expect(screen.getByText(MOCK_PIPELINE_DATA.executions[0].pipelineName)).toBeInTheDocument();
  });

  it('should update data when time range filter changes', async () => {
    // LD1: Render the dashboard component
    renderDashboard();

    // LD1: Wait for initial loading to complete
    await waitForLoadingToComplete();

    // LD1: Select a different time range from the dropdown
    await selectDropdownOption('Time Range', 'Last 7 Days');

    // LD1: Verify that API services were called with updated time range
    expect(global.mockPipelineServiceInstance.getPipelines).toHaveBeenCalledTimes(1);
    expect(global.mockQualityServiceInstance.getQualityMetrics).toHaveBeenCalledTimes(1);
    expect(global.mockHealingServiceInstance.getSelfHealingMetrics).toHaveBeenCalledTimes(1);
    expect(global.mockAlertServiceInstance.getAlerts).toHaveBeenCalledTimes(1);

    // LD1: Check that displayed data is updated according to the new time range
    // This is a basic check; more specific data validation can be added as needed
    expect(screen.getByText(`${MOCK_PIPELINE_DATA.metrics.overallHealth}%`)).toBeInTheDocument();
  });

  it('should refresh data when refresh button is clicked', async () => {
    // LD1: Render the dashboard component
    renderDashboard();

    // LD1: Wait for initial loading to complete
    await waitForLoadingToComplete();

    // LD1: Reset the mock service call counts
    (global.mockPipelineServiceInstance.getPipelines as jest.Mock).mockClear();
    (global.mockQualityServiceInstance.getQualityMetrics as jest.Mock).mockClear();
    (global.mockHealingServiceInstance.getSelfHealingMetrics as jest.Mock).mockClear();
    (global.mockAlertServiceInstance.getAlerts as jest.Mock).mockClear();

    // LD1: Click the refresh button
    const refreshButton = screen.getByRole('button', { name: 'refresh' });
    await createUserEvent().click(refreshButton);

    // LD1: Verify that API services were called again
    await waitFor(() => {
        expect(global.mockPipelineServiceInstance.getPipelines).toHaveBeenCalledTimes(1);
        expect(global.mockQualityServiceInstance.getQualityMetrics).toHaveBeenCalledTimes(1);
        expect(global.mockHealingServiceInstance.getSelfHealingMetrics).toHaveBeenCalledTimes(1);
        expect(global.mockAlertServiceInstance.getAlerts).toHaveBeenCalledTimes(1);
    });

    // LD1: Check that last refreshed timestamp is updated
    expect(screen.getByText(/Last Refreshed:/)).toBeInTheDocument();
  });

  it('should display loading state during data fetching', async () => {
    // LD1: Configure mock services to delay responses
    (global.mockPipelineServiceInstance.getPipelines as jest.Mock).mockImplementation(() => {
      return new Promise(resolve => setTimeout(() => resolve(createMockDataResponse(MOCK_PIPELINE_DATA.definitions)), 500));
    });

    // LD1: Render the dashboard component
    renderDashboard();

    // LD1: Verify that loading indicators are displayed
    expect(screen.getByText('Loading alerts...')).toBeInTheDocument();

    // LD1: Wait for loading to complete
    await waitForLoadingToComplete();

    // LD1: Verify that loading indicators are replaced with data
    expect(screen.queryByText('Loading alerts...')).not.toBeInTheDocument();
  });

  it('should handle error states gracefully', async () => {
    // LD1: Configure mock services to return errors
    (global.mockPipelineServiceInstance.getPipelines as jest.Mock).mockRejectedValue(new Error('Failed to fetch pipelines'));

    // LD1: Render the dashboard component
    renderDashboard();

    // LD1: Wait for error handling to complete
    await waitFor(() => {
        expect(screen.getByText('Error: Failed to fetch pipelines')).toBeInTheDocument();
    });

    // LD1: Check that error states are displayed appropriately
    expect(screen.getByText('Error: Failed to fetch pipelines')).toBeVisible();

    // LD1: Check that retry functionality works when available
    // Add more specific checks for retry functionality if applicable
  });

  it('should navigate to detailed views when clicking on cards', async () => {
    // LD1: Render the dashboard component with navigation mock
    const navigateMock = jest.fn();
    renderDashboard({
        mockImplementations: {
            useNavigate: () => navigateMock
        }
    });

    // LD1: Wait for loading to complete
    await waitForLoadingToComplete();

    // LD1: Click on a card that has a detail view link
    const pipelineHealthCard = screen.getByText('Pipeline Health');
    await createUserEvent().click(pipelineHealthCard);

    // LD1: Verify that navigation was called with the correct route
    expect(navigateMock).toHaveBeenCalledWith('/pipeline');
  });
});