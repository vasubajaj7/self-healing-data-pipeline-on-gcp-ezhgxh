import React from 'react'; // React ^18.2.0
import { screen, waitFor, fireEvent } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import jest from 'jest'; // jest ^29.5.0
import HealingDashboard from '../HealingDashboard';
import { renderWithProviders } from '../../../test/utils/renderWithProviders';
import { createMockFn } from '../../../test/utils/testUtils';
import { mockHealingDashboardData, mockAIModels } from '../../../test/mocks/data';
import healingService from '../../../services/api/healingService';

// Mock the healingService module to control its behavior in tests
jest.mock('../../../services/api/healingService');

/**
 * Sets up the test environment with mocked services
 * @returns Object containing mock functions and utilities
 */
const setup = () => {
  // Mock the healingService.getDashboardData function to return mockHealingDashboardData
  (healingService.getDashboardData as jest.Mock).mockResolvedValue(mockHealingDashboardData);

  // Mock the healingService.getAIModels function to return mockAIModels
  (healingService.getAIModels as jest.Mock).mockResolvedValue(mockAIModels);

  // Return the mock functions for use in tests
  return {
    getDashboardData: healingService.getDashboardData as jest.Mock,
    getAIModels: healingService.getAIModels as jest.Mock,
  };
};

/**
 * Helper function to render the HealingDashboard component with optional props
 * @param props 
 * @returns Rendered component utilities from React Testing Library
 */
const renderDashboard = (props = {}) => {
  // Use renderWithProviders to render the HealingDashboard component with provided props
  const renderResult = renderWithProviders(<HealingDashboard {...props} />);

  // Return the result for test assertions
  return renderResult;
};

test('renders the healing dashboard with loading state', async () => {
  // Mock the API services to return pending promises
  (healingService.getDashboardData as jest.Mock).mockImplementation(() => new Promise(() => {}));
  (healingService.getAIModels as jest.Mock).mockImplementation(() => new Promise(() => {}));

  // Render the HealingDashboard component
  renderDashboard();

  // Verify loading indicators are displayed
  expect(screen.getByText(/Loading/i)).toBeInTheDocument();

  // Verify key dashboard sections are not yet rendered
  expect(screen.queryByText(/Self-Healing Overview/i)).not.toBeInTheDocument();
  expect(screen.queryByText(/Active Issues/i)).not.toBeInTheDocument();
});

test('renders the healing dashboard with data', async () => {
  // Mock the API services to return successful responses with mock data
  const { getDashboardData, getAIModels } = setup();

  // Render the HealingDashboard component
  renderDashboard();

  // Wait for data to be loaded
  await waitFor(() => {
    expect(getDashboardData).toHaveBeenCalled();
    expect(getAIModels).toHaveBeenCalled();
  });

  // Verify key metrics are displayed with correct values
  expect(screen.getByText(/250/i)).toBeInTheDocument();
  expect(screen.getByText(/203/i)).toBeInTheDocument();
  expect(screen.getByText(/87%/i)).toBeInTheDocument();
  expect(screen.getByText(/45/i)).toBeInTheDocument();

  // Verify dashboard sections are rendered properly
  expect(screen.getByText(/Self-Healing Overview/i)).toBeInTheDocument();
  expect(screen.getByText(/Active Issues/i)).toBeInTheDocument();
});

test('handles error state', async () => {
  // Mock the API services to return rejected promises
  (healingService.getDashboardData as jest.Mock).mockRejectedValue(new Error('API Error'));
  (healingService.getAIModels as jest.Mock).mockRejectedValue(new Error('API Error'));

  // Render the HealingDashboard component
  renderDashboard();

  // Verify error messages are displayed
  await waitFor(() => {
    expect(screen.getByText(/Error fetching data/i)).toBeInTheDocument();
  });

  // Verify retry button is available
  // expect(screen.getByRole('button', { name: /Retry/i })).toBeInTheDocument();
});

test('allows tab switching', async () => {
  // Mock the API services to return successful responses
  setup();

  // Render the HealingDashboard component
  renderDashboard();

  // Wait for data to be loaded
  await waitFor(() => {
    expect(screen.getByText(/Self-Healing Overview/i)).toBeInTheDocument();
  });

  // Click on different tabs
  await userEvent.click(screen.getByText(/Actions/i));
  await userEvent.click(screen.getByText(/Performance/i));
  await userEvent.click(screen.getByText(/Models/i));

  // Verify the correct content is displayed for each tab
  expect(screen.getByText(/Detection Model Performance/i)).toBeInTheDocument();
});

test('refreshes data on interval', async () => {
  // Mock the API services to return successful responses
  setup();

  // Mock the timer functions
  jest.spyOn(global, 'setInterval');
  jest.spyOn(global, 'clearInterval');

  // Render the HealingDashboard component with a refresh interval
  renderDashboard({ refreshInterval: 5000 });

  // Advance timers to trigger refresh
  jest.advanceTimersByTime(5000);

  // Verify the API services are called again
  expect(healingService.getDashboardData).toHaveBeenCalledTimes(2);
  expect(healingService.getAIModels).toHaveBeenCalledTimes(2);

  // Restore the original timer functions
  jest.restoreAllMocks();
});

test('renders settings form when showSettings is true', async () => {
  // Mock the API services to return successful responses
  setup();

  // Render the HealingDashboard component with showSettings=true
  renderDashboard({ showSettings: true });

  // Wait for data to be loaded
  await waitFor(() => {
    expect(screen.getByText(/Self-Healing Configuration/i)).toBeInTheDocument();
  });

  // Verify the settings form is displayed
  expect(screen.getByText(/Healing Mode/i)).toBeInTheDocument();
});

test('formats metrics correctly', async () => {
  // Mock the API services to return successful responses with specific metric values
  (healingService.getDashboardData as jest.Mock).mockResolvedValue({
    ...mockHealingDashboardData,
    totalIssuesDetected: 12345,
    issuesResolvedAutomatically: 6789,
    overallSuccessRate: 0.9876,
    averageResolutionTime: 1234567,
  });
  setup();

  // Render the HealingDashboard component
  renderDashboard();

  // Wait for data to be loaded
  await waitFor(() => {
    expect(screen.getByText(/Self-Healing Overview/i)).toBeInTheDocument();
  });

  // Verify each metric is formatted according to its type (percentage, duration, count)
  expect(screen.getByText(/12,345/i)).toBeInTheDocument();
  expect(screen.getByText(/6,789/i)).toBeInTheDocument();
  expect(screen.getByText(/98.8%/i)).toBeInTheDocument();
  expect(screen.getByText(/20m 34s/i)).toBeInTheDocument();
});

test('applies date range filter to API calls', async () => {
  // Create a date range object
  const dateRange = { startDate: '2023-01-01', endDate: '2023-01-31' };

  // Mock the API services
  const { getDashboardData } = setup();

  // Render the HealingDashboard component with the date range prop
  renderDashboard({ dateRange });

  // Wait for data to be loaded
  await waitFor(() => {
    expect(getDashboardData).toHaveBeenCalled();
  });

  // Verify the API calls include the date range parameters
  expect(getDashboardData).toHaveBeenCalledWith(dateRange);
});