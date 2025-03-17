import React from 'react'; // React library for component creation // version: ^18.2.0
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react'; // Testing utilities for rendering and interacting with components // version: ^13.4.0
import userEvent from '@testing-library/user-event'; // User event simulation for more realistic interaction testing // version: ^14.4.3
import { AlertDashboard } from '../AlertDashboard'; // Component being tested
import { renderWithProviders } from '../../../test/utils/renderWithProviders'; // Utility for rendering components with all required providers
import { createMockFn, createMockApiResponse } from '../../../test/utils/testUtils'; // Utilities for creating mock functions and API responses
import { alertService } from '../../../services/api/alertService'; // Service for fetching and managing alert data
import { useApi } from '../../../hooks/useApi'; // Hook for handling API requests with loading and error states
import { mockAlerts, mockAlertStats, mockRelatedAlerts, mockSuggestedActions, mockNotificationChannels } from '../../../test/mocks/data'; // Mock data for testing
import { AlertSeverity } from '../../../types/alerts'; // Type definitions for alert data structures

// Mock the alertService module
jest.mock('../../../services/api/alertService');

// Mock the useApi hook
jest.mock('../../../hooks/useApi');

describe('AlertDashboard', () => {
  // Set up mocks for alertService and useApi
  const mockGetAlerts = jest.fn().mockResolvedValue({ data: mockAlerts, total: mockAlerts.length, page: 1, pageSize: 10 });
  const mockGetAlertById = jest.fn().mockResolvedValue(mockAlerts[0]);
  const mockGetAlertStats = jest.fn().mockResolvedValue(mockAlertStats);
  const mockGetRelatedAlerts = jest.fn().mockResolvedValue(mockRelatedAlerts);
  const mockGetSuggestedActions = jest.fn().mockResolvedValue({ actions: mockSuggestedActions });
  const mockGetNotificationChannels = jest.fn().mockResolvedValue(mockNotificationChannels);
  const mockAcknowledgeAlert = jest.fn().mockResolvedValue(mockAlerts[0]);
  const mockResolveAlert = jest.fn().mockResolvedValue(mockAlerts[0]);
  const mockEscalateAlert = jest.fn().mockResolvedValue(mockAlerts[0]);
  const mockSuppressSimilarAlerts = jest.fn().mockResolvedValue({ success: true, message: 'Alerts suppressed' });
  const mockUseApi = jest.fn().mockReturnValue({ loading: false, error: null });

  beforeEach(() => {
    // Reset all mocks
    (alertService.getAlerts as jest.Mock) = mockGetAlerts;
    (alertService.getAlertById as jest.Mock) = mockGetAlertById;
    (alertService.getAlertStats as jest.Mock) = mockGetAlertStats;
    (alertService.getRelatedAlerts as jest.Mock) = mockGetRelatedAlerts;
    (alertService.getSuggestedActions as jest.Mock) = mockGetSuggestedActions;
    (alertService.getNotificationChannels as jest.Mock) = mockGetNotificationChannels;
    (alertService.acknowledgeAlert as jest.Mock) = mockAcknowledgeAlert;
    (alertService.resolveAlert as jest.Mock) = mockResolveAlert;
    (alertService.escalateAlert as jest.Mock) = mockEscalateAlert;
    (alertService.suppressSimilarAlerts as jest.Mock) = mockSuppressSimilarAlerts;
    (useApi as jest.Mock) = mockUseApi;
  });

  it('should render the component correctly', async () => {
    // Render the AlertDashboard component with renderWithProviders
    renderWithProviders(<AlertDashboard />);

    // Verify that key elements are present in the document
    expect(screen.getByText('Alert Statistics')).toBeInTheDocument();
    expect(screen.getByText('Alert Trends')).toBeInTheDocument();

    // Check that ActiveAlertsTable is rendered
    expect(screen.getByRole('table')).toBeInTheDocument();

    // Check that AlertStatsCard is rendered
    expect(screen.getByText('Total Alerts:')).toBeInTheDocument();

    // Check that AlertTrendChart is rendered
    expect(screen.getByLabelText('Line chart')).toBeInTheDocument();

    // Check that NotificationChannelsCard is rendered
    expect(screen.getByText('Microsoft Teams')).toBeInTheDocument();
  });

  it('should display alert details when an alert is selected', async () => {
    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Simulate selecting an alert from the ActiveAlertsTable
    const table = screen.getByRole('table');
    const firstRow = table.querySelector('tbody tr');
    fireEvent.click(firstRow as Element);

    // Verify that getAlertById, getRelatedAlerts, and getSuggestedActions were called
    await waitFor(() => {
      expect(alertService.getAlertById).toHaveBeenCalled();
      expect(alertService.getRelatedAlerts).toHaveBeenCalled();
      expect(alertService.getSuggestedActions).toHaveBeenCalled();
    });

    // Wait for AlertDetailsCard to be rendered
    await waitFor(() => {
      expect(screen.getByText('Alert Details')).toBeInTheDocument();
    });

    // Check that RelatedAlertsCard is rendered
    expect(screen.getByText('Related Alerts')).toBeInTheDocument();

    // Check that SuggestedActionsCard is rendered
    expect(screen.getByText('Suggested Actions')).toBeInTheDocument();
  });

  it('should update time range when changed', async () => {
    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Find the time range selector
    const timeRangeSelect = screen.getByLabelText('Time Range');

    // Simulate changing the time range
    fireEvent.change(timeRangeSelect, { target: { value: '7d' } });

    // Verify that getAlertStats was called with the new time range
    await waitFor(() => {
      expect(alertService.getAlertStats).toHaveBeenCalledWith('7d');
    });
  });

  it('should acknowledge an alert when acknowledge button is clicked', async () => {
    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Simulate selecting an alert
    const table = screen.getByRole('table');
    const firstRow = table.querySelector('tbody tr');
    fireEvent.click(firstRow as Element);

    // Wait for AlertDetailsCard to be rendered
    await waitFor(() => {
      expect(screen.getByText('Alert Details')).toBeInTheDocument();
    });

    // Find the acknowledge button
    const acknowledgeButton = await screen.findByText('Acknowledge');

    // Simulate clicking the acknowledge button
    fireEvent.click(acknowledgeButton);

    // Verify that acknowledgeAlert was called with correct parameters
    await waitFor(() => {
      expect(alertService.acknowledgeAlert).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        'Current User',
        undefined
      );
    });

    // Verify that feedback notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert acknowledged')).toBeInTheDocument();
    });
  });

  it('should resolve an alert when resolve button is clicked', async () => {
    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Simulate selecting an alert
    const table = screen.getByRole('table');
    const firstRow = table.querySelector('tbody tr');
    fireEvent.click(firstRow as Element);

    // Wait for AlertDetailsCard to be rendered
    await waitFor(() => {
      expect(screen.getByText('Alert Details')).toBeInTheDocument();
    });

    // Find the resolve button
    const resolveButton = await screen.findByText('Resolve');

    // Simulate clicking the resolve button
    fireEvent.click(resolveButton);

    // Verify that resolveAlert was called with correct parameters
    await waitFor(() => {
      expect(alertService.resolveAlert).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        'Current User',
        ''
      );
    });

    // Verify that feedback notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert resolved')).toBeInTheDocument();
    });
  });

  it('should escalate an alert when escalate button is clicked', async () => {
    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Simulate selecting an alert
    const table = screen.getByRole('table');
    const firstRow = table.querySelector('tbody tr');
    fireEvent.click(firstRow as Element);

    // Wait for AlertDetailsCard to be rendered
    await waitFor(() => {
      expect(screen.getByText('Alert Details')).toBeInTheDocument();
    });

    // Find the escalate button
    const escalateButton = await screen.findByText('Escalate');

    // Simulate clicking the escalate button
    fireEvent.click(escalateButton);

    // Verify that escalateAlert was called with correct parameters
    await waitFor(() => {
      expect(alertService.escalateAlert).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        'Current User',
        '',
        ''
      );
    });

    // Verify that feedback notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert escalated')).toBeInTheDocument();
    });
  });

  it('should display loading indicators when data is being fetched', () => {
    // Mock useApi to return loading: true
    (useApi as jest.Mock).mockReturnValue({ loading: true, error: null });

    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Verify that loading indicators are displayed
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
  });

  it('should handle errors when API requests fail', () => {
    // Mock useApi to return error: { message: 'API Error' }
    (useApi as jest.Mock).mockReturnValue({ loading: false, error: { message: 'API Error' } });

    // Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // Verify that error messages are displayed
    expect(screen.getByText('API Error')).toBeInTheDocument();
  });
});