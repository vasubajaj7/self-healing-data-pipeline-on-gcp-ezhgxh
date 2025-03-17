import React from 'react'; // React library for component testing // version: ^18.2.0
import { screen, waitFor, fireEvent, within } from '@testing-library/react'; // Testing utilities for interacting with rendered components // version: ^13.4.0
import { rest } from 'msw'; // Mock Service Worker REST API mocking // version: ^1.2.1
import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals'; // Jest testing framework functions // version: ^29.5.0
import { renderWithProviders } from '../../web/src/test/utils/renderWithProviders'; // Utility for rendering components with all necessary providers for testing
import AlertDashboard from '../../web/src/components/alert/AlertDashboard'; // Main component for the alert management dashboard
import { alertService } from '../../web/src/services/api/alertService'; // Service for interacting with the alert management API
import { server } from '../../web/src/test/mocks/server'; // Mock server for intercepting API requests during tests
import { mockAlerts, mockAlertStatistics, generateMockDataResponse } from '../../web/src/test/mocks/data'; // Mock data for alert testing
import { Alert, AlertStatus, AlertType } from '../../web/src/types/alerts'; // Type definitions for alert data structures
import { endpoints } from '../../web/src/config/apiConfig'; // API endpoint definitions for alert management

/**
 * Sets up custom mock handlers for alert API endpoints
 */
const setupMockHandlers = () => {
  // LD1: Create mock handlers for alert API endpoints
  const customHandlers = [
    // LD1: Set up handler for getAlerts endpoint
    rest.get(endpoints.monitoring.alerts, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(generateMockDataResponse(mockAlerts)));
    }),
    // LD1: Set up handler for getAlertById endpoint
    rest.get(endpoints.monitoring.alertDetails(':id'), (req, res, ctx) => {
      const { id } = req.params;
      const alert = mockAlerts.find((a) => a.alertId === id);
      return res(ctx.status(200), ctx.json(generateMockDataResponse(alert)));
    }),
    // LD1: Set up handler for acknowledgeAlert endpoint
    rest.put(`/api/v1/monitoring/alerts/:id/acknowledge`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(generateMockDataResponse({ ...mockAlerts[0], status: AlertStatus.ACKNOWLEDGED })));
    }),
    // LD1: Set up handler for resolveAlert endpoint
    rest.put(`/api/v1/monitoring/alerts/:id/resolve`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(generateMockDataResponse({ ...mockAlerts[0], status: AlertStatus.RESOLVED })));
    }),
    // LD1: Set up handler for escalateAlert endpoint
    rest.put(`/api/v1/monitoring/alerts/:id/escalate`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(generateMockDataResponse({ ...mockAlerts[0], status: AlertStatus.ESCALATED })));
    }),
    // LD1: Set up handler for suppressSimilarAlerts endpoint
    rest.post(`/api/v1/monitoring/alerts/:id/suppress-similar`, (req, res, ctx) => {
      return res(ctx.status(200), ctx.json(generateMockDataResponse({ success: true, message: 'Similar alerts suppressed' })));
    }),
  ];

  // LD1: Apply the custom handlers to the mock server
  server.use(...customHandlers);
};

/**
 * Alert Dashboard Integration Tests
 * Tests the integration of the alert dashboard with alert services and API
 */
describe('Alert Dashboard Integration Tests', () => {
  // LD1: Before each test, reset mock server handlers and set up custom mock handlers
  beforeEach(() => {
    server.resetHandlers();
    setupMockHandlers();
  });

  // LD1: After each test, reset mock server handlers to default
  afterEach(() => {
    server.resetHandlers();
  });

  // LD1: Test case: renders the alert dashboard with active alerts
  it('renders the alert dashboard with active alerts', async () => {
    // LD1: Render the AlertDashboard component with renderWithProviders
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      // LD1: Verify that the active alerts table is displayed
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Verify that the correct number of alerts are shown
    const alertRows = await screen.findAllByTestId('alert-row');
    expect(alertRows).toHaveLength(mockAlerts.length);

    // LD1: Verify that alert statistics are displayed correctly
    expect(screen.getByText(`Total Alerts: ${mockAlerts.length}`)).toBeInTheDocument();
  });

  // LD1: Test case: displays alert details when an alert is selected
  it('displays alert details when an alert is selected', async () => {
    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Click on an alert in the active alerts table
    const alertRow = await screen.findByTestId(`alert-row-${mockAlerts[0].alertId}`);
    fireEvent.click(alertRow);

    // LD1: Verify that the alert details card is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert Details')).toBeInTheDocument();
    });

    // LD1: Verify that the alert details show the correct information
    expect(screen.getByText(mockAlerts[0].message)).toBeInTheDocument();

    // LD1: Verify that related alerts are displayed
    expect(screen.getByText('Related Alerts')).toBeInTheDocument();

    // LD1: Verify that suggested actions are displayed
    expect(screen.getByText('Suggested Actions')).toBeInTheDocument();
  });

  // LD1: Test case: allows acknowledging an alert
  it('allows acknowledging an alert', async () => {
    // LD1: Mock the acknowledgeAlert service function
    const acknowledgeAlertMock = jest.spyOn(alertService, 'acknowledgeAlert');

    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Click on an alert in the active alerts table
    const alertRow = await screen.findByTestId(`alert-row-${mockAlerts[0].alertId}`);
    fireEvent.click(alertRow);

    // LD1: Click the acknowledge button in the alert details
    const acknowledgeButton = await screen.findByRole('button', { name: 'Acknowledge' });
    fireEvent.click(acknowledgeButton);

    // LD1: Verify that the acknowledgeAlert service was called with correct parameters
    await waitFor(() => {
      expect(acknowledgeAlertMock).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        expect.any(String),
        expect.any(String)
      );
    });

    // LD1: Verify that a success notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert acknowledged')).toBeInTheDocument();
    });

    // LD1: Verify that the alert status is updated to ACKNOWLEDGED
    await waitFor(() => {
      expect(screen.getByText(AlertStatus.ACKNOWLEDGED)).toBeInTheDocument();
    });
  });

  // LD1: Test case: allows resolving an alert
  it('allows resolving an alert', async () => {
    // LD1: Mock the resolveAlert service function
    const resolveAlertMock = jest.spyOn(alertService, 'resolveAlert');

    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Click on an alert in the active alerts table
    const alertRow = await screen.findByTestId(`alert-row-${mockAlerts[0].alertId}`);
    fireEvent.click(alertRow);

    // LD1: Click the resolve button in the alert details
    const resolveButton = await screen.findByRole('button', { name: 'Resolve' });
    fireEvent.click(resolveButton);

    // LD1: Enter resolution notes in the dialog
    const notesInput = screen.getByLabelText('Resolution Notes');
    fireEvent.change(notesInput, { target: { value: 'Test resolution notes' } });

    // LD1: Click the confirm button
    const confirmButton = screen.getByRole('button', { name: 'Resolve' });
    fireEvent.click(confirmButton);

    // LD1: Verify that the resolveAlert service was called with correct parameters
    await waitFor(() => {
      expect(resolveAlertMock).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        expect.any(String),
        'Test resolution notes'
      );
    });

    // LD1: Verify that a success notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert resolved')).toBeInTheDocument();
    });

    // LD1: Verify that the alert status is updated to RESOLVED
    await waitFor(() => {
      expect(screen.getByText(AlertStatus.RESOLVED)).toBeInTheDocument();
    });
  });

  // LD1: Test case: allows escalating an alert
  it('allows escalating an alert', async () => {
    // LD1: Mock the escalateAlert service function
    const escalateAlertMock = jest.spyOn(alertService, 'escalateAlert');

    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Click on an alert in the active alerts table
    const alertRow = await screen.findByTestId(`alert-row-${mockAlerts[0].alertId}`);
    fireEvent.click(alertRow);

    // LD1: Click the escalate button in the alert details
    const escalateButton = await screen.findByRole('button', { name: 'Escalate' });
    fireEvent.click(escalateButton);

    // LD1: Select an escalation level in the dialog
    const levelInput = screen.getByLabelText('Escalation Level');
    fireEvent.change(levelInput, { target: { value: 'Team Lead' } });

    // LD1: Enter escalation reason in the dialog
    const reasonInput = screen.getByLabelText('Escalation Reason');
    fireEvent.change(reasonInput, { target: { value: 'Test escalation reason' } });

    // LD1: Click the confirm button
    const confirmButton = screen.getByRole('button', { name: 'Escalate' });
    fireEvent.click(confirmButton);

    // LD1: Verify that the escalateAlert service was called with correct parameters
    await waitFor(() => {
      expect(escalateAlertMock).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        expect.any(String),
        'Test escalation reason',
        'Team Lead'
      );
    });

    // LD1: Verify that a success notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Alert escalated')).toBeInTheDocument();
    });

    // LD1: Verify that the alert status is updated to ESCALATED
    await waitFor(() => {
      expect(screen.getByText(AlertStatus.ESCALATED)).toBeInTheDocument();
    });
  });

  // LD1: Test case: allows suppressing similar alerts
  it('allows suppressing similar alerts', async () => {
    // LD1: Mock the suppressSimilarAlerts service function
    const suppressSimilarAlertsMock = jest.spyOn(alertService, 'suppressSimilarAlerts');

    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Click on an alert in the active alerts table
    const alertRow = await screen.findByTestId(`alert-row-${mockAlerts[0].alertId}`);
    fireEvent.click(alertRow);

    // LD1: Click the suppress similar button in the alert details
    const suppressButton = await screen.findByRole('button', { name: 'Suppress Similar' });
    fireEvent.click(suppressButton);

    // LD1: Enter suppression duration in the dialog
    const durationInput = screen.getByLabelText('Duration (minutes)');
    fireEvent.change(durationInput, { target: { value: '60' } });

    // LD1: Enter suppression reason in the dialog
    const reasonInput = screen.getByLabelText('Suppression Reason');
    fireEvent.change(reasonInput, { target: { value: 'Test suppression reason' } });

    // LD1: Click the confirm button
    const confirmButton = screen.getByRole('button', { name: 'Suppress' });
    fireEvent.click(confirmButton);

    // LD1: Verify that the suppressSimilarAlerts service was called with correct parameters
    await waitFor(() => {
      expect(suppressSimilarAlertsMock).toHaveBeenCalledWith(
        mockAlerts[0].alertId,
        expect.any(String),
        60,
        'Test suppression reason'
      );
    });

    // LD1: Verify that a success notification is displayed
    await waitFor(() => {
      expect(screen.getByText('Similar alerts suppressed')).toBeInTheDocument();
    });
  });

  // LD1: Test case: displays notification channel status correctly
  it('displays notification channel status correctly', async () => {
    // LD1: Set up mock handler for notification channels with specific status
    server.use(
      rest.get('/api/v1/monitoring/alerts/notification-channels', (req, res, ctx) => {
        return res(
          ctx.status(200),
          ctx.json(generateMockDataResponse({
            teams: true,
            email: false,
            sms: true,
            configured: ['TEAMS', 'SMS']
          }))
        );
      })
    );

    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Notification Channels')).toBeInTheDocument();
    });

    // LD1: Verify that the notification channels card is displayed
    const notificationChannelsCard = screen.getByText('Notification Channels');
    expect(notificationChannelsCard).toBeInTheDocument();

    // LD1: Verify that the correct channel statuses are shown
    expect(screen.getByText('Microsoft Teams')).toBeInTheDocument();
    expect(screen.getByText('Email')).toBeInTheDocument();
    expect(screen.getByText('SMS')).toBeInTheDocument();

    // LD1: Verify that configured channels are listed correctly
    expect(screen.getByText('Microsoft Teams')).toBeVisible();
    expect(screen.getByText('SMS')).toBeVisible();
  });

  // LD1: Test case: allows filtering alerts by severity, status, and type
  it('allows filtering alerts by severity, status, and type', async () => {
    // LD1: Render the AlertDashboard component
    renderWithProviders(<AlertDashboard />);

    // LD1: Wait for the component to load
    await waitFor(() => {
      expect(screen.getByText('Active Alerts')).toBeInTheDocument();
    });

    // LD1: Open the filter panel in the active alerts table
    const filterButton = screen.getByLabelText('Filter alerts');
    fireEvent.click(filterButton);

    // LD1: Select a severity filter
    const severityCheckbox = await screen.findByRole('checkbox', { name: AlertSeverity.HIGH });
    fireEvent.click(severityCheckbox);

    // LD1: Verify that the alerts are filtered by severity
    await waitFor(() => {
      expect(screen.getAllByText(AlertSeverity.HIGH)).toHaveLength(1);
    });

    // LD1: Select a status filter
    const statusCheckbox = await screen.findByRole('checkbox', { name: AlertStatus.ACTIVE });
    fireEvent.click(statusCheckbox);

    // LD1: Verify that the alerts are filtered by status
    await waitFor(() => {
      expect(screen.getAllByText(AlertStatus.ACTIVE)).toHaveLength(1);
    });

    // LD1: Select a type filter
    const typeCheckbox = await screen.findByRole('checkbox', { name: AlertType.PIPELINE_FAILURE });
    fireEvent.click(typeCheckbox);

    // LD1: Verify that the alerts are filtered by type
    await waitFor(() => {
      expect(screen.getAllByText(AlertType.PIPELINE_FAILURE)).toHaveLength(1);
    });

    // LD1: Clear all filters
    const clearButton = screen.getByRole('button', { name: 'Clear' });
    fireEvent.click(clearButton);

    // LD1: Verify that all alerts are shown again
    await waitFor(() => {
      expect(screen.getAllByTestId('alert-row')).toHaveLength(mockAlerts.length);
    });
  });
});