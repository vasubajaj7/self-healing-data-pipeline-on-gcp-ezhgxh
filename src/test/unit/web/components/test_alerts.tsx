import React from 'react'; // React library for JSX support // version: ^18.2.0
import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals'; // Jest testing functions and assertions // version: ^29.5.0
import { render, screen, waitFor, within, fireEvent } from '@testing-library/react'; // React Testing Library for component testing // version: ^13.4.0
import userEvent from '@testing-library/user-event'; // User event simulation for testing interactions // version: ^14.4.3
import AlertDashboard from '../../../../web/src/components/alert/AlertDashboard'; // Component under test - main alert dashboard
import ActiveAlertsTable from '../../../../web/src/components/alert/ActiveAlertsTable'; // Component under test - table of active alerts
import AlertDetailsCard from '../../../../web/src/components/alert/AlertDetailsCard'; // Component under test - detailed alert information card
import { Alert, AlertType, AlertStatus, AlertSeverity } from '../../../../web/src/types/alerts'; // Type definitions for alert data structures
import { alertService } from '../../../../web/src/services/api/alertService'; // Service to be mocked for testing alert API interactions
import { renderComponent, createUserEvent, fillFormFields, selectDropdownOption, waitForLoadingToComplete, findTableRowByText } from '../../../utils/web_test_utils'; // Testing utilities for rendering components and simulating user interactions

jest.mock('../../../../web/src/services/api/alertService', () => ({
  getAlerts: jest.fn(),
  getAlertById: jest.fn(),
  acknowledgeAlert: jest.fn(),
  resolveAlert: jest.fn(),
  escalateAlert: jest.fn(),
  suppressSimilarAlerts: jest.fn(),
  getRelatedAlerts: jest.fn(),
  getSuggestedActions: jest.fn(),
  getNotificationChannels: jest.fn(),
  getAlertStats: jest.fn()
}));

interface MockAlertOverrides {
  alertId?: string;
  executionId?: string;
  alertType?: AlertType;
  severity?: AlertSeverity;
  status?: AlertStatus;
  message?: string;
  source?: string;
  component?: string;
  createdAt?: string;
  acknowledged?: boolean;
  acknowledgedBy?: string;
  acknowledgedAt?: string;
  relatedAlerts?: string[];
  selfHealingStatus?: string;
}

const createMockAlert = (overrides: MockAlertOverrides = {}): Alert => {
  const defaultAlert: Alert = {
    alertId: overrides.alertId || 'alert-123',
    executionId: overrides.executionId || 'exec-456',
    alertType: overrides.alertType || AlertType.DATA_QUALITY,
    severity: overrides.severity || AlertSeverity.HIGH,
    status: overrides.status || AlertStatus.ACTIVE,
    message: overrides.message || 'Data quality issue detected',
    source: overrides.source || 'Source System A',
    component: overrides.component || 'Transformation Job',
    createdAt: overrides.createdAt || '2024-01-01T00:00:00.000Z',
    acknowledged: overrides.acknowledged || false,
    acknowledgedBy: overrides.acknowledgedBy || null,
    acknowledgedAt: overrides.acknowledgedAt || null,
    relatedAlerts: overrides.relatedAlerts || [],
    selfHealingStatus: overrides.selfHealingStatus || null,
  };

  return { ...defaultAlert, ...overrides };
};

const createMockAlertList = (count: number): Alert[] => {
  const alerts: Alert[] = [];
  for (let i = 1; i <= count; i++) {
    alerts.push(createMockAlert({
      alertId: `alert-${i}`,
      message: `Alert ${i} - Data quality issue detected`,
      severity: i % 2 === 0 ? AlertSeverity.MEDIUM : AlertSeverity.HIGH,
      status: i % 3 === 0 ? AlertStatus.RESOLVED : AlertStatus.ACTIVE,
    }));
  }
  return alerts;
};

describe('ActiveAlertsTable', () => {
  it('renders the table with alert data', async () => {
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: createMockAlertList(5),
      pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    await waitForLoadingToComplete();

    const tableRows = screen.getAllByRole('row');
    expect(tableRows).toHaveLength(6);

    expect(screen.getByText('Alert 1 - Data quality issue detected')).toBeInTheDocument();
  });

  it('handles search functionality', async () => {
    (alertService.getAlerts as jest.Mock).mockImplementation((filter: any) => {
      const searchTerm = filter?.search || '';
      const filteredAlerts = createMockAlertList(5).filter(alert =>
        alert.message.toLowerCase().includes(searchTerm.toLowerCase())
      );
      return Promise.resolve({
        items: filteredAlerts,
        pagination: { page: 1, pageSize: 10, totalItems: filteredAlerts.length, totalPages: 1 },
      });
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    const searchInput = screen.getByPlaceholderText('Search alerts...');
    await userEvent.type(searchInput, 'Alert 2');

    await waitFor(() => {
      expect(alertService.getAlerts).toHaveBeenCalledWith(
        expect.objectContaining({ search: 'Alert 2' }),
        1,
        10
      );
    });

    expect(screen.getByText('Alert 2 - Data quality issue detected')).toBeInTheDocument();
  });

  it('handles filter functionality', async () => {
    (alertService.getAlerts as jest.Mock).mockImplementation((filter: any) => {
      const filteredAlerts = createMockAlertList(5).filter(alert => {
        if (filter.severity && !filter.severity.includes(alert.severity)) return false;
        if (filter.status && !filter.status.includes(alert.status)) return false;
        if (filter.type && !filter.type.includes(alert.alertType)) return false;
        return true;
      });
      return Promise.resolve({
        items: filteredAlerts,
        pagination: { page: 1, pageSize: 10, totalItems: filteredAlerts.length, totalPages: 1 },
      });
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    const filterButton = screen.getByLabelText('Filter alerts');
    await userEvent.click(filterButton);

    const severityCheckbox = screen.getByRole('checkbox', { name: AlertSeverity.HIGH });
    await userEvent.click(severityCheckbox);

    const statusCheckbox = screen.getByRole('checkbox', { name: AlertStatus.ACTIVE });
    await userEvent.click(statusCheckbox);

    const typeCheckbox = screen.getByRole('checkbox', { name: AlertType.DATA_QUALITY });
    await userEvent.click(typeCheckbox);

    await waitFor(() => {
      expect(alertService.getAlerts).toHaveBeenCalledWith(
        expect.objectContaining({
          severity: [AlertSeverity.HIGH],
          status: [AlertStatus.ACTIVE],
          type: [AlertType.DATA_QUALITY]
        }),
        1,
        10
      );
    });
  });

  it('handles pagination', async () => {
    (alertService.getAlerts as jest.Mock).mockImplementation((filter: any, page: number) => {
      const pageSize = 2;
      const startIndex = (page - 1) * pageSize;
      const paginatedAlerts = createMockAlertList(5).slice(startIndex, startIndex + pageSize);
      return Promise.resolve({
        items: paginatedAlerts,
        pagination: { page, pageSize, totalItems: 5, totalPages: 3 },
      });
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    await waitForLoadingToComplete();

    const nextPageButton = screen.getByRole('button', { name: 'Go to next page' });
    await userEvent.click(nextPageButton);

    await waitFor(() => {
      expect(alertService.getAlerts).toHaveBeenCalledWith(
        expect.anything(),
        2,
        10
      );
    });
  });

  it('handles row selection', async () => {
    const onAlertSelect = jest.fn();
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: createMockAlertList(1),
      pagination: { page: 1, pageSize: 10, totalItems: 1, totalPages: 1 },
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={onAlertSelect} />);

    await waitForLoadingToComplete();

    const tableRow = screen.getByRole('row');
    await userEvent.click(tableRow);

    expect(onAlertSelect).toHaveBeenCalledWith(expect.objectContaining({ alertId: 'alert-1' }));
  });

  it('handles refresh button click', async () => {
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: createMockAlertList(5),
      pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
    });

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    await waitForLoadingToComplete();

    const refreshButton = screen.getByLabelText('Refresh alerts');
    await userEvent.click(refreshButton);

    expect(alertService.getAlerts).toHaveBeenCalledTimes(2);
  });

  it('displays loading state', async () => {
    (alertService.getAlerts as jest.Mock).mockReturnValue(new Promise(resolve => {
      setTimeout(() => {
        resolve({
          items: createMockAlertList(5),
          pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
        });
      }, 100);
    }));

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitForLoadingToComplete();
  });

  it('handles error state', async () => {
    (alertService.getAlerts as jest.Mock).mockRejectedValue(new Error('Failed to fetch'));

    renderComponent(<ActiveAlertsTable onAlertSelect={() => { }} />);

    await waitFor(() => {
      expect(screen.getByText('Failed to fetch')).toBeInTheDocument();
    });

    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });
});

describe('AlertDetailsCard', () => {
  it('renders alert details correctly', async () => {
    const mockAlert = createMockAlert({
      message: 'Test Alert Message',
      source: 'Test Source',
      component: 'Test Component',
      createdAt: '2024-01-01T12:00:00.000Z',
    });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={() => { }} onEscalate={() => { }} onSuppress={() => { }} />);

    expect(screen.getByText('Test Alert Message')).toBeInTheDocument();
    expect(screen.getByText('Test Source')).toBeInTheDocument();
  });

  it('renders appropriate action buttons based on alert status', async () => {
    const renderWithStatus = (status: AlertStatus) => {
      const mockAlert = createMockAlert({ status });
      return renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={() => { }} onEscalate={() => { }} onSuppress={() => { }} />);
    };

    renderWithStatus(AlertStatus.ACTIVE);
    expect(screen.getByText('Acknowledge')).toBeInTheDocument();
    expect(screen.getByText('Escalate')).toBeInTheDocument();
    expect(screen.getByText('Suppress Similar')).toBeInTheDocument();

    renderWithStatus(AlertStatus.ACKNOWLEDGED);
    expect(screen.getByText('Resolve')).toBeInTheDocument();
    expect(screen.getByText('Escalate')).toBeInTheDocument();
    expect(screen.getByText('Suppress Similar')).toBeInTheDocument();

    renderWithStatus(AlertStatus.ESCALATED);
    expect(screen.getByText('Resolve')).toBeInTheDocument();
    expect(screen.getByText('Suppress Similar')).toBeInTheDocument();
  });

  it('handles acknowledge action', async () => {
    (alertService.acknowledgeAlert as jest.Mock).mockResolvedValue(createMockAlert({ status: AlertStatus.ACKNOWLEDGED }));
    const onAcknowledge = jest.fn();
    const mockAlert = createMockAlert({ status: AlertStatus.ACTIVE });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={onAcknowledge} onResolve={() => { }} onEscalate={() => { }} onSuppress={() => { }} />);

    const acknowledgeButton = screen.getByText('Acknowledge');
    await userEvent.click(acknowledgeButton);

    const commentsInput = screen.getByRole('textbox', { name: 'Comments (Optional)' });
    await userEvent.type(commentsInput, 'Test comments');

    const confirmButton = screen.getByRole('button', { name: 'Acknowledge' });
    await userEvent.click(confirmButton);

    expect(alertService.acknowledgeAlert).toHaveBeenCalledWith(mockAlert.alertId, 'Current User', 'Test comments');
    expect(onAcknowledge).toHaveBeenCalledWith('acknowledge', true);
  });

  it('handles resolve action', async () => {
    (alertService.resolveAlert as jest.Mock).mockResolvedValue(createMockAlert({ status: AlertStatus.RESOLVED }));
    const onResolve = jest.fn();
    const mockAlert = createMockAlert({ status: AlertStatus.ACKNOWLEDGED });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={onResolve} onEscalate={() => { }} onSuppress={() => { }} />);

    const resolveButton = screen.getByText('Resolve');
    await userEvent.click(resolveButton);

    const resolutionNotesInput = screen.getByRole('textbox', { name: 'Resolution Notes' });
    await userEvent.type(resolutionNotesInput, 'Test resolution notes');

    const confirmButton = screen.getByRole('button', { name: 'Resolve' });
    await userEvent.click(confirmButton);

    expect(alertService.resolveAlert).toHaveBeenCalledWith(mockAlert.alertId, 'Current User', 'Test resolution notes');
    expect(onResolve).toHaveBeenCalledWith('resolve', true);
  });

  it('handles escalate action', async () => {
    (alertService.escalateAlert as jest.Mock).mockResolvedValue(createMockAlert({ status: AlertStatus.ESCALATED }));
    const onEscalate = jest.fn();
    const mockAlert = createMockAlert({ status: AlertStatus.ACTIVE });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={() => { }} onEscalate={onEscalate} onSuppress={() => { }} />);

    const escalateButton = screen.getByText('Escalate');
    await userEvent.click(escalateButton);

    const escalationReasonInput = screen.getByRole('textbox', { name: 'Escalation Reason' });
    await userEvent.type(escalationReasonInput, 'Test escalation reason');

    const escalationLevelSelect = screen.getByRole('textbox', { name: 'Escalation Level' });
    await userEvent.click(escalationLevelSelect);
    const option = screen.getByText('Team Lead');
    await userEvent.click(option);

    const confirmButton = screen.getByRole('button', { name: 'Escalate' });
    await userEvent.click(confirmButton);

    expect(alertService.escalateAlert).toHaveBeenCalledWith(mockAlert.alertId, 'Current User', 'Test escalation reason', 'Team Lead');
    expect(onEscalate).toHaveBeenCalledWith('escalate', true);
  });

  it('handles suppress action', async () => {
    (alertService.suppressSimilarAlerts as jest.Mock).mockResolvedValue({ success: true, message: 'Suppressed' });
    const onSuppress = jest.fn();
    const mockAlert = createMockAlert({ status: AlertStatus.ACTIVE });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={() => { }} onEscalate={() => { }} onSuppress={onSuppress} />);

    const suppressButton = screen.getByText('Suppress Similar');
    await userEvent.click(suppressButton);

    const suppressionReasonInput = screen.getByRole('textbox', { name: 'Suppression Reason' });
    await userEvent.type(suppressionReasonInput, 'Test suppression reason');

    const durationMinutesInput = screen.getByRole('textbox', { name: 'Duration (minutes)' });
    await userEvent.clear(durationMinutesInput);
    await userEvent.type(durationMinutesInput, '120');

    const confirmButton = screen.getByRole('button', { name: 'Suppress' });
    await userEvent.click(confirmButton);

    expect(alertService.suppressSimilarAlerts).toHaveBeenCalledWith(mockAlert.alertId, 'Current User', 120, 'Test suppression reason');
    expect(onSuppress).toHaveBeenCalledWith('suppress', true);
  });

  it('handles action errors', async () => {
    (alertService.acknowledgeAlert as jest.Mock).mockRejectedValue(new Error('Acknowledge failed'));
    const onAcknowledge = jest.fn();
    const mockAlert = createMockAlert({ status: AlertStatus.ACTIVE });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={onAcknowledge} onResolve={() => { }} onEscalate={() => { }} onSuppress={() => { }} />);

    const acknowledgeButton = screen.getByText('Acknowledge');
    await userEvent.click(acknowledgeButton);

    const commentsInput = screen.getByRole('textbox', { name: 'Comments (Optional)' });
    await userEvent.type(commentsInput, 'Test comments');

    const confirmButton = screen.getByRole('button', { name: 'Acknowledge' });
    await userEvent.click(confirmButton);

    await waitFor(() => {
      expect(screen.getByText('Acknowledge failed')).toBeInTheDocument();
    });
    expect(onAcknowledge).toHaveBeenCalledWith('acknowledge', false);
  });

  it('displays loading state during actions', async () => {
    (alertService.acknowledgeAlert as jest.Mock).mockReturnValue(new Promise(resolve => {
      setTimeout(() => {
        resolve(createMockAlert({ status: AlertStatus.ACKNOWLEDGED }));
      }, 100);
    }));

    const mockAlert = createMockAlert({ status: AlertStatus.ACTIVE });

    renderComponent(<AlertDetailsCard alert={mockAlert} onAcknowledge={() => { }} onResolve={() => { }} onEscalate={() => { }} onSuppress={() => { }} />);

    const acknowledgeButton = screen.getByText('Acknowledge');
    await userEvent.click(acknowledgeButton);

    const commentsInput = screen.getByRole('textbox', { name: 'Comments (Optional)' });
    await userEvent.type(commentsInput, 'Test comments');

    const confirmButton = screen.getByRole('button', { name: 'Acknowledge' });
    await userEvent.click(confirmButton);

    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitForLoadingToComplete();
  });
});

describe('AlertDashboard', () => {
  it('renders all components correctly', async () => {
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: createMockAlertList(5),
      pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
    });
    (alertService.getAlertStats as jest.Mock).mockResolvedValue({
      critical: 1, high: 2, medium: 1, low: 1, total: 5, trend: []
    });
    (alertService.getNotificationChannels as jest.Mock).mockResolvedValue({
      teams: true, email: true, sms: false, configured: ['teams', 'email']
    });

    renderComponent(<AlertDashboard />);

    await waitForLoadingToComplete();

    expect(screen.getByText('Alert Statistics')).toBeInTheDocument();
    expect(screen.getByText('Active Alerts')).toBeInTheDocument();
  });

  it('loads notification channels on mount', async () => {
    (alertService.getNotificationChannels as jest.Mock).mockResolvedValue({
      teams: true, email: true, sms: false, configured: ['teams', 'email']
    });

    renderComponent(<AlertDashboard />);

    await waitFor(() => {
      expect(alertService.getNotificationChannels).toHaveBeenCalled();
    });
  });

  it('handles alert selection', async () => {
    const mockAlert = createMockAlert();
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: [mockAlert],
      pagination: { page: 1, pageSize: 10, totalItems: 1, totalPages: 1 },
    });
    (alertService.getAlertById as jest.Mock).mockResolvedValue(mockAlert);
    (alertService.getRelatedAlerts as jest.Mock).mockResolvedValue([]);
    (alertService.getSuggestedActions as jest.Mock).mockResolvedValue({ actions: [] });
    (alertService.getAlertStats as jest.Mock).mockResolvedValue({
      critical: 1, high: 2, medium: 1, low: 1, total: 5, trend: []
    });
    (alertService.getNotificationChannels as jest.Mock).mockResolvedValue({
      teams: true, email: true, sms: false, configured: ['teams', 'email']
    });

    renderComponent(<AlertDashboard />);

    await waitForLoadingToComplete();

    const tableRow = screen.getByRole('row');
    await userEvent.click(tableRow);

    expect(alertService.getAlertById).toHaveBeenCalledWith(mockAlert.alertId);
    expect(alertService.getRelatedAlerts).toHaveBeenCalledWith(mockAlert.alertId);
    expect(alertService.getSuggestedActions).toHaveBeenCalledWith(mockAlert.alertId);
    expect(screen.getByText('Alert Details')).toBeInTheDocument();
  });

  it('handles time range changes', async () => {
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: createMockAlertList(5),
      pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
    });
    (alertService.getAlertStats as jest.Mock).mockResolvedValue({
      critical: 1, high: 2, medium: 1, low: 1, total: 5, trend: []
    });
    (alertService.getNotificationChannels as jest.Mock).mockResolvedValue({
      teams: true, email: true, sms: false, configured: ['teams', 'email']
    });

    renderComponent(<AlertDashboard />);

    await waitForLoadingToComplete();

    const timeRangeSelect = screen.getByLabelText('Time Range');
    await userEvent.click(timeRangeSelect);
    const option = screen.getByText('Last 7 Days');
    await userEvent.click(option);

    expect(alertService.getAlertStats).toHaveBeenCalledWith('7d');
  });

  it('handles alert actions', async () => {
    const mockAlert = createMockAlert();
    (alertService.getAlerts as jest.Mock).mockResolvedValue({
      items: [mockAlert],
      pagination: { page: 1, pageSize: 10, totalItems: 1, totalPages: 1 },
    });
    (alertService.getAlertById as jest.Mock).mockResolvedValue(mockAlert);
    (alertService.getRelatedAlerts as jest.Mock).mockResolvedValue([]);
    (alertService.getSuggestedActions as jest.Mock).mockResolvedValue({ actions: [] });
    (alertService.getAlertStats as jest.Mock).mockResolvedValue({
      critical: 1, high: 2, medium: 1, low: 1, total: 5, trend: []
    });
    (alertService.getNotificationChannels as jest.Mock).mockResolvedValue({
      teams: true, email: true, sms: false, configured: ['teams', 'email']
    });
    (alertService.acknowledgeAlert as jest.Mock).mockResolvedValue(mockAlert);

    renderComponent(<AlertDashboard />);

    await waitForLoadingToComplete();

    const tableRow = screen.getByRole('row');
    await userEvent.click(tableRow);

    const acknowledgeButton = await screen.findByText('Acknowledge');
    await userEvent.click(acknowledgeButton);

    const commentsInput = screen.getByRole('textbox', { name: 'Comments (Optional)' });
    await userEvent.type(commentsInput, 'Test comments');

    const confirmButton = screen.getByRole('button', { name: 'Acknowledge' });
    await userEvent.click(confirmButton);

    expect(alertService.acknowledgeAlert).toHaveBeenCalledWith(mockAlert.alertId, 'Current User', 'Test comments');
  });

  it('displays loading states', async () => {
    (alertService.getAlerts as jest.Mock).mockReturnValue(new Promise(resolve => {
      setTimeout(() => {
        resolve({
          items: createMockAlertList(5),
          pagination: { page: 1, pageSize: 10, totalItems: 5, totalPages: 1 },
        });
      }, 100);
    }));
    (alertService.getAlertStats as jest.Mock).mockReturnValue(new Promise(resolve => {
      setTimeout(() => {
        resolve({
          critical: 1, high: 2, medium: 1, low: 1, total: 5, trend: []
        });
      }, 100);
    }));
    (alertService.getNotificationChannels as jest.Mock).mockReturnValue(new Promise(resolve => {
      setTimeout(() => {
        resolve({
          teams: true, email: true, sms: false, configured: ['teams', 'email']
        });
      }, 100);
    }));

    renderComponent(<AlertDashboard />);

    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitForLoadingToComplete();
  });

  it('handles error states', async () => {
    (alertService.getAlerts as jest.Mock).mockRejectedValue(new Error('Failed to fetch'));
    (alertService.getAlertStats as jest.Mock).mockRejectedValue(new Error('Failed to fetch'));
    (alertService.getNotificationChannels as jest.Mock).mockRejectedValue(new Error('Failed to fetch'));

    renderComponent(<AlertDashboard />);

    await waitFor(() => {
      expect(screen.getByText('Failed to fetch')).toBeInTheDocument();
    });
  });
});