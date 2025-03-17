import React, { useState, useEffect, useCallback } from 'react'; // React library and hooks for component creation and state management // version: ^18.2.0
import {
  Grid,
  Box,
  Paper,
  Typography,
  Divider,
  useTheme,
  Snackbar,
  Alert as MuiAlert,
  CircularProgress,
} from '@mui/material'; // Material-UI components for layout and UI elements // version: ^5.11.0
import ActiveAlertsTable from './ActiveAlertsTable'; // Component for displaying and filtering active alerts
import AlertDetailsCard from './AlertDetailsCard'; // Component for displaying detailed information about a selected alert
import AlertStatsCard from './AlertStatsCard'; // Component for displaying alert statistics and counts by severity
import AlertTrendChart from './AlertTrendChart'; // Component for visualizing alert trends over time
import RelatedAlertsCard from './RelatedAlertsCard'; // Component for displaying alerts related to the selected alert
import SuggestedActionsCard from './SuggestedActionsCard'; // Component for displaying AI-suggested actions for alert resolution
import NotificationChannelsCard from './NotificationChannelsCard'; // Component for displaying notification channel status
import { Alert, AlertFilter } from '../../types/alerts'; // Type definitions for alert data structures
import alertService from '../../services/api/alertService'; // Service for fetching alert data from the API
import { useApi } from '../../hooks/useApi'; // Hook for handling API requests with loading and error states

/**
 * Props interface for the AlertDashboard component
 */
interface AlertDashboardProps {
  /** Additional CSS class for styling */
  className?: string;
  /** Initial time range for alert statistics and trends */
  initialTimeRange?: string;
  /** Initial filters for the alerts table */
  initialFilters?: AlertFilter;
}

/**
 * Interface for feedback notification state
 */
interface FeedbackState {
  /** Whether the feedback notification is open */
  open: boolean;
  /** Message to display in the notification */
  message: string;
  /** Severity level of the notification */
  severity: 'success' | 'error' | 'info' | 'warning';
}

/**
 * Main component for the alert management dashboard
 */
const AlertDashboard: React.FC<AlertDashboardProps> = (props) => {
  // Destructure props including className
  const { className, initialTimeRange, initialFilters } = props;

  // Initialize state for selectedAlert, relatedAlerts, suggestedActions, notificationChannels, timeRange, and feedback
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [relatedAlerts, setRelatedAlerts] = useState<Alert[]>([]);
  const [suggestedActions, setSuggestedActions] = useState<{ description: string; confidence: number; actionType: string }[]>([]);
  const [notificationChannels, setNotificationChannels] = useState<{ teams: boolean; email: boolean; sms: boolean; configured: string[] }>({ teams: false, email: false, sms: false, configured: [] });
  const [timeRange, setTimeRange] = useState<string>(initialTimeRange || '24h');
  const [feedback, setFeedback] = useState<FeedbackState>({ open: false, message: '', severity: 'info' });
  const [filters, setFilters] = useState<AlertFilter>(initialFilters || {});

  // Set up API hooks for loading states and error handling
  const { loading: alertLoading, error: alertError } = useApi();
  const { loading: relatedLoading, error: relatedError } = useApi();
  const { loading: actionsLoading, error: actionsError } = useApi();
  const { loading: channelsLoading, error: channelsError } = useApi();

  // Define fetchAlertDetails function to retrieve detailed information about a selected alert
  const fetchAlertDetails = useCallback(async (alertId: string) => {
    if (!alertId) return;
    try {
      const alert = await alertService.getAlertById(alertId);
      const related = await alertService.getRelatedAlerts(alertId);
      const actions = await alertService.getSuggestedActions(alertId);
      setSelectedAlert(alert);
      setRelatedAlerts(related);
      setSuggestedActions(actions.actions);
    } catch (error) {
      console.error('Error fetching alert details:', error);
    }
  }, []);

  // Define handleAlertSelect function to update the selected alert and fetch its details
  const handleAlertSelect = (alert: Alert) => {
    setSelectedAlert(alert);
    fetchAlertDetails(alert.alertId);
  };

  // Define handleTimeRangeChange function to update the time range for statistics and trends
  const handleTimeRangeChange = (newTimeRange: string) => {
    setTimeRange(newTimeRange);
  };

  // Define handleAlertAction function to handle alert actions (acknowledge, resolve, escalate, suppress)
  const handleAlertAction = (actionType: string, success: boolean) => {
    let message = '';
    switch (actionType) {
      case 'acknowledge':
        message = success ? 'Alert acknowledged' : 'Failed to acknowledge alert';
        break;
      case 'resolve':
        message = success ? 'Alert resolved' : 'Failed to resolve alert';
        break;
      case 'escalate':
        message = success ? 'Alert escalated' : 'Failed to escalate alert';
        break;
      case 'suppress':
        message = success ? 'Similar alerts suppressed' : 'Failed to suppress similar alerts';
        break;
      default:
        message = 'Action completed';
    }
    setFeedback({ open: true, message, severity: success ? 'success' : 'error' });
    if (selectedAlert) {
      fetchAlertDetails(selectedAlert.alertId);
    }
  };

  // Define handleFeedback function to show feedback messages after actions
  const handleFeedbackClose = () => {
    setFeedback({ ...feedback, open: false });
  };

  // Define handleFilterChange function to update the filters
  const handleFilterChange = (newFilters: AlertFilter) => {
    setFilters(newFilters);
  };

  // Set up useEffect to fetch notification channel status on component mount
  useEffect(() => {
    const fetchNotificationChannels = async () => {
      try {
        const channels = await alertService.getNotificationChannels();
        setNotificationChannels(channels);
      } catch (error) {
        console.error('Error fetching notification channels:', error);
      }
    };
    fetchNotificationChannels();
  }, []);

  // Access the current theme for styling
  const theme = useTheme();

  // Render the dashboard layout with Grid components
  return (
    <Grid container spacing={3} className={className}>
      {/* Left column with ActiveAlertsTable */}
      <Grid item xs={12} md={8}>
        <ActiveAlertsTable
          onAlertSelect={handleAlertSelect}
          filters={filters}
          className="active-alerts-table"
        />
      </Grid>

      {/* Right column with AlertStatsCard, AlertTrendChart, and NotificationChannelsCard */}
      <Grid item xs={12} md={4}>
        <AlertStatsCard
          timeRange={timeRange}
          onSegmentClick={(severity) => handleFilterChange({ ...filters, severity: severity ? [severity] : undefined })}
          className="alert-stats-card"
        />
        <AlertTrendChart
          timeRange={timeRange}
          showTimeRangeSelector={true}
          onTimeRangeChange={handleTimeRangeChange}
          className="alert-trend-chart"
        />
        <NotificationChannelsCard className="notification-channels-card" />
      </Grid>

      {/* Alert Details Section (conditionally rendered) */}
      {selectedAlert && (
        <Grid item xs={12} marginTop={3}>
          <AlertDetailsCard
            alert={selectedAlert}
            onAcknowledge={handleAlertAction}
            onResolve={handleAlertAction}
            onEscalate={handleAlertAction}
            onSuppress={handleAlertAction}
            className="alert-details-card"
          />
        </Grid>
      )}

      {/* Related Alerts and Suggested Actions Section (conditionally rendered) */}
      {selectedAlert && (
        <Grid container spacing={3} marginTop={3}>
          <Grid item xs={12} md={6}>
            <RelatedAlertsCard
              alertId={selectedAlert.alertId}
              onAlertSelect={handleAlertSelect}
              className="related-alerts-card"
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <SuggestedActionsCard
              alertId={selectedAlert.alertId}
              onActionExecuted={handleAlertAction}
              className="suggested-actions-card"
            />
          </Grid>
        </Grid>
      )}

      {/* Loading indicator */}
      {(alertLoading || relatedLoading || actionsLoading || channelsLoading) && (
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
          <CircularProgress />
        </Box>
      )}

      {/* Feedback Snackbar */}
      <Snackbar
        open={feedback.open}
        autoHideDuration={6000}
        onClose={handleFeedbackClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <MuiAlert onClose={handleFeedbackClose} severity={feedback.severity} sx={{ width: '100%' }}>
          {feedback.message}
        </MuiAlert>
      </Snackbar>
    </Grid>
  );
};

export default AlertDashboard;