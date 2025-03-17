import React from 'react'; // react ^18.2.0
import Card from '../common/Card';
import { useDashboard } from '../../contexts/DashboardContext';
import { AlertSummary, AlertSeverity } from '../../types/dashboard';
import { Box, Typography, List, ListItem, ListItemText, Chip, Button } from '@mui/material'; // @mui/material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { formatDistanceToNow } from 'date-fns'; // date-fns ^2.29.3
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0

/**
 * @dev Interface for the AlertSummaryCard component props.
 * @param className - Optional CSS class name for styling.
 * @param maxAlerts - Optional maximum number of alerts to display.
 */
interface AlertSummaryCardProps {
  className?: string;
  maxAlerts?: number;
}

/**
 * @dev Formats alert timestamp as relative time (e.g., "5 minutes ago").
 * @param timestamp - The timestamp to format.
 * @returns A formatted string representing the relative time.
 */
const formatAlertTime = (timestamp: string): string => {
  const timeAgo = formatDistanceToNow(new Date(timestamp), { addSuffix: true });
  return timeAgo;
};

/**
 * @dev Gets the appropriate color for an alert severity level.
 * @param severity - The severity level of the alert.
 * @returns A color code string for the severity level.
 */
const getSeverityColor = (severity: AlertSeverity): string => {
  switch (severity) {
    case AlertSeverity.CRITICAL:
      return 'error.main';
    case AlertSeverity.HIGH:
      return 'warning.main';
    case AlertSeverity.MEDIUM:
      return 'info.main';
    case AlertSeverity.LOW:
      return 'success.main';
    default:
      return 'grey.500';
  }
};

/**
 * @dev Handles click on the 'View All Alerts' button.
 */
const handleViewAllClick = () => {
  navigate('/alerts');
};

/**
 * @dev Card component that displays a summary of active alerts.
 * @param props - The props for the component.
 * @returns A React element that displays a summary of active alerts.
 */
const AlertSummaryCard: React.FC<AlertSummaryCardProps> = ({ className, maxAlerts = 5 }) => {
  // Access dashboard data and loading state from the DashboardContext
  const { dashboardData, loading } = useDashboard();

  // Access theme values for styling
  const theme = useTheme();

  // Access the navigate function for routing
  const navigate = useNavigate();

  // Extract active alerts from dashboard data or use an empty array if not available
  const activeAlerts: AlertSummary[] = dashboardData?.activeAlerts || [];

  // Limit the number of alerts to display based on the maxAlerts prop
  const limitedAlerts = activeAlerts.slice(0, maxAlerts);

  return (
    <Card
      title="Active Alerts"
      action={
        <Button color="primary" onClick={handleViewAllClick}>
          View All
        </Button>
      }
      loading={loading}
      className={className}
    >
      {loading ? (
        <Typography>Loading alerts...</Typography>
      ) : limitedAlerts.length === 0 ? (
        <Typography>No active alerts</Typography>
      ) : (
        <List>
          {limitedAlerts.map((alert) => (
            <ListItem key={alert.id} alignItems="flex-start">
              <ListItemText
                primary={
                  <Box display="flex" alignItems="center">
                    <Chip
                      label={alert.severity}
                      size="small"
                      color="default"
                      sx={{
                        mr: 1,
                        backgroundColor: theme.palette[getSeverityColor(alert.severity)].light, // Color-code severity chips
                        color: theme.palette[getSeverityColor(alert.severity)].dark,
                      }}
                    />
                    <Typography variant="body2" color="textSecondary">
                      {alert.description}
                    </Typography>
                  </Box>
                }
                secondary={
                  <React.Fragment>
                    <Typography
                      sx={{ display: 'inline' }}
                      component="span"
                      variant="body2"
                      color="text.primary"
                    >
                      {alert.pipeline}
                    </Typography>
                    {` — ${formatAlertTime(alert.timestamp)}`} {/* Format timestamps as relative time */}
                    {alert.selfHealingStatus && (
                      <Typography variant="caption" display="block" color="textSecondary">
                        Self-Healing Status: {alert.selfHealingStatus} {/* Show self-healing status if available */}
                      </Typography>
                    )}
                  </React.Fragment>
                }
              />
            </ListItem>
          ))}
        </List>
      )}
    </Card>
  );
};

export default AlertSummaryCard;