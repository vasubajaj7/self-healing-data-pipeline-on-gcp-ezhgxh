import React, { useState, useEffect } from 'react';
import { Box, Typography, Chip, List, ListItem, Divider } from '@mui/material';
import { ErrorOutline, ArrowForward } from '@mui/icons-material';
import Card from '../common/Card';
import { Alert } from '../../types/alerts';
import { alertService } from '../../services/api/alertService';
import { formatDistanceToNow } from '../../utils/date';

/**
 * Props interface for the RelatedAlertsCard component
 */
interface RelatedAlertsCardProps {
  /** ID of the alert to find related alerts for */
  alertId: string;
  /** Callback function when a related alert is selected */
  onAlertSelect: (alert: Alert) => void;
  /** Additional CSS class for styling */
  className?: string;
  /** Maximum number of related alerts to display */
  maxItems?: number;
}

/**
 * A card component that displays alerts related to a selected alert
 * 
 * This component fetches and displays related alerts to help users
 * understand connections between different issues and provides
 * context for troubleshooting.
 */
const RelatedAlertsCard: React.FC<RelatedAlertsCardProps> = ({
  alertId,
  onAlertSelect,
  className,
  maxItems = 5
}) => {
  // State for storing related alerts data
  const [relatedAlerts, setRelatedAlerts] = useState<Alert[]>([]);
  // Loading state for API requests
  const [loading, setLoading] = useState<boolean>(false);
  // Error state for API request failures
  const [error, setError] = useState<string | null>(null);

  /**
   * Fetches related alerts from the API
   */
  const fetchRelatedAlerts = async () => {
    if (!alertId) return;

    setLoading(true);
    setError(null);

    try {
      const alerts = await alertService.getRelatedAlerts(alertId);
      setRelatedAlerts(alerts);
    } catch (err) {
      console.error('Error fetching related alerts:', err);
      setError('Unable to fetch related alerts. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Fetch related alerts when alertId changes
  useEffect(() => {
    if (alertId) {
      fetchRelatedAlerts();
    } else {
      setRelatedAlerts([]);
    }
  }, [alertId]);

  /**
   * Gets the color for a severity level
   * @param severity - Alert severity level
   * @returns Appropriate color based on severity
   */
  const getSeverityColor = (severity: string): string => {
    switch (severity.toUpperCase()) {
      case 'CRITICAL':
      case 'HIGH':
        return 'error';
      case 'MEDIUM':
        return 'warning';
      case 'LOW':
        return 'info';
      default:
        return 'default';
    }
  };

  // Styles for list component
  const listStyles = {
    padding: '0',
    width: '100%'
  };

  // Styles for list items
  const listItemStyles = {
    cursor: 'pointer',
    borderBottom: '1px solid rgba(0, 0, 0, 0.12)',
    padding: '8px 16px',
    transition: 'background-color 0.3s',
    '&:hover': {
      backgroundColor: 'rgba(0, 0, 0, 0.04)'
    }
  };

  // Styles for severity chips
  const severityChipStyles = {
    marginRight: '8px',
    height: '24px',
    fontSize: '0.75rem'
  };

  // Styles for alert messages
  const messageStyles = {
    fontWeight: '400',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap'
  };

  // Styles for timestamps
  const timestampStyles = {
    fontSize: '0.75rem',
    color: 'text.secondary'
  };

  // Styles for empty state
  const emptyStateStyles = {
    padding: '16px',
    textAlign: 'center',
    color: 'text.secondary'
  };

  return (
    <Card
      title="Related Alerts"
      loading={loading}
      error={error}
      className={className}
    >
      {relatedAlerts.length > 0 ? (
        <List sx={listStyles} aria-label="Related alerts list">
          {relatedAlerts.slice(0, maxItems).map((alert, index) => (
            <ListItem
              key={alert.alertId}
              sx={listItemStyles}
              onClick={() => onAlertSelect(alert)}
              aria-label={`Related alert: ${alert.message}`}
              divider={index < relatedAlerts.length - 1}
            >
              <Box display="flex" alignItems="center" width="100%">
                <Chip
                  icon={<ErrorOutline fontSize="small" />}
                  label={alert.severity}
                  color={getSeverityColor(alert.severity) as any}
                  size="small"
                  sx={severityChipStyles}
                />
                <Box flexGrow={1}>
                  <Typography variant="body2" sx={messageStyles}>
                    {alert.message}
                  </Typography>
                  <Typography variant="caption" sx={timestampStyles}>
                    {formatDistanceToNow(alert.createdAt)} ago
                  </Typography>
                </Box>
                <ArrowForward fontSize="small" color="action" />
              </Box>
            </ListItem>
          ))}
        </List>
      ) : (
        <Box sx={emptyStateStyles}>
          <Typography variant="body2">
            No related alerts found
          </Typography>
        </Box>
      )}
    </Card>
  );
};

export default RelatedAlertsCard;