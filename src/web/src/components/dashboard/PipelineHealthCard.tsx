import React, { useState, useEffect } from 'react'; // react ^18.2.0
import Card from '../common/Card';
import GaugeChart from '../charts/GaugeChart';
import { useDashboard } from '../../contexts/DashboardContext';
import { PipelineStatus, PipelineHealthMetrics } from '../../types/dashboard';
import { Box, Typography, Grid, Divider } from '@mui/material'; // @mui/material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { formatPercentage } from '../../utils/formatting';

/**
 * Props for the PipelineHealthCard component
 */
interface PipelineHealthCardProps {
  className?: string;
  loading?: boolean;
  error?: Error | null;
  metrics?: PipelineHealthMetrics;
  onClick?: () => void;
}

/**
 * Card component that displays pipeline health metrics with a gauge chart and status breakdown
 */
const PipelineHealthCard: React.FC<PipelineHealthCardProps> = ({
  className,
  loading = false,
  error = null,
  metrics,
  onClick,
}) => {
  // Access dashboard data using the useDashboard hook
  const { dashboardData } = useDashboard();

  // Access theme colors for styling
  const theme = useTheme();

  // Determine the metrics to use: props if provided, otherwise from dashboard data
  const healthMetrics = metrics || dashboardData?.pipelineHealth;

  // Calculate the healthy percentage from the metrics
  const healthyPercentage = healthMetrics ? healthMetrics.healthyPercentage : 0;

  // Define gauge chart thresholds based on health percentage ranges
  const gaugeThresholds = {
    0: theme.palette.error.main,
    60: theme.palette.warning.main,
    80: theme.palette.success.main,
  };

  // Define card styles for consistent UI
  const cardStyles = {
    height: '100%',
    cursor: 'pointer',
  };

  // Define gauge container styles for layout
  const gaugeContainer = {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    padding: '16px 0',
  };

  // Define status grid styles for organizing status counts
  const statusGrid = {
    marginTop: '16px',
  };

  // Define status item styles for text alignment
  const statusItem = {
    textAlign: 'center',
  };

  // Define status count styles for bold font and size
  const statusCount = {
    fontWeight: 'bold',
    fontSize: '1.5rem',
  };

  // Define status label styles for smaller font and secondary color
  const statusLabel = {
    fontSize: '0.875rem',
    color: 'text.secondary',
  };

  // Define color mapping for different pipeline statuses
  const colorMapping = {
    [PipelineStatus.HEALTHY]: theme.palette.success.main,
    [PipelineStatus.WARNING]: theme.palette.warning.main,
    [PipelineStatus.ERROR]: theme.palette.error.main,
    [PipelineStatus.INACTIVE]: theme.palette.text.disabled,
  };

  /**
   * Handles click events on the card to navigate to pipeline management page
   */
  const handleCardClick = () => {
    if (onClick) {
      onClick();
    }
  };

  return (
    <Card
      title="Pipeline Health"
      className={className}
      style={cardStyles}
      loading={loading}
      error={error}
      onClick={handleCardClick}
    >
      {healthMetrics ? (
        <>
          <Box style={gaugeContainer}>
            <GaugeChart
              value={healthyPercentage}
              thresholds={gaugeThresholds}
              height={180}
              width="180px"
              valueFormat="percentage"
            />
          </Box>
          <Grid container spacing={2} style={statusGrid}>
            <Grid item xs={6} sm={3} style={statusItem}>
              <Typography variant="h6" style={{ ...statusCount, color: colorMapping[PipelineStatus.HEALTHY] }}>
                {healthMetrics.healthyCount}
              </Typography>
              <Typography variant="body2" style={statusLabel}>
                Healthy
              </Typography>
            </Grid>
            <Grid item xs={6} sm={3} style={statusItem}>
              <Typography variant="h6" style={{ ...statusCount, color: colorMapping[PipelineStatus.WARNING] }}>
                {healthMetrics.warningCount}
              </Typography>
              <Typography variant="body2" style={statusLabel}>
                Warning
              </Typography>
            </Grid>
            <Grid item xs={6} sm={3} style={statusItem}>
              <Typography variant="h6" style={{ ...statusCount, color: colorMapping[PipelineStatus.ERROR] }}>
                {healthMetrics.errorCount}
              </Typography>
              <Typography variant="body2" style={statusLabel}>
                Error
              </Typography>
            </Grid>
            <Grid item xs={6} sm={3} style={statusItem}>
              <Typography variant="h6" style={{ ...statusCount, color: colorMapping[PipelineStatus.INACTIVE] }}>
                {healthMetrics.inactiveCount}
              </Typography>
              <Typography variant="body2" style={statusLabel}>
                Inactive
              </Typography>
            </Grid>
          </Grid>
        </>
      ) : (
        <Typography variant="body1">No pipeline health data available.</Typography>
      )}
    </Card>
  );
};

export default PipelineHealthCard;