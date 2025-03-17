import React, { useState, useEffect } from 'react'; // React library for component creation // version: ^18.2.0
import { Box, Typography, Grid, Divider } from '@mui/material'; // Material-UI components for layout and styling // version: ^5.11.0
import Card from '../common/Card'; // Container component for the alert statistics
import PieChart from '../charts/PieChart'; // Chart component to visualize alert distribution by severity
import { alertService } from '../../services/api/alertService'; // Service for fetching alert statistics data
import { useApi } from '../../hooks/useApi'; // Hook for handling API requests with loading and error states
import { AlertStats } from '../../types/alerts'; // Type definition for alert statistics data
import { AlertSeverity } from '../../types/global'; // Enum for alert severity levels

/**
 * Props for the AlertStatsCard component
 */
interface AlertStatsCardProps {
  /** Time range for alert statistics (e.g., '24h', '7d', '30d') */
  timeRange?: string;
  /** Additional CSS class for styling */
  className?: string;
  /** Height of the card */
  height?: number | string;
  /** Callback function when a chart segment is clicked, used for filtering */
  onSegmentClick?: (segmentLabel: string) => void;
}

/**
 * A component that displays alert statistics in a card format with a pie chart
 */
const AlertStatsCard: React.FC<AlertStatsCardProps> = ({
  timeRange,
  className,
  height,
  onSegmentClick
}) => {
  /**
   * Alert statistics data fetched from the API
   */
  const [alertStats, setAlertStats] = useState<AlertStats | null>(null);

  /**
   * Hook for handling API request states
   */
  const { loading, error } = useApi();

  /**
   * Fetches alert statistics from the API
   */
  const fetchAlertStats = async () => {
    try {
      // Call alertService.getAlertStats with the timeRange prop
      const stats = await alertService.getAlertStats(timeRange);
      // Update alertStats state with the response data
      setAlertStats(stats);
    } catch (error) {
      console.error('Failed to fetch alert stats:', error);
    }
  };

  /**
   * Side effect for data fetching
   */
  useEffect(() => {
    // Fetch alert statistics on component mount and when timeRange changes
    fetchAlertStats();
  }, [timeRange]);

  /**
   * Handles click events on pie chart segments
   * @param segmentLabel - The label of the clicked segment
   */
  const handleSegmentClick = (segmentLabel: string) => {
    // Map segment label to severity filter
    let severityFilter: AlertSeverity | undefined;
    switch (segmentLabel) {
      case 'Critical':
        severityFilter = AlertSeverity.CRITICAL;
        break;
      case 'High':
        severityFilter = AlertSeverity.HIGH;
        break;
      case 'Medium':
        severityFilter = AlertSeverity.MEDIUM;
        break;
      case 'Low':
        severityFilter = AlertSeverity.LOW;
        break;
      default:
        severityFilter = undefined;
        break;
    }

    // Call onSegmentClick prop with the severity filter if provided
    if (onSegmentClick) {
      onSegmentClick(severityFilter);
    }
  };

  /**
   * Transforms alert statistics into format required by PieChart
   */
  const prepareChartData = () => {
    if (!alertStats) return null;

    // Extract counts for each severity level from alertStats
    const critical = alertStats.critical;
    const high = alertStats.high;
    const medium = alertStats.medium;
    const low = alertStats.low;

    // Create labels and data arrays for the pie chart
    const labels = ['Critical', 'High', 'Medium', 'Low'];
    const data = [critical, high, medium, low];

    // Define colors for each severity level
    const chartColors = {
      critical: '#d32f2f',
      high: '#f44336',
      medium: '#ff9800',
      low: '#4caf50'
    };

    // Return formatted chart data object
    return {
      labels,
      datasets: [
        {
          label: 'Alerts by Severity',
          data,
          backgroundColor: [
            chartColors.critical,
            chartColors.high,
            chartColors.medium,
            chartColors.low
          ],
          borderWidth: 1,
          borderColor: '#fff',
          hoverOffset: 5
        }
      ]
    };
  };

  // Prepare chart data
  const chartData = prepareChartData();

  // Define chart options
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'bottom',
        labels: {
          usePointStyle: true,
          padding: 20
        }
      },
      tooltip: {
        callbacks: {
          label: (context: any) => {
            const label = context.label || '';
            const value = context.formattedValue || '';
            return `${label}: ${value}`;
          }
        }
      }
    },
    onClick: (event: any, elements: any[]) => {
      if (elements.length > 0) {
        const clickedElement = elements[0];
        const segmentLabel = chartData?.labels?.[clickedElement.index] as string;
        handleSegmentClick(segmentLabel);
      }
    }
  };

  return (
    <Card
      title="Alert Statistics"
      className={className}
      height={height}
      loading={loading}
      error={error?.message}
    >
      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <Box sx={{ height: '250px' }}>
            {chartData && (
              <PieChart data={chartData} options={chartOptions} />
            )}
          </Box>
        </Grid>
        <Grid item xs={12} md={6}>
          <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
            <Box sx={{ flexGrow: 1 }}>
              <Grid container spacing={2}>
                <Grid item xs={6} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '8px' }}>
                  <Typography variant="h6" style={{ fontWeight: 'bold', fontSize: '1.5rem' }}>
                    {alertStats?.critical || 0}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary" style={{ fontSize: '0.875rem' }}>
                    Critical
                  </Typography>
                </Grid>
                <Grid item xs={6} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '8px' }}>
                  <Typography variant="h6" style={{ fontWeight: 'bold', fontSize: '1.5rem' }}>
                    {alertStats?.high || 0}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary" style={{ fontSize: '0.875rem' }}>
                    High
                  </Typography>
                </Grid>
                <Grid item xs={6} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '8px' }}>
                  <Typography variant="h6" style={{ fontWeight: 'bold', fontSize: '1.5rem' }}>
                    {alertStats?.medium || 0}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary" style={{ fontSize: '0.875rem' }}>
                    Medium
                  </Typography>
                </Grid>
                <Grid item xs={6} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '8px' }}>
                  <Typography variant="h6" style={{ fontWeight: 'bold', fontSize: '1.5rem' }}>
                    {alertStats?.low || 0}
                  </Typography>
                  <Typography variant="subtitle2" color="text.secondary" style={{ fontSize: '0.875rem' }}>
                    Low
                  </Typography>
                </Grid>
              </Grid>
            </Box>
            <Divider />
            <Box sx={{ textAlign: 'center', padding: '12px' }}>
              <Typography variant="body2" color="text.secondary">
                Total Alerts: {alertStats?.total || 0}
              </Typography>
            </Box>
          </Box>
        </Grid>
      </Grid>
    </Card>
  );
};

export default AlertStatsCard;