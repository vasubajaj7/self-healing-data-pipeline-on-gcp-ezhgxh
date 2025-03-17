import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  CardContent,
  CardHeader,
  Typography,
  Box,
  FormControl,
  Select,
  MenuItem,
  InputLabel,
  CircularProgress
} from '@mui/material'; // version: ^5.11.0
import { format, subDays } from 'date-fns'; // version: ^2.29.3

import LineChart from '../charts/LineChart'; // path: src/web/src/components/charts/LineChart.tsx
import alertService from '../../services/api/alertService'; // path: src/web/src/services/api/alertService.ts
import { AlertSeverity } from '../../types/alerts'; // path: src/web/src/types/alerts.ts
import { colors } from '../../theme/colors'; // path: src/web/src/theme/colors.ts

/**
 * Props interface for the AlertTrendChart component
 */
interface AlertTrendChartProps {
  /** Initial time range for trend data (e.g., '24h', '7d', '30d') */
  timeRange?: string;
  /** Whether to show the time range selector */
  showTimeRangeSelector?: boolean;
  /** Height of the chart */
  height?: number | string;
  /** Width of the chart */
  width?: number | string;
  /** Additional CSS class for styling */
  className?: string;
  /** Callback when time range changes */
  onTimeRangeChange?: (timeRange: string) => void;
}

/**
 * Component that displays alert trends over time with severity breakdown
 */
const AlertTrendChart: React.FC<AlertTrendChartProps> = (props) => {
  // Destructure props with default values
  const {
    timeRange: initialTimeRange,
    showTimeRangeSelector = true,
    height = 300,
    width = '100%',
    className = '',
    onTimeRangeChange
  } = props;

  // Define state variables
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] = useState<object | null>(null);
  const [selectedTimeRange, setSelectedTimeRange] = useState<string>(initialTimeRange || '7d');

  /**
   * Memoized function to fetch trend data from the API
   */
  const fetchTrendData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Fetch alert stats using alertService
      const alertStats = await alertService.getAlertStats(selectedTimeRange);

      // Format the trend data for the chart
      const formattedData = formatTrendData(alertStats.trend);

      // Update chart data state
      setChartData(formattedData);
    } catch (e: any) {
      // Handle errors during data fetching
      setError(e.message || 'Failed to fetch alert trend data.');
    } finally {
      // Set loading state to false
      setLoading(false);
    }
  }, [selectedTimeRange]);

  // Fetch trend data when component mounts or time range changes
  useEffect(() => {
    fetchTrendData();
  }, [fetchTrendData]);

  /**
   * Handles time range selection change
   * @param event React.ChangeEvent<HTMLInputElement>
   */
  const handleTimeRangeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newTimeRange = event.target.value;
    setSelectedTimeRange(newTimeRange);
    onTimeRangeChange?.(newTimeRange);
  };

  /**
   * Renders the AlertTrendChart component
   */
  return (
    <Card className={className}>
      <CardHeader
        title="Alert Trends"
        action={showTimeRangeSelector && (
          <FormControl variant="outlined" size="small">
            <InputLabel id="time-range-label">Time Range</InputLabel>
            <Select
              labelId="time-range-label"
              id="time-range-select"
              value={selectedTimeRange}
              onChange={handleTimeRangeChange}
              label="Time Range"
            >
              {getTimeRangeOptions().map((option) => (
                <MenuItem key={option.value} value={option.value}>
                  {option.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        )}
      />
      <CardContent>
        {loading && (
          <Box display="flex" justifyContent="center" alignItems="center" height={height}>
            <CircularProgress />
          </Box>
        )}
        {error && (
          <Typography color="error" align="center">
            {error}
          </Typography>
        )}
        {chartData && (
          <LineChart
            data={chartData}
            options={chartOptions}
            height={height}
            width={width}
          />
        )}
      </CardContent>
    </Card>
  );
};

/**
 * Formats raw trend data for use with the LineChart component
 * @param rawData Array<{timestamp: string, count: number, severity?: string}>
 * @returns Formatted chart data with datasets for each severity level
 */
const formatTrendData = (rawData: Array<{ timestamp: string; count: number; severity?: string }>) => {
  // Group trend data by severity
  const groupedData: { [severity: string]: { timestamp: string; count: number }[] } = {};
  rawData.forEach((item) => {
    const severity = item.severity || 'info';
    if (!groupedData[severity]) {
      groupedData[severity] = [];
    }
    groupedData[severity].push({ timestamp: item.timestamp, count: item.count });
  });

  // Create datasets for each severity level (critical, high, medium, low)
  const datasets = Object.entries(groupedData).map(([severity, data]) => ({
    label: severity,
    data: data.map((item) => ({ x: item.timestamp, y: item.count })),
    borderColor: colors.severity[severity]?.main || colors.grey[500],
    backgroundColor: colors.severity[severity]?.light || colors.grey[200],
    fill: false,
  }));

  // Format timestamps for x-axis labels
  const labels = rawData.map((item) => format(new Date(item.timestamp), 'MMM dd, HH:mm'));

  // Return formatted chart data object
  return {
    labels: [...new Set(labels)], // Remove duplicate labels
    datasets,
  };
};

/**
 * Generates time range options for the selector
 * @returns Array<{value: string, label: string}> Array of time range options
 */
const getTimeRangeOptions = () => {
  // Create array of time range options (24h, 7d, 30d, 90d)
  const options = [
    { value: '24h', label: 'Last 24 Hours' },
    { value: '7d', label: 'Last 7 Days' },
    { value: '30d', label: 'Last 30 Days' },
    { value: '90d', label: 'Last 90 Days' },
  ];

  // Return formatted options with values and labels
  return options;
};

/**
 * Chart.js options for the LineChart component
 */
const chartOptions = {
  plugins: {
    legend: {
      position: 'bottom',
      labels: {
        usePointStyle: true,
        padding: 20,
      },
    },
    tooltip: {
      mode: 'index',
      intersect: false,
      callbacks: {
        title: (tooltipItems) => {
          return format(new Date(tooltipItems[0].label), 'MMM dd, HH:mm');
        },
        label: (tooltipItem) => {
          const datasetLabel = tooltipItem.dataset.label || 'Alerts';
          const count = tooltipItem.formattedValue;
          return `${datasetLabel}: ${count}`;
        },
      },
    },
  },
  scales: {
    x: {
      grid: {
        display: false,
      },
      ticks: {
        maxRotation: 0,
      },
    },
    y: {
      beginAtZero: true,
      ticks: {
        precision: 0,
      },
    },
  },
  responsive: true,
  maintainAspectRatio: false,
};

export default AlertTrendChart;