import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Card,
  CardContent,
  CardHeader,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  Alert,
  ToggleButtonGroup,
  ToggleButton,
} from '@mui/material'; // @mui/material ^5.11.0
import Storage from '@mui/icons-material/Storage'; // @mui/icons-material ^5.11.0
import Memory from '@mui/icons-material/Memory'; // @mui/icons-material ^5.11.0
import CloudQueue from '@mui/icons-material/CloudQueue'; // @mui/icons-material ^5.11.0
import DataUsage from '@mui/icons-material/DataUsage'; // @mui/icons-material ^5.11.0
import { format, subDays, startOfDay, endOfDay } from 'date-fns'; // date-fns ^2.30.0

import LineChart from '../charts/LineChart';
import optimizationService from '../../services/api/optimizationService';
import useApi from '../../hooks/useApi';
import { transformTimeSeriesData } from '../../services/charts/dataTransformers';
import { formatPercentage, formatDatetime } from '../../services/charts/chartUtils';
import { ResourceUtilizationMetrics } from '../../types/optimization';

/**
 * Interface defining the props for the ResourceUtilizationChart component
 */
interface ResourceUtilizationChartProps {
  height?: string | number;
  title?: string;
  className?: string;
  initialResourceType?: string;
  initialTimePeriod?: string;
  onResourceTypeChange?: (resourceType: string) => void;
}

/**
 * Interface defining the structure for resource types
 */
interface ResourceType {
  id: string;
  name: string;
  icon: React.ReactNode;
}

/**
 * Interface defining the structure for time period options
 */
interface TimePeriod {
  id: string;
  name: string;
  days: number;
}

/**
 * A component that displays resource utilization metrics over time with filtering options
 */
const ResourceUtilizationChart: React.FC<ResourceUtilizationChartProps> = (props) => {
  // State variables for managing resource type, time period, metrics, loading, and error
  const [resourceType, setResourceType] = useState<string>(props.initialResourceType || 'bigquery_slots');
  const [timePeriod, setTimePeriod] = useState<string>(props.initialTimePeriod || 'last7days');
  const [metrics, setMetrics] = useState<ResourceUtilizationMetrics[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);

  // Custom hook for API calls with loading and error states
  const { get } = useApi();

  // Constants for available resource types and time periods
  const resourceTypes: ResourceType[] = useMemo(() => [
    { id: 'bigquery_slots', name: 'BigQuery Slots', icon: <DataUsage /> },
    { id: 'composer_workers', name: 'Composer Workers', icon: <CloudQueue /> },
    { id: 'storage', name: 'Storage', icon: <Storage /> },
    { id: 'memory', name: 'Memory', icon: <Memory /> },
  ], []);

  const timePeriods: TimePeriod[] = useMemo(() => [
    { id: 'last24hours', name: 'Last 24 Hours', days: 1 },
    { id: 'last7days', name: 'Last 7 Days', days: 7 },
    { id: 'last30days', name: 'Last 30 Days', days: 30 },
    { id: 'last90days', name: 'Last 90 Days', days: 90 },
  ], []);

  /**
   * Load resource utilization metrics data
   */
  const loadResourceMetrics = useCallback(async () => {
    // Calculate the date range based on the selected time period
    const endDate = startOfDay(new Date());
    const startDate = subDays(endDate, timePeriods.find(p => p.id === timePeriod)?.days || 7);

    setLoading(true);
    setError(null);

    try {
      // Fetch resource utilization metrics from the API
      const response = await get<any>(optimizationService.getResourceUtilizationMetrics({
        resourceType: resourceType,
        startDate: format(startDate, 'yyyy-MM-dd'),
        endDate: format(endDate, 'yyyy-MM-dd'),
        pageSize: 1000, // Fetch all metrics in one page
        page: 1,
      }));

      // Update the state with the fetched metrics
      setMetrics(response?.items || []);
    } catch (err: any) {
      // Set the error state if data fetching fails
      setError(err);
    } finally {
      // Set loading to false after data fetching is complete
      setLoading(false);
    }
  }, [resourceType, timePeriod, get, timePeriods]);

  /**
   * Handle resource type selection change
   * @param event The change event
   */
  const handleResourceTypeChange = (event: any) => {
    // Update the resource type state with the selected value
    setResourceType(event.target.value);
    // Call the onResourceTypeChange callback if provided
    if (props.onResourceTypeChange) {
      props.onResourceTypeChange(event.target.value);
    }
  };

  /**
   * Handle time period selection change
   * @param event The change event
   * @param newValue The new value
   */
  const handleTimePeriodChange = (event: React.MouseEvent<HTMLElement>, newValue: string | null) => {
    // Update the time period state with the selected value if not null
    if (newValue) {
      setTimePeriod(newValue);
    }
  };

  /**
   * Format metrics data for the chart
   * @param metrics The metrics data to format
   * @returns The formatted metrics data
   */
  const formatChartData = useCallback((metrics: ResourceUtilizationMetrics[]) => {
    // Transform the metrics data into time series format for the LineChart component
    return transformTimeSeriesData({
      name: 'Utilization',
      data: metrics.map(m => ({
        timestamp: m.timestamp,
        value: m.utilizationPercentage,
      })),
    });
  }, []);

  // Fetch resource utilization metrics when resource type or time period changes
  useEffect(() => {
    loadResourceMetrics();
  }, [loadResourceMetrics]);

  return (
    <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }} className={props.className}>
      <CardHeader title={props.title || 'Resource Utilization'} />
      <CardContent sx={{ flexGrow: 1, padding: '16px', display: 'flex', flexDirection: 'column' }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
          <FormControl size="small" sx={{ minWidth: 200 }}>
            <InputLabel id="resource-type-label">Resource Type</InputLabel>
            <Select
              labelId="resource-type-label"
              value={resourceType}
              onChange={handleResourceTypeChange}
              label="Resource Type"
            >
              {resourceTypes.map(type => (
                <MenuItem key={type.id} value={type.id}>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    {type.icon}
                    <Box sx={{ ml: 1 }}>{type.name}</Box>
                  </Box>
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <ToggleButtonGroup
            value={timePeriod}
            exclusive
            onChange={handleTimePeriodChange}
            size="small"
          >
            {timePeriods.map(period => (
              <ToggleButton key={period.id} value={period.id}>{period.name}</ToggleButton>
            ))}
          </ToggleButtonGroup>
        </Box>
        <Box sx={{ flexGrow: 1, position: 'relative', minHeight: '200px' }}>
          {loading && (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
              <CircularProgress />
            </Box>
          )}
          {error && (
            <Alert severity="error">Error loading resource utilization data: {error.message}</Alert>
          )}
          {!loading && !error && (!metrics || metrics.length === 0) && (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
              <Typography color="text.secondary">No utilization data available for the selected resource and time period.</Typography>
            </Box>
          )}
          {!loading && !error && metrics && metrics.length > 0 && (
            <LineChart
              data={formatChartData(metrics)}
              height="100%"
              width="100%"
              options={{
                maintainAspectRatio: false,
                scales: {
                  y: {
                    beginAtZero: true,
                    max: 100,
                    title: {
                      display: true,
                      text: 'Utilization (%)',
                    },
                  },
                  x: {
                    title: {
                      display: true,
                      text: 'Time',
                    },
                  },
                },
                plugins: {
                  tooltip: {
                    callbacks: {
                      label: (context) => `Utilization: ${formatPercentage(context.parsed.y, 1)}`,
                    },
                  },
                },
              }}
            />
          )}
        </Box>
      </CardContent>
    </Card>
  );
};

export default ResourceUtilizationChart;