import React, { useState, useEffect, useMemo } from 'react'; // version: ^18.2.0
import {
  Box,
  Typography,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from '@mui/material'; // version: ^5.11.0
import merge from 'lodash'; // version: ^4.17.21
import LineChart from '../charts/LineChart';
import {
  transformTimeSeriesData,
} from '../../services/charts/dataTransformers';
import {
  formatDatetime,
  formatPercentage,
} from '../../services/charts/chartUtils';
import { getChartConfig, chartColorSchemes } from '../../config/chartConfig';
import { useQuality } from '../../contexts/QualityContext';
import {
  QualityDimension,
  QualityTimeSeries,
  QualityTimeRange,
} from '../../types/quality';

/**
 * Interface defining the props for the QualityTrendChart component
 */
export interface QualityTrendChartProps {
  /**
   * The dataset name to show quality trends for
   */
  dataset: string;
  /**
   * Optional table name to filter quality trends
   */
  table?: string;
  /**
   * Time range for the trend data (default: LAST_30_DAYS)
   */
  timeRange?: QualityTimeRange;
  /**
   * Height of the chart
   */
  height?: number | string;
  /**
   * Width of the chart
   */
  width?: number | string;
  /**
   * Additional CSS class for styling
   */
  className?: string;
  /**
   * Whether to show individual dimension trends (default: false)
   */
  showDimensions?: boolean;
  /**
   * Array of dimensions to display when showDimensions is true
   */
  selectedDimensions?: QualityDimension[];
  /**
   * Custom chart options to override defaults
   */
  options?: object;
  /**
   * Callback when time range selection changes
   */
  onTimeRangeChange?: (timeRange: QualityTimeRange) => void;
  /**
   * Callback when a data point is clicked
   */
  onDataPointClick?: (point: { date: string; dimension: string; score: number }) => void;
}

/**
 * A component that displays quality score trends over time using a line chart
 */
const QualityTrendChart: React.FC<QualityTrendChartProps> = ({
  dataset,
  table,
  timeRange: initialTimeRange,
  height = 300,
  width = '100%',
  className = '',
  showDimensions = false,
  selectedDimensions,
  options,
  onTimeRangeChange,
  onDataPointClick,
}) => {
  // Local state for time range selection
  const [localTimeRange, setLocalTimeRange] = useState<QualityTimeRange>(initialTimeRange || QualityTimeRange.LAST_30_DAYS);

  // Access quality data from context including time series data
  const { timeSeries, fetchTimeSeries, loading } = useQuality();

  // Effect to fetch time series data when dependencies change
  useEffect(() => {
    const fetchData = async () => {
      try {
        await fetchTimeSeries({
          dataset,
          table,
          startDate: undefined,
          endDate: undefined,
        });
      } catch (error) {
        console.error('Error fetching time series data:', error);
      }
    };

    fetchData();
  }, [dataset, table, localTimeRange, fetchTimeSeries]);

  // Transformed chart data for the line chart visualization
  const chartData = useMemo(() => {
    return transformTimeSeriesData(timeSeries, { showDimensions, selectedDimensions });
  }, [timeSeries, showDimensions, selectedDimensions]);

  // Chart options configuration with appropriate styling
  const chartOptions = useMemo(() => {
    return getChartOptions(options);
  }, [options]);

  /**
   * Formats the tooltip content for the quality trend chart
   * @param tooltipItem 
   * @returns Formatted tooltip string with date, quality score and dimension
   */
  const formatTooltip = (tooltipItem: any): string => {
    // Extract value, label and dataset label from tooltip item
    const { value, label, dataset } = tooltipItem;

    // Format date using formatDatetime function
    const formattedDate = formatDatetime(label);

    // Format quality score as percentage with 1 decimal place
    const formattedScore = formatPercentage(value, 1);

    // Return formatted string with date, dimension name, and score
    return `${formattedDate} - ${dataset.label}: ${formattedScore}`;
  };

  /**
   * Generates chart options for the quality trend chart
   * @param customOptions 
   * @returns Chart options configuration
   */
  const getChartOptions = (customOptions: any) => {
    // Get base chart configuration for line chart
    const baseOptions = getChartConfig('line');

    // Configure x-axis to display dates properly
    const xAxisConfig = {
      type: 'time',
      time: {
        unit: 'day',
        displayFormats: {
          day: 'MMM d',
        },
      },
      title: {
        display: true,
        text: 'Date',
      },
    };

    // Configure y-axis to display percentages with proper min/max
    const yAxisConfig = {
      min: 0,
      max: 100,
      ticks: {
        callback: (value: number) => formatPercentage(value / 100),
      },
      title: {
        display: true,
        text: 'Quality Score',
      },
    };

    // Set up tooltip formatting using formatTooltip function
    const tooltipConfig = {
      callbacks: {
        label: formatTooltip,
      },
    };

    // Configure legend display and position
    const legendConfig = {
      display: true,
      position: 'bottom' as const,
    };

    // Set up responsive options
    const responsiveConfig = {
      responsive: true,
      maintainAspectRatio: false,
    };

    // Merge with any custom options provided
    const mergedOptions = merge({}, baseOptions, {
      scales: {
        x: xAxisConfig,
        y: yAxisConfig,
      },
      plugins: {
        tooltip: tooltipConfig,
        legend: legendConfig,
      },
      responsive: responsiveConfig.responsive,
      maintainAspectRatio: responsiveConfig.maintainAspectRatio,
      onClick: (event: any, elements: any[]) => {
        if (elements.length > 0 && onDataPointClick) {
          const element = elements[0];
          const dataPoint = {
            date: chartData.labels?.[element.index] as string,
            dimension: chartData.datasets[element.datasetIndex].label as string,
            score: chartData.datasets[element.datasetIndex].data[element.index] as number,
          };
          onDataPointClick(dataPoint);
        }
      },
    }, customOptions);

    // Return complete chart options
    return mergedOptions;
  };

  return (
    <Box className={`quality-trend-chart ${className}`} height={height} width={width} position="relative">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="h6">Quality Score Trend</Typography>
        {onTimeRangeChange && (
          <FormControl>
            <InputLabel id="time-range-label">Time Range</InputLabel>
            <Select
              labelId="time-range-label"
              value={localTimeRange}
              label="Time Range"
              onChange={(e) => {
                const newTimeRange = e.target.value as QualityTimeRange;
                setLocalTimeRange(newTimeRange);
                onTimeRangeChange(newTimeRange);
              }}
            >
              <MenuItem value={QualityTimeRange.LAST_24_HOURS}>Last 24 Hours</MenuItem>
              <MenuItem value={QualityTimeRange.LAST_7_DAYS}>Last 7 Days</MenuItem>
              <MenuItem value={QualityTimeRange.LAST_30_DAYS}>Last 30 Days</MenuItem>
              <MenuItem value={QualityTimeRange.LAST_90_DAYS}>Last 90 Days</MenuItem>
              <MenuItem value={QualityTimeRange.CUSTOM}>Custom Range</MenuItem>
            </Select>
          </FormControl>
        )}
      </Box>
      {loading ? (
        <Box display="flex" justifyContent="center" alignItems="center" height={height}>
          <CircularProgress />
        </Box>
      ) : (
        <LineChart data={chartData} options={chartOptions} height={height} width={width} />
      )}
    </Box>
  );
};

QualityTrendChart.defaultProps = {
  height: 300,
  width: '100%',
  showDimensions: false,
  timeRange: QualityTimeRange.LAST_30_DAYS,
};

export default QualityTrendChart;