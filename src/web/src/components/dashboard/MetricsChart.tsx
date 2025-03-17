import React, { useState, useEffect, useMemo } from 'react';
import {
  Card,
  CardHeader,
  CardContent,
  Box,
  Typography,
  CircularProgress,
  useTheme
} from '@mui/material';

// Import chart components
import LineChart from '../charts/LineChart';
import AreaChart from '../charts/AreaChart';

// Import utility functions
import { 
  formatDatetime, 
  formatNumber, 
  formatPercentage 
} from '../../services/charts/chartUtils';

// Import colors from theme
import { colors } from '../../theme/colors';

// Import dashboard context
import { useDashboard } from '../../contexts/DashboardContext';

// Import types
import { TimeRange } from '../../types/dashboard';

/**
 * Props for the MetricsChart component
 */
interface MetricsChartProps {
  /** Chart title displayed in the card header */
  title: string;
  /** Type of metric to display (pipeline, quality, healing, etc.) */
  metricType: string;
  /** Raw data to be visualized in the chart */
  data: any;
  /** Type of chart to display (line, area); defaults to line */
  chartType?: string;
  /** Chart height in pixels or CSS value */
  height?: number | string;
  /** Flag indicating if data is being loaded */
  loading?: boolean;
  /** Flag to show/hide chart legend */
  showLegend?: boolean;
  /** Additional CSS class for styling */
  className?: string;
  /** Callback function when a data point is clicked */
  onDataPointClick?: (dataPoint: any) => void;
}

/**
 * A component that displays various pipeline metrics as charts with customizable options
 */
const MetricsChart: React.FC<MetricsChartProps> = ({
  title,
  metricType,
  data,
  chartType = 'line',
  height = 300,
  loading = false,
  showLegend = true,
  className,
  onDataPointClick
}) => {
  const theme = useTheme();
  const { filters } = useDashboard();
  const timeRange = filters.timeRange;

  // Prepare chart data based on metric type and raw data
  const chartData = useMemo(() => {
    return prepareChartData(data, metricType, timeRange);
  }, [data, metricType, timeRange]);

  // Get chart options based on metric type and chart type
  const chartOptions = useMemo(() => {
    return getChartOptions(metricType, chartType);
  }, [metricType, chartType]);

  // Handle data point click events
  const handleDataPointClick = (dataPoint: any) => {
    if (onDataPointClick) {
      onDataPointClick(dataPoint);
    }
  };

  return (
    <Card className={className} sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <CardHeader title={title} />
      <CardContent sx={{ flex: 1, minHeight: 0, position: 'relative' }}>
        {loading ? (
          <Box sx={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            backgroundColor: 'rgba(255, 255, 255, 0.7)'
          }}>
            <CircularProgress />
          </Box>
        ) : !chartData || !chartData.datasets || chartData.datasets.length === 0 ? (
          <Box sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            height: '100%',
            color: 'text.secondary'
          }}>
            <Typography color="text.secondary">No data available</Typography>
          </Box>
        ) : (
          chartType === 'area' ? (
            <AreaChart 
              data={chartData}
              options={{
                ...chartOptions,
                plugins: {
                  ...chartOptions.plugins,
                  legend: {
                    ...chartOptions.plugins.legend,
                    display: showLegend
                  }
                }
              }}
              height="100%"
              width="100%"
              onDataPointClick={handleDataPointClick}
            />
          ) : (
            <LineChart 
              data={chartData}
              options={{
                ...chartOptions,
                plugins: {
                  ...chartOptions.plugins,
                  legend: {
                    ...chartOptions.plugins.legend,
                    display: showLegend
                  }
                }
              }}
              height="100%"
              width="100%"
              onDataPointClick={handleDataPointClick}
            />
          )
        )}
      </CardContent>
    </Card>
  );
};

/**
 * Prepares and formats data for chart visualization based on metric type
 */
const prepareChartData = (rawData: any, metricType: string, timeRange: TimeRange): any => {
  if (!rawData) return { labels: [], datasets: [] };

  // Get appropriate colors for the metric type
  const metricColors = {
    pipeline: colors.chart.blue,
    quality: colors.chart.green,
    healing: colors.chart.purple,
    performance: colors.chart.orange
  };
  
  // Default color if metric type not found
  const baseColor = metricColors[metricType as keyof typeof metricColors] || colors.chart.blue;
  
  // Format data based on structure
  if (rawData.labels && Array.isArray(rawData.datasets)) {
    // Data is already in Chart.js format, just apply styling
    return {
      labels: rawData.labels,
      datasets: rawData.datasets.map((dataset: any, index: number) => {
        const color = dataset.borderColor || baseColor;
        
        return {
          ...dataset,
          borderColor: color,
          backgroundColor: dataset.backgroundColor || `${color}40`, // 25% opacity
          pointBackgroundColor: color,
          pointBorderColor: '#fff',
          pointHoverBackgroundColor: color,
          pointHoverBorderColor: '#fff'
        };
      })
    };
  } else if (Array.isArray(rawData)) {
    // Convert array data to Chart.js format
    // Apply time filter if needed
    const filteredData = applyTimeFilter(rawData, timeRange);
    
    // Determine label text based on metric type
    let dataLabel = '';
    switch (metricType) {
      case 'pipeline':
        dataLabel = 'Pipeline Status';
        break;
      case 'quality':
        dataLabel = 'Quality Score';
        break;
      case 'healing':
        dataLabel = 'Self-Healing Rate';
        break;
      case 'performance':
        dataLabel = 'Processing Time';
        break;
      default:
        dataLabel = 'Value';
    }
    
    return {
      labels: filteredData.map((item: any) => 
        formatDatetime(item.timestamp || item.date || item.x, 'MMM d')
      ),
      datasets: [{
        label: dataLabel,
        data: filteredData.map((item: any) => item.value || item.y || 0),
        borderColor: baseColor,
        backgroundColor: `${baseColor}40`,
        pointBackgroundColor: baseColor,
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: baseColor,
        pointHoverBorderColor: '#fff',
        fill: false
      }]
    };
  }
  
  // Return empty data if format is not recognized
  return { labels: [], datasets: [] };
};

/**
 * Applies time range filter to array data
 */
const applyTimeFilter = (data: any[], timeRange: TimeRange): any[] => {
  if (!data || data.length === 0 || timeRange === 'CUSTOM') {
    return data;
  }
  
  const { startDate, endDate } = getDateRangeFromTimeRange(timeRange);
  
  return data.filter((item: any) => {
    const itemDate = new Date(item.timestamp || item.date || item.x);
    return itemDate >= startDate && itemDate <= endDate;
  });
};

/**
 * Converts TimeRange enum to actual Date objects
 */
const getDateRangeFromTimeRange = (timeRange: TimeRange): { startDate: Date, endDate: Date } => {
  const endDate = new Date();
  let startDate = new Date();
  
  switch (timeRange) {
    case 'LAST_HOUR':
      startDate.setHours(endDate.getHours() - 1);
      break;
    case 'LAST_24_HOURS':
      startDate.setDate(endDate.getDate() - 1);
      break;
    case 'LAST_7_DAYS':
      startDate.setDate(endDate.getDate() - 7);
      break;
    case 'LAST_30_DAYS':
      startDate.setDate(endDate.getDate() - 30);
      break;
    default:
      startDate.setDate(endDate.getDate() - 1); // Default to 24 hours
  }
  
  return { startDate, endDate };
};

/**
 * Generates chart options based on metric type and chart type
 */
const getChartOptions = (metricType: string, chartType: string): any => {
  // Base options
  const baseOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
        position: 'top' as const,
      },
      tooltip: {
        mode: 'index' as const,
        intersect: false,
      },
    },
    scales: {
      x: {
        grid: {
          display: true,
          drawBorder: true,
          color: 'rgba(0, 0, 0, 0.1)',
        },
        ticks: {
          maxRotation: 0,
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          color: 'rgba(0, 0, 0, 0.1)',
        }
      }
    }
  };

  // Apply metric-specific customizations
  switch (metricType) {
    case 'pipeline':
      return {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          tooltip: {
            ...baseOptions.plugins.tooltip,
            callbacks: {
              label: (context: any) => {
                const label = context.dataset.label || '';
                const value = context.parsed.y;
                return `${label}: ${formatNumber(value)}`;
              }
            }
          }
        },
        scales: {
          ...baseOptions.scales,
          y: {
            ...baseOptions.scales.y,
            title: {
              display: true,
              text: 'Count'
            }
          }
        }
      };
      
    case 'quality':
      return {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          tooltip: {
            ...baseOptions.plugins.tooltip,
            callbacks: {
              label: (context: any) => {
                const label = context.dataset.label || '';
                const value = context.parsed.y;
                return `${label}: ${formatPercentage(value / 100)}`;
              }
            }
          }
        },
        scales: {
          ...baseOptions.scales,
          y: {
            ...baseOptions.scales.y,
            title: {
              display: true,
              text: 'Quality Score (%)'
            },
            ticks: {
              callback: (value: number) => `${value}%`
            }
          }
        }
      };
      
    case 'healing':
      return {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          tooltip: {
            ...baseOptions.plugins.tooltip,
            callbacks: {
              label: (context: any) => {
                const label = context.dataset.label || '';
                const value = context.parsed.y;
                return `${label}: ${formatPercentage(value / 100)}`;
              }
            }
          }
        },
        scales: {
          ...baseOptions.scales,
          y: {
            ...baseOptions.scales.y,
            title: {
              display: true,
              text: 'Self-Healing Rate (%)'
            },
            ticks: {
              callback: (value: number) => `${value}%`
            }
          }
        }
      };
      
    case 'performance':
      return {
        ...baseOptions,
        plugins: {
          ...baseOptions.plugins,
          tooltip: {
            ...baseOptions.plugins.tooltip,
            callbacks: {
              label: (context: any) => {
                const label = context.dataset.label || '';
                const value = context.parsed.y;
                return `${label}: ${formatNumber(value)}ms`;
              }
            }
          }
        },
        scales: {
          ...baseOptions.scales,
          y: {
            ...baseOptions.scales.y,
            title: {
              display: true,
              text: 'Processing Time (ms)'
            }
          }
        }
      };
      
    default:
      return baseOptions;
  }
};

export default MetricsChart;