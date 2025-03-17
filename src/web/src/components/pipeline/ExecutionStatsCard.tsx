import React, { useState, useEffect, useMemo } from 'react';
import { Box, Typography, Grid, Divider, Tooltip, CircularProgress } from '@mui/material';
import { InfoOutlined } from '@mui/icons-material';
import Card from '../common/Card';
import LineChart from '../charts/LineChart';
import pipelineService from '../../services/api/pipelineService';
import { formatDuration } from '../../utils/date';
import { formatNumber } from '../../utils/formatting';

/**
 * Props for the ExecutionStatsCard component
 */
interface ExecutionStatsCardProps {
  pipelineId?: string;  // ID of the pipeline to show stats for. If not provided, shows stats for all pipelines.
  timeRange?: string;   // Time range for the statistics (e.g., 'day', 'week', 'month')
  className?: string;   // Additional CSS class name
  height?: number | string; // Height of the card
  onRefresh?: () => void; // Callback function when data is refreshed
}

/**
 * Interface for execution statistics data
 */
interface ExecutionStats {
  successRate: number;
  totalExecutions: number;
  successfulExecutions: number;
  failedExecutions: number;
  avgDuration: number; // in seconds
  recordsProcessed: number;
  trendData: Array<{ date: string, successRate: number, avgDuration: number }>;
}

/**
 * Component that displays pipeline execution statistics in a card format
 */
const ExecutionStatsCard: React.FC<ExecutionStatsCardProps> = ({
  pipelineId,
  timeRange = 'week',
  className,
  height = 'auto',
  onRefresh
}) => {
  // State for execution statistics data, loading state, and error message
  const [stats, setStats] = useState<ExecutionStats | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Effect hook to fetch execution statistics
  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true);
      setError(null);
      
      try {
        const response = await pipelineService.getPipelineHealthMetrics({
          pipelineId,
          timeRange
        });
        
        setStats(response.data as unknown as ExecutionStats);
      } catch (err: any) {
        const errorMessage = err?.message || 'Failed to load execution statistics';
        setError(errorMessage);
        console.error('Error fetching execution stats:', err);
      } finally {
        setLoading(false);
        if (onRefresh) {
          onRefresh();
        }
      }
    };
    
    fetchStats();
  }, [pipelineId, timeRange, onRefresh]);
  
  // Memoized chart data preparation
  const chartData = useMemo(() => {
    if (!stats || !stats.trendData || stats.trendData.length === 0) {
      return { data: [] };
    }
    
    // Format the data for the LineChart component
    return {
      data: stats.trendData.map(item => ({
        timestamp: item.date,
        successRate: item.successRate,
        avgDuration: item.avgDuration
      }))
    };
  }, [stats]);
  
  // Get success rate color based on value
  const getSuccessRateColor = (rate: number): string => {
    if (rate >= 95) return '#2e7d32'; // success.dark
    if (rate >= 80) return '#ed6c02'; // warning.main
    return '#d32f2f'; // error.main
  };

  return (
    <Card
      title="Execution Statistics"
      loading={loading}
      error={error}
      height={height}
      className={className}
    >
      <Box sx={styles.statsContainer}>
        {stats ? (
          <>
            <Grid container spacing={2} sx={styles.metricsGrid}>
              <Grid item xs={4}>
                <Box sx={styles.metricItem}>
                  <Typography 
                    variant="h4" 
                    sx={{
                      ...styles.metricValue,
                      color: getSuccessRateColor(stats.successRate)
                    }}
                  >
                    {formatNumber(stats.successRate, 1)}%
                  </Typography>
                  <Typography 
                    variant="body2" 
                    sx={styles.metricLabel}
                  >
                    Success Rate
                    <Tooltip title="Percentage of pipeline executions that completed successfully">
                      <InfoOutlined sx={styles.infoIcon} />
                    </Tooltip>
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={4}>
                <Box sx={styles.metricItem}>
                  <Typography 
                    variant="h4" 
                    sx={styles.metricValue}
                  >
                    {formatNumber(stats.totalExecutions, 0)}
                  </Typography>
                  <Typography 
                    variant="body2" 
                    sx={styles.metricLabel}
                  >
                    Total Executions
                    <Tooltip title={`${stats.successfulExecutions} successful, ${stats.failedExecutions} failed`}>
                      <InfoOutlined sx={styles.infoIcon} />
                    </Tooltip>
                  </Typography>
                </Box>
              </Grid>
              <Grid item xs={4}>
                <Box sx={styles.metricItem}>
                  <Typography 
                    variant="h4" 
                    sx={styles.metricValue}
                  >
                    {formatDuration(stats.avgDuration * 1000)}
                  </Typography>
                  <Typography 
                    variant="body2" 
                    sx={styles.metricLabel}
                  >
                    Avg Duration
                    <Tooltip title="Average time to complete a pipeline execution">
                      <InfoOutlined sx={styles.infoIcon} />
                    </Tooltip>
                  </Typography>
                </Box>
              </Grid>
            </Grid>
            
            <Divider />
            
            <Box sx={styles.chartContainer}>
              <LineChart 
                data={chartData}
                height="100%"
                options={{
                  plugins: {
                    tooltip: {
                      mode: 'index',
                      intersect: false,
                      callbacks: {
                        label: function(context) {
                          const label = context.dataset.label || '';
                          const value = context.parsed.y;
                          
                          if (label.includes('Success')) {
                            return `${label}: ${formatNumber(value, 1)}%`;
                          }
                          return `${label}: ${formatDuration(value * 1000)}`;
                        }
                      }
                    }
                  },
                  scales: {
                    y: {
                      beginAtZero: true,
                      title: {
                        display: true,
                        text: 'Value'
                      }
                    },
                    x: {
                      title: {
                        display: true,
                        text: 'Date'
                      }
                    }
                  }
                }}
                lineTension={0.4}
                pointRadius={3}
              />
            </Box>
          </>
        ) : (
          <Box sx={styles.noData}>
            {loading ? <CircularProgress size={32} /> : 'No execution data available'}
          </Box>
        )}
      </Box>
    </Card>
  );
};

// Styles for the component
const styles = {
  statsContainer: {
    padding: '16px',
    display: 'flex',
    flexDirection: 'column',
    height: '100%'
  },
  metricsGrid: {
    marginBottom: '16px'
  },
  metricItem: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    textAlign: 'center'
  },
  metricValue: {
    fontSize: '1.5rem',
    fontWeight: 'bold',
    marginBottom: '4px'
  },
  metricLabel: {
    fontSize: '0.875rem',
    color: 'text.secondary',
    display: 'flex',
    alignItems: 'center'
  },
  infoIcon: {
    fontSize: '1rem',
    marginLeft: '4px',
    color: 'text.secondary'
  },
  chartContainer: {
    marginTop: '16px',
    height: '200px',
    flexGrow: 1
  },
  noData: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100%',
    color: 'text.secondary'
  },
  successRate: {
    color: 'dynamic-value-based-on-success-rate'
  }
};

export default ExecutionStatsCard;