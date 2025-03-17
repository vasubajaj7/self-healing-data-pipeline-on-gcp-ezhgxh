import React, { useState, useEffect } from 'react';
import { Box, Typography, Grid, Divider, Tooltip, Skeleton } from '@mui/material';

// Custom components
import Card from '../common/Card';
import Table from '../common/Table';
import Badge from '../common/Badge';
import LineChart from '../charts/LineChart';
import BarChart from '../charts/BarChart';

// Types and utilities
import { ColumnQualityDetails } from '../../types/quality';
import { formatPercentage, formatNumber } from '../../utils/formatting';
import { transformDistributionData, transformTimeSeriesData } from '../../services/charts/dataTransformers';
import qualityService from '../../services/api/qualityService';

/**
 * Props interface for the ColumnQualityDetails component
 */
interface ColumnQualityDetailsProps {
  /** Dataset name containing the column */
  dataset: string;
  /** Table name containing the column */
  table: string;
  /** Column name to display details for */
  column: string;
  /** Pre-fetched column quality data (if available) */
  data?: ColumnQualityDetails;
  /** Whether the component is in loading state */
  loading?: boolean;
  /** Error message if data fetching failed */
  error?: Error | string | null;
  /** Callback when a quality issue is clicked */
  onIssueClick?: (issueId: string) => void;
  /** Callback when a quality rule is clicked */
  onRuleClick?: (ruleId: string) => void;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Component that displays detailed quality metrics for a specific column
 */
const ColumnQualityDetails: React.FC<ColumnQualityDetailsProps> = ({
  dataset,
  table,
  column,
  data: initialData,
  loading: initialLoading = false,
  error: initialError = null,
  onIssueClick,
  onRuleClick,
  className
}) => {
  // State for column quality data
  const [data, setData] = useState<ColumnQualityDetails | undefined>(initialData);
  const [loading, setLoading] = useState<boolean>(initialLoading || !initialData);
  const [error, setError] = useState<Error | string | null>(initialError);

  // Fetch column quality data if not provided as props
  useEffect(() => {
    if (!initialData && !initialLoading) {
      setLoading(true);
      qualityService.getColumnQualityDetails(dataset, table, column)
        .then((response) => {
          setData(response.data);
          setError(null);
        })
        .catch((err) => {
          setError(err instanceof Error ? err : String(err));
          setData(undefined);
        })
        .finally(() => {
          setLoading(false);
        });
    }
  }, [dataset, table, column, initialData, initialLoading]);

  // Update state when props change
  useEffect(() => {
    if (initialData !== undefined) {
      setData(initialData);
    }
    setLoading(initialLoading);
    setError(initialError);
  }, [initialData, initialLoading, initialError]);

  // Handle error state
  if (error) {
    return (
      <Card
        error={error}
        title={`Column: ${column}`}
        className={className}
      />
    );
  }

  return (
    <Box className={className}>
      <Typography variant="h5" component="h2" sx={{ mb: 2 }}>
        Column: {column}
      </Typography>
      
      <Grid container spacing={3}>
        {/* Column Overview */}
        <Grid item xs={12} md={6}>
          <Card 
            title="Overview" 
            loading={loading}
            sx={{ height: '100%' }}
          >
            {loading ? (
              <Box sx={{ p: 2 }}>
                <Skeleton variant="text" width="60%" height={30} />
                <Skeleton variant="text" width="40%" height={30} />
                <Skeleton variant="text" width="70%" height={30} />
              </Box>
            ) : data && (
              <Box sx={{ p: 2 }}>
                <Typography variant="body1" gutterBottom>
                  <strong>Data Type:</strong> {data.dataType}
                </Typography>
                <Typography variant="body1" gutterBottom>
                  <strong>Null Values:</strong> {formatPercentage(data.nullPercentage)}
                </Typography>
                <Typography variant="body1" gutterBottom>
                  <strong>Unique Values:</strong> {formatPercentage(data.uniquePercentage)}
                </Typography>
                <Divider sx={{ my: 2 }} />
                <Box sx={{ mt: 2 }}>
                  <StatisticsCard 
                    statistics={data.statistics} 
                    dataType={data.dataType}
                    loading={loading}
                  />
                </Box>
              </Box>
            )}
          </Card>
        </Grid>

        {/* Top Values Distribution */}
        <Grid item xs={12} md={6}>
          <Card 
            title="Value Distribution" 
            loading={loading}
            sx={{ height: '100%' }}
          >
            {loading ? (
              <Box sx={{ p: 2, height: 250 }}>
                <Skeleton variant="rectangular" height="100%" />
              </Box>
            ) : data && (
              <Box sx={{ p: 2 }}>
                <TopValuesCard 
                  topValues={data.topValues} 
                  loading={loading}
                />
              </Box>
            )}
          </Card>
        </Grid>

        {/* Quality Issues */}
        <Grid item xs={12} md={6}>
          <Card 
            title="Quality Issues" 
            loading={loading}
            sx={{ height: '100%' }}
          >
            {loading ? (
              <Box sx={{ p: 2 }}>
                <Skeleton variant="rectangular" height={200} />
              </Box>
            ) : data && (
              <Box sx={{ p: 2 }}>
                <QualityIssuesCard 
                  qualityIssues={data.qualityIssues} 
                  loading={loading}
                  onIssueClick={onIssueClick}
                />
              </Box>
            )}
          </Card>
        </Grid>

        {/* Applied Rules */}
        <Grid item xs={12} md={6}>
          <Card 
            title="Applied Quality Rules" 
            loading={loading}
            sx={{ height: '100%' }}
          >
            {loading ? (
              <Box sx={{ p: 2 }}>
                <Skeleton variant="rectangular" height={200} />
              </Box>
            ) : data && (
              <Box sx={{ p: 2 }}>
                <AppliedRulesCard 
                  appliedRules={data.appliedRules} 
                  loading={loading}
                  onRuleClick={onRuleClick}
                />
              </Box>
            )}
          </Card>
        </Grid>

        {/* Historical Quality Trend */}
        <Grid item xs={12}>
          <Card 
            title="Historical Quality Trend" 
            loading={loading}
            sx={{ height: '100%' }}
          >
            {loading ? (
              <Box sx={{ p: 2, height: 300 }}>
                <Skeleton variant="rectangular" height="100%" />
              </Box>
            ) : data && data.statistics && data.statistics.history && (
              <Box sx={{ p: 2, height: 300 }}>
                <LineChart 
                  data={transformTimeSeriesData(
                    {
                      name: 'Quality Score',
                      data: data.statistics.history
                    },
                    { 
                      dateFormat: 'MMM d', 
                      smoothing: 0.4, 
                      fillArea: true 
                    }
                  )}
                  height="100%"
                  options={{ 
                    plugins: { 
                      tooltip: { 
                        callbacks: { 
                          label: (context: any) => `Quality: ${formatPercentage(context.raw)}` 
                        } 
                      } 
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        max: 100,
                        title: {
                          display: true,
                          text: 'Quality Score (%)'
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
                />
              </Box>
            )}
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

/**
 * Sub-component that displays statistical information for the column
 */
const StatisticsCard: React.FC<{ 
  statistics: any; 
  dataType: string; 
  loading: boolean 
}> = ({ statistics, dataType, loading }) => {
  if (loading) {
    return (
      <Box>
        <Skeleton variant="text" width="80%" height={30} />
        <Skeleton variant="text" width="60%" height={30} />
        <Skeleton variant="text" width="70%" height={30} />
      </Box>
    );
  }

  if (!statistics) {
    return <Typography variant="body2">No statistics available for this column.</Typography>;
  }

  // Render different statistics based on data type
  const renderStatisticItems = () => {
    const statisticsGridStyle = {
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
      gap: 2
    };

    const statisticItemStyle = {
      display: 'flex', 
      flexDirection: 'column', 
      padding: '8px 0'
    };

    const labelStyle = {
      fontWeight: 500, 
      color: 'text.secondary', 
      fontSize: '0.875rem'
    };

    const valueStyle = {
      fontWeight: 600, 
      fontSize: '1rem'
    };

    if (dataType.toLowerCase().includes('int') || dataType.toLowerCase().includes('float') || dataType.toLowerCase().includes('num')) {
      // Numeric column statistics
      return (
        <Box sx={statisticsGridStyle}>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Minimum</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.min)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Maximum</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.max)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Mean</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.mean, 2)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Median</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.median, 2)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Standard Deviation</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.stdDev, 2)}
            </Typography>
          </Box>
        </Box>
      );
    } else if (dataType.toLowerCase().includes('string') || dataType.toLowerCase().includes('char')) {
      // String column statistics
      return (
        <Box sx={statisticsGridStyle}>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Min Length</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.minLength, 0)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Max Length</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.maxLength, 0)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Avg Length</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.avgLength, 1)}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Empty Strings</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.emptyCount, 0)}
              {statistics.totalCount && (
                <Typography component="span" variant="caption" sx={{ ml: 1 }}>
                  ({formatPercentage(statistics.emptyCount / statistics.totalCount)})
                </Typography>
              )}
            </Typography>
          </Box>
        </Box>
      );
    } else if (dataType.toLowerCase().includes('date') || dataType.toLowerCase().includes('time')) {
      // Date/time column statistics
      return (
        <Box sx={statisticsGridStyle}>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Earliest Date</Typography>
            <Typography sx={valueStyle}>
              {statistics.min}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Latest Date</Typography>
            <Typography sx={valueStyle}>
              {statistics.max}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>Date Range</Typography>
            <Typography sx={valueStyle}>
              {statistics.range}
            </Typography>
          </Box>
        </Box>
      );
    } else if (dataType.toLowerCase().includes('bool')) {
      // Boolean column statistics
      return (
        <Box sx={statisticsGridStyle}>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>True Count</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.trueCount, 0)}
              {statistics.totalCount && (
                <Typography component="span" variant="caption" sx={{ ml: 1 }}>
                  ({formatPercentage(statistics.trueCount / statistics.totalCount)})
                </Typography>
              )}
            </Typography>
          </Box>
          <Box sx={statisticItemStyle}>
            <Typography sx={labelStyle}>False Count</Typography>
            <Typography sx={valueStyle}>
              {formatNumber(statistics.falseCount, 0)}
              {statistics.totalCount && (
                <Typography component="span" variant="caption" sx={{ ml: 1 }}>
                  ({formatPercentage(statistics.falseCount / statistics.totalCount)})
                </Typography>
              )}
            </Typography>
          </Box>
        </Box>
      );
    }

    // Generic statistics for other data types
    return (
      <Box sx={statisticsGridStyle}>
        {Object.entries(statistics).map(([key, value]) => (
          key !== 'history' && (
            <Box key={key} sx={statisticItemStyle}>
              <Typography sx={labelStyle}>{key}</Typography>
              <Typography sx={valueStyle}>
                {typeof value === 'number' ? formatNumber(value as number, 2) : String(value)}
              </Typography>
            </Box>
          )
        ))}
      </Box>
    );
  };

  return (
    <Box>
      {renderStatisticItems()}
    </Box>
  );
};

/**
 * Sub-component that displays the most frequent values in the column
 */
const TopValuesCard: React.FC<{ 
  topValues: Array<{ value: any; count: number; percentage: number }>; 
  loading: boolean 
}> = ({ topValues, loading }) => {
  if (loading) {
    return <Skeleton variant="rectangular" height={200} />;
  }

  if (!topValues || topValues.length === 0) {
    return <Typography variant="body2">No value distribution data available.</Typography>;
  }

  // Transform data for BarChart
  const chartData = transformDistributionData(
    {
      data: topValues.map(item => ({
        label: item.value !== null && item.value !== undefined ? String(item.value) : 'NULL',
        value: item.count,
        percentage: item.percentage
      }))
    },
    {
      sortBy: 'desc',
      maxSegments: 10,
      showPercentages: true,
      colorScheme: 'quality'
    }
  );

  return (
    <Box>
      <BarChart 
        data={chartData}
        height={250}
        options={{
          indexAxis: 'y',
          scales: {
            x: {
              title: {
                display: true,
                text: 'Count'
              }
            },
            y: {
              title: {
                display: true,
                text: 'Value'
              }
            }
          },
          plugins: {
            tooltip: {
              callbacks: {
                label: (context: any) => {
                  const dataIndex = context.dataIndex;
                  const originalValue = topValues[dataIndex];
                  return `Count: ${formatNumber(originalValue.count)}, Percentage: ${formatPercentage(originalValue.percentage)}`;
                }
              }
            },
            legend: {
              display: false
            }
          }
        }}
      />
    </Box>
  );
};

/**
 * Sub-component that displays quality issues related to the column
 */
const QualityIssuesCard: React.FC<{ 
  qualityIssues: any[]; 
  loading: boolean;
  onIssueClick?: (issueId: string) => void;
}> = ({ qualityIssues, loading, onIssueClick }) => {
  if (loading) {
    return <Skeleton variant="rectangular" height={200} />;
  }

  if (!qualityIssues || qualityIssues.length === 0) {
    return <Typography variant="body2">No quality issues found for this column.</Typography>;
  }

  // Define table columns
  const columns = [
    {
      id: 'severity',
      label: 'Severity',
      width: '100px',
      renderCell: (value: string) => {
        let color: 'error' | 'warning' | 'info' = 'info';
        if (value.toLowerCase() === 'high' || value.toLowerCase() === 'critical') {
          color = 'error';
        } else if (value.toLowerCase() === 'medium' || value.toLowerCase() === 'warning') {
          color = 'warning';
        }
        return <Badge label={value} color={color} variant="filled" size="small" />;
      }
    },
    { id: 'description', label: 'Description', width: '40%' },
    {
      id: 'status',
      label: 'Status',
      width: '120px',
      renderCell: (value: string) => {
        let color: 'success' | 'warning' | 'error' | 'info' | 'default' = 'default';
        if (value.toLowerCase().includes('resolved')) {
          color = 'success';
        } else if (value.toLowerCase().includes('progress')) {
          color = 'info';
        } else if (value.toLowerCase().includes('open')) {
          color = 'warning';
        }
        return <Badge label={value} color={color} variant="outlined" size="small" />;
      }
    },
    {
      id: 'healingStatus',
      label: 'Self-Healing',
      width: '150px',
      renderCell: (value: string) => {
        if (!value) return null;
        
        let color: 'success' | 'warning' | 'error' | 'info' | 'default' = 'default';
        
        if (value.toLowerCase().includes('completed')) {
          color = 'success';
        } else if (value.toLowerCase().includes('progress')) {
          color = 'info';
        } else if (value.toLowerCase().includes('failed')) {
          color = 'error';
        } else if (value.toLowerCase().includes('pending')) {
          color = 'warning';
        }
        
        return <Badge label={value} color={color} variant="outlined" size="small" />;
      }
    },
    { 
      id: 'detectedAt', 
      label: 'Detected', 
      width: '150px',
      format: (value: string) => new Date(value).toLocaleString()
    }
  ];

  return (
    <Table
      columns={columns}
      data={qualityIssues}
      onRowClick={onIssueClick ? (row) => onIssueClick(row.issueId) : undefined}
      pagination={false}
      dense={true}
      emptyMessage="No quality issues found for this column."
    />
  );
};

/**
 * Sub-component that displays quality rules applied to the column
 */
const AppliedRulesCard: React.FC<{ 
  appliedRules: any[]; 
  loading: boolean;
  onRuleClick?: (ruleId: string) => void;
}> = ({ appliedRules, loading, onRuleClick }) => {
  if (loading) {
    return <Skeleton variant="rectangular" height={200} />;
  }

  if (!appliedRules || appliedRules.length === 0) {
    return <Typography variant="body2">No quality rules applied to this column.</Typography>;
  }

  // Define table columns
  const columns = [
    { id: 'ruleName', label: 'Rule Name', width: '25%' },
    { 
      id: 'ruleType', 
      label: 'Type',
      width: '120px',
      renderCell: (value: string) => (
        <Tooltip title={value}>
          <Badge label={value} color="info" variant="filled" size="small" />
        </Tooltip>
      )
    },
    { 
      id: 'expectationType', 
      label: 'Expectation', 
      width: '25%' 
    },
    {
      id: 'severity',
      label: 'Severity',
      width: '100px',
      renderCell: (value: string) => {
        let color: 'error' | 'warning' | 'info' = 'info';
        if (value.toLowerCase() === 'high' || value.toLowerCase() === 'critical') {
          color = 'error';
        } else if (value.toLowerCase() === 'medium' || value.toLowerCase() === 'warning') {
          color = 'warning';
        }
        return <Badge label={value} color={color} variant="outlined" size="small" />;
      }
    },
    { 
      id: 'isActive', 
      label: 'Status',
      width: '100px',
      renderCell: (value: boolean) => (
        <Badge 
          label={value ? 'Active' : 'Inactive'} 
          color={value ? 'success' : 'default'} 
          variant="outlined" 
          size="small" 
        />
      )
    }
  ];

  return (
    <Table
      columns={columns}
      data={appliedRules}
      onRowClick={onRuleClick ? (row) => onRuleClick(row.ruleId) : undefined}
      pagination={false}
      dense={true}
      emptyMessage="No quality rules applied to this column."
    />
  );
};

export default ColumnQualityDetails;