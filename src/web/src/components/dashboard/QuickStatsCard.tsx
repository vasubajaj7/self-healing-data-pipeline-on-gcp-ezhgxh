import React from 'react'; // react ^18.2.0
import Card from '../../components/common/Card';
import { QuickStats, useDashboard } from '../../contexts/DashboardContext';
import { Box, Typography, Grid } from '@mui/material'; // @mui/material ^5.11.0
import TrendingUp from '@mui/icons-material/TrendingUp'; // @mui/icons-material ^5.11.0
import TrendingDown from '@mui/icons-material/TrendingDown'; // @mui/icons-material ^5.11.0
import TrendingFlat from '@mui/icons-material/TrendingFlat'; // @mui/icons-material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0

/**
 * Props for the QuickStatsCard component
 */
interface QuickStatsCardProps {
  className?: string;
  stats?: QuickStats;
  loading?: boolean;
  onClick?: () => void;
}

/**
 * Returns the appropriate trend icon based on the value
 * @param value 
 * @returns React.ReactNode
 */
const getTrendIcon = (value: number): React.ReactNode => {
  const theme = useTheme(); // Access theme colors for styling

  if (value > 0) {
    return <TrendingUp sx={{ color: theme.palette.success.main }} />;
  } else if (value < 0) {
    return <TrendingDown sx={{ color: theme.palette.error.main }} />;
  } else {
    return <TrendingFlat sx={{ color: theme.palette.text.secondary }} />;
  }
};

/**
 * Formats the trend value as a percentage with sign
 * @param value 
 * @returns string
 */
const formatTrendValue = (value: number): string => {
  const absoluteValue = Math.abs(value);
  const percentage = absoluteValue.toFixed(1);
  return value > 0 ? `+${percentage}%` : `${percentage}%`;
};

/**
 * Card component that displays quick statistics about the pipeline system
 */
const QuickStatsCard: React.FC<QuickStatsCardProps> = ({
  className,
  stats,
  loading = false,
  onClick,
}) => {
  // Use useDashboard hook to get dashboardData and loading state if stats not provided
  const { dashboardData, loading: dashboardLoading } = useDashboard();

  // Use useTheme hook to get theme colors for styling
  const theme = useTheme();

  // Get quickStats from dashboardData if stats not provided directly
  const quickStats = stats || dashboardData?.quickStats;

  // Define default props
  const defaultProps = {
    loading: false,
  };

  // Merge default props with provided props
  const props = {
    ...defaultProps,
    className,
    stats: quickStats,
    loading: loading || dashboardLoading,
    onClick,
  };

  return (
    <Card
      title="Quick Stats"
      className={props.className}
      loading={props.loading}
      onClick={props.onClick}
    >
      <Grid container spacing={2}>
        <Grid item xs={12} sm={4}>
          <Box sx={{ padding: theme.spacing(2), textAlign: 'center' }}>
            <Typography variant="h6" sx={{ fontSize: '1.5rem', fontWeight: 'bold', marginBottom: theme.spacing(1) }}>
              {props.stats?.activePipelines}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
              Active Pipelines
            </Typography>
          </Box>
        </Grid>
        <Grid item xs={12} sm={4}>
          <Box sx={{ padding: theme.spacing(2), textAlign: 'center' }}>
            <Typography variant="h6" sx={{ fontSize: '1.5rem', fontWeight: 'bold', marginBottom: theme.spacing(1) }}>
              {props.stats?.pendingJobs}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
              Pending Jobs
            </Typography>
          </Box>
        </Grid>
        <Grid item xs={12} sm={4}>
          <Box sx={{ padding: theme.spacing(2), textAlign: 'center' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: theme.spacing(0.5) }}>
              {getTrendIcon(props.stats?.alertRateChange || 0)}
              <Typography variant="h6" sx={{ fontSize: '1.5rem', fontWeight: 'bold', marginBottom: theme.spacing(1) }}>
                {formatTrendValue(props.stats?.alertRateChange || 0)}
              </Typography>
            </Box>
            <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
              Alert Rate ({props.stats?.alertRatePeriod})
            </Typography>
          </Box>
        </Grid>
      </Grid>
    </Card>
  );
};

export default QuickStatsCard;