import React from 'react'; // react ^18.2.0
import { Box, Typography, Grid, Divider } from '@mui/material'; // @mui/material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0
import Card from '../common/Card';
import GaugeChart from '../charts/GaugeChart';
import { useDashboard } from '../../contexts/DashboardContext';
import { DataQualityMetrics } from '../../types/dashboard';
import { formatPercentage } from '../../utils/formatting';

/**
 * Props for the DataQualityCard component
 */
interface DataQualityCardProps {
  className?: string;
  loading?: boolean;
  error?: Error | null;
  metrics?: DataQualityMetrics;
  onClick?: () => void;
}

/**
 * Card component that displays data quality metrics with a gauge chart and rule status breakdown
 */
const DataQualityCard: React.FC<DataQualityCardProps> = ({
  className,
  loading = false,
  error = null,
  metrics,
  onClick,
}) => {
  // Access dashboard data including data quality metrics
  const { dashboardData } = useDashboard();
  const dataQualityMetrics = metrics || dashboardData?.dataQuality;

  // Access theme colors for styling
  const theme = useTheme();

  // Access navigation function for routing
  const navigate = useNavigate();

  // Define gauge chart thresholds based on quality percentage ranges
  const gaugeThresholds = {
    0: theme.palette.error.main,
    70: theme.palette.warning.main,
    90: theme.palette.success.main,
  };

  // Define card styles
  const cardStyles = {
    height: '100%',
    cursor: 'pointer',
  };

  // Define gauge container styles
  const gaugeContainer = {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    padding: '16px 0',
  };

  // Define rules grid styles
  const rulesGrid = {
    marginTop: '16px',
  };

  // Define rule item styles
  const ruleItem = {
    textAlign: 'center',
  };

  // Define rule count styles
  const ruleCount = {
    fontWeight: 'bold',
    fontSize: '1.5rem',
  };

  // Define rule label styles
  const ruleLabel = {
    fontSize: '0.875rem',
    color: 'text.secondary',
  };

  // Define color mapping for rule statuses
  const colorMapping = {
    passing: 'success.main',
    failing: 'error.main',
    warning: 'warning.main',
  };

  /**
   * Handles click events on the card to navigate to data quality page
   */
  const handleCardClick = () => {
    // Use navigate function to redirect to the data quality page
    navigate('/quality');
    // Optionally pass any filter parameters to pre-filter the quality view
  };

  // Render Card component with title 'Data Quality'
  return (
    <Card
      title="Data Quality"
      className={className}
      style={cardStyles}
      loading={loading}
      error={error}
      onClick={handleCardClick}
    >
      {/* Show loading state if loading is true */}
      {loading && <Typography>Loading data quality metrics...</Typography>}

      {/* Show error state if error is not null */}
      {error && <Typography color="error">Error: {error.message}</Typography>}

      {/* Render GaugeChart with passPercentage value */}
      {dataQualityMetrics && (
        <Box sx={gaugeContainer}>
          <GaugeChart
            value={dataQualityMetrics.passPercentage}
            thresholds={gaugeThresholds}
            valueFormat="percentage"
          />
        </Box>
      )}

      {/* Render Grid with rule counts (passing, failing, warning) */}
      {dataQualityMetrics && (
        <Grid container spacing={2} sx={rulesGrid}>
          <Grid item xs={4} sx={ruleItem}>
            <Typography sx={ruleCount} color={colorMapping.passing}>
              {dataQualityMetrics.passingRules}
            </Typography>
            <Typography sx={ruleLabel}>Passing</Typography>
          </Grid>
          <Grid item xs={4} sx={ruleItem}>
            <Typography sx={ruleCount} color={colorMapping.failing}>
              {dataQualityMetrics.failingRules}
            </Typography>
            <Typography sx={ruleLabel}>Failing</Typography>
          </Grid>
          <Grid item xs={4} sx={ruleItem}>
            <Typography sx={ruleCount} color={colorMapping.warning}>
              {dataQualityMetrics.warningRules}
            </Typography>
            <Typography sx={ruleLabel}>Warning</Typography>
          </Grid>
        </Grid>
      )}
    </Card>
  );
};

export default DataQualityCard;