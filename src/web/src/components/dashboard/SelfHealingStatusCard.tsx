import React from 'react'; // version: ^18.2.0
import Card from '../common/Card'; // Base card component for consistent styling
import GaugeChart from '../charts/GaugeChart'; // Chart component for visualizing the auto-fix percentage
import { useDashboard } from '../../contexts/DashboardContext'; // Hook for accessing dashboard data and loading state
import { SelfHealingMetrics, SelfHealingStatusCardProps } from '../../types/dashboard'; // Type definition for self-healing metrics data
import { formatPercentage, formatInteger } from '../../utils/formatting'; // Utility function for formatting percentage values
import { Box, Typography, Grid } from '@mui/material'; // Layout component for structuring content
import { useTheme } from '@mui/material/styles'; // Hook for accessing theme values
import { useNavigate } from 'react-router-dom'; // Hook for navigation to detailed self-healing view
import { colors } from '../../theme/colors'; // Theme colors for consistent styling

/**
 * Card component that displays self-healing metrics and status
 */
const SelfHealingStatusCard: React.FC<SelfHealingStatusCardProps> = ({
  className,
  loading = false,
  metrics,
  onClick,
}) => {
  // Access dashboard data and loading state using the useDashboard hook
  const { dashboardData, loading: dashboardLoading } = useDashboard();

  // Access theme values for styling
  const theme = useTheme();

  // Access the navigate function for routing
  const navigate = useNavigate();

  /**
   * Handles click event on the card to navigate to the self-healing page
   */
  const handleCardClick = () => {
    navigate('/healing');
  };

  // Determine the data source: props or dashboard context
  const data = metrics || dashboardData?.selfHealing;

  // Define gauge chart thresholds based on auto-fix percentage
  const gaugeThresholds = {
    0: colors.status.error,
    50: colors.status.warning,
    75: colors.status.success,
  };

  return (
    <Card
      title="Self-Healing"
      className={className}
      loading={loading || dashboardLoading}
      onClick={onClick || handleCardClick}
      contentProps={{
        style: {
          display: 'flex',
          flexDirection: 'column',
          height: '100%',
        },
      }}
    >
      {/* Container for the gauge chart */}
      <Box sx={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        marginBottom: '16px',
      }}>
        <GaugeChart
          value={data?.autoFixPercentage || 0}
          thresholds={gaugeThresholds}
          height={150}
          valueFormat="percentage"
        />
      </Box>

      {/* Grid for displaying issue count metrics */}
      <Grid container spacing={2} sx={{ marginTop: 'auto' }}>
        <Grid item xs={6} sx={{ textAlign: 'center' }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold', fontSize: '1.25rem' }}>
            {formatInteger(data?.totalIssues)}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
            Total Issues
          </Typography>
        </Grid>
        <Grid item xs={6} sx={{ textAlign: 'center' }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold', fontSize: '1.25rem' }}>
            {formatPercentage(data?.autoFixPercentage, 0)}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
            Auto-Fixed
          </Typography>
        </Grid>
        <Grid item xs={6} sx={{ textAlign: 'center' }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold', fontSize: '1.25rem' }}>
            {formatInteger(data?.manualFixCount)}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
            Manual Fixes
          </Typography>
        </Grid>
        <Grid item xs={6} sx={{ textAlign: 'center' }}>
          <Typography variant="h6" sx={{ fontWeight: 'bold', fontSize: '1.25rem' }}>
            {formatInteger(data?.pendingCount)}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.875rem' }}>
            Pending
          </Typography>
        </Grid>
      </Grid>
    </Card>
  );
};

export default SelfHealingStatusCard;