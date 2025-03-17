import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { Grid, Box, Typography, FormControl, InputLabel, Select, MenuItem, IconButton } from '@mui/material'; // @mui/material ^5.11.0
import { Refresh } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0
import PipelineHealthCard from './PipelineHealthCard';
import DataQualityCard from './DataQualityCard';
import SelfHealingStatusCard from './SelfHealingStatusCard';
import AlertSummaryCard from './AlertSummaryCard';
import RecentExecutionsTable from './RecentExecutionsTable';
import SystemStatusCard from './SystemStatusCard';
import AiInsightsCard from './AiInsightsCard';
import QuickStatsCard from './QuickStatsCard';
import { useDashboard } from '../../contexts/DashboardContext';
import { DashboardFilters, TimeRange } from '../../types/dashboard';

/**
 * @dev Interface defining the props for the DashboardOverview component.
 * @param className - Optional CSS class name for styling.
 */
interface DashboardOverviewProps {
  className?: string;
}

/**
 * @dev Main dashboard component that displays a comprehensive overview of the self-healing data pipeline.
 * @param props - The props for the component.
 * @returns A React element that displays the dashboard overview.
 */
const DashboardOverview: React.FC<DashboardOverviewProps> = ({ className }) => {
  // LD1: Use useDashboard hook to get dashboard data, loading state, filters, and refresh functionality
  const { filters, setFilters, fetchDashboardData } = useDashboard();

  // LD1: Use useState to track last refresh time
  const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);

  // LD1: Use useTheme to get theme values for styling
  const theme = useTheme();

  // LD1: Use useNavigate for navigation to other pages
  const navigate = useNavigate();

  /**
   * @dev Handles changes to the time range filter.
   * @param event - The change event from the time range select.
   * @returns void
   */
  const handleTimeRangeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    // LD1: Extract the new time range value from the event
    const newTimeRange = event.target.value as TimeRange;

    // LD1: Update the filters state with the new time range
    setFilters({
      ...filters,
      timeRange: newTimeRange,
    });
  };

  /**
   * @dev Manually refreshes the dashboard data.
   * @returns void
   */
  const handleRefresh = () => {
    // LD1: Call fetchDashboardData function from useDashboard hook
    fetchDashboardData();

    // LD1: Update the lastRefreshed state with the current timestamp
    setLastRefreshed(new Date());
  };

  /**
   * @dev Navigates to the pipeline management page to view all executions.
   * @returns void
   */
  const handleViewAllExecutions = () => {
    // LD1: Use navigate function to redirect to the pipeline management page
    navigate('/pipeline');
  };

  return (
    <Box className={className} sx={{ padding: '24px' }}>
      {/* LD1: Header section with title and controls */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
        <Typography variant="h5" sx={{ fontSize: '1.5rem', fontWeight: '500' }}>
          Dashboard Overview
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          {/* LD1: Time range filter dropdown */}
          <FormControl sx={{ minWidth: '150px' }} size="small">
            <InputLabel id="time-range-label">Time Range</InputLabel>
            <Select
              labelId="time-range-label"
              id="time-range-select"
              value={filters.timeRange}
              label="Time Range"
              onChange={handleTimeRangeChange}
            >
              <MenuItem value="LAST_HOUR">Last Hour</MenuItem>
              <MenuItem value="LAST_24_HOURS">Last 24 Hours</MenuItem>
              <MenuItem value="LAST_7_DAYS">Last 7 Days</MenuItem>
              <MenuItem value="LAST_30_DAYS">Last 30 Days</MenuItem>
            </Select>
          </FormControl>
          {/* LD1: Refresh button with last refreshed timestamp */}
          <IconButton onClick={handleRefresh} aria-label="refresh" sx={{ marginLeft: '8px' }}>
            <Refresh />
          </IconButton>
          {lastRefreshed && (
            <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.75rem', marginLeft: '8px' }}>
              Last Refreshed: {lastRefreshed.toLocaleTimeString()}
            </Typography>
          )}
        </Box>
      </Box>

      {/* LD1: Grid container for dashboard cards */}
      <Grid container spacing={3} sx={{ marginTop: '16px' }}>
        {/* LD1: Top row cards */}
        <Grid item xs={12} sm={6} md={4} sx={{ height: '100%' }}>
          <PipelineHealthCard />
        </Grid>
        <Grid item xs={12} sm={6} md={4} sx={{ height: '100%' }}>
          <DataQualityCard />
        </Grid>
        <Grid item xs={12} sm={6} md={4} sx={{ height: '100%' }}>
          <SelfHealingStatusCard />
        </Grid>

        {/* LD1: Sidebar cards */}
        <Grid item xs={12} sm={6} md={3} sx={{ height: '100%' }}>
          <QuickStatsCard />
          <SystemStatusCard className="system-status-card" />
        </Grid>

        {/* LD1: Middle section cards */}
        <Grid item xs={12} sm={6} md={6} sx={{ height: '100%' }}>
          <AlertSummaryCard />
          <AiInsightsCard />
        </Grid>

        {/* LD1: Bottom row table spanning multiple columns */}
        <Grid item xs={12} sx={{ height: '100%' }}>
          <RecentExecutionsTable onViewAll={handleViewAllExecutions} />
        </Grid>
      </Grid>
    </Box>
  );
};

export default DashboardOverview;