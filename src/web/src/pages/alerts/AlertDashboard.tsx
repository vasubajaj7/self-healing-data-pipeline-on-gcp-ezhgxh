import React, { useState, useEffect, useCallback } from 'react'; // React library and hooks for component creation and state management // version: ^18.2.0
import {
  Grid,
  Box,
  Paper,
  Typography,
  Breadcrumbs,
  Link,
  Divider,
  useTheme,
  Snackbar,
  Alert as MuiAlert,
  CircularProgress,
} from '@mui/material'; // Material-UI components for layout and UI elements // version: ^5.11.0
import { NotificationsActive } from '@mui/icons-material'; // Material-UI icon for alert notifications // version: ^5.11.0
import { Helmet } from 'react-helmet'; // Component for managing document head for SEO and page title // version: ^6.1.0
import { useParams, useNavigate } from 'react-router-dom'; // React Router hooks for accessing URL parameters and navigation // version: ^6.6.1
import MainLayout from '../../components/layout/MainLayout'; // Main layout component that provides the application structure
import AlertDashboard from '../../components/alert/AlertDashboard'; // Component that contains the alert dashboard functionality
import { useAlertContext } from '../../contexts/AlertContext'; // Hook for accessing alert management functionality
import { useApi } from '../../hooks/useApi'; // Hook for making API requests with loading and error states
import { AlertFilter } from '../../types/alerts'; // Type definition for alert filtering parameters

/**
 * Interface for the route parameters, specifically for accessing an alert ID.
 */
interface RouteParams {
  alertId?: string;
}

/**
 * Page component for the alert dashboard
 */
const AlertDashboardPage: React.FC = () => {
  // LD1: Use the theme hook to access the current theme
  const theme = useTheme();

  // LD1: Use the alert context hook to access alert functionality
  const { fetchAlerts, fetchAlertStats, setFilters, setPage, setPageSize, setRefreshInterval, selectAlert } = useAlertContext();

  // LD1: Use the API hook for any additional API requests
  const { loading, error } = useApi();

  // LD1: Use the useParams hook to get any URL parameters like alertId
  const { alertId } = useParams<RouteParams>();

  // LD1: Use the useNavigate hook for programmatic navigation
  const navigate = useNavigate();

  // LD1: Initialize state for timeRange and filters
  const [timeRange, setTimeRange] = useState<string>('24h');
  const [filters, setLocalFilters] = useState<AlertFilter>({});

  // LD1: Set up an effect to fetch initial alert data on component mount
  useEffect(() => {
    fetchAlerts();
    fetchAlertStats();
  }, [fetchAlerts, fetchAlertStats]);

  // LD1: Define handleTimeRangeChange function to update the time range
  const handleTimeRangeChange = (newTimeRange: string) => {
    setTimeRange(newTimeRange);
    fetchAlertStats(newTimeRange);
  };

  // LD1: Define handleFilterChange function to update the filters
  const handleFilterChange = (newFilters: AlertFilter) => {
    setLocalFilters(newFilters);
    setFilters(newFilters);
  };

  // LD1: Define handleAlertSelect function to navigate to the selected alert
  const handleAlertSelect = (alert: Alert) => {
    selectAlert(alert);
    navigate(`/alerts/${alert.alertId}`);
  };

  // LD1: Render the page with MainLayout wrapper
  return (
    <MainLayout>
      {/* LD1: Include Helmet for setting page title and metadata */}
      <Helmet>
        <title>Alert Dashboard - Self-Healing Data Pipeline</title>
        <meta name="description" content="Monitor and manage alerts in the self-healing data pipeline." />
      </Helmet>

      {/* LD1: Render breadcrumbs navigation */}
      <Breadcrumbs aria-label="breadcrumb" sx={{ mb: 2 }}>
        <Link underline="hover" color="inherit" href="/">
          Dashboard
        </Link>
        <Typography color="text.primary">Alerts</Typography>
      </Breadcrumbs>

      {/* LD1: Render page title with alert icon */}
      <Box display="flex" alignItems="center" mb={3}>
        <NotificationsActive sx={{ mr: 1, color: theme.palette.primary.main }} />
        <Typography variant="h5" component="h1">
          Alert Dashboard
        </Typography>
      </Box>

      {/* LD1: Render the AlertDashboard component with props for timeRange, filters, and handlers */}
      <AlertDashboard
        timeRange={timeRange}
        initialFilters={filters}
        onTimeRangeChange={handleTimeRangeChange}
        onFilterChange={handleFilterChange}
        onAlertSelect={handleAlertSelect}
      />
    </MainLayout>
  );
};

// IE3: Export the AlertDashboardPage component as the default export
export default AlertDashboardPage;