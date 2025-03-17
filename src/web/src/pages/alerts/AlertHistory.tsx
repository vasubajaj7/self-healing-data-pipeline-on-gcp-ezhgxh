import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Breadcrumbs,
  Link,
  Paper,
  Divider,
  useTheme,
} from '@mui/material'; // @mui/material ^5.11.0
import { History, NotificationsActive } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { Helmet } from 'react-helmet'; // react-helmet ^6.1.0
import { useNavigate, useParams } from 'react-router-dom'; // react-router-dom ^6.6.1

import MainLayout from '../../components/layout/MainLayout';
import HistoricalAlertsTable from '../../components/alert/HistoricalAlertsTable';
import { useAlertContext } from '../../contexts/AlertContext';
import { Alert, AlertFilter } from '../../types/alerts';

/**
 * Page component for viewing historical alerts
 */
const AlertHistoryPage: React.FC = () => {
  // LD1: Use the theme hook to access the current theme
  const theme = useTheme();

  // LD1: Use the alert context hook to access alert functionality
  const {
    setFilters,
    selectAlert
  } = useAlertContext();

  // LD1: Use the useNavigate hook for programmatic navigation
  const navigate = useNavigate();

  // LD1: Use the useParams hook to get any URL parameters
  const params = useParams();

  // LD1: Initialize state for filters using useState
  const [filters, setFiltersState] = useState<AlertFilter>({});

  // LD1: Define handleAlertSelect function to navigate to alert details
  const handleAlertSelect = useCallback((alert: Alert) => {
    selectAlert(alert);
    navigate(`/alerts/${alert.alertId}`); // Ensure alertId is correctly accessed
  }, [selectAlert, navigate]);

  // LD1: Define handleFilterChange function to update the filter state
  const handleFilterChange = (newFilters: AlertFilter) => {
    setFilters(newFilters);
    setFiltersState(newFilters);
  };

  // LD1: Render the page with MainLayout wrapper
  return (
    <MainLayout>
      {/* LD1: Include Helmet for setting page title and metadata */}
      <Helmet>
        <title>Alert History - Self-Healing Data Pipeline</title>
        <meta name="description" content="View historical alerts and their resolutions." />
      </Helmet>

      <Box sx={{ mb: 4 }}>
        {/* LD1: Render breadcrumbs navigation */}
        <Breadcrumbs aria-label="breadcrumb">
          <Link underline="hover" color="inherit" href="/">
            Dashboard
          </Link>
          <Typography color="text.primary">Alert History</Typography>
        </Breadcrumbs>
      </Box>

      <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
        {/* LD1: Render page title with history icon */}
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <History sx={{ mr: 1, color: theme.palette.text.secondary }} />
          <Typography variant="h6" component="h2">
            Alert History
          </Typography>
        </Box>

        {/* LD1: Render description text explaining the purpose of the alert history page */}
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Review past incidents, their resolutions, and identify patterns over time.
        </Typography>

        <Divider sx={{ mb: 2 }} />

        {/* LD1: Render the HistoricalAlertsTable component with props for filters and alert selection handler */}
        <HistoricalAlertsTable
          onAlertSelect={handleAlertSelect}
          initialFilters={filters}
          className="historical-alerts-table"
        />
      </Paper>
    </MainLayout>
  );
};

// LD3: Export the AlertHistoryPage component as the default export
export default AlertHistoryPage;