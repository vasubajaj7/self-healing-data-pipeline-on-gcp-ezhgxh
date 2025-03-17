import React, { useEffect } from 'react'; // React library and hooks for component creation and lifecycle management // version: ^18.2.0
import {
  Box,
  Typography,
  Breadcrumbs,
  Link,
  useTheme,
} from '@mui/material'; // Material-UI components for layout and UI elements // version: ^5.11.0
import { NotificationsActive } from '@mui/icons-material'; // Material-UI icon for alert notifications // version: ^5.11.0
import { Helmet } from 'react-helmet'; // Component for managing document head for SEO and page title // version: ^6.1.0
import MainLayout from '../components/layout/MainLayout'; // Main layout component that provides the application structure
import AlertDashboard from '../components/alert/AlertDashboard'; // Component that contains the alert dashboard functionality
import { useAlertContext } from '../contexts/AlertContext'; // Hook for accessing alert management functionality // path: src/web/src/contexts/AlertContext.tsx
import { useApi } from '../hooks/useApi'; // Hook for making API requests with loading and error states // path: src/web/src/hooks/useApi.ts

/**
 * Main page component for the alert management functionality
 */
const Alerting: React.FC = () => {
  // Use the theme hook to access the current theme
  const theme = useTheme();

  // Use the alert context hook to access alert functionality
  const { fetchAlerts, fetchAlertStats } = useAlertContext();

  // Use the API hook for any additional API requests
  const {  } = useApi();

  // Set up an effect to fetch initial alert data on component mount
  useEffect(() => {
    fetchAlerts();
    fetchAlertStats();
  }, [fetchAlerts, fetchAlertStats]);

  // Render the page with MainLayout wrapper
  return (
    <MainLayout>
      {/* Include Helmet for setting page title and metadata */}
      <Helmet>
        <title>Alerts | Self-Healing Data Pipeline</title>
        <meta name="description" content="Monitor and manage alerts for the self-healing data pipeline" />
      </Helmet>

      {/* Render breadcrumbs navigation */}
      <Box sx={{ padding: theme.spacing(3), maxWidth: '100%' }}>
        <Box sx={{ marginBottom: theme.spacing(2) }}>
          <Breadcrumbs aria-label="breadcrumb">
            <Link underline="hover" color="inherit" href="/">
              Dashboard
            </Link>
            <Typography color="text.primary">Alerts</Typography>
          </Breadcrumbs>
        </Box>

        {/* Render page title with alert icon */}
        <Box sx={{ display: 'flex', alignItems: 'center', marginBottom: theme.spacing(3) }}>
          <NotificationsActive sx={{ marginRight: theme.spacing(1), color: theme.palette.primary.main }} />
          <Typography variant="h4">Alert Management</Typography>
        </Box>

        {/* Render the AlertDashboard component */}
        <AlertDashboard />
      </Box>
    </MainLayout>
  );
};

export default Alerting;