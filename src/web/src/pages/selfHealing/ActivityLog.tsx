import React, { useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Paper,
  Breadcrumbs,
  Link
} from '@mui/material'; // @mui/material ^5.11.0
import { NavigateNext } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { Link as RouterLink, useNavigate } from 'react-router-dom'; // react-router-dom ^6.6.1
import MainLayout from '../../components/layout/MainLayout';
import HealingActivityLog from '../../components/selfHealing/HealingActivityLog';

/**
 * Page component that displays the self-healing activity log
 * @returns Rendered activity log page
 */
const ActivityLog: React.FC = () => {
  // LD1: Get navigate function from React Router
  const navigate = useNavigate();

  // LD1: Define handleActivityClick function to navigate to healing execution details when an activity is clicked
  const handleActivityClick = useCallback((healingId: string) => {
    navigate(`/self-healing/activity/${healingId}`);
  }, [navigate]);

  // LD1: Render the page with MainLayout wrapper
  return (
    <MainLayout>
      {/* LD1: Include breadcrumb navigation */}
      <Box sx={{
        padding: 3,
        maxWidth: '100%'
      }}>
        <Box sx={{
          marginBottom: 2
        }}>
          <Breadcrumbs separator={<NavigateNext fontSize="small" />} aria-label="breadcrumb">
            <Link component={RouterLink} to="/dashboard" underline="hover" color="inherit">
              Dashboard
            </Link>
            <Link component={RouterLink} to="/self-healing" underline="hover" color="inherit">
              Self-Healing
            </Link>
            <Typography color="text.primary">Activity Log</Typography>
          </Breadcrumbs>
        </Box>

        {/* LD1: Render page title and description */}
        <Box sx={{
          marginBottom: 3
        }}>
          <Typography variant="h4" component="h1" sx={{
            fontWeight: 'bold',
            marginBottom: 1
          }}>
            Activity Log
          </Typography>
          <Typography variant="body1" color="text.secondary" sx={{
            marginBottom: 3
          }}>
            A comprehensive log of all self-healing actions in the data pipeline.
          </Typography>
        </Box>

        {/* LD1: Render HealingActivityLog component with full-page configuration */}
        <Paper elevation={3} sx={{
          padding: 0,
          overflow: 'hidden'
        }}>
          <HealingActivityLog
            showFilters={true}
            height='calc(100vh - 250px)'
            initialPageSize={25}
            autoRefresh={false}
            onActivityClick={handleActivityClick}
          />
        </Paper>
      </Box>
    </MainLayout>
  );
};

// LD3: Export the ActivityLog component as the default export
export default ActivityLog;