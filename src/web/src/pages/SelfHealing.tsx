import React, { useEffect } from 'react'; // react ^18.2.0
import { Box, Typography, Paper, Tabs, Tab } from '@mui/material'; // @mui/material ^5.11.0
import { useNavigate, useLocation, Routes, Route, Navigate, Outlet } from 'react-router-dom'; // react-router-dom ^6.8.0
import { useTranslation } from 'react-i18next'; // react-i18next ^12.0.0
import MainLayout from '../components/layout/MainLayout';
import HealingDashboard from '../components/selfHealing/HealingDashboard';
import { ROUTES } from '../routes/routes';
import { useAuth } from '../hooks/useAuth';
import Breadcrumbs from '../components/common/Breadcrumbs';

/**
 * Main component for the Self-Healing page that handles routing to sub-pages and displays the dashboard
 * @returns Rendered Self-Healing page with routing
 */
const SelfHealing: React.FC = () => {
  // LD1: Initialize navigate function from useNavigate hook
  const navigate = useNavigate();
  // LD1: Initialize location from useLocation hook
  const location = useLocation();
  // LD1: Get translation function from useTranslation hook
  const { t } = useTranslation();
  // LD1: Check if user has permission to access self-healing features using useAuth
  const { checkPermission } = useAuth();

  // LD1: Determine if current path is the main self-healing path or a sub-path
  const isMainPath = location.pathname === ROUTES.SELF_HEALING;

  // LD1: Effect to handle path changes and update UI accordingly
  useEffect(() => {
    // LD1: If user doesn't have permission, redirect to dashboard with access denied message
    if (!checkPermission('VIEW_HEALING')) {
      navigate(ROUTES.DASHBOARD, { state: { message: 'Access denied' } });
    }
  }, [checkPermission, navigate, location.pathname]);

  // LD1: Render MainLayout as the container
  return (
    <MainLayout>
      {/* LD1: Render Breadcrumbs for navigation hierarchy */}
      <Breadcrumbs />

      {/* LD1: Render page title and description */}
      <Box sx={{ mb: 2 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          {t('Self-Healing')}
        </Typography>
        <Typography variant="body1">
          {t('View and manage the self-healing capabilities of the data pipeline.')}
        </Typography>
      </Box>

      {/* LD1: If on main path, render HealingDashboard */}
      {isMainPath ? (
        <HealingDashboard />
      ) : (
        // LD1: If on sub-path, render Routes with Route components for sub-pages
        <Routes>
          {/* LD1: Include routes for ActivityLog, ModelManagement, and Configuration sub-pages */}
          <Route path="activity-log" element={<Typography>Activity Log Content</Typography>} />
          <Route path="models" element={<Typography>Model Management Content</Typography>} />
          <Route path="config" element={<Typography>Configuration Content</Typography>} />

          {/* LD1: Redirect to main dashboard if path is not recognized */}
          <Route path="*" element={<Navigate to={ROUTES.SELF_HEALING} replace />} />
        </Routes>
      )}
    </MainLayout>
  );
};

// LD3: Export the SelfHealing component as the default export
export default SelfHealing;