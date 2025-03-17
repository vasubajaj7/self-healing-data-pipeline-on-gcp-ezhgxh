import React, { useCallback } from 'react'; // react ^18.2.0
import { Box, Typography, Paper, Container } from '@mui/material'; // @mui/material ^5.11.0
import { useNavigate, useLocation } from 'react-router-dom'; // react-router-dom ^6.8.0
import { useTranslation } from 'react-i18next'; // react-i18next ^12.0.0
import MainLayout from '../../components/layout/MainLayout';
import Breadcrumbs from '../../components/common/Breadcrumbs';
import ModelManagementPanel from '../../components/selfHealing/ModelManagementPanel';
import { ROUTES } from '../../routes/routes';
import { useAuth } from '../../hooks/useAuth';

/**
 * Main component for the Model Management page that displays the model management interface
 * @returns Rendered Model Management page
 */
const ModelManagement: React.FC = () => {
  // LD1: Initialize navigate function from useNavigate hook
  const navigate = useNavigate();
  // LD1: Initialize location from useLocation hook
  const location = useLocation();
  // LD1: Get translation function from useTranslation hook
  const { t } = useTranslation();
  // LD1: Check if user has permission to access model management using useAuth
  const { checkPermission } = useAuth();

  // LD1: If user doesn't have permission, redirect to main dashboard
  const hasManageHealingPermission = React.useMemo(() => {
    return checkPermission('MANAGE_HEALING');
  }, [checkPermission]);

  React.useEffect(() => {
    if (!hasManageHealingPermission) {
      navigate(ROUTES.DASHBOARD, {
        replace: true,
        state: {
          accessDenied: true,
          message: 'You do not have permission to access this page.',
        },
      });
    }
  }, [hasManageHealingPermission, navigate]);

  // LD1: Render the page with appropriate layout and components
  return (
    <MainLayout>
      {/* LD1: Include breadcrumbs for navigation */}
      <Breadcrumbs
        items={[
          { path: ROUTES.DASHBOARD, label: t('Dashboard') },
          { path: ROUTES.SELF_HEALING, label: t('Self-Healing') },
          { path: ROUTES.SELF_HEALING, label: t('Models'), isLast: true },
        ]}
      />
      {/* LD1: Render page title and description */}
      <Container maxWidth="xl">
        <Box sx={{ pb: 4 }}>
          <Typography variant="h4" component="h1" gutterBottom>
            {t('AI Model Management')}
          </Typography>
          <Typography variant="body1">
            {t('View, train, and monitor AI models used in the self-healing data pipeline.')}
          </Typography>
        </Box>
        {/* LD1: Render ModelManagementPanel as the main content */}
        {hasManageHealingPermission && <ModelManagementPanel />}
      </Container>
    </MainLayout>
  );
};

// IE3: Export the ModelManagement component as the default export
export default ModelManagement;