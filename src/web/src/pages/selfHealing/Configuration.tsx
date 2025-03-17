import React, { useState, useEffect } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Divider,
  Alert,
} from '@mui/material'; // @mui/material ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0
import { useTranslation } from 'react-i18next'; // react-i18next ^12.0.0

import MainLayout from '../../components/layout/MainLayout';
import HealingSettingsForm from '../../components/selfHealing/HealingSettingsForm';
import Card from '../../components/common/Card';
import Breadcrumbs from '../../components/common/Breadcrumbs';
import { ROUTES } from '../../routes/routes';
import { useAuth } from '../../hooks/useAuth';

/**
 * Component for the Self-Healing Configuration page
 * @returns {JSX.Element} Rendered Configuration page with healing settings form
 */
const Configuration: React.FC = () => {
  // Initialize navigate function from useNavigate hook
  const navigate = useNavigate();

  // Get translation function from useTranslation hook
  const { t } = useTranslation();

  // Check if user has permission to configure self-healing settings
  const { hasPermission } = useAuth();
  const canManageHealing = hasPermission('MANAGE_HEALING');

  // Initialize state for settings updated notification
  const [settingsUpdated, setSettingsUpdated] = useState<boolean>(false);

  /**
   * Function to handle settings update completion
   */
  const handleSettingsUpdated = () => {
    setSettingsUpdated(true);
  };

  // Effect to reset the settings updated notification after a delay
  useEffect(() => {
    if (settingsUpdated) {
      const timer = setTimeout(() => {
        setSettingsUpdated(false);
      }, 5000);

      return () => clearTimeout(timer);
    }
  }, [settingsUpdated]);

  return (
    <MainLayout>
      {/* Render Breadcrumbs for navigation hierarchy */}
      <Breadcrumbs
        items={[
          { path: ROUTES.DASHBOARD, label: t('Dashboard') },
          { path: ROUTES.CONFIGURATION, label: t('Self-Healing Configuration'), isLast: true },
        ]}
      />

      {/* Render page title and description */}
      <Box sx={{ mb: 2 }}>
        <Typography variant="h4" component="h1">
          {t('Self-Healing Configuration')}
        </Typography>
        <Typography variant="body1">
          {t('Configure the behavior of the self-healing system.')}
        </Typography>
      </Box>

      <Divider sx={{ mb: 3 }} />

      {/* If user has permission, render HealingSettingsForm in a Card */}
      {canManageHealing ? (
        <Card title={t('Healing Settings')}>
          <HealingSettingsForm onSettingsUpdated={handleSettingsUpdated} />
          {settingsUpdated && (
            <Alert severity="success" sx={{ mt: 2 }}>
              {t('Settings updated successfully!')}
            </Alert>
          )}
        </Card>
      ) : (
        // If user doesn't have permission, render access denied message
        <Alert severity="warning">
          {t('You do not have permission to configure self-healing settings.')}
        </Alert>
      )}
    </MainLayout>
  );
};

export default Configuration;