import React, { useCallback } from 'react'; // react ^18.2.0
import { Navigate } from 'react-router-dom'; // react-router-dom ^6.6.1
import { Box, Typography } from '@mui/material'; // @mui/material ^5.11.0

import MainLayout from '../components/layout/MainLayout';
import ConfigDashboard from '../components/config/ConfigDashboard';
import { useAuth } from '../hooks/useAuth';
import { UserPermission } from '../types/user';

/**
 * Main configuration page component that renders the configuration dashboard
 * @returns {JSX.Element} The rendered configuration page
 */
const Configuration: React.FC = () => {
  // IE1: Access the useAuth hook to check user permissions
  const { checkPermission } = useAuth();

  // IE1: Check if the user has the MANAGE_CONFIGURATION permission
  const hasManageConfigurationPermission = checkPermission(UserPermission.MANAGE_CONFIGURATION);

  // IE1: If the user doesn't have the MANAGE_CONFIGURATION permission, redirect to the dashboard
  if (!hasManageConfigurationPermission) {
    return <Navigate to="/" replace />;
  }

  // LD1: Render MainLayout with ConfigDashboard component
  return (
    <MainLayout>
      <ConfigDashboard />
    </MainLayout>
  );
};

// O3: Export the Configuration component as the default export
export default Configuration;