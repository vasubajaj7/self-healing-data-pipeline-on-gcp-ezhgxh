import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { Box, Typography, Paper, Alert, Divider } from '@mui/material'; // @mui/material ^5.11.0
import { People, Security, Settings } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { useNavigate, useLocation } from 'react-router-dom'; // react-router-dom ^6.6.1
import MainLayout from '../components/layout/MainLayout';
import PageContainer from '../components/layout/PageContainer';
import UserManagement from '../components/administration/UserManagement';
import RoleManagement from '../components/administration/RoleManagement';
import SystemSettings from '../components/administration/SystemSettings';
import Tabs from '../components/common/Tabs';
import { useAuth } from '../hooks/useAuth';
import { UserPermission } from '../types/user';

interface AdminTab {
  id: string;
  label: string;
  icon: React.ReactNode;
  content: React.ReactNode;
  path: string;
  permission: UserPermission;
}

/**
 * Main component for the administration page
 */
const Administration: React.FC = () => {
  // LD1: Initialize state for active tab index
  const [activeTab, setActiveTab] = useState(0);

  // LD1: Get navigation and location hooks from React Router
  const navigate = useNavigate();
  const location = useLocation();

  // LD1: Get authentication context using useAuth hook
  const { checkPermission } = useAuth();

  // LD1: Check if user has VIEW_ADMIN permission
  const hasAdminPermission = checkPermission(UserPermission.VIEW_ADMIN);

  // LD1: Handle tab change by updating active tab state and updating URL
  const handleTabChange = useCallback(
    (index: number) => {
      setActiveTab(index);
      const tabPath = tabs[index].path;
      navigate(`/admin/${tabPath}`);
    },
    [navigate]
  );

  // LD1: Parse tab from URL on component mount
  useEffect(() => {
    const pathSegments = location.pathname.split('/');
    const tabPath = pathSegments[pathSegments.length - 1];

    const tabIndex = tabs.findIndex((tab) => tab.path === tabPath);
    if (tabIndex !== -1) {
      setActiveTab(tabIndex);
    }
  }, [location.pathname]);

  // LD1: Define tab configurations for Users, Roles, and Settings
  const tabs: AdminTab[] = [
    {
      id: 'users',
      label: 'Users',
      icon: <People />,
      content: <UserManagement />,
      path: 'users',
      permission: UserPermission.MANAGE_USERS,
    },
    {
      id: 'roles',
      label: 'Roles',
      icon: <Security />,
      content: <RoleManagement />,
      path: 'roles',
      permission: UserPermission.MANAGE_ROLES,
    },
    {
      id: 'settings',
      label: 'Settings',
      icon: <Settings />,
      content: <SystemSettings />,
      path: 'settings',
      permission: UserPermission.MANAGE_SYSTEM,
    },
  ].filter((tab) => checkPermission(tab.permission));

  // LD1: Render access denied message if user lacks permission
  if (!hasAdminPermission) {
    return (
      <MainLayout>
        <PageContainer>
          <Alert severity="error">
            You do not have permission to view this page.
          </Alert>
        </PageContainer>
      </MainLayout>
    );
  }

  // LD1: Render page title and description
  return (
    <MainLayout>
      <PageContainer>
        <Typography variant="h4" component="h1" gutterBottom>
          Administration
        </Typography>
        <Typography variant="body1" paragraph>
          Manage users, roles, and system settings.
        </Typography>
        <Divider sx={{ mb: 3 }} />

        {/* LD1: Render Tabs component with defined tabs */}
        <Tabs
          tabs={tabs}
          activeTab={activeTab}
          onTabChange={handleTabChange}
          aria-label="administration tabs"
        />
      </PageContainer>
    </MainLayout>
  );
};

// IE3: Export the Administration component as the default export
export default Administration;