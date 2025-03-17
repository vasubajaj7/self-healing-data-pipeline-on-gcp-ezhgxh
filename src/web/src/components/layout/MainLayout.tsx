import React, { useState, useEffect, useCallback } from 'react';
import { Box, CssBaseline } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import useMediaQuery from '@mui/material/useMediaQuery'; // @mui/material ^5.11.0
import { useLocation } from 'react-router-dom'; // react-router-dom ^6.6.1
import Header from './Header';
import Sidebar from './Sidebar';
import Footer from './Footer';
import PageContainer from './PageContainer';
import { useThemeContext } from '../../contexts/ThemeContext';
import { useAuth } from '../../hooks/useAuth';

// Define the MainLayoutProps interface
interface MainLayoutProps {
  children: React.ReactNode;
}

// Styled component for the layout container
const LayoutContainer = styled(Box)({
  display: 'flex',
  flexDirection: 'column',
  minHeight: '100vh',
});

// Styled component for the main content area
const MainContent = styled(Box)(({ theme }) => ({
  flexGrow: 1,
  marginLeft: 240, // Default width of the sidebar
  transition: theme.transitions.create('margin', {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.leavingScreen,
  }),
  [theme.breakpoints.down('md')]: {
    marginLeft: 0, // No margin on smaller screens
  },
}));

/**
 * Main layout component that provides the overall application structure
 */
const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
  // Access the theme context to determine the current theme
  const { theme } = useThemeContext();

  // Access the authentication context to check if the user is authenticated
  const { isAuthenticated } = useAuth();

  // Get the current location using the useLocation hook from React Router
  const location = useLocation();

  // Determine if the viewport is a mobile device using Material-UI's useMediaQuery hook
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));

  // State to manage whether the sidebar is collapsed or expanded
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(isMobile ? true : false);

  /**
   * Toggles the collapsed state of the sidebar
   */
  const handleSidebarToggle = useCallback(() => {
    setSidebarCollapsed(!sidebarCollapsed);
  }, [sidebarCollapsed]);

  // Effect to collapse sidebar on mobile when route changes
  useEffect(() => {
    if (isMobile) {
      setSidebarCollapsed(true);
    }
  }, [location.pathname, isMobile]);

  return (
    <LayoutContainer>
      <CssBaseline /> {/* Applies a consistent baseline across different browsers */}
      <Header onSidebarToggle={handleSidebarToggle} /> {/* Application header */}
      <Box sx={{ display: 'flex' }}>
        <Sidebar collapsed={sidebarCollapsed} onToggle={handleSidebarToggle} /> {/* Navigation sidebar */}
        <MainContent sx={{ ml: !sidebarCollapsed ? 240 : 70 }}> {/* Main content area */}
          <PageContainer>
            {children} {/* Render the content passed as children */}
          </PageContainer>
        </MainContent>
      </Box>
      <Footer /> {/* Application footer */}
    </LayoutContainer>
  );
};

export default MainLayout;