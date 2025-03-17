import React, { useState, useEffect, useCallback } from 'react';
import {
  AppBar,
  Toolbar,
  Typography,
  IconButton,
  Avatar,
  Menu,
  MenuItem,
  Divider,
  Box,
  Badge,
  Tooltip
} from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import {
  Menu as MenuIcon,
  Notifications,
  Settings,
  Brightness4,
  Brightness7,
  AccountCircle
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.6.1
import { useThemeContext } from '../../contexts/ThemeContext';
import { useAuth } from '../../hooks/useAuth';
import { ReactComponent as UserIcon } from '../../assets/icons/user.svg';
import Button from '../common/Button';

/**
 * Interface for notification objects
 */
interface Notification {
  id: string;
  title: string;
  message: string;
  timestamp: Date;
  read: boolean;
  type: string;
}

/**
 * Props for the Header component
 */
interface HeaderProps {
  onSidebarToggle?: () => void;
}

// Custom styled components for the header
const HeaderAppBar = styled(AppBar)(({ theme }) => ({
  zIndex: theme.zIndex.drawer + 1,
  backgroundColor: theme.palette.mode === 'light' ? theme.palette.primary.main : '#1a1a1a',
  boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.1)',
}));

const HeaderToolbar = styled(Toolbar)({
  display: 'flex',
  justifyContent: 'space-between',
  padding: '0 16px',
});

const LogoContainer = styled(Box)({
  display: 'flex',
  alignItems: 'center',
});

const Title = styled(Typography)(({ theme }) => ({
  flexGrow: 1,
  marginLeft: 16,
  color: theme.palette.primary.contrastText,
  fontWeight: 500,
  [theme.breakpoints.down('sm')]: {
    fontSize: '1.2rem',
  },
}));

const ActionContainer = styled(Box)({
  display: 'flex',
  alignItems: 'center',
  gap: '8px',
});

const ProfileMenu = styled(Menu)({
  marginTop: '45px',
});

const NotificationsMenu = styled(Menu)({
  marginTop: '45px',
  maxWidth: '400px',
  maxHeight: '500px',
});

const NotificationItem = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2),
  borderBottom: `1px solid ${theme.palette.divider}`,
  width: '100%',
  minWidth: '300px',
  cursor: 'pointer',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
}));

const UserAvatar = styled(Avatar)(({ theme }) => ({
  backgroundColor: theme.palette.primary.main,
  color: theme.palette.primary.contrastText,
  cursor: 'pointer',
  height: 40,
  width: 40,
  border: `2px solid ${theme.palette.background.paper}`,
}));

/**
 * Header component that displays application title, user profile, and actions
 */
const Header: React.FC<HeaderProps> = ({ onSidebarToggle }) => {
  // Access theme context for toggling theme
  const { mode, toggleTheme } = useThemeContext();
  
  // Access auth context for user info and logout
  const { user, logout, isAuthenticated } = useAuth();
  
  // Navigation hook for routing
  const navigate = useNavigate();
  
  // State for profile menu
  const [profileAnchorEl, setProfileAnchorEl] = useState<null | HTMLElement>(null);
  const profileMenuOpen = Boolean(profileAnchorEl);
  
  // State for notifications menu
  const [notificationsAnchorEl, setNotificationsAnchorEl] = useState<null | HTMLElement>(null);
  const notificationsMenuOpen = Boolean(notificationsAnchorEl);
  
  // State for notification count
  const [notificationCount, setNotificationCount] = useState<number>(0);
  
  // Sample notifications (in a real app, these would come from an API)
  const [notifications, setNotifications] = useState<Notification[]>([]);
  
  // Effect to fetch notifications (simulated)
  useEffect(() => {
    if (isAuthenticated) {
      // Simulated notifications data
      // In a real app, this would be an API call
      const mockNotifications: Notification[] = [
        {
          id: '1',
          title: 'Pipeline Failed',
          message: 'Customer data pipeline failed. Self-healing in progress.',
          timestamp: new Date(Date.now() - 10 * 60000), // 10 minutes ago
          read: false,
          type: 'error'
        },
        {
          id: '2',
          title: 'Schema Drift Detected',
          message: 'Schema drift detected in sales_metrics. Self-healing completed.',
          timestamp: new Date(Date.now() - 25 * 60000), // 25 minutes ago
          read: false,
          type: 'warning'
        },
        {
          id: '3',
          title: 'New Pipeline Added',
          message: 'A new pipeline "inventory_sync" has been added.',
          timestamp: new Date(Date.now() - 60 * 60000), // 1 hour ago
          read: true,
          type: 'info'
        }
      ];
      
      setNotifications(mockNotifications);
      
      // Calculate unread notifications count
      const unreadCount = mockNotifications.filter(n => !n.read).length;
      setNotificationCount(unreadCount);
    }
  }, [isAuthenticated]);
  
  // Handler for opening profile menu
  const handleProfileMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setProfileAnchorEl(event.currentTarget);
  };
  
  // Handler for closing profile menu
  const handleProfileMenuClose = () => {
    setProfileAnchorEl(null);
  };
  
  // Handler for opening notifications menu
  const handleNotificationsMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setNotificationsAnchorEl(event.currentTarget);
  };
  
  // Handler for closing notifications menu
  const handleNotificationsMenuClose = () => {
    setNotificationsAnchorEl(null);
  };
  
  // Handler for logout
  const handleLogout = useCallback(async () => {
    await logout();
    handleProfileMenuClose();
    navigate('/login');
  }, [logout, navigate]);
  
  // Handler for navigating to profile page
  const handleProfileClick = useCallback(() => {
    handleProfileMenuClose();
    navigate('/profile');
  }, [navigate]);
  
  // Handler for theme toggle
  const handleThemeToggle = useCallback(() => {
    toggleTheme();
  }, [toggleTheme]);

  // Function to get user initials for avatar
  const getUserInitials = (): string => {
    if (!user) return '';
    
    const firstInitial = user.firstName ? user.firstName.charAt(0) : '';
    const lastInitial = user.lastName ? user.lastName.charAt(0) : '';
    
    return (firstInitial + lastInitial).toUpperCase();
  };
  
  // Format the notification time as a relative time string
  const formatNotificationTime = (timestamp: Date): string => {
    const now = new Date();
    const diffMs = now.getTime() - timestamp.getTime();
    const diffMins = Math.round(diffMs / 60000);
    
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  };
  
  // Render the header
  return (
    <HeaderAppBar position="fixed">
      <HeaderToolbar>
        {/* Left side - Logo/Title and sidebar toggle */}
        <LogoContainer>
          {onSidebarToggle && (
            <IconButton
              color="inherit"
              aria-label="open drawer"
              edge="start"
              onClick={onSidebarToggle}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
          )}
          <Title variant="h6" component="div">
            SELF-HEALING DATA PIPELINE
          </Title>
        </LogoContainer>
        
        {/* Right side - Action buttons */}
        <ActionContainer>
          {/* Notifications Button */}
          <Tooltip title="Notifications">
            <IconButton
              color="inherit"
              onClick={handleNotificationsMenuOpen}
              aria-label="notifications"
              aria-controls={notificationsMenuOpen ? 'notifications-menu' : undefined}
              aria-haspopup="true"
              aria-expanded={notificationsMenuOpen ? 'true' : undefined}
            >
              <Badge badgeContent={notificationCount} color="error">
                <Notifications />
              </Badge>
            </IconButton>
          </Tooltip>
          
          {/* Theme Toggle Button */}
          <Tooltip title={mode === 'dark' ? 'Light Mode' : 'Dark Mode'}>
            <IconButton
              color="inherit"
              onClick={handleThemeToggle}
              aria-label="toggle theme"
            >
              {mode === 'dark' ? <Brightness7 /> : <Brightness4 />}
            </IconButton>
          </Tooltip>
          
          {/* User Profile Button/Avatar */}
          {isAuthenticated && user ? (
            <Tooltip title="Account settings">
              <IconButton
                onClick={handleProfileMenuOpen}
                aria-label="user account"
                aria-controls={profileMenuOpen ? 'profile-menu' : undefined}
                aria-haspopup="true"
                aria-expanded={profileMenuOpen ? 'true' : undefined}
              >
                <UserAvatar alt={user.firstName}>
                  {getUserInitials()}
                </UserAvatar>
              </IconButton>
            </Tooltip>
          ) : (
            <Button
              variant="contained"
              color="secondary"
              size="small"
              onClick={() => navigate('/login')}
            >
              Login
            </Button>
          )}
        </ActionContainer>
      </HeaderToolbar>
      
      {/* Profile Menu */}
      <Menu
        id="profile-menu"
        anchorEl={profileAnchorEl}
        open={profileMenuOpen}
        onClose={handleProfileMenuClose}
        MenuListProps={{
          'aria-labelledby': 'profile-button',
        }}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        <MenuItem onClick={handleProfileClick}>
          <AccountCircle sx={{ mr: 1 }} />
          Profile
        </MenuItem>
        <MenuItem onClick={handleProfileClick}>
          <Settings sx={{ mr: 1 }} />
          Settings
        </MenuItem>
        <Divider />
        <MenuItem onClick={handleLogout}>
          Logout
        </MenuItem>
      </Menu>
      
      {/* Notifications Menu */}
      <Menu
        id="notifications-menu"
        anchorEl={notificationsAnchorEl}
        open={notificationsMenuOpen}
        onClose={handleNotificationsMenuClose}
        MenuListProps={{
          'aria-labelledby': 'notifications-button',
          sx: { width: '350px', maxHeight: '500px', padding: 0 }
        }}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        {notifications.length === 0 ? (
          <Box sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="body2">No notifications</Typography>
          </Box>
        ) : (
          <>
            <Box sx={{ p: 2, borderBottom: '1px solid rgba(0, 0, 0, 0.12)' }}>
              <Typography variant="subtitle1" fontWeight="bold">
                Notifications
              </Typography>
            </Box>
            {notifications.map(notification => (
              <NotificationItem key={notification.id}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <Typography variant="subtitle2" fontWeight={notification.read ? 'normal' : 'bold'}>
                    {notification.title}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {formatNotificationTime(notification.timestamp)}
                  </Typography>
                </Box>
                <Typography variant="body2" color="text.secondary">
                  {notification.message}
                </Typography>
              </NotificationItem>
            ))}
            <Box sx={{ p: 1, borderTop: '1px solid rgba(0, 0, 0, 0.12)', textAlign: 'center' }}>
              <Button
                variant="text"
                size="small"
                onClick={() => {
                  handleNotificationsMenuClose();
                  navigate('/alerts');
                }}
              >
                View All Notifications
              </Button>
            </Box>
          </>
        )}
      </Menu>
    </HeaderAppBar>
  );
};

export default Header;