import React, { useState, useEffect } from 'react';
import { useLocation, useNavigate, Link } from 'react-router-dom';
import { 
  Drawer, 
  List, 
  ListItem, 
  ListItemIcon, 
  ListItemText, 
  Divider, 
  IconButton, 
  Tooltip, 
  Box, 
  Collapse 
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { 
  ChevronLeft, 
  ChevronRight, 
  ExpandLess, 
  ExpandMore 
} from '@mui/icons-material';

// Custom hooks
import { useThemeContext } from '../../contexts/ThemeContext';
import { useAuth } from '../../contexts/AuthContext';

// Routes and constants
import { ROUTES } from '../../utils/constants';
import { routes } from '../../routes/routes';

// Icons
import { ReactComponent as DashboardIcon } from '../../assets/icons/dashboard.svg';
import { ReactComponent as PipelineIcon } from '../../assets/icons/pipeline.svg';
import { ReactComponent as QualityIcon } from '../../assets/icons/quality.svg';
import { ReactComponent as HealingIcon } from '../../assets/icons/healing.svg';
import { ReactComponent as AlertIcon } from '../../assets/icons/alert.svg';
import { ReactComponent as ConfigIcon } from '../../assets/icons/config.svg';
import { ReactComponent as AdminIcon } from '../../assets/icons/admin.svg';

/**
 * Props for the Sidebar component
 */
interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
}

/**
 * State tracking which nested navigation sections are expanded
 */
interface ExpandedSections {
  [key: string]: boolean;
}

// Styled components
const SidebarDrawer = styled(Drawer)(({ theme }) => ({
  width: ({ open }: { open: boolean }) => open ? 240 : 70,
  flexShrink: 0,
  whiteSpace: 'nowrap',
  boxSizing: 'border-box',
  transition: theme.transitions.create('width', {
    easing: theme.transitions.easing.sharp,
    duration: theme.transitions.duration.enteringScreen,
  }),
  '& .MuiDrawer-paper': {
    width: ({ open }: { open: boolean }) => open ? 240 : 70,
    backgroundColor: theme.palette.mode === 'light' ? theme.palette.background.paper : theme.palette.background.paper,
    color: theme.palette.text.primary,
    borderRight: `1px solid ${theme.palette.divider}`,
    overflow: 'hidden',
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
}));

const DrawerHeader = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'flex-end',
  padding: theme.spacing(0, 1),
  ...theme.mixins.toolbar,
}));

const NavList = styled(List)(({ theme }) => ({
  padding: theme.spacing(1, 0),
}));

const NavItem = styled(ListItem)<{ active?: boolean }>(({ theme, active }) => ({
  padding: theme.spacing(1, 2),
  color: active ? theme.palette.primary.main : theme.palette.text.primary,
  backgroundColor: active ? `${theme.palette.primary.main}10` : 'transparent',
  borderLeft: active ? `3px solid ${theme.palette.primary.main}` : '3px solid transparent',
  '&:hover': {
    backgroundColor: theme.palette.mode === 'light' 
      ? theme.palette.grey[100] 
      : 'rgba(255, 255, 255, 0.08)',
    borderLeft: active 
      ? `3px solid ${theme.palette.primary.main}` 
      : `3px solid ${theme.palette.primary.main}40`,
  },
  transition: theme.transitions.create(['background-color', 'border-left'], {
    duration: theme.transitions.duration.shortest,
  }),
}));

const NavItemIcon = styled(ListItemIcon)<{ active?: boolean }>(({ theme, active }) => ({
  minWidth: 36,
  color: active ? theme.palette.primary.main : theme.palette.text.primary,
  '& svg': {
    width: 20,
    height: 20,
  },
}));

const NavItemText = styled(ListItemText)(({ theme }) => ({
  margin: 0,
  '& .MuiTypography-root': {
    fontWeight: 500,
    fontSize: '0.875rem',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
}));

const CollapseButton = styled(IconButton)(({ theme }) => ({
  marginRight: theme.spacing(1),
}));

const NestedList = styled(List)(({ theme }) => ({
  paddingLeft: theme.spacing(2),
}));

/**
 * Sidebar navigation component that displays navigation links based on user permissions
 * @param props Component props
 * @returns JSX.Element
 */
const Sidebar: React.FC<SidebarProps> = ({ collapsed, onToggle }) => {
  const { theme, mode } = useThemeContext();
  const { checkPermission, checkRole } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  
  // Track expanded state of nested navigation sections
  const [expandedSections, setExpandedSections] = useState<ExpandedSections>({});

  // Reset expanded sections when sidebar is collapsed
  useEffect(() => {
    if (collapsed) {
      setExpandedSections({});
    }
  }, [collapsed]);

  // Handle toggling of a nested navigation section
  const handleSectionToggle = (path: string) => {
    setExpandedSections(prevState => ({
      ...prevState,
      [path]: !prevState[path]
    }));
  };

  // Handle navigation to a route
  const handleNavigation = (path: string) => {
    navigate(path);
  };

  // Filter routes based on user permissions
  const navigationItems = routes.filter(route => {
    // Skip routes that shouldn't show in nav
    const showInNav = route.path !== 'LOGIN' && route.path !== 'NOT_FOUND' && route.path !== 'PROFILE';
    
    if (!showInNav) return false;
    
    // Check permissions
    if (route.permissions) {
      const hasPermission = route.permissions.some(permission => 
        checkPermission(permission as UserPermission)
      );
      if (!hasPermission) return false;
    }
    
    // Check roles
    if (route.roles) {
      const hasRole = route.roles.some(role => 
        checkRole(role as UserRole)
      );
      if (!hasRole) return false;
    }
    
    return true;
  });

  return (
    <SidebarDrawer
      variant="permanent"
      open={!collapsed}
      PaperProps={{ 
        elevation: 2,
        component: 'nav',
        'aria-label': 'navigation'
      }}
    >
      <DrawerHeader>
        <CollapseButton 
          onClick={onToggle}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          size="small"
        >
          {collapsed ? <ChevronRight /> : <ChevronLeft />}
        </CollapseButton>
      </DrawerHeader>
      
      <Divider />
      
      <NavList component="ul">
        {navigationItems.map((item) => {
          const routePath = ROUTES[item.path as keyof typeof ROUTES];
          const isActive = isRouteActive(routePath, location.pathname);
          
          // If item has children, render with expansion controls
          if (item.children && item.children.length > 0) {
            const isExpanded = expandedSections[routePath] || false;
            
            return (
              <React.Fragment key={routePath}>
                <NavItem 
                  active={isActive}
                  button
                  component="li"
                  onClick={() => {
                    if (collapsed) {
                      handleNavigation(routePath);
                    } else {
                      handleSectionToggle(routePath);
                    }
                  }}
                >
                  <Tooltip title={collapsed ? item.path : ""} placement="right">
                    <NavItemIcon active={isActive}>
                      {getIconComponent(item.path)}
                    </NavItemIcon>
                  </Tooltip>
                  
                  {!collapsed && (
                    <>
                      <NavItemText primary={item.path.replace('_', ' ')} />
                      {isExpanded ? <ExpandLess /> : <ExpandMore />}
                    </>
                  )}
                </NavItem>
                
                {!collapsed && (
                  <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                    <NestedList component="ul">
                      {item.children.map((child) => {
                        const childPath = `${routePath}${child.path.includes('ROUTES') 
                          ? child.path.split('.')[1].toLowerCase()
                          : child.path}`;
                        const isChildActive = isRouteActive(childPath, location.pathname);
                        
                        // Check child permissions
                        if (child.permissions && !child.permissions.some(
                          permission => checkPermission(permission as UserPermission)
                        )) {
                          return null;
                        }
                        
                        return (
                          <NavItem
                            key={childPath}
                            active={isChildActive}
                            button
                            component="li"
                            onClick={() => handleNavigation(childPath)}
                          >
                            <NavItemText 
                              primary={child.component.replace(/([A-Z])/g, ' $1').trim()} 
                              inset
                            />
                          </NavItem>
                        );
                      })}
                    </NestedList>
                  </Collapse>
                )}
              </React.Fragment>
            );
          }
          
          // Regular menu item without children
          return (
            <NavItem
              key={routePath}
              active={isActive}
              button
              component={Link}
              to={routePath}
              onClick={() => handleNavigation(routePath)}
            >
              <Tooltip title={collapsed ? item.path.replace('_', ' ') : ""} placement="right">
                <NavItemIcon active={isActive}>
                  {getIconComponent(item.path)}
                </NavItemIcon>
              </Tooltip>
              
              {!collapsed && (
                <NavItemText primary={item.path.replace('_', ' ')} />
              )}
            </NavItem>
          );
        })}
      </NavList>
    </SidebarDrawer>
  );
};

/**
 * Helper function to get the appropriate icon component based on icon name
 * @param iconName Icon name string
 * @returns React element or null
 */
function getIconComponent(iconName: string): React.ReactElement | null {
  switch (iconName) {
    case 'DASHBOARD':
      return <DashboardIcon />;
    case 'PIPELINE_MANAGEMENT':
      return <PipelineIcon />;
    case 'DATA_QUALITY':
      return <QualityIcon />;
    case 'SELF_HEALING':
      return <HealingIcon />;
    case 'ALERTING':
      return <AlertIcon />;
    case 'CONFIGURATION':
      return <ConfigIcon />;
    case 'ADMINISTRATION':
      return <AdminIcon />;
    default:
      return null;
  }
}

/**
 * Helper function to determine if a route is currently active
 * @param routePath Route path to check
 * @param currentPath Current location path
 * @returns Boolean indicating if route is active
 */
function isRouteActive(routePath: string, currentPath: string): boolean {
  if (routePath === '/') {
    return currentPath === '/';
  }
  return currentPath.startsWith(routePath);
}

export default Sidebar;