import React from 'react';
import { Breadcrumbs as MuiBreadcrumbs, Typography, Link, BreadcrumbsProps as MuiBreadcrumbsProps } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { NavigateNext, Home } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { useLocation, Link as RouterLink } from 'react-router-dom'; // react-router-dom ^6.8.0
import { useThemeContext } from '../../contexts/ThemeContext';
import { routes, ROUTES } from '../../routes/routes';

/**
 * Interface for a breadcrumb item
 */
interface BreadcrumbItem {
  path: string;
  label: string;
  isLast: boolean;
  icon?: React.ReactNode;
}

/**
 * Props for the Breadcrumbs component
 */
interface BreadcrumbsProps extends Omit<MuiBreadcrumbsProps, 'children'> {
  items?: BreadcrumbItem[];
  separator?: React.ReactNode;
  maxItems?: number;
  itemsAfterCollapse?: number;
  itemsBeforeCollapse?: number;
  showHomeIcon?: boolean;
  className?: string;
}

/**
 * Styled version of Material-UI Breadcrumbs with custom styling
 */
const StyledBreadcrumbs = styled(MuiBreadcrumbs)(({ theme }) => ({
  padding: '8px 0',
  margin: '0 0 16px 0',
  '& .MuiBreadcrumbs-ol': {
    display: 'flex',
    alignItems: 'center',
    flexWrap: 'wrap',
    padding: 0,
    margin: 0,
    listStyle: 'none',
  },
  '& .MuiBreadcrumbs-li': {
    display: 'flex',
    alignItems: 'center',
  },
  '& .MuiBreadcrumbs-separator': {
    display: 'flex',
    alignItems: 'center',
    color: theme.palette.text.disabled,
    fontSize: '1.2rem',
    marginLeft: theme.spacing(1),
    marginRight: theme.spacing(1),
  },
}));

/**
 * Generates breadcrumb items based on the current route path
 * @param pathname - Current path from location
 * @returns Array of breadcrumb items with path, label, and isLast properties
 */
const generateBreadcrumbs = (pathname: string): BreadcrumbItem[] => {
  // Split the pathname into segments
  const segments = pathname.split('/').filter(Boolean);
  
  // Initialize with the home breadcrumb
  const breadcrumbs: BreadcrumbItem[] = [
    {
      path: ROUTES.DASHBOARD,
      label: 'Home',
      isLast: segments.length === 0,
    }
  ];
  
  // If we're at the dashboard, return just the home breadcrumb
  if (segments.length === 0) {
    return breadcrumbs;
  }
  
  // Build path progressively and create breadcrumb items
  let currentPath = '';
  
  segments.forEach((segment, index) => {
    currentPath += `/${segment}`;
    const isLast = index === segments.length - 1;
    
    // Try to find a matching route in ROUTES
    let label = '';
    for (const [key, path] of Object.entries(ROUTES)) {
      if (path === currentPath) {
        label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        break;
      }
    }
    
    // If no match found, use the formatted segment name
    if (!label) {
      label = segment.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
    
    breadcrumbs.push({
      path: currentPath,
      label,
      isLast,
    });
  });
  
  return breadcrumbs;
};

/**
 * Custom breadcrumbs component with automatic route-based generation
 * Provides navigation context and path visualization
 */
const Breadcrumbs: React.FC<BreadcrumbsProps> = (props) => {
  const {
    items,
    separator = <NavigateNext fontSize="small" />,
    maxItems = 8,
    itemsAfterCollapse = 1,
    itemsBeforeCollapse = 1,
    showHomeIcon = true,
    className,
    ...rest
  } = props;
  
  const location = useLocation();
  const { theme } = useThemeContext();
  
  // Generate breadcrumbs based on current path if items are not provided
  const generatedBreadcrumbs = React.useMemo(
    () => generateBreadcrumbs(location.pathname),
    [location.pathname]
  );
  
  // Use provided items or generated breadcrumbs
  const breadcrumbItems = items || generatedBreadcrumbs;
  
  return (
    <StyledBreadcrumbs
      separator={separator}
      maxItems={maxItems}
      itemsAfterCollapse={itemsAfterCollapse}
      itemsBeforeCollapse={itemsBeforeCollapse}
      className={className}
      aria-label="breadcrumb navigation"
      {...rest}
    >
      {breadcrumbItems.map((item, index) => (
        <React.Fragment key={item.path}>
          {item.isLast ? (
            <Typography
              color="textPrimary"
              sx={{
                display: 'flex',
                alignItems: 'center',
                fontWeight: 500,
                fontSize: '0.875rem',
              }}
              aria-current="page"
            >
              {index === 0 && showHomeIcon && <Home sx={{ mr: 0.5, fontSize: '1.2rem' }} />}
              {item.icon && <span style={{ marginRight: 4 }}>{item.icon}</span>}
              {item.label}
            </Typography>
          ) : (
            <Link
              component={RouterLink}
              to={item.path}
              color="inherit"
              sx={{
                display: 'flex',
                alignItems: 'center',
                textDecoration: 'none',
                fontSize: '0.875rem',
                '&:hover': {
                  textDecoration: 'underline',
                  color: theme.palette.primary.main,
                },
              }}
            >
              {index === 0 && showHomeIcon && <Home sx={{ mr: 0.5, fontSize: '1.2rem' }} />}
              {item.icon && <span style={{ marginRight: 4 }}>{item.icon}</span>}
              {item.label}
            </Link>
          )}
        </React.Fragment>
      ))}
    </StyledBreadcrumbs>
  );
};

export default Breadcrumbs;