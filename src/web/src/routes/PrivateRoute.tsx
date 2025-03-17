import React from 'react';
import { Navigate, Outlet, useLocation } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { ROUTES } from './routes';
import Spinner from '../components/common/Spinner';

/**
 * Interface for PrivateRoute component props
 */
interface RouteProps {
  /**
   * Optional array of role names required to access the route
   */
  roles?: string[];
  
  /**
   * Optional array of permission names required to access the route
   */
  permissions?: string[];
}

/**
 * Higher-order component that protects routes requiring authentication
 * 
 * This component checks if the user is authenticated and has the required roles
 * and permissions before rendering the protected route. Otherwise, it redirects
 * to the login page.
 * 
 * @param {RouteProps} props - Component props containing roles and permissions requirements
 * @returns {JSX.Element} Either the protected route component, a loading spinner, or a redirect to login
 */
const PrivateRoute: React.FC<RouteProps> = ({ roles, permissions }) => {
  // Get authentication state and checking functions from useAuth hook
  const { isAuthenticated, checkRole, checkPermission, loading } = useAuth();
  
  // Get current location to redirect back after login
  const location = useLocation();

  // Show loading spinner while auth state is being determined
  if (loading) {
    return <Spinner size="medium" label="Verifying authentication..." />;
  }

  // Redirect to login page if user is not authenticated
  if (!isAuthenticated) {
    return (
      <Navigate 
        to={ROUTES.LOGIN} 
        state={{ from: location.pathname }}
        replace 
      />
    );
  }

  // If roles are specified, check if user has at least one of the required roles
  if (roles && roles.length > 0) {
    const hasRequiredRole = roles.some(role => checkRole(role));
    if (!hasRequiredRole) {
      // If user doesn't have any of the required roles, redirect to dashboard
      // or another appropriate page
      return <Navigate to={ROUTES.DASHBOARD} replace />;
    }
  }

  // If permissions are specified, check if user has all required permissions
  if (permissions && permissions.length > 0) {
    const hasAllRequiredPermissions = permissions.every(permission => 
      checkPermission(permission)
    );
    
    if (!hasAllRequiredPermissions) {
      // If user doesn't have all required permissions, redirect to dashboard
      // or another appropriate page
      return <Navigate to={ROUTES.DASHBOARD} replace />;
    }
  }

  // If user is authenticated and has required roles/permissions, render the route
  return <Outlet />;
};

export default PrivateRoute;