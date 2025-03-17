import React from 'react'; // React library for component creation // version: ^18.2.0
import { Routes, Route, Navigate, useLocation } from 'react-router-dom'; // React Router components for defining routes and navigation // version: ^6.8.0
import { routes } from './routes'; // Import route configuration array defining all application routes // path: src/web/src/routes/routes.ts
import PrivateRoute from './PrivateRoute'; // Component for protecting routes that require authentication // path: src/web/src/routes/PrivateRoute.tsx
import MainLayout from '../components/layout/MainLayout'; // Main layout component that provides the overall structure for the application // path: src/web/src/components/layout/MainLayout.tsx
import Login from '../pages/Login'; // Login page component // path: src/web/src/pages/Login.tsx
import Dashboard from '../pages/Dashboard'; // Dashboard page component // path: src/web/src/pages/Dashboard.tsx
import PipelineManagement from '../pages/PipelineManagement'; // Pipeline management page component // path: src/web/src/pages/PipelineManagement.tsx
import DataQuality from '../pages/DataQuality'; // Data quality page component // path: src/web/src/pages/DataQuality.tsx
import SelfHealing from '../pages/SelfHealing'; // Self-healing page component // path: src/web/src/pages/SelfHealing.tsx
import Alerting from '../pages/Alerting'; // Alerting page component // path: src/web/src/pages/Alerting.tsx
import Configuration from '../pages/Configuration'; // Configuration page component // path: src/web/src/pages/Configuration.tsx
import Administration from '../pages/Administration'; // Administration page component // path: src/web/src/pages/Administration.tsx
import Profile from '../pages/Profile'; // User profile page component // path: src/web/src/pages/Profile.tsx
import NotFound from '../pages/NotFound'; // 404 Not Found page component // path: src/web/src/pages/NotFound.tsx
import ActivityLog from '../pages/selfHealing/ActivityLog'; // Self-healing activity log page component // path: src/web/src/pages/selfHealing/ActivityLog.tsx
import ModelManagement from '../pages/selfHealing/ModelManagement'; // Self-healing model management page component // path: src/web/src/pages/selfHealing/ModelManagement.tsx
import HealingConfiguration from '../pages/selfHealing/Configuration'; // Self-healing configuration page component // path: src/web/src/pages/selfHealing/Configuration.tsx
import AlertDashboard from '../pages/alerts/AlertDashboard'; // Alert dashboard page component // path: src/web/src/pages/alerts/AlertDashboard.tsx
import NotificationConfig from '../pages/alerts/NotificationConfig'; // Notification configuration page component // path: src/web/src/pages/alerts/NotificationConfig.tsx
import AlertHistory from '../pages/alerts/AlertHistory'; // Alert history page component // path: src/web/src/pages/alerts/AlertHistory.tsx

/**
 * Main routing component that defines the application's route structure
 */
const AppRoutes: React.FC = () => {
  // LD1: Get the current location using the useLocation hook
  const location = useLocation();

  // LD1: Define a helper function to recursively render routes and their children
  const renderRoutes = (routeConfig: any) => {
    return routeConfig.map((route: any) => {
      // LD1: Extract route properties from the route configuration object
      const { path, component, layout, auth, roles, permissions, children } = route;

      // LD1: Determine the component to render based on the component name
      let RouteComponent: React.ComponentType<any>;
      switch (component) {
        case 'Login':
          RouteComponent = Login;
          break;
        case 'Dashboard':
          RouteComponent = Dashboard;
          break;
        case 'PipelineManagement':
          RouteComponent = PipelineManagement;
          break;
        case 'DataQuality':
          RouteComponent = DataQuality;
        break;
        case 'SelfHealing':
          RouteComponent = SelfHealing;
          break;
        case 'Alerting':
          RouteComponent = Alerting;
          break;
        case 'Configuration':
          RouteComponent = Configuration;
          break;
        case 'Administration':
          RouteComponent = Administration;
          break;
        case 'Profile':
          RouteComponent = Profile;
          break;
        case 'NotFound':
          RouteComponent = NotFound;
          break;
        case 'ActivityLog':
          RouteComponent = ActivityLog;
          break;
        case 'ModelManagement':
          RouteComponent = ModelManagement;
          break;
        case 'HealingConfiguration':
          RouteComponent = HealingConfiguration;
          break;
          case 'AlertDashboard':
            RouteComponent = AlertDashboard;
            break;
          case 'NotificationConfig':
            RouteComponent = NotificationConfig;
            break;
          case 'AlertHistory':
            RouteComponent = AlertHistory;
            break;
        default:
          return null; // LD1: Handle unknown components
      }

      // LD1: Determine the route path based on the route configuration
      let routePath = typeof path === 'string' ? routes[path as keyof typeof routes] : path;

      // LD1: Handle nested routes by recursively rendering child routes
      const renderChildRoutes = (childRoutes: any) => {
        return childRoutes.map((child: any) => {
          const { path: childPath, component: childComponent, permissions: childPermissions } = child;

          let ChildRouteComponent: React.ComponentType<any>;
          switch (childComponent) {
            case 'ActivityLog':
              ChildRouteComponent = ActivityLog;
              break;
            case 'ModelManagement':
              ChildRouteComponent = ModelManagement;
              break;
            case 'HealingConfiguration':
              ChildRouteComponent = HealingConfiguration;
              break;
              case 'AlertDashboard':
                ChildRouteComponent = AlertDashboard;
                break;
              case 'NotificationConfig':
                ChildRouteComponent = NotificationConfig;
                break;
              case 'AlertHistory':
                ChildRouteComponent = AlertHistory;
                break;
            default:
              return null; // LD1: Handle unknown components
          }

          const fullChildPath = `${routePath}${childPath.includes('ROUTES') ? childPath.split('.')[1].toLowerCase() : childPath}`;

          return (
            <Route
              key={fullChildPath}
              path={fullChildPath.replace('ROUTES.', '')}
              element={
                auth ? (
                  <PrivateRoute roles={roles} permissions={childPermissions}>
                    {layout ? (
                      <MainLayout>
                        <ChildRouteComponent />
                      </MainLayout>
                    ) : (
                      <ChildRouteComponent />
                    )}
                  </PrivateRoute>
                ) : (
                  layout ? (
                    <MainLayout>
                      <ChildRouteComponent />
                    </MainLayout>
                  ) : (
                    <ChildRouteComponent />
                  )
                }
              }
            />
          );
        });
      };

      // LD1: Render the Route component with appropriate properties
      return (
        <Route
          key={routePath}
          path={routePath}
          element={
            auth ? (
              <PrivateRoute roles={roles} permissions={permissions}>
                {layout ? (
                  <MainLayout>
                    <RouteComponent />
                  </MainLayout>
                ) : (
                  <RouteComponent />
                )}
              </PrivateRoute>
            ) : (
              layout ? (
                <MainLayout>
                  <RouteComponent />
                </MainLayout>
              ) : (
                <RouteComponent />
              )
            )
          }
        >
          {children && renderChildRoutes(children)}
        </Route>
      );
    });
  };

  // LD1: Render the Routes component with all defined routes
  return (
    <Routes location={location}>
      {renderRoutes(routes)}
    </Routes>
  );
};

// IE3: Export the AppRoutes component as the default export
export default AppRoutes;