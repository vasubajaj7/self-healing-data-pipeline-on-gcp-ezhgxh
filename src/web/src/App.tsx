import React from 'react'; // React library for component creation // version: ^18.2.0
import { BrowserRouter } from 'react-router-dom'; // React Router components for defining routes and navigation // version: ^6.8.0
import { CssBaseline } from '@mui/material'; // Material-UI component for normalizing CSS across browsers // version: ^5.11.0
import { ErrorBoundary } from 'react-error-boundary'; // Component for catching and handling React errors // version: ^3.1.4
import AppRoutes from './routes/AppRoutes'; // Main routing component that defines the application's route structure // path: src/web/src/routes/AppRoutes.tsx
import { AuthProvider } from './contexts/AuthContext'; // Authentication context provider for user authentication state // path: src/web/src/contexts/AuthContext.tsx
import { ThemeContextProvider } from './contexts/ThemeContext'; // Theme context provider for application theming and dark/light mode // path: src/web/src/contexts/ThemeContext.tsx
import { AlertProvider } from './contexts/AlertContext'; // Alert context provider for alert management functionality // path: src/web/src/contexts/AlertContext.tsx
import { DashboardProvider } from './contexts/DashboardContext'; // Dashboard context provider for dashboard data and functionality // path: src/web/src/contexts/DashboardContext.tsx
import { QualityProvider } from './contexts/QualityContext'; // Quality context provider for data quality functionality // path: src/web/src/contexts/QualityContext.tsx

/**
 * Fallback UI component displayed when an error is caught by ErrorBoundary
 * @object props
 * @returns Error UI with error details and reset button
 */
const ErrorFallback: React.FC<{ error: Error; resetErrorBoundary: () => void }> = ({ error, resetErrorBoundary }) => {
  // LD1: Style the error container for visibility
  return (
    <div role="alert" style={{
      position: 'fixed',
      top: 0,
      left: 0,
      width: '100%',
      height: '100%',
      backgroundColor: 'rgba(255, 255, 255, 0.9)',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 9999
    }}>
      // LD1: Display error message and stack trace
      <p>Something went wrong:</p>
      <pre style={{ color: 'red' }}>{error.message}</pre>
      <pre style={{ overflow: 'auto', maxHeight: '200px' }}>{error.stack}</pre>
      // LD1: Provide a button to reset the error boundary
      <button onClick={resetErrorBoundary}>Try again</button>
    </div>
  );
}

/**
 * Root component of the application that sets up providers and routing
 * @parameters none
 * @returns The rendered application with all providers and routing
 */
const App: React.FC = () => {
  return (
    // LD1: Set up BrowserRouter for client-side routing
    <BrowserRouter>
      // LD1: Set up ErrorBoundary for catching React errors
      <ErrorBoundary FallbackComponent={ErrorFallback}
        onReset={() => {
          // Reload the entire page to reset the application state
          window.location.reload();
        }}>
        // LD1: Set up ThemeContextProvider for theme management
        <ThemeContextProvider>
          // LD1: Set up AuthProvider for authentication state
          <AuthProvider>
            // LD1: Set up AlertProvider for alert management
            <AlertProvider>
              // LD1: Set up DashboardProvider for dashboard data
              <DashboardProvider>
                // LD1: Set up QualityProvider for quality data
                <QualityProvider>
                  // LD1: Include CssBaseline for consistent styling
                  <CssBaseline />
                  // LD1: Render AppRoutes component for application routing
                  <AppRoutes />
                </QualityProvider>
              </DashboardProvider>
            </AlertProvider>
          </AuthProvider>
        </ThemeContextProvider>
      </ErrorBoundary>
    </BrowserRouter>
  );
};

// IE3: Export the App component as the default export
export default App;