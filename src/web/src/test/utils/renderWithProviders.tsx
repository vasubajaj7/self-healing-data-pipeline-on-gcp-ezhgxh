import React, { ReactElement } from 'react'; // react ^18.2.0
import { render, RenderOptions, RenderResult } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { BrowserRouter } from 'react-router-dom'; // react-router-dom ^6.8.0
import { QueryClient, QueryClientProvider } from 'react-query'; // react-query ^3.39.2
import { ThemeContextProvider } from '../../contexts/ThemeContext';
import { AuthProvider } from '../../contexts/AuthContext';
import { AlertProvider } from '../../contexts/AlertContext';
import { DashboardProvider } from '../../contexts/DashboardContext';
import { QualityProvider } from '../../contexts/QualityContext';
import { mockResizeObserver, mockIntersectionObserver } from './testUtils';

/**
 * Interface for custom render options to extend Testing Library's render options
 */
interface CustomRenderOptions extends RenderOptions {
  route?: string;
  queryClient?: QueryClient;
  authProviderProps?: object;
  themeProviderProps?: object;
  alertProviderProps?: object;
  dashboardProviderProps?: object;
  qualityProviderProps?: object;
}

/**
 * Renders a React component with all necessary context providers for testing
 * @param ui - The React component to render
 * @param options - Additional render options
 * @returns The render result with additional helper methods from Testing Library
 */
const renderWithProviders = (
  ui: ReactElement,
  options: CustomRenderOptions = {}
): RenderResult => {
  // LD1: Create a new QueryClient instance for React Query
  const testQueryClient = createTestQueryClient();

  // LD1: Set up mock implementations for ResizeObserver and IntersectionObserver
  mockResizeObserver();
  mockIntersectionObserver();

  // LD1: Create a wrapper component that provides all necessary contexts
  const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    return (
      // LD1: Wrap the UI component with ThemeContextProvider
      <ThemeContextProvider {...options.themeProviderProps}>
        {/* LD1: Wrap with AuthProvider for authentication context */}
        <AuthProvider {...options.authProviderProps}>
          {/* LD1: Wrap with AlertProvider for alert management */}
          <AlertProvider {...options.alertProviderProps}>
            {/* LD1: Wrap with DashboardProvider for dashboard data */}
            <DashboardProvider {...options.dashboardProviderProps}>
              {/* LD1: Wrap with QualityProvider for quality data */}
              <QualityProvider {...options.qualityProviderProps}>
                {/* LD1: Wrap with BrowserRouter for routing */}
                <BrowserRouter>
                  {/* LD1: Wrap with QueryClientProvider for data fetching */}
                  <QueryClientProvider client={options.queryClient || testQueryClient}>
                    {children}
                  </QueryClientProvider>
                </BrowserRouter>
              </QualityProvider>
            </DashboardProvider>
          </AlertProvider>
        </AuthProvider>
      </ThemeContextProvider>
    );
  };

  // LD1: Use Testing Library's render function to render the wrapped component
  return render(ui, { wrapper: Wrapper, ...options }); // IE3: Exporting all members
};

/**
 * Creates a configured QueryClient instance for testing
 * @returns A configured QueryClient instance
 */
const createTestQueryClient = (): QueryClient => {
  // LD1: Create a new QueryClient with test-specific configuration
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        // LD1: Configure default options for queries and mutations
        retry: false, // LD1: Set retry to false to avoid retries in tests
        cacheTime: 0, // LD1: Set cacheTime to a low value for testing
      },
      mutations: {
        retry: false, // LD1: Set retry to false to avoid retries in tests
      },
    },
  });

  return queryClient; // LD1: Return the configured QueryClient instance
};

// IE3: Export utility for rendering components with all necessary providers in tests
export { renderWithProviders };

// IE3: Export utility for creating a test-specific QueryClient instance
export { createTestQueryClient };

// IE3: Export the CustomRenderOptions interface
export type { CustomRenderOptions };