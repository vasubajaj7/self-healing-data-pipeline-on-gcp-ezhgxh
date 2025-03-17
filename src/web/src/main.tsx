import React from 'react'; // Core React library for component creation // version: ^18.2.0
import ReactDOM from 'react-dom/client'; // React DOM rendering library // version: ^18.2.0
import { createRoot } from 'react-dom/client'; // React 18 function for creating a root to render into // version: ^18.2.0
import App from './App'; // Root component of the application that sets up providers and routing // path: src/web/src/App.tsx
import AppGlobalStyles from './theme/GlobalStyles'; // Component that provides global styles for the application // path: src/web/src/theme/GlobalStyles.tsx
import i18n from './config/i18n'; // Internationalization configuration for the application // path: src/web/src/config/i18n.ts
import { ENV } from './config/env'; // Environment configuration for the application // path: src/web/src/config/env.ts

/**
 * Renders the application to the DOM
 * @parameters none
 * @returns void
 */
const renderApp = (): void => {
  // LD1: Get the root element from the DOM
  const rootElement = document.getElementById('root');

  // LD1: Create a React 18 root using createRoot
  const root = createRoot(rootElement as HTMLElement);

  // LD1: Render the App component wrapped in React.StrictMode
  root.render(
    <React.StrictMode>
      {/* LD1: Include AppGlobalStyles for global styling */}
      <AppGlobalStyles />
      <App />
    </React.StrictMode>
  );
};

// IE1: Import global CSS styles
// IE2: Import and initialize i18n configuration
// IE2: Import environment configuration
// IE2: Import React and ReactDOM
// IE1: Import the App component and AppGlobalStyles
// LD1: Define the renderApp function
// LD1: Execute the renderApp function to mount the application
renderApp();