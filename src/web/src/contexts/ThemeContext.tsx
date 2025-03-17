import React, { createContext, useContext, useState, useEffect, useMemo, ReactNode } from 'react';
import { ThemeProvider } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { Theme } from '@mui/material'; // @mui/material ^5.11.0
import { lightTheme, darkTheme } from '../theme/theme';

/**
 * Interface defining the shape of the Theme Context
 */
interface ThemeContextType {
  theme: Theme;
  mode: 'light' | 'dark';
  toggleTheme: () => void;
}

/**
 * Props for the ThemeContextProvider component
 */
interface ThemeContextProviderProps {
  children: ReactNode;
}

/**
 * Create the Theme Context with a null initial value
 * (will be provided by ThemeContextProvider before use)
 */
export const ThemeContext = createContext<ThemeContextType>(null as unknown as ThemeContextType);

/**
 * Detects the user's system theme preference
 * @returns 'light' or 'dark' based on system preference
 */
const detectSystemTheme = (): 'light' | 'dark' => {
  if (typeof window !== 'undefined' && window.matchMedia) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  return 'light'; // Default to light if cannot detect
};

/**
 * Provider component that makes theme context available to its children
 * Manages theme state and provides theme toggling functionality
 */
export const ThemeContextProvider: React.FC<ThemeContextProviderProps> = ({ children }) => {
  // Initialize theme mode from localStorage or system preference
  const [mode, setMode] = useState<'light' | 'dark'>(() => {
    if (typeof window !== 'undefined') {
      const savedMode = localStorage.getItem('themeMode');
      if (savedMode === 'light' || savedMode === 'dark') {
        return savedMode;
      }
      return detectSystemTheme();
    }
    return 'light'; // Default for SSR
  });

  // Persist theme preference in localStorage when it changes
  useEffect(() => {
    if (typeof window !== 'undefined') {
      localStorage.setItem('themeMode', mode);
    }
  }, [mode]);

  // Toggle between light and dark themes
  const toggleTheme = () => {
    setMode((prevMode) => (prevMode === 'light' ? 'dark' : 'light'));
  };

  // Determine the current theme object based on mode
  const theme = mode === 'light' ? lightTheme : darkTheme;

  // Memoize the context value to prevent unnecessary re-renders
  const contextValue = useMemo(
    () => ({
      theme,
      mode,
      toggleTheme,
    }),
    [theme, mode]
  );

  return (
    <ThemeContext.Provider value={contextValue}>
      <ThemeProvider theme={theme}>{children}</ThemeProvider>
    </ThemeContext.Provider>
  );
};

/**
 * Custom hook that provides access to the ThemeContext
 * @returns The current theme context value
 * @throws Error if used outside of ThemeContextProvider
 */
export const useThemeContext = (): ThemeContextType => {
  const context = useContext(ThemeContext);
  
  if (!context) {
    throw new Error('useThemeContext must be used within a ThemeContextProvider');
  }
  
  return context;
};