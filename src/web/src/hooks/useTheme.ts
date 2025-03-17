import { Theme } from '@mui/material'; // ^5.11.0
import { useThemeContext } from '../contexts/ThemeContext';

/**
 * Interface defining the shape of the Theme Context
 */
interface ThemeContextType {
  theme: Theme;
  mode: 'light' | 'dark';
  toggleTheme: () => void;
}

/**
 * Custom hook that provides access to the application theme context
 * 
 * This hook simplifies access to the theme context throughout the application,
 * making it easy for components to use the current theme, check the theme mode,
 * and toggle between light and dark themes without directly importing the ThemeContext.
 * 
 * @returns Object containing theme, mode, and toggleTheme function
 */
const useTheme = (): ThemeContextType => {
  return useThemeContext();
};

export default useTheme;