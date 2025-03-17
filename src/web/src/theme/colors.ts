/**
 * Application color palette
 * 
 * This file defines all the colors used throughout the application to ensure
 * consistent styling and meet accessibility requirements (minimum 4.5:1 contrast ratio).
 * 
 * The colors are designed to support the self-healing data pipeline application's needs:
 * - Status visualization for pipeline components (healthy, warning, error states)
 * - Clear identification of issues requiring attention
 * - Data visualization in charts and dashboards
 * - Consistent UI styling across all application components
 */

// Color with variant definitions
interface ColorWithVariants {
  main: string;
  light: string;
  dark: string;
  contrastText: string;
}

// Grey scale definitions
interface GreyScale {
  50: string;
  100: string;
  200: string;
  300: string;
  400: string;
  500: string;
  600: string;
  700: string;
  800: string;
  900: string;
  A100: string;
  A200: string;
  A400: string;
  A700: string;
}

// Background colors
interface BackgroundColors {
  default: string;
  paper: string;
  dark: string;
}

// Text colors
interface TextColors {
  primary: string;
  secondary: string;
  disabled: string;
  hint: string;
  white: string;
}

// Chart colors
interface ChartColors {
  blue: string;
  green: string;
  purple: string;
  orange: string;
  red: string;
  teal: string;
  cyan: string;
  lime: string;
  amber: string;
  indigo: string;
}

// Status indicator colors
interface StatusColors {
  healthy: string;
  warning: string;
  error: string;
  inactive: string;
  processing: string;
}

// Color palette definition
interface ColorPalette {
  primary: ColorWithVariants;
  secondary: ColorWithVariants;
  error: ColorWithVariants;
  warning: ColorWithVariants;
  info: ColorWithVariants;
  success: ColorWithVariants;
  grey: GreyScale;
  background: BackgroundColors;
  text: TextColors;
  chart: ChartColors;
  status: StatusColors;
}

// Primary colors - Main application brand colors
const primary: ColorWithVariants = {
  main: '#1976d2',
  light: '#42a5f5',
  dark: '#1565c0',
  contrastText: '#ffffff'
};

// Secondary colors - Complementary to primary colors
const secondary: ColorWithVariants = {
  main: '#9c27b0',
  light: '#ba68c8',
  dark: '#7b1fa2',
  contrastText: '#ffffff'
};

// Semantic colors for feedback and status
const error: ColorWithVariants = {
  main: '#d32f2f',
  light: '#ef5350',
  dark: '#c62828',
  contrastText: '#ffffff'
};

const warning: ColorWithVariants = {
  main: '#ed6c02',
  light: '#ff9800',
  dark: '#e65100',
  contrastText: '#ffffff'
};

const info: ColorWithVariants = {
  main: '#0288d1',
  light: '#03a9f4',
  dark: '#01579b',
  contrastText: '#ffffff'
};

const success: ColorWithVariants = {
  main: '#2e7d32',
  light: '#4caf50',
  dark: '#1b5e20',
  contrastText: '#ffffff'
};

// Neutral colors and greys
const grey: GreyScale = {
  50: '#fafafa',
  100: '#f5f5f5',
  200: '#eeeeee',
  300: '#e0e0e0',
  400: '#bdbdbd',
  500: '#9e9e9e',
  600: '#757575',
  700: '#616161',
  800: '#424242',
  900: '#212121',
  A100: '#d5d5d5',
  A200: '#aaaaaa',
  A400: '#303030',
  A700: '#616161'
};

// Background colors
const background: BackgroundColors = {
  default: '#f5f5f5',
  paper: '#ffffff',
  dark: '#121212'
};

// Text colors
const text: TextColors = {
  primary: 'rgba(0, 0, 0, 0.87)',
  secondary: 'rgba(0, 0, 0, 0.6)',
  disabled: 'rgba(0, 0, 0, 0.38)',
  hint: 'rgba(0, 0, 0, 0.38)',
  white: '#ffffff'
};

// Chart and visualization colors
const chart: ChartColors = {
  blue: '#1976d2',
  green: '#2e7d32',
  purple: '#9c27b0',
  orange: '#ed6c02',
  red: '#d32f2f',
  teal: '#009688',
  cyan: '#00bcd4',
  lime: '#cddc39',
  amber: '#ffc107',
  indigo: '#3f51b5'
};

// Status indicator colors
const status: StatusColors = {
  healthy: '#2e7d32',
  warning: '#ed6c02',
  error: '#d32f2f',
  inactive: '#9e9e9e',
  processing: '#0288d1'
};

// Export the complete color palette
export const colors: ColorPalette = {
  primary,
  secondary,
  error,
  warning,
  info,
  success,
  grey,
  background,
  text,
  chart,
  status
};

export default colors;