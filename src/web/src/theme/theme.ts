/**
 * Application Theme Configuration
 * 
 * This file defines the main theme configuration for the application including:
 * - Color palette for light and dark modes
 * - Typography system
 * - Responsive breakpoints
 * - Component-specific style overrides
 * 
 * The theme objects are compatible with Material-UI's theming system and provide
 * a consistent styling foundation across the application.
 */

import { createTheme, Theme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { colors } from './colors';
import { typography } from './typography';
import { breakpoints } from './breakpoints';

/**
 * Creates a base theme with shared configuration between light and dark modes
 * @returns Base theme configuration object
 */
const createBaseTheme = () => {
  return {
    typography,
    breakpoints: {
      values: breakpoints.values,
    },
    components: {
      // Shared component customizations
      MuiButton: {
        styleOverrides: {
          root: {
            borderRadius: 8,
            textTransform: 'none',
            fontWeight: typography.fontWeight.medium,
            padding: '8px 16px',
          },
          containedPrimary: {
            '&:hover': {
              boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.2)',
            },
          },
        },
        defaultProps: {
          disableElevation: true,
        },
      },
      MuiCard: {
        styleOverrides: {
          root: {
            borderRadius: 12,
            boxShadow: '0px 2px 8px rgba(0, 0, 0, 0.1)',
          },
        },
      },
      MuiTable: {
        styleOverrides: {
          root: {
            backgroundColor: 'transparent',
          },
        },
      },
      MuiTableCell: {
        styleOverrides: {
          head: {
            fontWeight: typography.fontWeight.medium,
          },
        },
      },
      MuiTextField: {
        styleOverrides: {
          root: {
            '& .MuiOutlinedInput-root': {
              borderRadius: 8,
            },
          },
        },
      },
      MuiAlert: {
        styleOverrides: {
          root: {
            borderRadius: 8,
          },
          standardSuccess: {
            backgroundColor: 'rgba(46, 125, 50, 0.1)',
            color: colors.success.dark,
          },
          standardWarning: {
            backgroundColor: 'rgba(237, 108, 2, 0.1)',
            color: colors.warning.dark,
          },
          standardError: {
            backgroundColor: 'rgba(211, 47, 47, 0.1)',
            color: colors.error.dark,
          },
          standardInfo: {
            backgroundColor: 'rgba(2, 136, 209, 0.1)',
            color: colors.info.dark,
          },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: {
            borderRadius: 16,
          },
        },
      },
    },
  };
};

// Light theme configuration
export const lightTheme = createTheme({
  ...createBaseTheme(),
  palette: {
    mode: 'light',
    primary: colors.primary,
    secondary: colors.secondary,
    error: colors.error,
    warning: colors.warning,
    info: colors.info,
    success: colors.success,
    background: {
      default: colors.background.default,
      paper: colors.background.paper,
    },
    text: {
      primary: colors.text.primary,
      secondary: colors.text.secondary,
      disabled: colors.text.disabled,
    },
  },
  components: {
    ...createBaseTheme().components,
    // Light mode specific component customizations
    MuiAppBar: {
      styleOverrides: {
        root: {
          backgroundColor: colors.primary.main,
          color: colors.text.white,
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          backgroundColor: colors.background.paper,
          borderRight: `1px solid ${colors.grey[200]}`,
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:nth-of-type(odd)': {
            backgroundColor: colors.grey[50],
          },
          '&:hover': {
            backgroundColor: colors.grey[100],
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderBottom: `1px solid ${colors.grey[200]}`,
        },
        head: {
          backgroundColor: colors.grey[100],
        },
      },
    },
  },
});

// Dark theme configuration
export const darkTheme = createTheme({
  ...createBaseTheme(),
  palette: {
    mode: 'dark',
    primary: colors.primary,
    secondary: colors.secondary,
    error: colors.error,
    warning: colors.warning,
    info: colors.info,
    success: colors.success,
    background: {
      default: colors.background.dark,
      paper: '#1e1e1e', // Slightly lighter than default dark background
    },
    text: {
      primary: 'rgba(255, 255, 255, 0.87)',
      secondary: 'rgba(255, 255, 255, 0.6)',
      disabled: 'rgba(255, 255, 255, 0.38)',
    },
  },
  components: {
    ...createBaseTheme().components,
    // Dark mode specific component customizations
    MuiAppBar: {
      styleOverrides: {
        root: {
          backgroundColor: '#1a1a1a',
          color: colors.text.white,
        },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: {
          backgroundColor: colors.background.dark,
          borderRight: '1px solid rgba(255, 255, 255, 0.12)',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:nth-of-type(odd)': {
            backgroundColor: 'rgba(255, 255, 255, 0.05)',
          },
          '&:hover': {
            backgroundColor: 'rgba(255, 255, 255, 0.1)',
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          borderBottom: '1px solid rgba(255, 255, 255, 0.12)',
        },
        head: {
          backgroundColor: 'rgba(255, 255, 255, 0.08)',
        },
      },
    },
    MuiAlert: {
      styleOverrides: {
        standardSuccess: {
          backgroundColor: 'rgba(46, 125, 50, 0.15)',
          color: '#81c784', // Lighter green for dark mode
        },
        standardWarning: {
          backgroundColor: 'rgba(237, 108, 2, 0.15)',
          color: '#ffb74d', // Lighter orange for dark mode
        },
        standardError: {
          backgroundColor: 'rgba(211, 47, 47, 0.15)',
          color: '#e57373', // Lighter red for dark mode
        },
        standardInfo: {
          backgroundColor: 'rgba(2, 136, 209, 0.15)',
          color: '#4fc3f7', // Lighter blue for dark mode
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          backgroundColor: '#1e1e1e', // Slightly lighter than default dark background
          boxShadow: '0px 2px 8px rgba(0, 0, 0, 0.5)',
        },
      },
    },
    MuiDivider: {
      styleOverrides: {
        root: {
          backgroundColor: 'rgba(255, 255, 255, 0.12)',
        },
      },
    },
  },
});

// Default export for easier importing
export default lightTheme;