/**
 * Custom Button component that extends Material-UI Button with additional functionality
 * including loading states, custom styling, and consistent behavior.
 */
import React from 'react';
import { Button, ButtonProps, CircularProgress } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { lightTheme } from '../../theme/theme';

/**
 * Extended props interface for the Button component
 */
interface CustomButtonProps extends ButtonProps {
  /**
   * Whether to show a loading indicator
   */
  loading?: boolean;
  
  /**
   * Position of the loading indicator ('start' | 'end' | 'center')
   */
  loadingPosition?: 'start' | 'end' | 'center';
  
  /**
   * Size of the loading indicator in pixels
   */
  loadingSize?: number;
}

/**
 * Styled version of Material-UI Button with custom styling
 */
const StyledButton = styled(Button)(({ theme }) => ({
  // Base styling
  position: 'relative',
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  transition: 'background-color 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms, box-shadow 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms, border-color 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms',
  
  // Variant-specific styling
  '&.MuiButton-contained': {
    boxShadow: 'none',
    '&:hover': {
      boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.2)',
    },
  },
  
  '&.MuiButton-outlined': {
    '&:hover': {
      backgroundColor: 'rgba(25, 118, 210, 0.04)',
    },
  },
  
  '&.MuiButton-text': {
    '&:hover': {
      backgroundColor: 'rgba(25, 118, 210, 0.04)',
    },
  },
  
  // Color-specific styling
  '&.MuiButton-containedPrimary': {
    backgroundColor: theme.palette.primary.main,
    color: theme.palette.primary.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.primary.dark,
    },
  },
  
  '&.MuiButton-containedSecondary': {
    backgroundColor: theme.palette.secondary.main,
    color: theme.palette.secondary.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.secondary.dark,
    },
  },
  
  '&.MuiButton-containedError': {
    backgroundColor: theme.palette.error.main,
    color: theme.palette.error.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.error.dark,
    },
  },
  
  '&.MuiButton-containedWarning': {
    backgroundColor: theme.palette.warning.main,
    color: theme.palette.warning.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.warning.dark,
    },
  },
  
  '&.MuiButton-containedInfo': {
    backgroundColor: theme.palette.info.main,
    color: theme.palette.info.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.info.dark,
    },
  },
  
  '&.MuiButton-containedSuccess': {
    backgroundColor: theme.palette.success.main,
    color: theme.palette.success.contrastText,
    '&:hover': {
      backgroundColor: theme.palette.success.dark,
    },
  },
  
  // Size-specific styling
  '&.MuiButton-sizeSmall': {
    padding: '4px 10px',
    fontSize: '0.8125rem',
  },
  
  '&.MuiButton-sizeMedium': {
    padding: '6px 16px',
    fontSize: '0.875rem',
  },
  
  '&.MuiButton-sizeLarge': {
    padding: '8px 22px',
    fontSize: '0.9375rem',
  },
  
  // Disabled state
  '&.Mui-disabled': {
    opacity: 0.7,
  },
  
  // Full width styling
  '&.MuiButton-fullWidth': {
    width: '100%',
  },
}));

/**
 * Main button component with loading state and other enhancements
 */
const CustomButton: React.FC<CustomButtonProps> = ({
  children,
  variant = 'contained',
  color = 'primary',
  size = 'medium',
  loading = false,
  loadingPosition = 'center',
  loadingSize = 24,
  disabled,
  ...props
}) => {
  // Button is disabled when loading or explicitly disabled
  const isDisabled = loading || disabled;
  
  return (
    <StyledButton
      variant={variant}
      color={color}
      size={size}
      disabled={isDisabled}
      aria-busy={loading}
      {...props}
    >
      {loading && loadingPosition === 'start' && (
        <CircularProgress
          size={loadingSize}
          color="inherit"
          style={{ marginRight: 8 }}
        />
      )}
      
      {loadingPosition === 'center' && loading ? (
        <CircularProgress
          size={loadingSize}
          color="inherit"
        />
      ) : (
        children
      )}
      
      {loading && loadingPosition === 'end' && (
        <CircularProgress
          size={loadingSize}
          color="inherit"
          style={{ marginLeft: 8 }}
        />
      )}
    </StyledButton>
  );
};

// Default props
CustomButton.defaultProps = {
  variant: 'contained',
  color: 'primary',
  size: 'medium',
  loading: false,
  loadingPosition: 'center',
  loadingSize: 24,
};

export default CustomButton;