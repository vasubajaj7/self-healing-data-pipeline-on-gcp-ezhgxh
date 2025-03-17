import React from 'react';
import { CircularProgress, Box } from '@mui/material';
import { styled } from '@mui/material/styles';
// Importing theme for consistent styling
import { lightTheme } from '../../theme/theme';
// Importing colors for potential custom color overrides
import { colors } from '../../theme/colors';

// Define the props interface for the Spinner component
interface SpinnerProps {
  size?: number | 'small' | 'medium' | 'large';
  color?: 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning' | 'inherit';
  variant?: 'indeterminate' | 'determinate';
  value?: number;
  thickness?: number;
  overlay?: boolean;
  fullScreen?: boolean;
  label?: string;
  className?: string;
}

// Styled container for the spinner with support for overlay and fullscreen modes
const SpinnerContainer = styled(Box, {
  shouldForwardProp: (prop) => prop !== 'overlay' && prop !== 'fullScreen',
})<{ overlay?: boolean; fullScreen?: boolean }>(({ overlay, fullScreen }) => ({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  position: overlay || fullScreen ? 'absolute' : 'relative',
  top: overlay || fullScreen ? 0 : 'auto',
  left: overlay || fullScreen ? 0 : 'auto',
  right: overlay || fullScreen ? 0 : 'auto',
  bottom: overlay || fullScreen ? 0 : 'auto',
  width: fullScreen ? '100vw' : overlay ? '100%' : 'auto',
  height: fullScreen ? '100vh' : overlay ? '100%' : 'auto',
  backgroundColor: overlay || fullScreen ? 'rgba(255, 255, 255, 0.7)' : 'transparent',
  zIndex: fullScreen ? 1300 : overlay ? 10 : 1,
}));

// Styled component for the spinner label
const LabelText = styled(Box)(({ theme }) => ({
  marginTop: '8px',
  fontSize: '0.875rem',
  color: theme.palette.text.secondary,
  fontWeight: 500,
}));

/**
 * Spinner component that provides visual feedback during asynchronous operations
 * 
 * @param props - SpinnerProps interface
 * @returns React component
 */
const Spinner: React.FC<SpinnerProps> = ({
  size = 'medium',
  color = 'primary',
  variant = 'indeterminate',
  value = 0,
  thickness = 3.6,
  overlay = false,
  fullScreen = false,
  label,
  className,
}) => {
  // Convert string sizes to numeric values
  let numericSize: number;
  if (typeof size === 'string') {
    switch (size) {
      case 'small':
        numericSize = 24;
        break;
      case 'large':
        numericSize = 56;
        break;
      case 'medium':
      default:
        numericSize = 40;
        break;
    }
  } else {
    numericSize = size;
  }

  return (
    <SpinnerContainer overlay={overlay} fullScreen={fullScreen} className={className}>
      <CircularProgress
        size={numericSize}
        color={color}
        variant={variant}
        value={value}
        thickness={thickness}
        aria-label={label || 'Loading'}
        role="progressbar"
      />
      {label && <LabelText>{label}</LabelText>}
    </SpinnerContainer>
  );
};

export default Spinner;