import React from 'react';
import { Box, useMediaQuery } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useThemeContext } from '../../contexts/ThemeContext';

/**
 * Props for the PageContainer component
 */
interface PageContainerProps {
  /** Content to be rendered inside the container */
  children: React.ReactNode;
  /** Maximum width of the container, similar to MUI Container maxWidth prop */
  maxWidth?: string | false;
  /** Whether to disable default padding */
  disablePadding?: boolean;
  /** Additional styles to apply to the container */
  sx?: object;
}

/**
 * Container component that wraps page content with consistent styling
 * Provides responsive padding and proper theming across the application
 */
const Container = styled(Box)(({ theme }) => ({
  minHeight: 'calc(100vh - 64px)', // Subtract header height
  width: '100%',
  boxSizing: 'border-box',
  display: 'flex',
  flexDirection: 'column',
}));

/**
 * A container component that provides consistent padding, spacing, and styling for page content.
 * Serves as a wrapper for all page content within the main layout to ensure visual consistency.
 * 
 * @param {PageContainerProps} props - The component props
 * @returns {JSX.Element} Rendered container with children
 */
const PageContainer: React.FC<PageContainerProps> = ({
  children,
  maxWidth = 'lg',
  disablePadding = false,
  sx = {},
}) => {
  // Access theme context for current theme
  const { theme } = useThemeContext();
  
  // Check if viewport is mobile using useMediaQuery
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));
  
  return (
    <Container
      sx={{
        // Apply responsive padding based on screen size
        padding: disablePadding ? 0 : (isMobile ? 2 : 3),
        // Apply maxWidth if provided (similar to MUI Container)
        maxWidth: maxWidth !== false ? maxWidth : '100%',
        // Center container when maxWidth is set
        margin: maxWidth !== false ? '0 auto' : 0,
        // Apply background color based on current theme
        backgroundColor: theme.palette.background.default,
        // Apply any additional styles passed via sx prop
        ...sx,
      }}
    >
      {children}
    </Container>
  );
};

export default PageContainer;