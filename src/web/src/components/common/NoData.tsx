/**
 * NoData component
 * 
 * A reusable component that displays a standardized empty state when
 * no data is available to show in tables, charts, or other data-driven components.
 * Provides consistent visual feedback and optional actions for users.
 */
import React from 'react'; // react ^18.2.0
import { Box, Typography } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { InboxOutlined } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { grey } from '../../theme/colors';
import Button from './Button';

/**
 * Props for the NoData component
 */
interface NoDataProps {
  /** Primary message to display when no data is available */
  message?: string;
  /** Secondary message with additional context or instructions */
  subMessage?: string;
  /** Custom icon to display instead of the default */
  icon?: React.ReactNode;
  /** Optional action button configuration */
  action?: {
    label: string;
    onClick: () => void;
    variant?: 'text' | 'outlined' | 'contained';
    color?: 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning';
  };
  /** Custom height for the container */
  height?: number | string;
  /** Minimum height for the container */
  minHeight?: number | string;
  /** Custom padding for the container */
  padding?: number | string;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Styled container for the empty state display
 */
const NoDataContainer = styled(Box)(({ height, minHeight, padding }: {
  height?: number | string;
  minHeight?: number | string;
  padding?: number | string;
}) => ({
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  textAlign: 'center',
  width: '100%',
  height: height || 'auto',
  minHeight: minHeight || '200px',
  padding: padding || '24px',
  boxSizing: 'border-box',
}));

/**
 * Container for the empty state icon
 */
const IconContainer = styled(Box)({
  marginBottom: '16px',
  color: grey[400],
  fontSize: '64px',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
});

/**
 * Component that displays a standardized empty state when no data is available
 */
const NoData: React.FC<NoDataProps> = ({
  message = 'No data available',
  subMessage = 'There is no data to display at this time.',
  icon = <InboxOutlined fontSize="inherit" />,
  action,
  height,
  minHeight,
  padding,
  className,
}) => {
  return (
    <NoDataContainer 
      height={height} 
      minHeight={minHeight} 
      padding={padding} 
      className={className}
      role="region"
      aria-label="No data available"
    >
      <IconContainer>
        {icon}
      </IconContainer>
      
      <Typography 
        variant="h6" 
        color="text.primary" 
        gutterBottom 
        fontWeight={500}
      >
        {message}
      </Typography>
      
      {subMessage && (
        <Typography 
          variant="body2" 
          color="text.secondary" 
          sx={{ marginBottom: '16px' }}
        >
          {subMessage}
        </Typography>
      )}
      
      {action && (
        <Button
          variant={action.variant || 'contained'}
          color={action.color || 'primary'}
          onClick={action.onClick}
          size="medium"
          sx={{ marginTop: '8px' }}
        >
          {action.label}
        </Button>
      )}
    </NoDataContainer>
  );
};

export default NoData;