import React from 'react';
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import { status as statusColors } from '../../theme/colors';

// Props interface for the StatusIndicator component
interface StatusIndicatorProps {
  /** The status to display (healthy, warning, error, inactive, processing) */
  status: string;
  /** Size of the indicator in pixels */
  size?: number;
  /** Whether to show a text label next to the indicator */
  showLabel?: boolean;
  /** Custom label text (defaults to capitalized status if not provided) */
  label?: string;
  /** Tooltip text to display on hover */
  tooltip?: string;
  /** Additional CSS class for styling */
  className?: string;
}

// Styled component for the status indicator dot
const StatusDot = styled('span')<{ size: number; statusColor: string }>(
  ({ size, statusColor }) => ({
    width: `${size}px`,
    height: `${size}px`,
    borderRadius: '50%',
    backgroundColor: statusColor,
    display: 'inline-block',
    boxShadow: '0 0 4px rgba(0, 0, 0, 0.2)',
  })
);

// Styled component for the status text label
const StatusLabel = styled('span')({
  marginLeft: '8px',
  fontSize: '0.875rem',
  fontWeight: 500,
});

/**
 * StatusIndicator - A visual component that displays a colored indicator 
 * representing the status of a system component, pipeline, or data quality metric.
 */
const StatusIndicator: React.FC<StatusIndicatorProps> = ({
  status,
  size = 12,
  showLabel = false,
  label,
  tooltip,
  className,
}) => {
  // Map status to appropriate color
  const statusColor = statusColors[status as keyof typeof statusColors] || statusColors.inactive;
  
  // Generate label text - use provided label or capitalize status
  const labelText = label || `${status.charAt(0).toUpperCase()}${status.slice(1)}`;
  
  // Create content with dot and optional label
  const indicatorContent = (
    <Box display="flex" alignItems="center" flexShrink={0} className={className}>
      <StatusDot 
        size={size} 
        statusColor={statusColor}
        role="img"
        aria-label={`Status: ${status}`}
      />
      {showLabel && <StatusLabel>{labelText}</StatusLabel>}
    </Box>
  );

  // Wrap in tooltip if provided
  if (tooltip) {
    return (
      <Tooltip title={tooltip} arrow>
        {indicatorContent}
      </Tooltip>
    );
  }

  return indicatorContent;
};

export default StatusIndicator;