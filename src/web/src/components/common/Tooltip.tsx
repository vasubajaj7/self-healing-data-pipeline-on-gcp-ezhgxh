/**
 * CustomTooltip Component
 * 
 * A reusable tooltip component that extends Material-UI Tooltip with custom styling
 * and additional functionality for the self-healing data pipeline application.
 * 
 * Provides consistent tooltip appearance and behavior across the application while
 * supporting accessibility requirements.
 */
import React from 'react';
import { Tooltip, TooltipProps } from '@mui/material';
import { styled } from '@mui/material/styles';
import { lightTheme } from '../../theme/theme';

/**
 * Extended props interface for the custom tooltip component.
 * @extends TooltipProps from Material-UI
 */
interface CustomTooltipProps extends TooltipProps {
  /**
   * Maximum width of the tooltip content.
   * @default 220
   */
  maxWidth?: number | string;
  
  /**
   * Custom background color for the tooltip.
   * @default theme palette grey[700] or 'rgba(97, 97, 97, 0.92)'
   */
  backgroundColor?: string;
  
  /**
   * Custom text color for the tooltip content.
   * @default theme palette common.white or '#fff'
   */
  textColor?: string;
}

/**
 * Styled version of Material-UI Tooltip with custom styling options.
 */
const StyledTooltip = styled(Tooltip)<{
  maxWidth?: number | string;
  backgroundColor?: string;
  textColor?: string;
}>(({ theme, maxWidth, backgroundColor, textColor }) => ({
  '& .MuiTooltip-tooltip': {
    backgroundColor: backgroundColor || (theme.palette.grey?.[700] || 'rgba(97, 97, 97, 0.92)'),
    color: textColor || (theme.palette.common?.white || '#fff'),
    fontSize: theme.typography?.caption?.fontSize || '0.75rem',
    padding: '8px 12px',
    maxWidth: maxWidth || 220,
    borderRadius: theme.shape?.borderRadius || 4,
    boxShadow: theme.shadows?.[1] || '0px 2px 4px rgba(0, 0, 0, 0.2)',
    wordWrap: 'break-word',
    fontWeight: theme.typography?.fontWeightRegular || 400,
    lineHeight: 1.5,
  },
  '& .MuiTooltip-arrow': {
    color: backgroundColor || (theme.palette.grey?.[700] || 'rgba(97, 97, 97, 0.92)'),
  },
}));

/**
 * A custom tooltip component that extends Material-UI's Tooltip with enhanced styling and features.
 * Provides consistent tooltip appearance and behavior across the application.
 * 
 * @example
 * ```tsx
 * <CustomTooltip title="This is a tooltip">
 *   <Button>Hover me</Button>
 * </CustomTooltip>
 * ```
 */
const CustomTooltip: React.FC<CustomTooltipProps> = ({
  children,
  title,
  maxWidth = 220,
  backgroundColor,
  textColor,
  arrow = true,
  placement = 'top',
  enterDelay = 300,
  leaveDelay = 100,
  ...props
}) => {
  return (
    <StyledTooltip
      title={title}
      maxWidth={maxWidth}
      backgroundColor={backgroundColor}
      textColor={textColor}
      arrow={arrow}
      placement={placement}
      enterDelay={enterDelay}
      leaveDelay={leaveDelay}
      {...props}
    >
      {children}
    </StyledTooltip>
  );
};

export default CustomTooltip;