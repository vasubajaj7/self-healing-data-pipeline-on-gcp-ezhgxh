import React from 'react';
import { styled } from '@mui/material/styles';
import { colors } from '../../theme/colors';

/**
 * Props for the Badge component
 */
interface BadgeProps {
  /** Text or node to display inside the badge */
  label: string | React.ReactNode;
  /** Visual style variant of the badge */
  variant?: 'filled' | 'outlined' | 'text';
  /** Color of the badge */
  color?: 'success' | 'warning' | 'error' | 'info' | 'default';
  /** Size of the badge */
  size?: 'small' | 'medium' | 'large';
  /** Additional CSS class name */
  className?: string;
  /** Additional inline styles */
  style?: React.CSSProperties;
}

/**
 * A styled component that forms the base of the Badge
 */
const StyledBadge = styled('span')<{
  variant: string;
  color: string;
  size: string;
}>(({ variant, color, size }) => ({
  // Base styling for all badges
  borderRadius: '16px',
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  fontWeight: 500,
  letterSpacing: '0.02em',
  textTransform: 'uppercase',
  lineHeight: 1.5,
  
  // Size variants
  ...(size === 'small' && {
    fontSize: '0.75rem',
    ...(variant === 'filled' && { padding: '2px 8px' }),
    ...(variant === 'outlined' && { padding: '1px 7px' }),
    ...(variant === 'text' && { padding: '2px 8px' }),
  }),
  
  ...(size === 'medium' && {
    fontSize: '0.875rem',
    ...(variant === 'filled' && { padding: '4px 12px' }),
    ...(variant === 'outlined' && { padding: '3px 11px' }),
    ...(variant === 'text' && { padding: '4px 12px' }),
  }),
  
  ...(size === 'large' && {
    fontSize: '1rem',
    ...(variant === 'filled' && { padding: '6px 16px' }),
    ...(variant === 'outlined' && { padding: '5px 15px' }),
    ...(variant === 'text' && { padding: '6px 16px' }),
  }),
  
  // Variant + color combinations
  ...(variant === 'filled' && {
    ...(color === 'success' && {
      backgroundColor: colors.status.healthy,
      color: colors.text.white,
    }),
    ...(color === 'warning' && {
      backgroundColor: colors.status.warning,
      color: colors.text.white,
    }),
    ...(color === 'error' && {
      backgroundColor: colors.status.error,
      color: colors.text.white,
    }),
    ...(color === 'info' && {
      backgroundColor: colors.status.processing,
      color: colors.text.white,
    }),
    ...(color === 'default' && {
      backgroundColor: colors.status.inactive,
      color: colors.text.white,
    }),
  }),
  
  ...(variant === 'outlined' && {
    border: '1px solid',
    backgroundColor: 'transparent',
    ...(color === 'success' && {
      borderColor: colors.status.healthy,
      color: colors.status.healthy,
    }),
    ...(color === 'warning' && {
      borderColor: colors.status.warning,
      color: colors.status.warning,
    }),
    ...(color === 'error' && {
      borderColor: colors.status.error,
      color: colors.status.error,
    }),
    ...(color === 'info' && {
      borderColor: colors.status.processing,
      color: colors.status.processing,
    }),
    ...(color === 'default' && {
      borderColor: colors.status.inactive,
      color: colors.status.inactive,
    }),
  }),
  
  ...(variant === 'text' && {
    backgroundColor: 'transparent',
    ...(color === 'success' && {
      color: colors.status.healthy,
    }),
    ...(color === 'warning' && {
      color: colors.status.warning,
    }),
    ...(color === 'error' && {
      color: colors.status.error,
    }),
    ...(color === 'info' && {
      color: colors.status.processing,
    }),
    ...(color === 'default' && {
      color: colors.status.inactive,
    }),
  }),
}));

/**
 * Badge component displays status indicators, labels, or counts with different colors and styles.
 * It provides visual representation for status, category, or count information.
 */
const Badge: React.FC<BadgeProps> = ({
  label,
  variant = 'filled',
  color = 'default',
  size = 'medium',
  className,
  style,
}) => {
  return (
    <StyledBadge
      variant={variant}
      color={color}
      size={size}
      className={className}
      style={style}
    >
      {label}
    </StyledBadge>
  );
};

export default Badge;