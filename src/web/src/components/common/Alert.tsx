import React from 'react';
import { Alert as MuiAlert, AlertProps as MuiAlertProps, AlertTitle } from '@mui/material'; // @mui/material ^5.11.0
import styled from '@emotion/styled'; // @emotion/styled ^11.10.5
import { ErrorOutline, WarningAmber, InfoOutlined, CheckCircleOutline } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { colors } from '../../theme/colors';

/**
 * Props interface for the Alert component, extending Material-UI AlertProps
 */
interface AlertProps extends MuiAlertProps {
  /** Optional title for the alert */
  title?: string;
  /** Callback function when the alert is closed */
  onClose?: () => void;
  /** Custom action element to display in the alert */
  action?: React.ReactNode;
  /** Shadow depth for the alert component */
  elevation?: number;
  /** Visual style variant of the alert */
  variant?: 'standard' | 'filled' | 'outlined';
}

/**
 * Returns the appropriate icon component based on the alert severity
 */
const getAlertIcon = (severity: string): React.ReactNode => {
  switch (severity) {
    case 'error':
      return <ErrorOutline />;
    case 'warning':
      return <WarningAmber />;
    case 'info':
      return <InfoOutlined />;
    case 'success':
      return <CheckCircleOutline />;
    default:
      return null;
  }
};

/**
 * Styled version of Material-UI Alert with custom styling based on the application theme
 */
const StyledAlert = styled(MuiAlert)<AlertProps>`
  border-radius: 8px;
  padding: 10px 16px;
  margin: 12px 0;
  font-size: 0.875rem;
  font-weight: 400;
  line-height: 1.5;
  box-shadow: ${props => props.elevation ? `0px ${props.elevation}px ${props.elevation * 2}px rgba(0, 0, 0, 0.1)` : 'none'};
  
  /* Standard variant styles */
  &.MuiAlert-standardError {
    background-color: ${colors.error.light};
    color: ${colors.error.dark};
    border: 1px solid ${colors.error.main};
    .MuiAlert-icon {
      color: ${colors.error.main};
    }
  }
  
  &.MuiAlert-standardWarning {
    background-color: ${colors.warning.light};
    color: ${colors.warning.dark};
    border: 1px solid ${colors.warning.main};
    .MuiAlert-icon {
      color: ${colors.warning.main};
    }
  }
  
  &.MuiAlert-standardInfo {
    background-color: ${colors.info.light};
    color: ${colors.info.dark};
    border: 1px solid ${colors.info.main};
    .MuiAlert-icon {
      color: ${colors.info.main};
    }
  }
  
  &.MuiAlert-standardSuccess {
    background-color: ${colors.success.light};
    color: ${colors.success.dark};
    border: 1px solid ${colors.success.main};
    .MuiAlert-icon {
      color: ${colors.success.main};
    }
  }
  
  /* Filled variant styles */
  &.MuiAlert-filledError {
    background-color: ${colors.error.main};
    color: ${colors.text.white};
  }
  
  &.MuiAlert-filledWarning {
    background-color: ${colors.warning.main};
    color: ${colors.text.white};
  }
  
  &.MuiAlert-filledInfo {
    background-color: ${colors.info.main};
    color: ${colors.text.white};
  }
  
  &.MuiAlert-filledSuccess {
    background-color: ${colors.success.main};
    color: ${colors.text.white};
  }
  
  /* Outlined variant styles */
  &.MuiAlert-outlinedError {
    background-color: transparent;
    border: 1px solid ${colors.error.main};
    color: ${colors.error.dark};
    .MuiAlert-icon {
      color: ${colors.error.main};
    }
  }
  
  &.MuiAlert-outlinedWarning {
    background-color: transparent;
    border: 1px solid ${colors.warning.main};
    color: ${colors.warning.dark};
    .MuiAlert-icon {
      color: ${colors.warning.main};
    }
  }
  
  &.MuiAlert-outlinedInfo {
    background-color: transparent;
    border: 1px solid ${colors.info.main};
    color: ${colors.info.dark};
    .MuiAlert-icon {
      color: ${colors.info.main};
    }
  }
  
  &.MuiAlert-outlinedSuccess {
    background-color: transparent;
    border: 1px solid ${colors.success.main};
    color: ${colors.success.dark};
    .MuiAlert-icon {
      color: ${colors.success.main};
    }
  }
  
  /* Title styling */
  .MuiAlertTitle-root {
    font-weight: 600;
    margin-bottom: 6px;
    font-size: 1rem;
  }
  
  /* Action area styling */
  .MuiAlert-action {
    align-items: center;
    padding-top: 0;
    margin-right: -8px;
  }
  
  /* Message styling */
  .MuiAlert-message {
    padding: 8px 0;
  }
  
  /* Responsive adjustments */
  @media (max-width: 600px) {
    padding: 8px 12px;
    font-size: 0.8125rem;
    
    .MuiAlertTitle-root {
      font-size: 0.9375rem;
    }
  }
`;

/**
 * Enhanced alert component with custom styling and additional functionality 
 * for the self-healing data pipeline application.
 */
const Alert: React.FC<AlertProps> = ({
  title,
  children,
  severity = 'info',
  onClose,
  action,
  variant = 'standard',
  elevation = 1,
  ...props
}) => {
  const icon = getAlertIcon(severity as string);
  
  return (
    <StyledAlert
      severity={severity}
      variant={variant}
      elevation={elevation}
      icon={icon}
      onClose={onClose}
      action={action}
      {...props}
    >
      {title && <AlertTitle>{title}</AlertTitle>}
      {children}
    </StyledAlert>
  );
};

export default Alert;