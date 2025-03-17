import React from 'react';
import { 
  Dialog, 
  DialogProps, 
  DialogTitle, 
  DialogContent, 
  DialogActions,
  IconButton,
  Typography,
} from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import CloseIcon from '@mui/icons-material/Close'; // @mui/icons-material ^5.11.0
import Button from './Button';
import { lightTheme } from '../../theme/theme';

/**
 * Extended props interface for the Modal component
 */
interface ModalProps extends DialogProps {
  /**
   * Title to display in the modal header
   */
  title?: string | React.ReactNode;
  
  /**
   * Content to display in the modal body
   */
  children?: React.ReactNode;
  
  /**
   * Action buttons to display in the modal footer
   */
  actions?: React.ReactNode;
  
  /**
   * Whether to show the close button in the header
   */
  showCloseButton?: boolean;
  
  /**
   * Custom padding for the content area
   */
  contentPadding?: string | number;
  
  /**
   * Maximum width of the modal
   */
  maxWidth?: 'xs' | 'sm' | 'md' | 'lg' | 'xl' | false;
  
  /**
   * Whether the modal should take up the full width of its container
   */
  fullWidth?: boolean;
  
  /**
   * Whether to disable closing the modal when clicking the backdrop
   */
  disableBackdropClick?: boolean;
  
  /**
   * Whether to disable closing the modal when pressing the Escape key
   */
  disableEscapeKeyDown?: boolean;
  
  /**
   * Callback fired when the modal is closed
   */
  onClose?: () => void;
}

/**
 * Styled version of Material-UI Dialog with custom styling
 */
const StyledDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    borderRadius: '8px',
    boxShadow: '0px 11px 15px -7px rgba(0,0,0,0.2), 0px 24px 38px 3px rgba(0,0,0,0.14), 0px 9px 46px 8px rgba(0,0,0,0.12)',
  },
  '& .MuiDialog-paperScrollPaper': {
    maxHeight: 'calc(100% - 64px)',
  },
  '& .MuiDialog-paperWidthXs': {
    maxWidth: '444px',
  },
  '& .MuiDialog-paperWidthSm': {
    maxWidth: '600px',
  },
  '& .MuiDialog-paperWidthMd': {
    maxWidth: '960px',
  },
  '& .MuiDialog-paperWidthLg': {
    maxWidth: '1280px',
  },
  '& .MuiDialog-paperWidthXl': {
    maxWidth: '1920px',
  },
  '& .MuiDialog-paperFullWidth': {
    width: 'calc(100% - 64px)',
  },
}));

/**
 * Styled version of DialogTitle with custom styling
 */
const StyledDialogTitle = styled(DialogTitle)(({ theme }) => ({
  padding: '16px 24px',
  borderBottom: `1px solid ${theme.palette.divider}`,
  '& .titleContainer': {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  '& .title': {
    fontWeight: theme.typography.fontWeight.medium,
    flex: 1,
  },
  '& .closeButton': {
    color: theme.palette.grey[500],
    marginLeft: theme.spacing(1),
  },
}));

/**
 * Styled version of DialogContent with custom styling and dynamic padding
 */
const StyledDialogContent = styled(DialogContent, {
  shouldForwardProp: (prop) => prop !== 'contentPadding',
})<{ contentPadding?: string | number }>(({ theme, contentPadding }) => ({
  padding: contentPadding,
  overflow: 'auto',
  '&.MuiDialogContent-dividers': {
    borderTop: `1px solid ${theme.palette.divider}`,
    borderBottom: `1px solid ${theme.palette.divider}`,
  },
}));

/**
 * Styled version of DialogActions with custom styling
 */
const StyledDialogActions = styled(DialogActions)(({ theme }) => ({
  padding: '16px 24px',
  borderTop: `1px solid ${theme.palette.divider}`,
  display: 'flex',
  justifyContent: 'flex-end',
  '& > *:not(:first-of-type)': {
    marginLeft: theme.spacing(1),
  },
}));

/**
 * Modal component with customizable title, content, actions, and styling.
 * Extends Material-UI Dialog with additional functionality.
 */
const Modal: React.FC<ModalProps> = ({
  title,
  children,
  actions,
  showCloseButton = true,
  contentPadding = '24px',
  maxWidth = 'sm',
  fullWidth = true,
  disableBackdropClick = false,
  disableEscapeKeyDown = false,
  onClose,
  ...rest
}) => {
  // Handle Dialog onClose events to respect disableBackdropClick prop
  const handleDialogClose = (_: any, reason: 'backdropClick' | 'escapeKeyDown') => {
    if (reason === 'backdropClick' && disableBackdropClick) {
      return;
    }
    
    if (onClose) {
      onClose();
    }
  };

  return (
    <StyledDialog
      maxWidth={maxWidth}
      fullWidth={fullWidth}
      onClose={handleDialogClose}
      disableEscapeKeyDown={disableEscapeKeyDown}
      aria-labelledby={title ? 'modal-title' : undefined}
      {...rest}
    >
      {title && (
        <StyledDialogTitle id="modal-title">
          <div className="titleContainer">
            <Typography variant="h6" component="h2" className="title">
              {title}
            </Typography>
            {showCloseButton && onClose && (
              <IconButton 
                aria-label="close" 
                onClick={onClose}
                size="small"
                className="closeButton"
              >
                <CloseIcon />
              </IconButton>
            )}
          </div>
        </StyledDialogTitle>
      )}
      <StyledDialogContent contentPadding={contentPadding} dividers={!!title && !!actions}>
        {children}
      </StyledDialogContent>
      {actions && <StyledDialogActions>{actions}</StyledDialogActions>}
    </StyledDialog>
  );
};

// Default props
Modal.defaultProps = {
  maxWidth: 'sm',
  fullWidth: true,
  showCloseButton: true,
  contentPadding: '24px',
  disableBackdropClick: false,
  disableEscapeKeyDown: false,
};

export default Modal;