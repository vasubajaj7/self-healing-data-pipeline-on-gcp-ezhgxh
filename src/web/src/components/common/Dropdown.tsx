import React, { useState, useRef } from 'react';
import { 
  Menu, 
  MenuItem, 
  Button, 
  IconButton, 
  ClickAwayListener, 
  Paper, 
  Popper, 
  Grow,
  Divider
} from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'; // @mui/icons-material ^5.11.0
import MoreVertIcon from '@mui/icons-material/MoreVert'; // @mui/icons-material ^5.11.0
import { lightTheme } from '../../theme/theme';

/**
 * Interface for dropdown menu items
 */
interface DropdownItem {
  label: string;
  value?: string | number;
  icon?: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  divider?: boolean;
}

/**
 * Props interface for the Dropdown component
 */
interface DropdownProps {
  items: DropdownItem[];
  label?: string;
  icon?: React.ReactNode;
  variant?: 'text' | 'outlined' | 'contained';
  color?: 'primary' | 'secondary' | 'error' | 'warning' | 'info' | 'success' | 'default';
  size?: 'small' | 'medium' | 'large';
  iconOnly?: boolean;
  placement?: 'bottom-start' | 'bottom' | 'bottom-end' | 'top-start' | 'top' | 'top-end' | 'left-start' | 'left' | 'left-end' | 'right-start' | 'right' | 'right-end';
  width?: number | string;
  maxHeight?: number | string;
  disabled?: boolean;
  onOpen?: () => void;
  onClose?: () => void;
  className?: string;
  menuClassName?: string;
  buttonProps?: object;
  menuProps?: object;
}

// Styled version of Material-UI Menu with custom styling
const StyledMenu = styled(Menu)(({ theme }) => ({
  '& .MuiPaper-root': {
    backgroundColor: theme.palette.background.paper,
    borderRadius: theme.shape.borderRadius,
    boxShadow: theme.shadows[2],
    marginTop: '4px',
    overflow: 'auto',
  },
  '& .MuiList-root': {
    padding: '4px 0',
  },
}));

// Styled version of Material-UI MenuItem for consistent option styling
const StyledMenuItem = styled(MenuItem, {
  shouldForwardProp: (prop) => prop !== 'hasIcon',
})<{ hasIcon?: boolean }>(({ theme, hasIcon }) => ({
  padding: '8px 16px',
  fontSize: '0.875rem',
  minHeight: '40px',
  transition: 'background-color 150ms cubic-bezier(0.4, 0, 0.2, 1) 0ms',
  '&:hover': {
    backgroundColor: `rgba(${theme.palette.primary.main}, 0.08)`,
  },
  '&.Mui-disabled': {
    opacity: 0.6,
    pointerEvents: 'none',
  },
  ...(hasIcon && {
    '& .MuiSvgIcon-root, & > svg': {
      marginRight: '8px',
      fontSize: '1.25rem',
      color: theme.palette.text.secondary,
    },
  }),
}));

/**
 * Dropdown component with trigger button and menu
 * 
 * A reusable dropdown component that provides a customizable dropdown menu
 * with various styling options and integration with the application's theme.
 * Supports both simple selection and complex menu structures.
 */
const Dropdown: React.FC<DropdownProps> = ({
  items,
  label,
  icon,
  variant = 'outlined',
  color = 'primary',
  size = 'medium',
  iconOnly = false,
  placement = 'bottom-start',
  width,
  maxHeight,
  disabled = false,
  onOpen,
  onClose,
  className,
  menuClassName,
  buttonProps = {},
  menuProps = {},
}) => {
  // State for tracking open/closed state of dropdown
  const [open, setOpen] = useState<boolean>(false);
  
  // Reference to track the anchor element for the dropdown
  const anchorRef = useRef<HTMLButtonElement>(null);

  // Handle click events on the trigger button to toggle dropdown
  const handleToggle = () => {
    const newOpen = !open;
    setOpen(newOpen);
    if (newOpen && onOpen) {
      onOpen();
    } else if (!newOpen && onClose) {
      onClose();
    }
  };

  // Handle click away events to close the dropdown
  const handleClose = (event: Event | React.SyntheticEvent) => {
    if (
      anchorRef.current &&
      anchorRef.current.contains(event.target as HTMLElement)
    ) {
      return;
    }
    setOpen(false);
    if (onClose) onClose();
  };

  // Handle item click events and close dropdown after selection
  const handleMenuItemClick = (
    onClick?: () => void,
    event?: React.MouseEvent<HTMLLIElement>
  ) => {
    if (onClick) onClick();
    setOpen(false);
    if (onClose) onClose();
  };

  // Handle keyboard navigation (Escape to close)
  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'Escape') {
      setOpen(false);
      if (onClose) onClose();
    }
  };

  // Generate a unique ID for the menu
  const menuId = `dropdown-menu-${Math.random().toString(36).substr(2, 9)}`;

  // Create paper props including width and maxHeight
  const paperProps = {
    style: {
      maxHeight: maxHeight,
      width: width,
    },
  };

  return (
    <div className={className}>
      {iconOnly ? (
        // Icon-only button as trigger
        <IconButton
          ref={anchorRef}
          aria-controls={open ? menuId : undefined}
          aria-expanded={open ? 'true' : undefined}
          aria-haspopup="true"
          onClick={handleToggle}
          disabled={disabled}
          color={color}
          size={size}
          title={label || 'Options'} // Tooltip for accessibility
          {...buttonProps}
        >
          {icon || <MoreVertIcon />}
        </IconButton>
      ) : (
        // Button with text and optional icon as trigger
        <Button
          ref={anchorRef}
          aria-controls={open ? menuId : undefined}
          aria-expanded={open ? 'true' : undefined}
          aria-haspopup="true"
          onClick={handleToggle}
          variant={variant}
          color={color}
          size={size}
          disabled={disabled}
          endIcon={<KeyboardArrowDownIcon />}
          {...buttonProps}
        >
          {icon && <span style={{ marginRight: 8 }}>{icon}</span>}
          {label}
        </Button>
      )}

      <Popper
        open={open}
        anchorEl={anchorRef.current}
        role={undefined}
        placement={placement}
        transition
        disablePortal
        style={{ zIndex: 1300 }}
      >
        {({ TransitionProps }) => (
          <Grow
            {...TransitionProps}
            style={{
              transformOrigin:
                placement === 'bottom-start' ? 'left top' : 'left bottom',
            }}
          >
            <Paper elevation={8}>
              <ClickAwayListener onClickAway={handleClose}>
                <StyledMenu
                  id={menuId}
                  open={open}
                  onClose={handleClose}
                  onKeyDown={handleKeyDown}
                  className={menuClassName}
                  MenuListProps={{
                    'aria-labelledby': anchorRef.current?.id,
                    role: 'menu',
                    dense: true,
                  }}
                  anchorEl={anchorRef.current}
                  PaperProps={paperProps}
                  {...menuProps}
                >
                  {items.map((item, index) => (
                    <React.Fragment key={`dropdown-item-${index}`}>
                      <StyledMenuItem
                        onClick={(event) =>
                          handleMenuItemClick(item.onClick, event)
                        }
                        disabled={item.disabled}
                        hasIcon={!!item.icon}
                        role="menuitem"
                        data-value={item.value}
                      >
                        {item.icon}
                        {item.label}
                      </StyledMenuItem>
                      {item.divider && <Divider style={{ margin: '4px 0' }} />}
                    </React.Fragment>
                  ))}
                </StyledMenu>
              </ClickAwayListener>
            </Paper>
          </Grow>
        )}
      </Popper>
    </div>
  );
};

export default Dropdown;