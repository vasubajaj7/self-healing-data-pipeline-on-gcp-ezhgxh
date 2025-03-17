import React from 'react'; // react ^18.2.0
import { TextField, TextFieldProps } from '@mui/material'; // @mui/material ^5.11.0
import { InputAdornment } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { FormHelperText } from '@mui/material'; // @mui/material ^5.11.0
import { lightTheme } from '../../theme/theme';

/**
 * Extended props interface for the Input component
 */
interface CustomInputProps extends Omit<TextFieldProps, 'onChange'> {
  label: string;
  value: string | number;
  onChange: (value: string) => void;
  error?: boolean;
  helperText?: React.ReactNode;
  fullWidth?: boolean;
  required?: boolean;
  disabled?: boolean;
  placeholder?: string;
  type?: 'text' | 'password' | 'email' | 'number' | 'tel' | 'url' | 'date' | 'datetime-local' | 'time';
  startAdornment?: React.ReactNode;
  endAdornment?: React.ReactNode;
  size?: 'small' | 'medium';
  multiline?: boolean;
  rows?: number;
  maxRows?: number;
  autoFocus?: boolean;
  onBlur?: React.FocusEventHandler<HTMLInputElement>;
  onFocus?: React.FocusEventHandler<HTMLInputElement>;
  onKeyDown?: React.KeyboardEventHandler<HTMLInputElement>;
  onKeyUp?: React.KeyboardEventHandler<HTMLInputElement>;
}

/**
 * Styled version of Material-UI TextField with custom styling
 */
const StyledTextField = styled(TextField)(({ theme }) => ({
  '& .MuiOutlinedInput-root': {
    borderRadius: '4px',
    fontFamily: theme.typography.fontFamily,
    fontSize: theme.typography.fontSize,
    
    '&:hover .MuiOutlinedInput-notchedOutline': {
      borderColor: theme.palette.mode === 'light' 
        ? 'rgba(0, 0, 0, 0.32)' 
        : 'rgba(255, 255, 255, 0.32)',
    },
    
    '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
      borderColor: theme.palette.primary.main,
    },
    
    '&.Mui-error .MuiOutlinedInput-notchedOutline': {
      borderColor: theme.palette.error.main,
    },
    
    '&.Mui-disabled': {
      opacity: 0.7,
    },
    
    '& input': {
      padding: '10px 14px',
    },
    
    '&.MuiInputBase-sizeSmall input': {
      padding: '8px 10px',
    }
  },
  
  '& .MuiInputLabel-root': {
    fontSize: theme.typography.body1.fontSize,
    
    '&.Mui-focused': {
      color: theme.palette.primary.main,
      transform: 'none',
    },
    
    '&.Mui-error': {
      color: theme.palette.error.main,
    }
  },
  
  '& .MuiFormHelperText-root': {
    margin: '4px 0 0 0',
    fontSize: theme.typography.caption.fontSize,
    
    '&.Mui-error': {
      color: theme.palette.error.main,
    }
  },
  
  '&.MuiTextField-root': {
    marginBottom: '16px',
  }
}));

/**
 * Input component with validation and error handling
 * A reusable form input component that extends Material-UI TextField with custom styling 
 * and additional functionality.
 * 
 * @param {CustomInputProps} props - The props for the Input component
 * @returns {JSX.Element} The rendered Input component
 */
const Input: React.FC<CustomInputProps> = ({
  label,
  value,
  onChange,
  error = false,
  helperText,
  fullWidth = true,
  required = false,
  disabled = false,
  placeholder,
  type = 'text',
  startAdornment,
  endAdornment,
  size = 'medium',
  multiline = false,
  rows,
  maxRows,
  autoFocus = false,
  onBlur,
  onFocus,
  onKeyDown,
  onKeyUp,
  ...rest
}) => {
  /**
   * Handles input change events, extracting the value and calling the onChange callback
   */
  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange(event.target.value);
  };

  return (
    <StyledTextField
      label={label}
      value={value}
      onChange={handleChange}
      error={error}
      helperText={helperText}
      fullWidth={fullWidth}
      required={required}
      disabled={disabled}
      placeholder={placeholder}
      type={type}
      size={size}
      multiline={multiline}
      rows={rows}
      maxRows={maxRows}
      autoFocus={autoFocus}
      onBlur={onBlur}
      onFocus={onFocus}
      onKeyDown={onKeyDown}
      onKeyUp={onKeyUp}
      variant="outlined"
      InputProps={{
        startAdornment: startAdornment ? (
          <InputAdornment position="start">{startAdornment}</InputAdornment>
        ) : undefined,
        endAdornment: endAdornment ? (
          <InputAdornment position="end">{endAdornment}</InputAdornment>
        ) : undefined,
      }}
      // Accessibility attributes
      aria-required={required}
      aria-invalid={error}
      {...rest}
    />
  );
};

export default Input;