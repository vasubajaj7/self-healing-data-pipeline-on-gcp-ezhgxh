import React from 'react';
import {
  Select as MuiSelect,
  SelectProps,
  MenuItem,
  ListSubheader,
  FormControl,
  InputLabel,
  FormHelperText,
} from '@mui/material'; // ^5.11.0
import { styled } from '@mui/material/styles'; // ^5.11.0
import { lightTheme } from '../../theme/theme';

/**
 * Interface for select option items
 */
export interface SelectOption {
  value: string | number;
  label: string;
  disabled?: boolean;
  group?: string;
}

/**
 * Extended props interface for the Select component
 */
export interface CustomSelectProps extends Omit<SelectProps, 'onChange'> {
  label: string;
  options: SelectOption[];
  value: string | number | string[] | number[];
  onChange: (value: string | number | string[] | number[]) => void;
  error?: boolean;
  helperText?: React.ReactNode;
  fullWidth?: boolean;
  required?: boolean;
  disabled?: boolean;
  placeholder?: string;
  size?: 'small' | 'medium';
  groupBy?: boolean;
  renderValue?: (selected: unknown) => React.ReactNode;
}

/**
 * Styled version of Material-UI Select with custom styling
 */
const StyledSelect = styled(MuiSelect)(({ theme }) => ({
  '& .MuiOutlinedInput-notchedOutline': {
    borderRadius: '4px',
  },
  '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
    borderColor: theme.palette.primary.main,
    borderWidth: '2px',
  },
  '&:hover .MuiOutlinedInput-notchedOutline': {
    borderColor: theme.palette.mode === 'light' 
      ? 'rgba(0, 0, 0, 0.32)' 
      : 'rgba(255, 255, 255, 0.32)',
  },
  '&.Mui-error .MuiOutlinedInput-notchedOutline': {
    borderColor: theme.palette.error.main,
  },
  '&.Mui-disabled': {
    opacity: 0.7,
  },
  '& .MuiSelect-icon': {
    color: theme.palette.action.active,
  },
}));

/**
 * Styled version of Material-UI MenuItem for consistent option styling
 */
const StyledMenuItem = styled(MenuItem)(({ theme }) => ({
  padding: '6px 16px',
  fontSize: theme.typography.body1.fontSize,
  '&.Mui-selected': {
    backgroundColor: theme.palette.action.selected,
    '&.Mui-focusVisible': {
      backgroundColor: theme.palette.action.selected,
    },
    '&:hover': {
      backgroundColor: theme.palette.action.hover,
    },
  },
  '&.Mui-disabled': {
    opacity: 0.5,
  },
}));

/**
 * Styled version of Material-UI ListSubheader for option groups
 */
const StyledListSubheader = styled(ListSubheader)(({ theme }) => ({
  backgroundColor: theme.palette.background.paper,
  fontWeight: 500,
  lineHeight: '36px',
  color: theme.palette.text.primary,
}));

/**
 * A reusable Select component that extends Material-UI Select with custom styling and
 * additional functionality for the self-healing data pipeline application.
 * 
 * Features:
 * - Supports single and multiple selection
 * - Option grouping capability
 * - Placeholder text support
 * - Consistent styling with application theme
 * - Accessibility compliant
 * - Form validation integration
 */
const Select: React.FC<CustomSelectProps> = ({
  label,
  options,
  value,
  onChange,
  error = false,
  helperText,
  fullWidth = true,
  required = false,
  disabled = false,
  placeholder,
  variant = 'outlined',
  size = 'medium',
  groupBy = false,
  multiple = false,
  renderValue,
  ...rest
}) => {
  // Handle select change by extracting the value and calling onChange
  const handleChange = (event: any) => {
    onChange(event.target.value);
  };

  // Group options by their group property if groupBy is true
  const groupedOptions = React.useMemo(() => {
    if (!groupBy) return null;

    const groups: Record<string, SelectOption[]> = {};
    
    options.forEach(option => {
      const group = option.group || 'Other';
      if (!groups[group]) {
        groups[group] = [];
      }
      groups[group].push(option);
    });
    
    return groups;
  }, [options, groupBy]);

  // Generate a unique id for the select for accessibility
  const selectId = React.useMemo(() => `select-${label.toLowerCase().replace(/\s+/g, '-')}`, [label]);

  // Create custom renderValue function that handles placeholder and multiple selection
  const customRenderValue = (selected: unknown) => {
    if (renderValue) {
      return renderValue(selected);
    }
    
    if (placeholder && (!selected || (Array.isArray(selected) && selected.length === 0))) {
      return <em>{placeholder}</em>;
    }
    
    if (multiple && Array.isArray(selected)) {
      return selected
        .map(val => options.find(option => option.value === val)?.label || val)
        .join(', ');
    }
    
    return options.find(option => option.value === selected)?.label || selected;
  };

  return (
    <FormControl 
      fullWidth={fullWidth} 
      error={error} 
      disabled={disabled} 
      required={required}
      variant={variant as any}
      size={size}
    >
      <InputLabel id={`${selectId}-label`} htmlFor={selectId}>{label}</InputLabel>
      
      <StyledSelect
        labelId={`${selectId}-label`}
        id={selectId}
        value={value}
        onChange={handleChange}
        label={label}
        multiple={multiple}
        displayEmpty={!!placeholder}
        renderValue={customRenderValue}
        aria-describedby={helperText ? `${selectId}-helper-text` : undefined}
        {...rest}
      >
        {placeholder && !multiple && (
          <MenuItem value="" disabled>
            <em>{placeholder}</em>
          </MenuItem>
        )}
        
        {groupBy && groupedOptions
          ? (
            // Render options grouped by their group property
            Object.entries(groupedOptions).map(([group, groupOptions]) => [
              <StyledListSubheader key={`group-${group}`}>
                {group}
              </StyledListSubheader>,
              ...groupOptions.map(option => (
                <StyledMenuItem 
                  key={option.value} 
                  value={option.value}
                  disabled={option.disabled}
                >
                  {option.label}
                </StyledMenuItem>
              ))
            ]).flat()
          ) : (
            // Render options without grouping
            options.map(option => (
              <StyledMenuItem 
                key={option.value} 
                value={option.value}
                disabled={option.disabled}
              >
                {option.label}
              </StyledMenuItem>
            ))
          )
        }
      </StyledSelect>
      
      {helperText && (
        <FormHelperText id={`${selectId}-helper-text`}>{helperText}</FormHelperText>
      )}
    </FormControl>
  );
};

export default Select;