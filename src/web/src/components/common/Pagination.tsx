import React from 'react';
import { Box, Typography, IconButton } from '@mui/material'; // @mui/material ^5.11.0
import { FirstPage, LastPage, NavigateBefore, NavigateNext } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import Button from './Button';
import Select from './Select';
import { PAGINATION } from '../../utils/constants';

/**
 * Props interface for the Pagination component
 */
interface PaginationProps {
  /** Total number of items in the dataset */
  totalItems: number;
  /** Current page number (1-based) */
  page: number;
  /** Number of items per page */
  pageSize: number;
  /** Function called when page changes */
  onPageChange: (page: number) => void;
  /** Function called when page size changes */
  onPageSizeChange: (pageSize: number) => void;
  /** Available page size options */
  pageSizeOptions?: number[];
  /** Whether to show first/last page buttons */
  showFirstLastButtons?: boolean;
  /** Whether to use a compact layout */
  compact?: boolean;
  /** Whether the pagination controls are disabled */
  disabled?: boolean;
  /** Additional CSS class name */
  className?: string;
}

/**
 * Styled container for the pagination component
 */
const PaginationContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: theme.spacing(1.5),
  '&.compact': {
    padding: theme.spacing(0.75),
  }
}));

/**
 * Styled component for displaying pagination information
 */
const PageInfo = styled(Typography)(({ theme }) => ({
  ...theme.typography.body2,
  marginRight: theme.spacing(2),
  '&.compact': {
    ...theme.typography.caption,
    marginRight: theme.spacing(1),
  }
}));

/**
 * Styled container for navigation buttons
 */
const NavigationControls = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  '& > button': {
    margin: theme.spacing(0, 0.5),
  },
  '&.compact > button': {
    margin: theme.spacing(0, 0.25),
  }
}));

/**
 * Styled container for page size selection
 */
const PageSizeSelector = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  marginLeft: theme.spacing(2),
  '&.compact': {
    marginLeft: theme.spacing(1),
  }
}));

/**
 * A reusable pagination component for navigating through paginated data
 * in tables and lists throughout the self-healing data pipeline application.
 * 
 * The component provides:
 * - Page navigation controls (first, previous, next, last)
 * - Page size selection
 * - Pagination information display (showing X-Y of Z items)
 * 
 * @example
 * ```jsx
 * <Pagination
 *   totalItems={100}
 *   page={1}
 *   pageSize={20}
 *   onPageChange={(newPage) => setPage(newPage)}
 *   onPageSizeChange={(newSize) => setPageSize(newSize)}
 * />
 * ```
 */
const Pagination: React.FC<PaginationProps> = ({
  totalItems,
  page,
  pageSize,
  onPageChange,
  onPageSizeChange,
  pageSizeOptions = PAGINATION.PAGE_SIZE_OPTIONS,
  showFirstLastButtons = true,
  compact = false,
  disabled = false,
  className,
}) => {
  // Calculate total pages
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  
  // Calculate current range of items being displayed
  const startItem = totalItems === 0 ? 0 : (page - 1) * pageSize + 1;
  const endItem = Math.min(page * pageSize, totalItems);
  
  // Create page size options for the select component
  const sizeOptions = pageSizeOptions.map(size => ({
    value: size,
    label: `${size} per page`,
  }));
  
  return (
    <PaginationContainer className={`${compact ? 'compact' : ''} ${className || ''}`}>
      <PageInfo className={compact ? 'compact' : ''} color="textSecondary">
        {totalItems === 0 
          ? 'No items' 
          : `${startItem}-${endItem} of ${totalItems}`}
      </PageInfo>
      
      <NavigationControls className={compact ? 'compact' : ''}>
        {showFirstLastButtons && (
          <IconButton
            onClick={() => onPageChange(1)}
            disabled={page <= 1 || disabled}
            size={compact ? 'small' : 'medium'}
            aria-label="First page"
          >
            <FirstPage fontSize={compact ? 'small' : 'medium'} />
          </IconButton>
        )}
        
        <IconButton
          onClick={() => onPageChange(Math.max(1, page - 1))}
          disabled={page <= 1 || disabled}
          size={compact ? 'small' : 'medium'}
          aria-label="Previous page"
        >
          <NavigateBefore fontSize={compact ? 'small' : 'medium'} />
        </IconButton>
        
        <IconButton
          onClick={() => onPageChange(Math.min(totalPages, page + 1))}
          disabled={page >= totalPages || disabled}
          size={compact ? 'small' : 'medium'}
          aria-label="Next page"
        >
          <NavigateNext fontSize={compact ? 'small' : 'medium'} />
        </IconButton>
        
        {showFirstLastButtons && (
          <IconButton
            onClick={() => onPageChange(totalPages)}
            disabled={page >= totalPages || disabled}
            size={compact ? 'small' : 'medium'}
            aria-label="Last page"
          >
            <LastPage fontSize={compact ? 'small' : 'medium'} />
          </IconButton>
        )}
      </NavigationControls>
      
      <PageSizeSelector className={compact ? 'compact' : ''}>
        <Select
          label="Items per page"
          options={sizeOptions}
          value={pageSize}
          onChange={(value) => onPageSizeChange(Number(value))}
          size={compact ? 'small' : 'medium'}
          disabled={disabled}
          variant="outlined"
          sx={{ minWidth: 120 }}
        />
      </PageSizeSelector>
    </PaginationContainer>
  );
};

export default Pagination;