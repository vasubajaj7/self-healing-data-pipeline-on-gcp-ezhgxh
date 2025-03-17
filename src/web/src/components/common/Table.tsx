import React, { useState, useEffect, useMemo, useCallback } from 'react'; // react ^18.2.0
import {
  Table as MUITable,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  Paper,
  Checkbox,
  Box,
  Typography,
  Tooltip
} from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { ArrowUpward, ArrowDownward } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Pagination from './Pagination';
import NoData from './NoData';
import Spinner from './Spinner';
import { lightTheme } from '../../theme/theme';

/**
 * Interface for table column configuration
 */
export interface Column {
  id: string;
  label: string;
  numeric?: boolean;
  width?: string | number;
  minWidth?: string | number;
  maxWidth?: string | number;
  align?: string;
  sortable?: boolean;
  hidden?: boolean;
  format?: (value: any, row: any) => React.ReactNode;
  renderCell?: (value: any, row: any, index: number) => React.ReactNode;
}

/**
 * Props interface for the Table component
 */
export interface TableProps<T extends object = any> {
  columns: Column[];
  data: T[];
  loading?: boolean;
  error?: string | Error;
  emptyMessage?: string;
  title?: string;
  subtitle?: string;
  pagination?: boolean;
  initialPageSize?: number;
  pageSizeOptions?: number[];
  onPageChange?: (page: number, pageSize: number) => void;
  totalItems?: number;
  defaultSortBy?: string;
  defaultSortDirection?: string;
  onSortChange?: (column: string, direction: 'asc' | 'desc') => void;
  selectable?: boolean;
  selectedRows?: any[];
  onSelectionChange?: (selectedRows: any[]) => void;
  getRowId?: (row: T) => string | number;
  rowsPerPageLabel?: string;
  stickyHeader?: boolean;
  maxHeight?: string | number;
  dense?: boolean;
  className?: string;
  onRowClick?: (row: T, index: number) => void;
  actions?: React.ReactNode;
}

/**
 * Compares two values for descending sort order
 */
function descendingComparator<T>(a: T, b: T, orderBy: keyof T) {
  if (b[orderBy] < a[orderBy]) {
    return -1;
  }
  if (b[orderBy] > a[orderBy]) {
    return 1;
  }
  return 0;
}

/**
 * Returns a comparator function based on sort order
 */
function getComparator<Key extends keyof any>(
  order: 'asc' | 'desc',
  orderBy: Key
): (a: { [key in Key]: any }, b: { [key in Key]: any }) => number {
  return order === 'desc'
    ? (a, b) => descendingComparator(a, b, orderBy)
    : (a, b) => -descendingComparator(a, b, orderBy);
}

/**
 * Performs a stable sort on an array
 */
function stableSort<T>(array: T[], comparator: (a: T, b: T) => number) {
  const stabilizedThis = array.map((el, index) => [el, index] as [T, number]);
  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0]);
    if (order !== 0) return order;
    return a[1] - b[1];
  });
  return stabilizedThis.map((el) => el[0]);
}

/**
 * Styled container for the table with customized appearance
 */
const StyledTableContainer = styled(TableContainer, {
  shouldForwardProp: (prop) => prop !== 'maxHeight'
})<{ maxHeight?: string | number }>(({ theme, maxHeight }) => ({
  margin: theme.spacing(2, 0),
  boxShadow: theme.shadows[1],
  border: `1px solid ${theme.palette.divider}`,
  borderRadius: theme.shape.borderRadius,
  overflow: 'hidden',
  ...(maxHeight && {
    maxHeight,
    overflow: 'auto',
  }),
}));

/**
 * Styled table component with customized appearance
 */
const StyledTable = styled(MUITable, {
  shouldForwardProp: (prop) => prop !== 'dense'
})<{ dense?: boolean }>(({ dense }) => ({
  tableLayout: 'fixed',
  width: '100%',
}));

/**
 * Styled table header with customized appearance
 */
const StyledTableHead = styled(TableHead)(({ theme }) => ({
  backgroundColor: theme.palette.background.paper,
  fontWeight: 'bold'
}));

/**
 * Styled table cell with customized appearance
 */
const StyledTableCell = styled(TableCell, {
  shouldForwardProp: (prop) => 
    !['align', 'numeric', 'width', 'minWidth', 'maxWidth', 'dense'].includes(String(prop))
})<{ 
  align?: string;
  numeric?: boolean;
  width?: string | number;
  minWidth?: string | number;
  maxWidth?: string | number;
  dense?: boolean;
}>(({ theme, align, numeric, width, minWidth, maxWidth, dense }) => ({
  padding: dense ? theme.spacing(1, 2) : theme.spacing(1.5, 2),
  borderBottom: `1px solid ${theme.palette.divider}`,
  fontSize: '0.875rem',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  ...(align && { textAlign: align }),
  ...(numeric && { textAlign: 'right' }),
  ...(width && { width }),
  ...(minWidth && { minWidth }),
  ...(maxWidth && { maxWidth }),
  '&.MuiTableCell-head': {
    fontWeight: 500,
    backgroundColor: theme.palette.action.hover,
  },
}));

/**
 * Styled table row with customized appearance and hover effects
 */
const StyledTableRow = styled(TableRow, {
  shouldForwardProp: (prop) => !['clickable', 'selected'].includes(String(prop))
})<{ clickable?: boolean; selected?: boolean }>(({ theme, clickable, selected }) => ({
  transition: 'background-color 0.2s ease',
  cursor: clickable ? 'pointer' : 'default',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
  ...(selected && {
    backgroundColor: theme.palette.action.selected,
    '&:hover': {
      backgroundColor: theme.palette.action.selected,
    },
  }),
}));

/**
 * Component for the table header with sorting functionality
 */
const TableHeader: React.FC<{
  columns: Column[];
  order: 'asc' | 'desc';
  orderBy: string;
  onRequestSort: (property: string) => void;
  selectable: boolean;
  onSelectAllClick: (event: React.ChangeEvent<HTMLInputElement>) => void;
  numSelected: number;
  rowCount: number;
  dense: boolean;
}> = ({
  columns,
  order,
  orderBy,
  onRequestSort,
  selectable,
  onSelectAllClick,
  numSelected,
  rowCount,
  dense
}) => {
  const createSortHandler = (property: string) => () => {
    onRequestSort(property);
  };

  return (
    <StyledTableHead>
      <TableRow>
        {selectable && (
          <StyledTableCell padding="checkbox" dense={dense}>
            <Checkbox
              indeterminate={numSelected > 0 && numSelected < rowCount}
              checked={rowCount > 0 && numSelected === rowCount}
              onChange={onSelectAllClick}
              inputProps={{ 'aria-label': 'select all' }}
            />
          </StyledTableCell>
        )}
        {columns.map((column) => {
          if (column.hidden) return null;
          
          return (
            <StyledTableCell
              key={column.id}
              align={column.align}
              numeric={column.numeric}
              width={column.width}
              minWidth={column.minWidth}
              maxWidth={column.maxWidth}
              dense={dense}
            >
              {column.sortable ? (
                <TableSortLabel
                  active={orderBy === column.id}
                  direction={orderBy === column.id ? order : 'asc'}
                  onClick={createSortHandler(column.id)}
                  IconComponent={order === 'asc' ? ArrowUpward : ArrowDownward}
                >
                  {column.label}
                </TableSortLabel>
              ) : (
                column.label
              )}
            </StyledTableCell>
          );
        })}
      </TableRow>
    </StyledTableHead>
  );
};

/**
 * Component for the table toolbar with title and actions
 */
const TableToolbar: React.FC<{
  title?: string;
  subtitle?: string;
  actions?: React.ReactNode;
  numSelected: number;
  selectedActions?: React.ReactNode;
}> = ({
  title,
  subtitle,
  actions,
  numSelected,
  selectedActions
}) => {
  if (!title && !subtitle && !actions && !selectedActions) return null;

  return (
    <Box
      sx={{
        padding: (theme) => theme.spacing(1, 2),
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        borderBottom: (theme) => `1px solid ${theme.palette.divider}`
      }}
    >
      <Box>
        {numSelected > 0 ? (
          <Typography variant="subtitle1" component="div">
            {numSelected} selected
          </Typography>
        ) : (
          <>
            {title && (
              <Typography variant="h6" component="div" sx={{ fontWeight: 500, fontSize: '1rem' }}>
                {title}
              </Typography>
            )}
            {subtitle && (
              <Typography variant="body2" color="text.secondary" component="div" sx={{ fontSize: '0.75rem' }}>
                {subtitle}
              </Typography>
            )}
          </>
        )}
      </Box>
      <Box>
        {numSelected > 0 ? selectedActions : actions}
      </Box>
    </Box>
  );
};

/**
 * Main table component that combines all subcomponents
 */
function Table<T extends object = any>({
  columns,
  data,
  loading = false,
  error,
  emptyMessage = 'No data available',
  title,
  subtitle,
  pagination = true,
  initialPageSize = 10,
  pageSizeOptions = [5, 10, 25, 50],
  onPageChange,
  totalItems,
  defaultSortBy,
  defaultSortDirection = 'asc',
  onSortChange,
  selectable = false,
  selectedRows,
  onSelectionChange,
  getRowId = (row: any) => row.id,
  rowsPerPageLabel = 'Rows per page:',
  stickyHeader = false,
  maxHeight,
  dense = false,
  className,
  onRowClick,
  actions
}: TableProps<T>) {
  // State for sorting
  const [order, setOrder] = useState<'asc' | 'desc'>(
    defaultSortDirection === 'desc' ? 'desc' : 'asc'
  );
  const [orderBy, setOrderBy] = useState<string>(
    defaultSortBy || (columns[0]?.sortable ? columns[0].id : '')
  );

  // State for selection
  const [selected, setSelected] = useState<(string | number)[]>([]);

  // State for pagination
  const [page, setPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(initialPageSize);

  // Update selected rows if controlled externally
  useEffect(() => {
    if (selectedRows) {
      setSelected(selectedRows.map(row => getRowId(row as T)));
    }
  }, [selectedRows, getRowId]);

  // Handle sort request
  const handleRequestSort = useCallback((property: string) => {
    const isAsc = orderBy === property && order === 'asc';
    const newOrder = isAsc ? 'desc' : 'asc';
    setOrder(newOrder);
    setOrderBy(property);

    if (onSortChange) {
      onSortChange(property, newOrder);
    }
  }, [order, orderBy, onSortChange]);

  // Handle select all click
  const handleSelectAllClick = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      const newSelected = data.map(row => getRowId(row));
      setSelected(newSelected);
      if (onSelectionChange) {
        onSelectionChange(data);
      }
    } else {
      setSelected([]);
      if (onSelectionChange) {
        onSelectionChange([]);
      }
    }
  }, [data, getRowId, onSelectionChange]);

  // Handle click on a row for selection
  const handleRowSelect = useCallback((id: string | number, row: T) => {
    const selectedIndex = selected.indexOf(id);
    let newSelected: (string | number)[] = [];

    if (selectedIndex === -1) {
      newSelected = [...selected, id];
    } else {
      newSelected = selected.filter(item => item !== id);
    }

    setSelected(newSelected);

    if (onSelectionChange) {
      const selectedItems = data.filter(item => 
        newSelected.includes(getRowId(item))
      );
      onSelectionChange(selectedItems);
    }
  }, [selected, data, getRowId, onSelectionChange]);

  // Handle page change
  const handleChangePage = useCallback((newPage: number) => {
    setPage(newPage);
    if (onPageChange) {
      onPageChange(newPage, rowsPerPage);
    }
  }, [rowsPerPage, onPageChange]);

  // Handle rows per page change
  const handleChangeRowsPerPage = useCallback((newRowsPerPage: number) => {
    setRowsPerPage(newRowsPerPage);
    setPage(1);
    if (onPageChange) {
      onPageChange(1, newRowsPerPage);
    }
  }, [onPageChange]);

  // Calculate visible rows
  const visibleRows = useMemo(() => {
    if (onPageChange && totalItems !== undefined) {
      // Server-side pagination - just use data as is
      return data;
    }

    // Client-side sorting and pagination
    const sortedRows = orderBy 
      ? stableSort(data, getComparator(order, orderBy))
      : data;

    if (pagination) {
      const startIndex = (page - 1) * rowsPerPage;
      return sortedRows.slice(startIndex, startIndex + rowsPerPage);
    }

    return sortedRows;
  }, [data, order, orderBy, page, rowsPerPage, pagination, onPageChange, totalItems]);

  // Check if a row is selected
  const isSelected = (id: string | number) => selected.indexOf(id) !== -1;

  // Calculate actual total items
  const actualTotalItems = totalItems !== undefined ? totalItems : data.length;

  return (
    <Box>
      <TableToolbar
        title={title}
        subtitle={subtitle}
        actions={actions}
        numSelected={selected.length}
      />

      <StyledTableContainer 
        component={Paper} 
        maxHeight={maxHeight}
        className={className}
      >
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
            <Spinner size="medium" />
          </Box>
        ) : error ? (
          <Box sx={{ p: 2, color: 'error.main', textAlign: 'center' }}>
            <Typography color="error">
              {error instanceof Error ? error.message : error}
            </Typography>
          </Box>
        ) : data.length === 0 ? (
          <NoData message={emptyMessage} />
        ) : (
          <StyledTable 
            stickyHeader={stickyHeader}
            aria-label={title || "Data table"}
          >
            <TableHeader
              columns={columns}
              order={order}
              orderBy={orderBy}
              onRequestSort={handleRequestSort}
              selectable={selectable}
              onSelectAllClick={handleSelectAllClick}
              numSelected={selected.length}
              rowCount={data.length}
              dense={dense}
            />
            <TableBody>
              {visibleRows.map((row, index) => {
                const id = getRowId(row);
                const isItemSelected = isSelected(id);

                return (
                  <StyledTableRow
                    hover
                    key={id}
                    selected={isItemSelected}
                    clickable={!!onRowClick}
                    onClick={() => onRowClick && onRowClick(row, index)}
                  >
                    {selectable && (
                      <StyledTableCell padding="checkbox" dense={dense}>
                        <Checkbox
                          checked={isItemSelected}
                          onClick={(event) => {
                            event.stopPropagation();
                            handleRowSelect(id, row);
                          }}
                          inputProps={{ 'aria-labelledby': `table-checkbox-${id}` }}
                        />
                      </StyledTableCell>
                    )}
                    {columns.map((column) => {
                      if (column.hidden) return null;
                      
                      const value = row[column.id as keyof T];
                      
                      return (
                        <StyledTableCell
                          key={`${id}-${column.id}`}
                          align={column.align}
                          numeric={column.numeric}
                          width={column.width}
                          minWidth={column.minWidth}
                          maxWidth={column.maxWidth}
                          dense={dense}
                        >
                          {column.renderCell ? (
                            column.renderCell(value, row, index)
                          ) : column.format ? (
                            column.format(value, row)
                          ) : (
                            value !== undefined && value !== null ? value : ''
                          )}
                        </StyledTableCell>
                      );
                    })}
                  </StyledTableRow>
                );
              })}
            </TableBody>
          </StyledTable>
        )}
      </StyledTableContainer>

      {pagination && !loading && data.length > 0 && (
        <Pagination
          totalItems={actualTotalItems}
          page={page}
          pageSize={rowsPerPage}
          onPageChange={handleChangePage}
          onPageSizeChange={handleChangeRowsPerPage}
          pageSizeOptions={pageSizeOptions}
          compact={dense}
        />
      )}
    </Box>
  );
}

export default Table;