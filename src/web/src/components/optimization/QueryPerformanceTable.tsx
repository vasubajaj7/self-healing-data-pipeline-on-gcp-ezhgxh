import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Chip,
  TextField,
  InputAdornment,
  IconButton,
  Tooltip,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Search,
  FilterList,
  TrendingUp,
  TrendingDown,
  AttachMoney,
  Storage,
  Timer,
  CheckCircle,
  Warning,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { format } from 'date-fns'; // date-fns ^2.30.0
import Table from '../../components/common/Table';
import Card from '../../components/common/Card';
import useApi from '../../hooks/useApi';
import optimizationService from '../../services/api/optimizationService';
import { QueryPerformanceMetrics } from '../../types/optimization';

/**
 * Interface for filter state
 */
interface FilterState {
  search: string;
  minExecutionTime: number | null;
  maxExecutionTime: number | null;
  minBytesProcessed: number | null;
  maxBytesProcessed: number | null;
  minCost: number | null;
  maxCost: number | null;
  hasRecommendations: boolean | null;
  user: string;
  project: string;
}

/**
 * Interface for the props of the QueryPerformanceTable component
 */
interface QueryPerformanceTableProps {
  title?: string;
  subtitle?: string;
  timeRange?: { startDate: string; endDate: string };
  onQuerySelect?: (queryId: string) => void;
  className?: string;
}

/**
 * Formats a number of bytes into a human-readable string with appropriate units
 * @param bytes The number of bytes
 * @returns Formatted string with appropriate unit (KB, MB, GB, etc.)
 */
const formatBytes = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = 2;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
};

/**
 * Formats milliseconds into a human-readable duration string
 * @param ms The duration in milliseconds
 * @returns Formatted duration string
 */
const formatDuration = (ms: number): string => {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)}s`;
  }
  const minutes = Math.floor(ms / 60000);
  const seconds = ((ms % 60000) / 1000).toFixed(0);
  return `${minutes}m ${seconds}s`;
};

/**
 * Formats a cost value as a currency string
 * @param value The cost value
 * @returns Formatted currency string
 */
const formatCost = (value: number): string => {
  return `$${value.toFixed(2)}`;
};

/**
 * Styled Card component for the query performance table
 */
const StyledCard = styled(Card)({
  width: '100%',
  overflow: 'hidden',
  marginBottom: (theme) => theme.spacing(3),
});

/**
 * Container for filter controls
 */
const FilterContainer = styled(Box)(({ theme }) => ({
  padding: theme.spacing(2),
  backgroundColor: theme.palette.background.default,
  borderBottom: `1px solid ${theme.palette.divider}`,
}));

/**
 * Container for the table header
 */
const HeaderContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  padding: theme.spacing(2),
  borderBottom: `1px solid ${theme.palette.divider}`,
}));

/**
 * Container for title and subtitle
 */
const TitleContainer = styled(Box)({
  display: 'flex',
  flexDirection: 'column',
});

/**
 * Styled chip for recommendation indicator
 */
const RecommendationChip = styled(Chip)(({ theme }) => ({
  backgroundColor: theme.palette.warning.light,
  color: theme.palette.warning.contrastText,
  fontSize: '0.75rem',
}));

/**
 * Header component for the query performance table
 */
const TableHeader: React.FC<{
  title: string;
  subtitle: string;
  onToggleFilters: () => void;
  showFilters: boolean;
}> = ({ title, subtitle, onToggleFilters, showFilters }) => {
  return (
    <HeaderContainer>
      <TitleContainer>
        <Typography variant="h6" component="div">
          {title}
        </Typography>
        <Typography variant="subtitle2" color="textSecondary">
          {subtitle}
        </Typography>
      </TitleContainer>
      <Tooltip title={showFilters ? 'Hide Filters' : 'Show Filters'}>
        <IconButton onClick={onToggleFilters}>
          <FilterList />
        </IconButton>
      </Tooltip>
    </HeaderContainer>
  );
};

/**
 * Panel for filtering query performance data
 */
const FilterPanel: React.FC<{
  filters: FilterState;
  onFilterChange: (field: string, value: any) => void;
  onResetFilters: () => void;
}> = ({ filters, onFilterChange, onResetFilters }) => {
  return (
    <FilterContainer>
      <Box display="grid" gridTemplateColumns="repeat(auto-fit, minmax(200px, 1fr))" gap={2}>
        <TextField
          label="Search Queries"
          variant="outlined"
          size="small"
          value={filters.search}
          onChange={(e) => onFilterChange('search', e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Search />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          label="Min Execution Time (ms)"
          type="number"
          variant="outlined"
          size="small"
          value={filters.minExecutionTime === null ? '' : filters.minExecutionTime}
          onChange={(e) =>
            onFilterChange(
              'minExecutionTime',
              e.target.value === '' ? null : Number(e.target.value)
            )
          }
        />
        <TextField
          label="Max Execution Time (ms)"
          type="number"
          variant="outlined"
          size="small"
          value={filters.maxExecutionTime === null ? '' : filters.maxExecutionTime}
          onChange={(e) =>
            onFilterChange(
              'maxExecutionTime',
              e.target.value === '' ? null : Number(e.target.value)
            )
          }
        />
        <TextField
          label="Min Bytes Processed"
          type="number"
          variant="outlined"
          size="small"
          value={filters.minBytesProcessed === null ? '' : filters.minBytesProcessed}
          onChange={(e) =>
            onFilterChange(
              'minBytesProcessed',
              e.target.value === '' ? null : Number(e.target.value)
            )
          }
        />
        <TextField
          label="Max Bytes Processed"
          type="number"
          variant="outlined"
          size="small"
          value={filters.maxBytesProcessed === null ? '' : filters.maxBytesProcessed}
          onChange={(e) =>
            onFilterChange(
              'maxBytesProcessed',
              e.target.value === '' ? null : Number(e.target.value)
            )
          }
        />
        <TextField
          label="Min Cost"
          type="number"
          variant="outlined"
          size="small"
          value={filters.minCost === null ? '' : filters.minCost}
          onChange={(e) =>
            onFilterChange('minCost', e.target.value === '' ? null : Number(e.target.value))
          }
        />
        <TextField
          label="Max Cost"
          type="number"
          variant="outlined"
          size="small"
          value={filters.maxCost === null ? '' : filters.maxCost}
          onChange={(e) =>
            onFilterChange('maxCost', e.target.value === '' ? null : Number(e.target.value))
          }
        />
        <FormControl variant="outlined" size="small">
          <InputLabel id="user-filter-label">User</InputLabel>
          <Select
            labelId="user-filter-label"
            value={filters.user}
            onChange={(e) => onFilterChange('user', e.target.value)}
            label="User"
          >
            <MenuItem value="">
              <em>All Users</em>
            </MenuItem>
            <MenuItem value="user1">User 1</MenuItem>
            <MenuItem value="user2">User 2</MenuItem>
          </Select>
        </FormControl>
         <FormControl variant="outlined" size="small">
          <InputLabel id="project-filter-label">Project</InputLabel>
          <Select
            labelId="project-filter-label"
            value={filters.project}
            onChange={(e) => onFilterChange('project', e.target.value)}
            label="Project"
          >
            <MenuItem value="">
              <em>All Projects</em>
            </MenuItem>
            <MenuItem value="project1">Project 1</MenuItem>
            <MenuItem value="project2">Project 2</MenuItem>
          </Select>
        </FormControl>
        <Box display="flex" alignItems="center">
          <Typography variant="body2">Has Recommendations:</Typography>
          <Box ml={1}>
            <label>
              <input
                type="checkbox"
                checked={filters.hasRecommendations === true}
                onChange={(e) =>
                  onFilterChange(
                    'hasRecommendations',
                    e.target.checked ? true : null
                  )
                }
              />
              Yes
            </label>
          </Box>
        </Box>
      </Box>
      <Box mt={2} display="flex" justifyContent="flex-end">
        <Button variant="outlined" size="small" onClick={resetFilters}>
          Reset Filters
        </Button>
      </Box>
    </FilterContainer>
  );
};

/**
 * Table component that displays BigQuery query performance metrics with filtering and sorting capabilities
 */
const QueryPerformanceTable: React.FC<QueryPerformanceTableProps> = ({
  title = 'Query Performance',
  subtitle = 'Analyze and optimize your BigQuery queries',
  timeRange = { startDate: '7d', endDate: 'now' },
  onQuerySelect,
  className,
}) => {
  // Define state variables
  const [queries, setQueries] = useState<QueryPerformanceMetrics[]>([]);
  const [page, setPage] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [totalItems, setTotalItems] = useState<number>(0);
  const [sortBy, setSortBy] = useState<string>('executionTime');
  const [sortDirection, setSortDirection] = useState<string>('desc');
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    minExecutionTime: null,
    maxExecutionTime: null,
    minBytesProcessed: null,
    maxBytesProcessed: null,
    minCost: null,
    maxCost: null,
    hasRecommendations: null,
    user: '',
    project: '',
  });
  const [showFilters, setShowFilters] = useState<boolean>(false);

  // Use the useApi hook to handle API calls
  const { get, loading, error } = useApi();

  // Define the loadQueries function to fetch query performance metrics from the API
  const loadQueries = useCallback(async () => {
    try {
      const params = {
        page: page + 1,
        pageSize,
        sortBy,
        descending: sortDirection === 'desc',
        startDate: timeRange.startDate,
        endDate: timeRange.endDate,
        search: filters.search,
        minExecutionTime: filters.minExecutionTime,
        maxExecutionTime: filters.maxExecutionTime,
        minBytesProcessed: filters.minBytesProcessed,
        maxBytesProcessed: filters.maxBytesProcessed,
        minCost: filters.minCost,
        maxCost: filters.maxCost,
        hasRecommendations: filters.hasRecommendations,
        user: filters.user,
        project: filters.project,
      };

      const response = await get<any>(
        '/api/optimization/query-performance',
        { params }
      );

      setQueries(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (error) {
      console.error('Failed to load queries:', error);
    }
  }, [timeRange, page, pageSize, sortBy, sortDirection, filters, get]);

  // Use useEffect to call loadQueries when dependencies change
  useEffect(() => {
    loadQueries();
  }, [loadQueries]);

  // Define the handlePageChange function to handle page changes in the pagination
  const handlePageChange = (newPage: number, newPageSize: number) => {
    setPage(newPage);
    setPageSize(newPageSize);
  };

  // Define the handleSortChange function to handle sort changes in the table
  const handleSortChange = (column: string, direction: string) => {
    setSortBy(column);
    setSortDirection(direction);
    setPage(0);
  };

  // Define the handleRowClick function to handle row clicks in the table
  const handleRowClick = (row: any) => {
    if (onQuerySelect) {
      onQuerySelect(row.queryId);
    }
  };

  // Define the handleFilterChange function to handle filter changes
  const handleFilterChange = (field: string, value: any) => {
    setFilters((prevFilters) => ({
      ...prevFilters,
      [field]: value,
    }));
    setPage(0);
  };

  // Define the toggleFilters function to toggle the filter panel visibility
  const toggleFilters = () => {
    setShowFilters((prevShowFilters) => !prevShowFilters);
  };

  // Define the resetFilters function to reset all filters to default values
  const resetFilters = () => {
    setFilters({
      search: '',
      minExecutionTime: null,
      maxExecutionTime: null,
      minBytesProcessed: null,
      maxBytesProcessed: null,
      minCost: null,
      maxCost: null,
      hasRecommendations: null,
      user: '',
      project: '',
    });
    setPage(0);
  };

  // Define the table columns
  const columns = useMemo(
    () => [
      {
        id: 'queryText',
        label: 'Query',
        sortable: true,
        renderCell: (value: string) => (
          <Tooltip title={value} placement="top">
            <span>{value.substring(0, 100)}...</span>
          </Tooltip>
        ),
      },
      {
        id: 'executionTime',
        label: 'Execution Time',
        numeric: true,
        sortable: true,
        format: (value: number) => formatDuration(value),
      },
      {
        id: 'bytesProcessed',
        label: 'Bytes Processed',
        numeric: true,
        sortable: true,
        format: (value: number) => formatBytes(value),
      },
      {
        id: 'estimatedCost',
        label: 'Estimated Cost',
        numeric: true,
        sortable: true,
        format: (value: number) => formatCost(value),
      },
      {
        id: 'executionDateTime',
        label: 'Execution Date',
        sortable: true,
        format: (value: string) => format(new Date(value), 'MMM dd, yyyy HH:mm'),
      },
      {
        id: 'hasOptimizationRecommendations',
        label: 'Recommendations',
        sortable: false,
        align: 'center',
        width: '120px',
        renderCell: (value: boolean) =>
          value ? (
            <RecommendationChip icon={<CheckCircle />} label="Available" />
          ) : (
            <Chip icon={<Warning />} label="None" color="default" />
          ),
      },
    ],
    []
  );

  return (
    <StyledCard className={className}>
      <TableHeader
        title={title}
        subtitle={subtitle}
        onToggleFilters={toggleFilters}
        showFilters={showFilters}
      />
      {showFilters && (
        <FilterPanel
          filters={filters}
          onFilterChange={handleFilterChange}
          onResetFilters={resetFilters}
        />
      )}
      <Table
        columns={columns}
        data={queries}
        loading={loading}
        error={error}
        pagination
        initialPageSize={pageSize}
        pageSizeOptions={[5, 10, 25, 50]}
        onPageChange={handlePageChange}
        totalItems={totalItems}
        defaultSortBy={sortBy}
        defaultSortDirection={sortDirection}
        onSortChange={handleSortChange}
        onRowClick={handleRowClick}
      />
    </StyledCard>
  );
};

export default QueryPerformanceTable;