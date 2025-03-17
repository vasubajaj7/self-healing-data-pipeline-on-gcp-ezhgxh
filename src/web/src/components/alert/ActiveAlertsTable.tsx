import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  TextField,
  InputAdornment,
  IconButton,
  Chip,
  Tooltip,
  Menu,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Checkbox,
  ListItemText,
  OutlinedInput,
} from '@mui/material'; // @mui/material ^5.11.0
import { Search, FilterList, Refresh } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../../components/common/Table';
import Badge from '../../components/common/Badge';
import {
  Alert,
  AlertSeverity,
  AlertStatus,
  AlertType,
  AlertFilter,
} from '../../types/alerts';
import alertService from '../../services/api/alertService';
import { useDebounce } from '../../hooks/useDebounce';
import { formatDistanceToNow } from '../../utils/date';

/**
 * Props interface for the ActiveAlertsTable component
 */
interface ActiveAlertsTableProps {
  /** Callback function when an alert is selected */
  onAlertSelect: (alert: Alert) => void;
  /** Initial filter settings for the alerts table */
  filters?: AlertFilter;
  /** Interval in milliseconds for auto-refreshing alerts */
  refreshInterval?: number;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Component that displays a table of active alerts with filtering and selection capabilities
 */
const ActiveAlertsTable: React.FC<ActiveAlertsTableProps> = ({
  onAlertSelect,
  filters,
  refreshInterval = 60000, // Default refresh interval of 60 seconds
  className,
}) => {
  // State for alerts data and pagination
  const [alerts, setAlerts] = useState<{
    items: Alert[];
    pagination: { page: number; pageSize: number; totalItems: number; totalPages: number };
  }>({
    items: [],
    pagination: { page: 0, pageSize: 10, totalItems: 0, totalPages: 0 },
  });

  // State for loading and error handling
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // State for search term and active filters
  const [searchTerm, setSearchTerm] = useState('');
  const [activeFilters, setActiveFilters] = useState<AlertFilter>(filters || {});

  // State for filter menu anchor element
  const [filterMenuAnchor, setFilterMenuAnchor] = useState<HTMLElement | null>(null);

  // Debounce the search term to prevent excessive API calls
  const debouncedSearchTerm = useDebounce(searchTerm, 500);

  // Memoized function for fetching alerts from the API
  const fetchAlerts = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Construct filter object with search term and active filters
      const filter = {
        ...activeFilters,
        search: debouncedSearchTerm,
      };

      // Call alertService.getAlerts with filters and pagination
      const response = await alertService.getAlerts(
        filter,
        alerts.pagination.page,
        alerts.pagination.pageSize
      );

      // Update alerts state with response data
      setAlerts({
        items: response.items,
        pagination: response.pagination,
      });
    } catch (err: any) {
      setError(err.message || 'Failed to fetch alerts');
    } finally {
      setLoading(false);
    }
  }, [debouncedSearchTerm, activeFilters, alerts.pagination.page, alerts.pagination.pageSize]);

  // useEffect to fetch alerts on component mount and when filters change
  useEffect(() => {
    fetchAlerts();
  }, [fetchAlerts]);

  // useEffect for auto-refresh based on refreshInterval prop
  useEffect(() => {
    if (refreshInterval) {
      const interval = setInterval(fetchAlerts, refreshInterval);
      return () => clearInterval(interval);
    }
  }, [refreshInterval, fetchAlerts]);

  // Function to handle changes to the search input
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(event.target.value);
  };

  // Function to update the active filters
  const handleFilterChange = (newFilters: AlertFilter) => {
    setActiveFilters(newFilters);
    setAlerts((prevAlerts) => ({
      ...prevAlerts,
      pagination: { ...prevAlerts.pagination, page: 1 },
    }));
  };

  // Function to manually refresh the alerts data
  const handleRefresh = () => {
    fetchAlerts();
  };

  // Function to handle pagination changes
  const handlePageChange = (page: number, pageSize: number) => {
    setAlerts((prevAlerts) => ({
      ...prevAlerts,
      pagination: { ...prevAlerts.pagination, page, pageSize },
    }));
  };

  // Function to open the filter menu
  const handleFilterMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterMenuAnchor(event.currentTarget);
  };

  // Function to close the filter menu
  const handleFilterMenuClose = () => {
    setFilterMenuAnchor(null);
  };

  // Define table columns with appropriate formatters for each field
  const columns = useMemo(() => [
    {
      id: 'severity',
      label: 'Severity',
      width: '120px',
      sortable: true,
      renderCell: (value: AlertSeverity) => (
        <Badge label={value} color={value.toLowerCase() as 'success' | 'warning' | 'error' | 'info' | 'default'} />
      ),
    },
    {
      id: 'status',
      label: 'Status',
      width: '140px',
      sortable: true,
      renderCell: (value: AlertStatus) => (
        <Badge label={value} color={value.toLowerCase() as 'success' | 'warning' | 'error' | 'info' | 'default'} />
      ),
    },
    {
      id: 'message',
      label: 'Message',
      sortable: true,
      renderCell: (value: string) => (
        <Tooltip title={value} placement="bottom-start">
          <span>{value}</span>
        </Tooltip>
      ),
    },
    {
      id: 'source',
      label: 'Source',
      width: '150px',
      sortable: true,
    },
    {
      id: 'component',
      label: 'Component',
      width: '150px',
      sortable: true,
    },
    {
      id: 'createdAt',
      label: 'Time',
      width: '120px',
      sortable: true,
      format: (value: string) => formatDistanceToNow(value),
    },
    {
      id: 'selfHealingStatus',
      label: 'Self-Healing',
      width: '150px',
      sortable: true,
      renderCell: (value: string | undefined) => (
        value ? <Badge label={value} color="info" /> : 'N/A'
      ),
    },
  ], []);

  // Render the component
  return (
    <Box className={className}>
      {/* Search Input */}
      <TextField
        placeholder="Search alerts..."
        variant="outlined"
        size="small"
        fullWidth
        value={searchTerm}
        onChange={handleSearchChange}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <Search />
            </InputAdornment>
          ),
        }}
        sx={{ mb: 2 }}
      />

      {/* Filter Button */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <IconButton aria-label="Filter alerts" onClick={handleFilterMenuOpen}>
          <FilterList />
        </IconButton>
        <IconButton aria-label="Refresh alerts" onClick={handleRefresh}>
          <Refresh />
        </IconButton>
      </Box>

      {/* Filter Menu */}
      <Menu
        anchorEl={filterMenuAnchor}
        open={Boolean(filterMenuAnchor)}
        onClose={handleFilterMenuClose}
      >
        <MenuItem>
          <FormControl sx={{ m: 1, width: 300 }}>
            <InputLabel id="severity-multiple-checkbox-label">Severity</InputLabel>
            <Select
              labelId="severity-multiple-checkbox-label"
              id="severity-multiple-checkbox"
              multiple
              value={activeFilters.severity || []}
              onChange={(e) => handleFilterChange({ ...activeFilters, severity: e.target.value as AlertSeverity[] })}
              input={<OutlinedInput label="Severity" />}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {Object.values(AlertSeverity).map((severity) => (
                <MenuItem key={severity} value={severity}>
                  <Checkbox checked={(activeFilters.severity || []).indexOf(severity) > -1} />
                  <ListItemText primary={severity} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </MenuItem>
        <MenuItem>
          <FormControl sx={{ m: 1, width: 300 }}>
            <InputLabel id="status-multiple-checkbox-label">Status</InputLabel>
            <Select
              labelId="status-multiple-checkbox-label"
              id="status-multiple-checkbox"
              multiple
              value={activeFilters.status || []}
              onChange={(e) => handleFilterChange({ ...activeFilters, status: e.target.value as AlertStatus[] })}
              input={<OutlinedInput label="Status" />}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {Object.values(AlertStatus).map((status) => (
                <MenuItem key={status} value={status}>
                  <Checkbox checked={(activeFilters.status || []).indexOf(status) > -1} />
                  <ListItemText primary={status} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </MenuItem>
        <MenuItem>
          <FormControl sx={{ m: 1, width: 300 }}>
            <InputLabel id="type-multiple-checkbox-label">Type</InputLabel>
            <Select
              labelId="type-multiple-checkbox-label"
              id="type-multiple-checkbox"
              multiple
              value={activeFilters.type || []}
              onChange={(e) => handleFilterChange({ ...activeFilters, type: e.target.value as AlertType[] })}
              input={<OutlinedInput label="Type" />}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {Object.values(AlertType).map((type) => (
                <MenuItem key={type} value={type}>
                  <Checkbox checked={(activeFilters.type || []).indexOf(type) > -1} />
                  <ListItemText primary={type} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </MenuItem>
      </Menu>

      {/* Table Component */}
      <Table
        columns={columns}
        data={alerts.items}
        loading={loading}
        error={error}
        pagination
        totalItems={alerts.pagination.totalItems}
        onPageChange={handlePageChange}
        onRowClick={onAlertSelect}
      />
    </Box>
  );
};

export default ActiveAlertsTable;