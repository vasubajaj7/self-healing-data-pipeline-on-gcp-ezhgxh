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
  DatePicker
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Search,
  FilterList,
  Refresh,
  CalendarToday
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns'; // @mui/x-date-pickers/AdapterDateFns ^5.0.0
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider'; // @mui/x-date-pickers/LocalizationProvider ^5.0.0
import Table from '../../components/common/Table';
import Badge from '../../components/common/Badge';
import {
  Alert,
  AlertSeverity,
  AlertStatus,
  AlertType,
  AlertFilter
} from '../../types/alerts';
import alertService from '../../services/api/alertService';
import { useDebounce } from '../../hooks/useDebounce';
import { formatDistanceToNow, formatDate } from '../../utils/date';

/**
 * Props interface for the HistoricalAlertsTable component
 */
interface HistoricalAlertsTableProps {
  /** Callback function when an alert is selected */
  onAlertSelect: (alert: Alert) => void;
  /** Initial filter settings for the historical alerts table */
  initialFilters?: AlertFilter;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Component that displays a table of historical alerts with filtering and selection capabilities
 */
const HistoricalAlertsTable: React.FC<HistoricalAlertsTableProps> = ({
  onAlertSelect,
  initialFilters = {},
  className
}) => {
  // State for storing historical alert data and pagination information
  const [alerts, setAlerts] = useState<{
    items: Alert[];
    pagination: {
      page: number;
      pageSize: number;
      totalItems: number;
      totalPages: number;
    };
  }>({ items: [], pagination: { page: 0, pageSize: 10, totalItems: 0, totalPages: 0 } });

  // Loading state for API requests
  const [loading, setLoading] = useState<boolean>(false);

  // Error state for API request failures
  const [error, setError] = useState<string | null>(null);

  // Current search term for filtering alerts
  const [searchTerm, setSearchTerm] = useState<string>('');

  // Active filter settings for the alerts table
  const [activeFilters, setActiveFilters] = useState<AlertFilter>(initialFilters);

  // Anchor element for the filter menu dropdown
  const [filterMenuAnchor, setFilterMenuAnchor] = React.useState<null | HTMLElement>(null);

  // Anchor element for the date range menu dropdown
  const [dateRangeMenuAnchor, setDateRangeMenuAnchor] = React.useState<null | HTMLElement>(null);

  // Debounces search term changes to prevent excessive API calls
  const debouncedSearchTerm = useDebounce(searchTerm, 500);

  // Memoized function for fetching historical alerts from the API
  const fetchAlertHistory = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Construct filter object with search term and active filters
      const filter = {
        ...activeFilters,
        search: debouncedSearchTerm,
      };

      // Call alertService.getAlertHistory with filters and pagination
      const response = await alertService.getAlertHistory(
        filter,
        alerts.pagination.page,
        alerts.pagination.pageSize
      );

      // Update alerts state with response data
      setAlerts(response);
    } catch (error: any) {
      // Handle errors by setting error state
      setError(error.message || 'Failed to fetch alerts');
    } finally {
      // Set loading state to false
      setLoading(false);
    }
  }, [debouncedSearchTerm, activeFilters, alerts.pagination.page, alerts.pagination.pageSize]);

  // Set up useEffect to fetch alerts on component mount and when filters change
  useEffect(() => {
    fetchAlertHistory();
  }, [debouncedSearchTerm, activeFilters, alerts.pagination.page, alerts.pagination.pageSize, fetchAlertHistory]);

  // Handles changes to the search input
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(event.target.value);
  };

  // Updates the active filters
  const handleFilterChange = (newFilters: AlertFilter) => {
    setActiveFilters(newFilters);
    setAlerts(prev => ({
      ...prev,
      pagination: {
        ...prev.pagination,
        page: 1
      }
    }));
  };

  // Updates the date range filters
  const handleDateRangeChange = (type: string, date: Date | null) => {
    setActiveFilters(prev => ({
      ...prev,
      [type]: date ? formatDate(date, 'yyyy-MM-dd') : undefined
    }));
    setAlerts(prev => ({
      ...prev,
      pagination: {
        ...prev.pagination,
        page: 1
      }
    }));
  };

  // Manually refreshes the historical alerts data
  const handleRefresh = () => {
    fetchAlertHistory();
  };

  // Handles pagination changes
  const handlePageChange = (page: number, pageSize: number) => {
    setAlerts(prev => ({
      ...prev,
      pagination: {
        ...prev.pagination,
        page,
        pageSize
      }
    }));
  };

  // Opens the filter menu dropdown
  const handleFilterMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setFilterMenuAnchor(event.currentTarget);
  };

  // Closes the filter menu dropdown
  const handleFilterMenuClose = () => {
    setFilterMenuAnchor(null);
  };

  // Opens the date range menu dropdown
  const handleDateRangeMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setDateRangeMenuAnchor(event.currentTarget);
  };

  // Closes the date range menu dropdown
  const handleDateRangeMenuClose = () => {
    setDateRangeMenuAnchor(null);
  };

  // Memoized table column definitions
  const columns = useMemo(() => [
    {
      id: 'severity',
      label: 'Severity',
      width: '120px',
      sortable: true,
      renderCell: (value: AlertSeverity) => (
        <Badge label={value} color={value.toLowerCase() as "success" | "warning" | "error" | "info" | "default"} />
      ),
    },
    {
      id: 'status',
      label: 'Status',
      width: '140px',
      sortable: true,
      renderCell: (value: AlertStatus) => (
        <Badge label={value} color="info" />
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
      label: 'Created',
      width: '150px',
      sortable: true,
      format: (value: string) => formatDate(value, 'MM/dd/yyyy HH:mm'),
    },
    {
      id: 'resolvedAt',
      label: 'Resolved',
      width: '150px',
      sortable: true,
      format: (value: string | null) => value ? formatDate(value, 'MM/dd/yyyy HH:mm') : 'N/A',
    },
    {
      id: 'resolvedBy',
      label: 'Resolved By',
      width: '150px',
      sortable: true,
      format: (value: string | null) => value || 'N/A',
    },
    {
      id: 'selfHealingStatus',
      label: 'Self-Healing',
      width: '150px',
      sortable: true,
      renderCell: (value: string | null) => value || 'N/A',
    },
  ], []);

  return (
    <Box className={className}>
      {/* Filter Controls */}
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', mb: 2 }}>
        <TextField
          placeholder="Search historical alerts..."
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
        />
        <IconButton aria-label="Filter alerts" onClick={handleFilterMenuOpen}>
          <FilterList />
        </IconButton>
        <IconButton aria-label="Set date range" onClick={handleDateRangeMenuOpen}>
          <CalendarToday />
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

      {/* Date Range Menu */}
      <Menu
        anchorEl={dateRangeMenuAnchor}
        open={Boolean(dateRangeMenuAnchor)}
        onClose={handleDateRangeMenuClose}
      >
        <MenuItem>
          <LocalizationProvider dateAdapter={AdapterDateFns}>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, m: 1 }}>
              <DatePicker
                label="Start Date"
                value={activeFilters.startDate || null}
                onChange={(value) => handleDateRangeChange('startDate', value)}
              />
              <DatePicker
                label="End Date"
                value={activeFilters.endDate || null}
                onChange={(value) => handleDateRangeChange('endDate', value)}
              />
            </Box>
          </LocalizationProvider>
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

export default HistoricalAlertsTable;