import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Chip,
  IconButton,
  Tooltip,
  TextField,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  DialogContentText,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Edit,
  Delete,
  Refresh,
  Check,
  Warning,
  Error,
  Storage,
  Database,
  Api,
  Code,
  FilterList,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0

import Table from '../common/Table';
import Card from '../common/Card';
import Button from '../common/Button';
import ConnectionStatsCard from './ConnectionStatsCard';
import Badge from '../common/Badge';
import { SourceSystem, SourceSystemType } from '../../types/config';
import configService from '../../services/api/configService';
import { formatDate } from '../../utils/date';
import useNotification from '../../hooks/useNotification';

/**
 * Props for the DataSourcesTable component
 */
interface DataSourcesTableProps {
  onSourceSelect: (source: SourceSystem) => void;
  onAddSource: () => void;
  refreshTrigger?: number;
  className?: string;
}

interface SourceTypeIconProps {
  type: SourceSystemType;
}

interface StatusBadgeProps {
  status: string;
}

interface FilterState {
  sourceType: string;
  status: string;
  searchTerm: string;
}

/**
 * Component that renders an icon based on the source type
 */
const SourceTypeIcon: React.FC<SourceTypeIconProps> = ({ type }) => {
  switch (type) {
    case 'GCS':
      return <Storage />;
    case 'CLOUD_SQL':
      return <Database />;
    case 'API':
      return <Api />;
    case 'CUSTOM':
      return <Code />;
    default:
      return <Code />;
  }
};

/**
 * Component that renders a badge with appropriate color based on status
 */
const StatusBadge: React.FC<StatusBadgeProps> = ({ status }) => {
  const theme = useTheme();

  let color = theme.palette.text.disabled;
  if (status === 'OK') {
    color = theme.palette.success.main;
  } else if (status === 'WARNING') {
    color = theme.palette.warning.main;
  } else if (status === 'ERROR') {
    color = theme.palette.error.main;
  }

  return <Badge label={status} color={color === theme.palette.success.main ? 'success' : color === theme.palette.warning.main ? 'warning' : color === theme.palette.error.main ? 'error' : 'default'} />;
};

/**
 * Component for filtering the data sources table
 */
const FilterPanel: React.FC<{
  filters: FilterState;
  onFilterChange: (filters: FilterState) => void;
}> = ({ filters, onFilterChange }) => {
  return (
    <Box sx={{ display: 'flex', gap: 2, mb: 2, alignItems: 'center' }}>
      <TextField
        label="Search"
        variant="outlined"
        size="small"
        value={filters.searchTerm}
        onChange={(e) => onFilterChange({ ...filters, searchTerm: e.target.value })}
      />
      <FormControl size="small">
        <InputLabel id="source-type-filter-label">Source Type</InputLabel>
        <Select
          labelId="source-type-filter-label"
          value={filters.sourceType}
          label="Source Type"
          onChange={(e) => onFilterChange({ ...filters, sourceType: e.target.value as string })}
        >
          <MenuItem value=""><em>All</em></MenuItem>
          <MenuItem value="GCS">Google Cloud Storage</MenuItem>
          <MenuItem value="CLOUD_SQL">Cloud SQL</MenuItem>
          <MenuItem value="API">API</MenuItem>
          <MenuItem value="CUSTOM">Custom</MenuItem>
        </Select>
      </FormControl>
      <FormControl size="small">
        <InputLabel id="status-filter-label">Status</InputLabel>
        <Select
          labelId="status-filter-label"
          value={filters.status}
          label="Status"
          onChange={(e) => onFilterChange({ ...filters, status: e.target.value as string })}
        >
          <MenuItem value=""><em>All</em></MenuItem>
          <MenuItem value="OK">OK</MenuItem>
          <MenuItem value="WARNING">Warning</MenuItem>
          <MenuItem value="ERROR">Error</MenuItem>
        </Select>
      </FormControl>
    </Box>
  );
};

/**
 * Main component for displaying and managing data sources
 */
const DataSourcesTable: React.FC<DataSourcesTableProps> = ({
  onSourceSelect,
  onAddSource,
  refreshTrigger,
  className,
}) => {
  const [dataSources, setDataSources] = useState<SourceSystem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [totalItems, setTotalItems] = useState<number>(0);
  const [filters, setFilters] = useState<FilterState>({ sourceType: '', status: '', searchTerm: '' });
  const [showFilters, setShowFilters] = useState<boolean>(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState<boolean>(false);
  const [sourceToDelete, setSourceToDelete] = useState<SourceSystem | null>(null);
  const [testingConnection, setTestingConnection] = useState<string | null>(null);
  const { showNotification } = useNotification();

  /**
   * Fetch data sources from the API
   */
  const fetchDataSources = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await configService.getDataSources({
        page: page + 1,
        pageSize,
        sourceType: filters.sourceType,
        status: filters.status,
        searchTerm: filters.searchTerm,
      });
      setDataSources(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch data sources.');
      showNotification(err.message || 'Failed to fetch data sources.', { variant: 'error' });
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, filters, showNotification]);

  useEffect(() => {
    fetchDataSources();
  }, [fetchDataSources, refreshTrigger]);

  /**
   * Handle page change in pagination
   */
  const handlePageChange = useCallback((newPage: number, newPageSize: number) => {
    setPage(newPage);
    setPageSize(newPageSize);
  }, []);

  /**
   * Handle changes to filters
   */
  const handleFilterChange = useCallback((newFilters: FilterState) => {
    setFilters(newFilters);
    setPage(0); // Reset to first page when filters change
  }, []);

  /**
   * Handle click on delete button
   */
  const handleDeleteClick = useCallback((source: SourceSystem) => {
    setSourceToDelete(source);
    setDeleteDialogOpen(true);
  }, []);

  /**
   * Handle confirmation of delete action
   */
  const handleDeleteConfirm = useCallback(async () => {
    if (sourceToDelete) {
      try {
        await configService.deleteDataSource(sourceToDelete.sourceId);
        showNotification('Data source deleted successfully.', { variant: 'success' });
        fetchDataSources(); // Refresh data after deletion
      } catch (err: any) {
        showNotification(err.message || 'Failed to delete data source.', { variant: 'error' });
      } finally {
        setDeleteDialogOpen(false);
        setSourceToDelete(null);
      }
    }
  }, [sourceToDelete, configService, fetchDataSources, showNotification]);

  /**
   * Handle test connection action
   */
  const handleTestConnection = useCallback(async (source: SourceSystem) => {
    setTestingConnection(source.sourceId);
    try {
      const response = await configService.testDataSourceConnection(source.sourceId);
      showNotification(response.message || 'Connection test successful.', { variant: 'success' });
    } catch (err: any) {
      showNotification(err.message || 'Connection test failed.', { variant: 'error' });
    } finally {
      setTestingConnection(null);
    }
  }, [configService, showNotification]);

  /**
   * Handle click on a table row
   */
  const handleRowClick = useCallback((source: SourceSystem) => {
    onSourceSelect(source);
  }, [onSourceSelect]);

  const tableColumns = useMemo(() => [
    {
      id: 'type',
      label: 'Type',
      width: '80px',
      renderCell: (value: any, row: SourceSystem) => <SourceTypeIcon type={row.sourceType as SourceSystemType} />,
    },
    {
      id: 'name',
      label: 'Name',
      sortable: true,
      minWidth: '150px',
    },
    {
      id: 'sourceType',
      label: 'Source Type',
      sortable: true,
      width: '120px',
      format: (value: any) => {
        switch (value) {
          case 'GCS':
            return 'Google Cloud Storage';
          case 'CLOUD_SQL':
            return 'Cloud SQL';
          case 'API':
            return 'API';
          case 'CUSTOM':
            return 'Custom';
          default:
            return 'Unknown';
        }
      },
    },
    {
      id: 'status',
      label: 'Status',
      sortable: true,
      width: '100px',
      renderCell: (value: any) => <StatusBadge status={value} />,
    },
    {
      id: 'description',
      label: 'Description',
      sortable: true,
      minWidth: '200px',
    },
    {
      id: 'updatedAt',
      label: 'Last Updated',
      sortable: true,
      width: '150px',
      format: (value: any) => formatDate(value, 'MMM dd, yyyy HH:mm'),
    },
    {
      id: 'actions',
      label: 'Actions',
      sortable: false,
      width: '150px',
      renderCell: (value: any, row: SourceSystem) => (
        <>
          <Tooltip title="Edit">
            <IconButton onClick={(e) => {
              e.stopPropagation();
              onSourceSelect(row);
            }} aria-label="edit">
              <Edit />
            </IconButton>
          </Tooltip>
          <Tooltip title="Test Connection">
            <IconButton
              disabled={testingConnection === row.sourceId}
              onClick={(e) => {
                e.stopPropagation();
                handleTestConnection(row);
              }}
              aria-label="test connection"
            >
              {testingConnection === row.sourceId ? <Refresh className="rotate" /> : <Check />}
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete">
            <IconButton onClick={(e) => {
              e.stopPropagation();
              handleDeleteClick(row);
            }} aria-label="delete">
              <Delete />
            </IconButton>
          </Tooltip>
        </>
      ),
    },
  ], [onSourceSelect, handleTestConnection, handleDeleteClick, testingConnection]);

  return (
    <Grid container spacing={2} className={className}>
      <Grid item xs={12} md={4}>
        <ConnectionStatsCard dataSources={dataSources} loading={loading} error={error} />
        <Button variant="contained" startIcon={<Add />} fullWidth onClick={onAddSource}>
          Add New Source
        </Button>
      </Grid>
      <Grid item xs={12} md={8}>
        <Card
          title="Data Sources"
          action={
            <Tooltip title="Toggle Filters">
              <IconButton onClick={() => setShowFilters(!showFilters)} aria-label="toggle filters">
                <FilterList />
              </IconButton>
            </Tooltip>
          }
        >
          {showFilters && (
            <FilterPanel filters={filters} onFilterChange={handleFilterChange} />
          )}
          <Table
            columns={tableColumns}
            data={dataSources}
            loading={loading}
            error={error}
            emptyMessage="No data sources available."
            pagination
            initialPageSize={pageSize}
            pageSizeOptions={[5, 10, 20]}
            onPageChange={handlePageChange}
            totalItems={totalItems}
            onRowClick={handleRowClick}
          />
        </Card>
      </Grid>
      <Dialog
        open={deleteDialogOpen}
        onClose={() => setDeleteDialogOpen(false)}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title">Delete Data Source</DialogTitle>
        <DialogContent>
          <DialogContentText id="alert-dialog-description">
            Are you sure you want to delete this data source? This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleDeleteConfirm} color="error">
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </Grid>
  );
};

export default DataSourcesTable;