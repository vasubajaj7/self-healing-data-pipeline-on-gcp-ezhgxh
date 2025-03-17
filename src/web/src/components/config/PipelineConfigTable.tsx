import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Chip,
  IconButton,
  Tooltip,
  TextField,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Grid,
  Paper,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Edit,
  Delete,
  PlayArrow,
  Pause,
  FilterList,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { format } from 'date-fns'; // date-fns ^2.29.3
import Table from '../common/Table';
import Button from '../common/Button';
import { PipelineConfig } from '../../types/config';
import configService from '../../services/api/configService';
import useApi from '../../hooks/useApi';
import useNotification from '../../hooks/useNotification';

/**
 * Props for the PipelineConfigTable component
 */
interface PipelineConfigTableProps {
  /**
   * Callback function when create pipeline button is clicked
   */
  onCreatePipeline: () => void;
  /**
   * Callback function when edit pipeline button is clicked
   */
  onEditPipeline: (pipeline: PipelineConfig) => void;
  /**
   * Callback function when a pipeline row is clicked for viewing
   */
  onViewPipeline: (pipeline: PipelineConfig) => void;
  /**
   * Optional source ID to filter pipelines by source
   */
  sourceId?: string;
  /**
   * Value that changes to trigger a refresh of the table data
   */
  refreshTrigger?: number;
}

/**
 * State for filtering pipeline configurations
 */
interface FilterState {
  /**
   * Filter by pipeline name
   */
  name: string;
  /**
   * Filter by pipeline status
   */
  status: string;
  /**
   * Filter by active status
   */
  isActive: string;
}

/**
 * Formats a date string into a readable format
 * @param dateString
 * @returns Formatted date string
 */
const formatDate = (dateString: string): string => {
  // Use date-fns format function to format the date
  return format(new Date(dateString), 'MMM dd, yyyy HH:mm');
  // Return the formatted date string
};

/**
 * Component that renders a status chip with appropriate color based on pipeline status
 */
const StatusChip: React.FC<{ status: string }> = ({ status }) => {
  let color = 'default';

  if (status === 'HEALTHY') {
    color = 'success';
  } else if (status === 'ERROR') {
    color = 'error';
  } else if (status === 'WARNING') {
    color = 'warning';
  }

  // Render Chip component with appropriate color and label
  return <Chip label={status} color={color} size="small" />;
};

/**
 * Main component that displays a table of pipeline configurations with filtering and actions
 */
const PipelineConfigTable: React.FC<PipelineConfigTableProps> = ({
  onCreatePipeline,
  onEditPipeline,
  onViewPipeline,
  sourceId,
  refreshTrigger,
}) => {
  // Initialize state for pagination, filters, and selected pipelines
  const [page, setPage] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [totalItems, setTotalItems] = useState<number>(0);
  const [pipelines, setPipelines] = useState<PipelineConfig[]>([]);
  const [filters, setFilters] = useState<FilterState>({ name: '', status: '', isActive: '' });
  const [selectedPipelines, setSelectedPipelines] = useState<PipelineConfig[]>([]);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState<boolean>(false);
  const [pipelineToDelete, setPipelineToDelete] = useState<PipelineConfig | null>(null);

  // Use useApi hook to manage API request state
  const { get, delete: apiDelete, loading, error } = useApi();
  const { showSuccess, showError } = useNotification();

  /**
   * Fetch pipeline configurations with current pagination and filters
   */
  const fetchPipelines = useCallback(async () => {
    try {
      // Construct query parameters for API request
      const params: any = { page: page + 1, pageSize };
      if (filters.name) params.name = filters.name;
      if (filters.status) params.status = filters.status;
      if (filters.isActive) params.isActive = filters.isActive;
      if (sourceId) params.sourceId = sourceId;

      // Call API to get pipeline configurations
      const response = await get<any>('/admin/settings/pipelines', { params });

      // Update state with fetched data
      setPipelines(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (error: any) {
      showError(`Failed to fetch pipelines: ${error.message}`);
    }
  }, [page, pageSize, filters, sourceId, get, showError]);

  /**
   * Handle changes to filter inputs
   */
  const handleFilterChange = (event: React.ChangeEvent<HTMLInputElement | { name?: string; value: unknown }>) => {
    const { name, value } = event.target;
    // Update filter state and reset pagination to first page
    setFilters(prevFilters => ({ ...prevFilters, [name]: value }));
    setPage(0);
  };

  /**
   * Handle pagination page change
   */
  const handlePageChange = (newPage: number, newPageSize: number) => {
    // Update pagination state
    setPage(newPage);
    setPageSize(newPageSize);
  };

  /**
   * Handle pipeline deletion
   */
  const handleDeletePipeline = (pipeline: PipelineConfig) => {
    // Set pipeline to delete and open confirmation dialog
    setPipelineToDelete(pipeline);
    setDeleteDialogOpen(true);
  };

  /**
   * Confirm and execute pipeline deletion
   */
  const confirmDeletePipeline = async () => {
    if (!pipelineToDelete) return;

    try {
      // Call API to delete pipeline configuration
      await apiDelete(`/admin/settings/pipelines/${pipelineToDelete.pipelineId}`);

      // Show success notification and refresh pipelines
      showSuccess('Pipeline deleted successfully');
      await fetchPipelines();
    } catch (error: any) {
      showError(`Failed to delete pipeline: ${error.message}`);
    } finally {
      // Close confirmation dialog and clear pipeline to delete
      setDeleteDialogOpen(false);
      setPipelineToDelete(null);
    }
  };

  /**
   * Toggle pipeline active status
   */
  const handleToggleActive = async (pipeline: PipelineConfig) => {
    try {
      // Call API to update pipeline configuration with toggled active status
      await configService.updatePipelineConfig(pipeline.pipelineId, { isActive: !pipeline.isActive });

      // Show success notification and refresh pipelines
      showSuccess(`Pipeline ${pipeline.isActive ? 'deactivated' : 'activated'} successfully`);
      await fetchPipelines();
    } catch (error: any) {
      showError(`Failed to toggle pipeline active status: ${error.message}`);
    }
  };

  // Define table columns with appropriate formatters and renderers
  const columns = React.useMemo(() => [
    { id: 'name', label: 'Pipeline Name', sortable: true, minWidth: 150 },
    { id: 'targetDataset', label: 'Target Dataset', sortable: true, minWidth: 120 },
    { id: 'targetTable', label: 'Target Table', sortable: true, minWidth: 120 },
    {
      id: 'status',
      label: 'Status',
      sortable: true,
      minWidth: 100,
      renderCell: (value: any) => <StatusChip status={value} />,
    },
    {
      id: 'schedule',
      label: 'Schedule',
      sortable: true,
      minWidth: 120,
      format: (value: any) => value || 'On-demand',
    },
    {
      id: 'isActive',
      label: 'Active',
      sortable: true,
      minWidth: 80,
      renderCell: (value: any, row: PipelineConfig) => (
        <Chip
          label={value ? 'Active' : 'Inactive'}
          color={value ? 'success' : 'default'}
          size="small"
        />
      ),
    },
    {
      id: 'updatedAt',
      label: 'Last Updated',
      sortable: true,
      minWidth: 150,
      format: (value: any) => formatDate(value),
    },
    {
      id: 'actions',
      label: 'Actions',
      sortable: false,
      minWidth: 120,
      renderCell: (value: any, row: PipelineConfig) => (
        <>
          <Tooltip title="Edit Pipeline">
            <IconButton onClick={() => onEditPipeline(row)} aria-label="edit">
              <Edit />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete Pipeline">
            <IconButton onClick={() => handleDeletePipeline(row)} aria-label="delete">
              <Delete />
            </IconButton>
          </Tooltip>
          <Tooltip title={`${row.isActive ? 'Deactivate' : 'Activate'} Pipeline`}>
            <IconButton onClick={() => handleToggleActive(row)} aria-label="toggle active">
              {row.isActive ? <Pause /> : <PlayArrow />}
            </IconButton>
          </Tooltip>
        </>
      ),
    },
  ], [onEditPipeline, handleDeletePipeline, handleToggleActive]);

  // Fetch pipeline configurations on component mount and when filters or pagination change
  useEffect(() => {
    fetchPipelines();
  }, [fetchPipelines, refreshTrigger]);

  return (
    <Box>
      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={4}>
          <TextField
            label="Pipeline Name"
            name="name"
            value={filters.name}
            onChange={handleFilterChange}
            fullWidth
            size="small"
          />
        </Grid>
        <Grid item xs={4}>
          <FormControl fullWidth size="small">
            <InputLabel id="status-label">Status</InputLabel>
            <Select
              labelId="status-label"
              name="status"
              value={filters.status}
              label="Status"
              onChange={handleFilterChange}
            >
              <MenuItem value="">All</MenuItem>
              <MenuItem value="HEALTHY">Healthy</MenuItem>
              <MenuItem value="WARNING">Warning</MenuItem>
              <MenuItem value="ERROR">Error</MenuItem>
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={4}>
          <FormControl fullWidth size="small">
            <InputLabel id="active-label">Active Status</InputLabel>
            <Select
              labelId="active-label"
              name="isActive"
              value={filters.isActive}
              label="Active Status"
              onChange={handleFilterChange}
            >
              <MenuItem value="">All</MenuItem>
              <MenuItem value="true">Active</MenuItem>
              <MenuItem value="false">Inactive</MenuItem>
            </Select>
          </FormControl>
        </Grid>
      </Grid>

      <Table
        title="Pipeline Configurations"
        data={pipelines}
        columns={columns}
        loading={loading}
        error={error?.message}
        totalItems={totalItems}
        pagination
        onPageChange={handlePageChange}
        onRowClick={onViewPipeline}
        actions={
          <Button variant="contained" startIcon={<Add />} onClick={onCreatePipeline}>
            Create Pipeline
          </Button>
        }
      />
    </Box>
  );
};

export default PipelineConfigTable;