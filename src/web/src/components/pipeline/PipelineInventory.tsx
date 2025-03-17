import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  TextField,
  InputAdornment,
  MenuItem,
  Select,
  FormControl,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Chip,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Search,
  FilterList,
  PlayArrow,
  Edit,
  Delete,
  Visibility,
  History,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../common/Table';
import Button from '../common/Button';
import Spinner from '../common/Spinner';
import {
  PipelineDefinition,
  PipelineStatus,
} from '../../types/api';
import { formatDateTimeShort } from '../../utils/date';
import pipelineService from '../../services/api/pipelineService';
import { useApi } from '../../hooks/useApi';
import { useNotification } from '../../hooks/useNotification';

/**
 * Props for the PipelineInventory component
 */
interface PipelineInventoryProps {
  /**
   * Callback function when a pipeline is selected
   * @param pipeline The selected pipeline definition
   */
  onSelectPipeline?: (pipeline: PipelineDefinition) => void;
  /**
   * Callback function when the Create button is clicked
   */
  onCreatePipeline?: () => void;
  /**
   * Callback function when the Edit button is clicked
   * @param pipeline The pipeline definition to edit
   */
  onEditPipeline?: (pipeline: PipelineDefinition) => void;
  /**
   * Callback function when the View History button is clicked
   * @param pipeline The pipeline definition to view history
   */
  onViewHistory?: (pipeline: PipelineDefinition) => void;
  /**
   * Additional CSS class for styling
   */
  className?: string;
}

/**
 * Returns the appropriate color for a pipeline status
 * @param status The pipeline status
 * @returns CSS color value for the status
 */
function getStatusColor(status: PipelineStatus): string {
  switch (status) {
    case PipelineStatus.HEALTHY:
      return 'green';
    case PipelineStatus.WARNING:
      return 'orange';
    case PipelineStatus.ERROR:
      return 'red';
    case PipelineStatus.INACTIVE:
      return 'gray';
    default:
      return 'black';
  }
}

/**
 * Component that displays a table of pipeline definitions with filtering, sorting, and action capabilities
 */
const PipelineInventory: React.FC<PipelineInventoryProps> = ({
  onSelectPipeline,
  onCreatePipeline,
  onEditPipeline,
  onViewHistory,
  className,
}) => {
  // State variables for managing pipeline data, search term, filters, pagination, and sorting
  const [pipelines, setPipelines] = useState<PipelineDefinition[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [totalItems, setTotalItems] = useState(0);
  const [sortBy, setSortBy] = useState('updatedAt');
  const [sortDirection, setSortDirection] = useState('desc');

  // State variables for managing the delete confirmation dialog
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [pipelineToDelete, setPipelineToDelete] = useState<PipelineDefinition | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);

  // Hooks for API calls and notifications
  const notification = useNotification();
  const pipelinesApi = useApi<PipelineDefinition[]>();
  const deleteApi = useApi<{ success: boolean }>();

  /**
   * Fetches pipeline definitions with current filters and pagination
   */
  const fetchPipelines = useCallback(async () => {
    // Prepare query parameters with page, pageSize, sortBy, sortDirection
    let params: any = {
      page,
      pageSize,
      sortBy,
      descending: sortDirection === 'desc',
    };

    // Add searchTerm to parameters if it's not empty
    if (searchTerm) {
      params = { ...params, searchTerm };
    }

    // Add statusFilter to parameters if it's not 'ALL'
    if (statusFilter !== 'ALL') {
      params = { ...params, status: statusFilter };
    }

    try {
      // Call pipelinesApi.execute with the parameters
      const response = await pipelinesApi.get(
        '/pipelines',
        { params }
      );

      // Update pipelines state with the response items
      setPipelines(response as any);

      // Update totalItems state with the response pagination.totalItems
      setTotalItems(response.length);
    } catch (error: any) {
      // Show error notification if the request fails
      notification.showError(
        error.message || 'Failed to fetch pipelines.'
      );
    }
  }, [page, pageSize, sortBy, sortDirection, searchTerm, statusFilter, notification, pipelinesApi]);

  /**
   * Handles changes to the search input
   * @param event React.ChangeEvent<HTMLInputElement>
   */
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    // Update searchTerm state with the input value
    setSearchTerm(event.target.value);

    // Reset page to 1
    setPage(1);

    // Fetch pipelines with the new search term
    fetchPipelines();
  };

  /**
   * Handles changes to the status filter dropdown
   * @param event React.ChangeEvent<{ value: unknown }>
   */
  const handleStatusFilterChange = (event: React.ChangeEvent<{ value: unknown }>) => {
    // Update statusFilter state with the selected value
    setStatusFilter(event.target.value as string);

    // Reset page to 1
    setPage(1);

    // Fetch pipelines with the new status filter
    fetchPipelines();
  };

  /**
   * Handles page changes in pagination
   * @param newPage number
   * @param newPageSize number
   */
  const handlePageChange = (newPage: number, newPageSize: number) => {
    // Update page state with the new page
    setPage(newPage);

    // Update pageSize state with the new page size
    setPageSize(newPageSize);

    // Fetch pipelines with the new pagination parameters
    fetchPipelines();
  };

  /**
   * Handles changes to the sort field and direction
   * @param column string
   * @param direction string
   */
  const handleSortChange = (column: string, direction: string) => {
    // Update sortBy state with the new column
    setSortBy(column);

    // Update sortDirection state with the new direction
    setSortDirection(direction);

    // Fetch pipelines with the new sort parameters
    fetchPipelines();
  };

  /**
   * Handles clicking on a pipeline row
   * @param pipeline PipelineDefinition
   */
  const handleRowClick = (pipeline: PipelineDefinition) => {
    // Call onSelectPipeline prop with the selected pipeline if provided
    onSelectPipeline?.(pipeline);
  };

  /**
   * Handles clicking the Create button
   */
  const handleCreateClick = () => {
    // Call onCreatePipeline prop if provided
    onCreatePipeline?.();
  };

  /**
   * Handles clicking the Edit button for a pipeline
   * @param pipeline PipelineDefinition
   * @param event React.MouseEvent
   */
  const handleEditClick = (pipeline: PipelineDefinition, event: React.MouseEvent) => {
    // Stop event propagation to prevent row click
    event.stopPropagation();

    // Call onEditPipeline prop with the pipeline if provided
    onEditPipeline?.(pipeline);
  };

  /**
   * Handles clicking the View button for a pipeline
   * @param pipeline PipelineDefinition
   * @param event React.MouseEvent
   */
  const handleViewClick = (pipeline: PipelineDefinition, event: React.MouseEvent) => {
    // Stop event propagation to prevent row click
    event.stopPropagation();

    // Call onSelectPipeline prop with the pipeline if provided
    onSelectPipeline?.(pipeline);
  };

  /**
   * Handles clicking the History button for a pipeline
   * @param pipeline PipelineDefinition
   * @param event React.MouseEvent
   */
  const handleHistoryClick = (pipeline: PipelineDefinition, event: React.MouseEvent) => {
    // Stop event propagation to prevent row click
    event.stopPropagation();

    // Call onViewHistory prop with the pipeline if provided
    onViewHistory?.(pipeline);
  };

  /**
   * Handles clicking the Delete button for a pipeline
   * @param pipeline PipelineDefinition
   * @param event React.MouseEvent
   */
  const handleDeleteClick = (pipeline: PipelineDefinition, event: React.MouseEvent) => {
    // Stop event propagation to prevent row click
    event.stopPropagation();

    // Set pipelineToDelete state to the pipeline
    setPipelineToDelete(pipeline);

    // Open the delete confirmation dialog
    setDeleteDialogOpen(true);
  };

  /**
   * Handles confirming pipeline deletion
   */
  const handleDeleteConfirm = async () => {
    // Set isDeleting state to true
    setIsDeleting(true);

    try {
      // Call deleteApi.execute with the pipelineToDelete.pipelineId
      if (pipelineToDelete) {
        await deleteApi.get(`/pipelines/${pipelineToDelete.pipelineId}`);

        // On success, show success notification
        notification.showSuccess('Pipeline deleted successfully.');

        // On success, close the delete dialog
        setDeleteDialogOpen(false);

        // On success, refresh the pipeline list
        fetchPipelines();
      }
    } catch (error: any) {
      // On error, show error notification
      notification.showError(
        error.message || 'Failed to delete pipeline.'
      );
    } finally {
      // Finally, set isDeleting state to false
      setIsDeleting(false);
    }
  };

  /**
   * Handles canceling pipeline deletion
   */
  const handleDeleteCancel = () => {
    // Close the delete dialog
    setDeleteDialogOpen(false);

    // Set pipelineToDelete state to null
    setPipelineToDelete(null);
  };

  // Fetch pipelines on component mount
  useEffect(() => {
    fetchPipelines();
  }, [fetchPipelines]);

  // Define table columns with appropriate headers, data fields, and formatters
  const columns = useMemo(
    () => [
      {
        id: 'pipelineName',
        label: 'Name',
        sortable: true,
      },
      {
        id: 'sourceName',
        label: 'Source',
        sortable: true,
      },
      {
        id: 'targetDataset',
        label: 'Dataset',
      },
      {
        id: 'targetTable',
        label: 'Table',
      },
      {
        id: 'lastExecutionStatus',
        label: 'Status',
        sortable: true,
        format: (status: PipelineStatus) => (
          <Chip
            label={status}
            size="small"
            sx={{ backgroundColor: getStatusColor(status) }}
          />
        ),
      },
      {
        id: 'updatedAt',
        label: 'Last Update',
        sortable: true,
        format: (date: string) => formatDateTimeShort(date),
      },
      {
        id: 'actions',
        label: 'Actions',
        align: 'right',
        renderCell: (_: any, pipeline: PipelineDefinition) => (
          <>
            <Button
              variant="text"
              size="small"
              onClick={(event) => handleViewClick(pipeline, event)}
              startIcon={<Visibility />}
            >
              View
            </Button>
            <Button
              variant="text"
              size="small"
              onClick={(event) => handleEditClick(pipeline, event)}
              startIcon={<Edit />}
            >
              Edit
            </Button>
            <Button
              variant="text"
              size="small"
              onClick={(event) => handleHistoryClick(pipeline, event)}
              startIcon={<History />}
            >
              History
            </Button>
            <Button
              variant="text"
              size="small"
              color="error"
              onClick={(event) => handleDeleteClick(pipeline, event)}
              startIcon={<Delete />}
            >
              Delete
            </Button>
          </>
        ),
      },
    ],
    [handleViewClick, handleEditClick, handleHistoryClick, handleDeleteClick]
  );

  return (
    <Box className={className}>
      {/* Header section with title and create button */}
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          mb: 2,
        }}
      >
        <Typography variant="h5" component="h2">
          Pipeline Inventory
        </Typography>
        <Button
          variant="contained"
          startIcon={<Add />}
          onClick={handleCreateClick}
        >
          Create Pipeline
        </Button>
      </Box>

      {/* Filter section with search input and status filter dropdown */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          mb: 2,
        }}
      >
        <TextField
          label="Search"
          variant="outlined"
          size="small"
          value={searchTerm}
          onChange={handleSearchChange}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Search />
              </InputAdornment>
            ),
          }}
          sx={{ width: 200, mr: 2 }}
        />
        <FormControl variant="outlined" size="small" sx={{ width: 150, mr: 2 }}>
          <Select
            value={statusFilter}
            onChange={handleStatusFilterChange}
            displayEmpty
          >
            <MenuItem value="ALL">All Statuses</MenuItem>
            <MenuItem value={PipelineStatus.HEALTHY}>Healthy</MenuItem>
            <MenuItem value={PipelineStatus.WARNING}>Warning</MenuItem>
            <MenuItem value={PipelineStatus.ERROR}>Error</MenuItem>
            <MenuItem value={PipelineStatus.INACTIVE}>Inactive</MenuItem>
          </Select>
        </FormControl>
      </Box>

      {/* Table component with the defined columns and pipeline data */}
      <Table
        columns={columns}
        data={pipelines}
        loading={pipelinesApi.loading}
        error={pipelinesApi.error?.message}
        pagination
        totalItems={totalItems}
        page={page}
        pageSize={pageSize}
        onPageChange={handlePageChange}
        defaultSortBy={sortBy}
        defaultSortDirection={sortDirection}
        onSortChange={handleSortChange}
        onRowClick={handleRowClick}
      />

      {/* Delete confirmation dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={handleDeleteCancel}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title">Confirm Delete</DialogTitle>
        <DialogContent>
          <DialogContentText id="alert-dialog-description">
            Are you sure you want to delete this pipeline? This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleDeleteCancel}>Cancel</Button>
          <Button
            onClick={handleDeleteConfirm}
            color="error"
            disabled={isDeleting}
            autoFocus
          >
            {isDeleting ? <Spinner size="small" /> : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PipelineInventory;