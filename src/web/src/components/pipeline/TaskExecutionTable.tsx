import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Chip,
  Tooltip,
  Box,
  Typography,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  ErrorOutline,
  CheckCircleOutline,
  HourglassEmpty,
  Refresh,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../common/Table';
import { useApi } from '../../hooks/useApi';
import pipelineService from '../../services/api/pipelineService';
import { TaskExecution } from '../../types/api';
import { formatDateTime, formatDuration } from '../../utils/date';
import { PipelineStatus } from '../../types/global.d';

/**
 * Props interface for the TaskExecutionTable component
 */
interface TaskExecutionTableProps {
  /**
   * ID of the pipeline execution to display tasks for
   */
  executionId: string;
  /**
   * Callback function when a task row is clicked
   */
  onTaskClick?: (task: TaskExecution) => void;
  /**
   * Interval in milliseconds to refresh task data (0 to disable auto-refresh)
   */
  refreshInterval?: number;
  /**
   * Initial number of tasks to display per page
   */
  initialPageSize?: number;
  /**
   * Additional CSS class name for styling
   */
  className?: string;
}

/**
 * Renders a status chip with appropriate color and icon based on task status
 * @param status Task status from PipelineStatus enum
 * @returns A styled chip component representing the status
 */
const getStatusChip = (status: PipelineStatus): JSX.Element => {
  let color: 'success' | 'error' | 'warning' | 'default' = 'default';
  let icon: React.ReactNode = null;

  switch (status) {
    case PipelineStatus.HEALTHY:
      color = 'success';
      icon = <CheckCircleOutline fontSize="small" />;
      break;
    case PipelineStatus.RUNNING:
      color = 'default';
      icon = <HourglassEmpty fontSize="small" />;
      break;
    case PipelineStatus.FAILED:
      color = 'error';
      icon = <ErrorOutline fontSize="small" />;
      break;
    case PipelineStatus.WARNING:
      color = 'warning';
      icon = <ErrorOutline fontSize="small" />;
      break;
    default:
      break;
  }

  return (
    <Chip
      label={status}
      color={color}
      icon={icon}
      size="small"
    />
  );
};

/**
 * Calculates and formats the duration of a task execution
 * @param startTime Timestamp of when the task started
 * @param endTime Timestamp of when the task ended, can be null or undefined if task is still running
 * @returns Formatted duration string or 'In progress' if task is still running
 */
const formatTaskDuration = (startTime: string, endTime: string | null | undefined): string => {
  if (!endTime) {
    return 'In progress';
  }

  const durationMs = formatDuration(startTime, endTime);
  return durationMs;
};

/**
 * Component that displays task executions for a pipeline in a tabular format
 */
const TaskExecutionTable: React.FC<TaskExecutionTableProps> = ({
  executionId,
  onTaskClick,
  refreshInterval = 0,
  initialPageSize = 10,
  className,
}) => {
  // Initialize state for page, pageSize, sortBy, sortDirection
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [sortBy, setSortBy] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  // Initialize state for tasks data, loading state, and error state
  const [tasks, setTasks] = useState<TaskExecution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [totalItems, setTotalItems] = useState(0);
  const [refreshTimer, setRefreshTimer] = useState<NodeJS.Timeout | null>(null);

  // Use the useApi hook for API requests
  const { get } = useApi();

  /**
   * Fetches task execution data from the API
   */
  const fetchTasks = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await get<{ items: TaskExecution[]; pagination: any }>(
        `/pipelines/executions/${executionId}/tasks`,
        {
          params: {
            page,
            pageSize,
            sortBy,
            descending: sortDirection === 'desc',
          },
        }
      );

      setTasks(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch task executions.');
    } finally {
      setLoading(false);
    }
  }, [executionId, get, page, pageSize, sortBy, sortDirection]);

  // Use useEffect to fetch tasks on mount and when dependencies change
  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  // Set up refresh interval if refreshInterval prop is provided
  useEffect(() => {
    if (refreshInterval > 0) {
      const timer = setInterval(fetchTasks, refreshInterval);
      setRefreshTimer(timer);
      return () => clearInterval(timer); // Cleanup on unmount
    }
    return () => {}; // No timer to clear
  }, [refreshInterval, fetchTasks]);

  /**
   * Handles page change events from the Table component
   * @param newPage The new page number
   */
  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  /**
   * Handles sort change events from the Table component
   * @param column The column to sort by
   * @param direction The sort direction ('asc' or 'desc')
   */
  const handleSortChange = (column: string, direction: 'asc' | 'desc') => {
    setSortBy(column);
    setSortDirection(direction);
  };

  // Define table columns configuration with appropriate formatters
  const columns = useMemo(() => [
    { id: 'taskId', label: 'Task ID', sortable: true, width: '20%' },
    { id: 'taskType', label: 'Type', sortable: true, width: '15%' },
    {
      id: 'status',
      label: 'Status',
      sortable: true,
      width: '10%',
      format: (value: PipelineStatus) => getStatusChip(value),
    },
    {
      id: 'startTime',
      label: 'Start Time',
      sortable: true,
      width: '15%',
      format: (value: string) => formatDateTime(value),
    },
    {
      id: 'endTime',
      label: 'End Time',
      sortable: true,
      width: '15%',
      format: (value: string | null | undefined) => formatDateTime(value),
    },
    {
      id: 'duration',
      label: 'Duration',
      sortable: false,
      width: '10%',
      format: (row: TaskExecution) => formatTaskDuration(row.startTime, row.endTime),
    },
    { id: 'retryCount', label: 'Retries', sortable: true, width: '5%' },
    {
      id: 'errorDetails',
      label: 'Error Details',
      sortable: false,
      width: '10%',
      format: (value: string) => (
        value ? (
          <Tooltip title={value} placement="right">
            <Box sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {value}
            </Box>
          </Tooltip>
        ) : null
      ),
    },
  ], []);

  return (
    <Table
      title="Task Executions"
      columns={columns}
      data={tasks}
      loading={loading}
      error={error}
      pagination={true}
      initialPageSize={initialPageSize}
      onPageChange={handlePageChange}
      totalItems={totalItems}
      defaultSortBy={sortBy}
      defaultSortDirection={sortDirection}
      onSortChange={handleSortChange}
      className={className}
      onRowClick={onTaskClick}
    />
  );
};

export default TaskExecutionTable;