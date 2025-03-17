import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Chip,
  TextField,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Button
} from '@mui/material'; // @mui/material ^5.11.0
import { FilterList, Refresh } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { DatePicker } from '@mui/x-date-pickers'; // @mui/x-date-pickers ^5.0.0
import Table from '../common/Table';
import Card from '../common/Card';
import { useApi } from '../../hooks/useApi';
import { usePagination } from '../../hooks/usePagination';
import healingService from '../../services/api/healingService';
import {
  HealingActivityLogEntry,
  ActivityType
} from '../../types/selfHealing';
import { formatDate, formatDateTime } from '../../utils/date';

/**
 * Returns the appropriate color for an activity type chip
 * @param activityType The activity type
 * @returns Color name for the activity type chip
 */
const getActivityTypeColor = (activityType: ActivityType): string => {
  switch (activityType) {
    case ActivityType.HEALING_COMPLETED:
      return 'success';
    case ActivityType.HEALING_FAILED:
      return 'error';
    case ActivityType.ISSUE_DETECTED:
      return 'warning';
    case ActivityType.HEALING_STARTED:
      return 'info';
    case ActivityType.MODEL_TRAINING:
      return 'primary';
    case ActivityType.CONFIG_CHANGED:
      return 'secondary';
    case ActivityType.MANUAL_INTERVENTION:
      return 'default';
    default:
      return 'default';
  }
};

/**
 * Returns a human-readable label for an activity type
 * @param activityType The activity type
 * @returns Human-readable label
 */
const getActivityTypeLabel = (activityType: ActivityType): string => {
  switch (activityType) {
    case ActivityType.ISSUE_DETECTED:
      return 'Issue Detected';
    case ActivityType.HEALING_STARTED:
      return 'Healing Started';
    case ActivityType.HEALING_COMPLETED:
      return 'Healing Completed';
    case ActivityType.HEALING_FAILED:
      return 'Healing Failed';
    case ActivityType.MODEL_TRAINING:
      return 'Model Training';
    case ActivityType.CONFIG_CHANGED:
      return 'Config Changed';
    case ActivityType.MANUAL_INTERVENTION:
      return 'Manual Intervention';
    default:
      return activityType;
  }
};

/**
 * Props for the HealingActivityLog component
 */
interface HealingActivityLogProps {
  title?: string;
  maxItems?: number;
  initialPageSize?: number;
  showFilters?: boolean;
  height?: string | number;
  onActivityClick?: (activity: HealingActivityLogEntry) => void;
  autoRefresh?: boolean;
  refreshInterval?: number;
}

/**
 * Component that displays a chronological log of self-healing activities
 */
const HealingActivityLog: React.FC<HealingActivityLogProps> = ({
  title = 'Healing Activity Log',
  maxItems = 100,
  initialPageSize = 10,
  showFilters = true,
  height = 'auto',
  onActivityClick,
  autoRefresh = false,
  refreshInterval = 30000,
}) => {
  // State for activity log entries, total items, loading, and error
  const [activities, setActivities] = useState<HealingActivityLogEntry[]>([]);
  const [totalItems, setTotalItems] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<any>(null);

  // State for filter panel visibility
  const [filterOpen, setFilterOpen] = useState<boolean>(false);

  // State for activity type filter
  const [activityTypeFilter, setActivityTypeFilter] = useState<ActivityType | ''>('');

  // State for start and end dates
  const [startDate, setStartDate] = useState<Date | null>(null);
  const [endDate, setEndDate] = useState<Date | null>(null);

  // Pagination hook
  const { pagination, setPage } = usePagination('healing-activity-log', totalItems, { initialParams: { pageSize: initialPageSize } });

  // API hook for fetching data
  const { get } = useApi();

  /**
   * Fetches activity log entries based on current filters and pagination
   */
  const fetchActivities = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Prepare request parameters
      const params: any = {
        page: pagination.page,
        pageSize: pagination.pageSize,
      };

      // Add date range filters if specified
      if (startDate && endDate) {
        params.startDate = startDate.toISOString();
        params.endDate = endDate.toISOString();
      }

      // Add activity type filter if specified
      if (activityTypeFilter) {
        params.activityType = activityTypeFilter;
      }

      // Call the API to get the activity log
      const response = await get<any>(healingService.getActivityLog, params);

      // Update state with the fetched data
      setActivities(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (error) {
      setError(error);
    } finally {
      setLoading(false);
    }
  }, [get, pagination.page, pagination.pageSize, startDate, endDate, activityTypeFilter]);

  /**
   * Manually refreshes the activity log
   */
  const handleRefresh = useCallback(() => {
    fetchActivities();
  }, [fetchActivities]);

  /**
   * Toggles the filter panel visibility
   */
  const handleFilterToggle = useCallback(() => {
    setFilterOpen((prev) => !prev);
  }, []);

  /**
   * Handles changes to filter values
   */
  const handleFilterChange = useCallback((filterName: string, value: any) => {
    switch (filterName) {
      case 'activityType':
        setActivityTypeFilter(value);
        break;
      case 'startDate':
        setStartDate(value);
        break;
      case 'endDate':
        setEndDate(value);
        break;
      default:
        break;
    }

    setPage(1);
    fetchActivities();
  }, [fetchActivities, setPage]);

  /**
   * Resets all filters to default values
   */
  const handleResetFilters = useCallback(() => {
    setActivityTypeFilter('');
    setStartDate(null);
    setEndDate(null);
    setPage(1);
    fetchActivities();
  }, [fetchActivities, setPage]);

  /**
   * Handles click on an activity log entry
   */
  const handleActivityClick = useCallback((activity: HealingActivityLogEntry) => {
    if (onActivityClick) {
      onActivityClick(activity);
    }
  }, [onActivityClick]);

  // Load data on component mount and when filters change
  useEffect(() => {
    fetchActivities();
  }, [fetchActivities]);

  // Set up auto-refresh interval if enabled
  useEffect(() => {
    if (autoRefresh) {
      const intervalId = setInterval(handleRefresh, refreshInterval);
      return () => clearInterval(intervalId);
    }
  }, [autoRefresh, handleRefresh, refreshInterval]);

  // Define table columns
  const columns = React.useMemo(() => [
    {
      id: 'timestamp',
      label: 'Timestamp',
      format: (value: any) => formatDateTime(value),
      width: '180px',
    },
    {
      id: 'activityType',
      label: 'Activity Type',
      renderCell: (value: any) => (
        <Chip
          label={getActivityTypeLabel(value)}
          color={getActivityTypeColor(value)}
          size="small"
          sx={{ fontWeight: 'medium' }}
        />
      ),
      width: '150px',
    },
    {
      id: 'description',
      label: 'Description',
      minWidth: '300px',
    },
    {
      id: 'relatedIds',
      label: 'Related IDs',
      renderCell: (row: any) => (
        <>
          {row.executionId && (
            <div>Execution ID: {row.executionId}</div>
          )}
          {row.healingId && (
            <div>Healing ID: {row.healingId}</div>
          )}
          {row.modelId && (
            <div>Model ID: {row.modelId}</div>
          )}
        </>
      ),
      width: '200px',
    },
    {
      id: 'user',
      label: 'User',
      renderCell: (row: any) => row.userId ? row.userId : 'System',
      width: '120px',
    },
  ], []);

  return (
    <Card container={{ height: height, display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 2 }}>
        <Typography variant="h6" component="div">
          {title}
        </Typography>
        <Box>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleRefresh}
            sx={{ mr: 1 }}
          >
            Refresh
          </Button>
          {showFilters && (
            <Button
              variant="outlined"
              startIcon={<FilterList />}
              onClick={handleFilterToggle}
            >
              Filters
            </Button>
          )}
        </Box>
      </Box>
      {showFilters && filterOpen && (
        <Box sx={{ padding: 2, marginBottom: 2, backgroundColor: 'background.paper', borderRadius: 1 }}>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, alignItems: 'center' }}>
            <FormControl sx={{ minWidth: 150 }}>
              <InputLabel id="activity-type-label">Activity Type</InputLabel>
              <Select
                labelId="activity-type-label"
                value={activityTypeFilter}
                label="Activity Type"
                onChange={(e) => handleFilterChange('activityType', e.target.value)}
              >
                <MenuItem value="">
                  <em>All</em>
                </MenuItem>
                <MenuItem value={ActivityType.ISSUE_DETECTED}>Issue Detected</MenuItem>
                <MenuItem value={ActivityType.HEALING_STARTED}>Healing Started</MenuItem>
                <MenuItem value={ActivityType.HEALING_COMPLETED}>Healing Completed</MenuItem>
                <MenuItem value={ActivityType.HEALING_FAILED}>Healing Failed</MenuItem>
                <MenuItem value={ActivityType.MODEL_TRAINING}>Model Training</MenuItem>
                <MenuItem value={ActivityType.CONFIG_CHANGED}>Config Changed</MenuItem>
                <MenuItem value={ActivityType.MANUAL_INTERVENTION}>Manual Intervention</MenuItem>
              </Select>
            </FormControl>
            <DatePicker
              label="Start Date"
              value={startDate}
              onChange={(date) => handleFilterChange('startDate', date)}
              renderInput={(params) => <TextField {...params} size="small" />}
            />
            <DatePicker
              label="End Date"
              value={endDate}
              onChange={(date) => handleFilterChange('endDate', date)}
              renderInput={(params) => <TextField {...params} size="small" />}
            />
            <Button variant="outlined" onClick={handleResetFilters}>
              Reset Filters
            </Button>
          </Box>
        </Box>
      )}
      <Table
        columns={columns}
        data={activities}
        loading={loading}
        error={error}
        pagination
        initialPageSize={initialPageSize}
        totalItems={totalItems}
        onPageChange={setPage}
        maxHeight={height}
        onRowClick={onActivityClick}
      />
    </Card>
  );
};

export default HealingActivityLog;