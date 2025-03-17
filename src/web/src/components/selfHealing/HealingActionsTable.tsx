import React, { useState, useEffect, useMemo, useCallback } from 'react'; // react ^18.2.0
import { Typography, Box, Tooltip, IconButton, Chip } from '@mui/material'; // @mui/material ^5.11.0
import { AutoFixHigh, Info, Edit, Delete, CheckCircle, Error } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../common/Table';
import Card from '../common/Card';
import Badge from '../common/Badge';
import { useApi } from '../../hooks/useApi';
import healingService from '../../services/api/healingService';
import { HealingAction, ActionType } from '../../types/selfHealing';
import { formatDate } from '../../utils/date';

/**
 * Interface defining the props for the HealingActionsTable component.
 * Includes optional properties for filtering actions, custom titles, and callbacks.
 */
interface HealingActionsTableProps {
  patternId?: string;
  actionType?: ActionType;
  showActiveOnly?: boolean;
  title?: string;
  maxHeight?: string | number;
  onActionSelect?: (action: HealingAction) => void;
  onActionEdit?: (action: HealingAction) => void;
  onActionDelete?: (action: HealingAction) => void;
  className?: string;
}

/**
 * Interface for extended healing action with execution statistics
 */
interface ActionWithExecutions {
  action: HealingAction;
  executionCount: number;
  successCount: number;
  lastExecuted?: string;
}

/**
 * Formats a success rate value as a percentage with appropriate styling
 * @param rate The success rate number
 * @returns A React node containing the formatted success rate
 */
const formatSuccessRate = (rate: number): React.ReactNode => {
  if (rate === undefined || rate === null) {
    return '-';
  }

  const percentage = (rate * 100).toFixed(1);
  let color = 'success';

  if (rate < 0.5) {
    color = 'error';
  } else if (rate < 0.8) {
    color = 'warning';
  }

  return (
    <Typography variant="body2" color={color}>
      {percentage}%
    </Typography>
  );
};

/**
 * Returns a formatted label for an action type with appropriate styling
 * @param actionType The action type
 * @returns A React node containing the formatted action type badge
 */
const getActionTypeLabel = (actionType: ActionType): React.ReactNode => {
  let label = actionType;
  let color = 'info';

  switch (actionType) {
    case ActionType.DATA_CORRECTION:
      label = 'Data Correction';
      color = 'success';
      break;
    case ActionType.PARAMETER_ADJUSTMENT:
      label = 'Parameter Adjustment';
      color = 'warning';
      break;
    case ActionType.RESOURCE_OPTIMIZATION:
      label = 'Resource Optimization';
      color = 'info';
      break;
    case ActionType.RETRY:
      label = 'Retry';
      color = 'default';
      break;
    case ActionType.SCHEMA_CORRECTION:
      label = 'Schema Correction';
      color = 'success';
      break;
    default:
      break;
  }

  return (
    <Badge label={label} variant="outlined" color={color} />
  );
};

/**
 * Returns a status chip indicating if an action is active or inactive
 * @param isActive Whether the action is active
 * @returns A React node containing the status chip
 */
const getActionStatusChip = (isActive: boolean): React.ReactNode => {
  return (
    <Chip
      label={isActive ? 'Active' : 'Inactive'}
      color={isActive ? 'success' : 'default'}
      icon={isActive ? <CheckCircle /> : <Error />}
      size="small"
    />
  );
};

/**
 * A table component that displays healing actions with their details and success rates
 * @param props HealingActionsTableProps
 * @returns React component
 */
const HealingActionsTable: React.FC<HealingActionsTableProps> = ({
  patternId,
  actionType,
  showActiveOnly = false,
  title = 'Healing Actions',
  maxHeight,
  onActionSelect,
  onActionEdit,
  onActionDelete,
  className,
}) => {
  // Initialize state for actions data and loading
  const [actionsWithExecutions, setActionsWithExecutions] = useState<ActionWithExecutions[]>([]);
  const [loading, setLoading] = useState(false);

  // Use the useApi hook for API calls
  const { get, loading: apiLoading, error } = useApi();

  // Define table columns
  const columns = useMemo(() => [
    { id: 'name', label: 'Name', sortable: true },
    { id: 'actionType', label: 'Type', renderCell: (value: ActionType) => getActionTypeLabel(value) },
    { id: 'successRate', label: 'Success Rate', sortable: true, align: 'right', renderCell: (value: number) => formatSuccessRate(value) },
    { id: 'lastExecuted', label: 'Last Executed', sortable: true, renderCell: (value: string) => value ? formatDate(value, 'MM/dd/yyyy HH:mm') : '-' },
    { id: 'status', label: 'Status', renderCell: (row: ActionWithExecutions) => getActionStatusChip(row.action.isActive) },
    {
      id: 'actions',
      label: 'Actions',
      align: 'center',
      width: 120,
      renderCell: (value: any, row: ActionWithExecutions) => (
        <Box>
          <Tooltip title="View Details">
            <IconButton onClick={() => onActionSelect && onActionSelect(row.action)} aria-label="view details">
              <Info />
            </IconButton>
          </Tooltip>
          <Tooltip title="Edit Action">
            <IconButton onClick={() => onActionEdit && onActionEdit(row.action)} aria-label="edit action">
              <Edit />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete Action">
            <IconButton onClick={() => onActionDelete && onActionDelete(row.action)} aria-label="delete action">
              <Delete />
            </IconButton>
          </Tooltip>
        </Box>
      ),
    },
  ], [onActionSelect, onActionEdit, onActionDelete]);

  // Fetch actions data and execution statistics
  useEffect(() => {
    const fetchActions = async () => {
      setLoading(true);
      try {
        // Construct query parameters for filtering
        const params: any = {};
        if (patternId) params.patternId = patternId;
        if (actionType) params.actionType = actionType;
        if (showActiveOnly) params.isActive = true;

        // Fetch healing actions from the API
        const actionsResponse = await get<any>(healingService.getHealingActions, params);
        const actions = actionsResponse.items as HealingAction[];

        // Fetch execution statistics for each action
        const actionsWithStats = await Promise.all(
          actions.map(async (action) => {
            const executionParams: any = { actionId: action.actionId, pageSize: 1, page: 1 };
            const executionsResponse = await get<any>(healingService.getHealingExecutions, executionParams);
            const executions = executionsResponse.items;

            return {
              action: action,
              executionCount: executionsResponse.pagination.totalItems,
              successCount: executions.filter((e: any) => e.successful).length,
              lastExecuted: executions.length > 0 ? executions[0].executionTime : null,
            };
          })
        );

        // Update state with combined data
        setActionsWithExecutions(actionsWithStats);
      } catch (error) {
        console.error('Failed to fetch healing actions:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchActions();
  }, [get, patternId, actionType, showActiveOnly]);

  return (
    <Card title={title} loading={loading || apiLoading} error={error} maxHeight={maxHeight} className={className}>
      <Table
        columns={columns}
        data={actionsWithExecutions}
        loading={loading || apiLoading}
        error={error}
        emptyMessage="No healing actions available"
        getRowId={(row) => row.action.actionId}
      />
    </Card>
  );
};

export default HealingActionsTable;