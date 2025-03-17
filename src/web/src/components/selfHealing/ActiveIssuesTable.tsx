import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Chip,
  Button,
  Tooltip,
  IconButton,
  Box,
  Typography,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  AutoFixHigh,
  ErrorOutline,
  Warning,
  Info,
  CheckCircleOutline,
  HourglassEmpty,
  Healing,
  Done,
  Close,
  MoreVert,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../common/Table';
import { useApi } from '../../hooks/useApi';
import healingService from '../../services/api/healingService';
import {
  HealingIssue,
  IssueType,
  HealingStatus,
  AlertSeverity,
} from '../../types/selfHealing';
import { formatDate } from '../../utils/date';
import { ActiveIssuesTableProps, ManualHealingDialogProps } from '../../types/selfHealing';

/**
 * Returns the appropriate icon component based on issue severity
 * @param severity AlertSeverity
 * @returns React.ReactNode Icon component for the severity level
 */
const getSeverityIcon = (severity: AlertSeverity): React.ReactNode => {
  switch (severity) {
    case AlertSeverity.CRITICAL:
      return <ErrorOutline color="error" />;
    case AlertSeverity.HIGH:
      return <Warning color="warning" />;
    case AlertSeverity.MEDIUM:
      return <Info color="info" />;
    case AlertSeverity.LOW:
      return <Info />;
    default:
      return null;
  }
};

/**
 * Returns the appropriate icon component based on healing status
 * @param status HealingStatus
 * @returns React.ReactNode Icon component for the status
 */
const getStatusIcon = (status: HealingStatus): React.ReactNode => {
  switch (status) {
    case HealingStatus.PENDING:
      return <HourglassEmpty />;
    case HealingStatus.IN_PROGRESS:
      return <Healing color="primary" />;
    case HealingStatus.COMPLETED:
      return <CheckCircleOutline color="success" />;
    case HealingStatus.FAILED:
      return <ErrorOutline color="error" />;
    case HealingStatus.APPROVAL_REQUIRED:
      return <HourglassEmpty color="warning" />;
    default:
      return null;
  }
};

/**
 * Returns a human-readable label for issue type
 * @param issueType IssueType
 * @returns string Human-readable label
 */
const getIssueTypeLabel = (issueType: IssueType): string => {
  switch (issueType) {
    case IssueType.DATA_FORMAT:
      return 'Data Format';
    case IssueType.DATA_QUALITY:
      return 'Data Quality';
    case IssueType.SYSTEM_FAILURE:
      return 'System Failure';
    case IssueType.PERFORMANCE:
      return 'Performance';
    default:
      return issueType;
  }
};

/**
 * Returns the appropriate color for a severity level
 * @param severity AlertSeverity
 * @returns string Color value for the severity
 */
const getSeverityColor = (severity: AlertSeverity): string => {
  switch (severity) {
    case AlertSeverity.CRITICAL:
      return 'error';
    case AlertSeverity.HIGH:
      return 'warning';
    case AlertSeverity.MEDIUM:
      return 'info';
    case AlertSeverity.LOW:
      return 'default';
    default:
      return 'default';
  }
};

/**
 * Returns the appropriate color for a healing status
 * @param status HealingStatus
 * @returns string Color value for the status
 */
const getStatusColor = (status: HealingStatus): string => {
  switch (status) {
    case HealingStatus.PENDING:
      return 'default';
    case HealingStatus.IN_PROGRESS:
      return 'primary';
    case HealingStatus.COMPLETED:
      return 'success';
    case HealingStatus.FAILED:
      return 'error';
    case HealingStatus.APPROVAL_REQUIRED:
      return 'warning';
    default:
      return 'default';
  }
};

/**
 * Dialog component for triggering manual healing of an issue
 */
const ManualHealingDialog: React.FC<ManualHealingDialogProps> = ({ open, issue, onClose, onHeal }) => {
  const [selectedActionId, setSelectedActionId] = useState<string | null>(null);

  const handleActionChange = (event: React.ChangeEvent<{ value: unknown }>) => {
    setSelectedActionId(event.target.value as string);
  };

  const handleHeal = () => {
    if (issue && selectedActionId) {
      onHeal(issue, selectedActionId);
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={onClose} aria-labelledby="manual-healing-dialog-title">
      <DialogTitle id="manual-healing-dialog-title">Manual Healing</DialogTitle>
      <DialogContent>
        {issue && (
          <Box>
            <Typography variant="subtitle1">Issue Details</Typography>
            <Typography>Type: {getIssueTypeLabel(issue.issueType)}</Typography>
            <Typography>Description: {issue.description}</Typography>
            <Typography>Component: {issue.component}</Typography>
            {issue.suggestedActions && issue.suggestedActions.length > 0 ? (
              <FormControl fullWidth>
                <InputLabel id="healing-action-select-label">Select Healing Action</InputLabel>
                <Select
                  labelId="healing-action-select-label"
                  id="healing-action-select"
                  value={selectedActionId || ''}
                  onChange={handleActionChange}
                >
                  {issue.suggestedActions.map((action) => (
                    <MenuItem key={action.actionId} value={action.actionId}>
                      {action.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            ) : (
              <Typography>No suggested actions available.</Typography>
            )}
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleHeal} disabled={!selectedActionId}>
          Confirm Healing
        </Button>
      </DialogActions>
    </Dialog>
  );
};

/**
 * Table component that displays active healing issues with their details and status
 */
const ActiveIssuesTable: React.FC<ActiveIssuesTableProps> = ({
  className,
  title = 'Active Issues',
  maxItems = 10,
  showPagination = true,
  onIssueSelect,
  filters,
  refreshInterval = 30000,
  enableActions = true,
}) => {
  const [issues, setIssues] = useState<HealingIssue[]>([]);
  const [page, setPage] = useState(1);
  const [totalItems, setTotalItems] = useState(0);
  const [selectedIssue, setSelectedIssue] = useState<HealingIssue | null>(null);

  const { get, loading, error } = useApi();
  const { executeRequest: triggerManualHealing, loading: healingLoading } = useApiRequest();

  const fetchIssues = useCallback(async () => {
    try {
      const response = await get<any>(endpoints.healing.ISSUES, {
        params: {
          page,
          pageSize: maxItems,
          ...filters,
        },
      });
      setIssues(response.items);
      setTotalItems(response.pagination.totalItems);
    } catch (err) {
      console.error('Failed to fetch active issues:', err);
    }
  }, [get, page, maxItems, filters]);

  useEffect(() => {
    fetchIssues();

    if (refreshInterval > 0) {
      const intervalId = setInterval(fetchIssues, refreshInterval);
      return () => clearInterval(intervalId);
    }
  }, [fetchIssues, refreshInterval]);

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  const handleHealIssue = async (issue: HealingIssue, actionId: string) => {
    try {
      await triggerManualHealing(healingService.triggerManualHealing, {
        issueId: issue.issueId,
        actionId: actionId,
        parameters: {},
        notes: 'Triggered from Active Issues Table',
      });
      fetchIssues(); // Refresh issues after healing
    } catch (err) {
      console.error('Failed to trigger manual healing:', err);
    }
  };

  const columns = React.useMemo(() => [
    {
      id: 'issueType',
      label: 'Issue Type',
      format: (value: IssueType) => getIssueTypeLabel(value),
    },
    {
      id: 'component',
      label: 'Component',
    },
    {
      id: 'severity',
      label: 'Severity',
      format: (value: AlertSeverity) => (
        <Chip
          icon={getSeverityIcon(value)}
          label={value}
          color={getSeverityColor(value)}
          size="small"
        />
      ),
    },
    {
      id: 'status',
      label: 'Status',
      format: (value: HealingStatus) => (
        <Chip
          icon={getStatusIcon(value)}
          label={value}
          color={getStatusColor(value)}
          size="small"
        />
      ),
    },
    {
      id: 'detectedAt',
      label: 'Detected At',
      format: (value: string) => formatDate(value, 'MMM d, yyyy HH:mm'),
    },
    {
      id: 'actions',
      label: 'Actions',
      renderCell: (value: any, row: HealingIssue) => (
        <Box>
          {enableActions && row.suggestedActions && row.suggestedActions.length > 0 ? (
            <Tooltip title="Heal Issue">
              <IconButton onClick={() => setSelectedIssue(row)}>
                <AutoFixHigh />
              </IconButton>
            </Tooltip>
          ) : (
            <Tooltip title="No Actions Available">
              <IconButton disabled>
                <AutoFixHigh />
              </IconButton>
            </Tooltip>
          )}
          <Tooltip title="View Details">
            <IconButton onClick={() => onIssueSelect && onIssueSelect(row)}>
              <MoreVert />
            </IconButton>
          </Tooltip>
        </Box>
      ),
    },
  ], [enableActions, onIssueSelect]);

  return (
    <Box>
      <Table
        className={className}
        title={title}
        columns={columns}
        data={issues}
        loading={loading}
        error={error?.message}
        pagination={showPagination}
        totalItems={totalItems}
        onPageChange={handlePageChange}
        maxItems={maxItems}
        onRowClick={onIssueSelect}
      />
      <ManualHealingDialog
        open={!!selectedIssue}
        issue={selectedIssue}
        onClose={() => setSelectedIssue(null)}
        onHeal={handleHealIssue}
      />
    </Box>
  );
};

export default ActiveIssuesTable;