import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
} from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Divider,
  Chip,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  FilterList,
  Refresh,
  CheckCircle,
  Error,
  Warning,
  Info,
  AutoFixHigh,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Table from '../common/Table';
import Badge from '../common/Badge';
import Button from '../common/Button';
import Select from '../common/Select';
import Tooltip from '../common/Tooltip';
import { useQuality } from '../../contexts/QualityContext';
import {
  QualityIssue,
  QualityDimension,
  QualityIssueStatus,
} from '../../types/quality';
import { AlertSeverity, HealingStatus } from '../../types/global';
import { formatDate } from '../../utils/date';

/**
 * Interface for the ValidationIssuesTable component props
 */
interface ValidationIssuesTableProps {
  className?: string;
  title?: string;
  showFilters?: boolean;
  pageSize?: number;
  onIssueSelect?: (issue: QualityIssue) => void;
  selectedDataset?: string;
  selectedTable?: string;
  hideDatasetColumn?: boolean;
  hideTableColumn?: boolean;
  allowStatusChange?: boolean;
}

/**
 * A table component that displays data quality validation issues with filtering, sorting, and action capabilities
 */
const ValidationIssuesTable: React.FC<ValidationIssuesTableProps> = ({
  className,
  title = 'Validation Issues',
  showFilters = true,
  pageSize = 10,
  onIssueSelect,
  selectedDataset,
  selectedTable,
  hideDatasetColumn = false,
  hideTableColumn = false,
  allowStatusChange = true,
}) => {
  // Access quality context
  const { issues, loading, error, filters, setFilters, updateIssueStatus, fetchIssues } = useQuality();

  // State for selected issue
  const [selectedIssue, setSelectedIssue] = useState<QualityIssue | null>(null);

  /**
   * Handles changes to the filter values
   * @param filterName - The name of the filter to update
   * @param value - The new value for the filter
   */
  const handleFilterChange = (filterName: string, value: any) => {
    // Create a new filter object with the updated filter value
    const newFilters = { ...filters, [filterName]: value };
    // Call setFilters from the quality context with the new filter object
    setFilters(newFilters);
    // This will trigger a re-fetch of issues with the new filters applied
  };

  /**
   * Handles changing the status of a quality issue
   * @param issueId - The ID of the issue to update
   * @param newStatus - The new status to set for the issue
   */
  const handleStatusChange = async (issueId: string, newStatus: QualityIssueStatus) => {
    try {
      // Call updateIssueStatus from the quality context with the issue ID and new status
      await updateIssueStatus(issueId, newStatus);
      // Handle any errors that occur during the update
    } catch (err) {
      console.error('Failed to update issue status:', err);
    } finally {
      // Refresh the issues list after successful update
      fetchIssues();
    }
  };

  /**
   * Handles manual refresh of the issues list
   */
  const handleRefresh = async () => {
    try {
      // Call fetchIssues from the quality context to reload the issues data
      await fetchIssues();
      // Handle any errors that occur during the refresh
    } catch (err) {
      console.error('Failed to refresh issues:', err);
    }
  };

  /**
   * Handles selection of an issue for detailed view
   * @param issue - The selected quality issue
   */
  const handleIssueSelect = (issue: QualityIssue) => {
    // Set the selected issue in component state
    setSelectedIssue(issue);
    // If onIssueSelect prop is provided, call it with the selected issue
    if (onIssueSelect) {
      onIssueSelect(issue);
    }
  };

  /**
   * Returns the appropriate icon for a severity level
   * @param severity - The severity level
   */
  const getSeverityIcon = (severity: AlertSeverity): React.ReactNode => {
    switch (severity) {
      case AlertSeverity.CRITICAL:
        return <Error color="error" />;
      case AlertSeverity.HIGH:
        return <Warning color="warning" />;
      case AlertSeverity.MEDIUM:
        return <Info color="info" />;
      case AlertSeverity.LOW:
        return <CheckCircle color="success" />;
      default:
        return <Info />;
    }
  };

  /**
   * Returns the appropriate icon for a healing status
   * @param status - The healing status
   */
  const getHealingStatusIcon = (status: HealingStatus | null | undefined): React.ReactNode => {
    if (status === null || status === undefined) {
      return null;
    }

    switch (status) {
      case HealingStatus.COMPLETED:
        return <CheckCircle color="success" />;
      case HealingStatus.IN_PROGRESS:
        return <AutoFixHigh color="info" />;
      case HealingStatus.FAILED:
        return <Error color="error" />;
      case HealingStatus.PENDING:
        return <Warning color="warning" />;
      default:
        return null;
    }
  };

  /**
   * Renders a cell with severity information
   * @param severity - The severity level
   */
  const renderSeverityCell = (severity: AlertSeverity): React.ReactNode => {
    // Create a Badge component with color based on severity
    return (
      <Badge
        label={
          <>
            {getSeverityIcon(severity)}
            {severity.charAt(0) + severity.slice(1).toLowerCase()}
          </>
        }
        color={
          severity === AlertSeverity.CRITICAL
            ? 'error'
            : severity === AlertSeverity.HIGH
              ? 'warning'
              : severity === AlertSeverity.MEDIUM
                ? 'info'
                : 'success'
        }
      />
    );
  };

  /**
   * Renders a cell with issue status information
   * @param status - The issue status
   * @param issue - The quality issue
   */
  const renderStatusCell = (status: QualityIssueStatus, issue: QualityIssue): React.ReactNode => {
    // Create a Badge component with color based on status
    return (
      <Badge
        label={status.charAt(0) + status.slice(1).toLowerCase()}
      />
    );
  };

  /**
   * Renders a cell with healing status information
   * @param status - The healing status
   * @param issue - The quality issue
   */
  const renderHealingStatusCell = (status: HealingStatus | null | undefined, issue: QualityIssue): React.ReactNode => {
    // If status is null or undefined, return 'N/A'
    if (status === null || status === undefined) {
      return 'N/A';
    }

    // Create a Badge component with color based on healing status
    return (
      <Tooltip title={`Healing ${status.charAt(0) + status.slice(1).toLowerCase()}`}>
        <Badge
          label={
            <>
              {getHealingStatusIcon(status)}
              {status.charAt(0) + status.slice(1).toLowerCase()}
            </>
          }
          color={
            status === HealingStatus.COMPLETED
              ? 'success'
              : status === HealingStatus.IN_PROGRESS
                ? 'info'
                : status === HealingStatus.FAILED
                  ? 'error'
                  : 'default'
          }
        />
      </Tooltip>
    );
  };

  // Define table columns
  const columns = useMemo(() => [
    {
      id: 'severity',
      label: 'Severity',
      width: '120px',
      sortable: true,
      renderCell: renderSeverityCell,
    },
    {
      id: 'dataset',
      label: 'Dataset',
      width: '150px',
      sortable: true,
      hidden: hideDatasetColumn,
    },
    {
      id: 'table',
      label: 'Table',
      width: '150px',
      sortable: true,
      hidden: hideTableColumn,
    },
    {
      id: 'column',
      label: 'Column',
      width: '150px',
      sortable: true,
    },
    {
      id: 'description',
      label: 'Description',
      minWidth: '250px',
      sortable: true,
    },
    {
      id: 'dimension',
      label: 'Dimension',
      width: '150px',
      sortable: true,
      format: (value: string) => value.charAt(0) + value.slice(1).toLowerCase(),
    },
    {
      id: 'status',
      label: 'Status',
      width: '150px',
      sortable: true,
      renderCell: renderStatusCell,
    },
    {
      id: 'healingStatus',
      label: 'Self-Healing',
      width: '150px',
      sortable: true,
      renderCell: renderHealingStatusCell,
    },
    {
      id: 'detectedAt',
      label: 'Detected',
      width: '180px',
      sortable: true,
      format: (value: string) => formatDate(value),
    },
  ], [hideDatasetColumn, hideTableColumn]);

  // Filter options
  const dimensionOptions = useMemo(() => Object.values(QualityDimension).map(dimension => ({
    value: dimension,
    label: dimension.charAt(0) + dimension.slice(1).toLowerCase(),
  })), []);

  const severityOptions = useMemo(() => Object.values(AlertSeverity).map(severity => ({
    value: severity,
    label: severity.charAt(0) + severity.slice(1).toLowerCase(),
  })), []);

  const statusOptions = useMemo(() => Object.values(QualityIssueStatus).map(status => ({
    value: status,
    label: status.charAt(0) + status.slice(1).toLowerCase(),
  })), []);

  // Apply selectedDataset and selectedTable filters when provided as props
  useEffect(() => {
    if (selectedDataset) {
      setFilters({ dataset: selectedDataset });
    }
    if (selectedTable) {
      setFilters({ table: selectedTable });
    }
  }, [selectedDataset, selectedTable, setFilters]);

  return (
    <Box className={className}>
      <Table
        title={title}
        columns={columns}
        data={issues}
        loading={loading}
        error={error}
        pageSize={pageSize}
        onRowClick={handleIssueSelect}
        actions={
          showFilters && (
            <Grid container spacing={2} alignItems="center">
              <Grid item>
                <Typography variant="subtitle2">Filters:</Typography>
              </Grid>
              <Grid item>
                <Select
                  label="Dimension"
                  options={dimensionOptions}
                  value={filters.dimension || ''}
                  onChange={(value) => handleFilterChange('dimension', value)}
                />
              </Grid>
              <Grid item>
                <Select
                  label="Severity"
                  options={severityOptions}
                  value={filters.severity || ''}
                  onChange={(value) => handleFilterChange('severity', value)}
                />
              </Grid>
              <Grid item>
                <Select
                  label="Status"
                  options={statusOptions}
                  value={filters.status || ''}
                  onChange={(value) => handleFilterChange('status', value)}
                />
              </Grid>
            </Grid>
          )
        }
      />
    </Box>
  );
};

export default ValidationIssuesTable;