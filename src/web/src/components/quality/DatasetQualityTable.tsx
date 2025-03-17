import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import { Box, Typography } from '@mui/material'; // @mui/material ^5.11.0
import TrendingUp from '@mui/icons-material/TrendingUp'; // @mui/icons-material ^5.11.0
import TrendingDown from '@mui/icons-material/TrendingDown'; // @mui/icons-material ^5.11.0
import TrendingFlat from '@mui/icons-material/TrendingFlat'; // @mui/icons-material ^5.11.0

import Table from '../common/Table';
import Badge from '../common/Badge';
import Tooltip from '../common/Tooltip';
import qualityService from '../../services/api/qualityService';
import useApi from '../../hooks/useApi';
import { formatDate, formatPercentage } from '../../utils/formatting';
import { DatasetQualitySummary, QualityTrend } from '../../types/quality';

/**
 * Interface defining the props for the DatasetQualityTable component
 */
interface DatasetQualityTableProps {
  /** Additional CSS class for styling */
  className?: string;
  /** Filter results to a specific dataset */
  dataset?: string;
  /** Callback when a dataset is selected */
  onDatasetSelect?: (dataset: string) => void;
  /** Callback when a table is selected */
  onTableSelect?: (table: string, dataset: string) => void;
  /** Interval in milliseconds to refresh data */
  refreshInterval?: number;
  /** Maximum number of datasets to display */
  maxItems?: number;
  /** Whether to show action buttons */
  showActions?: boolean;
}

/**
 * Returns the appropriate trend icon component based on the trend value
 * @param trend The trend value
 * @returns The trend icon component
 */
const getTrendIcon = (trend: QualityTrend): React.ReactNode => {
  switch (trend) {
    case QualityTrend.IMPROVING:
      return <TrendingUp color="success" />;
    case QualityTrend.DECLINING:
      return <TrendingDown color="error" />;
    case QualityTrend.STABLE:
      return <TrendingFlat color="default" />;
    case QualityTrend.UNKNOWN:
      return null;
    default:
      return null;
  }
};

/**
 * Returns the appropriate color based on the quality score
 * @param score The quality score
 * @returns The color code or name
 */
const getQualityColor = (score: number): string => {
  if (score >= 90) {
    return 'success';
  }
  if (score >= 70) {
    return 'warning';
  }
  return 'error';
};

/**
 * A table component that displays dataset quality summaries
 */
const DatasetQualityTable: React.FC<DatasetQualityTableProps> = ({
  className,
  dataset,
  onDatasetSelect,
  onTableSelect,
  refreshInterval = 60000,
  maxItems = 10,
  showActions = true,
}) => {
  // State for currently expanded dataset
  const [expandedDataset, setExpandedDataset] = useState<string | null>(null);
  // State for current column to sort by
  const [sortBy, setSortBy] = useState<string>('overallScore');
  // State for current sort direction
  const [sortDirection, setSortDirection] = useState<string>('desc');

  // Use the useApi hook to fetch dataset quality summaries
  const { data, loading, error, get } = useApi<DatasetQualitySummary[]>();

  // Define the fetchData function to fetch data from the API
  const fetchData = useCallback(async () => {
    await get(qualityService.getDatasetQualitySummaries, { dataset });
  }, [get, dataset]);

  // Fetch data on component mount and when dataset filter changes
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Set up periodic data refresh
  useEffect(() => {
    const intervalId = setInterval(fetchData, refreshInterval);
    // Clear interval on component unmount
    return () => clearInterval(intervalId);
  }, [fetchData, refreshInterval]);

  // Define table columns
  const columns = useMemo(() => [
    {
      id: 'dataset',
      label: 'Dataset',
      sortable: true,
      width: '30%',
    },
    {
      id: 'overallScore',
      label: 'Quality Score',
      sortable: true,
      width: '20%',
      renderCell: (value: number) => (
        <Tooltip title={`Quality Score: ${formatPercentage(value)}`}>
          <Typography color={getQualityColor(value)}>
            {formatPercentage(value)}
          </Typography>
        </Tooltip>
      ),
    },
    {
      id: 'trend',
      label: 'Trend',
      sortable: true,
      width: '15%',
      renderCell: (value: QualityTrend) => (
        <Tooltip title={`Quality Trend: ${value}`}>
          <Box display="flex" alignItems="center">
            {getTrendIcon(value)}
          </Box>
        </Tooltip>
      ),
    },
    {
      id: 'issueCount',
      label: 'Issues',
      sortable: true,
      width: '15%',
      renderCell: (value: number) => (
        <Tooltip title={`Number of Issues: ${value}`}>
          <Badge label={value.toString()} color={value > 0 ? 'warning' : 'success'} />
        </Tooltip>
      ),
    },
    {
      id: 'lastUpdated',
      label: 'Last Updated',
      sortable: true,
      width: '20%',
      format: (value: string) => formatDate(value),
    },
  ], []);

  // Handle row click to trigger onDatasetSelect callback
  const handleRowClick = useCallback((row: DatasetQualitySummary) => {
    if (onDatasetSelect) {
      onDatasetSelect(row.dataset);
    }
    setExpandedDataset(expandedDataset === row.dataset ? null : row.dataset);
  }, [onDatasetSelect, expandedDataset]);

  // Define expandable row content
  const renderExpandedRow = useCallback((row: DatasetQualitySummary) => {
    return (
      <Table
        title="Table Quality"
        columns={[
          { id: 'table', label: 'Table Name', sortable: true },
          { id: 'qualityScore', label: 'Quality Score', sortable: true },
          { id: 'issues', label: 'Issues', sortable: false },
        ]}
        data={row.tables.map((tableData) => ({
          table: tableData.table,
          qualityScore: formatPercentage(tableData.qualityScore),
          issues: 'TBD',
        }))}
        onRowClick={(tableRow) => {
          if (onTableSelect) {
            onTableSelect(tableRow.table, row.dataset);
          }
        }}
      />
    );
  }, [onTableSelect]);

  return (
    <Table
      className={className}
      title="Dataset Quality"
      columns={columns}
      data={data || []}
      loading={loading}
      error={error?.message}
      defaultSortBy={sortBy}
      defaultSortDirection={sortDirection}
      onSortChange={(column, direction) => {
        setSortBy(column);
        setSortDirection(direction);
      }}
      onRowClick={handleRowClick}
    />
  );
};

export default DatasetQualityTable;