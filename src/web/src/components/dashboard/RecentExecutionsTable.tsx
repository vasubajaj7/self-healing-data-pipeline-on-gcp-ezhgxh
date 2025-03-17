import React, { useCallback } from 'react'; // react ^18.2.0
import { Box, Typography, Chip, Tooltip } from '@mui/material'; // @mui/material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0
import { format } from 'date-fns'; // date-fns ^2.29.3
import Table from '../common/Table';
import { useDashboard } from '../../contexts/DashboardContext';
import { PipelineExecution } from '../../types/dashboard';

/**
 * Interface defining the props for the RecentExecutionsTable component
 */
interface RecentExecutionsTableProps {
  className?: string;
  maxItems?: number;
  showHeader?: boolean;
  onViewAll?: () => void;
}

/**
 * Component that displays a table of recent pipeline executions
 */
const RecentExecutionsTable: React.FC<RecentExecutionsTableProps> = ({
  className,
  maxItems = 5,
  showHeader = true,
  onViewAll,
}) => {
  // LD1: Destructure props with defaults
  // LD1: Use useDashboard hook to get dashboard data and loading state
  const { dashboardData, loading } = useDashboard();
  const theme = useTheme();
  const navigate = useNavigate();

  // LD1: Define table columns with appropriate formatters
  const columns = React.useMemo(
    () => [
      {
        id: 'pipelineName',
        label: 'Pipeline',
        sortable: true,
        renderCell: (value: string) => (
          <Tooltip title={value} placement="top">
            <Typography variant="body2" sx={{ maxWidth: '200px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {value}
            </Typography>
          </Tooltip>
        ),
      },
      {
        id: 'status',
        label: 'Status',
        sortable: true,
        renderCell: (status: string, row: PipelineExecution) => {
          // LD1: Format status column with colored chips
          let color = 'default';
          if (status === 'succeeded') {
            color = 'success';
          } else if (status === 'failed') {
            color = 'error';
          } else if (status === 'running') {
            color = 'info';
          }
          return (
            <Chip
              label={status}
              color={color}
              size="small"
              sx={{ minWidth: 80, justifyContent: 'center' }}
            />
          );
        },
      },
      {
        id: 'startTime',
        label: 'Start Time',
        sortable: true,
        format: (startTime: string) => {
          // LD1: Format timestamp column with date-fns
          return format(new Date(startTime), 'MMM dd, yyyy HH:mm:ss');
        },
      },
      {
        id: 'duration',
        label: 'Duration',
        sortable: true,
        format: (duration: number) => {
          // LD1: Format duration column with formatDuration function
          const formatDuration = (ms: number): string => {
            const seconds = Math.floor((ms / 1000) % 60);
            const minutes = Math.floor((ms / (1000 * 60)) % 60);
            const hours = Math.floor((ms / (1000 * 60 * 60)) % 24);
          
            const parts: string[] = [];
            if (hours > 0) {
              parts.push(`${hours}h`);
            }
            if (minutes > 0) {
              parts.push(`${minutes}m`);
            }
            parts.push(`${seconds}s`);
          
            return parts.join(' ');
          };
          return formatDuration(duration);
        },
      },
    ],
    [theme]
  );

  // LD1: Handle row click to navigate to execution details
  const handleRowClick = useCallback(
    (row: PipelineExecution) => {
      navigate(`/pipeline/${row.pipelineName}`);
    },
    [navigate]
  );

  let executions = dashboardData?.recentExecutions || [];

  if (maxItems && executions.length > maxItems) {
    executions = executions.slice(0, maxItems);
  }

  return (
    <Box className={className}>
      {/* LD1: Render Table component with columns and data */}
      <Table<PipelineExecution>
        title={showHeader ? 'Recent Executions' : undefined}
        columns={columns}
        data={executions}
        loading={loading} // LD1: Apply loading state from dashboard context
        onRowClick={handleRowClick}
      />
      {onViewAll && (
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 1, pr: 2 }}>
          <Typography
            component="a"
            href="#"
            onClick={(e) => {
              e.preventDefault();
              onViewAll();
            }}
            sx={{
              fontSize: '0.875rem',
              fontWeight: 500,
              color: theme.palette.primary.main,
              textDecoration: 'none',
              cursor: 'pointer',
              '&:hover': {
                textDecoration: 'underline',
              },
            }}
          >
            View All
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default RecentExecutionsTable;