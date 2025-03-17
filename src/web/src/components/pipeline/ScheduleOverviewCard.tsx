import React, { useState, useEffect, useMemo } from 'react';
import { Box, Typography, Tooltip, Chip, Button } from '@mui/material';
import { Schedule, Refresh } from '@mui/icons-material';

// Import internal components
import Card from '../common/Card';
import Table from '../common/Table';

// Import hooks and services
import { useApi } from '../../hooks/useApi';
import pipelineService from '../../services/api/pipelineService';

// Import utilities
import { formatDateTimeShort, formatTimeShort, getTimeAgo } from '../../utils/date';
import { truncateText } from '../../utils/formatting';

/**
 * Props for the ScheduleOverviewCard component
 */
interface ScheduleOverviewCardProps {
  /**
   * Optional ID of a specific pipeline to show schedule for. If not provided, shows schedules for all pipelines.
   */
  pipelineId?: string;
  /**
   * Maximum number of scheduled executions to display
   */
  limit?: number;
  /**
   * Whether to show the card header
   */
  showHeader?: boolean;
  /**
   * Additional CSS class for styling
   */
  className?: string;
}

/**
 * Interface for pipeline schedule information
 */
interface ScheduleInfo {
  pipelineId: string;
  pipelineName: string;
  schedule: string;
  nextRun: string;
  frequency: string;
}

/**
 * Custom hook to fetch and format pipeline schedule data
 */
const useScheduleData = (pipelineId?: string, limit: number = 5) => {
  const [schedules, setSchedules] = useState<ScheduleInfo[]>([]);
  const api = useApi();

  const fetchSchedules = async () => {
    try {
      if (pipelineId) {
        // Fetch schedule for a specific pipeline
        const scheduleResponse = await api.get(
          `/api/pipeline/${pipelineId}/schedule`
        );
        
        // Get pipeline details to get the name
        const pipelineResponse = await api.get(
          `/api/pipeline/${pipelineId}`
        );
        
        if (scheduleResponse && pipelineResponse) {
          setSchedules([{
            pipelineId,
            pipelineName: pipelineResponse.pipelineName || 'Unknown Pipeline',
            schedule: scheduleResponse.schedule,
            nextRun: scheduleResponse.nextRun,
            frequency: scheduleResponse.frequency || 'Custom Schedule'
          }]);
        }
      } else {
        // Fetch all pipeline definitions
        const pipelinesResponse = await api.get('/api/pipeline/list', {
          params: { page: 1, pageSize: limit * 2 } // Get more than we need to ensure we have enough after filtering
        });
        
        if (pipelinesResponse?.items && pipelinesResponse.items.length > 0) {
          const pipelines = pipelinesResponse.items;
          const schedulePromises = [];
          
          for (const pipeline of pipelines) {
            schedulePromises.push(
              api.get(`/api/pipeline/${pipeline.pipelineId}/schedule`)
                .then(scheduleData => ({
                  pipelineId: pipeline.pipelineId,
                  pipelineName: pipeline.pipelineName,
                  schedule: scheduleData.schedule,
                  nextRun: scheduleData.nextRun,
                  frequency: scheduleData.frequency || 'Custom Schedule'
                }))
                .catch(() => null) // Skip pipelines with no schedule
            );
          }
          
          const scheduleResults = await Promise.all(schedulePromises);
          
          // Filter out null results, sort by next run time, and limit
          const validSchedules = scheduleResults
            .filter(s => s !== null)
            .sort((a, b) => new Date(a!.nextRun).getTime() - new Date(b!.nextRun).getTime())
            .slice(0, limit);
            
          setSchedules(validSchedules as ScheduleInfo[]);
        }
      }
    } catch (error) {
      console.error('Error fetching pipeline schedules:', error);
      setSchedules([]);
    }
  };

  // Initial fetch on component mount and when dependencies change
  useEffect(() => {
    fetchSchedules();
  }, [pipelineId, limit]);

  return {
    schedules,
    loading: api.loading,
    error: api.error,
    refreshData: fetchSchedules
  };
};

/**
 * Card component that displays upcoming pipeline schedules
 */
const ScheduleOverviewCard: React.FC<ScheduleOverviewCardProps> = ({
  pipelineId,
  limit = 5,
  showHeader = true,
  className
}) => {
  const { schedules, loading, error, refreshData } = useScheduleData(pipelineId, limit);

  // Define table columns for the schedule data
  const columns = useMemo(() => [
    {
      id: 'pipelineName',
      label: 'Pipeline',
      format: (value: string) => (
        <Tooltip title={value}>
          <Typography variant="body2" noWrap>
            {truncateText(value, 25)}
          </Typography>
        </Tooltip>
      )
    },
    {
      id: 'nextRun',
      label: 'Next Run',
      format: (value: string) => formatDateTimeShort(value)
    },
    {
      id: 'frequency',
      label: 'Frequency',
      format: (value: string) => (
        <Chip 
          label={value} 
          size="small" 
          color="primary" 
          variant="outlined"
          sx={{ fontSize: '0.75rem', margin: '0 4px' }}
        />
      )
    },
    {
      id: 'timeUntil',
      label: 'Time Until',
      format: (_: any, row: ScheduleInfo) => {
        const nextRun = new Date(row.nextRun);
        const now = new Date();
        
        if (nextRun <= now) {
          return <Typography variant="body2" color="error">Overdue</Typography>;
        }
        
        return getTimeAgo(nextRun);
      }
    }
  ], []);

  return (
    <Card
      title={
        showHeader ? (
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <Schedule sx={{ marginRight: '8px', verticalAlign: 'middle' }} />
              <Typography variant="h6">Schedule Overview</Typography>
            </Box>
            <Button 
              variant="text" 
              size="small"
              onClick={refreshData}
              startIcon={<Refresh />}
              sx={{ minWidth: 'auto', padding: '4px' }}
              aria-label="Refresh schedule data"
            >
              Refresh
            </Button>
          </Box>
        ) : undefined
      }
      loading={loading}
      error={error}
      className={className}
      minHeight="300px"
    >
      <Table 
        columns={columns}
        data={schedules}
        emptyMessage="No upcoming scheduled executions found"
        pagination={false}
        dense={true}
        stickyHeader={schedules.length > 5}
      />
    </Card>
  );
};

export default ScheduleOverviewCard;