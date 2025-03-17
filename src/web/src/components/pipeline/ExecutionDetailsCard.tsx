import React, { useState, useCallback } from 'react';
import { Box, Typography, Divider, Chip, Grid } from '@mui/material';
import { Stop, Refresh, Visibility } from '@mui/icons-material';

import Card from '../common/Card';
import Button from '../common/Button';
import { PipelineExecution } from '../../types/api';
import { PipelineStatus } from '../../types/global';
import { formatDateTimeShort, getDateDifference } from '../../utils/date';
import pipelineService from '../../services/api/pipelineService';

/**
 * Props for the ExecutionDetailsCard component
 */
interface ExecutionDetailsCardProps {
  execution: PipelineExecution;
  loading?: boolean;
  onViewLogs: () => void;
  onViewDataSamples: () => void;
  onRefresh: () => void;
  className?: string;
}

/**
 * Calculates the duration between start and end time
 * @param startTime The start timestamp
 * @param endTime The end timestamp (optional, if not provided uses current time)
 * @returns Formatted duration string
 */
const calculateDuration = (startTime: string, endTime?: string | null): string => {
  const end = endTime ? new Date(endTime) : new Date();
  const diffInMinutes = getDateDifference(startTime, end, 'minutes') || 0;
  
  const hours = Math.floor(diffInMinutes / 60);
  const minutes = Math.floor(diffInMinutes % 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
};

/**
 * Gets the color for a status chip based on pipeline status
 * @param status The pipeline status
 * @returns Color style object
 */
const getStatusColor = (status: PipelineStatus) => {
  switch (status) {
    case PipelineStatus.HEALTHY:
      return { backgroundColor: '#4caf50', color: 'white' };
    case PipelineStatus.WARNING:
      return { backgroundColor: '#ff9800', color: 'white' };
    case PipelineStatus.ERROR:
      return { backgroundColor: '#f44336', color: 'white' };
    case PipelineStatus.INACTIVE:
      return { backgroundColor: '#9e9e9e', color: 'white' };
    default:
      return { backgroundColor: '#9e9e9e', color: 'white' };
  }
};

/**
 * Component that displays detailed information about a pipeline execution
 * Shows execution metadata, status, timing information, error details if applicable,
 * and provides action buttons for execution management.
 */
const ExecutionDetailsCard: React.FC<ExecutionDetailsCardProps> = ({
  execution,
  loading = false,
  onViewLogs,
  onViewDataSamples,
  onRefresh,
  className,
}) => {
  // State to track operations in progress
  const [isStoppingExecution, setIsStoppingExecution] = useState(false);
  const [isRetryingExecution, setIsRetryingExecution] = useState(false);

  // Determine if execution is currently running
  const isRunning = !execution.endTime || execution.status === PipelineStatus.HEALTHY;
  
  // Handle stopping the execution
  const handleStopExecution = useCallback(async () => {
    if (isStoppingExecution || !execution.executionId) return;
    
    try {
      setIsStoppingExecution(true);
      await pipelineService.stopPipeline(execution.executionId);
      onRefresh();
    } catch (error) {
      console.error('Failed to stop execution:', error);
    } finally {
      setIsStoppingExecution(false);
    }
  }, [execution.executionId, isStoppingExecution, onRefresh]);

  // Handle retrying the execution
  const handleRetryExecution = useCallback(async () => {
    if (isRetryingExecution || !execution.executionId) return;
    
    try {
      setIsRetryingExecution(true);
      await pipelineService.retryPipeline(execution.executionId);
      onRefresh();
    } catch (error) {
      console.error('Failed to retry execution:', error);
    } finally {
      setIsRetryingExecution(false);
    }
  }, [execution.executionId, isRetryingExecution, onRefresh]);

  // Custom styles for component elements
  const customStyles = {
    metadataItem: {
      marginBottom: '8px',
      display: 'flex',
      alignItems: 'flex-start'
    },
    metadataLabel: {
      fontWeight: 'bold',
      marginRight: '8px',
      minWidth: '120px'
    },
    metadataValue: {
      wordBreak: 'break-word'
    },
    errorDetails: {
      marginTop: '16px',
      marginBottom: '16px',
      backgroundColor: 'rgba(244, 67, 54, 0.08)',
      padding: '12px',
      borderRadius: '4px',
      border: '1px solid rgba(244, 67, 54, 0.5)'
    },
    actionsContainer: {
      display: 'flex',
      justifyContent: 'flex-end',
      gap: '8px',
      marginTop: '16px'
    },
    statusChip: {
      HEALTHY: { backgroundColor: '#4caf50', color: 'white' },
      WARNING: { backgroundColor: '#ff9800', color: 'white' },
      ERROR: { backgroundColor: '#f44336', color: 'white' },
      INACTIVE: { backgroundColor: '#9e9e9e', color: 'white' }
    }
  };

  return (
    <Card 
      title="Execution Details"
      loading={loading}
      className={className}
    >
      <Box sx={{ padding: 2 }}>
        {/* Status display */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6" component="div">
            Status
          </Typography>
          <Chip 
            label={execution.status} 
            sx={getStatusColor(execution.status)}
          />
        </Box>
        
        <Divider sx={{ my: 2 }} />
        
        {/* Execution metadata */}
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <Box sx={customStyles.metadataItem}>
              <Typography sx={customStyles.metadataLabel} variant="body2">
                Pipeline:
              </Typography>
              <Typography variant="body2" sx={customStyles.metadataValue}>
                {execution.pipelineName}
              </Typography>
            </Box>
          </Grid>
          
          <Grid item xs={12}>
            <Box sx={customStyles.metadataItem}>
              <Typography sx={customStyles.metadataLabel} variant="body2">
                Execution ID:
              </Typography>
              <Typography variant="body2" sx={customStyles.metadataValue}>
                {execution.executionId}
              </Typography>
            </Box>
          </Grid>
          
          <Grid item xs={12}>
            <Box sx={customStyles.metadataItem}>
              <Typography sx={customStyles.metadataLabel} variant="body2">
                Start Time:
              </Typography>
              <Typography variant="body2" sx={customStyles.metadataValue}>
                {formatDateTimeShort(execution.startTime)}
              </Typography>
            </Box>
          </Grid>
          
          <Grid item xs={12}>
            <Box sx={customStyles.metadataItem}>
              <Typography sx={customStyles.metadataLabel} variant="body2">
                End Time:
              </Typography>
              <Typography variant="body2" sx={customStyles.metadataValue}>
                {execution.endTime ? formatDateTimeShort(execution.endTime) : 'Running...'}
              </Typography>
            </Box>
          </Grid>
          
          <Grid item xs={12}>
            <Box sx={customStyles.metadataItem}>
              <Typography sx={customStyles.metadataLabel} variant="body2">
                Duration:
              </Typography>
              <Typography variant="body2" sx={customStyles.metadataValue}>
                {execution.startTime ? calculateDuration(execution.startTime, execution.endTime) : 'N/A'}
              </Typography>
            </Box>
          </Grid>
          
          {execution.recordsProcessed !== undefined && (
            <Grid item xs={12}>
              <Box sx={customStyles.metadataItem}>
                <Typography sx={customStyles.metadataLabel} variant="body2">
                  Records Processed:
                </Typography>
                <Typography variant="body2" sx={customStyles.metadataValue}>
                  {execution.recordsProcessed.toLocaleString()}
                </Typography>
              </Box>
            </Grid>
          )}
        </Grid>
        
        {/* Error details section */}
        {execution.errorDetails && (
          <Box sx={customStyles.errorDetails}>
            <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>
              Error Details:
            </Typography>
            <Typography variant="body2" component="pre" sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
              {execution.errorDetails}
            </Typography>
          </Box>
        )}
        
        {/* Self-healing status section */}
        {execution.status === PipelineStatus.ERROR && execution.selfHealingStatus && (
          <Box sx={{ mt: 2, mb: 2 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 'bold', mb: 1 }}>
              Self-Healing Status:
            </Typography>
            <Typography variant="body2">
              {execution.selfHealingStatus === 'IN_PROGRESS' 
                ? 'Self-healing in progress...' 
                : execution.selfHealingStatus === 'COMPLETED'
                ? 'Self-healing completed successfully'
                : execution.selfHealingStatus === 'FAILED'
                ? 'Self-healing attempt failed'
                : execution.selfHealingStatus}
            </Typography>
          </Box>
        )}
        
        {/* Action buttons */}
        <Box sx={customStyles.actionsContainer}>
          <Button
            variant="outlined"
            onClick={onViewLogs}
            startIcon={<Visibility />}
            size="small"
          >
            View Logs
          </Button>
          
          <Button
            variant="outlined"
            onClick={onViewDataSamples}
            startIcon={<Visibility />}
            size="small"
          >
            View Data Samples
          </Button>
          
          {isRunning && (
            <Button
              variant="outlined"
              color="error"
              onClick={handleStopExecution}
              startIcon={<Stop />}
              size="small"
              loading={isStoppingExecution}
            >
              Stop
            </Button>
          )}
          
          {!isRunning && execution.status === PipelineStatus.ERROR && (
            <Button
              variant="outlined"
              color="primary"
              onClick={handleRetryExecution}
              startIcon={<Refresh />}
              size="small"
              loading={isRetryingExecution}
            >
              Retry
            </Button>
          )}
          
          <Button
            variant="outlined"
            color="primary"
            onClick={onRefresh}
            startIcon={<Refresh />}
            size="small"
          >
            Refresh
          </Button>
        </Box>
      </Box>
    </Card>
  );
};

export default ExecutionDetailsCard;