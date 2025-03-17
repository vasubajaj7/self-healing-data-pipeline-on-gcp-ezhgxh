import React, { useState, useCallback } from 'react';
import { Box, Typography, Divider, Chip, Grid } from '@mui/material';
import { PlayArrow, Edit, Stop, Refresh, Delete } from '@mui/icons-material';

import Card from '../common/Card';
import Button from '../common/Button';
import { PipelineDefinition } from '../../types/api';
import { PipelineStatus } from '../../types/dashboard';
import { formatDateTimeShort } from '../../utils/date';
import pipelineService from '../../services/api/pipelineService';

/**
 * Props for the PipelineDetailsCard component
 */
interface PipelineDetailsCardProps {
  /**
   * Pipeline definition data to display
   */
  pipeline: PipelineDefinition;
  
  /**
   * Whether the pipeline data is currently loading
   */
  loading?: boolean;
  
  /**
   * Callback function when the Edit button is clicked
   */
  onEdit?: () => void;
  
  /**
   * Callback function when the Delete button is clicked
   */
  onDelete?: () => void;
  
  /**
   * Callback function when the View History button is clicked
   */
  onViewHistory?: () => void;
  
  /**
   * Additional CSS class name
   */
  className?: string;
}

/**
 * Component that displays detailed information about a pipeline definition
 * Presents pipeline metadata, status, and action buttons for pipeline operations
 */
const PipelineDetailsCard: React.FC<PipelineDetailsCardProps> = ({
  pipeline,
  loading = false,
  onEdit,
  onDelete,
  onViewHistory,
  className
}) => {
  // State to track if the pipeline is currently being run
  const [isRunning, setIsRunning] = useState(false);
  
  // Callback to handle running the pipeline
  const handleRunPipeline = useCallback(async () => {
    if (isRunning || !pipeline.pipelineId) return;
    
    try {
      setIsRunning(true);
      await pipelineService.runPipeline(pipeline.pipelineId);
      // Success handling could be added here
    } catch (error) {
      console.error('Failed to run pipeline:', error);
      // Error handling could be added here
    } finally {
      setIsRunning(false);
    }
  }, [pipeline.pipelineId, isRunning]);

  // Custom styles
  const styles = {
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
    description: {
      marginTop: '16px',
      marginBottom: '16px'
    },
    actionsContainer: {
      display: 'flex',
      justifyContent: 'flex-end',
      gap: '8px',
      marginTop: '16px'
    },
    statusChip: {
      HEALTHY: {
        backgroundColor: '#4caf50',
        color: 'white'
      },
      WARNING: {
        backgroundColor: '#ff9800',
        color: 'white'
      },
      ERROR: {
        backgroundColor: '#f44336',
        color: 'white'
      },
      INACTIVE: {
        backgroundColor: '#9e9e9e',
        color: 'white'
      }
    }
  };

  return (
    <Card 
      title={pipeline.pipelineName}
      loading={loading}
      className={className}
    >
      <Box>
        {/* Status and ID display */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="body2" color="text.secondary">
            ID: {pipeline.pipelineId}
          </Typography>
          {pipeline.lastExecutionStatus && (
            <Chip 
              label={pipeline.lastExecutionStatus} 
              size="small"
              sx={styles.statusChip[pipeline.lastExecutionStatus] || {}}
            />
          )}
        </Box>
        
        <Divider sx={{ mb: 2 }} />
        
        {/* Pipeline metadata */}
        <Box>
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Source:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>{pipeline.sourceName}</Typography>
          </Box>
          
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Target Dataset:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>{pipeline.targetDataset}</Typography>
          </Box>
          
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Target Table:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>{pipeline.targetTable}</Typography>
          </Box>
          
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Created:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>
              {formatDateTimeShort(pipeline.createdAt)}
            </Typography>
          </Box>
          
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Last Updated:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>
              {formatDateTimeShort(pipeline.updatedAt)}
            </Typography>
          </Box>
          
          {pipeline.lastExecutionTime && (
            <Box sx={styles.metadataItem}>
              <Typography variant="body2" sx={styles.metadataLabel}>Last Execution:</Typography>
              <Typography variant="body2" sx={styles.metadataValue}>
                {formatDateTimeShort(pipeline.lastExecutionTime)}
              </Typography>
            </Box>
          )}
          
          <Box sx={styles.metadataItem}>
            <Typography variant="body2" sx={styles.metadataLabel}>Status:</Typography>
            <Typography variant="body2" sx={styles.metadataValue}>
              {pipeline.isActive ? 'Active' : 'Inactive'}
            </Typography>
          </Box>
        </Box>
        
        {/* Description if available */}
        {pipeline.description && (
          <Box sx={styles.description}>
            <Typography variant="body2" sx={styles.metadataLabel}>Description:</Typography>
            <Typography variant="body2">{pipeline.description}</Typography>
          </Box>
        )}
        
        <Divider sx={{ mt: 2, mb: 2 }} />
        
        {/* Action buttons */}
        <Box sx={styles.actionsContainer}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<PlayArrow />}
            onClick={handleRunPipeline}
            loading={isRunning}
            disabled={!pipeline.isActive}
            title={!pipeline.isActive ? "Pipeline is inactive" : "Run pipeline now"}
          >
            Run Now
          </Button>
          
          {onEdit && (
            <Button
              variant="outlined"
              color="primary"
              startIcon={<Edit />}
              onClick={onEdit}
              title="Edit pipeline"
            >
              Edit
            </Button>
          )}
          
          {onViewHistory && (
            <Button
              variant="outlined"
              color="primary"
              startIcon={<Refresh />}
              onClick={onViewHistory}
              title="View execution history"
            >
              View History
            </Button>
          )}
          
          {onDelete && (
            <Button
              variant="outlined"
              color="error"
              startIcon={<Delete />}
              onClick={onDelete}
              title="Delete pipeline"
            >
              Delete
            </Button>
          )}
        </Box>
      </Box>
    </Card>
  );
};

export default PipelineDetailsCard;