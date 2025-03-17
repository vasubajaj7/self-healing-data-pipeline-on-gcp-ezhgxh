import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  Radio,
  FormControlLabel,
  Divider
} from '@mui/material'; // ^5.11.0
import {
  PlayArrow,
  Stop,
  Refresh,
  Schedule,
  Edit
} from '@mui/icons-material'; // ^5.11.0

import Button from '../common/Button';
import Card from '../common/Card';
import Modal from '../common/Modal';
import Alert from '../common/Alert';
import pipelineService from '../../services/api/pipelineService';
import { useApi } from '../../hooks/useApi';
import { PipelineDefinition, PipelineStatus } from '../../types/api';

/**
 * Props for the PipelineControlPanel component
 */
interface PipelineControlPanelProps {
  /** Pipeline definition data to control */
  pipeline: PipelineDefinition;
  /** Callback function when pipeline data should be refreshed */
  onRefresh?: () => void;
  /** Callback function when pipeline schedule is updated */
  onScheduleUpdate?: () => void;
  /** Additional CSS class name */
  className?: string;
}

/**
 * Component that provides a control panel for pipeline operations
 */
const PipelineControlPanel: React.FC<PipelineControlPanelProps> = ({
  pipeline,
  onRefresh,
  onScheduleUpdate,
  className
}) => {
  // Modal visibility states
  const [isRunModalOpen, setIsRunModalOpen] = useState(false);
  const [isStopModalOpen, setIsStopModalOpen] = useState(false);
  const [isRetryModalOpen, setIsRetryModalOpen] = useState(false);
  const [isScheduleModalOpen, setIsScheduleModalOpen] = useState(false);
  
  // Parameter states
  const [runParams, setRunParams] = useState<Record<string, any>>({});
  const [retryParams, setRetryParams] = useState<Record<string, any>>({});
  const [scheduleData, setScheduleData] = useState<{ schedule: string, nextRun: string }>({ 
    schedule: '', 
    nextRun: '' 
  });
  const [newSchedule, setNewSchedule] = useState('');
  
  // Status message state
  const [statusMessage, setStatusMessage] = useState<{ 
    type: 'success' | 'error' | 'info', 
    message: string 
  } | null>(null);

  // API hooks for pipeline operations
  const runPipelineApi = useApi();
  const stopPipelineApi = useApi();
  const retryPipelineApi = useApi();
  const getScheduleApi = useApi();
  const updateScheduleApi = useApi();

  // Clear status message after a timeout
  useEffect(() => {
    if (statusMessage) {
      const timerId = setTimeout(() => {
        setStatusMessage(null);
      }, 5000);
      
      return () => clearTimeout(timerId);
    }
  }, [statusMessage]);

  // Handle running the pipeline
  const handleRunPipeline = () => {
    setIsRunModalOpen(true);
    setRunParams({});
  };

  // Confirm running the pipeline
  const handleRunConfirm = async () => {
    try {
      await runPipelineApi.post(
        pipelineService.runPipeline(pipeline.pipelineId, runParams)
      );
      
      setIsRunModalOpen(false);
      setStatusMessage({ 
        type: 'success', 
        message: 'Pipeline started successfully' 
      });
      
      if (onRefresh) {
        onRefresh();
      }
    } catch (error) {
      setStatusMessage({ 
        type: 'error', 
        message: `Failed to start pipeline: ${error instanceof Error ? error.message : String(error)}` 
      });
    }
  };

  // Handle stopping the pipeline
  const handleStopPipeline = () => {
    setIsStopModalOpen(true);
  };

  // Confirm stopping the pipeline
  const handleStopConfirm = async () => {
    try {
      // Assuming there's an active execution ID that we need to stop
      // In a real-world scenario, this would come from the pipeline's current execution
      const executionId = pipeline.lastExecutionStatus === PipelineStatus.HEALTHY ? 
        (pipeline as any).activeExecutionId : '';
        
      if (!executionId) {
        throw new Error('No active execution found to stop');
      }
      
      await stopPipelineApi.post(
        pipelineService.stopPipeline(executionId)
      );
      
      setIsStopModalOpen(false);
      setStatusMessage({ 
        type: 'success', 
        message: 'Pipeline stopped successfully' 
      });
      
      if (onRefresh) {
        onRefresh();
      }
    } catch (error) {
      setStatusMessage({ 
        type: 'error', 
        message: `Failed to stop pipeline: ${error instanceof Error ? error.message : String(error)}` 
      });
    }
  };

  // Handle retrying the pipeline
  const handleRetryPipeline = () => {
    setIsRetryModalOpen(true);
    setRetryParams({});
  };

  // Confirm retrying the pipeline
  const handleRetryConfirm = async () => {
    try {
      // Assuming there's a failed execution ID that we need to retry
      // In a real-world scenario, this would come from the pipeline's last failed execution
      const executionId = pipeline.lastExecutionStatus === PipelineStatus.ERROR ? 
        (pipeline as any).lastExecutionId : '';
        
      if (!executionId) {
        throw new Error('No failed execution found to retry');
      }
      
      await retryPipelineApi.post(
        pipelineService.retryPipeline(executionId, retryParams)
      );
      
      setIsRetryModalOpen(false);
      setStatusMessage({ 
        type: 'success', 
        message: 'Pipeline retry initiated successfully' 
      });
      
      if (onRefresh) {
        onRefresh();
      }
    } catch (error) {
      setStatusMessage({ 
        type: 'error', 
        message: `Failed to retry pipeline: ${error instanceof Error ? error.message : String(error)}` 
      });
    }
  };

  // Handle scheduling the pipeline
  const handleSchedulePipeline = () => {
    fetchScheduleInfo();
    setIsScheduleModalOpen(true);
  };

  // Fetch current schedule information
  const fetchScheduleInfo = async () => {
    try {
      const response = await getScheduleApi.get(
        pipelineService.getPipelineSchedule(pipeline.pipelineId)
      );
      
      setScheduleData(response.data);
      setNewSchedule(response.data.schedule);
    } catch (error) {
      setStatusMessage({ 
        type: 'error', 
        message: `Failed to fetch schedule: ${error instanceof Error ? error.message : String(error)}` 
      });
    }
  };

  // Confirm scheduling the pipeline
  const handleScheduleConfirm = async () => {
    try {
      await updateScheduleApi.put(
        pipelineService.updatePipelineSchedule(pipeline.pipelineId, { schedule: newSchedule })
      );
      
      setIsScheduleModalOpen(false);
      setStatusMessage({ 
        type: 'success', 
        message: 'Pipeline schedule updated successfully' 
      });
      
      if (onScheduleUpdate) {
        onScheduleUpdate();
      }
    } catch (error) {
      setStatusMessage({ 
        type: 'error', 
        message: `Failed to update schedule: ${error instanceof Error ? error.message : String(error)}` 
      });
    }
  };

  // Handle parameter changes for run and retry modals
  const handleParamChange = (paramType: 'run' | 'retry', paramName: string, paramValue: any) => {
    if (paramType === 'run') {
      setRunParams(prev => ({
        ...prev,
        [paramName]: paramValue
      }));
    } else {
      setRetryParams(prev => ({
        ...prev,
        [paramName]: paramValue
      }));
    }
  };

  // Handle schedule input changes
  const handleScheduleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setNewSchedule(event.target.value);
  };

  // Clear status message
  const clearStatusMessage = useCallback(() => {
    setStatusMessage(null);
  }, []);

  return (
    <Card 
      title="Pipeline Controls"
      className={className}
      sx={{ marginBottom: '16px' }}
    >
      <Box>
        <Typography variant="body2">
          Use these controls to manage pipeline execution and scheduling.
        </Typography>
        
        <Box sx={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginTop: '16px' }}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<PlayArrow />}
            onClick={handleRunPipeline}
            loading={runPipelineApi.loading}
            disabled={runPipelineApi.loading}
          >
            Run Now
          </Button>
          
          <Button
            variant="contained"
            color="error"
            startIcon={<Stop />}
            onClick={handleStopPipeline}
            loading={stopPipelineApi.loading}
            disabled={
              stopPipelineApi.loading || 
              pipeline.lastExecutionStatus !== PipelineStatus.HEALTHY
            }
          >
            Stop
          </Button>
          
          <Button
            variant="contained"
            color="warning"
            startIcon={<Refresh />}
            onClick={handleRetryPipeline}
            loading={retryPipelineApi.loading}
            disabled={
              retryPipelineApi.loading || 
              pipeline.lastExecutionStatus !== PipelineStatus.ERROR
            }
          >
            Retry
          </Button>
          
          <Button
            variant="contained"
            color="info"
            startIcon={<Schedule />}
            onClick={handleSchedulePipeline}
            loading={getScheduleApi.loading || updateScheduleApi.loading}
            disabled={getScheduleApi.loading || updateScheduleApi.loading}
          >
            Schedule
          </Button>
        </Box>
        
        {statusMessage && (
          <Box sx={{ marginTop: '16px' }}>
            <Alert 
              severity={statusMessage.type} 
              onClose={clearStatusMessage}
            >
              {statusMessage.message}
            </Alert>
          </Box>
        )}
      </Box>
      
      {/* Run Pipeline Modal */}
      <Modal
        title="Run Pipeline"
        open={isRunModalOpen}
        onClose={() => setIsRunModalOpen(false)}
        actions={
          <>
            <Button variant="text" onClick={() => setIsRunModalOpen(false)}>
              Cancel
            </Button>
            <Button 
              variant="contained" 
              onClick={handleRunConfirm}
              loading={runPipelineApi.loading}
              disabled={runPipelineApi.loading}
            >
              Run
            </Button>
          </>
        }
      >
        <Box sx={{ minWidth: '400px' }}>
          <Typography variant="body1" gutterBottom>
            Run the pipeline "{pipeline.pipelineName}" now?
          </Typography>
          
          <Typography variant="body2" color="textSecondary" gutterBottom>
            Enter optional parameter values below to customize this execution.
          </Typography>
          
          <FormControl fullWidth sx={{ marginTop: '16px' }}>
            <TextField
              label="Back-fill date (optional)"
              name="backfillDate"
              type="date"
              InputLabelProps={{ shrink: true }}
              onChange={(e) => handleParamChange('run', 'backfillDate', e.target.value)}
              value={runParams.backfillDate || ''}
              fullWidth
              margin="normal"
            />
            
            <FormControl component="fieldset" sx={{ marginTop: '16px' }}>
              <FormLabel component="legend">Processing Mode</FormLabel>
              <RadioGroup 
                defaultValue="incremental"
                onChange={(e) => handleParamChange('run', 'processingMode', e.target.value)}
              >
                <FormControlLabel 
                  value="incremental" 
                  control={<Radio />} 
                  label="Incremental (process new data only)" 
                />
                <FormControlLabel 
                  value="full" 
                  control={<Radio />} 
                  label="Full Refresh (process all data)" 
                />
              </RadioGroup>
            </FormControl>
          </FormControl>
        </Box>
      </Modal>
      
      {/* Stop Pipeline Modal */}
      <Modal
        title="Stop Pipeline"
        open={isStopModalOpen}
        onClose={() => setIsStopModalOpen(false)}
        actions={
          <>
            <Button variant="text" onClick={() => setIsStopModalOpen(false)}>
              Cancel
            </Button>
            <Button 
              variant="contained" 
              color="error"
              onClick={handleStopConfirm}
              loading={stopPipelineApi.loading}
              disabled={stopPipelineApi.loading}
            >
              Stop Pipeline
            </Button>
          </>
        }
      >
        <Box sx={{ minWidth: '400px' }}>
          <Typography variant="body1" gutterBottom>
            Are you sure you want to stop the currently running pipeline?
          </Typography>
          
          <Typography variant="body2" color="textSecondary">
            Stopping a pipeline may result in incomplete data processing. The self-healing system will attempt to recover gracefully, but manual intervention may be required.
          </Typography>
        </Box>
      </Modal>
      
      {/* Retry Pipeline Modal */}
      <Modal
        title="Retry Pipeline"
        open={isRetryModalOpen}
        onClose={() => setIsRetryModalOpen(false)}
        actions={
          <>
            <Button variant="text" onClick={() => setIsRetryModalOpen(false)}>
              Cancel
            </Button>
            <Button 
              variant="contained" 
              color="warning"
              onClick={handleRetryConfirm}
              loading={retryPipelineApi.loading}
              disabled={retryPipelineApi.loading}
            >
              Retry
            </Button>
          </>
        }
      >
        <Box sx={{ minWidth: '400px' }}>
          <Typography variant="body1" gutterBottom>
            Retry the failed pipeline execution?
          </Typography>
          
          <Typography variant="body2" color="textSecondary" gutterBottom>
            The self-healing system has analyzed the failure and suggests the following retry parameters:
          </Typography>
          
          <Divider sx={{ my: 2 }} />
          
          <FormControl fullWidth sx={{ marginTop: '16px' }}>
            <TextField
              label="Memory Allocation (MB)"
              name="memoryMb"
              type="number"
              InputLabelProps={{ shrink: true }}
              onChange={(e) => handleParamChange('retry', 'memoryMb', parseInt(e.target.value, 10))}
              value={retryParams.memoryMb || 2048}
              fullWidth
              margin="normal"
            />
            
            <FormControl component="fieldset" sx={{ marginTop: '16px' }}>
              <FormLabel component="legend">Retry Strategy</FormLabel>
              <RadioGroup 
                defaultValue="failedTasksOnly"
                onChange={(e) => handleParamChange('retry', 'retryStrategy', e.target.value)}
              >
                <FormControlLabel 
                  value="failedTasksOnly" 
                  control={<Radio />} 
                  label="Retry failed tasks only" 
                />
                <FormControlLabel 
                  value="fromFailedTask" 
                  control={<Radio />} 
                  label="Retry from failed task" 
                />
                <FormControlLabel 
                  value="fullPipeline" 
                  control={<Radio />} 
                  label="Retry entire pipeline" 
                />
              </RadioGroup>
            </FormControl>
          </FormControl>
        </Box>
      </Modal>
      
      {/* Schedule Pipeline Modal */}
      <Modal
        title="Schedule Pipeline"
        open={isScheduleModalOpen}
        onClose={() => setIsScheduleModalOpen(false)}
        actions={
          <>
            <Button variant="text" onClick={() => setIsScheduleModalOpen(false)}>
              Cancel
            </Button>
            <Button 
              variant="contained" 
              color="primary"
              onClick={handleScheduleConfirm}
              loading={updateScheduleApi.loading}
              disabled={updateScheduleApi.loading || !newSchedule}
            >
              Save Schedule
            </Button>
          </>
        }
      >
        <Box sx={{ minWidth: '400px' }}>
          <Typography variant="body1" gutterBottom>
            Configure the recurring schedule for this pipeline
          </Typography>
          
          {scheduleData.schedule && (
            <Box sx={{ marginTop: '8px', marginBottom: '16px' }}>
              <Typography variant="body2" color="textSecondary">
                Current Schedule: <strong>{scheduleData.schedule}</strong>
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Next Execution: {scheduleData.nextRun || 'Not scheduled'}
              </Typography>
            </Box>
          )}
          
          <FormControl fullWidth sx={{ marginTop: '16px' }}>
            <TextField
              label="Cron Expression"
              name="schedule"
              value={newSchedule}
              onChange={handleScheduleChange}
              fullWidth
              margin="normal"
              placeholder="e.g. 0 0 * * * (daily at midnight)"
              helperText="Enter a cron expression to define the schedule. Format: minute hour day-of-month month day-of-week"
            />
            
            <Box sx={{ marginTop: '16px' }}>
              <Typography variant="body2" color="textSecondary">
                Common examples:
              </Typography>
              <Typography variant="caption" display="block">
                • Daily at midnight: <strong>0 0 * * *</strong>
              </Typography>
              <Typography variant="caption" display="block">
                • Every hour: <strong>0 * * * *</strong>
              </Typography>
              <Typography variant="caption" display="block">
                • Every Monday at 9 AM: <strong>0 9 * * 1</strong>
              </Typography>
              <Typography variant="caption" display="block">
                • Monthly on the 1st at midnight: <strong>0 0 1 * *</strong>
              </Typography>
            </Box>
          </FormControl>
        </Box>
      </Modal>
    </Card>
  );
};

export default PipelineControlPanel;