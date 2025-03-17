import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Divider,
  Chip,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField
} from '@mui/material';
import { PlayArrow, Info, CheckCircle, Error } from '@mui/icons-material';
import Card from '../common/Card';
import Button from '../common/Button';
import { SuggestedAction } from '../../types/alerts';
import { alertService } from '../../services/api/alertService';

/**
 * Props interface for the SuggestedActionsCard component
 */
interface SuggestedActionsCardProps {
  /** ID of the alert to fetch suggested actions for */
  alertId: string;
  /** Callback function when an action is executed */
  onActionExecuted: (actionType: string, success: boolean) => void;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Interface for action execution dialog state
 */
interface ActionExecutionDialogState {
  /** Whether the dialog is open */
  open: boolean;
  /** The action being executed */
  action: SuggestedAction | null;
  /** Parameters for the action execution */
  parameters: Record<string, any>;
}

/**
 * Component that displays AI-suggested actions for resolving an alert in the data pipeline.
 * It fetches suggested actions for a specific alert, displays them with confidence levels,
 * and provides the ability to execute these actions.
 */
const SuggestedActionsCard: React.FC<SuggestedActionsCardProps> = ({
  alertId,
  onActionExecuted,
  className
}) => {
  // State for managing suggested actions and component state
  const [actions, setActions] = useState<SuggestedAction[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [executingAction, setExecutingAction] = useState<string | null>(null);
  const [dialogState, setDialogState] = useState<ActionExecutionDialogState>({
    open: false,
    action: null,
    parameters: {}
  });

  /**
   * Fetches suggested actions for the current alert
   */
  const fetchSuggestedActions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await alertService.getSuggestedActions(alertId);
      setActions(response.actions);
    } catch (err) {
      setError('Failed to load suggested actions');
      console.error('Error fetching suggested actions:', err);
    } finally {
      setLoading(false);
    }
  }, [alertId]);

  // Fetch actions when component mounts or alertId changes
  useEffect(() => {
    fetchSuggestedActions();
  }, [fetchSuggestedActions]);

  /**
   * Handles the execution of a suggested action
   * @param action The action to execute
   */
  const handleExecuteAction = useCallback((action: SuggestedAction) => {
    // Check if action requires parameters
    if (action.parameters && Object.keys(action.parameters).length > 0) {
      // Open dialog for parameter input
      setDialogState({
        open: true,
        action,
        parameters: {} // Initialize with empty parameters
      });
    } else {
      // Execute action directly if no parameters are required
      executeAction(action, {});
    }
  }, []);

  /**
   * Executes the selected action with provided parameters
   * @param action The action to execute
   * @param parameters The parameters for the action
   */
  const executeAction = useCallback(async (action: SuggestedAction, parameters: Record<string, any>) => {
    setExecutingAction(action.actionId);
    try {
      await alertService.executeAction(action.actionId, alertId, parameters);
      onActionExecuted(action.actionType, true);
    } catch (err) {
      console.error('Error executing action:', err);
      setError(`Failed to execute action: ${action.description}`);
      onActionExecuted(action.actionType, false);
    } finally {
      setExecutingAction(null);
      // Close dialog if it was open
      if (dialogState.open) {
        setDialogState({
          open: false,
          action: null,
          parameters: {}
        });
      }
    }
  }, [alertId, dialogState.open, onActionExecuted]);

  /**
   * Closes the action execution dialog
   */
  const handleDialogClose = useCallback(() => {
    setDialogState({
      open: false,
      action: null,
      parameters: {}
    });
  }, []);

  /**
   * Confirms the action execution from the dialog
   */
  const handleDialogConfirm = useCallback(() => {
    if (dialogState.action) {
      executeAction(dialogState.action, dialogState.parameters);
    }
  }, [dialogState.action, dialogState.parameters, executeAction]);

  /**
   * Updates parameter values when inputs change
   * @param event Change event from the input field
   */
  const handleParameterChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = event.target;
    setDialogState(prevState => ({
      ...prevState,
      parameters: {
        ...prevState.parameters,
        [name]: value
      }
    }));
  }, []);

  /**
   * Renders a chip displaying the confidence level
   * @param confidence Confidence score (0-1)
   * @returns JSX element with appropriate styling
   */
  const renderConfidenceChip = (confidence: number) => {
    let color: 'success' | 'warning' | 'error' = 'success';
    let label = 'High';
    
    if (confidence < 0.5) {
      color = 'error';
      label = 'Low';
    } else if (confidence < 0.8) {
      color = 'warning';
      label = 'Medium';
    }
    
    return (
      <Chip 
        size="small"
        color={color}
        label={`${label} Confidence (${Math.round(confidence * 100)}%)`}
        sx={{ ml: 1 }}
      />
    );
  };

  return (
    <Card
      title="Suggested Actions"
      loading={loading}
      error={error}
      className={className}
    >
      {actions.length === 0 ? (
        <Typography variant="body2" color="textSecondary">
          No suggested actions available
        </Typography>
      ) : (
        <List>
          {actions.map((action) => (
            <ListItem key={action.actionId} divider>
              <ListItemText
                primary={
                  <Box display="flex" alignItems="center">
                    <Typography variant="subtitle1">
                      {action.description}
                    </Typography>
                    {renderConfidenceChip(action.confidence)}
                  </Box>
                }
                secondary={
                  action.estimatedImpact && (
                    <Box display="flex" alignItems="center" mt={0.5}>
                      <Info fontSize="small" color="action" />
                      <Typography variant="caption" sx={{ ml: 0.5 }}>
                        {action.estimatedImpact}
                      </Typography>
                    </Box>
                  )
                }
              />
              <ListItemSecondaryAction>
                <Button
                  variant="contained"
                  color="primary"
                  startIcon={<PlayArrow />}
                  onClick={() => handleExecuteAction(action)}
                  loading={executingAction === action.actionId}
                  disabled={executingAction !== null}
                >
                  Execute
                </Button>
              </ListItemSecondaryAction>
            </ListItem>
          ))}
        </List>
      )}

      {/* Dialog for action parameters */}
      <Dialog
        open={dialogState.open}
        onClose={handleDialogClose}
        aria-labelledby="action-execution-dialog-title"
      >
        <DialogTitle id="action-execution-dialog-title">
          Execute {dialogState.action?.actionType} Action
        </DialogTitle>
        <DialogContent>
          <Typography gutterBottom>
            {dialogState.action?.description}
          </Typography>
          <Box my={2}>
            {dialogState.action?.parameters && Object.entries(dialogState.action.parameters).map(([key, config]) => (
              <TextField
                key={key}
                fullWidth
                margin="dense"
                label={config.label || key}
                name={key}
                onChange={handleParameterChange}
                value={dialogState.parameters[key] || ''}
                helperText={config.description}
                required={config.required}
              />
            ))}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleDialogClose} color="inherit">
            Cancel
          </Button>
          <Button onClick={handleDialogConfirm} color="primary">
            Execute
          </Button>
        </DialogActions>
      </Dialog>
    </Card>
  );
};

export default SuggestedActionsCard;