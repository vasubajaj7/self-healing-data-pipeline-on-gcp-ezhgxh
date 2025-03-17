import React, { useState } from 'react';
import {
  Box,
  Typography,
  Divider,
  Chip,
  Grid,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import {
  CheckCircle,
  Error,
  Warning,
  Info,
  AccessTime,
  Person,
  Notes,
  ArrowUpward,
  Block,
} from '@mui/icons-material';
import Card from '../common/Card';
import Button from '../common/Button';
import {
  Alert,
  AlertStatus,
  AlertSeverity,
  AlertType,
  AlertAcknowledgement,
  AlertEscalation,
  AlertResolution,
  AlertSuppression,
} from '../../types/alerts';
import { formatDate } from '../../utils/date';
import { alertService } from '../../services/api/alertService';

/**
 * Props interface for the AlertDetailsCard component
 */
interface AlertDetailsCardProps {
  /** The alert object to display details for */
  alert: Alert;
  /** Callback function when an alert is acknowledged */
  onAcknowledge: (actionType: string, success: boolean) => void;
  /** Callback function when an alert is resolved */
  onResolve: (actionType: string, success: boolean) => void;
  /** Callback function when an alert is escalated */
  onEscalate: (actionType: string, success: boolean) => void;
  /** Callback function when similar alerts are suppressed */
  onSuppress: (actionType: string, success: boolean) => void;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Interface for dialog state management
 */
interface DialogState {
  open: boolean;
  type: string;
}

/**
 * Component that displays detailed information about an alert and provides action buttons
 * for acknowledging, resolving, escalating, and suppressing alerts.
 */
const AlertDetailsCard: React.FC<AlertDetailsCardProps> = ({
  alert,
  onAcknowledge,
  onResolve,
  onEscalate,
  onSuppress,
  className,
}) => {
  // State for loading, dialogs, form values, and errors
  const [loading, setLoading] = useState(false);
  const [dialogState, setDialogState] = useState<DialogState>({ open: false, type: '' });
  const [formValues, setFormValues] = useState<Record<string, any>>({});
  const [error, setError] = useState<string | null>(null);

  // Function to handle opening the dialog
  const handleDialogOpen = (type: string) => {
    setDialogState({ open: true, type });
    setFormValues({});
    setError(null);
  };

  // Function to handle closing the dialog
  const handleDialogClose = () => {
    setDialogState({ open: false, type: '' });
  };

  // Function to handle form input changes
  const handleFormChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setFormValues({
      ...formValues,
      [event.target.name]: event.target.value,
    });
  };

  // Handle acknowledging an alert
  const handleAcknowledge = async () => {
    setLoading(true);
    try {
      await alertService.acknowledgeAlert(
        alert.alertId,
        'Current User', // In a real app, this would come from auth context
        formValues.comments
      );
      onAcknowledge('acknowledge', true);
      handleDialogClose();
    } catch (err) {
      setError((err as Error).message || 'Failed to acknowledge alert');
      onAcknowledge('acknowledge', false);
    } finally {
      setLoading(false);
    }
  };

  // Handle resolving an alert
  const handleResolve = async () => {
    setLoading(true);
    try {
      await alertService.resolveAlert(
        alert.alertId,
        'Current User', // In a real app, this would come from auth context
        formValues.resolutionNotes
      );
      onResolve('resolve', true);
      handleDialogClose();
    } catch (err) {
      setError((err as Error).message || 'Failed to resolve alert');
      onResolve('resolve', false);
    } finally {
      setLoading(false);
    }
  };

  // Handle escalating an alert
  const handleEscalate = async () => {
    setLoading(true);
    try {
      await alertService.escalateAlert(
        alert.alertId,
        'Current User', // In a real app, this would come from auth context
        formValues.escalationReason,
        formValues.escalationLevel
      );
      onEscalate('escalate', true);
      handleDialogClose();
    } catch (err) {
      setError((err as Error).message || 'Failed to escalate alert');
      onEscalate('escalate', false);
    } finally {
      setLoading(false);
    }
  };

  // Handle suppressing similar alerts
  const handleSuppress = async () => {
    setLoading(true);
    try {
      await alertService.suppressSimilarAlerts(
        alert.alertId,
        'Current User', // In a real app, this would come from auth context
        Number(formValues.durationMinutes) || 60,
        formValues.suppressionReason
      );
      onSuppress('suppress', true);
      handleDialogClose();
    } catch (err) {
      setError((err as Error).message || 'Failed to suppress similar alerts');
      onSuppress('suppress', false);
    } finally {
      setLoading(false);
    }
  };

  // Render status chip with appropriate color and icon
  const renderStatusChip = () => {
    let icon;
    let color:
      | 'error'
      | 'warning'
      | 'success'
      | 'secondary'
      | 'default'
      | 'primary'
      | 'info';

    // Determine icon and color based on status
    switch (alert.status) {
      case AlertStatus.ACTIVE:
        icon = <Error />;
        color = 'error';
        break;
      case AlertStatus.ACKNOWLEDGED:
        icon = <AccessTime />;
        color = 'warning';
        break;
      case AlertStatus.RESOLVED:
        icon = <CheckCircle />;
        color = 'success';
        break;
      case AlertStatus.ESCALATED:
        icon = <ArrowUpward />;
        color = 'secondary';
        break;
      case AlertStatus.SUPPRESSED:
        icon = <Block />;
        color = 'default';
        break;
      default:
        icon = <Info />;
        color = 'info';
    }

    return (
      <Chip
        icon={icon}
        label={alert.status}
        color={color}
        size="small"
        sx={{
          fontWeight: '500',
          textTransform: 'uppercase',
          fontSize: '0.75rem',
          borderRadius: '16px',
        }}
      />
    );
  };

  // Render severity chip with appropriate color and icon
  const renderSeverityChip = () => {
    let icon;
    let color:
      | 'error'
      | 'warning'
      | 'info'
      | 'success'
      | 'default'
      | 'primary'
      | 'secondary';

    // Determine icon and color based on severity
    switch (alert.severity) {
      case AlertSeverity.CRITICAL:
      case AlertSeverity.HIGH:
        icon = <Error />;
        color = 'error';
        break;
      case AlertSeverity.MEDIUM:
        icon = <Warning />;
        color = 'warning';
        break;
      case AlertSeverity.LOW:
        icon = <Info />;
        color = 'info';
        break;
      default:
        icon = <Info />;
        color = 'info';
    }

    return (
      <Chip
        icon={icon}
        label={alert.severity}
        color={color}
        size="small"
        sx={{
          fontWeight: '500',
          textTransform: 'uppercase',
          fontSize: '0.75rem',
          borderRadius: '16px',
        }}
      />
    );
  };

  // Render appropriate action buttons based on alert status
  const renderActionButtons = () => {
    return (
      <Grid container spacing={1} mt={2}>
        {alert.status === AlertStatus.ACTIVE && (
          <Grid item>
            <Button
              variant="contained"
              color="primary"
              onClick={() => handleDialogOpen('acknowledge')}
              disabled={loading}
            >
              Acknowledge
            </Button>
          </Grid>
        )}
        
        {alert.status !== AlertStatus.RESOLVED && (
          <Grid item>
            <Button
              variant="contained"
              color="success"
              onClick={() => handleDialogOpen('resolve')}
              disabled={loading}
            >
              Resolve
            </Button>
          </Grid>
        )}
        
        {alert.status !== AlertStatus.ESCALATED && (
          <Grid item>
            <Button
              variant="contained"
              color="secondary"
              onClick={() => handleDialogOpen('escalate')}
              disabled={loading}
            >
              Escalate
            </Button>
          </Grid>
        )}
        
        {alert.status !== AlertStatus.SUPPRESSED && (
          <Grid item>
            <Button
              variant="contained"
              color="warning"
              onClick={() => handleDialogOpen('suppress')}
              disabled={loading}
            >
              Suppress Similar
            </Button>
          </Grid>
        )}
      </Grid>
    );
  };

  // Render confirmation dialog based on dialog type
  const renderConfirmationDialog = () => {
    // Common props for TextField components
    const textFieldProps = {
      fullWidth: true,
      margin: 'normal' as 'normal',
      onChange: handleFormChange,
    };

    // Define dialog config based on type
    let dialogTitle = '';
    let dialogContent = null;
    let confirmHandler = () => {};
    let confirmText = '';
    let confirmColor:
      | 'primary'
      | 'inherit'
      | 'secondary'
      | 'success'
      | 'error'
      | 'info'
      | 'warning' = 'primary';

    switch (dialogState.type) {
      case 'acknowledge':
        dialogTitle = 'Acknowledge Alert';
        dialogContent = (
          <TextField
            {...textFieldProps}
            name="comments"
            label="Comments (Optional)"
            multiline
            rows={3}
          />
        );
        confirmHandler = handleAcknowledge;
        confirmText = 'Acknowledge';
        confirmColor = 'primary';
        break;

      case 'resolve':
        dialogTitle = 'Resolve Alert';
        dialogContent = (
          <TextField
            {...textFieldProps}
            name="resolutionNotes"
            label="Resolution Notes"
            multiline
            rows={3}
            required
          />
        );
        confirmHandler = handleResolve;
        confirmText = 'Resolve';
        confirmColor = 'success';
        break;

      case 'escalate':
        dialogTitle = 'Escalate Alert';
        dialogContent = (
          <>
            <TextField
              {...textFieldProps}
              name="escalationReason"
              label="Escalation Reason"
              multiline
              rows={3}
              required
            />
            <TextField
              {...textFieldProps}
              name="escalationLevel"
              label="Escalation Level"
              select
              SelectProps={{
                native: true,
              }}
              required
            >
              <option value="Team Lead">Team Lead</option>
              <option value="Manager">Manager</option>
              <option value="Director">Director</option>
              <option value="Emergency Response">Emergency Response</option>
            </TextField>
          </>
        );
        confirmHandler = handleEscalate;
        confirmText = 'Escalate';
        confirmColor = 'secondary';
        break;

      case 'suppress':
        dialogTitle = 'Suppress Similar Alerts';
        dialogContent = (
          <>
            <TextField
              {...textFieldProps}
              name="suppressionReason"
              label="Suppression Reason"
              multiline
              rows={3}
              required
            />
            <TextField
              {...textFieldProps}
              name="durationMinutes"
              label="Duration (minutes)"
              type="number"
              defaultValue={60}
              required
            />
          </>
        );
        confirmHandler = handleSuppress;
        confirmText = 'Suppress';
        confirmColor = 'warning';
        break;
    }

    return (
      <Dialog
        open={dialogState.open}
        onClose={handleDialogClose}
        aria-labelledby="alert-dialog-title"
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle id="alert-dialog-title">{dialogTitle}</DialogTitle>
        <DialogContent>
          {dialogContent}
          {error && (
            <Typography color="error" variant="body2" sx={{ mt: 2 }}>
              {error}
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={handleDialogClose} disabled={loading}>
            Cancel
          </Button>
          <Button
            onClick={confirmHandler}
            color={confirmColor}
            variant="contained"
            loading={loading}
            autoFocus
          >
            {confirmText}
          </Button>
        </DialogActions>
      </Dialog>
    );
  };

  // Section styling
  const sectionStyle = {
    marginBottom: '16px',
    padding: '16px',
    borderRadius: '4px',
    backgroundColor: 'rgba(0, 0, 0, 0.02)',
  };

  // Field label styling
  const labelStyle = {
    fontWeight: '500',
    color: 'rgba(0, 0, 0, 0.6)',
    marginBottom: '4px',
  };

  // Field value styling
  const valueStyle = {
    fontWeight: '400',
    wordBreak: 'break-word',
  };

  return (
    <Card
      title="Alert Details"
      className={className}
      loading={loading}
      error={error}
    >
      <Box>
        {/* Basic Info Section */}
        <Box sx={sectionStyle}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Alert ID
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {alert.alertId}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Type
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {alert.alertType}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Source
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {alert.source}
              </Typography>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Component
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {alert.component}
              </Typography>
            </Grid>
          </Grid>
        </Box>

        {/* Status Section */}
        <Box sx={sectionStyle}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Status
              </Typography>
              <Box sx={{ mt: 1 }}>{renderStatusChip()}</Box>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Severity
              </Typography>
              <Box sx={{ mt: 1 }}>{renderSeverityChip()}</Box>
            </Grid>
          </Grid>
        </Box>

        {/* Timestamps Section */}
        <Box sx={sectionStyle}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6}>
              <Typography variant="subtitle2" sx={labelStyle}>
                Created At
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {formatDate(alert.createdAt, 'MMM dd, yyyy HH:mm:ss')}
              </Typography>
            </Grid>
            
            {alert.acknowledgedAt && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Acknowledged At
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {formatDate(alert.acknowledgedAt, 'MMM dd, yyyy HH:mm:ss')}
                </Typography>
              </Grid>
            )}
            
            {alert.acknowledgedBy && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Acknowledged By
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {alert.acknowledgedBy}
                </Typography>
              </Grid>
            )}
            
            {alert.resolvedAt && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Resolved At
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {formatDate(alert.resolvedAt, 'MMM dd, yyyy HH:mm:ss')}
                </Typography>
              </Grid>
            )}
            
            {alert.resolvedBy && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Resolved By
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {alert.resolvedBy}
                </Typography>
              </Grid>
            )}
            
            {alert.escalatedAt && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Escalated At
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {formatDate(alert.escalatedAt, 'MMM dd, yyyy HH:mm:ss')}
                </Typography>
              </Grid>
            )}
            
            {alert.escalatedBy && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Escalated By
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {alert.escalatedBy}
                </Typography>
              </Grid>
            )}
            
            {alert.escalationLevel && (
              <Grid item xs={12} sm={6}>
                <Typography variant="subtitle2" sx={labelStyle}>
                  Escalation Level
                </Typography>
                <Typography variant="body1" sx={valueStyle}>
                  {alert.escalationLevel}
                </Typography>
              </Grid>
            )}
          </Grid>
        </Box>

        {/* Message and Details Section */}
        <Box sx={sectionStyle}>
          <Typography variant="subtitle2" sx={labelStyle}>
            Message
          </Typography>
          <Typography variant="body1" sx={valueStyle}>
            {alert.message}
          </Typography>

          {alert.details && (
            <>
              <Typography variant="subtitle2" sx={{ ...labelStyle, mt: 2 }}>
                Additional Details
              </Typography>
              <Typography variant="body1" sx={valueStyle}>
                {typeof alert.details === 'object'
                  ? JSON.stringify(alert.details, null, 2)
                  : String(alert.details)}
              </Typography>
            </>
          )}
        </Box>

        {/* Self-Healing Status Section (if applicable) */}
        {alert.selfHealingStatus && (
          <Box sx={sectionStyle}>
            <Typography variant="subtitle2" sx={labelStyle}>
              Self-Healing Status
            </Typography>
            <Typography variant="body1" sx={valueStyle}>
              {alert.selfHealingStatus}
            </Typography>
          </Box>
        )}

        {/* Action Buttons */}
        {renderActionButtons()}

        {/* Confirmation Dialogs */}
        {renderConfirmationDialog()}
      </Box>
    </Card>
  );
};

export default AlertDetailsCard;