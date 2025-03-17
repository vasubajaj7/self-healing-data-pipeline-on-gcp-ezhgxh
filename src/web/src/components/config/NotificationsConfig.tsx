import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Grid,
  Typography,
  Button,
  Switch,
  FormControlLabel,
  Divider,
  CircularProgress
} from '@mui/material';
import {
  Add as AddIcon,
  Send as SendIcon,
  Email as EmailIcon,
  Message as MessageIcon
} from '@mui/icons-material';
import * as yup from 'yup';

import Card from '../common/Card';
import Form from '../common/Form';
import Input from '../common/Input';
import Select from '../common/Select';
import configService from '../../services/api/configService';
import { NotificationConfig, EmailSettings } from '../../types/config';
import useNotification from '../../hooks/useNotification';

/**
 * Interface for the form values used in the notification configuration forms
 */
interface NotificationFormValues {
  teamsWebhookUrl?: string;
  emailRecipients?: string;
  emailCcRecipients?: string;
  emailSubjectPrefix?: string;
  enabledChannels: Record<string, boolean>;
  alertThresholds: Record<string, string>;
}

/**
 * Component for configuring notification settings in the application
 */
const NotificationsConfig: React.FC = () => {
  // State for notification config
  const [config, setConfig] = useState<NotificationConfig>({
    teamsWebhookUrl: '',
    emailSettings: {
      recipients: [],
      ccRecipients: [],
      subjectPrefix: ''
    },
    enabledChannels: {
      teams: false,
      email: false
    },
    alertThresholds: {
      pipeline: 'HIGH',
      quality: 'MEDIUM',
      performance: 'MEDIUM',
      system: 'CRITICAL'
    },
    updatedAt: new Date().toISOString()
  });

  // Loading and testing states
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [isTestingTeams, setIsTestingTeams] = useState<boolean>(false);
  const [isTestingEmail, setIsTestingEmail] = useState<boolean>(false);
  
  // Notifications hook for showing success/error messages
  const { showSuccess, showError } = useNotification();

  // Validation schema for notification forms
  const validationSchema = yup.object({
    teamsWebhookUrl: yup.string().url('Please enter a valid URL'),
    emailRecipients: yup.string()
      .test('emails', 'Please enter valid email addresses', value => {
        if (!value) return true;
        const emails = value.split(',').map(email => email.trim());
        const emailRegex = /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i;
        return emails.every(email => emailRegex.test(email));
      }),
    emailCcRecipients: yup.string()
      .test('emails', 'Please enter valid email addresses', value => {
        if (!value) return true;
        const emails = value.split(',').map(email => email.trim());
        const emailRegex = /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i;
        return emails.every(email => emailRegex.test(email));
      }),
    emailSubjectPrefix: yup.string()
  });

  /**
   * Fetches the current notification configuration from the API
   */
  const fetchNotificationConfig = useCallback(async () => {
    setIsLoading(true);
    try {
      const response = await configService.getNotificationConfig();
      setConfig(response.data);
    } catch (error) {
      showError('Failed to load notification configuration. Please try again.');
      console.error('Error fetching notification config:', error);
    } finally {
      setIsLoading(false);
    }
  }, [showError]);

  // Load configuration on component mount
  useEffect(() => {
    fetchNotificationConfig();
  }, [fetchNotificationConfig]);

  /**
   * Handles the submission of updated notification configuration
   * @param updatedConfig - The updated notification configuration
   */
  const handleUpdateConfig = async (updatedConfig: NotificationConfig) => {
    setIsLoading(true);
    try {
      const response = await configService.updateNotificationConfig(updatedConfig);
      setConfig(response.data);
      showSuccess('Notification settings updated successfully.');
    } catch (error) {
      showError('Failed to update notification settings. Please try again.');
      console.error('Error updating notification config:', error);
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Tests the Microsoft Teams webhook by sending a test notification
   */
  const handleTestTeamsWebhook = async () => {
    if (!config.teamsWebhookUrl) {
      showError('Please enter a Teams webhook URL before testing.');
      return;
    }

    setIsTestingTeams(true);
    try {
      const response = await configService.testNotificationChannel({
        channel: 'teams',
        config: { webhookUrl: config.teamsWebhookUrl }
      });
      
      if (response.data.success) {
        showSuccess('Test notification sent to Microsoft Teams successfully.');
      } else {
        showError(`Failed to send test notification: ${response.data.message}`);
      }
    } catch (error) {
      showError('Failed to send test notification to Microsoft Teams.');
      console.error('Error testing Teams webhook:', error);
    } finally {
      setIsTestingTeams(false);
    }
  };

  /**
   * Tests the email notification by sending a test email
   */
  const handleTestEmail = async () => {
    if (!config.emailSettings || !config.emailSettings.recipients.length) {
      showError('Please enter at least one email recipient before testing.');
      return;
    }

    setIsTestingEmail(true);
    try {
      const response = await configService.testNotificationChannel({
        channel: 'email',
        config: config.emailSettings
      });
      
      if (response.data.success) {
        showSuccess('Test email sent successfully.');
      } else {
        showError(`Failed to send test email: ${response.data.message}`);
      }
    } catch (error) {
      showError('Failed to send test email. Please check the email configuration.');
      console.error('Error testing email notification:', error);
    } finally {
      setIsTestingEmail(false);
    }
  };

  /**
   * Card component for Microsoft Teams notification configuration
   */
  const TeamsNotificationCard: React.FC<{
    config: NotificationConfig;
    isLoading: boolean;
    isTesting: boolean;
    onTest: () => Promise<void>;
  }> = ({ config, isLoading, isTesting, onTest }) => (
    <Card
      title="Microsoft Teams Notifications"
      action={
        <FormControlLabel
          control={
            <Switch
              checked={config.enabledChannels.teams}
              onChange={(e) => {
                const enabled = e.target.checked;
                setConfig(prev => ({
                  ...prev,
                  enabledChannels: {
                    ...prev.enabledChannels,
                    teams: enabled
                  }
                }));
              }}
              disabled={isLoading}
            />
          }
          label={config.enabledChannels.teams ? "Enabled" : "Disabled"}
        />
      }
      loading={isLoading}
      avatar={<MessageIcon color="primary" />}
    >
      <Box sx={{ mt: 2 }}>
        <Input
          label="Teams Webhook URL"
          value={config.teamsWebhookUrl || ''}
          onChange={(value) => {
            setConfig(prev => ({
              ...prev,
              teamsWebhookUrl: value
            }));
          }}
          fullWidth
          disabled={isLoading || !config.enabledChannels.teams}
          placeholder="https://outlook.office.com/webhook/..."
          helperText="Enter the Microsoft Teams webhook URL for sending notifications"
        />
        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<SendIcon />}
            onClick={onTest}
            disabled={isLoading || isTesting || !config.enabledChannels.teams || !config.teamsWebhookUrl}
          >
            {isTesting ? (
              <>
                <CircularProgress size={20} color="inherit" sx={{ mr: 1 }} />
                Testing...
              </>
            ) : (
              'Test Webhook'
            )}
          </Button>
        </Box>
      </Box>
    </Card>
  );

  /**
   * Card component for email notification configuration
   */
  const EmailNotificationCard: React.FC<{
    config: NotificationConfig;
    isLoading: boolean;
    isTesting: boolean;
    onTest: () => Promise<void>;
  }> = ({ config, isLoading, isTesting, onTest }) => (
    <Card
      title="Email Notifications"
      action={
        <FormControlLabel
          control={
            <Switch
              checked={config.enabledChannels.email}
              onChange={(e) => {
                const enabled = e.target.checked;
                setConfig(prev => ({
                  ...prev,
                  enabledChannels: {
                    ...prev.enabledChannels,
                    email: enabled
                  }
                }));
              }}
              disabled={isLoading}
            />
          }
          label={config.enabledChannels.email ? "Enabled" : "Disabled"}
        />
      }
      loading={isLoading}
      avatar={<EmailIcon color="primary" />}
    >
      <Box sx={{ mt: 2 }}>
        <Input
          label="Email Recipients"
          value={config.emailSettings?.recipients.join(', ') || ''}
          onChange={(value) => {
            setConfig(prev => ({
              ...prev,
              emailSettings: {
                ...prev.emailSettings || { recipients: [], ccRecipients: [], subjectPrefix: '' },
                recipients: value.split(',').map(email => email.trim()).filter(email => email)
              }
            }));
          }}
          fullWidth
          disabled={isLoading || !config.enabledChannels.email}
          placeholder="email1@example.com, email2@example.com"
          helperText="Enter comma-separated list of email recipients"
        />
        <Box sx={{ mt: 2 }}>
          <Input
            label="CC Recipients (Optional)"
            value={config.emailSettings?.ccRecipients?.join(', ') || ''}
            onChange={(value) => {
              setConfig(prev => ({
                ...prev,
                emailSettings: {
                  ...prev.emailSettings || { recipients: [], ccRecipients: [], subjectPrefix: '' },
                  ccRecipients: value.split(',').map(email => email.trim()).filter(email => email)
                }
              }));
            }}
            fullWidth
            disabled={isLoading || !config.enabledChannels.email}
            placeholder="email1@example.com, email2@example.com"
            helperText="Enter comma-separated list of CC recipients"
          />
        </Box>
        <Box sx={{ mt: 2 }}>
          <Input
            label="Email Subject Prefix (Optional)"
            value={config.emailSettings?.subjectPrefix || ''}
            onChange={(value) => {
              setConfig(prev => ({
                ...prev,
                emailSettings: {
                  ...prev.emailSettings || { recipients: [], ccRecipients: [], subjectPrefix: '' },
                  subjectPrefix: value
                }
              }));
            }}
            fullWidth
            disabled={isLoading || !config.enabledChannels.email}
            placeholder="[Self-Healing Pipeline]"
            helperText="Enter a prefix to add to email subject lines"
          />
        </Box>
        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end' }}>
          <Button
            variant="contained"
            color="primary"
            startIcon={<SendIcon />}
            onClick={onTest}
            disabled={
              isLoading || 
              isTesting || 
              !config.enabledChannels.email || 
              !(config.emailSettings?.recipients?.length > 0)
            }
          >
            {isTesting ? (
              <>
                <CircularProgress size={20} color="inherit" sx={{ mr: 1 }} />
                Testing...
              </>
            ) : (
              'Test Email'
            )}
          </Button>
        </Box>
      </Box>
    </Card>
  );

  /**
   * Card component for configuring alert thresholds by severity
   */
  const AlertThresholdsCard: React.FC<{
    config: NotificationConfig;
    isLoading: boolean;
  }> = ({ config, isLoading }) => {
    // Options for alert severity levels
    const severityOptions = [
      { value: 'CRITICAL', label: 'Critical' },
      { value: 'HIGH', label: 'High' },
      { value: 'MEDIUM', label: 'Medium' },
      { value: 'LOW', label: 'Low' }
    ];
    
    return (
      <Card
        title="Alert Thresholds"
        loading={isLoading}
      >
        <Box sx={{ mt: 2 }}>
          <Typography variant="body2" color="textSecondary" gutterBottom>
            Select the minimum severity level for each alert type to trigger notifications.
          </Typography>
          
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} sm={6}>
              <Select
                label="Pipeline Alerts"
                options={severityOptions}
                value={config.alertThresholds.pipeline || 'HIGH'}
                onChange={(value) => {
                  setConfig(prev => ({
                    ...prev,
                    alertThresholds: {
                      ...prev.alertThresholds,
                      pipeline: value as string
                    }
                  }));
                }}
                fullWidth
                disabled={isLoading}
              />
              <Typography variant="caption" color="textSecondary">
                Alerts related to pipeline execution status
              </Typography>
            </Grid>
            
            <Grid item xs={12} sm={6}>
              <Select
                label="Quality Alerts"
                options={severityOptions}
                value={config.alertThresholds.quality || 'MEDIUM'}
                onChange={(value) => {
                  setConfig(prev => ({
                    ...prev,
                    alertThresholds: {
                      ...prev.alertThresholds,
                      quality: value as string
                    }
                  }));
                }}
                fullWidth
                disabled={isLoading}
              />
              <Typography variant="caption" color="textSecondary">
                Alerts related to data quality validation
              </Typography>
            </Grid>
            
            <Grid item xs={12} sm={6}>
              <Select
                label="Performance Alerts"
                options={severityOptions}
                value={config.alertThresholds.performance || 'MEDIUM'}
                onChange={(value) => {
                  setConfig(prev => ({
                    ...prev,
                    alertThresholds: {
                      ...prev.alertThresholds,
                      performance: value as string
                    }
                  }));
                }}
                fullWidth
                disabled={isLoading}
              />
              <Typography variant="caption" color="textSecondary">
                Alerts related to system performance
              </Typography>
            </Grid>
            
            <Grid item xs={12} sm={6}>
              <Select
                label="System Alerts"
                options={severityOptions}
                value={config.alertThresholds.system || 'CRITICAL'}
                onChange={(value) => {
                  setConfig(prev => ({
                    ...prev,
                    alertThresholds: {
                      ...prev.alertThresholds,
                      system: value as string
                    }
                  }));
                }}
                fullWidth
                disabled={isLoading}
              />
              <Typography variant="caption" color="textSecondary">
                Alerts related to system health and infrastructure
              </Typography>
            </Grid>
          </Grid>
        </Box>
      </Card>
    );
  };

  return (
    <Box sx={{ mb: 4 }}>
      <Typography variant="h5" gutterBottom>
        Notification Settings
      </Typography>
      <Typography variant="body1" color="textSecondary" sx={{ mb: 3 }}>
        Configure how and when you want to receive notifications from the self-healing pipeline system.
      </Typography>
      
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <TeamsNotificationCard 
            config={config} 
            isLoading={isLoading} 
            isTesting={isTestingTeams}
            onTest={handleTestTeamsWebhook}
          />
        </Grid>
        <Grid item xs={12} md={6}>
          <EmailNotificationCard 
            config={config} 
            isLoading={isLoading} 
            isTesting={isTestingEmail}
            onTest={handleTestEmail}
          />
        </Grid>
        <Grid item xs={12}>
          <AlertThresholdsCard 
            config={config} 
            isLoading={isLoading} 
          />
        </Grid>
        <Grid item xs={12}>
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
            <Button
              variant="contained"
              color="primary"
              onClick={() => handleUpdateConfig(config)}
              disabled={isLoading || isTestingTeams || isTestingEmail}
            >
              {isLoading ? (
                <>
                  <CircularProgress size={20} color="inherit" sx={{ mr: 1 }} />
                  Saving...
                </>
              ) : (
                'Save Changes'
              )}
            </Button>
          </Box>
        </Grid>
      </Grid>
    </Box>
  );
};

export default NotificationsConfig;