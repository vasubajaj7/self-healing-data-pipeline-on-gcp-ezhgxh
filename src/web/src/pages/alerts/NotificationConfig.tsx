import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Grid,
  Paper,
  Tabs,
  Tab,
  TextField,
  Switch,
  FormControlLabel,
  FormGroup,
  Button,
  Divider,
  Alert,
  Snackbar,
  CircularProgress,
  useTheme,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Notifications,
  Email,
  Sms,
  Settings,
  Save,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { Helmet } from 'react-helmet'; // react-helmet ^6.1.0

import MainLayout from '../../components/layout/MainLayout';
import NotificationChannelsCard from '../../components/alert/NotificationChannelsCard';
import { alertService } from '../../services/api/alertService';
import { useApi } from '../../hooks/useApi';
import {
  NotificationConfig,
  EmailConfig,
  SMSConfig,
  NotificationThreshold,
  NotificationChannel,
  AlertSeverity,
} from '../../types/alerts';

/**
 * Interface for TabPanel props
 */
interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

/**
 * TabPanel component to display content for each tab
 */
function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}

/**
 * Interface for feedback notification state
 */
interface FeedbackState {
  open: boolean;
  message: string;
  severity: 'success' | 'error' | 'info' | 'warning';
}

/**
 * Main component for notification configuration page
 */
const NotificationConfig: React.FC = () => {
  // State variables
  const [config, setConfig] = useState<NotificationConfig | null>(null);
  const [activeTab, setActiveTab] = useState<number>(0);
  const [feedback, setFeedback] = useState<FeedbackState>({ open: false, message: '', severity: 'info' });
  const [isDirty, setIsDirty] = useState<boolean>(false);

  // API hooks
  const { loading: configLoading, error: configError, get: apiGet } = useApi();
  const { loading: saveLoading, error: saveError, put: apiPut } = useApi();
  const theme = useTheme();

  /**
   * Fetches current notification configuration
   */
  const fetchNotificationConfig = useCallback(async () => {
    setFeedback({ open: false, message: '', severity: 'info' }); // Clear any existing feedback
    try {
      const fetchedConfig = await alertService.getNotificationChannels();

      // Create a default configuration if none exists
      const defaultConfig: NotificationConfig = {
        channels: {
          [NotificationChannel.TEAMS]: fetchedConfig.teams,
          [NotificationChannel.EMAIL]: fetchedConfig.email,
          [NotificationChannel.SMS]: fetchedConfig.sms,
        },
        teamsWebhookUrl: '',
        emailConfig: {
          recipients: [],
          subjectPrefix: '',
          includeDetails: false,
        },
        smsConfig: {
          phoneNumbers: [],
          criticalOnly: false,
        },
        alertThresholds: {
          [AlertSeverity.CRITICAL]: { enabled: true, minInterval: 0 },
          [AlertSeverity.HIGH]: { enabled: true, minInterval: 0 },
          [AlertSeverity.MEDIUM]: { enabled: true, minInterval: 0 },
          [AlertSeverity.LOW]: { enabled: false, minInterval: 0 },
        },
        updatedAt: new Date().toISOString(),
      };

      setConfig(defaultConfig);
    } catch (error: any) {
      setFeedback({ open: true, message: `Failed to load configuration: ${error.message}`, severity: 'error' });
    }
  }, [apiGet]);

  /**
   * Handles tab selection change
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  /**
   * Handles changes to configuration values
   */
  const handleConfigChange = (path: string, value: any) => {
    if (!config) return;

    // Deep copy the config
    const newConfig = JSON.parse(JSON.stringify(config));

    // Update the value at the specified path
    let current = newConfig;
    const pathParts = path.split('.');
    for (let i = 0; i < pathParts.length - 1; i++) {
      current = current[pathParts[i]];
    }
    current[pathParts[pathParts.length - 1]] = value;

    setConfig(newConfig);
    setIsDirty(true);
  };

  /**
   * Saves the current configuration
   */
  const handleSaveConfig = async () => {
    if (!config) return;

    setFeedback({ open: false, message: '', severity: 'info' }); // Clear any existing feedback
    try {
      await alertService.updateNotificationConfig(config);
      setFeedback({ open: true, message: 'Configuration saved successfully!', severity: 'success' });
      setIsDirty(false);
    } catch (error: any) {
      setFeedback({ open: true, message: `Failed to save configuration: ${error.message}`, severity: 'error' });
    }
  };

  /**
   * Handles closing of feedback notification
   */
  const handleFeedbackClose = () => {
    setFeedback({ ...feedback, open: false });
  };

  // Fetch configuration on component mount
  useEffect(() => {
    fetchNotificationConfig();
  }, [fetchNotificationConfig]);

  return (
    <MainLayout>
      <Helmet>
        <title>Notification Configuration - Self-Healing Data Pipeline</title>
      </Helmet>
      <Box sx={{ mb: 2 }}>
        <Typography variant="h4" gutterBottom>
          <Notifications sx={{ mr: 1, verticalAlign: 'middle' }} />
          Notification Configuration
        </Typography>
      </Box>

      <Paper elevation={3} sx={{ width: '100%', overflow: 'hidden' }}>
        <Tabs
          value={activeTab}
          onChange={handleTabChange}
          aria-label="notification configuration tabs"
          indicatorColor="primary"
          textColor="primary"
          variant="scrollable"
          scrollButtons="auto"
        >
          <Tab label="Channels" icon={<Notifications />} iconPosition="start" />
          <Tab label="Teams" icon={<Notifications />} iconPosition="start" disabled={!config?.channels[NotificationChannel.TEAMS]} />
          <Tab label="Email" icon={<Email />} iconPosition="start" disabled={!config?.channels[NotificationChannel.EMAIL]} />
          <Tab label="SMS" icon={<Sms />} iconPosition="start" disabled={!config?.channels[NotificationChannel.SMS]} />
          <Tab label="Thresholds" icon={<Settings />} iconPosition="start" />
        </Tabs>

        {configLoading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
            <CircularProgress />
          </Box>
        ) : (
          <>
            <TabPanel value={activeTab} index={0}>
              <ChannelsTab config={config} onChange={handleConfigChange} />
            </TabPanel>
            <TabPanel value={activeTab} index={1}>
              <TeamsTab config={config} onChange={handleConfigChange} />
            </TabPanel>
            <TabPanel value={activeTab} index={2}>
              <EmailTab config={config} onChange={handleConfigChange} />
            </TabPanel>
            <TabPanel value={activeTab} index={3}>
              <SMSTab config={config} onChange={handleConfigChange} />
            </TabPanel>
            <TabPanel value={activeTab} index={4}>
              <ThresholdsTab config={config} onChange={handleConfigChange} />
            </TabPanel>
          </>
        )}

        <Divider />
        <Box sx={{ display: 'flex', justifyContent: 'flex-end', p: 2 }}>
          <Button
            variant="contained"
            color="primary"
            onClick={handleSaveConfig}
            disabled={!isDirty || saveLoading}
            startIcon={<Save />}
          >
            {saveLoading ? 'Saving...' : 'Save Changes'}
          </Button>
        </Box>
      </Paper>

      <Snackbar
        open={feedback.open}
        autoHideDuration={5000}
        onClose={handleFeedbackClose}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={handleFeedbackClose} severity={feedback.severity} sx={{ width: '100%' }}>
          {feedback.message}
        </Alert>
      </Snackbar>
    </MainLayout>
  );
};

export default NotificationConfig;

/**
 * Props for the TabPanel component
 */
interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

/**
 * TabPanel component to display content for each tab
 */
function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          <Typography>{children}</Typography>
        </Box>
      )}
    </div>
  );
}

/**
 * Props for the ChannelsTab component
 */
interface ChannelsTabProps {
  config: NotificationConfig | null;
  onChange: (path: string, value: any) => void;
}

/**
 * Tab content for enabling/disabling notification channels
 */
const ChannelsTab: React.FC<ChannelsTabProps> = ({ config, onChange }) => {
  if (!config) return null;

  return (
    <FormGroup>
      <FormControlLabel
        control={
          <Switch
            checked={config.channels[NotificationChannel.TEAMS]}
            onChange={(e) => onChange('channels.TEAMS', e.target.checked)}
            name="teams"
          />
        }
        label="Microsoft Teams"
      />
      <FormControlLabel
        control={
          <Switch
            checked={config.channels[NotificationChannel.EMAIL]}
            onChange={(e) => onChange('channels.EMAIL', e.target.checked)}
            name="email"
          />
        }
        label="Email"
      />
      <FormControlLabel
        control={
          <Switch
            checked={config.channels[NotificationChannel.SMS]}
            onChange={(e) => onChange('channels.SMS', e.target.checked)}
            name="sms"
          />
        }
        label="SMS"
      />
    </FormGroup>
  );
};

/**
 * Props for the TeamsTab component
 */
interface TeamsTabProps {
  config: NotificationConfig | null;
  onChange: (path: string, value: any) => void;
}

/**
 * Tab content for Microsoft Teams webhook configuration
 */
const TeamsTab: React.FC<TeamsTabProps> = ({ config, onChange }) => {
  if (!config) return null;

  return (
    <Box>
      <TextField
        label="Webhook URL"
        fullWidth
        margin="normal"
        value={config.teamsWebhookUrl || ''}
        onChange={(e) => onChange('teamsWebhookUrl', e.target.value)}
        disabled={!config.channels[NotificationChannel.TEAMS]}
        helperText="Enter the Microsoft Teams webhook URL"
      />
    </Box>
  );
};

/**
 * Props for the EmailTab component
 */
interface EmailTabProps {
  config: NotificationConfig | null;
  onChange: (path: string, value: any) => void;
}

/**
 * Tab content for email notification configuration
 */
const EmailTab: React.FC<EmailTabProps> = ({ config, onChange }) => {
  if (!config || !config.emailConfig) return null;

  return (
    <Box>
      <TextField
        label="Recipient Email Addresses (comma-separated)"
        fullWidth
        margin="normal"
        value={config.emailConfig.recipients.join(', ') || ''}
        onChange={(e) => onChange('emailConfig.recipients', e.target.value.split(',').map((email) => email.trim()))}
        disabled={!config.channels[NotificationChannel.EMAIL]}
        helperText="Enter the email addresses to send notifications to, separated by commas"
      />
      <TextField
        label="Subject Prefix"
        fullWidth
        margin="normal"
        value={config.emailConfig.subjectPrefix || ''}
        onChange={(e) => onChange('emailConfig.subjectPrefix', e.target.value)}
        disabled={!config.channels[NotificationChannel.EMAIL]}
        helperText="Optional prefix for email subjects"
      />
      <FormControlLabel
        control={
          <Switch
            checked={config.emailConfig.includeDetails}
            onChange={(e) => onChange('emailConfig.includeDetails', e.target.checked)}
            name="includeDetails"
            disabled={!config.channels[NotificationChannel.EMAIL]}
          />
        }
        label="Include Detailed Information"
      />
    </Box>
  );
};

/**
 * Props for the SMSTab component
 */
interface SMSTabProps {
  config: NotificationConfig | null;
  onChange: (path: string, value: any) => void;
}

/**
 * Tab content for SMS notification configuration
 */
const SMSTab: React.FC<SMSTabProps> = ({ config, onChange }) => {
  if (!config || !config.smsConfig) return null;

  return (
    <Box>
      <TextField
        label="Phone Numbers (comma-separated)"
        fullWidth
        margin="normal"
        value={config.smsConfig.phoneNumbers.join(', ') || ''}
        onChange={(e) => onChange('smsConfig.phoneNumbers', e.target.value.split(',').map((phone) => phone.trim()))}
        disabled={!config.channels[NotificationChannel.SMS]}
        helperText="Enter the phone numbers to send SMS notifications to, separated by commas"
      />
      <FormControlLabel
        control={
          <Switch
            checked={config.smsConfig.criticalOnly}
            onChange={(e) => onChange('smsConfig.criticalOnly', e.target.checked)}
            name="criticalOnly"
            disabled={!config.channels[NotificationChannel.SMS]}
          />
        }
        label="Critical Alerts Only"
      />
    </Box>
  );
};

/**
 * Props for the ThresholdsTab component
 */
interface ThresholdsTabProps {
  config: NotificationConfig | null;
  onChange: (path: string, value: any) => void;
}

/**
 * Tab content for alert threshold configuration
 */
const ThresholdsTab: React.FC<ThresholdsTabProps> = ({ config, onChange }) => {
  if (!config) return null;

  return (
    <Grid container spacing={2}>
      {Object.values(AlertSeverity).map((severity) => (
        <Grid item xs={12} sm={6} key={severity}>
          <Paper elevation={2} sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              {severity} Alerts
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={config.alertThresholds[severity].enabled}
                  onChange={(e) => onChange(`alertThresholds.${severity}.enabled`, e.target.checked)}
                  name={`${severity}-enabled`}
                />
              }
              label="Enable Notifications"
            />
            <TextField
              label="Minimum Interval (minutes)"
              type="number"
              fullWidth
              margin="normal"
              value={config.alertThresholds[severity].minInterval}
              onChange={(e) => onChange(`alertThresholds.${severity}.minInterval`, parseInt(e.target.value))}
              helperText="Minimum time between notifications (in minutes)"
            />
            {/* Add batch size configuration if needed in the future */}
          </Paper>
        </Grid>
      ))}
    </Grid>
  );
};