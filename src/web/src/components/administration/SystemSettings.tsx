import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Grid,
  Paper,
  Divider,
  Switch,
  Slider,
  Tabs,
  Tab,
  Alert,
  Snackbar,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Save,
  Settings,
  Notifications,
  Speed,
  Security,
  CloudSync,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11
import Card from '../common/Card';
import Form from '../common/Form';
import Input from '../common/Input';
import Select from '../common/Select';
import Button from '../common/Button';
import adminService from '../../services/api/adminService';
import useAuth from '../../hooks/useAuth';
import { HealingConfig, AlertConfig, OptimizationConfig } from '../../types/api';

// Define the TabPanelProps interface
interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

/**
 * TabPanel component for rendering tab content
 * @param {TabPanelProps} props - The props for the TabPanel component
 * @returns {JSX.Element | null} The rendered TabPanel component
 */
function TabPanel(props: TabPanelProps): JSX.Element | null {
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

// Define the GeneralSettingsFormValues interface
interface GeneralSettingsFormValues {
  applicationName: string;
  defaultPageSize: number;
  defaultLanguage: string;
  dataRetentionDays: number;
  enableTelemetry: boolean;
  maintenanceMode: boolean;
}

// Define the HealingConfigFormValues interface
interface HealingConfigFormValues {
  healingMode: string;
  globalConfidenceThreshold: number;
  maxRetryAttempts: number;
  approvalRequiredHighImpact: boolean;
  learningModeActive: boolean;
  additionalSettings: Record<string, any>;
}

// Define the AlertConfigFormValues interface
interface AlertConfigFormValues {
  teamsWebhookUrl: Record<string, string>;
  emailConfig: Record<string, any>;
  alertThresholds: Record<string, number>;
  enabledChannels: Record<string, boolean>;
}

// Define the OptimizationConfigFormValues interface
interface OptimizationConfigFormValues {
  queryOptimizationSettings: Record<string, any>;
  schemaOptimizationSettings: Record<string, any>;
  resourceOptimizationSettings: Record<string, any>;
  autoImplementationEnabled: boolean;
}

/**
 * SystemSettings component for managing system-wide settings
 * @returns {JSX.Element} The rendered SystemSettings component
 */
const SystemSettings: React.FC = () => {
  // State variables
  const [activeTab, setActiveTab] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [generalSettings, setGeneralSettings] = useState<any>({});
  const [healingConfig, setHealingConfig] = useState<HealingConfig>({
    healingMode: 'semi-automatic',
    globalConfidenceThreshold: 85,
    maxRetryAttempts: 3,
    approvalRequiredHighImpact: true,
    learningModeActive: true,
    additionalSettings: {},
    updatedAt: new Date().toISOString()
  });
  const [alertConfig, setAlertConfig] = useState<AlertConfig>({
    teamsWebhookUrl: {},
    emailConfig: {},
    alertThresholds: {},
    enabledChannels: { teams: true, email: true },
    updatedAt: new Date().toISOString()
  });
  const [optimizationConfig, setOptimizationConfig] = useState<OptimizationConfig>({
    queryOptimizationSettings: {},
    schemaOptimizationSettings: {},
    resourceOptimizationSettings: {},
    autoImplementationEnabled: false,
    updatedAt: new Date().toISOString()
  });
  const [notification, setNotification] = useState<{ open: boolean; message: string; type: 'success' | 'error' | 'info' | 'warning' }>({
    open: false,
    message: '',
    type: 'info'
  });

  // Authentication hook
  const { checkPermission } = useAuth();

  /**
   * Fetches system settings from the API
   */
  const fetchSystemSettings = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await adminService.getSystemSettings();
      setGeneralSettings(response.data.generalSettings || {});
      setHealingConfig(response.data.healingConfig || {
        healingMode: 'semi-automatic',
        globalConfidenceThreshold: 85,
        maxRetryAttempts: 3,
        approvalRequiredHighImpact: true,
        learningModeActive: true,
        additionalSettings: {},
        updatedAt: new Date().toISOString()
      });
      setAlertConfig(response.data.alertConfig || {
        teamsWebhookUrl: {},
        emailConfig: {},
        alertThresholds: {},
        enabledChannels: { teams: true, email: true },
        updatedAt: new Date().toISOString()
      });
      setOptimizationConfig(response.data.optimizationConfig || {
        queryOptimizationSettings: {},
        schemaOptimizationSettings: {},
        resourceOptimizationSettings: {},
        autoImplementationEnabled: false,
        updatedAt: new Date().toISOString()
      });
    } catch (err: any) {
      setError(err.message || 'Failed to load system settings.');
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * Handles tab change events
   * @param {React.SyntheticEvent} event - The event object
   * @param {number} newValue - The new tab index
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  /**
   * Handles general settings form submission
   * @param {any} values - The form values
   * @param {FormikHelpers<any>} helpers - The Formik helpers
   */
  const handleGeneralSettingsSubmit = async (values: any, helpers: FormikHelpers<any>) => {
    try {
      await adminService.updateSystemSettings({ generalSettings: values });
      showNotification('General settings updated successfully', 'success');
      fetchSystemSettings();
      helpers.setSubmitting(false);
    } catch (err: any) {
      setError(err.message || 'Failed to update general settings.');
      showNotification(err.message || 'Failed to update general settings.', 'error');
      helpers.setSubmitting(false);
    }
  };

  /**
   * Handles self-healing configuration form submission
   * @param {HealingConfig} values - The form values
   * @param {FormikHelpers<HealingConfig>} helpers - The Formik helpers
   */
  const handleHealingConfigSubmit = async (values: HealingConfig, helpers: FormikHelpers<HealingConfig>) => {
    try {
      await adminService.updateSystemSettings({ healingConfig: values });
      showNotification('Self-healing configuration updated successfully', 'success');
      fetchSystemSettings();
      helpers.setSubmitting(false);
    } catch (err: any) {
      setError(err.message || 'Failed to update self-healing configuration.');
      showNotification(err.message || 'Failed to update self-healing configuration.', 'error');
      helpers.setSubmitting(false);
    }
  };

  /**
   * Handles alert configuration form submission
   * @param {AlertConfig} values - The form values
   * @param {FormikHelpers<AlertConfig>} helpers - The Formik helpers
   */
  const handleAlertConfigSubmit = async (values: AlertConfig, helpers: FormikHelpers<AlertConfig>) => {
    try {
      await adminService.updateSystemSettings({ alertConfig: values });
      showNotification('Alert configuration updated successfully', 'success');
      fetchSystemSettings();
      helpers.setSubmitting(false);
    } catch (err: any) {
      setError(err.message || 'Failed to update alert configuration.');
      showNotification(err.message || 'Failed to update alert configuration.', 'error');
      helpers.setSubmitting(false);
    }
  };

  /**
   * Handles optimization configuration form submission
   * @param {OptimizationConfig} values - The form values
   * @param {FormikHelpers<OptimizationConfig>} helpers - The Formik helpers
   */
  const handleOptimizationConfigSubmit = async (values: OptimizationConfig, helpers: FormikHelpers<OptimizationConfig>) => {
    try {
      await adminService.updateSystemSettings({ optimizationConfig: values });
      showNotification('Optimization configuration updated successfully', 'success');
      fetchSystemSettings();
      helpers.setSubmitting(false);
    } catch (err: any) {
      setError(err.message || 'Failed to update optimization configuration.');
      showNotification(err.message || 'Failed to update optimization configuration.', 'error');
      helpers.setSubmitting(false);
    }
  };

  /**
   * Shows a notification message
   * @param {string} message - The message to display
   * @param {'success' | 'error' | 'info' | 'warning'} type - The type of notification
   */
  const showNotification = (message: string, type: 'success' | 'error' | 'info' | 'warning') => {
    setNotification({ open: true, message, type });
  };

  /**
   * Closes the notification snackbar
   */
  const closeNotification = () => {
    setNotification({ ...notification, open: false });
  };

  // Validation schemas using yup
  const generalSettingsSchema = yup.object().shape({
    applicationName: yup.string().required('Application name is required'),
    defaultPageSize: yup.number().positive('Page size must be positive').integer('Page size must be an integer').required('Default page size is required'),
    defaultLanguage: yup.string().required('Default language is required'),
    dataRetentionDays: yup.number().positive('Data retention days must be positive').integer('Data retention days must be an integer').required('Data retention days is required'),
    enableTelemetry: yup.boolean().required('Enable telemetry is required'),
    maintenanceMode: yup.boolean().required('Maintenance mode is required'),
  });

  const healingConfigSchema = yup.object().shape({
    healingMode: yup.string().required('Healing mode is required'),
    globalConfidenceThreshold: yup.number().min(0).max(100).required('Confidence threshold is required'),
    maxRetryAttempts: yup.number().positive('Max retry attempts must be positive').integer('Max retry attempts must be an integer').required('Max retry attempts is required'),
    approvalRequiredHighImpact: yup.boolean().required('Approval required for high impact is required'),
    learningModeActive: yup.boolean().required('Learning mode active is required'),
  });

  const alertConfigSchema = yup.object().shape({
    teamsWebhookUrl: yup.object().shape({}).optional(),
    emailConfig: yup.object().shape({}).optional(),
    alertThresholds: yup.object().shape({}).optional(),
    enabledChannels: yup.object().shape({}).optional(),
  });

  const optimizationConfigSchema = yup.object().shape({
    queryOptimizationSettings: yup.object().shape({}).optional(),
    schemaOptimizationSettings: yup.object().shape({}).optional(),
    resourceOptimizationSettings: yup.object().shape({}).optional(),
    autoImplementationEnabled: yup.boolean().required('Auto implementation enabled is required'),
  });

  // Check for MANAGE_SETTINGS permission
  if (!checkPermission('MANAGE_SETTINGS')) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography variant="h6" color="error">
          Access Denied: You do not have permission to manage system settings.
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%' }}>
      <Typography variant="h4" gutterBottom>
        System Settings
      </Typography>
      <Tabs value={activeTab} onChange={handleTabChange} aria-label="system settings tabs">
        <Tab icon={<Settings />} label="General" id="simple-tab-0" aria-controls="simple-tabpanel-0" />
        <Tab icon={<Security />} label="Self-Healing" id="simple-tab-1" aria-controls="simple-tabpanel-1" />
        <Tab icon={<Notifications />} label="Alerting" id="simple-tab-2" aria-controls="simple-tabpanel-2" />
        <Tab icon={<Speed />} label="Optimization" id="simple-tab-3" aria-controls="simple-tabpanel-3" />
      </Tabs>

      <TabPanel value={activeTab} index={0}>
        <Card title="General Settings" loading={loading} error={error}>
          <Form
            initialValues={{
              applicationName: generalSettings.applicationName || '',
              defaultPageSize: generalSettings.defaultPageSize || 25,
              defaultLanguage: generalSettings.defaultLanguage || 'en',
              dataRetentionDays: generalSettings.dataRetentionDays || 30,
              enableTelemetry: generalSettings.enableTelemetry !== undefined ? generalSettings.enableTelemetry : true,
              maintenanceMode: generalSettings.maintenanceMode !== undefined ? generalSettings.maintenanceMode : false,
            }}
            validationSchema={generalSettingsSchema}
            onSubmit={handleGeneralSettingsSubmit}
          >
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                <Input label="Application Name" name="applicationName" value={generalSettings.applicationName || ''} onChange={(value) => setGeneralSettings({ ...generalSettings, applicationName: value })} />
              </Grid>
              <Grid item xs={12} md={6}>
                <Input label="Default Page Size" name="defaultPageSize" type="number" value={generalSettings.defaultPageSize || 25} onChange={(value) => setGeneralSettings({ ...generalSettings, defaultPageSize: value })} />
              </Grid>
              <Grid item xs={12} md={6}>
                <Input label="Default Language" name="defaultLanguage" value={generalSettings.defaultLanguage || 'en'} onChange={(value) => setGeneralSettings({ ...generalSettings, defaultLanguage: value })} />
              </Grid>
              <Grid item xs={12} md={6}>
                <Input label="Data Retention Days" name="dataRetentionDays" type="number" value={generalSettings.dataRetentionDays || 30} onChange={(value) => setGeneralSettings({ ...generalSettings, dataRetentionDays: value })} />
              </Grid>
              <Grid item xs={12}>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={generalSettings.enableTelemetry !== undefined ? generalSettings.enableTelemetry : true}
                    onChange={(e) => setGeneralSettings({ ...generalSettings, enableTelemetry: e.target.checked })}
                    name="enableTelemetry"
                    inputProps={{ 'aria-label': 'Enable Telemetry' }}
                  />
                  <Typography>Enable Telemetry</Typography>
                </Box>
              </Grid>
              <Grid item xs={12}>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={generalSettings.maintenanceMode !== undefined ? generalSettings.maintenanceMode : false}
                    onChange={(e) => setGeneralSettings({ ...generalSettings, maintenanceMode: e.target.checked })}
                    name="maintenanceMode"
                    inputProps={{ 'aria-label': 'Maintenance Mode' }}
                  />
                  <Typography>Maintenance Mode</Typography>
                </Box>
              </Grid>
            </Grid>
            <Button type="submit" variant="contained" startIcon={<Save />}>
              Save General Settings
            </Button>
          </Form>
        </Card>
      </TabPanel>

      <TabPanel value={activeTab} index={1}>
        <Card title="Self-Healing Configuration" loading={loading} error={error}>
          <Form
            initialValues={{
              healingMode: healingConfig.healingMode || 'semi-automatic',
              globalConfidenceThreshold: healingConfig.globalConfidenceThreshold || 85,
              maxRetryAttempts: healingConfig.maxRetryAttempts || 3,
              approvalRequiredHighImpact: healingConfig.approvalRequiredHighImpact !== undefined ? healingConfig.approvalRequiredHighImpact : true,
              learningModeActive: healingConfig.learningModeActive !== undefined ? healingConfig.learningModeActive : true,
              additionalSettings: healingConfig.additionalSettings || {},
            }}
            validationSchema={healingConfigSchema}
            onSubmit={handleHealingConfigSubmit}
          >
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                <Select
                  label="Healing Mode"
                  name="healingMode"
                  value={healingConfig.healingMode || 'semi-automatic'}
                  onChange={(value) => setHealingConfig({ ...healingConfig, healingMode: value as string })}
                  options={[
                    { value: 'automatic', label: 'Automatic' },
                    { value: 'semi-automatic', label: 'Semi-Automatic' },
                    { value: 'manual', label: 'Manual' },
                  ]}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Typography id="confidence-slider" gutterBottom>
                  Global Confidence Threshold
                </Typography>
                <Slider
                  value={healingConfig.globalConfidenceThreshold || 85}
                  onChange={(event, newValue) => setHealingConfig({ ...healingConfig, globalConfidenceThreshold: newValue as number })}
                  aria-labelledby="continuous-slider"
                  valueLabelDisplay="auto"
                  step={5}
                  marks
                  min={0}
                  max={100}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <Input label="Max Retry Attempts" name="maxRetryAttempts" type="number" value={healingConfig.maxRetryAttempts || 3} onChange={(value) => setHealingConfig({ ...healingConfig, maxRetryAttempts: value as number })} />
              </Grid>
              <Grid item xs={12}>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={healingConfig.approvalRequiredHighImpact !== undefined ? healingConfig.approvalRequiredHighImpact : true}
                    onChange={(e) => setHealingConfig({ ...healingConfig, approvalRequiredHighImpact: e.target.checked })}
                    name="approvalRequiredHighImpact"
                    inputProps={{ 'aria-label': 'Approval Required for High Impact' }}
                  />
                  <Typography>Approval Required for High Impact</Typography>
                </Box>
              </Grid>
              <Grid item xs={12}>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={healingConfig.learningModeActive !== undefined ? healingConfig.learningModeActive : true}
                    onChange={(e) => setHealingConfig({ ...healingConfig, learningModeActive: e.target.checked })}
                    name="learningModeActive"
                    inputProps={{ 'aria-label': 'Learning Mode Active' }}
                  />
                  <Typography>Learning Mode Active</Typography>
                </Box>
              </Grid>
            </Grid>
            <Button type="submit" variant="contained" startIcon={<Save />}>
              Save Healing Configuration
            </Button>
          </Form>
        </Card>
      </TabPanel>

      <TabPanel value={activeTab} index={2}>
        <Card title="Alert Configuration" loading={loading} error={error}>
          <Form
            initialValues={{
              teamsWebhookUrl: alertConfig.teamsWebhookUrl || {},
              emailConfig: alertConfig.emailConfig || {},
              alertThresholds: alertConfig.alertThresholds || {},
              enabledChannels: alertConfig.enabledChannels || { teams: true, email: true },
            }}
            validationSchema={alertConfigSchema}
            onSubmit={handleAlertConfigSubmit}
          >
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Typography variant="h6">Teams Webhook URL</Typography>
                <Input label="Teams Webhook URL" name="teamsWebhookUrl" value={alertConfig.teamsWebhookUrl?.url || ''} onChange={(value) => setAlertConfig({ ...alertConfig, teamsWebhookUrl: { url: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6">Email Configuration</Typography>
                <Input label="Email Configuration" name="emailConfig" value={alertConfig.emailConfig?.email || ''} onChange={(value) => setAlertConfig({ ...alertConfig, emailConfig: { email: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6">Alert Thresholds</Typography>
                <Input label="Alert Thresholds" name="alertThresholds" value={alertConfig.alertThresholds?.threshold || ''} onChange={(value) => setAlertConfig({ ...alertConfig, alertThresholds: { threshold: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6">Enabled Channels</Typography>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={alertConfig.enabledChannels?.teams !== undefined ? alertConfig.enabledChannels.teams : true}
                    onChange={(e) => setAlertConfig({ ...alertConfig, enabledChannels: { ...alertConfig.enabledChannels, teams: e.target.checked } })}
                    name="teamsEnabled"
                    inputProps={{ 'aria-label': 'Teams Enabled' }}
                  />
                  <Typography>Teams</Typography>
                </Box>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={alertConfig.enabledChannels?.email !== undefined ? alertConfig.enabledChannels.email : true}
                    onChange={(e) => setAlertConfig({ ...alertConfig, enabledChannels: { ...alertConfig.enabledChannels, email: e.target.checked } })}
                    name="emailEnabled"
                    inputProps={{ 'aria-label': 'Email Enabled' }}
                  />
                  <Typography>Email</Typography>
                </Box>
              </Grid>
            </Grid>
            <Button type="submit" variant="contained" startIcon={<Save />}>
              Save Alert Configuration
            </Button>
          </Form>
        </Card>
      </TabPanel>

      <TabPanel value={activeTab} index={3}>
        <Card title="Optimization Configuration" loading={loading} error={error}>
          <Form
            initialValues={{
              queryOptimizationSettings: optimizationConfig.queryOptimizationSettings || {},
              schemaOptimizationSettings: optimizationConfig.schemaOptimizationSettings || {},
              resourceOptimizationSettings: optimizationConfig.resourceOptimizationSettings || {},
              autoImplementationEnabled: optimizationConfig.autoImplementationEnabled !== undefined ? optimizationConfig.autoImplementationEnabled : false,
            }}
            validationSchema={optimizationConfigSchema}
            onSubmit={handleOptimizationConfigSubmit}
          >
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Typography variant="h6">Query Optimization Settings</Typography>
                <Input label="Query Optimization Settings" name="queryOptimizationSettings" value={optimizationConfig.queryOptimizationSettings?.settings || ''} onChange={(value) => setOptimizationConfig({ ...optimizationConfig, queryOptimizationSettings: { settings: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6">Schema Optimization Settings</Typography>
                <Input label="Schema Optimization Settings" name="schemaOptimizationSettings" value={optimizationConfig.schemaOptimizationSettings?.settings || ''} onChange={(value) => setOptimizationConfig({ ...optimizationConfig, schemaOptimizationSettings: { settings: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6">Resource Optimization Settings</Typography>
                <Input label="Resource Optimization Settings" name="resourceOptimizationSettings" value={optimizationConfig.resourceOptimizationSettings?.settings || ''} onChange={(value) => setOptimizationConfig({ ...optimizationConfig, resourceOptimizationSettings: { settings: value } })} />
              </Grid>
              <Grid item xs={12}>
                <Box display="flex" alignItems="center">
                  <Switch
                    checked={optimizationConfig.autoImplementationEnabled !== undefined ? optimizationConfig.autoImplementationEnabled : false}
                    onChange={(e) => setOptimizationConfig({ ...optimizationConfig, autoImplementationEnabled: e.target.checked })}
                    name="autoImplementationEnabled"
                    inputProps={{ 'aria-label': 'Auto Implementation Enabled' }}
                  />
                  <Typography>Auto Implementation Enabled</Typography>
                </Box>
              </Grid>
            </Grid>
            <Button type="submit" variant="contained" startIcon={<Save />}>
              Save Optimization Configuration
            </Button>
          </Form>
        </Card>
      </TabPanel>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={closeNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={closeNotification} severity={notification.type} sx={{ width: '100%' }}>
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default SystemSettings;