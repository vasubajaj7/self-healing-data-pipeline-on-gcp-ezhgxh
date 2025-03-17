import React, { useState, useEffect } from 'react';
import {
  Grid,
  Typography,
  Button,
  Switch,
  TextField,
  Tabs,
  Tab,
  Box,
  Avatar,
  Divider,
  Alert,
  CircularProgress,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions
} from '@mui/material';
import {
  PersonOutline,
  Security,
  Notifications,
  Palette,
  VpnKey,
  QrCode2
} from '@mui/icons-material';
import * as yup from 'yup';
import { useAuth } from '../../contexts/AuthContext';
import { useThemeContext } from '../../contexts/ThemeContext';
import Card from '../common/Card';
import Form from '../common/Form';
import { UserProfile as UserProfileType, UserPreferencesUpdateRequest, PasswordUpdateRequest, UserNotificationPreferences } from '../../types/user';
import authService from '../../services/api/authService';

/**
 * State interface for the UserProfile component
 */
interface UserProfileState {
  activeTab: number;
  loading: boolean;
  error: string | null;
  success: string | null;
  mfaSetupData: { qrCodeUrl: string; secret: string } | null;
  mfaDialogOpen: boolean;
  verificationCode: string;
}

/**
 * Form values for profile update form
 */
interface ProfileFormValues {
  firstName: string;
  lastName: string;
  email: string;
  jobTitle: string;
  department: string;
}

/**
 * Form values for password update form
 */
interface PasswordFormValues {
  currentPassword: string;
  newPassword: string;
  confirmPassword: string;
}

/**
 * Validation schema for profile form
 */
const profileValidationSchema = yup.object({
  firstName: yup.string().required('First name is required'),
  lastName: yup.string().required('Last name is required'),
  email: yup.string().email('Invalid email format').required('Email is required'),
  jobTitle: yup.string(),
  department: yup.string()
});

/**
 * Validation schema for password form
 */
const passwordValidationSchema = yup.object({
  currentPassword: yup.string().required('Current password is required'),
  newPassword: yup.string()
    .min(8, 'Password must be at least 8 characters')
    .required('New password is required'),
  confirmPassword: yup.string()
    .oneOf([yup.ref('newPassword')], 'Passwords must match')
    .required('Please confirm your password')
});

/**
 * UserProfile component displays and allows editing of the user's profile information
 * including personal details, security settings, notification preferences, and app preferences.
 */
const UserProfile: React.FC = () => {
  // Get user data from auth context
  const { user, getUserProfile } = useAuth();
  
  // Get theme context for theme preferences
  const { mode, toggleTheme } = useThemeContext();

  // Component state
  const [state, setState] = useState<UserProfileState>({
    activeTab: 0,
    loading: false,
    error: null,
    success: null,
    mfaSetupData: null,
    mfaDialogOpen: false,
    verificationCode: ''
  });

  // Load user profile data when component mounts
  useEffect(() => {
    if (user) {
      getUserProfile().catch(error => {
        console.error('Error fetching user profile:', error);
        setState(prev => ({
          ...prev,
          error: 'Failed to load user profile data'
        }));
      });
    }
  }, [getUserProfile, user]);

  /**
   * Handles tab change
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setState(prev => ({
      ...prev,
      activeTab: newValue,
      error: null,
      success: null
    }));
  };

  /**
   * Clears notifications after a delay
   */
  const clearNotifications = () => {
    setTimeout(() => {
      setState(prev => ({
        ...prev,
        error: null,
        success: null
      }));
    }, 5000);
  };

  /**
   * Handles profile update submission
   */
  const handleProfileUpdate = async (values: UserPreferencesUpdateRequest, helpers: any) => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      // In a real app, you would call an API to update the profile
      // For now, we'll simulate a successful update
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Refresh user data
      await getUserProfile();

      setState(prev => ({
        ...prev,
        loading: false,
        success: 'Profile updated successfully'
      }));

      clearNotifications();
    } catch (error) {
      console.error('Error updating profile:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to update profile. Please try again.'
      }));
      clearNotifications();
    } finally {
      helpers.setSubmitting(false);
    }
  };

  /**
   * Handles password update submission
   */
  const handlePasswordUpdate = async (values: PasswordUpdateRequest, helpers: any) => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      await authService.updatePassword({
        currentPassword: values.currentPassword,
        newPassword: values.newPassword
      });

      setState(prev => ({
        ...prev,
        loading: false,
        success: 'Password updated successfully'
      }));

      helpers.resetForm();
      clearNotifications();
    } catch (error) {
      console.error('Error updating password:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to update password. Please ensure your current password is correct.'
      }));
      clearNotifications();
    } finally {
      helpers.setSubmitting(false);
    }
  };

  /**
   * Initiates MFA setup process
   */
  const handleMfaSetup = async () => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      const setupData = await authService.setupMfa();

      setState(prev => ({
        ...prev,
        loading: false,
        mfaSetupData: setupData,
        mfaDialogOpen: true
      }));
    } catch (error) {
      console.error('Error setting up MFA:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to set up MFA. Please try again.'
      }));
      clearNotifications();
    }
  };

  /**
   * Verifies MFA setup with verification code
   */
  const handleMfaVerification = async (verificationCode: string) => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      const success = await authService.verifyMfaSetup(verificationCode);

      if (success) {
        // Refresh user data to update MFA status
        await getUserProfile();

        setState(prev => ({
          ...prev,
          loading: false,
          success: 'MFA enabled successfully',
          mfaDialogOpen: false,
          mfaSetupData: null,
          verificationCode: ''
        }));
      } else {
        throw new Error('Verification failed');
      }

      clearNotifications();
    } catch (error) {
      console.error('Error verifying MFA setup:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to verify MFA setup. Please check your verification code and try again.',
        verificationCode: ''
      }));
      clearNotifications();
    }
  };

  /**
   * Disables MFA for the user
   */
  const handleMfaDisable = async () => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      const success = await authService.disableMfa();

      if (success) {
        // Refresh user data to update MFA status
        await getUserProfile();

        setState(prev => ({
          ...prev,
          loading: false,
          success: 'MFA disabled successfully'
        }));
      } else {
        throw new Error('Failed to disable MFA');
      }

      clearNotifications();
    } catch (error) {
      console.error('Error disabling MFA:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to disable MFA. Please try again.'
      }));
      clearNotifications();
    }
  };

  /**
   * Updates user notification preferences
   */
  const handleNotificationPreferenceChange = async (channel: string, enabled: boolean) => {
    try {
      setState(prev => ({ ...prev, loading: true, error: null, success: null }));

      // In a real app, you would call an API to update preferences
      // For now, we'll simulate a successful update
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Create updated preferences object
      const updatedPreferences: UserNotificationPreferences = {
        ...(user?.preferences?.notifications || {
          email: true,
          teams: true,
          inApp: true,
          alertLevels: { critical: true, high: true, medium: true, low: true }
        }),
        [channel]: enabled
      };

      // Refresh user data
      await getUserProfile();

      setState(prev => ({
        ...prev,
        loading: false,
        success: 'Notification preferences updated successfully'
      }));

      clearNotifications();
    } catch (error) {
      console.error('Error updating notification preferences:', error);
      setState(prev => ({
        ...prev,
        loading: false,
        error: 'Failed to update notification preferences. Please try again.'
      }));
      clearNotifications();
    }
  };

  // Prepare initial form values for profile
  const initialProfileValues: ProfileFormValues = {
    firstName: user?.firstName || '',
    lastName: user?.lastName || '',
    email: user?.email || '',
    jobTitle: user?.jobTitle || '',
    department: user?.department || ''
  };

  // Initial form values for password
  const initialPasswordValues: PasswordFormValues = {
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  };

  return (
    <Grid container spacing={3}>
      {/* Profile Header */}
      <Grid item xs={12}>
        <Card>
          <Grid container spacing={2} alignItems="center">
            <Grid item>
              <Avatar 
                sx={{ 
                  width: 80, 
                  height: 80, 
                  bgcolor: 'primary.main' 
                }}
              >
                {user?.firstName?.charAt(0) || ''}{user?.lastName?.charAt(0) || ''}
              </Avatar>
            </Grid>
            <Grid item xs>
              <Typography variant="h5">
                {user?.firstName} {user?.lastName}
              </Typography>
              <Typography variant="body1" color="textSecondary">
                {user?.email}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                {user?.jobTitle}{user?.jobTitle && user?.department ? ' • ' : ''}
                {user?.department}
              </Typography>
            </Grid>
          </Grid>
        </Card>
      </Grid>

      {/* Success and Error Alerts */}
      {state.success && (
        <Grid item xs={12}>
          <Alert severity="success">{state.success}</Alert>
        </Grid>
      )}
      {state.error && (
        <Grid item xs={12}>
          <Alert severity="error">{state.error}</Alert>
        </Grid>
      )}

      {/* Tab Navigation and Content */}
      <Grid item xs={12}>
        <Card>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs 
              value={state.activeTab} 
              onChange={handleTabChange}
              aria-label="profile settings tabs"
            >
              <Tab 
                icon={<PersonOutline />} 
                label="Profile" 
                id="profile-tab"
                aria-controls="profile-tab-panel"
              />
              <Tab 
                icon={<Security />} 
                label="Security" 
                id="security-tab"
                aria-controls="security-tab-panel"
              />
              <Tab 
                icon={<Notifications />} 
                label="Notifications" 
                id="notifications-tab"
                aria-controls="notifications-tab-panel"
              />
              <Tab 
                icon={<Palette />} 
                label="Preferences" 
                id="preferences-tab"
                aria-controls="preferences-tab-panel"
              />
            </Tabs>
          </Box>

          {/* Profile Tab */}
          <TabPanel value={state.activeTab} index={0}>
            <Form
              initialValues={initialProfileValues}
              validationSchema={profileValidationSchema}
              onSubmit={handleProfileUpdate}
              enableReinitialize
            >
              {({ values, handleChange, handleBlur, errors, touched }) => (
                <>
                  <Grid container spacing={2}>
                    <Grid item xs={12} sm={6}>
                      <TextField
                        fullWidth
                        id="firstName"
                        name="firstName"
                        label="First Name"
                        value={values.firstName}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.firstName && Boolean(errors.firstName)}
                        helperText={touched.firstName && errors.firstName}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <TextField
                        fullWidth
                        id="lastName"
                        name="lastName"
                        label="Last Name"
                        value={values.lastName}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.lastName && Boolean(errors.lastName)}
                        helperText={touched.lastName && errors.lastName}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        id="email"
                        name="email"
                        label="Email"
                        value={values.email}
                        disabled
                        InputProps={{
                          readOnly: true,
                        }}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <TextField
                        fullWidth
                        id="jobTitle"
                        name="jobTitle"
                        label="Job Title"
                        value={values.jobTitle}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.jobTitle && Boolean(errors.jobTitle)}
                        helperText={touched.jobTitle && errors.jobTitle}
                      />
                    </Grid>
                    <Grid item xs={12} sm={6}>
                      <TextField
                        fullWidth
                        id="department"
                        name="department"
                        label="Department"
                        value={values.department}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.department && Boolean(errors.department)}
                        helperText={touched.department && errors.department}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <Button 
                        type="submit" 
                        variant="contained" 
                        color="primary"
                        disabled={state.loading}
                      >
                        {state.loading ? (
                          <CircularProgress size={24} color="inherit" />
                        ) : (
                          'Update Profile'
                        )}
                      </Button>
                    </Grid>
                  </Grid>
                </>
              )}
            </Form>
          </TabPanel>

          {/* Security Tab */}
          <TabPanel value={state.activeTab} index={1}>
            <Typography variant="h6" gutterBottom>
              Password
            </Typography>
            <Form
              initialValues={initialPasswordValues}
              validationSchema={passwordValidationSchema}
              onSubmit={handlePasswordUpdate}
            >
              {({ values, handleChange, handleBlur, errors, touched }) => (
                <>
                  <Grid container spacing={2}>
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        id="currentPassword"
                        name="currentPassword"
                        label="Current Password"
                        type="password"
                        value={values.currentPassword}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.currentPassword && Boolean(errors.currentPassword)}
                        helperText={touched.currentPassword && errors.currentPassword}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        id="newPassword"
                        name="newPassword"
                        label="New Password"
                        type="password"
                        value={values.newPassword}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.newPassword && Boolean(errors.newPassword)}
                        helperText={touched.newPassword && errors.newPassword}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        id="confirmPassword"
                        name="confirmPassword"
                        label="Confirm New Password"
                        type="password"
                        value={values.confirmPassword}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.confirmPassword && Boolean(errors.confirmPassword)}
                        helperText={touched.confirmPassword && errors.confirmPassword}
                      />
                    </Grid>
                    <Grid item xs={12}>
                      <Button 
                        type="submit" 
                        variant="contained" 
                        color="primary"
                        disabled={state.loading}
                        startIcon={<VpnKey />}
                      >
                        {state.loading ? (
                          <CircularProgress size={24} color="inherit" />
                        ) : (
                          'Update Password'
                        )}
                      </Button>
                    </Grid>
                  </Grid>
                </>
              )}
            </Form>

            <Divider sx={{ my: 3 }} />

            <Typography variant="h6" gutterBottom>
              Multi-Factor Authentication (MFA)
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Typography variant="body1">
                  {user?.mfaEnabled ? (
                    'You have enabled multi-factor authentication. This adds an extra layer of security to your account.'
                  ) : (
                    'Enable multi-factor authentication to add an extra layer of security to your account.'
                  )}
                </Typography>
              </Grid>
              <Grid item xs={12}>
                {user?.mfaEnabled ? (
                  <Button 
                    variant="outlined" 
                    color="secondary"
                    onClick={handleMfaDisable}
                    disabled={state.loading}
                  >
                    {state.loading ? (
                      <CircularProgress size={24} color="inherit" />
                    ) : (
                      'Disable MFA'
                    )}
                  </Button>
                ) : (
                  <Button 
                    variant="contained" 
                    color="primary"
                    onClick={handleMfaSetup}
                    disabled={state.loading}
                    startIcon={<QrCode2 />}
                  >
                    {state.loading ? (
                      <CircularProgress size={24} color="inherit" />
                    ) : (
                      'Set up MFA'
                    )}
                  </Button>
                )}
              </Grid>
            </Grid>
          </TabPanel>

          {/* Notifications Tab */}
          <TabPanel value={state.activeTab} index={2}>
            <Typography variant="h6" gutterBottom>
              Notification Settings
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Typography variant="body1" gutterBottom>
                  Configure how you would like to receive notifications from the application.
                </Typography>
              </Grid>
              <Grid item xs={12}>
                <Grid container alignItems="center" justifyContent="space-between">
                  <Grid item>
                    <Typography variant="body1">Email Notifications</Typography>
                    <Typography variant="body2" color="textSecondary">
                      Receive notifications via email
                    </Typography>
                  </Grid>
                  <Grid item>
                    <Switch
                      checked={user?.preferences?.notifications?.email !== false}
                      onChange={(e) => handleNotificationPreferenceChange('email', e.target.checked)}
                      color="primary"
                      disabled={state.loading}
                    />
                  </Grid>
                </Grid>
              </Grid>
              <Grid item xs={12}>
                <Divider />
              </Grid>
              <Grid item xs={12}>
                <Grid container alignItems="center" justifyContent="space-between">
                  <Grid item>
                    <Typography variant="body1">Microsoft Teams Notifications</Typography>
                    <Typography variant="body2" color="textSecondary">
                      Receive notifications via Microsoft Teams
                    </Typography>
                  </Grid>
                  <Grid item>
                    <Switch
                      checked={user?.preferences?.notifications?.teams !== false}
                      onChange={(e) => handleNotificationPreferenceChange('teams', e.target.checked)}
                      color="primary"
                      disabled={state.loading}
                    />
                  </Grid>
                </Grid>
              </Grid>
              <Grid item xs={12}>
                <Divider />
              </Grid>
              <Grid item xs={12}>
                <Grid container alignItems="center" justifyContent="space-between">
                  <Grid item>
                    <Typography variant="body1">In-App Notifications</Typography>
                    <Typography variant="body2" color="textSecondary">
                      Receive notifications within the application
                    </Typography>
                  </Grid>
                  <Grid item>
                    <Switch
                      checked={user?.preferences?.notifications?.inApp !== false}
                      onChange={(e) => handleNotificationPreferenceChange('inApp', e.target.checked)}
                      color="primary"
                      disabled={state.loading}
                    />
                  </Grid>
                </Grid>
              </Grid>
            </Grid>
          </TabPanel>

          {/* Preferences Tab */}
          <TabPanel value={state.activeTab} index={3}>
            <Typography variant="h6" gutterBottom>
              Application Preferences
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Typography variant="body1" gutterBottom>
                  Customize your application experience.
                </Typography>
              </Grid>
              <Grid item xs={12}>
                <Grid container alignItems="center" justifyContent="space-between">
                  <Grid item>
                    <Typography variant="body1">Dark Mode</Typography>
                    <Typography variant="body2" color="textSecondary">
                      Toggle between light and dark theme
                    </Typography>
                  </Grid>
                  <Grid item>
                    <Switch
                      checked={mode === 'dark'}
                      onChange={toggleTheme}
                      color="primary"
                    />
                  </Grid>
                </Grid>
              </Grid>
              <Grid item xs={12}>
                <Divider />
              </Grid>
              {/* Add more preferences here as needed */}
            </Grid>
          </TabPanel>
        </Card>
      </Grid>

      {/* MFA Setup Dialog */}
      <Dialog
        open={state.mfaDialogOpen}
        onClose={() => setState(prev => ({ ...prev, mfaDialogOpen: false, mfaSetupData: null }))}
        aria-labelledby="mfa-setup-dialog-title"
      >
        <DialogTitle id="mfa-setup-dialog-title">
          Set Up Multi-Factor Authentication
        </DialogTitle>
        <DialogContent>
          <Grid container spacing={2} direction="column" alignItems="center">
            <Grid item xs={12}>
              <Typography variant="body1" gutterBottom>
                Scan this QR code with your authenticator app (such as Google Authenticator, 
                Microsoft Authenticator, or Authy).
              </Typography>
            </Grid>
            {state.mfaSetupData?.qrCodeUrl && (
              <Grid item xs={12} sx={{ textAlign: 'center' }}>
                <Box
                  component="img"
                  src={state.mfaSetupData.qrCodeUrl}
                  alt="QR Code for MFA Setup"
                  sx={{ maxWidth: 200, height: 'auto' }}
                />
              </Grid>
            )}
            <Grid item xs={12}>
              <Typography variant="body2" gutterBottom>
                If you can't scan the QR code, enter this code manually:
              </Typography>
              <Typography variant="body1" sx={{ fontWeight: 'bold', wordBreak: 'break-all', textAlign: 'center' }}>
                {state.mfaSetupData?.secret}
              </Typography>
            </Grid>
            <Grid item xs={12} sx={{ mt: 2 }}>
              <TextField
                fullWidth
                label="Verification Code"
                placeholder="Enter the 6-digit code from your authenticator app"
                value={state.verificationCode}
                onChange={(e) => setState(prev => ({ ...prev, verificationCode: e.target.value }))}
                inputProps={{ maxLength: 6, pattern: '[0-9]*' }}
                sx={{ mt: 2 }}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => setState(prev => ({ ...prev, mfaDialogOpen: false, mfaSetupData: null }))}
            color="primary"
          >
            Cancel
          </Button>
          <Button
            onClick={() => handleMfaVerification(state.verificationCode)}
            color="primary"
            variant="contained"
            disabled={state.verificationCode.length !== 6 || state.loading}
          >
            {state.loading ? <CircularProgress size={24} /> : 'Verify'}
          </Button>
        </DialogActions>
      </Dialog>
    </Grid>
  );
};

/**
 * Tab panel component to display tab content
 */
interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`profile-tabpanel-${index}`}
      aria-labelledby={`profile-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

export default UserProfile;