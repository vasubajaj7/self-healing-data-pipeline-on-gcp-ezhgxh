import React, { useState } from 'react';
import { Box, Typography, Paper } from '@mui/material';
import { Email } from '@mui/icons-material';
import * as yup from 'yup';

import authService from '../../services/api/authService';
import { PasswordResetRequest } from '../../types/user';
import Button from '../common/Button';
import Input from '../common/Input';
import Alert from '../common/Alert';
import Form from '../common/Form';

/**
 * Props interface for the ForgotPasswordForm component
 */
interface ForgotPasswordFormProps {
  /**
   * Callback function when password reset request is successful
   */
  onSuccess?: () => void;
  /**
   * Callback function when user cancels the password reset process
   */
  onCancel?: () => void;
}

/**
 * Form values for the password reset form
 */
interface ResetFormValues {
  email: string;
}

/**
 * Validates the password reset form fields
 */
const validateResetForm = (values: object) => {
  const errors: Record<string, string> = {};
  
  if (!values.email) {
    errors.email = 'Email is required';
  } else if (!/^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$/i.test(values.email)) {
    errors.email = 'Please enter a valid email address';
  }
  
  return errors;
};

/**
 * Form component for requesting a password reset
 * 
 * This component allows users to request a password reset by entering their email address.
 * It displays appropriate feedback for success or error states and includes validation.
 */
const ForgotPasswordForm: React.FC<ForgotPasswordFormProps> = ({ onSuccess, onCancel }) => {
  // State management
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  /**
   * Updates email state when input value changes
   */
  const handleEmailChange = (value: string) => {
    if (error) setError(null);
  };

  /**
   * Handles form submission for password reset request
   */
  const handleSubmit = async (values: ResetFormValues) => {
    setLoading(true);
    setError(null);
    
    try {
      const resetRequest: PasswordResetRequest = {
        email: values.email
      };
      
      await authService.requestPasswordReset(resetRequest);
      setSuccess(true);
      if (onSuccess) onSuccess();
    } catch (err: any) {
      // Use error message if available, or a generic message
      setError(err.message || 'Failed to send password reset request. Please try again later.');
    } finally {
      setLoading(false);
    }
  };

  /**
   * Handles cancellation of password reset process
   */
  const handleCancel = () => {
    if (onCancel) onCancel();
  };

  // Render success state if the reset email was sent
  if (success) {
    return (
      <Paper sx={{
        maxWidth: '400px',
        margin: '0 auto',
        padding: '24px',
        borderRadius: '8px'
      }}>
        <Typography variant="h6" sx={{
          marginBottom: '16px',
          textAlign: 'center',
          fontWeight: '500'
        }}>
          Password Reset Email Sent
        </Typography>
        <Typography variant="body1" sx={{
          textAlign: 'center',
          marginBottom: '24px'
        }}>
          If an account exists with the email you entered, you will receive a password reset link shortly.
          Please check your email and follow the instructions to reset your password.
        </Typography>
        <Button onClick={handleCancel} fullWidth>
          Return to Login
        </Button>
      </Paper>
    );
  }

  // Render the password reset request form
  return (
    <Paper sx={{
      maxWidth: '400px',
      margin: '0 auto',
      padding: '24px',
      borderRadius: '8px'
    }}>
      <Typography variant="h6" sx={{
        marginBottom: '16px',
        textAlign: 'center',
        fontWeight: '500'
      }}>
        Forgot Password
      </Typography>
      <Typography variant="body2" sx={{
        marginBottom: '24px',
        textAlign: 'center',
        fontSize: '0.875rem',
        color: 'text.secondary'
      }}>
        Enter your email address below and we'll send you a link to reset your password.
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ marginBottom: '16px' }}>
          {error}
        </Alert>
      )}
      
      <Form
        initialValues={{ email: '' }}
        validationSchema={yup.object({
          email: yup.string().email('Please enter a valid email address').required('Email is required')
        })}
        onSubmit={handleSubmit}
      >
        {(formik) => (
          <>
            <Box sx={{ marginBottom: '16px' }}>
              <Input
                label="Email Address"
                value={formik.values.email}
                onChange={(value) => {
                  formik.setFieldValue('email', value);
                  handleEmailChange(value);
                }}
                onBlur={formik.handleBlur('email')}
                error={Boolean(formik.touched.email && formik.errors.email)}
                helperText={formik.touched.email && formik.errors.email ? String(formik.errors.email) : undefined}
                type="email"
                fullWidth
                required
                startAdornment={<Email />}
                disabled={loading}
              />
            </Box>
            
            <Box sx={{
              display: 'flex',
              justifyContent: 'space-between',
              marginTop: '24px',
              gap: '16px'
            }}>
              <Button
                type="submit"
                disabled={loading}
                loading={loading}
                sx={{ flexGrow: 1 }}
              >
                Send Reset Link
              </Button>
              <Button
                onClick={handleCancel}
                variant="outlined"
                disabled={loading}
                sx={{ flexGrow: 1 }}
              >
                Cancel
              </Button>
            </Box>
          </>
        )}
      </Form>
    </Paper>
  );
};

export default ForgotPasswordForm;