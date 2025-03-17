/**
 * Login form component for the self-healing data pipeline application
 * Provides user authentication with username/password functionality, validation,
 * error handling, and support for multi-factor authentication flow.
 */
import React, { useState } from 'react'; // react ^18.2.0
import { 
  Box, 
  Typography, 
  Paper, 
  Checkbox, 
  FormControlLabel, 
  Divider 
} from '@mui/material'; // @mui/material ^5.11.0
import { 
  Person, 
  Lock, 
  Visibility, 
  VisibilityOff 
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11
import { useAuth } from '../../hooks/useAuth';
import { LoginCredentials, MfaRequest } from '../../types/user';
import Button from '../common/Button';
import Input from '../common/Input';
import Alert from '../common/Alert';
import Form from '../common/Form';

/**
 * Props interface for the LoginForm component
 */
interface LoginFormProps {
  /** Callback function when user clicks forgot password link */
  onForgotPassword?: () => void;
  
  /** Callback function when login is successful */
  onSuccess?: () => void;
  
  /** URL to redirect to after successful login */
  redirectUrl?: string;
}

/**
 * Validates the login form fields
 * @param values - Form values
 * @returns Validation errors if any
 */
const validateLoginForm = (values: { username: string; password: string }) => {
  const errors: { username?: string; password?: string } = {};
  
  if (!values.username) {
    errors.username = 'Username is required';
  }
  
  if (!values.password) {
    errors.password = 'Password is required';
  }
  
  return errors;
};

/**
 * LoginForm component for user authentication
 */
const LoginForm: React.FC<LoginFormProps> = ({ 
  onForgotPassword, 
  onSuccess,
  redirectUrl 
}) => {
  // Form state
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [rememberMe, setRememberMe] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  
  // Auth state
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // MFA state
  const [mfaRequired, setMfaRequired] = useState(false);
  const [mfaToken, setMfaToken] = useState('');
  const [verificationCode, setVerificationCode] = useState('');
  
  // Authentication hook
  const { login, verifyMfa } = useAuth();
  
  // Input handlers
  const handleUsernameChange = (value: string) => {
    setUsername(value);
    if (error) setError(null);
  };
  
  const handlePasswordChange = (value: string) => {
    setPassword(value);
    if (error) setError(null);
  };
  
  const handleRememberMeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setRememberMe(e.target.checked);
  };
  
  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };
  
  const handleVerificationCodeChange = (value: string) => {
    setVerificationCode(value);
    if (error) setError(null);
  };
  
  // Login submission handler
  const handleLogin = async () => {
    // Validate form fields
    if (!username) {
      setError('Username is required');
      return;
    }
    
    if (!password) {
      setError('Password is required');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      const credentials: LoginCredentials = {
        username,
        password,
        rememberMe
      };
      
      const response = await login(credentials);
      
      if (response.requiresMfa) {
        // If MFA is required, set MFA state
        setMfaRequired(true);
        setMfaToken(response.mfaToken || '');
      } else {
        // If login successful and no MFA required
        if (onSuccess) {
          onSuccess();
        } else if (redirectUrl) {
          window.location.href = redirectUrl;
        }
      }
    } catch (err: any) {
      // Handle login error
      setError(err.message || 'Failed to login. Please check your credentials.');
    } finally {
      setLoading(false);
    }
  };
  
  // MFA verification handler
  const handleMfaVerification = async () => {
    // Validate verification code
    if (!verificationCode) {
      setError('Verification code is required');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      const mfaRequest: MfaRequest = {
        mfaToken,
        verificationCode
      };
      
      await verifyMfa(mfaRequest);
      
      // If MFA verification successful
      if (onSuccess) {
        onSuccess();
      } else if (redirectUrl) {
        window.location.href = redirectUrl;
      }
    } catch (err: any) {
      // Handle MFA verification error
      setError(err.message || 'Failed to verify MFA code. Please try again.');
    } finally {
      setLoading(false);
    }
  };
  
  // Render MFA verification form if MFA is required
  if (mfaRequired) {
    return (
      <Box component={Paper} sx={styles.formContainer}>
        <Typography variant="h5" sx={styles.formTitle}>
          Verification Required
        </Typography>
        
        <Typography variant="body2" sx={styles.mfaInstructions}>
          A verification code has been sent to your authentication app.
          Please enter the code to continue.
        </Typography>
        
        <Box sx={styles.formFields}>
          <Input
            label="Verification Code"
            value={verificationCode}
            onChange={handleVerificationCodeChange}
            placeholder="Enter 6-digit code"
            type="text"
            fullWidth
            required
            autoFocus
            error={!!error}
          />
        </Box>
        
        <Button
          fullWidth
          loading={loading}
          onClick={handleMfaVerification}
          sx={styles.submitButton}
        >
          Verify
        </Button>
        
        {error && (
          <Alert 
            severity="error" 
            sx={styles.errorAlert}
          >
            {error}
          </Alert>
        )}
      </Box>
    );
  }
  
  // Render login form
  return (
    <Box component={Paper} sx={styles.formContainer}>
      <Typography variant="h5" sx={styles.formTitle}>
        Sign In
      </Typography>
      
      <Box sx={styles.formFields}>
        <Input
          label="Username"
          value={username}
          onChange={handleUsernameChange}
          placeholder="Enter your username"
          type="text"
          fullWidth
          required
          startAdornment={<Person />}
          autoFocus
          error={!!error}
        />
        
        <Input
          label="Password"
          value={password}
          onChange={handlePasswordChange}
          placeholder="Enter your password"
          type={showPassword ? 'text' : 'password'}
          fullWidth
          required
          startAdornment={<Lock />}
          endAdornment={
            <div onClick={togglePasswordVisibility} style={{ cursor: 'pointer' }}>
              {showPassword ? <VisibilityOff /> : <Visibility />}
            </div>
          }
          error={!!error}
        />
      </Box>
      
      <Box sx={styles.rememberMeContainer}>
        <FormControlLabel
          control={
            <Checkbox 
              checked={rememberMe} 
              onChange={handleRememberMeChange}
              color="primary"
            />
          }
          label="Remember me"
        />
        
        {onForgotPassword && (
          <Typography 
            variant="body2" 
            onClick={onForgotPassword} 
            sx={styles.forgotPasswordLink}
          >
            Forgot password?
          </Typography>
        )}
      </Box>
      
      <Button
        fullWidth
        loading={loading}
        onClick={handleLogin}
        sx={styles.submitButton}
      >
        Sign In
      </Button>
      
      {error && (
        <Alert 
          severity="error" 
          sx={styles.errorAlert}
        >
          {error}
        </Alert>
      )}
    </Box>
  );
};

// Component styles
const styles = {
  formContainer: {
    maxWidth: '400px',
    margin: '0 auto',
    padding: '24px',
    borderRadius: '8px',
  },
  formTitle: {
    marginBottom: '24px',
    textAlign: 'center',
    fontWeight: '500',
  },
  formFields: {
    marginBottom: '16px',
  },
  rememberMeContainer: {
    marginTop: '8px',
    marginBottom: '16px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  forgotPasswordLink: {
    cursor: 'pointer',
    color: 'primary.main',
    fontSize: '0.875rem',
  },
  submitButton: {
    marginTop: '16px',
    width: '100%',
  },
  errorAlert: {
    marginTop: '16px',
  },
  mfaInstructions: {
    marginBottom: '24px',
    textAlign: 'center',
    fontSize: '0.875rem',
    color: 'text.secondary',
  },
};

export default LoginForm;