import React, { useState, useEffect } from 'react'; // react ^18.2.0
import { useNavigate, useLocation } from 'react-router-dom'; // react-router-dom ^6.8.0
import { Box, Container, Paper, Typography, Grid } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import LoginForm from '../components/authentication/LoginForm';
import { useAuth } from '../contexts/AuthContext';
import ForgotPasswordForm from '../components/authentication/ForgotPasswordForm';

// Styled component for the login container
const LoginContainer = styled(Box)(({ theme }) => ({
  height: '100vh',
  backgroundImage: 'url(/images/login-bg.jpg)', // Ensure the path is correct
  backgroundSize: 'cover',
  backgroundPosition: 'center',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
}));

// Styled component for the login form container
const LoginPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(4),
  borderRadius: theme.spacing(1),
}));

// Styled component for the logo container
const LogoContainer = styled(Box)(({ theme }) => ({
  textAlign: 'center',
  marginBottom: theme.spacing(3),
}));

// Styled component for the application title
const AppTitle = styled(Typography)(({ theme }) => ({
  fontWeight: theme.typography.fontWeightBold,
  fontSize: theme.typography.h4.fontSize,
}));

/**
 * Login page component that renders the authentication interface
 * @returns Rendered login page with form and branding
 */
const Login: React.FC = () => {
  // Initialize state for showing forgot password form
  const [showForgotPassword, setShowForgotPassword] = useState(false);

  // Get authentication state from useAuth hook
  const { isAuthenticated } = useAuth();

  // Get navigation function from useNavigate hook
  const navigate = useNavigate();

  // Get location state from useLocation hook
  const location = useLocation();

  // Check if there's a redirect URL in location state
  const redirectUrl = location.state?.from?.pathname || '/dashboard';

  // Set up effect to redirect authenticated users
  useEffect(() => {
    if (isAuthenticated) {
      // Handle successful login by navigating to redirect URL or dashboard
      navigate(redirectUrl, { replace: true });
    }
  }, [isAuthenticated, navigate, redirectUrl]);

  // Handle toggling between login and forgot password forms
  const handleForgotPasswordClick = () => {
    setShowForgotPassword(true);
  };

  // Handle toggling back to the login form
  const handleBackToLogin = () => {
    setShowForgotPassword(false);
  };

  return (
    // Render login container with appropriate styling
    <LoginContainer>
      <Container maxWidth="sm">
        <Grid container justifyContent="center">
          <Grid item xs={12}>
            {/* Render LoginPaper with elevation and styling */}
            <LoginPaper elevation={3}>
              {/* Render LogoContainer with application logo */}
              <LogoContainer>
                {/* Application logo (replace with actual logo component) */}
                <img src="/images/logo.png" alt="Self-Healing Pipeline Logo" width="150" />
              </LogoContainer>

              {/* Render AppTitle with application name */}
              <AppTitle variant="h5" align="center" gutterBottom>
                Self-Healing Data Pipeline
              </AppTitle>

              {/* Conditionally render LoginForm or ForgotPasswordForm based on showForgotPassword state */}
              {!showForgotPassword ? (
                <LoginForm
                  onForgotPassword={handleForgotPasswordClick}
                  onSuccess={() => navigate(redirectUrl, { replace: true })}
                  redirectUrl={redirectUrl}
                />
              ) : (
                <ForgotPasswordForm
                  onSuccess={handleBackToLogin}
                  onCancel={handleBackToLogin}
                />
              )}
            </LoginPaper>
          </Grid>
        </Grid>
      </Container>
    </LoginContainer>
  );
};

// Export the Login component as the default export
export default Login;