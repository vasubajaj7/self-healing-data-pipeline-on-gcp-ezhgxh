import React, { useCallback } from 'react'; // react ^18.2.0
import { Box, Typography, Container, Paper } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0

import MainLayout from '../components/layout/MainLayout';
import Button from '../components/common/Button';
import { ROUTES } from '../routes/routes';

// Styled component for the container
const NotFoundContainer = styled(Container)({
  display: 'flex',
  flexDirection: 'column',
  justifyContent: 'center',
  alignItems: 'center',
  minHeight: '80vh',
});

// Styled component for the paper
const NotFoundPaper = styled(Paper)({
  padding: 4,
  textAlign: 'center',
  maxWidth: 600,
  boxShadow: '0px 3px 5px -1px rgba(0,0,0,0.2),0px 6px 10px 0px rgba(0,0,0,0.14),0px 1px 18px 0px rgba(0,0,0,0.12)',
});

// Styled component for the error code
const ErrorCode = styled(Typography)(({ theme }) => ({
  fontSize: '6rem',
  fontWeight: 'bold',
  color: theme.palette.primary.main,
}));

// Styled component for the button container
const ButtonContainer = styled(Box)({
  display: 'flex',
  gap: 2,
  marginTop: 3,
  justifyContent: 'center',
});

/**
 * 404 Not Found page component that displays when users navigate to non-existent routes
 */
const NotFound: React.FC = () => {
  // Initialize navigate function from useNavigate hook
  const navigate = useNavigate();

  // Create handleGoHome function to navigate to dashboard
  const handleGoHome = useCallback(() => {
    navigate(ROUTES.DASHBOARD);
  }, [navigate]);

  // Create handleGoBack function to navigate to previous page
  const handleGoBack = useCallback(() => {
    navigate(-1);
  }, [navigate]);

  // Render Container component with centered content
  return (
    <MainLayout>
      {/* Render NotFoundContainer as the main wrapper */}
      <NotFoundContainer>
        {/* Render NotFoundPaper containing the error content */}
        <NotFoundPaper>
          {/* Display large 404 error code with ErrorCode component */}
          <ErrorCode variant="h1">404</ErrorCode>
          {/* Display friendly error message explaining that the page was not found */}
          <Typography variant="h5" paragraph>
            Oops! The page you're looking for could not be found.
          </Typography>
          {/* Display additional helper text suggesting navigation options */}
          <Typography variant="body2" color="textSecondary" paragraph>
            You may have mistyped the address or the page may have been removed.
          </Typography>
          {/* Render ButtonContainer with two action buttons */}
          <ButtonContainer>
            {/* Render 'Go Back' button that calls handleGoBack when clicked */}
            <Button variant="contained" color="primary" onClick={handleGoBack}>
              Go Back
            </Button>
            {/* Render 'Go to Dashboard' button that calls handleGoHome when clicked */}
            <Button variant="outlined" color="primary" onClick={handleGoHome}>
              Go to Dashboard
            </Button>
          </ButtonContainer>
        </NotFoundPaper>
      </NotFoundContainer>
    </MainLayout>
  );
};

// Export the NotFound component as the default export
export default NotFound;