import React, { Component, ErrorInfo } from 'react'; // ^18.2.0
import { Box, Typography, Button } from '@mui/material'; // ^5.11.0
import { ErrorOutline } from '@mui/icons-material'; // ^5.11.0
import Alert from './Alert';
import { logError } from '../../utils/errorHandling';

/**
 * Props for the ErrorBoundary component
 */
interface ErrorBoundaryProps {
  /** Child components to be rendered and monitored for errors */
  children: React.ReactNode;
  /** Optional custom fallback UI to display when an error occurs */
  fallback?: React.ReactNode;
  /** Optional callback function to be called when an error is caught */
  onError?: (error: Error, errorInfo: ErrorInfo) => void;
}

/**
 * State for the ErrorBoundary component
 */
interface ErrorBoundaryState {
  /** Indicates whether an error has been caught */
  hasError: boolean;
  /** The error that was caught, or null if no error */
  error: Error | null;
  /** React error info object containing component stack trace */
  errorInfo: ErrorInfo | null;
}

/**
 * Class component that implements React's error boundary functionality
 * 
 * This component catches JavaScript errors in its child component tree,
 * logs those errors, and displays a fallback UI instead of crashing
 * the entire application.
 */
class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null
    };
  }

  /**
   * Static lifecycle method called when an error is thrown in a child component
   */
  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    // Update state so the next render will show the fallback UI
    return { hasError: true, error };
  }

  /**
   * Lifecycle method called after an error has been thrown by a descendant component
   */
  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    // Update state with error info
    this.setState({
      errorInfo
    });

    // Log the error with context
    logError(error, 'ErrorBoundary');

    // Call the onError prop if provided
    if (this.props.onError) {
      this.props.onError(error, errorInfo);
    }
  }

  /**
   * Method to reset the error state and attempt to re-render the children
   */
  resetError = (): void => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null
    });
  };

  /**
   * Method to render the default fallback UI when an error occurs
   */
  renderFallbackUI = (): JSX.Element => {
    const { error, errorInfo } = this.state;

    return (
      <Box
        role="alert"
        aria-live="assertive"
        sx={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: { xs: 2, sm: 3 },
          margin: { xs: 1, sm: 2 },
          textAlign: 'center',
          maxWidth: '100%'
        }}
      >
        <ErrorOutline color="error" sx={{ fontSize: { xs: 48, sm: 64 }, mb: 2 }} />
        
        <Typography variant="h5" color="error" gutterBottom>
          Something went wrong
        </Typography>
        
        <Alert 
          severity="error"
          title="Component Error" 
          sx={{ width: '100%', mb: 2 }}
        >
          {error?.message || 'An unexpected error occurred'}
        </Alert>
        
        {process.env.NODE_ENV === 'development' && errorInfo && (
          <Box 
            sx={{ 
              mt: 2, 
              p: 2, 
              bgcolor: 'background.paper', 
              borderRadius: 1,
              maxHeight: '200px',
              overflow: 'auto',
              width: '100%',
              textAlign: 'left'
            }}
          >
            <Typography variant="subtitle2" gutterBottom>
              Component Stack Trace:
            </Typography>
            <Typography 
              variant="body2" 
              component="pre" 
              sx={{ 
                fontSize: '0.8rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word'
              }}
            >
              {errorInfo.componentStack}
            </Typography>
          </Box>
        )}
        
        <Button 
          variant="contained" 
          color="primary" 
          onClick={this.resetError}
          sx={{ mt: 3 }}
          aria-label="Try to recover from error"
        >
          Try Again
        </Button>
      </Box>
    );
  };

  render() {
    const { hasError } = this.state;
    const { children, fallback } = this.props;

    if (hasError) {
      // Render custom fallback UI if provided, otherwise render default
      return fallback || this.renderFallbackUI();
    }

    // If no error, render children normally
    return children;
  }
}

export default ErrorBoundary;