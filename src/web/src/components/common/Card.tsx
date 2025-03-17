import React from 'react';
import { Card, CardProps, CardHeader, CardContent, CardActions } from '@mui/material';
import { styled } from '@mui/material/styles';
import Skeleton from '@mui/material/Skeleton';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { lightTheme } from '../../theme/theme';

/**
 * Extended props interface for the Card component with additional functionality
 */
interface CustomCardProps extends CardProps {
  title?: string | React.ReactNode;
  subheader?: string | React.ReactNode;
  action?: React.ReactNode;
  avatar?: React.ReactNode;
  loading?: boolean;
  error?: string | Error | null;
  headerProps?: object;
  contentProps?: object;
  actionsProps?: object;
  footerActions?: React.ReactNode;
  minHeight?: number | string;
  maxHeight?: number | string;
  variant?: string;
  children?: React.ReactNode;
}

/**
 * Styled Card component with custom styling for the self-healing pipeline application
 */
const StyledCard = styled(Card, {
  shouldForwardProp: (prop) => 
    prop !== 'loading' && prop !== 'error' && prop !== 'minHeight' && prop !== 'maxHeight'
})<CustomCardProps>(({ theme, loading, error, minHeight, maxHeight }) => ({
  borderRadius: '8px',
  transition: 'box-shadow 300ms cubic-bezier(0.4, 0, 0.2, 1) 0ms',
  ...(minHeight && {
    minHeight,
  }),
  ...(maxHeight && {
    maxHeight,
    overflow: 'auto',
  }),
  ...(error && {
    border: `1px solid ${theme.palette.error.main}`,
    backgroundColor: 'rgba(244, 67, 54, 0.08)',
  }),
  ...(loading && {
    opacity: 0.7,
  }),
  '& .MuiCardHeader-root': {
    padding: '16px',
    borderBottom: `1px solid ${theme.palette.divider}`,
  },
  '& .MuiCardContent-root': {
    padding: '16px',
    height: '100%',
    overflow: 'auto',
  },
  '& .MuiCardActions-root': {
    padding: '8px 16px',
    borderTop: `1px solid ${theme.palette.divider}`,
  },
}));

/**
 * Custom Card component that extends Material-UI Card with additional functionality
 * such as loading states, error handling, and consistent styling.
 */
const CustomCard: React.FC<CustomCardProps> = ({
  title,
  subheader,
  action,
  avatar,
  loading = false,
  error = null,
  headerProps,
  contentProps,
  actionsProps,
  footerActions,
  minHeight = 'auto',
  maxHeight,
  variant = 'outlined',
  children,
  ...rest
}) => {
  // Define aria attributes for accessibility
  const ariaProps = {
    ...(loading && { 'aria-busy': true, 'aria-label': 'Loading content' }),
    ...(error && { 'aria-errormessage': error instanceof Error ? error.message : String(error) }),
  };

  // Handle loading state
  if (loading) {
    return (
      <StyledCard 
        variant={variant} 
        minHeight={minHeight} 
        maxHeight={maxHeight} 
        loading={loading} 
        {...ariaProps}
        {...rest}
      >
        {(title || subheader || action || avatar) && (
          <CardHeader
            title={<Skeleton variant="text" width="80%" height={28} />}
            subheader={subheader ? <Skeleton variant="text" width="60%" height={20} /> : undefined}
            action={action ? <Skeleton variant="rectangular" width={40} height={40} /> : undefined}
            avatar={avatar ? <Skeleton variant="circular" width={40} height={40} /> : undefined}
            {...headerProps}
          />
        )}
        <CardContent {...contentProps}>
          <Box>
            <Skeleton variant="rectangular" width="100%" height={100} sx={{ borderRadius: '4px' }} />
            <Skeleton variant="text" width="90%" height={20} sx={{ mt: 1, borderRadius: '4px' }} />
            <Skeleton variant="text" width="80%" height={20} sx={{ mt: 1, borderRadius: '4px' }} />
          </Box>
        </CardContent>
        {footerActions && (
          <CardActions {...actionsProps}>
            <Skeleton variant="rectangular" width={120} height={36} sx={{ borderRadius: '4px' }} />
          </CardActions>
        )}
      </StyledCard>
    );
  }

  // Handle error state
  if (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return (
      <StyledCard 
        variant={variant} 
        minHeight={minHeight} 
        maxHeight={maxHeight} 
        error={true} 
        {...ariaProps}
        {...rest}
      >
        {(title || subheader || action || avatar) && (
          <CardHeader
            title={title}
            subheader={subheader}
            action={action}
            avatar={avatar}
            {...headerProps}
          />
        )}
        <CardContent {...contentProps}>
          <Box sx={{ color: 'error.main' }}>
            <Typography variant="body1" color="error">
              {errorMessage}
            </Typography>
          </Box>
        </CardContent>
        {footerActions && (
          <CardActions {...actionsProps}>
            {footerActions}
          </CardActions>
        )}
      </StyledCard>
    );
  }

  // Normal state
  return (
    <StyledCard 
      variant={variant} 
      minHeight={minHeight} 
      maxHeight={maxHeight} 
      {...ariaProps}
      {...rest}
    >
      {(title || subheader || action || avatar) && (
        <CardHeader
          title={title}
          subheader={subheader}
          action={action}
          avatar={avatar}
          {...headerProps}
        />
      )}
      <CardContent {...contentProps}>
        {children}
      </CardContent>
      {footerActions && (
        <CardActions {...actionsProps}>
          {footerActions}
        </CardActions>
      )}
    </StyledCard>
  );
};

export default CustomCard;