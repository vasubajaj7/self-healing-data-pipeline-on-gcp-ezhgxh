import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Divider,
  Chip,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  TrendingUp,
  CheckCircle,
  Cancel,
  Info,
  Warning,
  Error as ErrorIcon,
  ArrowUpward,
  Code,
  Visibility,
  VisibilityOff,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { styled } from '@mui/material/styles';
import { format } from 'date-fns'; // date-fns ^2.30.0
import Prism from 'prismjs'; // prismjs ^1.29.0

import Card from '../common/Card';
import Button from '../common/Button';
import useApi from '../../hooks/useApi';
import optimizationService from '../../services/api/optimizationService';
import {
  OptimizationType,
  OptimizationStatus,
  ImpactLevel,
  QueryOptimizationRecommendation,
  SchemaOptimizationRecommendation,
  ResourceOptimizationRecommendation,
} from '../../types/optimization';

// Define the props for the OptimizationRecommendations component
interface OptimizationRecommendationsProps {
  type: OptimizationType;
  title?: string;
  maxItems?: number;
  status?: OptimizationStatus;
  impactLevel?: ImpactLevel;
  onRecommendationApplied?: () => void;
  onRecommendationRejected?: () => void;
  className?: string;
}

// Define the props for the RecommendationItem component
interface RecommendationItemProps {
  recommendation: QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation;
  type: OptimizationType;
  onApply: () => void;
  onReject: () => void;
}

// Define the props for the RecommendationDetailDialog component
interface RecommendationDetailDialogProps {
  open: boolean;
  onClose: () => void;
  recommendation: QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation | null;
  type: OptimizationType;
  onApply: () => void;
  onReject: () => void;
}

// Define the props for the RejectDialog component
interface RejectDialogProps {
  open: boolean;
  onClose: () => void;
  onReject: (reason: string) => void;
  recommendationId: string;
  type: OptimizationType;
}

// Styled ListItem component with hover effect and padding
const StyledListItem = styled(ListItem)(({ theme }) => ({
  padding: '12px 16px',
  borderBottom: '1px solid rgba(0, 0, 0, 0.12)',
  transition: 'background-color 0.2s',
  '&:hover': {
    backgroundColor: 'rgba(0, 0, 0, 0.04)',
  },
}));

// Styled Chip component for impact level
const ImpactChip = styled(Chip)({
  marginRight: '8px',
  fontWeight: '500',
});

// Styled Chip component for status
const StatusChip = styled(Chip)({
  marginLeft: '8px',
  fontWeight: '500',
});

// Styled component for code display
const CodeBlock = styled('pre')({
  backgroundColor: '#f5f5f5',
  padding: '12px',
  borderRadius: '4px',
  overflowX: 'auto',
  fontFamily: 'monospace',
  fontSize: '0.875rem',
  maxHeight: '300px',
  overflowY: 'auto',
});

// Styled container for before/after comparison
const ComparisonContainer = styled('div')({
  display: 'flex',
  flexDirection: 'column',
  gap: '16px',
  marginTop: '16px',
  marginBottom: '16px',
});

// Function to format a number as a currency value with dollar sign
const formatCurrency = (value: number): string => {
  return `$${value.toFixed(2)}`;
};

// Function to format a number as a percentage with sign
const formatPercentage = (value: number): string => {
  const formattedValue = (value > 0 ? '+' : '') + value.toFixed(1) + '%';
  return formattedValue;
};

// Function to return the color associated with an impact level
const getImpactColor = (impact: ImpactLevel): string => {
  switch (impact) {
    case ImpactLevel.HIGH:
      return 'error';
    case ImpactLevel.MEDIUM:
      return 'warning';
    case ImpactLevel.LOW:
      return 'info';
    default:
      return 'default';
  }
};

// Function to return the color associated with a recommendation status
const getStatusColor = (status: OptimizationStatus): string => {
  switch (status) {
    case OptimizationStatus.APPLIED:
      return 'success';
    case OptimizationStatus.REJECTED:
      return 'error';
    case OptimizationStatus.RECOMMENDED:
      return 'warning';
    default:
      return 'default';
  }
};

// Function to return the icon component associated with a recommendation status
const getStatusIcon = (status: OptimizationStatus): React.ReactNode => {
  switch (status) {
    case OptimizationStatus.APPLIED:
      return <CheckCircle />;
    case OptimizationStatus.REJECTED:
      return <Cancel />;
    case OptimizationStatus.RECOMMENDED:
      return <TrendingUp />;
    default:
      return <Info />;
  }
};

// Function to return the icon component associated with an impact level
const getImpactIcon = (impact: ImpactLevel): React.ReactNode => {
  switch (impact) {
    case ImpactLevel.HIGH:
      return <ErrorIcon />;
    case ImpactLevel.MEDIUM:
      return <Warning />;
    case ImpactLevel.LOW:
      return <Info />;
    default:
      return <Info />;
  }
};

// Main component that displays a list of optimization recommendations
const OptimizationRecommendations: React.FC<OptimizationRecommendationsProps> = ({
  type,
  title,
  maxItems = 5,
  status,
  impactLevel,
  onRecommendationApplied,
  onRecommendationRejected,
  className,
}) => {
  // Define state variables
  const [recommendations, setRecommendations] = useState<(QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation)[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [selectedRecommendation, setSelectedRecommendation] = useState<QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation | null>(null);
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
  const [rejectRecommendationId, setRejectRecommendationId] = useState('');

  // Use the useApi hook for API calls
  const { executeRequest } = useApi();

  // Callback function to load optimization recommendations based on type
  const loadRecommendations = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      let fetchedRecommendations: (QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation)[] = [];

      // Fetch recommendations based on the optimization type
      if (type === OptimizationType.QUERY) {
        const response = await executeRequest(optimizationService.getQueryOptimizationRecommendations, {
          maxItems,
          status,
          impactLevel,
        });
        fetchedRecommendations = response?.items || [];
      } else if (type === OptimizationType.SCHEMA) {
        const response = await executeRequest(optimizationService.getSchemaOptimizationRecommendations, {
          maxItems,
          status,
          impactLevel,
        });
        fetchedRecommendations = response?.items || [];
      } else if (type === OptimizationType.RESOURCE) {
        const response = await executeRequest(optimizationService.getResourceOptimizationRecommendations, {
          maxItems,
          status,
          impactLevel,
        });
        fetchedRecommendations = response?.items || [];
      }

      // Update the state with the fetched recommendations
      setRecommendations(fetchedRecommendations);
    } catch (err: any) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [type, maxItems, status, impactLevel, executeRequest]);

  // Handle viewing recommendation details
  const handleViewDetails = (recommendation: QueryOptimizationRecommendation | SchemaOptimizationRecommendation | ResourceOptimizationRecommendation) => {
    setSelectedRecommendation(recommendation);
    setDetailDialogOpen(true);
  };

  // Handle closing the detail dialog
  const handleCloseDetailDialog = () => {
    setDetailDialogOpen(false);
    setSelectedRecommendation(null);
  };

  // Handle applying a recommendation
  const handleApplyRecommendation = async (recommendationId: string) => {
    try {
      await executeRequest(optimizationService.applyOptimization, {
        recommendationId,
        optimizationType: type,
        comments: 'Applied via UI',
        customParameters: {},
      });

      // Reload recommendations after applying
      await loadRecommendations();

      // Call the onRecommendationApplied callback if provided
      onRecommendationApplied?.();
    } catch (err: any) {
      setError(err);
    }
  };

  // Handle opening the reject dialog
  const handleOpenRejectDialog = (recommendationId: string) => {
    setRejectRecommendationId(recommendationId);
    setRejectDialogOpen(true);
  };

  // Handle closing the reject dialog
  const handleCloseRejectDialog = () => {
    setRejectDialogOpen(false);
    setRejectRecommendationId('');
  };

  // Handle rejecting a recommendation with reason
  const handleRejectRecommendation = async (reason: string) => {
    try {
      await executeRequest(optimizationService.rejectOptimization, {
        recommendationId: rejectRecommendationId,
        optimizationType: type,
        rejectionReason: reason,
        additionalComments: 'Rejected via UI',
      });

      // Reload recommendations after rejecting
      await loadRecommendations();

      // Call the onRecommendationRejected callback if provided
      onRecommendationRejected?.();
    } catch (err: any) {
      setError(err);
    } finally {
      handleCloseRejectDialog();
    }
  };

  // Load recommendations when component mounts or dependencies change
  useEffect(() => {
    loadRecommendations();
  }, [loadRecommendations]);

  // Render the component
  return (
    <Card title={title || `Optimization Recommendations (${type})`} className={className}>
      {loading && <Box display="flex" justifyContent="center"><CircularProgress /></Box>}
      {error && <Alert severity="error">{error.message}</Alert>}
      {!loading && !error && recommendations.length === 0 && (
        <Typography variant="body1" color="textSecondary">
          No optimization recommendations found.
        </Typography>
      )}
      {!loading && !error && recommendations.length > 0 && (
        <List>
          {recommendations.map((recommendation, index) => (
            <RecommendationItem
              key={index}
              recommendation={recommendation}
              type={type}
              onApply={() => handleApplyRecommendation(recommendation.recommendationId)}
              onReject={() => handleOpenRejectDialog(recommendation.recommendationId)}
            />
          ))}
        </List>
      )}
      <RecommendationDetailDialog
        open={detailDialogOpen}
        onClose={handleCloseDetailDialog}
        recommendation={selectedRecommendation}
        type={type}
        onApply={handleApplyRecommendation}
        onReject={handleOpenRejectDialog}
      />
      <RejectDialog
        open={rejectDialogOpen}
        onClose={handleCloseRejectDialog}
        onReject={handleRejectRecommendation}
        recommendationId={rejectRecommendationId}
        type={type}
      />
    </Card>
  );
};

// Component that displays a single recommendation item in the list
const RecommendationItem: React.FC<RecommendationItemProps> = ({ recommendation, type, onApply, onReject }) => {
  const { description, impactLevel, estimatedImprovementPercentage, status, recommendationId } = recommendation;

  const isApplied = status === OptimizationStatus.APPLIED;
  const isRejected = status === OptimizationStatus.REJECTED;

  return (
    <StyledListItem alignItems="flex-start">
      <ListItemText
        primary={description}
        secondary={
          <React.Fragment>
            <ImpactChip
              label={`${impactLevel}`}
              color={getImpactColor(impactLevel)}
              icon={getImpactIcon(impactLevel)}
              size="small"
            />
            {estimatedImprovementPercentage && (
              <span>
                Estimated Improvement: {formatPercentage(estimatedImprovementPercentage)}
              </span>
            )}
          </React.Fragment>
        }
      />
      <ListItemSecondaryAction>
        <StatusChip
          label={status}
          color={getStatusColor(status)}
          icon={getStatusIcon(status)}
          size="small"
        />
        <Tooltip title="View Details">
          <IconButton edge="end" aria-label="details" onClick={() => {}}>
            <Visibility />
          </IconButton>
        </Tooltip>
        <Button
          size="small"
          onClick={onApply}
          disabled={isApplied || isRejected}
        >
          Apply
        </Button>
        <Button
          size="small"
          color="error"
          onClick={onReject}
          disabled={isApplied || isRejected}
        >
          Reject
        </Button>
      </ListItemSecondaryAction>
    </StyledListItem>
  );
};

// Dialog component that displays detailed information about a recommendation
const RecommendationDetailDialog: React.FC<RecommendationDetailDialogProps> = ({ open, onClose, recommendation, type, onApply, onReject }) => {
  const [showCode, setShowCode] = useState(false);

  useEffect(() => {
    if (recommendation && showCode) {
      Prism.highlightAll();
    }
  }, [recommendation, showCode]);

  if (!recommendation) {
    return null;
  }

  const { description, impactLevel, estimatedImprovementPercentage, estimatedCostReduction, originalQuery, optimizedQuery, currentSchema, recommendedSchema, implementationScript, status, recommendationId } = recommendation;

  const isApplied = status === OptimizationStatus.APPLIED;
  const isRejected = status === OptimizationStatus.REJECTED;

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{description}</DialogTitle>
      <DialogContent>
        <Box display="flex" alignItems="center" mb={2}>
          <ImpactChip
            label={`${impactLevel}`}
            color={getImpactColor(impactLevel)}
            icon={getImpactIcon(impactLevel)}
            size="small"
          />
          {estimatedImprovementPercentage && (
            <Typography variant="body2">
              Estimated Improvement: {formatPercentage(estimatedImprovementPercentage)}
            </Typography>
          )}
          {estimatedCostReduction && (
            <Typography variant="body2" ml={2}>
              Estimated Cost Reduction: {formatCurrency(estimatedCostReduction)}
            </Typography>
          )}
        </Box>
        <Divider />
        <ComparisonContainer>
          {originalQuery && optimizedQuery && (
            <>
              <Typography variant="subtitle1">Original Query</Typography>
              <CodeBlock>
                <code className="language-sql">{originalQuery}</code>
              </CodeBlock>
              <Typography variant="subtitle1">Optimized Query</Typography>
              <CodeBlock>
                <code className="language-sql">{optimizedQuery}</code>
              </CodeBlock>
            </>
          )}
          {currentSchema && recommendedSchema && (
            <>
              <Typography variant="subtitle1">Current Schema</Typography>
              <CodeBlock>
                <code className="language-json">{JSON.stringify(currentSchema, null, 2)}</code>
              </CodeBlock>
              <Typography variant="subtitle1">Recommended Schema</Typography>
              <CodeBlock>
                <code className="language-json">{JSON.stringify(recommendedSchema, null, 2)}</code>
              </CodeBlock>
            </>
          )}
          {implementationScript && (
            <>
              <Typography variant="subtitle1">Implementation Details</Typography>
              <CodeBlock>
                <code className="language-sql">{implementationScript}</code>
              </CodeBlock>
            </>
          )}
        </ComparisonContainer>
        <Button onClick={() => setShowCode(!showCode)}>
          {showCode ? <><VisibilityOff /> Hide Details</> : <><Visibility /> Show Details</>}
        </Button>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          onClick={() => onApply(recommendationId)}
          disabled={isApplied || isRejected}
        >
          Apply
        </Button>
        <Button
          color="error"
          onClick={() => onReject(recommendationId)}
          disabled={isApplied || isRejected}
        >
          Reject
        </Button>
      </DialogActions>
    </Dialog>
  );
};

// Dialog component for rejecting a recommendation with a reason
const RejectDialog: React.FC<RejectDialogProps> = ({ open, onClose, onReject, recommendationId, type }) => {
  const [reason, setReason] = useState('');
  const [error, setError] = useState('');

  // Handle changes to the reason text field
  const handleReasonChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setReason(event.target.value);
    setError('');
  };

  // Handle submitting the rejection
  const handleSubmit = () => {
    if (!reason) {
      setError('Reason is required');
      return;
    }
    onReject(reason);
  };

  return (
    <Dialog open={open} onClose={onClose}>
      <DialogTitle>Reject Recommendation</DialogTitle>
      <DialogContent>
        <Typography variant="body1" gutterBottom>
          Are you sure you want to reject this recommendation? Please provide a reason.
        </Typography>
        <TextField
          label="Reason for Rejection"
          multiline
          rows={4}
          fullWidth
          value={reason}
          onChange={handleReasonChange}
          error={!!error}
          helperText={error}
        />
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button color="error" onClick={handleSubmit} disabled={!reason}>
          Reject
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default OptimizationRecommendations;