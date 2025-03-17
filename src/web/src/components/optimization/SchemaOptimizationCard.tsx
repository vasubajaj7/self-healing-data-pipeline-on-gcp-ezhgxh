import React, { useState, useEffect, useCallback } from 'react';
import { 
  Box, 
  Typography, 
  Chip, 
  Button, 
  Divider, 
  TextField, 
  Dialog, 
  DialogTitle, 
  DialogContent, 
  DialogActions, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  IconButton, 
  Tooltip, 
  CircularProgress, 
  Alert, 
  Collapse 
} from '@mui/material';
import { 
  Storage, 
  Check, 
  Close, 
  ExpandMore, 
  ExpandLess, 
  Info, 
  Code, 
  ThumbUp, 
  ThumbDown 
} from '@mui/icons-material';
import { format } from 'date-fns';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { sql } from 'react-syntax-highlighter/dist/esm/languages/prism';
import { materialDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

import Card from '../common/Card';
import optimizationService from '../../services/api/optimizationService';
import useApi from '../../hooks/useApi';
import { 
  OptimizationType, 
  SchemaOptimizationType, 
  OptimizationStatus, 
  ImpactLevel, 
  SchemaOptimizationRecommendation 
} from '../../types/optimization';

/**
 * Props for the SchemaOptimizationCard component
 */
interface SchemaOptimizationCardProps {
  className?: string;
  title?: string;
  maxHeight?: string | number;
  onRecommendationApplied?: () => void;
  onRecommendationRejected?: () => void;
}

/**
 * Props for the RecommendationItem component
 */
interface RecommendationItemProps {
  recommendation: SchemaOptimizationRecommendation;
  onApply: (id: string) => Promise<void>;
  onReject: (id: string, reason: string) => Promise<void>;
}

/**
 * Filter options for schema optimization recommendations
 */
interface FilterOptions {
  optimizationType: SchemaOptimizationType | '';
  status: OptimizationStatus | '';
  impactLevel: ImpactLevel | '';
}

/**
 * Formats a number as a currency value with dollar sign
 */
const formatCurrency = (value: number): string => {
  return `$${value.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}`;
};

/**
 * Formats a number as a percentage with sign
 */
const formatPercentage = (value: number): string => {
  return `${value.toFixed(1)}%`;
};

/**
 * Formats a timestamp into a readable date string
 */
const formatDate = (timestamp: string): string => {
  return format(new Date(timestamp), 'MMM dd, yyyy HH:mm');
};

/**
 * A component that displays BigQuery table schema optimization recommendations
 * with options to apply or reject them.
 */
const SchemaOptimizationCard: React.FC<SchemaOptimizationCardProps> = ({ 
  className, 
  title = 'Schema Optimization Recommendations', 
  maxHeight,
  onRecommendationApplied,
  onRecommendationRejected
}) => {
  // State management
  const [recommendations, setRecommendations] = useState<SchemaOptimizationRecommendation[]>([]);
  const [filters, setFilters] = useState<FilterOptions>({
    optimizationType: '',
    status: '',
    impactLevel: ''
  });

  // API call hook
  const { get, loading, error } = useApi();

  // Load recommendations from API
  const loadRecommendations = useCallback(async () => {
    try {
      const response = await get(
        optimizationService.getSchemaOptimizationRecommendations({
          optimizationType: filters.optimizationType || undefined,
          status: filters.status || undefined,
          impactLevel: filters.impactLevel || undefined
        })
      );
      setRecommendations(response.items);
    } catch (err) {
      console.error('Failed to load schema optimization recommendations:', err);
    }
  }, [get, filters]);

  // Load recommendations when component mounts or filters change
  useEffect(() => {
    loadRecommendations();
  }, [loadRecommendations]);

  // Handle filter changes
  const handleFilterChange = (event: React.ChangeEvent<{ name?: string; value: unknown }>) => {
    const name = event.target.name as keyof FilterOptions;
    const value = event.target.value as string;
    
    setFilters(prev => ({
      ...prev,
      [name]: value
    }));
  };

  // Apply a recommendation
  const handleApplyRecommendation = async (id: string) => {
    try {
      await optimizationService.applyOptimization({
        recommendationId: id,
        optimizationType: OptimizationType.SCHEMA
      });
      
      // Update the recommendation status in the state
      setRecommendations(prev => 
        prev.map(rec => 
          rec.recommendationId === id 
            ? { ...rec, status: OptimizationStatus.APPLIED, implementedAt: new Date().toISOString() } 
            : rec
        )
      );
      
      // Call the onRecommendationApplied callback if provided
      if (onRecommendationApplied) {
        onRecommendationApplied();
      }
    } catch (err) {
      console.error('Failed to apply schema optimization:', err);
    }
  };

  // Reject a recommendation
  const handleRejectRecommendation = async (id: string, reason: string) => {
    try {
      await optimizationService.rejectOptimization({
        recommendationId: id,
        optimizationType: OptimizationType.SCHEMA,
        rejectionReason: reason
      });
      
      // Update the recommendation status in the state
      setRecommendations(prev => 
        prev.map(rec => 
          rec.recommendationId === id 
            ? { 
                ...rec, 
                status: OptimizationStatus.REJECTED, 
                rejectedAt: new Date().toISOString(),
                rejectionReason: reason
              } 
            : rec
        )
      );
      
      // Call the onRecommendationRejected callback if provided
      if (onRecommendationRejected) {
        onRecommendationRejected();
      }
    } catch (err) {
      console.error('Failed to reject schema optimization:', err);
    }
  };

  return (
    <Card 
      title={title}
      maxHeight={maxHeight}
      className={className}
      loading={loading}
      error={error?.error?.message}
      action={
        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel id="optimization-type-label">Type</InputLabel>
            <Select
              labelId="optimization-type-label"
              id="optimization-type"
              name="optimizationType"
              value={filters.optimizationType}
              label="Type"
              onChange={handleFilterChange}
            >
              <MenuItem value="">All Types</MenuItem>
              {Object.values(SchemaOptimizationType).map((type) => (
                <MenuItem key={type} value={type}>{type}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel id="status-label">Status</InputLabel>
            <Select
              labelId="status-label"
              id="status"
              name="status"
              value={filters.status}
              label="Status"
              onChange={handleFilterChange}
            >
              <MenuItem value="">All Statuses</MenuItem>
              {Object.values(OptimizationStatus).map((status) => (
                <MenuItem key={status} value={status}>{status}</MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <InputLabel id="impact-label">Impact</InputLabel>
            <Select
              labelId="impact-label"
              id="impact"
              name="impactLevel"
              value={filters.impactLevel}
              label="Impact"
              onChange={handleFilterChange}
            >
              <MenuItem value="">All Impacts</MenuItem>
              {Object.values(ImpactLevel).map((impact) => (
                <MenuItem key={impact} value={impact}>{impact}</MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>
      }
    >
      {recommendations.length === 0 ? (
        <Box sx={{ py: 3, textAlign: 'center' }}>
          <Typography variant="body1" color="text.secondary">
            No schema optimization recommendations found matching your criteria.
          </Typography>
        </Box>
      ) : (
        recommendations.map((recommendation) => (
          <RecommendationItem
            key={recommendation.recommendationId}
            recommendation={recommendation}
            onApply={handleApplyRecommendation}
            onReject={handleRejectRecommendation}
          />
        ))
      )}
    </Card>
  );
};

/**
 * Component that displays a single schema optimization recommendation
 */
const RecommendationItem: React.FC<RecommendationItemProps> = ({ 
  recommendation, 
  onApply, 
  onReject 
}) => {
  // State management
  const [expanded, setExpanded] = useState(false);
  const [showScript, setShowScript] = useState(false);
  const [applyDialogOpen, setApplyDialogOpen] = useState(false);
  const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
  const [rejectionReason, setRejectionReason] = useState('');
  const [applying, setApplying] = useState(false);
  const [rejecting, setRejecting] = useState(false);

  // Toggle expanded state
  const toggleExpanded = () => {
    setExpanded(!expanded);
  };

  // Toggle script visibility
  const toggleScript = (event: React.MouseEvent) => {
    event.stopPropagation();
    setShowScript(!showScript);
  };

  // Handle apply button click
  const handleApplyClick = (event: React.MouseEvent) => {
    event.stopPropagation();
    setApplyDialogOpen(true);
  };

  // Handle reject button click
  const handleRejectClick = (event: React.MouseEvent) => {
    event.stopPropagation();
    setRejectDialogOpen(true);
  };

  // Confirm applying the recommendation
  const confirmApply = async () => {
    setApplying(true);
    try {
      await onApply(recommendation.recommendationId);
    } finally {
      setApplying(false);
      setApplyDialogOpen(false);
    }
  };

  // Confirm rejecting the recommendation
  const confirmReject = async () => {
    if (!rejectionReason.trim()) return;
    
    setRejecting(true);
    try {
      await onReject(recommendation.recommendationId, rejectionReason);
    } finally {
      setRejecting(false);
      setRejectDialogOpen(false);
      setRejectionReason('');
    }
  };

  // Style mappings for impact levels and statuses
  const impactLevelColors = {
    [ImpactLevel.HIGH]: 'error.main',
    [ImpactLevel.MEDIUM]: 'warning.main',
    [ImpactLevel.LOW]: 'info.main'
  };

  const statusColors = {
    [OptimizationStatus.RECOMMENDED]: 'info.main',
    [OptimizationStatus.APPLIED]: 'success.main',
    [OptimizationStatus.REJECTED]: 'error.main',
    [OptimizationStatus.PENDING]: 'warning.main'
  };

  return (
    <Box 
      sx={{ 
        border: '1px solid rgba(0, 0, 0, 0.12)', 
        borderRadius: '4px', 
        padding: 2, 
        mb: 2,
        backgroundColor: 'background.paper'
      }}
    >
      {/* Recommendation header - always visible */}
      <Box 
        sx={{ 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center',
          cursor: 'pointer'
        }}
        onClick={toggleExpanded}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Storage />
          <Box>
            <Typography variant="subtitle1">
              {recommendation.optimizationType}
              <Chip 
                size="small" 
                label={recommendation.impactLevel} 
                color="primary"
                sx={{ 
                  ml: 1,
                  bgcolor: impactLevelColors[recommendation.impactLevel],
                  color: 'white'
                }} 
              />
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {recommendation.datasetId}.{recommendation.tableId}
            </Typography>
          </Box>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box sx={{ textAlign: 'right', mr: 2 }}>
            <Typography variant="body2">
              Est. Improvement: {formatPercentage(recommendation.estimatedImprovementPercentage)}
            </Typography>
            <Typography variant="body2">
              Cost Reduction: {formatCurrency(recommendation.estimatedCostReduction)}
            </Typography>
          </Box>
          <Chip 
            label={recommendation.status} 
            size="small" 
            sx={{ 
              bgcolor: statusColors[recommendation.status],
              color: 'white'
            }} 
          />
          {expanded ? <ExpandLess /> : <ExpandMore />}
        </Box>
      </Box>

      {/* Expanded recommendation details */}
      <Collapse in={expanded}>
        <Box 
          sx={{ 
            mt: 2, 
            p: 1, 
            backgroundColor: 'rgba(0, 0, 0, 0.02)', 
            borderRadius: '4px'
          }}
        >
          <Typography variant="body2">{recommendation.description}</Typography>
          <Divider sx={{ my: 1 }} />
          
          <Box sx={{ my: 1 }}>
            <Typography variant="subtitle2">Details:</Typography>
            <Typography variant="body2">
              Created: {formatDate(recommendation.createdAt)}
            </Typography>
            {recommendation.implementedAt && (
              <Typography variant="body2">
                Implemented: {formatDate(recommendation.implementedAt)}
              </Typography>
            )}
            {recommendation.rejectedAt && (
              <Typography variant="body2">
                Rejected: {formatDate(recommendation.rejectedAt)}
              </Typography>
            )}
            {recommendation.rejectionReason && (
              <Typography variant="body2">
                Rejection Reason: {recommendation.rejectionReason}
              </Typography>
            )}
          </Box>

          <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 2 }}>
            <Button
              size="small"
              startIcon={<Code />}
              onClick={toggleScript}
              variant={showScript ? "contained" : "outlined"}
            >
              {showScript ? "Hide Implementation" : "View Implementation"}
            </Button>
            
            <Box>
              {recommendation.status === OptimizationStatus.RECOMMENDED && (
                <>
                  <Button
                    size="small"
                    startIcon={<ThumbUp />}
                    color="success"
                    variant="outlined"
                    onClick={handleApplyClick}
                    sx={{ mr: 1 }}
                  >
                    Apply
                  </Button>
                  <Button
                    size="small"
                    startIcon={<ThumbDown />}
                    color="error"
                    variant="outlined"
                    onClick={handleRejectClick}
                  >
                    Reject
                  </Button>
                </>
              )}
            </Box>
          </Box>

          {showScript && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2">Implementation Script:</Typography>
              <SyntaxHighlighter 
                language="sql" 
                style={materialDark}
                customStyle={{ 
                  borderRadius: '4px', 
                  maxHeight: '300px',
                  fontSize: '0.85rem'
                }}
              >
                {recommendation.implementationScript}
              </SyntaxHighlighter>
            </Box>
          )}
        </Box>
      </Collapse>

      {/* Apply Confirmation Dialog */}
      <Dialog open={applyDialogOpen} onClose={() => setApplyDialogOpen(false)}>
        <DialogTitle>Apply Schema Optimization</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to apply this {recommendation.optimizationType} optimization to {recommendation.datasetId}.{recommendation.tableId}?
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            This will execute the following SQL implementation script on your BigQuery instance.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setApplyDialogOpen(false)} disabled={applying}>
            Cancel
          </Button>
          <Button 
            onClick={confirmApply} 
            color="primary" 
            variant="contained"
            disabled={applying}
            startIcon={applying ? <CircularProgress size={20} /> : <Check />}
          >
            {applying ? 'Applying...' : 'Apply'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Reject Dialog */}
      <Dialog open={rejectDialogOpen} onClose={() => setRejectDialogOpen(false)}>
        <DialogTitle>Reject Schema Optimization</DialogTitle>
        <DialogContent>
          <Typography>
            Please provide a reason for rejecting this recommendation:
          </Typography>
          <TextField
            autoFocus
            margin="dense"
            id="reason"
            label="Reason for rejection"
            type="text"
            fullWidth
            multiline
            rows={3}
            value={rejectionReason}
            onChange={(e) => setRejectionReason(e.target.value)}
            required
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRejectDialogOpen(false)} disabled={rejecting}>
            Cancel
          </Button>
          <Button 
            onClick={confirmReject} 
            color="error" 
            variant="contained"
            disabled={rejecting || !rejectionReason.trim()}
            startIcon={rejecting ? <CircularProgress size={20} /> : <Close />}
          >
            {rejecting ? 'Rejecting...' : 'Reject'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SchemaOptimizationCard;