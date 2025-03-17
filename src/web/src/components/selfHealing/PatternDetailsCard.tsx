import React, { useState, useEffect, useMemo } from 'react';
import {
  Typography,
  Box,
  Divider,
  Chip,
  Grid,
  Paper,
  Tooltip
} from '@mui/material';
import {
  CheckCircle,
  Error,
  Edit,
  Delete,
  Info
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

import Card from '../common/Card';
import Badge from '../common/Badge';
import HealingActionsTable from './HealingActionsTable';
import { HealingPattern, IssueType } from '../../types/selfHealing';
import { useApi } from '../../hooks/useApi';
import healingService, { getHealingPattern, getHealingExecutions } from '../../services/api/healingService';
import { formatDate } from '../../utils/date';

/**
 * Props for the PatternDetailsCard component
 */
interface PatternDetailsCardProps {
  patternId: string;
  pattern?: HealingPattern;
  title?: string;
  showActions?: boolean;
  showStats?: boolean;
  onEdit?: (pattern: HealingPattern) => void;
  onDelete?: (pattern: HealingPattern) => void;
  onActionSelect?: (actionId: string) => void;
  className?: string;
}

/**
 * Interface for pattern execution statistics
 */
interface PatternStats {
  executionCount: number;
  successCount: number;
  successRate: number;
  lastExecuted?: string;
}

/**
 * Styled component for pattern detail sections
 */
const StyledPatternSection = styled(Paper)(({ theme }) => ({
  marginBottom: '16px',
  padding: '16px',
  backgroundColor: 'rgba(0, 0, 0, 0.02)',
  borderRadius: '4px'
}));

/**
 * Styled component for section titles
 */
const SectionTitle = styled(Typography)(({ theme }) => ({
  fontWeight: '500',
  marginBottom: '8px',
  display: 'flex',
  alignItems: 'center',
  gap: '8px'
}));

/**
 * Styled component for pattern property display
 */
const PatternProperty = styled(Box)(({ theme }) => ({
  display: 'flex',
  marginBottom: '8px',
  alignItems: 'flex-start'
}));

/**
 * Styled component for property labels
 */
const PropertyLabel = styled(Typography)(({ theme }) => ({
  fontWeight: '500',
  minWidth: '140px',
  color: 'text.secondary'
}));

/**
 * Styled component for property values
 */
const PropertyValue = styled(Typography)(({ theme }) => ({
  flex: '1'
}));

/**
 * Styled component for status indicators
 */
const StatusChip = styled(Chip, {
  shouldForwardProp: (prop) => prop !== 'active',
})<{ active: boolean }>(({ theme, active }) => ({
  backgroundColor: active ? 'rgba(76, 175, 80, 0.1)' : 'rgba(244, 67, 54, 0.1)',
  color: active ? '#4caf50' : '#f44336',
  fontWeight: '500',
  fontSize: '0.75rem'
}));

/**
 * Returns a formatted label for an issue type with appropriate styling
 * @param issueType The issue type
 * @returns Formatted issue type badge
 */
const getIssueTypeLabel = (issueType: IssueType): React.ReactNode => {
  let label = 'Unknown';
  let color = 'default';

  switch (issueType) {
    case IssueType.DATA_FORMAT:
      label = 'Data Format';
      color = 'info';
      break;
    case IssueType.DATA_QUALITY:
      label = 'Data Quality';
      color = 'warning';
      break;
    case IssueType.SYSTEM_FAILURE:
      label = 'System Failure';
      color = 'error';
      break;
    case IssueType.PERFORMANCE:
      label = 'Performance';
      color = 'success';
      break;
  }

  return (
    <Badge label={label} variant="outlined" color={color as any} />
  );
};

/**
 * Formats a confidence threshold value as a percentage with appropriate styling
 * @param threshold The confidence threshold
 * @returns Formatted confidence threshold with color-coded styling
 */
const formatConfidenceThreshold = (threshold: number): React.ReactNode => {
  const percentage = `${(threshold * 100).toFixed(1)}%`;
  let color = 'success.main';

  if (threshold < 0.5) {
    color = 'error.main';
  } else if (threshold < 0.8) {
    color = 'warning.main';
  }

  return (
    <Typography variant="body2" color={color} fontWeight={500}>
      {percentage}
    </Typography>
  );
};

/**
 * Formats pattern statistics including execution count and success rate
 * @param executionCount Number of pattern executions
 * @param successRate Success rate as decimal (0-1)
 * @returns Formatted statistics display
 */
const formatPatternStats = (executionCount: number, successRate: number): React.ReactNode => {
  const formattedSuccessRate = `${(successRate * 100).toFixed(1)}%`;
  let color = 'success.main';

  if (successRate < 0.5) {
    color = 'error.main';
  } else if (successRate < 0.8) {
    color = 'warning.main';
  }

  return (
    <Box>
      <Typography variant="body2">
        Executions: <strong>{executionCount}</strong>
      </Typography>
      <Typography variant="body2">
        Success Rate: <span style={{ color: color === 'success.main' ? '#2e7d32' : color === 'warning.main' ? '#ed6c02' : '#d32f2f', fontWeight: 500 }}>{formattedSuccessRate}</span>
      </Typography>
    </Box>
  );
};

/**
 * Component that displays detailed information about a healing pattern
 */
const PatternDetailsCard: React.FC<PatternDetailsCardProps> = ({
  patternId,
  pattern,
  title = 'Pattern Details',
  showActions = true,
  showStats = true,
  onEdit,
  onDelete,
  onActionSelect,
  className
}) => {
  // State for pattern data and loading
  const [patternData, setPatternData] = useState<HealingPattern | null>(null);
  const [patternStats, setPatternStats] = useState<PatternStats | null>(null);
  
  // API hooks for data fetching
  const { get, loading, error } = useApi();

  // Fetch pattern data if not provided in props
  useEffect(() => {
    const fetchPatternData = async () => {
      if (!pattern && patternId) {
        try {
          const response = await get(healingService.getHealingPattern, patternId);
          setPatternData(response.data);
        } catch (err) {
          console.error('Error fetching pattern details:', err);
        }
      } else if (pattern) {
        setPatternData(pattern);
      }
    };

    fetchPatternData();
  }, [patternId, pattern, get]);

  // Fetch pattern statistics if needed
  useEffect(() => {
    const fetchPatternStats = async () => {
      if (!showStats || !patternId) return;
      
      try {
        const params = { patternId, page: 1, pageSize: 100 };
        const response = await get(healingService.getHealingExecutions, params);
        
        if (response && response.items && response.pagination) {
          const executionCount = response.pagination.totalItems;
          const successCount = response.items.filter(item => item.successful).length;
          const successRate = executionCount > 0 ? successCount / executionCount : 0;
          const lastExecuted = response.items.length > 0 ? response.items[0].executionTime : undefined;
          
          setPatternStats({
            executionCount,
            successCount,
            successRate,
            lastExecuted
          });
        }
      } catch (err) {
        console.error('Error fetching pattern statistics:', err);
      }
    };

    fetchPatternStats();
  }, [patternId, showStats, get]);

  // Prepare header actions
  const headerActions = useMemo(() => {
    if (!patternData || (!onEdit && !onDelete)) return null;
    
    return (
      <Box>
        {onEdit && (
          <Tooltip title="Edit Pattern">
            <Edit 
              style={{ cursor: 'pointer', marginRight: '8px' }}
              onClick={() => onEdit(patternData)}
              color="primary"
            />
          </Tooltip>
        )}
        {onDelete && (
          <Tooltip title="Delete Pattern">
            <Delete 
              style={{ cursor: 'pointer' }}
              onClick={() => onDelete(patternData)}
              color="error"
            />
          </Tooltip>
        )}
      </Box>
    );
  }, [patternData, onEdit, onDelete]);

  // Determine active pattern based on data
  const isActive = patternData?.isActive || false;

  return (
    <Card 
      title={title}
      action={headerActions}
      loading={loading && !patternData}
      error={error}
      className={className}
    >
      {patternData ? (
        <Box>
          {/* Pattern Metadata */}
          <StyledPatternSection elevation={0}>
            <SectionTitle variant="subtitle1">
              <Info fontSize="small" color="primary" />
              Pattern Information
            </SectionTitle>
            <Divider sx={{ mb: 2 }} />
            
            <PatternProperty>
              <PropertyLabel variant="body2">Name:</PropertyLabel>
              <PropertyValue variant="body2" fontWeight={500}>{patternData.name}</PropertyValue>
            </PatternProperty>
            
            <PatternProperty>
              <PropertyLabel variant="body2">Issue Type:</PropertyLabel>
              <PropertyValue variant="body2">{getIssueTypeLabel(patternData.issueType)}</PropertyValue>
            </PatternProperty>
            
            <PatternProperty>
              <PropertyLabel variant="body2">Status:</PropertyLabel>
              <PropertyValue variant="body2">
                <StatusChip
                  active={isActive}
                  label={isActive ? 'Active' : 'Inactive'}
                  size="small"
                  icon={isActive ? <CheckCircle fontSize="small" /> : <Error fontSize="small" />}
                />
              </PropertyValue>
            </PatternProperty>
            
            <PatternProperty>
              <PropertyLabel variant="body2">Confidence Threshold:</PropertyLabel>
              <PropertyValue variant="body2">{formatConfidenceThreshold(patternData.confidenceThreshold)}</PropertyValue>
            </PatternProperty>
            
            <PatternProperty>
              <PropertyLabel variant="body2">Created:</PropertyLabel>
              <PropertyValue variant="body2">{formatDate(patternData.createdAt, 'MMM dd, yyyy HH:mm')}</PropertyValue>
            </PatternProperty>
            
            <PatternProperty>
              <PropertyLabel variant="body2">Last Updated:</PropertyLabel>
              <PropertyValue variant="body2">{formatDate(patternData.updatedAt, 'MMM dd, yyyy HH:mm')}</PropertyValue>
            </PatternProperty>
            
            {patternData.description && (
              <PatternProperty>
                <PropertyLabel variant="body2">Description:</PropertyLabel>
                <PropertyValue variant="body2">{patternData.description}</PropertyValue>
              </PatternProperty>
            )}
          </StyledPatternSection>
          
          {/* Pattern Detection Logic */}
          <StyledPatternSection elevation={0}>
            <SectionTitle variant="subtitle1">
              <Info fontSize="small" color="primary" />
              Detection Pattern
            </SectionTitle>
            <Divider sx={{ mb: 2 }} />
            
            <Box
              sx={{
                backgroundColor: 'background.paper',
                padding: 1.5,
                borderRadius: 1,
                overflowX: 'auto',
                fontFamily: 'monospace',
                fontSize: '0.875rem'
              }}
            >
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                {JSON.stringify(patternData.detectionPattern, null, 2)}
              </pre>
            </Box>
          </StyledPatternSection>
          
          {/* Statistics Section */}
          {showStats && patternStats && (
            <StyledPatternSection elevation={0}>
              <SectionTitle variant="subtitle1">
                <Info fontSize="small" color="primary" />
                Pattern Statistics
              </SectionTitle>
              <Divider sx={{ mb: 2 }} />
              
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  {formatPatternStats(patternStats.executionCount, patternStats.successRate)}
                </Grid>
                <Grid item xs={12} md={6}>
                  {patternStats.lastExecuted && (
                    <Typography variant="body2">
                      Last Execution: {formatDate(patternStats.lastExecuted, 'MMM dd, yyyy HH:mm')}
                    </Typography>
                  )}
                </Grid>
              </Grid>
            </StyledPatternSection>
          )}
          
          {/* Associated Actions */}
          {showActions && (
            <HealingActionsTable 
              patternId={patternId}
              title="Associated Healing Actions"
              onActionSelect={onActionSelect}
            />
          )}
        </Box>
      ) : (
        !loading && !error && (
          <Typography variant="body1" color="text.secondary">
            Pattern not found or no data available.
          </Typography>
        )
      )}
    </Card>
  );
};

export default PatternDetailsCard;