import React, { useState, useEffect } from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Divider,
  Tooltip,
  Chip,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
} from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import {
  CheckCircleOutline,
  Warning,
  Error,
  Info,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Card from '../common/Card';
import { AIModel, ModelHealth } from '../../types/selfHealing';
import { formatDate } from '../../utils/date';
import healingService from '../../services/api/healingService';
import { useApi } from '../../hooks/useApi';
import { lightTheme } from '../../theme/theme';

/**
 * Interface defining the props for the ModelHealthCard component.
 */
interface ModelHealthCardProps {
  modelId: string;
  model: AIModel;
  title?: string;
  className?: string;
  showRecommendations?: boolean;
}

/**
 * Interface for a single health indicator with status and description.
 */
interface HealthIndicator {
  name: string;
  status: string;
  description: string;
  details?: string;
}

/**
 * Determines the color for a status indicator based on the status value.
 * @param status The status value (OK, WARNING, ERROR, etc.)
 * @returns Color code for the status
 */
const getStatusColor = (status: string): string => {
  if (status === 'OK' || status === 'GOOD') {
    return lightTheme.palette.success.main; // Success color (#4caf50)
  }
  if (status === 'WARNING' || status === 'WARN') {
    return lightTheme.palette.warning.main; // Warning color (#ff9800)
  }
  if (status === 'ERROR' || status === 'CRITICAL') {
    return lightTheme.palette.error.main; // Error color (#f44336)
  return lightTheme.palette.grey[500]; // Neutral color (#9e9e9e)
};

/**
 * Returns the appropriate icon component based on the status value.
 * @param status The status value (OK, WARNING, ERROR, etc.)
 * @returns Icon component for the status
 */
const getStatusIcon = (status: string): React.ReactNode => {
  if (status === 'OK' || status === 'GOOD') {
    return <CheckCircleOutline />;
  }
  if (status === 'WARNING' || status === 'WARN') {
    return <Warning />;
  }
  if (status === 'ERROR' || status === 'CRITICAL') {
    return <Error />;
  }
  return <Info />;
};

/**
 * Styled component for displaying health status indicators.
 */
const StatusChip = styled(Chip, {
  shouldForwardProp: (prop) => prop !== 'status',
})<{ status: string }>(({ status }) => ({
  fontSize: '0.75rem',
  height: '24px',
  backgroundColor: `rgba(${parseInt(getStatusColor(status).slice(1, 3), 16)}, ${parseInt(getStatusColor(status).slice(3, 5), 16)}, ${parseInt(getStatusColor(status).slice(5, 7), 16)}, 0.1)`,
  color: getStatusColor(status),
  display: 'flex',
  alignItems: 'center',
  gap: '4px',
}));

/**
 * Styled component for individual health metric items.
 */
const HealthItem = styled(Box)({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  padding: '12px',
  borderRadius: '4px',
  backgroundColor: 'rgba(0, 0, 0, 0.02)',
});

/**
 * Styled component for health metric labels.
 */
const HealthLabel = styled(Box)({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  fontWeight: '500',
});

/**
 * Styled component for recommendation list items.
 */
const RecommendationItem = styled(ListItem)({
  padding: '4px 0',
  fontSize: '0.875rem',
});

/**
 * Component that displays health metrics and recommendations for an AI model.
 */
const ModelHealthCard: React.FC<ModelHealthCardProps> = ({
  modelId,
  model,
  title = 'Model Health',
  className,
  showRecommendations = true,
}) => {
  const { executeRequest, data: health, loading, error } = useApiRequest<ModelHealth>();

  /**
   * Fetches model health data from the API.
   */
  const fetchModelHealth = async () => {
    await executeRequest(healingService.getModelHealth, modelId);
  };

  useEffect(() => {
    fetchModelHealth();
  }, [modelId]);

  const driftStatus = health?.driftStatus || 'UNKNOWN';
  const featureHealth = health?.featureHealth || 'UNKNOWN';
  const predictionQuality = health?.predictionQuality || 'UNKNOWN';
  const inferencePerformance = health?.inferencePerformance || 'UNKNOWN';

  const healthIndicators: HealthIndicator[] = [
    {
      name: 'Drift Status',
      status: driftStatus,
      description: 'Indicates the level of drift detected in the model\'s input data.',
    },
    {
      name: 'Feature Health',
      status: featureHealth,
      description: 'Shows the health of the features used by the model.',
    },
    {
      name: 'Prediction Quality',
      status: predictionQuality,
      description: 'Reflects the accuracy and reliability of the model\'s predictions.',
    },
    {
      name: 'Inference Performance',
      status: inferencePerformance,
      description: 'Measures the speed and efficiency of the model\'s inference process.',
    },
  ];

  const recommendations = health?.recommendations || [];
  const lastChecked = health?.lastChecked;

  return (
    <Card title={title} className={className} loading={loading} error={error} minHeight="320px">
      <Grid container spacing={2}>
        {healthIndicators.map((item) => (
          <Grid item xs={12} md={6} key={item.name}>
            <HealthItem>
              <HealthLabel>
                <Typography variant="body1" sx={{ fontSize: '0.875rem', fontWeight: '500' }}>
                  {item.name}
                </Typography>
                <Tooltip title={item.description} placement="top">
                  <StatusChip
                    label={item.status}
                    status={item.status}
                    icon={getStatusIcon(item.status)}
                    size="small"
                  />
                </Tooltip>
              </HealthLabel>
            </HealthItem>
          </Grid>
        ))}
      </Grid>
      {showRecommendations && recommendations.length > 0 && (
        <>
          <Divider sx={{ marginTop: '16px' }} />
          <Box sx={{ marginTop: '16px', padding: '12px', backgroundColor: 'rgba(33, 150, 243, 0.05)', borderRadius: '4px' }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 'medium' }}>
              Recommendations:
            </Typography>
            <List>
              {recommendations.map((recommendation, index) => (
                <RecommendationItem key={index}>
                  <ListItemIcon>
                    <Info color="primary" />
                  </ListItemIcon>
                  <ListItemText primary={recommendation} />
                </RecommendationItem>
              ))}
            </List>
          </Box>
        </>
      )}
      {lastChecked && (
        <Typography variant="caption" sx={{ fontSize: '0.75rem', color: 'text.secondary', fontStyle: 'italic', display: 'block', textAlign: 'right', marginTop: '16px' }}>
          Last checked: {formatDate(lastChecked, 'MMMM d, yyyy HH:mm:ss')}
        </Typography>
      )}
    </Card>
  );
};

export default ModelHealthCard;