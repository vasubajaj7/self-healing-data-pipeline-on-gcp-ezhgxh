import React, { useState, useEffect } from 'react'; // react ^18.2.0
import { Box, Grid, Typography, Divider, Tooltip, Chip } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0

import Card from '../common/Card';
import LineChart from '../charts/LineChart';
import { AIModel, ModelEvaluationResult } from '../../types/selfHealing';
import { formatDate } from '../../utils/date';
import healingService from '../../services/api/healingService';
import { useApi } from '../../hooks/useApi';
import { transformTimeSeriesData } from '../../services/charts/dataTransformers';

/**
 * Interface defining the props for the ModelPerformanceCard component
 */
interface ModelPerformanceCardProps {
  modelId: string;
  model: AIModel;
  title?: string;
  className?: string;
  showHistory?: boolean;
}

/**
 * Interface for a single performance metric with current and previous values
 */
interface PerformanceMetric {
  name: string;
  value: number;
  previousValue: number | null;
  change: number;
  changeIndicator: 'positive' | 'negative' | 'neutral';
  description: string;
}

/**
 * Formats a decimal value as a percentage with specified decimal places
 * @param value The decimal value
 * @param decimalPlaces Number of decimal places
 * @returns Formatted percentage string
 */
const formatPercentage = (value: number, decimalPlaces: number): string => {
  const percentage = (value * 100).toFixed(decimalPlaces);
  return `${percentage}%`;
};

/**
 * Determines the change indicator (positive, negative, neutral) based on metric type and change value
 * @param metricType Type of metric
 * @param change Change value
 * @returns Change indicator string
 */
const getChangeIndicator = (metricType: string, change: number): 'positive' | 'negative' | 'neutral' => {
  const threshold = 0.001; // Threshold to consider change as neutral
  if (Math.abs(change) < threshold) {
    return 'neutral';
  }

  if (['accuracy', 'precision', 'recall', 'f1Score'].includes(metricType)) {
    return change > 0 ? 'positive' : 'negative';
  } else if (['rmse', 'inferenceTime'].includes(metricType)) {
    return change < 0 ? 'positive' : 'negative'; // Lower is better for these metrics
  }

  return 'neutral';
};

/**
 * Prepares performance data for chart visualization
 * @param evaluationHistory Array of model evaluation results
 * @returns Chart data object with datasets for each metric
 */
const preparePerformanceData = (evaluationHistory: ModelEvaluationResult[]) => {
  const sortedHistory = [...evaluationHistory].sort((a, b) => new Date(a.evaluationTime).getTime() - new Date(b.evaluationTime).getTime());

  const labels = sortedHistory.map(item => formatDate(item.evaluationTime, 'MMM d'));

  const accuracyData = sortedHistory.map(item => item.accuracy);
  const precisionData = sortedHistory.map(item => item.precision);
  const recallData = sortedHistory.map(item => item.recall);
  const f1ScoreData = sortedHistory.map(item => item.f1Score);

  return {
    labels: labels,
    datasets: [
      {
        label: 'Accuracy',
        data: accuracyData,
        borderColor: '#1976d2',
        backgroundColor: 'rgba(25, 118, 210, 0.1)',
        fill: true,
      },
      {
        label: 'Precision',
        data: precisionData,
        borderColor: '#4caf50',
        backgroundColor: 'rgba(76, 175, 80, 0.1)',
        fill: true,
      },
      {
        label: 'Recall',
        data: recallData,
        borderColor: '#ff9800',
        backgroundColor: 'rgba(255, 152, 0, 0.1)',
        fill: true,
      },
      {
        label: 'F1 Score',
        data: f1ScoreData,
        borderColor: '#9c27b0',
        backgroundColor: 'rgba(156, 39, 176, 0.1)',
        fill: true,
      },
    ],
  };
};

/**
 * Styled component for individual performance metric items
 */
const MetricItem = styled(Box)({
  display: 'flex',
  flexDirection: 'column',
  gap: '8px',
  padding: '12px',
  borderRadius: '4px',
  backgroundColor: 'rgba(0, 0, 0, 0.02)',
});

/**
 * Styled component for metric labels
 */
const MetricLabel = styled(Typography)({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  fontWeight: '500',
});

/**
 * Styled component for metric values with change indicator
 */
const MetricValue = styled(Typography)({
  display: 'flex',
  alignItems: 'center',
  gap: '8px',
  fontSize: '1.25rem',
  fontWeight: 'bold',
});

/**
 * Styled component for displaying metric changes
 */
const ChangeChip = styled(Chip, {
  shouldForwardProp: (prop) => prop !== 'changeIndicator',
})<{ changeIndicator: string }>(({ changeIndicator }) => ({
  fontSize: '0.75rem',
  height: '20px',
  backgroundColor:
    changeIndicator === 'positive'
      ? 'rgba(76, 175, 80, 0.1)'
      : changeIndicator === 'negative'
      ? 'rgba(244, 67, 54, 0.1)'
      : 'rgba(158, 158, 158, 0.1)',
  color:
    changeIndicator === 'positive'
      ? '#4caf50'
      : changeIndicator === 'negative'
      ? '#f44336'
      : '#9e9e9e',
}));

/**
 * Component that displays performance metrics and trends for an AI model
 */
const ModelPerformanceCard: React.FC<ModelPerformanceCardProps> = ({ modelId, model, title = 'Model Performance', className, showHistory = true }) => {
  const { data: evaluation, loading, error } = useApi<ModelEvaluationResult>({
    defaultValue: null,
  });

  useEffect(() => {
    if (modelId) {
      healingService.getModelEvaluation(modelId);
    }
  }, [modelId]);

  const currentEvaluation = evaluation;
  const previousEvaluation = null;

  const accuracyChange = currentEvaluation && previousEvaluation ? currentEvaluation.accuracy - previousEvaluation.accuracy : 0;
  const precisionChange = currentEvaluation && previousEvaluation ? currentEvaluation.precision - previousEvaluation.precision : 0;
  const recallChange = currentEvaluation && previousEvaluation ? currentEvaluation.recall - previousEvaluation.recall : 0;
  const f1ScoreChange = currentEvaluation && previousEvaluation ? currentEvaluation.f1Score - previousEvaluation.f1Score : 0;
  const rmseChange = currentEvaluation && previousEvaluation && currentEvaluation.rmse && previousEvaluation.rmse ? currentEvaluation.rmse - previousEvaluation.rmse : 0;
  const inferenceTimeChange = currentEvaluation && previousEvaluation ? currentEvaluation.averageInferenceTime - previousEvaluation.averageInferenceTime : 0;

  const accuracyChangeIndicator = getChangeIndicator('accuracy', accuracyChange);
  const precisionChangeIndicator = getChangeIndicator('precision', precisionChange);
  const recallChangeIndicator = getChangeIndicator('recall', recallChange);
  const f1ScoreChangeIndicator = getChangeIndicator('f1Score', f1ScoreChange);
  const rmseChangeIndicator = getChangeIndicator('rmse', rmseChange);
  const inferenceTimeChangeIndicator = getChangeIndicator('inferenceTime', inferenceTimeChange);

  const performanceMetrics: PerformanceMetric[] = [
    {
      name: 'Accuracy',
      value: currentEvaluation?.accuracy || 0,
      previousValue: previousEvaluation?.accuracy || null,
      change: accuracyChange,
      changeIndicator: accuracyChangeIndicator,
      description: 'Percentage of correct predictions',
    },
    {
      name: 'Precision',
      value: currentEvaluation?.precision || 0,
      previousValue: previousEvaluation?.precision || null,
      change: precisionChange,
      changeIndicator: precisionChangeIndicator,
      description: 'Ratio of true positives to predicted positives',
    },
    {
      name: 'Recall',
      value: currentEvaluation?.recall || 0,
      previousValue: previousEvaluation?.recall || null,
      change: recallChange,
      changeIndicator: recallChangeIndicator,
      description: 'Ratio of true positives to actual positives',
    },
    {
      name: 'F1 Score',
      value: currentEvaluation?.f1Score || 0,
      previousValue: previousEvaluation?.f1Score || null,
      change: f1ScoreChange,
      changeIndicator: f1ScoreChangeIndicator,
      description: 'Harmonic mean of precision and recall',
    },
    {
      name: 'RMSE',
      value: currentEvaluation?.rmse || 0,
      previousValue: previousEvaluation?.rmse || null,
      change: rmseChange,
      changeIndicator: rmseChangeIndicator,
      description: 'Root Mean Squared Error',
    },
    {
      name: 'Inference Time',
      value: currentEvaluation?.averageInferenceTime || 0,
      previousValue: previousEvaluation?.averageInferenceTime || null,
      change: inferenceTimeChange,
      changeIndicator: inferenceTimeChangeIndicator,
      description: 'Average time to make a prediction (ms)',
    },
  ];

  const chartData = preparePerformanceData([currentEvaluation].filter(Boolean));

  return (
    <Card title={title} className={className} minHeight="320px">
      <Grid container spacing={2}>
        {performanceMetrics.map((metric) => (
          <Grid item xs={12} sm={6} md={4} key={metric.name}>
            <MetricItem>
              <MetricLabel>
                {metric.name}
                <Tooltip title={metric.description} placement="top">
                  <span>
                    {/* Add info icon here if needed */}
                  </span>
                </Tooltip>
              </MetricLabel>
              <MetricValue>
                {formatPercentage(metric.value, 2)}
                {metric.previousValue !== null && (
                  <ChangeChip label={metric.change > 0 ? `+${formatPercentage(metric.change, 2)}` : `${formatPercentage(metric.change, 2)}`} changeIndicator={metric.changeIndicator} />
                )}
              </MetricValue>
            </MetricItem>
          </Grid>
        ))}
      </Grid>
      <Divider sx={{ margin: '16px 0' }} />
      {showHistory && (
        <Box sx={{ height: '200px', marginTop: '16px', marginBottom: '16px' }}>
          <LineChart data={chartData} options={{ maintainAspectRatio: false }} />
        </Box>
      )}
      <Typography variant="caption" color="text.secondary" sx={{ fontStyle: 'italic' }}>
        Model Version: {model.version} | Last Evaluation: {formatDate(currentEvaluation?.evaluationTime, 'MMM d, yyyy')}
      </Typography>
    </Card>
  );
};

export default ModelPerformanceCard;