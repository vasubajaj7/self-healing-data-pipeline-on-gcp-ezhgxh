import React, { useMemo } from 'react'; // react ^18.2.0
import { Box, Grid, Typography, LinearProgress, Tooltip } from '@mui/material'; // @mui/material ^5.11.0
import Card from '../common/Card';
import LineChart from '../charts/LineChart';
import { useQuality } from '../../contexts/QualityContext';
import { QualityDimension } from '../../types/quality';
import { colors } from '../../theme/colors'; // Import color definitions for dimension styling

/**
 * @description Props for the QualityDimensionsCard component
 */
interface QualityDimensionsCardProps {
  className?: string;
  title?: string;
  dataset?: string;
  table?: string;
  showChart?: boolean;
  minHeight?: number | string;
}

/**
 * @description Internal interface for dimension score data
 */
interface DimensionScore {
  dimension: QualityDimension;
  score: number;
  label: string;
  color: string;
}

/**
 * @description Formats the dimension enum value into a readable label
 * @param {QualityDimension} dimension
 * @returns {string} Formatted dimension label
 */
const formatDimensionLabel = (dimension: QualityDimension): string => {
  const label = dimension.toLowerCase();
  return label.charAt(0).toUpperCase() + label.slice(1);
};

/**
 * @description Gets the color for a specific quality dimension
 * @param {QualityDimension} dimension
 * @returns {string} Color hex code for the dimension
 */
const getDimensionColor = (dimension: QualityDimension): string => {
  switch (dimension) {
    case QualityDimension.COMPLETENESS:
      return colors.chart.blue;
    case QualityDimension.ACCURACY:
      return colors.chart.green;
    case QualityDimension.CONSISTENCY:
      return colors.chart.purple;
    case QualityDimension.TIMELINESS:
      return colors.chart.orange;
    case QualityDimension.VALIDITY:
      return colors.chart.red;
    default:
      return colors.chart.blue;
  }
};

/**
 * @description Determines the color variant based on score value
 * @param {number} score
 * @returns {string} Color variant (success, warning, error)
 */
const getScoreVariant = (score: number): string => {
  if (score >= 90) {
    return 'success';
  } else if (score >= 70) {
    return 'warning';
  } else {
    return 'error';
  }
};

/**
 * @description A card component that displays quality dimensions with scores and visualizations
 */
const QualityDimensionsCard: React.FC<QualityDimensionsCardProps> = ({
  className,
  title = 'Quality Dimensions',
  dataset,
  table,
  showChart = true,
  minHeight,
}) => {
  // Access quality data from useQuality hook
  const { statistics, loading, filters } = useQuality();

  // Processed dimension scores with formatting and colors
  const dimensionScores: DimensionScore[] = useMemo(() => {
    if (!statistics) return [];

    return Object.values(QualityDimension).map(dimension => {
      const score = statistics.issuesByDimension[dimension] || 0;
      const formattedLabel = formatDimensionLabel(dimension);
      const color = getDimensionColor(dimension);

      return {
        dimension,
        score,
        label: formattedLabel,
        color,
      };
    });
  }, [statistics]);

  // Prepared data for the LineChart component
  const chartData = useMemo(() => {
    if (!statistics) return null;

    return {
      labels: dimensionScores.map(d => d.label),
      datasets: [
        {
          label: 'Issues',
          data: dimensionScores.map(d => d.score),
          backgroundColor: dimensionScores.map(d => d.color),
          borderColor: dimensionScores.map(d => d.color),
        },
      ],
    };
  }, [dimensionScores]);

  return (
    <Card title={title} className={className} loading={loading} minHeight={minHeight}>
      {showChart && chartData ? (
        <LineChart data={chartData} options={{ responsive: true, maintainAspectRatio: false }} />
      ) : loading ? (
        <Typography>Loading chart...</Typography>
      ) : (
        <Typography>No chart data available.</Typography>
      )}
      <Grid container spacing={2} mt={2}>
        {dimensionScores.map((dimension) => (
          <Grid item xs={12} sm={6} md={4} key={dimension.dimension}>
            <Box display="flex" alignItems="center">
              <Box width="100%" mr={1}>
                <Tooltip title={`${dimension.score}% ${dimension.label}`} placement="top">
                  <LinearProgress
                    variant="determinate"
                    value={dimension.score}
                    color={getScoreVariant(dimension.score) as 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning'}
                  />
                </Tooltip>
              </Box>
              <Box minWidth={35}>
                <Typography variant="body2" color="textSecondary">{`${Math.round(
                  dimension.score,
                )}%`}</Typography>
              </Box>
            </Box>
            <Typography variant="subtitle2" color="textSecondary">
              {dimension.label}
            </Typography>
          </Grid>
        ))}
      </Grid>
    </Card>
  );
};

QualityDimensionsCard.defaultProps = {
  title: 'Quality Dimensions',
  showChart: true,
};

export default QualityDimensionsCard;