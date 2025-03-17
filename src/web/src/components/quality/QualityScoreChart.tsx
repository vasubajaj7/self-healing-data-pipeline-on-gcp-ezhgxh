import React, { useMemo } from 'react'; // version: ^18.2.0
import { Box, Typography, CircularProgress } from '@mui/material'; // version: ^5.11.0
import merge from 'lodash'; // version: ^4.17.21
import LineChart from '../charts/LineChart';
import { formatPercentage } from '../../services/charts/chartUtils';
import { getChartConfig, chartColorSchemes } from '../../config/chartConfig';
import { transformGaugeData } from '../../services/charts/dataTransformers';
import { useQuality } from '../../contexts/QualityContext';
import { QualityDimension } from '../../types/quality';

/**
 * Interface defining the props for the QualityScoreChart component
 */
export interface QualityScoreChartProps {
  /** The quality score value (0-100) */
  score: number;
  /** Title to display above the chart */
  title?: string;
  /** Height of the chart */
  height?: number | string;
  /** Width of the chart */
  width?: number | string;
  /** Additional CSS class for styling */
  className?: string;
  /** Whether to show individual dimension scores */
  showDimensions?: boolean;
  /** Scores for individual quality dimensions */
  dimensionScores?: Record<QualityDimension, number>;
  /** Custom chart options to override defaults */
  options?: any;
  /** Whether the chart data is loading */
  isLoading?: boolean;
  /** Callback function when chart is clicked */
  onClick?: () => void;
}

/**
 * A component that displays a quality score in a gauge-like chart format with color-coded indicators
 */
const QualityScoreChart: React.FC<QualityScoreChartProps> = ({
  score,
  title,
  height = 200,
  width = '100%',
  className,
  showDimensions = false,
  dimensionScores,
  options,
  isLoading = false,
  onClick
}) => {
  // Transform chart data for gauge visualization
  const chartData = useMemo(() => transformGaugeData({ value: score, min: 0, max: 100 }), [score]);

  // Configure chart options with appropriate styling based on score
  const chartOptions = useMemo(() => getChartOptions(score, options), [score, options]);

  /**
   * Formats the tooltip content for the quality score chart
   * @param tooltipItem - The tooltip item object
   * @returns Formatted tooltip string with quality score and dimension
   */
  const formatTooltip = (tooltipItem: any): string => {
    const value = tooltipItem.raw;
    return `Quality Score: ${formatPercentage(value / 100, 1)}`;
  };

  /**
   * Determines the appropriate color based on quality score value
   * @param score - The quality score value
   * @returns Color hex code for the given quality score
   */
  const getQualityColor = (score: number): string => {
    if (score < 70) {
      return chartColorSchemes.status.error; // Red
    } else if (score < 90) {
      return chartColorSchemes.status.warning; // Yellow
    } else {
      return chartColorSchemes.status.healthy; // Green
    }
  };

  /**
   * Generates chart options for the quality score chart
   * @param score - The quality score value
   * @param customOptions - Custom chart options
   * @returns Chart options configuration
   */
  const getChartOptions = (score: number, customOptions?: any): any => {
    // Get base chart configuration for doughnut chart
    const baseOptions = getChartConfig('doughnut');

    // Configure chart to appear as a gauge (partial doughnut)
    const gaugeOptions = {
      cutout: '80%',
      rotation: -90,
      circumference: 180,
      plugins: {
        tooltip: {
          callbacks: {
            label: formatTooltip
          }
        },
        legend: {
          display: false
        }
      }
    };

    // Set up color based on quality score
    const color = getQualityColor(score);

    // Configure center text to show quality score
    const centerText = {
      id: 'centerText',
      beforeDatasetsDraw: (chart: any) => {
        const width = chart.width;
        const height = chart.height;
        const ctx = chart.ctx;

        ctx.restore();
        const fontSize = (height / 114).toFixed(2);
        ctx.font = fontSize + "em sans-serif";
        ctx.textBaseline = "middle";

        const text = `${score}%`;
        const textX = Math.round((width - ctx.measureText(text).width) / 2);
        const textY = height / 2;

        ctx.fillStyle = color;
        ctx.fillText(text, textX, textY);
        ctx.save();
      }
    };

    // Merge with any custom options provided
    const mergedOptions = merge({}, baseOptions, gaugeOptions, customOptions, {
      plugins: [centerText]
    });

    return mergedOptions;
  };

  return (
    <Box className={className} height={height} width={width} onClick={onClick}>
      {isLoading ? (
        <Box display="flex" justifyContent="center" alignItems="center" height="100%">
          <CircularProgress />
        </Box>
      ) : (
        <>
          {title && (
            <Typography variant="subtitle2" align="center" gutterBottom>
              {title}
            </Typography>
          )}
          <LineChart data={chartData} options={chartOptions} height={height} width={width} />
          {showDimensions && dimensionScores && (
            <Box mt={2}>
              <Typography variant="body2" align="center">
                {Object.entries(dimensionScores).map(([dimension, score]) => (
                  <div key={dimension}>
                    {dimension}: {formatPercentage(score / 100, 1)}
                  </div>
                ))}
              </Typography>
            </Box>
          )}
        </>
      )}
    </Box>
  );
};

QualityScoreChart.defaultProps = {
  height: 200,
  width: '100%',
  showDimensions: false,
  isLoading: false
};

export default QualityScoreChart;