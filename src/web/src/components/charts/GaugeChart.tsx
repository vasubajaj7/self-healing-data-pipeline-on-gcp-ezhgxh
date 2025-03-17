import React, { useRef, useEffect, useState } from 'react';
import { Chart, ChartOptions, ChartData } from 'chart.js'; // version: ^4.3.0
import { merge } from 'lodash'; // version: ^4.17.21
import { 
  formatChartData, 
  formatTooltipLabel, 
  applyChartAnimation, 
  createGradient, 
  getStatusColor 
} from '../../services/charts/chartUtils';
import { transformGaugeData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Props for the GaugeChart component
 */
interface GaugeChartProps {
  /** The current value to display on the gauge */
  value: number;
  /** Minimum value of the gauge range (default: 0) */
  min?: number;
  /** Maximum value of the gauge range (default: 100) */
  max?: number;
  /** Threshold values and their corresponding colors */
  thresholds?: {
    [key: number]: string;
  };
  /** Additional Chart.js options to customize the chart */
  options?: ChartOptions;
  /** Height of the chart canvas (default: 200) */
  height?: number | string;
  /** Width of the chart canvas (default: '100%') */
  width?: number | string;
  /** Additional CSS class names */
  className?: string;
  /** ID attribute for the canvas element */
  id?: string;
  /** Whether to show the value in the center of the gauge (default: true) */
  showValue?: boolean;
  /** Format for the displayed value ('percentage', 'decimal', or custom format) */
  valueFormat?: string;
  /** Width of the gauge arc as percentage of a full circle (default: 75) */
  arcWidth?: number;
  /** Whether to use gradient fill for the gauge (default: false) */
  useGradient?: boolean;
}

/**
 * GaugeChart - A customizable gauge chart component for visualizing single values
 * 
 * Uses Chart.js to render a doughnut chart styled as a gauge with customizable
 * appearance and behavior for displaying metrics like completion percentages,
 * health scores, or resource utilization.
 */
const GaugeChart: React.FC<GaugeChartProps> = ({
  value,
  min = 0,
  max = 100,
  thresholds = {
    33: colors.status.error,
    66: colors.status.warning,
    100: colors.status.healthy
  },
  options = {},
  height = 200,
  width = '100%',
  className = '',
  id,
  showValue = true,
  valueFormat,
  arcWidth = 75,
  useGradient = false
}) => {
  // Reference to the canvas element
  const chartRef = useRef<HTMLCanvasElement>(null);
  // Chart.js instance
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  // Initialize chart on component mount
  useEffect(() => {
    if (chartRef.current) {
      initializeChart();
    }
    
    // Clean up on unmount
    return () => {
      destroyChart();
    };
  }, []);

  // Update chart when props change
  useEffect(() => {
    if (chartInstance) {
      updateChart();
    }
  }, [value, min, max, JSON.stringify(thresholds), options, showValue, valueFormat, arcWidth, useGradient]);

  /**
   * Initialize the gauge chart
   */
  const initializeChart = () => {
    if (!chartRef.current) return;
    
    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;
    
    // Convert thresholds to array format expected by transformGaugeData
    const thresholdArray = Object.entries(thresholds).map(([value, color]) => ({
      value: parseFloat(value),
      color
    }));
    
    // Transform data for the gauge chart
    const chartData = transformGaugeData({ value }, {
      min,
      max,
      thresholds: thresholdArray,
      arcSize: arcWidth / 100,
      showValue
    });
    
    // Get default doughnut chart configuration and apply gauge-specific options
    const defaultOptions = getChartConfig('doughnut');
    const gaugeOptions = merge({}, defaultOptions, {
      cutout: `${85 - arcWidth/4}%`,
      circumference: 2 * Math.PI * (arcWidth / 100),
      rotation: (Math.PI * (1 - arcWidth / 100)) - (Math.PI / 2),
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          enabled: true,
          callbacks: {
            label: (context: any) => {
              return formatTooltipLabel(context, 'default', {
                dataType: valueFormat === 'percentage' ? 'percentage' : 'number',
                precision: 1
              });
            }
          }
        }
      }
    }, options);
    
    // Apply animation settings
    const animatedOptions = applyChartAnimation(gaugeOptions, 'doughnut');
    
    // Apply color based on value
    if (chartData.datasets && chartData.datasets.length > 0) {
      const color = getColorForValue(value, thresholds);
      
      // Apply gradient if requested
      if (useGradient && ctx) {
        const gradient = createGradient(
          ctx,
          color,
          `${color}80`, // Add transparency for gradient effect
          { vertical: false, height: chartRef.current.height, width: chartRef.current.width }
        );
        chartData.datasets[0].backgroundColor[0] = gradient;
      } else {
        chartData.datasets[0].backgroundColor[0] = color;
      }
    }
    
    // Create the chart
    const chart = new Chart(ctx, {
      type: 'doughnut',
      data: chartData,
      options: animatedOptions as ChartOptions,
      plugins: [
        {
          id: 'centerText',
          afterDraw: (chart: any) => {
            if (showValue) {
              renderCenterText();
            }
          }
        }
      ]
    });
    
    setChartInstance(chart);
  };

  /**
   * Update chart with new data and options
   */
  const updateChart = () => {
    if (!chartInstance) return;
    
    // Convert thresholds to array format expected by transformGaugeData
    const thresholdArray = Object.entries(thresholds).map(([value, color]) => ({
      value: parseFloat(value),
      color
    }));
    
    // Transform data for the gauge chart
    const chartData = transformGaugeData({ value }, {
      min,
      max,
      thresholds: thresholdArray,
      arcSize: arcWidth / 100,
      showValue
    });
    
    // Update options
    const gaugeOptions = merge({}, chartInstance.options, {
      cutout: `${85 - arcWidth/4}%`,
      circumference: 2 * Math.PI * (arcWidth / 100),
      rotation: (Math.PI * (1 - arcWidth / 100)) - (Math.PI / 2),
    }, options);
    
    // Apply color based on value
    if (chartData.datasets && chartData.datasets.length > 0) {
      const color = getColorForValue(value, thresholds);
      
      // Apply gradient if requested
      if (useGradient && chartRef.current) {
        const ctx = chartRef.current.getContext('2d');
        if (ctx) {
          const gradient = createGradient(
            ctx,
            color,
            `${color}80`, // Add transparency for gradient effect
            { vertical: false, height: chartRef.current.height, width: chartRef.current.width }
          );
          chartData.datasets[0].backgroundColor[0] = gradient;
        }
      } else {
        chartData.datasets[0].backgroundColor[0] = color;
      }
    }
    
    // Update chart data and options
    chartInstance.data = chartData;
    chartInstance.options = gaugeOptions as ChartOptions;
    
    // Update chart
    chartInstance.update();
  };

  /**
   * Clean up chart instance
   */
  const destroyChart = () => {
    if (chartInstance) {
      chartInstance.destroy();
      setChartInstance(null);
    }
  };

  /**
   * Get color based on value and thresholds
   */
  const getColorForValue = (value: number, thresholds: { [key: number]: string }) => {
    const thresholdValues = Object.keys(thresholds)
      .map(key => parseFloat(key))
      .sort((a, b) => a - b);
    
    // Normalize value to 0-100 range for threshold comparison
    const normalizedValue = ((value - min) / (max - min)) * 100;
    
    // Find appropriate threshold
    for (const threshold of thresholdValues) {
      if (normalizedValue <= threshold) {
        return thresholds[threshold];
      }
    }
    
    // Default to the highest threshold color
    return thresholds[thresholdValues[thresholdValues.length - 1]] || colors.status.healthy;
  };

  /**
   * Render text in the center of the gauge
   */
  const renderCenterText = () => {
    if (!chartInstance || !showValue || !chartRef.current) return;
    
    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;
    
    const centerX = chartInstance.width / 2;
    const centerY = chartInstance.height / 2;
    
    // Configure text style
    ctx.save();
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.font = 'bold 24px Arial';
    ctx.fillStyle = colors.text.primary;
    
    // Format value based on valueFormat
    let displayValue: string;
    if (valueFormat === 'percentage') {
      displayValue = `${Math.round(value)}%`;
    } else if (valueFormat === 'decimal') {
      displayValue = value.toFixed(1);
    } else if (valueFormat) {
      // Assume custom format with placeholders
      displayValue = valueFormat.replace('{value}', value.toString());
    } else {
      displayValue = value.toString();
    }
    
    // Draw text
    ctx.fillText(displayValue, centerX, centerY);
    ctx.restore();
  };

  return (
    <canvas
      ref={chartRef}
      height={height}
      width={width}
      className={`gauge-chart ${className}`}
      id={id}
      aria-label={`Gauge chart showing ${value} out of ${max}`}
      role="img"
    />
  );
};

/**
 * Sets up Chart.js plugins for gauge-specific features
 * 
 * @param chart - Chart.js instance
 * @param options - Options for the plugins
 */
const setupGaugePlugins = (chart: Chart, options: any) => {
  // Register plugin for center text rendering if showValue is true
  if (options.showValue) {
    const centerTextPlugin = {
      id: 'centerText',
      afterDraw: (chart: Chart) => {
        if (typeof options.renderCenterText === 'function') {
          options.renderCenterText();
        }
      }
    };
    
    // Add plugin to chart
    chart.plugins.push(centerTextPlugin);
  }
};

export default GaugeChart;
export type { GaugeChartProps };