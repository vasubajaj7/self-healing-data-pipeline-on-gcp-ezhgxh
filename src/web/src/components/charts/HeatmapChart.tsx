import React, { useRef, useEffect, useState } from 'react';
import Chart from 'chart.js/auto'; // version: ^4.3.0
import { ChartOptions, ChartData } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21

import { 
  formatChartData, 
  formatAxisLabel, 
  formatTooltipLabel, 
  applyChartAnimation,
  createGradient
} from '../../services/charts/chartUtils';
import { transformHeatmapData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Interface defining the props for the HeatmapChart component
 */
export interface HeatmapChartProps {
  /** Matrix data to visualize */
  data: any[] | Record<string, any>;
  /** Custom chart options to override defaults */
  options?: Record<string, any>;
  /** Height of the chart */
  height?: number | string;
  /** Width of the chart */
  width?: number | string;
  /** Additional CSS class name */
  className?: string;
  /** HTML ID for the canvas element */
  id?: string;
  /** Color scale for the heatmap (array of colors from low to high) */
  colorScale?: string[] | Record<string, any>;
  /** Radius of the heatmap cells */
  cellRadius?: number;
  /** Whether to show values in tooltips */
  showValues?: boolean;
  /** Callback when a cell is clicked */
  onCellClick?: (cellData: any) => void;
}

/**
 * A React component that renders a heatmap chart using Chart.js
 * Used for visualization of matrix data with color intensity representing values.
 * 
 * @param props - The component props
 * @returns A React component
 */
const HeatmapChart: React.FC<HeatmapChartProps> = ({
  data,
  options = {},
  height = 400,
  width = '100%',
  className = '',
  id = 'heatmap-chart',
  colorScale = ['#d4f7ff', '#0088aa', '#005566'],
  cellRadius = 20,
  showValues = true,
  onCellClick
}) => {
  const chartRef = useRef<HTMLCanvasElement>(null);
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  /**
   * Creates and initializes the Chart.js instance with heatmap configuration
   */
  const initializeChart = () => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;

    // Transform data for heatmap visualization
    const transformedData = transformHeatmapData(data, {
      colorScale: Array.isArray(colorScale) ? colorScale : undefined,
      showValues,
      normalizeValues: true
    });

    // Get base chart configuration
    const baseConfig = getChartConfig('scatter', options);

    // Apply heatmap styling
    const styledData = applyHeatmapStyles(transformedData, ctx);

    // Merge options with defaults
    const chartOptions = merge({}, baseConfig, {
      scales: {
        x: {
          type: 'linear',
          position: 'bottom',
          min: -0.5,
          max: styledData.labels.length - 0.5,
          ticks: {
            stepSize: 1,
            callback: (value: number) => {
              return styledData.labels[Math.round(value)] || '';
            }
          },
          grid: {
            display: false
          }
        },
        y: {
          type: 'linear',
          position: 'left',
          reverse: true,
          min: -0.5,
          max: data.yLabels?.length 
            ? data.yLabels.length - 0.5 
            : Math.floor(styledData.datasets[0].data.length / styledData.labels.length) - 0.5,
          ticks: {
            stepSize: 1,
            callback: (value: number) => {
              return data.yLabels?.[Math.round(value)] || Math.round(value);
            }
          },
          grid: {
            display: false
          }
        }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const { x, y, value } = context.raw;
              const xLabel = styledData.labels[Math.round(x)] || x;
              const yLabel = data.yLabels?.[Math.round(y)] || y;
              return `${xLabel}, ${yLabel}: ${value}`;
            }
          }
        },
        legend: {
          display: false
        }
      }
    });

    // Apply animation settings
    const animatedOptions = applyChartAnimation(chartOptions, 'scatter', true);

    // Create and store chart instance
    const newChart = new Chart(ctx, {
      type: 'scatter',
      data: styledData,
      options: animatedOptions
    });

    setChartInstance(newChart);

    // Setup event handlers
    if (onCellClick) {
      setupChartEvents(newChart, onCellClick);
    }
  };

  /**
   * Updates the chart with new data or options
   */
  const updateChart = () => {
    if (!chartInstance) return;

    // Transform data for heatmap visualization
    const transformedData = transformHeatmapData(data, {
      colorScale: Array.isArray(colorScale) ? colorScale : undefined,
      showValues,
      normalizeValues: true
    });

    // Apply heatmap styling
    const styledData = applyHeatmapStyles(transformedData, chartInstance.ctx);

    // Update chart data
    chartInstance.data = styledData;
    
    // Update options if needed
    if (options) {
      const baseConfig = getChartConfig('scatter', options);
      chartInstance.options = merge({}, baseConfig, chartInstance.options, options);
      
      // Update scales max values based on data
      if (chartInstance.options.scales && chartInstance.options.scales.x) {
        chartInstance.options.scales.x.max = styledData.labels.length - 0.5;
      }
      
      if (chartInstance.options.scales && chartInstance.options.scales.y) {
        chartInstance.options.scales.y.max = data.yLabels?.length 
          ? data.yLabels.length - 0.5 
          : Math.floor(styledData.datasets[0].data.length / styledData.labels.length) - 0.5;
      }
    }

    chartInstance.update();
  };

  /**
   * Cleans up the Chart.js instance
   */
  const destroyChart = () => {
    if (chartInstance) {
      chartInstance.destroy();
      setChartInstance(null);
    }
  };

  /**
   * Applies heatmap-specific styles to the chart datasets
   * 
   * @param chartData - The chart data to style
   * @param ctx - The canvas rendering context
   * @returns Updated chart data with heatmap styles
   */
  const applyHeatmapStyles = (chartData: ChartData, ctx: CanvasRenderingContext2D): ChartData => {
    if (!chartData.datasets || chartData.datasets.length === 0) return chartData;

    const dataset = chartData.datasets[0];
    
    // Apply point styling
    dataset.pointRadius = cellRadius;
    dataset.pointStyle = 'rect';
    dataset.borderWidth = 1;
    dataset.borderColor = 'rgba(255,255,255,0.2)';
    
    // Apply colors based on values
    dataset.backgroundColor = (context: any) => {
      if (!context.raw) return 'rgba(0,0,0,0.2)';
      
      // Get normalized value from the data point
      const normalizedValue = context.raw.normalizedValue;
      
      // Handle specific color scale
      if (Array.isArray(colorScale)) {
        if (colorScale.length === 1) return colorScale[0];
        
        // Calculate color based on normalized value
        const index = Math.min(colorScale.length - 2, Math.floor(normalizedValue * (colorScale.length - 1)));
        const remainder = (normalizedValue * (colorScale.length - 1)) - index;
        
        const color1 = colorScale[index];
        const color2 = colorScale[Math.min(colorScale.length - 1, index + 1)];
        
        // Interpolate colors for smooth gradient
        const r1 = parseInt(color1.substring(1, 3), 16);
        const g1 = parseInt(color1.substring(3, 5), 16);
        const b1 = parseInt(color1.substring(5, 7), 16);
        
        const r2 = parseInt(color2.substring(1, 3), 16);
        const g2 = parseInt(color2.substring(3, 5), 16);
        const b2 = parseInt(color2.substring(5, 7), 16);
        
        const r = Math.round(r1 + remainder * (r2 - r1));
        const g = Math.round(g1 + remainder * (g2 - g1));
        const b = Math.round(b1 + remainder * (b2 - b1));
        
        return `rgb(${r}, ${g}, ${b})`;
      }
      
      // Default blue scale if no color scale provided
      return colors.chart.blue + Math.round(normalizedValue * 200).toString(16).padStart(2, '0');
    };

    // Hover effects
    dataset.pointHoverRadius = cellRadius * 1.1;
    dataset.pointHoverBackgroundColor = (context: any) => {
      if (!context.raw) return 'rgba(0,0,0,0.2)';
      
      // Make hover state slightly brighter
      const backgroundColor = dataset.backgroundColor;
      if (typeof backgroundColor === 'function') {
        const baseColor = backgroundColor(context);
        return baseColor + 'E0'; // Add transparency
      }
      return 'rgba(0,0,0,0.2)';
    };

    return chartData;
  };

  // Initialize chart on mount
  useEffect(() => {
    initializeChart();
    
    return () => {
      destroyChart();
    };
  }, []);

  // Update chart when data or options change
  useEffect(() => {
    if (chartInstance) {
      updateChart();
    }
  }, [data, options, colorScale, showValues, cellRadius]);

  return (
    <div 
      className={`heatmap-chart-container ${className}`}
      style={{ height, width, position: 'relative' }}
    >
      <canvas
        id={id}
        ref={chartRef}
        aria-label="Heatmap Chart"
        role="img"
      />
    </div>
  );
};

/**
 * Sets up event listeners for the chart
 * 
 * @param chart - The Chart.js instance
 * @param onCellClick - Callback function when a cell is clicked
 */
const setupChartEvents = (chart: Chart, onCellClick: (cellData: any) => void) => {
  if (!chart || !onCellClick) return;

  const canvas = chart.canvas;
  
  const clickHandler = (e: MouseEvent) => {
    const elements = chart.getElementsAtEventForMode(
      e,
      'nearest',
      { intersect: true },
      false
    );

    if (elements.length > 0) {
      const { datasetIndex, index } = elements[0];
      const dataset = chart.data.datasets[datasetIndex];
      const cellData = dataset.data[index];
      
      onCellClick({
        x: cellData.x,
        y: cellData.y,
        value: cellData.value,
        normalizedValue: cellData.normalizedValue,
        xLabel: chart.data.labels?.[Math.round(cellData.x)],
        yLabel: chart.options.scales?.y?.ticks?.callback?.(cellData.y),
        originalEvent: e
      });
    }
  };

  canvas.addEventListener('click', clickHandler);
  
  // Return cleanup function to remove event listener
  return () => {
    canvas.removeEventListener('click', clickHandler);
  };
};

/**
 * Generates a color gradient array for heatmap values
 * 
 * @param colorScale - Array of color hex codes
 * @param steps - Number of color steps to generate
 * @returns Array of hex color codes for gradient
 */
const generateColorGradient = (colorScale: string[], steps: number = 100): string[] => {
  if (!colorScale || colorScale.length === 0) {
    return ['#d4f7ff', '#0088aa', '#005566']; // Default blue gradient
  }
  
  if (colorScale.length === 1) {
    return [colorScale[0]];
  }
  
  const gradient: string[] = [];
  
  // For each step, calculate the interpolated color
  for (let i = 0; i < steps; i++) {
    const normalizedPosition = i / (steps - 1);
    const segment = Math.min(colorScale.length - 2, Math.floor(normalizedPosition * (colorScale.length - 1)));
    const remainder = (normalizedPosition * (colorScale.length - 1)) - segment;
    
    // Get colors for interpolation
    const color1 = colorScale[segment];
    const color2 = colorScale[segment + 1];
    
    // Simple linear interpolation between colors
    // Convert hex to rgb, interpolate, then back to hex
    const r1 = parseInt(color1.substring(1, 3), 16);
    const g1 = parseInt(color1.substring(3, 5), 16);
    const b1 = parseInt(color1.substring(5, 7), 16);
    
    const r2 = parseInt(color2.substring(1, 3), 16);
    const g2 = parseInt(color2.substring(3, 5), 16);
    const b2 = parseInt(color2.substring(5, 7), 16);
    
    const r = Math.round(r1 + remainder * (r2 - r1));
    const g = Math.round(g1 + remainder * (g2 - g1));
    const b = Math.round(b1 + remainder * (b2 - b1));
    
    const hexColor = `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
    gradient.push(hexColor);
  }
  
  return gradient;
};

export default HeatmapChart;
export { HeatmapChartProps };