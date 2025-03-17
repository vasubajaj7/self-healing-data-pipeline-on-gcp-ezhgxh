import React, { useRef, useEffect, useState } from 'react'; // version: ^18.2.0
import { Chart, ChartOptions, ChartData } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash'; // version: ^4.17.21

import {
  formatChartData,
  formatAxisLabel,
  formatTooltipLabel,
  applyChartAnimation
} from '../../services/charts/chartUtils';
import { transformComparisonData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { chart as chartColors } from '../../theme/colors';

/**
 * Props interface for the BarChart component
 */
export interface BarChartProps {
  /** Raw data for the chart */
  data: any;
  /** Custom chart options to override defaults */
  options?: Partial<ChartOptions>;
  /** Chart height in pixels or CSS value */
  height?: number | string;
  /** Chart width in pixels or CSS value */
  width?: number | string;
  /** Additional CSS class names */
  className?: string;
  /** Unique identifier for the canvas element */
  id?: string;
  /** Whether to display bars horizontally (false for vertical) */
  horizontal?: boolean;
  /** Whether to stack bars on top of each other */
  stacked?: boolean;
  /** Thickness of bars in pixels */
  barThickness?: number;
  /** Border radius of bars in pixels */
  borderRadius?: number;
  /** Callback function when a bar is clicked */
  onBarClick?: (data: any) => void;
}

/**
 * A reusable bar chart component that visualizes categorical comparison data
 * with customizable styling, animations, and interactive features.
 */
const BarChart: React.FC<BarChartProps> = (props) => {
  const {
    data,
    options,
    height = 300,
    width = '100%',
    className = '',
    id,
    horizontal = false,
    stacked = false,
    barThickness,
    borderRadius,
    onBarClick
  } = props;

  const chartRef = useRef<HTMLCanvasElement>(null);
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  // Initialize chart when component mounts
  useEffect(() => {
    if (chartRef.current) {
      initializeChart();
    }

    // Cleanup chart when component unmounts
    return () => {
      destroyChart();
    };
  }, []);

  // Update chart when props change
  useEffect(() => {
    if (chartInstance) {
      updateChart();
    }
  }, [data, options, horizontal, stacked, barThickness, borderRadius]);

  /**
   * Creates and initializes the Chart.js instance with bar chart configuration
   */
  const initializeChart = () => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;

    // Transform data for Chart.js consumption
    const transformedData = transformComparisonData(data, {
      horizontal,
      stacked
    });

    // Get default bar chart configuration
    let chartOptions = getChartConfig('bar');
    
    // Configure horizontal bar orientation if specified
    if (horizontal) {
      chartOptions.indexAxis = 'y';
    }
    
    // Configure stacked option
    if (chartOptions.scales) {
      if (chartOptions.scales.x) {
        chartOptions.scales.x.stacked = stacked;
      }
      if (chartOptions.scales.y) {
        chartOptions.scales.y.stacked = stacked;
      }
    }
    
    // Merge with custom options
    if (options) {
      chartOptions = merge({}, chartOptions, options);
    }
    
    // Apply animations
    chartOptions = applyChartAnimation(chartOptions, 'bar', true);
    
    // Apply bar styling
    const styledData = applyBarStyles(transformedData);

    // Create chart instance
    const newChartInstance = new Chart(ctx, {
      type: 'bar',
      data: styledData,
      options: chartOptions
    });

    // Set up event handlers
    if (onBarClick) {
      setupChartEvents(newChartInstance, onBarClick);
    }

    setChartInstance(newChartInstance);
  };

  /**
   * Updates the chart with new data or options
   */
  const updateChart = () => {
    if (!chartInstance) return;

    // Transform data for Chart.js consumption
    const transformedData = transformComparisonData(data, {
      horizontal,
      stacked
    });

    // Apply bar styling
    const styledData = applyBarStyles(transformedData);

    // Update chart data
    chartInstance.data = styledData;
    
    // Update options
    let updatedOptions = { ...chartInstance.options };
    
    // Update horizontal orientation
    updatedOptions.indexAxis = horizontal ? 'y' : 'x';
    
    // Update stacked option
    if (updatedOptions.scales) {
      if (updatedOptions.scales.x) {
        updatedOptions.scales.x.stacked = stacked;
      }
      if (updatedOptions.scales.y) {
        updatedOptions.scales.y.stacked = stacked;
      }
    }
    
    // Merge with custom options
    if (options) {
      updatedOptions = merge({}, updatedOptions, options);
    }
    
    chartInstance.options = updatedOptions;

    // Update chart
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
   * Applies bar-specific styles to the chart datasets
   * 
   * @param chartData - Chart data to style
   * @returns Updated chart data with bar styles
   */
  const applyBarStyles = (chartData: ChartData): ChartData => {
    const updatedData = { ...chartData };
    
    // Define color sequence from the theme
    const colorList = [
      chartColors.blue,
      chartColors.green,
      chartColors.purple,
      chartColors.orange,
      chartColors.red,
      chartColors.teal,
      chartColors.cyan,
      chartColors.lime,
      chartColors.amber,
      chartColors.indigo
    ];
    
    // Apply styles to each dataset
    updatedData.datasets = updatedData.datasets.map((dataset, index) => {
      // Use dataset color or pick from color list
      const color = dataset.backgroundColor as string || colorList[index % colorList.length];
      
      return {
        ...dataset,
        barThickness,
        borderRadius: borderRadius ?? 4, // Use nullish coalescing
        backgroundColor: color,
        hoverBackgroundColor: `${color}D0`, // Add transparency for hover
        borderWidth: 1,
        borderColor: color,
      };
    });
    
    return updatedData;
  };

  return (
    <canvas
      ref={chartRef}
      height={height}
      width={width}
      className={`bar-chart ${className}`}
      id={id}
      aria-label="Bar chart visualization"
      role="img"
    />
  );
};

/**
 * Sets up event listeners for the chart
 * 
 * @param chart - Chart.js instance
 * @param onBarClick - Click handler callback
 */
const setupChartEvents = (chart: Chart, onBarClick: (data: any) => void) => {
  const handleClick = (event: Event) => {
    const points = chart.getElementsAtEventForMode(
      event,
      'nearest',
      { intersect: true },
      false
    );
    
    if (points.length) {
      const firstPoint = points[0];
      const datasetIndex = firstPoint.datasetIndex;
      const index = firstPoint.index;
      
      const datasetLabel = chart.data.datasets[datasetIndex].label;
      const value = chart.data.datasets[datasetIndex].data[index];
      const label = chart.data.labels?.[index];
      
      onBarClick({
        datasetIndex,
        index,
        datasetLabel,
        value,
        label
      });
    }
  };

  chart.canvas.addEventListener('click', handleClick);
};

export default BarChart;