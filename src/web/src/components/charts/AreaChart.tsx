import React, { useRef, useEffect, useState } from 'react';
import { Chart, ChartOptions, ChartData } from 'chart.js/auto'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21

// Internal imports
import { 
  formatChartData,
  formatAxisLabel, 
  formatTooltipLabel,
  applyChartAnimation,
  createGradient
} from '../../services/charts/chartUtils';
import { transformTimeSeriesData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Props interface for the AreaChart component
 */
export interface AreaChartProps {
  /** Data to be visualized in the chart */
  data: any[] | object;
  /** Custom chart options to override defaults */
  options?: object;
  /** Chart height (in pixels or CSS value) */
  height?: number | string;
  /** Chart width (in pixels or CSS value) */
  width?: number | string;
  /** Additional CSS class names */
  className?: string;
  /** Unique identifier for the chart */
  id?: string;
  /** Line tension/smoothing (0-1) */
  lineTension?: number;
  /** Radius of data points */
  pointRadius?: number;
  /** Opacity of the area fill (0-1) */
  fillOpacity?: number;
  /** Whether to use gradient fills */
  useGradientFill?: boolean;
  /** Callback for data point click events */
  onDataPointClick?: (point: any, event: any) => void;
}

/**
 * A reusable area chart component that visualizes time series data with filled areas,
 * customizable styling, animations, and interactive features.
 */
const AreaChart: React.FC<AreaChartProps> = ({
  data,
  options = {},
  height = 300,
  width = '100%',
  className = '',
  id,
  lineTension = 0.4,
  pointRadius = 3,
  fillOpacity = 0.7,
  useGradientFill = true,
  onDataPointClick
}) => {
  // Create ref for canvas element
  const chartRef = useRef<HTMLCanvasElement>(null);
  // State to track chart instance
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  /**
   * Creates and initializes the Chart.js instance with area chart configuration
   */
  const initializeChart = (): void => {
    if (!chartRef.current) return;
    
    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;
    
    // Transform the data for area chart visualization
    const transformedData = transformTimeSeriesData(
      { data: Array.isArray(data) ? data : [data] },
      { fillArea: true, smoothing: lineTension }
    );
    
    // Get default area chart configuration
    const defaultOptions = getChartConfig('area');
    
    // Apply area-specific styling to the data
    const styledData = applyAreaStyles(transformedData, ctx);
    
    // Merge custom options with defaults
    const chartOptions = merge({}, defaultOptions, options);
    
    // Apply animations
    const animatedOptions = applyChartAnimation(chartOptions, 'area');
    
    // Create the chart instance
    const newChartInstance = new Chart(ctx, {
      type: 'line', // Line chart type but styled as area
      data: styledData,
      options: animatedOptions
    });
    
    setChartInstance(newChartInstance);
  };

  /**
   * Updates the chart with new data or options
   */
  const updateChart = (): void => {
    if (!chartInstance || !chartRef.current) return;
    
    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;
    
    // Transform the data for area chart visualization
    const transformedData = transformTimeSeriesData(
      { data: Array.isArray(data) ? data : [data] },
      { fillArea: true, smoothing: lineTension }
    );
    
    // Apply area-specific styling to the data
    const styledData = applyAreaStyles(transformedData, ctx);
    
    // Update the chart data
    chartInstance.data = styledData;
    
    // Update options if provided
    if (options) {
      chartInstance.options = merge({}, chartInstance.options, options);
    }
    
    // Update the chart
    chartInstance.update();
  };

  /**
   * Cleans up the Chart.js instance
   */
  const destroyChart = (): void => {
    if (chartInstance) {
      chartInstance.destroy();
      setChartInstance(null);
    }
  };

  /**
   * Applies area-specific styles to the chart datasets
   * @param data Chart data
   * @param ctx Canvas rendering context
   * @returns Updated chart data with area styles
   */
  const applyAreaStyles = (data: ChartData, ctx: CanvasRenderingContext2D): ChartData => {
    // Clone data to avoid modifying original
    const styledData = JSON.parse(JSON.stringify(data));
    
    // Apply styles to each dataset
    styledData.datasets = styledData.datasets.map((dataset: any, index: number) => {
      // Set fill to true for area appearance
      dataset.fill = true;
      
      // Apply gradient if enabled
      if (useGradientFill && ctx) {
        const color = dataset.borderColor || colors.chart.blue;
        dataset.backgroundColor = createGradient(
          ctx,
          color,
          'rgba(255, 255, 255, 0.1)',
          {
            vertical: true,
            height: typeof height === 'number' ? height : 300,
            stops: [
              { offset: 0, color: `${color}${Math.round(fillOpacity * 255).toString(16)}` },
              { offset: 1, color: `${color}00` } // Transparent at bottom
            ]
          }
        );
      } else {
        // Use semi-transparent fill
        const color = dataset.borderColor || colors.chart.blue;
        dataset.backgroundColor = `${color}${Math.round(fillOpacity * 255).toString(16)}`;
      }
      
      // Configure line styling
      dataset.borderWidth = 2;
      dataset.tension = lineTension;
      
      // Configure point styling
      dataset.pointRadius = pointRadius;
      dataset.pointHoverRadius = pointRadius + 2;
      dataset.pointBackgroundColor = dataset.borderColor;
      dataset.pointBorderColor = '#fff';
      dataset.pointBorderWidth = 1;
      
      return dataset;
    });
    
    return styledData;
  };

  /**
   * Handles click events on data points
   * @param event Click event
   */
  const handleDataPointClick = (event: Event): void => {
    if (!chartInstance || !onDataPointClick) return;
    
    const chart = chartInstance;
    const points = chart.getElementsAtEventForMode(
      event as unknown as Event, 
      'nearest', 
      { intersect: true }, 
      false
    );
    
    if (points.length > 0) {
      const firstPoint = points[0];
      const datasetIndex = firstPoint.datasetIndex;
      const index = firstPoint.index;
      
      const dataPoint = {
        dataset: chart.data.datasets[datasetIndex],
        dataIndex: index,
        value: chart.data.datasets[datasetIndex].data[index],
        label: chart.data.labels ? chart.data.labels[index] : '',
      };
      
      onDataPointClick(dataPoint, event);
    }
  };

  // Initialize chart when component mounts
  useEffect(() => {
    initializeChart();
    
    // Clean up chart instance when component unmounts
    return () => {
      destroyChart();
    };
  }, []);

  // Update chart when props change
  useEffect(() => {
    if (chartInstance) {
      updateChart();
    }
  }, [data, options, lineTension, pointRadius, fillOpacity, useGradientFill]);

  // Set up event listeners for chart interactions
  useEffect(() => {
    if (!chartInstance || !onDataPointClick || !chartRef.current) return;
    
    // Add click event listener
    const canvas = chartRef.current;
    canvas.addEventListener('click', handleDataPointClick as EventListener);
    
    // Clean up event listener
    return () => {
      canvas.removeEventListener('click', handleDataPointClick as EventListener);
    };
  }, [chartInstance, onDataPointClick]);

  // Render the chart canvas
  return (
    <canvas
      ref={chartRef}
      height={height}
      width={width}
      className={className}
      id={id}
      aria-label="Area Chart"
      role="img"
    ></canvas>
  );
};

/**
 * Sets up event listeners for the chart
 * @param chart Chart instance 
 * @param onDataPointClick Callback for data point clicks
 * @returns Cleanup function to remove listeners
 */
export function setupChartEvents(chart: Chart, onDataPointClick?: Function): void {
  if (!chart || !onDataPointClick) return;
  
  const canvas = chart.canvas;
  if (!canvas) return;
  
  const handleClick = (event: Event) => {
    const points = chart.getElementsAtEventForMode(
      event as unknown as Event,
      'nearest',
      { intersect: true },
      false
    );
    
    if (points.length > 0) {
      const firstPoint = points[0];
      const datasetIndex = firstPoint.datasetIndex;
      const index = firstPoint.index;
      
      const dataPoint = {
        dataset: chart.data.datasets[datasetIndex],
        dataIndex: index,
        value: chart.data.datasets[datasetIndex].data[index],
        label: chart.data.labels ? chart.data.labels[index] : '',
      };
      
      onDataPointClick(dataPoint, event);
    }
  };
  
  // Attach event listener
  canvas.addEventListener('click', handleClick);
  
  // Return cleanup function
  return () => {
    canvas.removeEventListener('click', handleClick);
  };
}

export default AreaChart;