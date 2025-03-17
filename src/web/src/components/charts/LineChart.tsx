import React, { useRef, useEffect, useState } from 'react';
import Chart from 'chart.js/auto'; // version: ^4.3.0
import { ChartOptions, ChartData } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21

import { 
  formatChartData, 
  formatAxisLabel, 
  formatTooltipLabel, 
  applyChartAnimation 
} from '../../services/charts/chartUtils';
import { transformTimeSeriesData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Props interface for the LineChart component
 */
export interface LineChartProps {
  /** Raw data for the chart or pre-formatted ChartData object */
  data: any[] | Record<string, any>;
  /** Custom chart options to merge with defaults */
  options?: Record<string, any>;
  /** Chart height (number for pixels, string for other units) */
  height?: number | string;
  /** Chart width (number for pixels, string for other units) */
  width?: number | string;
  /** Additional CSS class for container */
  className?: string;
  /** HTML ID for the canvas element */
  id?: string;
  /** Line tension (curvature) between 0 and 1 */
  lineTension?: number;
  /** Radius of data points in pixels */
  pointRadius?: number;
  /** Callback for clicking on data points */
  onDataPointClick?: (point: any) => void;
}

/**
 * A reusable line chart component that visualizes time series data
 * with customizable styling, animations, and interactive features
 */
const LineChart: React.FC<LineChartProps> = ({
  data,
  options = {},
  height = 300,
  width = '100%',
  className = '',
  id = 'line-chart',
  lineTension = 0.4,
  pointRadius = 3,
  onDataPointClick
}) => {
  const chartRef = useRef<HTMLCanvasElement>(null);
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  // Initialize chart when component mounts or when data/options change
  useEffect(() => {
    if (chartInstance) {
      // Update chart if it already exists
      updateChart();
    } else {
      // Initialize chart if it doesn't exist
      initializeChart();
    }

    // Cleanup when component unmounts
    return () => {
      destroyChart();
    };
  }, [data, options, lineTension, pointRadius]);

  // Handle window resize events
  useEffect(() => {
    const handleResize = () => {
      if (chartInstance) {
        chartInstance.resize();
      }
    };

    window.addEventListener('resize', handleResize);
    
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, [chartInstance]);

  // Setup click event handlers when chart instance changes
  useEffect(() => {
    if (!chartInstance || !onDataPointClick) return;
    
    const canvas = chartInstance.canvas;
    if (!canvas) return;
    
    const handleClick = (e: MouseEvent) => {
      const elements = chartInstance.getElementsAtEventForMode(
        e,
        'nearest',
        { intersect: true },
        false
      );
      
      if (elements.length > 0) {
        const { datasetIndex, index } = elements[0];
        const dataPoint = {
          dataset: chartInstance.data.datasets[datasetIndex],
          label: chartInstance.data.labels?.[index],
          value: chartInstance.data.datasets[datasetIndex].data[index],
          index
        };
        
        onDataPointClick(dataPoint);
      }
    };
    
    canvas.addEventListener('click', handleClick);
    
    return () => {
      canvas.removeEventListener('click', handleClick);
    };
  }, [chartInstance, onDataPointClick]);

  /**
   * Creates and initializes the Chart.js instance with line chart configuration
   */
  const initializeChart = () => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;

    // Transform the data for Chart.js
    const transformedData = transformTimeSeriesData(data, options);
    
    // Get chart configuration with default options
    const chartConfig = getChartConfig('line', options);
    
    // Apply specific line styling
    const styledData = applyLineStyles(transformedData);

    // Merge default configuration with provided options
    const mergedConfig = merge({}, chartConfig, options);
    
    // Apply animations if enabled
    const animatedConfig = applyChartAnimation(mergedConfig, 'line', true);

    // Create the chart
    const chart = new Chart(ctx, {
      type: 'line',
      data: styledData,
      options: animatedConfig
    });

    // Store chart instance
    setChartInstance(chart);
  };

  /**
   * Updates the chart with new data or options
   */
  const updateChart = () => {
    if (!chartInstance) return;

    // Transform the data for Chart.js
    const transformedData = transformTimeSeriesData(data, options);
    
    // Apply specific line styling
    const styledData = applyLineStyles(transformedData);

    // Update chart data and options
    chartInstance.data = styledData;
    
    if (options) {
      // Merge new options
      chartInstance.options = merge({}, chartInstance.options, options);
    }

    // Update the chart
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
   * Applies line-specific styles to the chart datasets
   */
  const applyLineStyles = (chartData: ChartData): ChartData => {
    const lineData = { ...chartData };
    
    // Apply styling to each dataset
    if (lineData.datasets) {
      lineData.datasets = lineData.datasets.map((dataset, index) => {
        // Get color from theme if not already specified
        const colorKeys = Object.keys(colors.chart);
        const color = dataset.borderColor || colors.chart[colorKeys[index % colorKeys.length]];
        
        return {
          ...dataset,
          tension: lineTension,
          borderWidth: 2,
          borderColor: color,
          backgroundColor: dataset.backgroundColor || 'transparent',
          pointRadius: pointRadius,
          pointHoverRadius: pointRadius + 2,
          pointBackgroundColor: color,
          pointBorderColor: '#fff',
          pointBorderWidth: 1,
          pointHitRadius: 10
        };
      });
    }
    
    return lineData;
  };

  return (
    <div 
      className={`line-chart-container ${className}`} 
      style={{ height, width, position: 'relative' }}
    >
      <canvas 
        ref={chartRef}
        id={id}
        aria-label="Line chart"
        role="img"
      />
    </div>
  );
};

/**
 * Sets up event listeners for the chart
 * 
 * @param chart - Chart.js instance
 * @param onDataPointClick - Click handler function
 * @returns Cleanup function to remove listeners
 */
export const setupChartEvents = (chart: Chart, onDataPointClick: (point: any) => void) => {
  // Get the canvas element
  const canvas = chart.canvas;
  
  if (!canvas) return () => {};
  
  // Add click event listener
  const handleClick = (e: MouseEvent) => {
    const elements = chart.getElementsAtEventForMode(
      e,
      'nearest',
      { intersect: true },
      false
    );
    
    if (elements.length > 0) {
      const { datasetIndex, index } = elements[0];
      const dataPoint = {
        dataset: chart.data.datasets[datasetIndex],
        label: chart.data.labels?.[index],
        value: chart.data.datasets[datasetIndex].data[index],
        index
      };
      
      onDataPointClick(dataPoint);
    }
  };
  
  canvas.addEventListener('click', handleClick);
  
  // Return cleanup function
  return () => {
    canvas.removeEventListener('click', handleClick);
  };
};

export default LineChart;