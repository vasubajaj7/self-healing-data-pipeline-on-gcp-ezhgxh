import React, { useRef, useEffect, useState } from 'react';
import Chart from 'chart.js/auto'; // version: ^4.3.0
import { ChartData, ChartOptions } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21

import { 
  formatChartData,
  formatTooltipLabel,
  applyChartAnimation
} from '../../services/charts/chartUtils';
import { transformDistributionData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Props for the PieChart component
 */
export interface PieChartProps {
  /** Data to display in the chart */
  data: any[] | { labels: string[]; datasets: any[] };
  /** Custom chart options */
  options?: ChartOptions;
  /** Chart height */
  height?: number | string;
  /** Chart width */
  width?: number | string;
  /** Additional CSS class name */
  className?: string;
  /** Component ID */
  id?: string;
  /** Cutout percentage (0 for pie, higher for doughnut) */
  cutout?: number | string;
  /** Hover offset in pixels */
  hoverOffset?: number;
  /** Border width in pixels */
  borderWidth?: number;
  /** Color context for theme consistency */
  colorContext?: string;
  /** Click handler for pie segments */
  onSegmentClick?: (data: any) => void;
}

/**
 * A reusable pie chart component for visualizing distribution data
 * such as quality scores, self-healing success rates, and resource allocation.
 */
const PieChart: React.FC<PieChartProps> = ({
  data,
  options = {},
  height = 300,
  width = '100%',
  className = '',
  id,
  cutout = 0,
  hoverOffset = 20,
  borderWidth = 1,
  colorContext = 'pipeline',
  onSegmentClick
}) => {
  const chartRef = useRef<HTMLCanvasElement>(null);
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  // Initialize the chart when the component mounts
  useEffect(() => {
    if (chartRef.current) {
      initializeChart();
    }

    // Clean up the chart when the component unmounts
    return () => {
      destroyChart();
    };
  }, []);

  // Update the chart when props change
  useEffect(() => {
    if (chartInstance && data) {
      updateChart();
    }
  }, [data, options, cutout, hoverOffset, borderWidth, colorContext]);

  // Set up event listeners for click interactions
  useEffect(() => {
    if (chartInstance && onSegmentClick) {
      const chart = chartInstance;
      
      const clickHandler = (e: any) => {
        handleSegmentClick(e);
      };
      
      chart.canvas.addEventListener('click', clickHandler);
      
      return () => {
        chart.canvas.removeEventListener('click', clickHandler);
      };
    }
  }, [chartInstance, onSegmentClick]);

  /**
   * Initializes the Chart.js instance
   */
  const initializeChart = () => {
    if (!chartRef.current) return;
    
    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;

    // Transform data for pie chart format
    const transformedData = transformDistributionData(data, {
      colorContext,
      showPercentages: true,
      calculatePercentages: true
    });

    // Get base configuration for pie chart
    const baseConfig = getChartConfig('pie');
    
    // Apply pie chart specific styling
    const chartData = applyPieStyles(transformedData);
    
    // Merge custom options with default configuration
    const mergedOptions = merge({}, baseConfig, {
      cutout: cutout,
      plugins: {
        tooltip: {
          callbacks: {
            label: (context: any) => formatTooltipLabel(context, colorContext)
          }
        }
      }
    }, options);

    // Apply animations
    const animatedOptions = applyChartAnimation(mergedOptions, 'pie', true);

    // Create chart instance
    const newChartInstance = new Chart(ctx, {
      type: 'pie',
      data: chartData,
      options: animatedOptions
    });

    setChartInstance(newChartInstance);
  };

  /**
   * Updates the chart with new data or options
   */
  const updateChart = () => {
    if (!chartInstance) return;

    // Transform and update the data
    const transformedData = transformDistributionData(data, {
      colorContext,
      showPercentages: true,
      calculatePercentages: true
    });
    
    const chartData = applyPieStyles(transformedData);
    
    // Update chart data and options
    chartInstance.data = chartData;
    
    if (options) {
      const baseConfig = getChartConfig('pie');
      const mergedOptions = merge({}, baseConfig, {
        cutout: cutout,
        plugins: {
          tooltip: {
            callbacks: {
              label: (context: any) => formatTooltipLabel(context, colorContext)
            }
          }
        }
      }, options);
      
      chartInstance.options = mergedOptions;
    }
    
    chartInstance.update();
  };

  /**
   * Cleans up the chart instance
   */
  const destroyChart = () => {
    if (chartInstance) {
      chartInstance.destroy();
      setChartInstance(null);
    }
  };

  /**
   * Applies pie-specific styles to chart datasets
   */
  const applyPieStyles = (chartData: ChartData): ChartData => {
    const data = { ...chartData };

    if (data.datasets) {
      data.datasets = data.datasets.map(dataset => {
        return {
          ...dataset,
          borderWidth: borderWidth,
          hoverOffset: hoverOffset,
          hoverBorderWidth: borderWidth + 1,
          hoverBorderColor: 'white'
        };
      });
    }

    return data;
  };

  /**
   * Handles click events on pie segments
   */
  const handleSegmentClick = (event: Event) => {
    if (!chartInstance || !onSegmentClick) return;

    const activeElements = chartInstance.getElementsAtEventForMode(
      event as unknown as Event, 
      'nearest', 
      { intersect: true }, 
      false
    );

    if (activeElements.length > 0) {
      const { index } = activeElements[0];
      const value = chartInstance.data.datasets[0].data[index];
      const label = chartInstance.data.labels?.[index];
      
      onSegmentClick({
        index,
        value,
        label,
        dataset: chartInstance.data.datasets[0],
        raw: data
      });
    }
  };

  return (
    <div 
      style={{ 
        width, 
        height, 
        position: 'relative' 
      }}
      className={className}
    >
      <canvas 
        ref={chartRef}
        id={id}
        aria-label="Pie chart"
        role="img"
      />
    </div>
  );
};

export default PieChart;