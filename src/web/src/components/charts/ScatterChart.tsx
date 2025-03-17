import React from 'react'; // version: ^18.2.0
import Chart from 'chart.js/auto'; // version: ^4.3.0
import { ChartData, ChartOptions } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash/merge'; // version: ^4.17.21

import { 
  formatChartData, 
  formatAxisLabel, 
  formatTooltipLabel, 
  applyChartAnimation 
} from '../../services/charts/chartUtils';
import { transformScatterData } from '../../services/charts/dataTransformers';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';

/**
 * Props interface for the ScatterChart component
 */
export interface ScatterChartProps {
  data: any[] | Record<string, any>; // Chart data in various formats
  options?: Record<string, any>; // Custom chart options
  height?: number | string; // Chart height
  width?: number | string; // Chart width
  className?: string; // CSS class name
  id?: string; // Element ID
  pointRadius?: number; // Size of data points
  pointStyle?: string; // Style of data points
  showTrendline?: boolean; // Whether to show trend line
  onPointClick?: (pointData: any) => void; // Point click handler
}

/**
 * Sets up event listeners for the chart
 * 
 * @param chart - Chart.js instance
 * @param onPointClick - Click event callback function
 * @returns Cleanup function to remove listeners
 */
export function setupChartEvents(chart: Chart, onPointClick?: (pointData: any) => void): void {
  if (!chart.canvas || !onPointClick) return;
  
  const handleClick = (event: Event) => {
    const points = chart.getElementsAtEventForMode(
      event as unknown as Event, 
      'nearest', 
      { intersect: true }, 
      false
    );
    
    if (points.length) {
      const datasetIndex = points[0].datasetIndex;
      const index = points[0].index;
      const pointData = chart.data.datasets[datasetIndex].data[index];
      
      onPointClick({
        point: pointData,
        dataset: chart.data.datasets[datasetIndex],
        index,
        datasetIndex
      });
    }
  };
  
  chart.canvas.addEventListener('click', handleClick);
  
  // Return cleanup function to remove event listeners
  return () => {
    if (chart.canvas) {
      chart.canvas.removeEventListener('click', handleClick);
    }
  };
}

/**
 * A React component that renders a scatter chart using Chart.js
 * 
 * Visualizes correlation data with customizable styling, animations, 
 * and interactive features for data analysis in the self-healing pipeline dashboard.
 */
class ScatterChart extends React.Component<ScatterChartProps> {
  chartRef: React.RefObject<HTMLCanvasElement>;
  chartInstance: Chart | null;
  
  constructor(props: ScatterChartProps) {
    super(props);
    this.chartRef = React.createRef<HTMLCanvasElement>();
    this.chartInstance = null;
  }
  
  componentDidMount() {
    this.initializeChart();
  }
  
  componentDidUpdate(prevProps: ScatterChartProps) {
    if (
      prevProps.data !== this.props.data ||
      prevProps.options !== this.props.options ||
      prevProps.pointRadius !== this.props.pointRadius ||
      prevProps.pointStyle !== this.props.pointStyle ||
      prevProps.showTrendline !== this.props.showTrendline
    ) {
      this.updateChart();
    }
  }
  
  componentWillUnmount() {
    this.destroyChart();
  }
  
  /**
   * Creates and initializes the Chart.js instance with scatter chart configuration
   */
  initializeChart() {
    if (!this.chartRef.current) return;
    
    const ctx = this.chartRef.current.getContext('2d');
    if (!ctx) return;
    
    // Transform the data for scatter chart
    const chartData = transformScatterData(this.props.data);
    
    // Apply point styling
    const styledData = this.applyPointStyles(chartData);
    
    // Get the base chart configuration
    const chartConfig = getChartConfig('scatter');
    
    // Merge with custom options
    const mergedOptions = merge({}, chartConfig, this.props.options);
    
    // Apply animations
    const animatedOptions = applyChartAnimation(mergedOptions, 'scatter', true);
    
    // Add trendline if requested
    if (this.props.showTrendline && styledData.datasets && styledData.datasets.length > 0) {
      this.addTrendline(styledData);
    }
    
    // Create new chart instance
    this.chartInstance = new Chart(ctx, {
      type: 'scatter',
      data: styledData,
      options: animatedOptions
    });
    
    // Set up click events if callback provided
    if (this.props.onPointClick && this.chartInstance) {
      setupChartEvents(this.chartInstance, this.props.onPointClick);
    }
  }
  
  /**
   * Updates the chart with new data or options
   */
  updateChart() {
    if (!this.chartInstance) return;
    
    // Transform the data for scatter chart
    const chartData = transformScatterData(this.props.data);
    
    // Apply point styling
    const styledData = this.applyPointStyles(chartData);
    
    // Add trendline if requested
    if (this.props.showTrendline && styledData.datasets && styledData.datasets.length > 0) {
      this.addTrendline(styledData);
    }
    
    // Update chart data
    this.chartInstance.data = styledData;
    
    // Update chart options
    if (this.props.options) {
      this.chartInstance.options = merge({}, this.chartInstance.options, this.props.options);
    }
    
    // Update chart
    this.chartInstance.update();
  }
  
  /**
   * Cleans up the Chart.js instance
   */
  destroyChart() {
    if (this.chartInstance) {
      this.chartInstance.destroy();
      this.chartInstance = null;
    }
  }
  
  /**
   * Applies scatter-specific styles to the chart datasets
   * 
   * @param chartData - Original chart data
   * @returns Updated chart data with point styles
   */
  applyPointStyles(chartData: ChartData): ChartData {
    if (!chartData.datasets) return chartData;
    
    return {
      ...chartData,
      datasets: chartData.datasets.map((dataset, index) => {
        return {
          ...dataset,
          pointRadius: dataset.pointRadius || this.props.pointRadius || 5,
          pointStyle: dataset.pointStyle || this.props.pointStyle || 'circle',
          backgroundColor: dataset.backgroundColor || colors.chart.blue,
          borderColor: dataset.borderColor || colors.chart.blue,
          borderWidth: dataset.borderWidth || 1,
          hoverRadius: (dataset.pointRadius || this.props.pointRadius || 5) + 2,
          hoverBorderWidth: 2,
          hoverBackgroundColor: dataset.hoverBackgroundColor || `${dataset.backgroundColor || colors.chart.blue}D0`
        };
      })
    };
  }
  
  /**
   * Adds a trendline dataset to the chart data
   * 
   * @param chartData - Chart data to add trendline to
   */
  private addTrendline(chartData: ChartData): void {
    if (!chartData.datasets || chartData.datasets.length === 0) return;
    
    // Use first dataset for trendline calculation
    const dataset = chartData.datasets[0];
    const points = dataset.data as {x: number, y: number}[];
    
    if (!Array.isArray(points) || points.length < 2) return;
    
    // Simple linear regression
    let sumX = 0;
    let sumY = 0;
    let sumXY = 0;
    let sumX2 = 0;
    let n = 0;
    
    for (const point of points) {
      if (point && typeof point.x === 'number' && typeof point.y === 'number') {
        sumX += point.x;
        sumY += point.y;
        sumXY += point.x * point.y;
        sumX2 += point.x * point.x;
        n++;
      }
    }
    
    if (n < 2) return; // Need at least 2 valid points
    
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;
    
    // Find min and max x values for the line
    let minX = Number.MAX_VALUE;
    let maxX = Number.MIN_VALUE;
    
    for (const point of points) {
      if (point && typeof point.x === 'number') {
        minX = Math.min(minX, point.x);
        maxX = Math.max(maxX, point.x);
      }
    }
    
    if (minX === Number.MAX_VALUE || maxX === Number.MIN_VALUE) return;
    
    // Add trendline dataset
    chartData.datasets.push({
      type: 'line',
      label: 'Trendline',
      data: [
        { x: minX, y: minX * slope + intercept },
        { x: maxX, y: maxX * slope + intercept }
      ],
      borderColor: 'rgba(0, 0, 0, 0.5)',
      borderWidth: 2,
      borderDash: [5, 5],
      fill: false,
      pointRadius: 0,
      order: 1 // Ensure trendline is drawn on top
    });
  }
  
  /**
   * Handles click events on data points
   * 
   * @param event - Click event
   */
  handlePointClick = (event: Event) => {
    if (!this.chartInstance || !this.props.onPointClick) return;
    
    const points = this.chartInstance.getElementsAtEventForMode(
      event as unknown as Event, 
      'nearest', 
      { intersect: true }, 
      false
    );
    
    if (points.length) {
      const datasetIndex = points[0].datasetIndex;
      const index = points[0].index;
      const pointData = this.chartInstance.data.datasets[datasetIndex].data[index];
      
      this.props.onPointClick({
        point: pointData,
        dataset: this.chartInstance.data.datasets[datasetIndex],
        index,
        datasetIndex
      });
    }
  }
  
  /**
   * Renders the scatter chart component
   * 
   * @returns Rendered component
   */
  render() {
    const { 
      height = 300, 
      width = '100%', 
      className = '', 
      id = 'scatter-chart' 
    } = this.props;
    
    return (
      <canvas
        ref={this.chartRef}
        height={height}
        width={width}
        className={className}
        id={id}
        aria-label="Scatter Chart"
        role="img"
      />
    );
  }
}

export default ScatterChart;