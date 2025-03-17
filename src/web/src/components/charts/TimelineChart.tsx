import React, { useRef, useEffect, useState } from 'react';
import Chart from 'chart.js/auto'; // version: ^4.3.0
import { ChartOptions, ChartData } from 'chart.js'; // version: ^4.3.0
import merge from 'lodash'; // version: ^4.17.21

import {
  formatChartData,
  formatDatetime,
  formatTooltipLabel,
  getStatusColor,
  applyChartAnimation
} from '../../services/charts/chartUtils';
import { getChartConfig } from '../../config/chartConfig';
import { colors } from '../../theme/colors';
import { PipelineStatus } from '../../types/dashboard';

/**
 * Interface defining a timeline event
 */
export interface TimelineEvent {
  id: string;
  title: string;
  category: string;
  startTime: Date | string | number;
  endTime: Date | string | number;
  status: string;
  description?: string;
  metadata?: Record<string, any>;
}

/**
 * Props for the TimelineChart component
 */
export interface TimelineChartProps {
  events: TimelineEvent[];
  options?: Record<string, any>;
  height?: number | string;
  width?: number | string;
  className?: string;
  id?: string;
  showLegend?: boolean;
  onEventClick?: (event: TimelineEvent) => void;
}

/**
 * A reusable timeline chart component for visualizing events over time
 * 
 * This component uses Chart.js to create horizontal bar charts representing
 * events across time, grouped by category. Each event is colored based on its
 * status and can be clicked to trigger custom actions.
 * 
 * @param props - The component props
 * @returns The timeline chart component
 */
const TimelineChart: React.FC<TimelineChartProps> = ({
  events = [],
  options = {},
  height = 400,
  width = '100%',
  className = '',
  id = 'timeline-chart',
  showLegend = true,
  onEventClick
}) => {
  const chartRef = useRef<HTMLCanvasElement>(null);
  const [chartInstance, setChartInstance] = useState<Chart | null>(null);

  // Initialize chart when component mounts
  useEffect(() => {
    if (chartRef.current) {
      initializeChart();
    }
    
    // Clean up chart when component unmounts
    return () => {
      destroyChart();
    };
  }, []);

  // Update chart when props change
  useEffect(() => {
    if (chartInstance) {
      updateChart();
    }
  }, [events, options, showLegend, height, width]);

  /**
   * Initialize Chart.js instance with timeline configuration
   */
  const initializeChart = () => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext('2d');
    if (!ctx) return;

    // Format timeline data
    const data = formatTimelineData(events);

    // Get base configuration with defaults
    const baseConfig = getChartConfig('bar');

    // Apply timeline-specific styling and options
    const timelineConfig = applyTimelineStyles(baseConfig);

    // Create chart instance
    const newChartInstance = new Chart(ctx, {
      type: 'bar',
      data,
      options: merge({}, timelineConfig, options)
    });

    // Setup event listeners if click handler provided
    if (onEventClick) {
      setupTimelineEvents(newChartInstance, onEventClick);
    }

    setChartInstance(newChartInstance);
  };

  /**
   * Update chart with new data or options
   */
  const updateChart = () => {
    if (!chartInstance) return;

    // Format updated data
    const data = formatTimelineData(events);

    // Update chart data
    chartInstance.data = data;
    
    // Update chart options
    const baseConfig = getChartConfig('bar');
    const timelineConfig = applyTimelineStyles(baseConfig);
    chartInstance.options = merge({}, timelineConfig, options);

    // Show/hide legend based on prop
    if (chartInstance.options.plugins && chartInstance.options.plugins.legend) {
      chartInstance.options.plugins.legend.display = showLegend;
    }

    // Refresh the chart
    chartInstance.update();
  };

  /**
   * Clean up Chart.js instance
   */
  const destroyChart = () => {
    if (chartInstance) {
      chartInstance.destroy();
      setChartInstance(null);
    }
  };

  /**
   * Format data for timeline chart
   * 
   * @param events - Array of timeline events
   * @returns Formatted chart data
   */
  const formatTimelineData = (events: TimelineEvent[]): ChartData => {
    if (!events.length) {
      return {
        labels: [],
        datasets: []
      };
    }

    // Group events by category
    const categories = [...new Set(events.map(event => event.category))];
    
    // Sort events chronologically
    const sortedEvents = [...events].sort((a, b) => {
      const aStart = new Date(a.startTime).getTime();
      const bStart = new Date(b.startTime).getTime();
      return aStart - bStart;
    });

    // Create dataset with events as bars
    const dataset = {
      label: 'Timeline Events',
      data: categories.map(category => {
        // For each category, find events in that category
        return sortedEvents
          .filter(event => event.category === category)
          .map(event => {
            const start = new Date(event.startTime).getTime();
            const end = new Date(event.endTime).getTime();
            
            // Use theme colors for status
            const statusColor = getStatusColor(event.status);
            
            return {
              x: [start, end],
              y: category,
              originalEvent: event, // Store original event for tooltip and click handling
              backgroundColor: statusColor,
              borderColor: statusColor,
              borderWidth: 1,
              borderRadius: 4,
            };
          });
      }).flat(),
    };

    return {
      labels: categories,
      datasets: [dataset]
    };
  };

  /**
   * Apply timeline-specific styles to chart configuration
   * 
   * @param options - Base chart options
   * @returns Updated chart options for timeline
   */
  const applyTimelineStyles = (options: ChartOptions): ChartOptions => {
    const timelineOptions = merge({}, options, {
      indexAxis: 'y', // Horizontal bar chart for timeline
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'hour',
            displayFormats: {
              hour: 'HH:mm',
              day: 'MMM d',
            },
            tooltipFormat: 'MMM d, yyyy HH:mm'
          },
          title: {
            display: true,
            text: 'Time'
          },
          grid: {
            color: colors.grey[200]
          }
        },
        y: {
          title: {
            display: true,
            text: 'Category'
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
              // Get the original event data
              const event = context.raw.originalEvent;
              if (!event) return '';
              
              // Format tooltip with event details
              return [
                `Title: ${event.title}`,
                `Start: ${formatDatetime(event.startTime)}`,
                `End: ${formatDatetime(event.endTime)}`,
                `Status: ${event.status}`,
                event.description ? `Description: ${event.description}` : ''
              ].filter(Boolean);
            }
          }
        },
        legend: {
          display: showLegend,
          position: 'top',
        }
      }
    });

    return timelineOptions;
  };

  return (
    <div 
      className={`timeline-chart-container ${className}`}
      style={{ height, width }}
    >
      <canvas
        ref={chartRef}
        id={id}
        role="img"
        aria-label="Timeline of events"
      />
    </div>
  );
};

/**
 * Sets up event listeners for the timeline chart
 * 
 * @param chart - Chart.js instance
 * @param onEventClick - Event click callback function
 * @returns Cleanup function to remove event listeners
 */
export const setupTimelineEvents = (chart: Chart, onEventClick: (event: TimelineEvent) => void): () => void => {
  const handleClick = (e: MouseEvent) => {
    const activePoints = chart.getElementsAtEventForMode(
      e,
      'nearest',
      { intersect: true },
      false
    );
    
    if (activePoints.length > 0) {
      const firstPoint = activePoints[0];
      const datasetIndex = firstPoint.datasetIndex;
      const index = firstPoint.index;
      
      // Get the event data from the chart
      const eventData = chart.data.datasets[datasetIndex].data[index];
      if (eventData && (eventData as any).originalEvent) {
        onEventClick((eventData as any).originalEvent);
      }
    }
  };

  // Add click event listener
  chart.canvas.addEventListener('click', handleClick);
  
  // Return cleanup function to remove listener when component unmounts
  return () => {
    chart.canvas.removeEventListener('click', handleClick);
  };
};

export default TimelineChart;