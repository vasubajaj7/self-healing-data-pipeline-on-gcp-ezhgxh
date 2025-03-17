/**
 * Chart Utility Functions
 * 
 * Utility functions for formatting and manipulating chart data in the
 * self-healing data pipeline interface. Provides consistent rendering,
 * formatting, and styling across all visualization components.
 */

import { format } from 'date-fns'; // version: ^2.30.0
import { ChartData, ChartOptions } from 'chart.js'; // version: ^4.3.0
import { chart, status } from '../../theme/colors';
import { chartColorSchemes } from '../../config/chartConfig';
import { PipelineStatus, AlertSeverity } from '../../types/dashboard';

/**
 * Formats raw data into the structure required by Chart.js
 *
 * @param data - Raw data to be formatted
 * @param options - Formatting options
 * @returns Formatted chart data ready for rendering
 */
export const formatChartData = (data: any, options: Record<string, any> = {}): ChartData => {
  // Validate input data
  if (!data) {
    throw new Error('Data is required for chart formatting');
  }

  // Extract labels and datasets from raw data with default values
  const labels = data.labels || [];
  const datasets = data.datasets || [];

  // Apply default styling to datasets if needed
  const formattedDatasets = datasets.map((dataset: any, index: number) => {
    // Generate consistent colors if not provided
    const color = dataset.backgroundColor || 
      generateChartColors(options.context || 'pipeline', datasets.length)[index];
    
    return {
      // Default dataset properties
      label: dataset.label || `Dataset ${index + 1}`,
      backgroundColor: dataset.backgroundColor || color,
      borderColor: dataset.borderColor || color,
      data: dataset.data || [],
      // Apply any additional dataset properties from options
      ...dataset
    };
  });

  // Return formatted ChartData object
  return {
    labels,
    datasets: formattedDatasets
  };
};

/**
 * Formats axis labels for better readability based on data type
 *
 * @param label - The original axis label
 * @param dataType - Type of data (date, number, percentage, etc.)
 * @param options - Additional formatting options
 * @returns Formatted axis label
 */
export const formatAxisLabel = (
  label: string,
  dataType: string = 'string',
  options: Record<string, any> = {}
): string => {
  // Handle empty labels
  if (!label) return '';

  // Format based on data type
  switch (dataType.toLowerCase()) {
    case 'date':
      // Parse date and format using date-fns
      try {
        const dateValue = new Date(label);
        return formatDatetime(dateValue, options.dateFormat || 'MMM d');
      } catch (error) {
        console.warn('Invalid date format for label', label);
        return label;
      }
    
    case 'number':
      // Format numeric labels
      try {
        return formatNumber(parseFloat(label), options);
      } catch (error) {
        return label;
      }
    
    case 'percentage':
      // Format percentage labels
      try {
        return formatPercentage(parseFloat(label), options.precision || 1);
      } catch (error) {
        return label;
      }
    
    case 'duration':
      // Format duration labels
      try {
        return formatDuration(parseFloat(label), options);
      } catch (error) {
        return label;
      }
    
    default:
      // Handle long labels with truncation if needed
      const maxLength = options.maxLength || 20;
      return truncateLabel(label, maxLength);
  }
};

/**
 * Formats tooltip labels with context-specific information
 *
 * @param tooltipItem - The tooltip item object from Chart.js
 * @param context - Context for the tooltip (pipeline, quality, healing, etc.)
 * @param options - Additional formatting options
 * @returns Formatted tooltip label
 */
export const formatTooltipLabel = (
  tooltipItem: any,
  context: string = 'default',
  options: Record<string, any> = {}
): string => {
  if (!tooltipItem) return '';

  // Extract value and label from tooltip item
  const { dataset, dataIndex, formattedValue, label } = tooltipItem;
  const value = tooltipItem.raw !== undefined ? tooltipItem.raw : formattedValue;
  const datasetLabel = dataset?.label || '';
  
  // Default formatted value
  let formattedLabel = `${datasetLabel}: ${formattedValue}`;
  
  // Determine formatting based on context
  switch (context.toLowerCase()) {
    case 'pipeline':
      // Format for pipeline context
      if (options.dataType === 'percentage') {
        formattedLabel = `${datasetLabel}: ${formatPercentage(value, options.precision || 1)}`;
      } else if (options.dataType === 'duration') {
        formattedLabel = `${datasetLabel}: ${formatDuration(value, options)}`;
      }
      break;
    
    case 'quality':
      // Format for quality context (often percentage-based)
      if (options.dataType === 'percentage') {
        formattedLabel = `${datasetLabel}: ${formatPercentage(value, options.precision || 1)}`;
      } else if (options.showCount && dataset?.data) {
        // Show count if available
        formattedLabel = `${datasetLabel}: ${formattedValue} (${dataset.data[dataIndex]} items)`;
      }
      break;
    
    case 'healing':
      // Format for self-healing context
      if (options.dataType === 'percentage') {
        formattedLabel = `${datasetLabel}: ${formatPercentage(value, options.precision || 1)}`;
      } else if (options.dataType === 'status') {
        // Format with status information
        const statusText = options.statusMap?.[value] || value;
        formattedLabel = `${datasetLabel}: ${statusText}`;
      }
      break;
    
    case 'alert':
      // Format for alert context
      if (options.dataType === 'severity') {
        // Format with severity information
        const severity = options.severityMap?.[value] || value;
        formattedLabel = `${datasetLabel}: ${severity}`;
      }
      break;
    
    default:
      // Apply basic value formatting for unknown contexts
      if (options.dataType === 'date') {
        formattedLabel = `${datasetLabel}: ${formatDatetime(value, options.dateFormat || 'MMM d, yyyy')}`;
      } else if (options.dataType === 'number') {
        formattedLabel = `${datasetLabel}: ${formatNumber(value, options)}`;
      }
  }
  
  // Add any additional context from options
  if (options.prefix) {
    formattedLabel = `${options.prefix} ${formattedLabel}`;
  }
  
  if (options.suffix) {
    formattedLabel = `${formattedLabel} ${options.suffix}`;
  }
  
  return formattedLabel;
};

/**
 * Truncates long labels with ellipsis to fit in limited space
 *
 * @param label - The original label
 * @param maxLength - Maximum length before truncation
 * @returns Truncated label with ellipsis if needed
 */
export const truncateLabel = (label: string, maxLength: number = 20): string => {
  if (!label) return '';
  
  // Check if label exceeds maximum length
  if (label.length <= maxLength) {
    return label;
  }
  
  // Truncate and add ellipsis
  return `${label.substring(0, maxLength - 3)}...`;
};

/**
 * Formats a number as a percentage with specified precision
 *
 * @param value - The numeric value (can be decimal like 0.95 or whole number like 95)
 * @param precision - Number of decimal places to include
 * @returns Formatted percentage string
 */
export const formatPercentage = (value: number, precision: number = 1): string => {
  if (typeof value !== 'number') return '0%';
  
  // Determine if the value is already in percentage form (>1) or decimal form (<1)
  const percentValue = value > 1 && value <= 100 ? value : value * 100;
  
  // Round to specified precision
  const rounded = Number(percentValue.toFixed(precision));
  
  // Return formatted percentage
  return `${rounded}%`;
};

/**
 * Formats a number with appropriate separators and abbreviations
 *
 * @param value - The numeric value to format
 * @param options - Formatting options
 * @returns Formatted number string
 */
export const formatNumber = (
  value: number,
  options: Record<string, any> = {}
): string => {
  if (typeof value !== 'number' || isNaN(value)) return '0';
  
  const {
    abbreviate = false,
    precision = 1,
    separator = ',',
    compact = false
  } = options;
  
  // Handle abbreviation for large numbers if requested
  if (abbreviate) {
    if (value >= 1_000_000_000) {
      return `${(value / 1_000_000_000).toFixed(precision)}B`;
    } else if (value >= 1_000_000) {
      return `${(value / 1_000_000).toFixed(precision)}M`;
    } else if (value >= 1_000) {
      return `${(value / 1_000).toFixed(precision)}K`;
    }
  }
  
  // Format with numerical separators
  if (compact) {
    // Use Intl.NumberFormat for compact display
    return new Intl.NumberFormat('en-US', {
      notation: 'compact',
      maximumFractionDigits: precision
    }).format(value);
  } else {
    // Use Intl.NumberFormat for regular formatting with separators
    return new Intl.NumberFormat('en-US', {
      maximumFractionDigits: precision
    }).format(value);
  }
};

/**
 * Formats a duration in milliseconds to a human-readable format
 *
 * @param milliseconds - Duration in milliseconds
 * @param options - Formatting options
 * @returns Formatted duration string
 */
export const formatDuration = (
  milliseconds: number,
  options: Record<string, any> = {}
): string => {
  if (typeof milliseconds !== 'number' || isNaN(milliseconds) || milliseconds < 0) {
    return '0s';
  }
  
  const {
    compact = false,
    separator = ' ',
    precision = 1
  } = options;
  
  // Convert to appropriate units
  const seconds = milliseconds / 1000;
  
  if (seconds < 60) {
    // Less than a minute: show seconds
    return `${seconds.toFixed(precision)}s`;
  } else if (seconds < 3600) {
    // Less than an hour: show minutes and seconds
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    
    return compact
      ? `${minutes}m${separator}${remainingSeconds.toFixed(0)}s`
      : `${minutes} min${separator}${remainingSeconds.toFixed(0)} sec`;
  } else if (seconds < 86400) {
    // Less than a day: show hours and minutes
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    
    return compact
      ? `${hours}h${separator}${minutes}m`
      : `${hours} hr${hours !== 1 ? 's' : ''}${separator}${minutes} min`;
  } else {
    // More than a day: show days and hours
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    
    return compact
      ? `${days}d${separator}${hours}h`
      : `${days} day${days !== 1 ? 's' : ''}${separator}${hours} hr${hours !== 1 ? 's' : ''}`;
  }
};

/**
 * Formats a date/time value using date-fns with appropriate format
 *
 * @param datetime - Date or time value (Date object, string, or timestamp)
 * @param formatString - Format string for date-fns
 * @returns Formatted date/time string
 */
export const formatDatetime = (
  datetime: Date | string | number,
  formatString: string = 'MMM d, yyyy h:mm a'
): string => {
  if (!datetime) return '';
  
  try {
    // Convert to Date object if not already
    const date = datetime instanceof Date ? datetime : new Date(datetime);
    
    // Format using date-fns
    return format(date, formatString);
  } catch (error) {
    console.warn('Invalid date format', datetime);
    return String(datetime);
  }
};

/**
 * Applies animation settings to chart options based on chart type
 *
 * @param options - Chart.js options object
 * @param chartType - Type of chart (line, bar, pie, etc.)
 * @param enabled - Whether animations should be enabled
 * @returns Chart options with animation settings
 */
export const applyChartAnimation = (
  options: ChartOptions,
  chartType: string,
  enabled: boolean = true
): ChartOptions => {
  // Clone options to avoid modifying the original
  const updatedOptions = { ...options };
  
  // If animations disabled, set duration to 0
  if (!enabled) {
    updatedOptions.animation = { duration: 0 };
    return updatedOptions;
  }
  
  // Apply animation settings based on chart type
  switch (chartType.toLowerCase()) {
    case 'line':
    case 'area':
      updatedOptions.animation = {
        tension: {
          duration: 1000,
          easing: 'easeOutQuart',
          from: 0,
          to: 0.4,
          loop: false
        }
      };
      break;
    
    case 'bar':
      updatedOptions.animation = {
        type: 'number',
        properties: ['y'],
        from: 0,
        to: 1,
        duration: 800,
        easing: 'easeOutQuad'
      };
      break;
    
    case 'pie':
    case 'doughnut':
      updatedOptions.animation = {
        animateRotate: true,
        animateScale: true,
        duration: 800,
        easing: 'easeOutQuad'
      };
      break;
    
    default:
      updatedOptions.animation = {
        duration: 800,
        easing: 'easeOutQuad'
      };
  }
  
  return updatedOptions;
};

/**
 * Returns the appropriate color for a given status value
 *
 * @param statusValue - Status value to map to a color
 * @returns Color hex code
 */
export const getStatusColor = (statusValue: string): string => {
  // Normalize status string
  const normalizedStatus = String(statusValue).toUpperCase();
  
  // Return appropriate color from theme
  switch (normalizedStatus) {
    case 'HEALTHY':
      return status.healthy;
    case 'WARNING':
      return status.warning;
    case 'ERROR':
      return status.error;
    case 'INACTIVE':
      return status.inactive;
    case 'PROCESSING':
      return status.processing;
    default:
      // Fallback to a default color
      return status.inactive;
  }
};

/**
 * Returns the appropriate color for a given alert severity
 *
 * @param severity - Alert severity value
 * @returns Color hex code
 */
export const getSeverityColor = (severity: AlertSeverity): string => {
  switch (severity) {
    case AlertSeverity.CRITICAL:
      return status.error; // Use the error color for critical alerts
    case AlertSeverity.HIGH:
      return status.error; // Use the error color for high alerts
    case AlertSeverity.MEDIUM:
      return status.warning; // Use the warning color for medium alerts
    case AlertSeverity.LOW:
      return status.warning; // Use a lighter warning color for low alerts
    default:
      return status.inactive; // Fallback
  }
};

/**
 * Generates an array of colors for chart datasets based on context
 *
 * @param context - Chart context (pipeline, quality, healing, alert)
 * @param count - Number of colors needed
 * @returns Array of color hex codes
 */
export const generateChartColors = (
  context: string = 'pipeline',
  count: number = 1
): string[] => {
  // Get the base color scheme for the specified context
  let baseScheme: string[] = [];
  
  switch (context.toLowerCase()) {
    case 'pipeline':
      baseScheme = chartColorSchemes.pipeline || [];
      break;
    case 'quality':
      baseScheme = chartColorSchemes.quality || [];
      break;
    case 'healing':
      baseScheme = chartColorSchemes.healing || [];
      break;
    case 'alert':
      baseScheme = chartColorSchemes.alert || [];
      break;
    default:
      // Default to pipeline colors if context not recognized
      baseScheme = chartColorSchemes.pipeline || [];
  }
  
  // Fallback to chart colors if scheme is empty
  if (!baseScheme.length) {
    baseScheme = [
      chart.blue,
      chart.green,
      chart.purple,
      chart.orange,
      chart.red
    ];
  }
  
  // If count is less than or equal to available colors, return subset
  if (count <= baseScheme.length) {
    return baseScheme.slice(0, count);
  }
  
  // If more colors needed, cycle through base scheme
  const result = [...baseScheme];
  for (let i = baseScheme.length; i < count; i++) {
    result.push(baseScheme[i % baseScheme.length]);
  }
  
  return result;
};

/**
 * Creates a gradient fill for chart backgrounds
 *
 * @param ctx - Canvas rendering context
 * @param startColor - Starting color (top/left)
 * @param endColor - Ending color (bottom/right)
 * @param options - Gradient options
 * @returns Canvas gradient object
 */
export const createGradient = (
  ctx: CanvasRenderingContext2D,
  startColor: string,
  endColor: string,
  options: Record<string, any> = {}
): CanvasGradient => {
  const {
    vertical = true,
    height = 400,
    width = 400,
    stops = [{offset: 0, color: startColor}, {offset: 1, color: endColor}]
  } = options;
  
  // Create linear gradient (vertical by default)
  const gradient = vertical
    ? ctx.createLinearGradient(0, 0, 0, height)
    : ctx.createLinearGradient(0, 0, width, 0);
  
  // Add color stops
  if (stops && Array.isArray(stops)) {
    stops.forEach(stop => {
      gradient.addColorStop(stop.offset, stop.color);
    });
  } else {
    // Default gradient if stops not provided
    gradient.addColorStop(0, startColor);
    gradient.addColorStop(1, endColor);
  }
  
  return gradient;
};

export {
  formatChartData,
  formatAxisLabel,
  formatTooltipLabel,
  truncateLabel,
  formatPercentage,
  formatNumber,
  formatDuration,
  formatDatetime,
  applyChartAnimation,
  getStatusColor,
  getSeverityColor,
  generateChartColors,
  createGradient
};