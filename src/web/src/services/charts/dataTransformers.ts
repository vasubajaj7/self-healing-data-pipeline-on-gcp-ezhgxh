/**
 * Data Transformer Utilities
 * 
 * Utility functions for transforming raw data into formats suitable for different chart types
 * in the self-healing data pipeline web interface. These transformers bridge the gap between
 * backend API responses and visualization requirements.
 */

import { ChartData } from 'chart.js'; // version: ^4.3.0
import { format } from 'date-fns'; // version: ^2.30.0
import merge from 'lodash/merge'; // version: ^4.17.21
import { chart, status } from '../../theme/colors';
import { chartColorSchemes } from '../../config/chartConfig';
import { formatDatetime, formatNumber, formatPercentage } from '../charts/chartUtils';

/**
 * Transforms time series data for line and area charts
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformTimeSeriesData = (
  rawData: any,
  options: {
    dateFormat?: string;
    interpolate?: boolean;
    sortByDate?: boolean;
    colorScheme?: string;
    fillArea?: boolean;
    smoothing?: number;
    showPoints?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    dateFormat = 'MMM d, yyyy',
    interpolate = false,
    sortByDate = true,
    colorScheme = 'pipeline',
    fillArea = false,
    smoothing = 0.4,
    showPoints = true
  } = options;

  // Validate input data
  if (!rawData || !Array.isArray(rawData.data)) {
    throw new Error('Invalid time series data format. Expected { data: [...] }');
  }

  // Extract time series data
  let timeSeriesData = [...rawData.data];

  // Sort chronologically if requested
  if (sortByDate) {
    timeSeriesData.sort((a, b) => {
      const dateA = new Date(a.timestamp || a.date || a.x);
      const dateB = new Date(b.timestamp || b.date || b.x);
      return dateA.getTime() - dateB.getTime();
    });
  }

  // Extract timestamps and format for x-axis labels
  const timestamps = timeSeriesData.map(item => item.timestamp || item.date || item.x);
  const labels = timestamps.map(timestamp => formatDatetime(timestamp, dateFormat));

  // Extract series from data
  let seriesNames: string[] = [];
  let seriesData: {[key: string]: number[]} = {};

  // If data has multiple series
  if (rawData.series && Array.isArray(rawData.series)) {
    seriesNames = rawData.series.map((s: any) => s.name || 'Series');
    
    // Initialize series data arrays
    seriesNames.forEach(series => {
      seriesData[series] = [];
    });

    // Extract values for each series
    timeSeriesData.forEach((item, index) => {
      seriesNames.forEach(series => {
        const value = item[series] !== undefined ? item[series] : (
          item.values && item.values[series] !== undefined ? item.values[series] : null
        );
        
        // Handle missing data points
        if (value === null && interpolate && index > 0 && index < timeSeriesData.length - 1) {
          // Simple linear interpolation
          const prevVal = seriesData[series][index - 1] || 0;
          const nextIndex = timeSeriesData.findIndex((_, i) => 
            i > index && (timeSeriesData[i][series] !== null || 
                        (timeSeriesData[i].values && timeSeriesData[i].values[series] !== null))
          );
          
          if (nextIndex !== -1) {
            const nextVal = timeSeriesData[nextIndex][series] !== undefined ? 
                            timeSeriesData[nextIndex][series] : 
                            timeSeriesData[nextIndex].values[series];
            const steps = nextIndex - index + 1;
            const interpolatedValue = prevVal + ((nextVal - prevVal) / steps);
            seriesData[series].push(interpolatedValue);
          } else {
            seriesData[series].push(prevVal); // No next value, use previous
          }
        } else {
          seriesData[series].push(value !== null ? value : null);
        }
      });
    });
  } else {
    // Single series data
    const seriesName = rawData.name || 'Value';
    seriesNames = [seriesName];
    seriesData[seriesName] = timeSeriesData.map(item => 
      item.value !== undefined ? item.value : item.y
    );
  }

  // Get colors for series
  const colors = seriesNames.map((_, index) => {
    // Use provided colors or generate from color scheme
    if (rawData.colors && rawData.colors[index]) {
      return rawData.colors[index];
    }
    
    const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                         chartColorSchemes.pipeline;
    return schemeColors[index % schemeColors.length];
  });

  // Format data into Chart.js dataset structure
  const datasets = seriesNames.map((series, index) => {
    const color = colors[index];
    
    return {
      label: series,
      data: seriesData[series],
      borderColor: color,
      backgroundColor: fillArea ? `${color}40` : color, // Add transparency if fill is enabled
      fill: fillArea,
      tension: smoothing,
      pointRadius: showPoints ? 3 : 0,
    };
  });

  return {
    labels,
    datasets
  };
};

/**
 * Transforms categorical comparison data for bar and column charts
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformComparisonData = (
  rawData: any,
  options: {
    horizontal?: boolean;
    stacked?: boolean;
    sortBy?: 'asc' | 'desc' | 'none' | string;
    colorScheme?: string;
    showTotal?: boolean;
    maxCategories?: number;
    groupOthers?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    horizontal = false,
    stacked = false,
    sortBy = 'none',
    colorScheme = 'pipeline',
    showTotal = false,
    maxCategories = 20,
    groupOthers = true
  } = options;

  // Validate input data
  if (!rawData || (!Array.isArray(rawData.data) && !Array.isArray(rawData))) {
    throw new Error('Invalid comparison data format. Expected array or { data: [...] }');
  }

  // Extract data items
  const dataItems = Array.isArray(rawData) ? rawData : rawData.data;
  
  // Extract categories and values
  let categories: string[] = [];
  let seriesNames: string[] = [];
  let seriesData: {[key: string]: number[]} = {};
  
  // Check if data has a single series or multiple series
  const hasMultipleSeries = dataItems.some(item => 
    item.values && typeof item.values === 'object' && !Array.isArray(item.values)
  );
  
  if (hasMultipleSeries) {
    // Multi-series data
    categories = dataItems.map(item => item.category || item.label || item.name);
    
    // Extract series names from the first item that has values
    const firstItemWithValues = dataItems.find(item => 
      item.values && typeof item.values === 'object'
    );
    
    if (firstItemWithValues) {
      seriesNames = Object.keys(firstItemWithValues.values);
    }
    
    // Initialize series data arrays
    seriesNames.forEach(series => {
      seriesData[series] = [];
    });
    
    // Extract values for each series
    dataItems.forEach(item => {
      seriesNames.forEach(series => {
        const value = item.values && item.values[series] !== undefined ? 
                      item.values[series] : 0;
        seriesData[series].push(value);
      });
    });
  } else {
    // Single series data
    seriesNames = [rawData.name || 'Value'];
    categories = dataItems.map(item => item.category || item.label || item.name);
    seriesData[seriesNames[0]] = dataItems.map(item => item.value !== undefined ? item.value : 0);
  }
  
  // Limit number of categories if needed and group others
  if (categories.length > maxCategories && groupOthers) {
    const topCategories = categories.slice(0, maxCategories - 1);
    const otherCategoryIndex = maxCategories - 1;
    
    // Group "Others" values
    seriesNames.forEach(series => {
      const otherValues = seriesData[series].slice(maxCategories - 1);
      const otherSum = otherValues.reduce((sum, val) => sum + val, 0);
      seriesData[series] = seriesData[series].slice(0, maxCategories - 1);
      seriesData[series].push(otherSum);
    });
    
    categories = [...topCategories, 'Others'];
  }
  
  // Apply sorting if specified
  if (sortBy !== 'none') {
    // Create pairs of categories and values for sorting
    const pairs = categories.map((category, index) => {
      // For sorting, use the first series or the specified series
      const sortSeries = typeof sortBy === 'string' && seriesNames.includes(sortBy) ? 
                         sortBy : seriesNames[0];
      return { 
        category, 
        value: seriesData[sortSeries][index]
      };
    });
    
    // Sort pairs
    pairs.sort((a, b) => {
      if (sortBy === 'asc') {
        return a.value - b.value;
      } else if (sortBy === 'desc') {
        return b.value - a.value;
      }
      return 0;
    });
    
    // Reconstruct categories and values in sorted order
    const newCategories = pairs.map(p => p.category);
    const newSeriesData: {[key: string]: number[]} = {};
    
    seriesNames.forEach(series => {
      newSeriesData[series] = pairs.map((pair, index) => {
        const originalIndex = categories.indexOf(pair.category);
        return seriesData[series][originalIndex];
      });
    });
    
    categories = newCategories;
    seriesData = newSeriesData;
  }
  
  // Get colors for series
  const colors = seriesNames.map((_, index) => {
    // Use provided colors or generate from color scheme
    if (rawData.colors && rawData.colors[index]) {
      return rawData.colors[index];
    }
    
    const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                         chartColorSchemes.pipeline;
    return schemeColors[index % schemeColors.length];
  });
  
  // Format data into Chart.js dataset structure
  const datasets = seriesNames.map((series, index) => {
    const color = colors[index];
    
    return {
      label: series,
      data: seriesData[series],
      backgroundColor: color,
      borderColor: `${color}`,
      borderWidth: 1,
      // Bar chart specific options
      barPercentage: 0.8,
      categoryPercentage: stacked ? 0.9 : 0.8,
      // Stacked chart configuration
      stack: stacked ? 'stack1' : undefined
    };
  });
  
  // Add total dataset if requested
  if (showTotal && stacked) {
    const totalData = categories.map((_, index) => {
      return seriesNames.reduce((sum, series) => {
        return sum + (seriesData[series][index] || 0);
      }, 0);
    });
    
    datasets.push({
      label: 'Total',
      data: totalData,
      type: 'line',
      borderColor: '#333',
      backgroundColor: 'transparent',
      borderWidth: 2,
      pointRadius: 3,
      fill: false,
      order: 0  // Display line on top of bars
    });
  }
  
  return {
    labels: categories,
    datasets
  };
};

/**
 * Transforms distribution data for pie, doughnut, and radar charts
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformDistributionData = (
  rawData: any,
  options: {
    colorScheme?: string;
    sortBy?: 'asc' | 'desc' | 'none';
    otherThreshold?: number;
    maxSegments?: number;
    showPercentages?: boolean;
    calculatePercentages?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    colorScheme = 'pipeline',
    sortBy = 'none',
    otherThreshold = 0.02, // 2% threshold
    maxSegments = 10,
    showPercentages = false,
    calculatePercentages = false
  } = options;
  
  // Validate input data
  if (!rawData || (!Array.isArray(rawData.data) && !Array.isArray(rawData))) {
    throw new Error('Invalid distribution data format. Expected array or { data: [...] }');
  }
  
  // Extract data items
  const dataItems = Array.isArray(rawData) ? rawData : rawData.data;
  
  // Extract labels and values
  let labels: string[] = [];
  let values: number[] = [];
  
  dataItems.forEach(item => {
    const label = item.label || item.category || item.name;
    const value = item.value !== undefined ? item.value : 0;
    
    labels.push(label);
    values.push(value);
  });
  
  // Calculate total
  const total = values.reduce((sum, value) => sum + value, 0);
  
  // Calculate percentages if requested
  const percentages = calculatePercentages ? 
                      values.map(value => (value / total) * 100) : 
                      values;
  
  // Group small segments into "Other" if needed
  if (dataItems.length > maxSegments || otherThreshold > 0) {
    // Pair labels with values
    const pairs = labels.map((label, i) => ({ 
      label, 
      value: values[i],
      percentage: calculatePercentages ? percentages[i] : (values[i] / total) * 100
    }));
    
    // Sort by value if requested
    if (sortBy !== 'none') {
      pairs.sort((a, b) => {
        if (sortBy === 'asc') {
          return a.value - b.value;
        }
        return b.value - a.value; // desc is default for pie charts
      });
    }
    
    // Identify items to group as "Other"
    const keepItems: typeof pairs = [];
    const otherItems: typeof pairs = [];
    
    pairs.forEach(item => {
      const itemPercentage = (item.value / total) * 100;
      
      if (
        keepItems.length < maxSegments - 1 && // Keep room for "Other"
        itemPercentage >= otherThreshold * 100 // Above threshold
      ) {
        keepItems.push(item);
      } else {
        otherItems.push(item);
      }
    });
    
    // Only add "Other" if there are items to group
    if (otherItems.length > 0) {
      const otherValue = otherItems.reduce((sum, item) => sum + item.value, 0);
      const otherPercentage = otherItems.reduce((sum, item) => sum + item.percentage, 0);
      
      // Add "Other" to keep items
      keepItems.push({
        label: 'Other',
        value: otherValue,
        percentage: otherPercentage
      });
    }
    
    // Update labels and values with the new set
    labels = keepItems.map(item => showPercentages ? 
      `${item.label} (${formatPercentage(item.percentage / 100)})` : 
      item.label
    );
    
    values = keepItems.map(item => item.value);
    
    if (calculatePercentages) {
      percentages = keepItems.map(item => item.percentage);
    }
  } else if (showPercentages) {
    // Add percentages to labels
    labels = labels.map((label, i) => {
      const percentage = (values[i] / total) * 100;
      return `${label} (${formatPercentage(percentage / 100)})`;
    });
  }
  
  // Get colors for segments
  const colors = labels.map((_, index) => {
    // Use provided colors or generate from color scheme
    if (rawData.colors && rawData.colors[index]) {
      return rawData.colors[index];
    }
    
    const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                         chartColorSchemes.pipeline;
    return schemeColors[index % schemeColors.length];
  });
  
  // Format data into Chart.js dataset structure
  return {
    labels,
    datasets: [{
      data: calculatePercentages ? percentages : values,
      backgroundColor: colors,
      borderColor: colors.map(color => `${color}90`), // Slightly darker border
      borderWidth: 1,
      hoverOffset: 20
    }]
  };
};

/**
 * Transforms x/y coordinate data for scatter and bubble charts
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformScatterData = (
  rawData: any,
  options: {
    colorScheme?: string;
    showLegend?: boolean;
    radiusRange?: [number, number];
    xAxisLabel?: string;
    yAxisLabel?: string;
    useRadiusValue?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    colorScheme = 'pipeline',
    showLegend = true,
    radiusRange = [4, 20],
    xAxisLabel = 'X',
    yAxisLabel = 'Y',
    useRadiusValue = true
  } = options;
  
  // Validate input data
  if (!rawData || (!Array.isArray(rawData.data) && !Array.isArray(rawData))) {
    throw new Error('Invalid scatter data format. Expected array or { data: [...] }');
  }
  
  // Extract data items
  const dataItems = Array.isArray(rawData) ? rawData : rawData.data;
  
  // Check if data has multiple series
  const hasMultipleSeries = rawData.series && Array.isArray(rawData.series);
  
  let datasets: any[] = [];
  
  if (hasMultipleSeries) {
    // Handle multi-series data
    const seriesNames = rawData.series.map((s: any) => s.name || 'Series');
    
    // Get colors for series
    const colors = seriesNames.map((_: string, index: number) => {
      // Use provided colors or generate from color scheme
      if (rawData.colors && rawData.colors[index]) {
        return rawData.colors[index];
      }
      
      const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                          chartColorSchemes.pipeline;
      return schemeColors[index % schemeColors.length];
    });
    
    // Group data by series
    const seriesData: {[key: string]: any[]} = {};
    seriesNames.forEach(series => {
      seriesData[series] = [];
    });
    
    dataItems.forEach(item => {
      const x = item.x !== undefined ? item.x : 0;
      const y = item.y !== undefined ? item.y : 0;
      const r = item.r !== undefined ? item.r : (item.radius !== undefined ? item.radius : 5);
      const series = item.series || 'default';
      
      if (seriesData[series]) {
        seriesData[series].push({ x, y, r });
      }
    });
    
    // Create datasets for each series
    datasets = seriesNames.map((series, index) => {
      const color = colors[index];
      
      return {
        label: series,
        data: seriesData[series],
        backgroundColor: `${color}80`, // Semi-transparent
        borderColor: color,
        pointHoverRadius: 7,
        // Only needed for bubble chart if using radius values
        pointRadius: useRadiusValue ? (ctx: any) => {
          const value = ctx.raw.r;
          // Scale radius between min and max
          const [min, max] = radiusRange;
          return min + ((value / 100) * (max - min));
        } : 5
      };
    });
  } else {
    // Handle single series data
    const seriesName = rawData.name || 'Data Points';
    const color = rawData.color || chartColorSchemes.pipeline[0];
    
    // Transform data points
    const points = dataItems.map((item: any) => {
      return {
        x: item.x !== undefined ? item.x : 0,
        y: item.y !== undefined ? item.y : 0,
        r: item.r !== undefined ? item.r : (item.radius !== undefined ? item.radius : 5)
      };
    });
    
    datasets = [{
      label: seriesName,
      data: points,
      backgroundColor: `${color}80`, // Semi-transparent
      borderColor: color,
      // Only needed for bubble chart if using radius values
      pointRadius: useRadiusValue ? (ctx: any) => {
        const value = ctx.raw.r;
        // Scale radius between min and max
        const [min, max] = radiusRange;
        return min + ((value / 100) * (max - min));
      } : 5,
      pointHoverRadius: 7
    }];
  }
  
  return {
    // Scatter charts don't use labels as x-axis is numeric
    labels: [],
    datasets
  };
};

/**
 * Transforms matrix data for heatmap visualization
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformHeatmapData = (
  rawData: any,
  options: {
    colorScale?: string[];
    normalizeValues?: boolean;
    showValues?: boolean;
    maxValue?: number;
    minValue?: number;
  } = {}
): ChartData => {
  // Default options
  const {
    colorScale = ['#d4f7ff', '#0088aa', '#005566'], // Light -> Dark
    normalizeValues = true,
    showValues = true,
    maxValue,
    minValue
  } = options;
  
  // Validate input data
  if (!rawData || !rawData.data || !Array.isArray(rawData.data)) {
    throw new Error('Invalid heatmap data format. Expected { data: [...] }');
  }
  
  // Extract matrix data
  const matrixData = rawData.data;
  
  // Extract x and y axis labels
  const xLabels = rawData.xLabels || matrixData[0].map((_: any, i: number) => `X${i}`);
  const yLabels = rawData.yLabels || matrixData.map((_, i: number) => `Y${i}`);
  
  // Determine min and max values for normalization
  let dataMin = minValue !== undefined ? minValue : Number.MAX_VALUE;
  let dataMax = maxValue !== undefined ? maxValue : Number.MIN_VALUE;
  
  if (normalizeValues && (minValue === undefined || maxValue === undefined)) {
    // Find min and max values in the data
    matrixData.forEach((row: number[]) => {
      row.forEach(value => {
        if (value < dataMin) dataMin = value;
        if (value > dataMax) dataMax = value;
      });
    });
  }
  
  // Convert matrix to heatmap data points
  const heatmapData: any[] = [];
  
  matrixData.forEach((row: number[], y: number) => {
    row.forEach((value: number, x: number) => {
      // Normalize value between 0 and 1 if requested
      let normalizedValue = value;
      if (normalizeValues && dataMax !== dataMin) {
        normalizedValue = (value - dataMin) / (dataMax - dataMin);
      }
      
      // Add data point
      heatmapData.push({
        x,
        y,
        value, // Original value
        normalizedValue // Normalized value for color scaling
      });
    });
  });
  
  // Create a function to determine color based on value
  const getColor = (value: number): string => {
    if (colorScale.length === 1) return colorScale[0];
    
    // Normalize value between 0 and 1 if not already
    const normalizedVal = normalizeValues ? value : (value - dataMin) / (dataMax - dataMin);
    
    // Calculate which color segments the value falls between
    const segments = colorScale.length - 1;
    const segment = Math.min(segments, Math.floor(normalizedVal * segments));
    const remainder = (normalizedVal * segments) - segment;
    
    // Get colors for interpolation
    const color1 = colorScale[segment];
    const color2 = colorScale[Math.min(segments, segment + 1)];
    
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
    
    return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
  };
  
  // Format data for scatter chart (used to represent heatmap)
  return {
    labels: xLabels,
    datasets: [{
      label: rawData.name || 'Heatmap',
      data: heatmapData.map(point => ({
        x: point.x,
        y: point.y,
        value: point.value,
        normalizedValue: point.normalizedValue
      })),
      backgroundColor: (ctx: any) => {
        if (!ctx.raw) return 'rgba(0,0,0,0.2)';
        return getColor(ctx.raw.normalizedValue);
      },
      borderWidth: 1,
      borderColor: 'rgba(255,255,255,0.2)',
      pointStyle: 'rect',
      pointRadius: 20, // Size of heatmap cells
      pointHoverRadius: 20,
      hoverBackgroundColor: (ctx: any) => {
        if (!ctx.raw) return 'rgba(0,0,0,0.2)';
        const color = getColor(ctx.raw.normalizedValue);
        // Brighten the color slightly
        return color + '90'; // Add some transparency
      }
    }]
  };
};

/**
 * Transforms single value data for gauge chart visualization
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformGaugeData = (
  rawData: any,
  options: {
    min?: number;
    max?: number;
    thresholds?: {value: number, color: string}[];
    arcSize?: number;
    showValue?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    min = 0,
    max = 100,
    thresholds = [
      { value: 33, color: status.error },    // Red for lower third
      { value: 66, color: status.warning },  // Yellow for middle third
      { value: 100, color: status.healthy }  // Green for upper third
    ],
    arcSize = 0.75, // 3/4 of a circle
    showValue = true
  } = options;
  
  // Validate input data
  if (!rawData || rawData.value === undefined) {
    throw new Error('Invalid gauge data format. Expected { value: number }');
  }
  
  // Get the current value, clamped to min and max
  const value = Math.max(min, Math.min(max, rawData.value));
  
  // Normalize value to percentage
  const percentage = ((value - min) / (max - min)) * 100;
  
  // Sort thresholds by value
  const sortedThresholds = [...thresholds].sort((a, b) => a.value - b.value);
  
  // Determine colors based on thresholds
  let segmentColors: string[] = [];
  
  // Default to first threshold color if none match
  let activeSegmentColor = sortedThresholds[0].color;
  
  // Find all segment colors and active color
  sortedThresholds.forEach((threshold, index) => {
    const normalizedThreshold = ((threshold.value - min) / (max - min)) * 100;
    
    segmentColors.push(threshold.color);
    
    // Determine which threshold the value falls under
    if (percentage <= normalizedThreshold) {
      activeSegmentColor = threshold.color;
    }
  });
  
  // For gauge implementation, we need to create a special doughnut chart
  // with empty segments to create the gauge appearance
  
  // Calculate the "empty" portion of the circle
  const circumference = 2 * Math.PI;
  const totalArc = circumference * arcSize;
  const emptyArc = circumference - totalArc;
  
  // Create three segments for the display: background, value, and empty
  const datasets = [{
    data: [percentage, 100 - percentage, 100], // Value segment, remainder, empty segment
    backgroundColor: [
      activeSegmentColor,          // Value segment color
      '#e0e0e0',                   // Background color
      'transparent'                // Empty segment (invisible)
    ],
    borderWidth: 0,
    circumference: totalArc,       // Only draw a partial circle
    rotation: (Math.PI * (1 - arcSize)) - (Math.PI / 2)  // Start from bottom-left
  }];
  
  // Add a custom field for display value
  const chartData: ChartData & { value?: number, displayValue?: string } = {
    labels: ['Value', 'Remainder', 'Empty'],
    datasets
  };
  
  // Add value for custom gauge renderer
  chartData.value = value;
  chartData.displayValue = showValue ? 
                           formatNumber(value, { precision: 1 }) : 
                           undefined;
  
  return chartData;
};

/**
 * Transforms hierarchical data for treemap visualization
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformTreemapData = (
  rawData: any,
  options: {
    colorScheme?: string;
    depthColors?: boolean;
    valueFormat?: 'number' | 'percentage' | 'compact';
    showLabels?: boolean;
    groupSmallValues?: boolean;
    smallValueThreshold?: number;
  } = {}
): ChartData => {
  // Default options
  const {
    colorScheme = 'pipeline',
    depthColors = true,
    valueFormat = 'number',
    showLabels = true,
    groupSmallValues = true,
    smallValueThreshold = 0.02 // 2% of total
  } = options;
  
  // Validate input data
  if (!rawData || !rawData.data) {
    throw new Error('Invalid treemap data format. Expected hierarchical data object');
  }
  
  // Function to recursively process hierarchical data
  const processNode = (
    node: any, 
    parentPath: string[] = [], 
    depth: number = 0
  ): any[] => {
    // Skip empty nodes
    if (!node) return [];
    
    // For leaf nodes (no children)
    if (!node.children || node.children.length === 0) {
      return [{
        label: node.name || node.label,
        value: node.value || 0,
        path: [...parentPath, node.name || node.label],
        depth
      }];
    }
    
    // Process children recursively
    let results: any[] = [];
    
    (node.children || []).forEach((child: any) => {
      const childResults = processNode(
        child,
        [...parentPath, node.name || node.label],
        depth + 1
      );
      results = [...results, ...childResults];
    });
    
    return results;
  };
  
  // Process the entire hierarchy
  const allNodes = processNode(rawData.data);
  
  // Calculate total value for percentages
  const totalValue = allNodes.reduce((sum, node) => sum + node.value, 0);
  
  // Group small values if requested
  let processedNodes: any[] = allNodes;
  if (groupSmallValues) {
    const threshold = totalValue * smallValueThreshold;
    
    const largeNodes = allNodes.filter(node => node.value >= threshold);
    const smallNodes = allNodes.filter(node => node.value < threshold);
    
    if (smallNodes.length > 0) {
      const smallValue = smallNodes.reduce((sum, node) => sum + node.value, 0);
      
      // Add an "Other" node
      processedNodes = [
        ...largeNodes,
        {
          label: 'Other',
          value: smallValue,
          path: ['Other'],
          depth: 0
        }
      ];
    }
  }
  
  // Format the labels with values if needed
  const formattedNodes = processedNodes.map(node => {
    let formattedLabel = node.label;
    
    if (showLabels) {
      let valueText = '';
      
      if (valueFormat === 'percentage') {
        const percentage = (node.value / totalValue) * 100;
        valueText = formatPercentage(percentage / 100);
      } else if (valueFormat === 'compact') {
        valueText = formatNumber(node.value, { compact: true });
      } else {
        valueText = formatNumber(node.value);
      }
      
      formattedLabel = `${node.label} (${valueText})`;
    }
    
    return {
      ...node,
      formattedLabel
    };
  });
  
  // Determine colors based on depth or sequence
  const getNodeColor = (node: any, index: number): string => {
    // Get color scheme
    const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                         chartColorSchemes.pipeline;
    
    if (depthColors) {
      // Use depth to determine color
      return schemeColors[node.depth % schemeColors.length];
    } else {
      // Use sequential coloring
      return schemeColors[index % schemeColors.length];
    }
  };
  
  // Create datasets in the format expected by the treemap plugin
  return {
    labels: formattedNodes.map(node => node.formattedLabel),
    datasets: [{
      tree: rawData.data,
      data: formattedNodes.map(node => node.value),
      backgroundColor: formattedNodes.map((node, index) => getNodeColor(node, index)),
      borderWidth: 1,
      borderColor: '#fff',
      spacing: 2,
      // Custom fields for the treemap plugin
      key: 'value',
      groups: formattedNodes.map(node => node.path),
      labels: {
        display: showLabels,
        font: {
          size: 11
        }
      }
    }]
  };
};

/**
 * Transforms sequential stage data for funnel chart visualization
 * 
 * @param rawData - Raw data to transform
 * @param options - Configuration options for the transformation
 * @returns ChartData object for Chart.js
 */
export const transformFunnelData = (
  rawData: any,
  options: {
    colorScheme?: string;
    showPercentages?: boolean;
    calculateDropoff?: boolean;
    sortByValue?: boolean;
    ascending?: boolean;
  } = {}
): ChartData => {
  // Default options
  const {
    colorScheme = 'pipeline',
    showPercentages = true,
    calculateDropoff = true,
    sortByValue = false,
    ascending = false
  } = options;
  
  // Validate input data
  if (!rawData || (!Array.isArray(rawData.data) && !Array.isArray(rawData))) {
    throw new Error('Invalid funnel data format. Expected array or { data: [...] }');
  }
  
  // Extract data items
  const dataItems = Array.isArray(rawData) ? rawData : rawData.data;
  
  // Extract stage labels and values
  let stages: {
    label: string;
    value: number;
    percentage?: number;
    dropPercentage?: number;
  }[] = dataItems.map(item => ({
    label: item.stage || item.label || item.name,
    value: item.value !== undefined ? item.value : 0
  }));
  
  // Sort stages if requested
  if (sortByValue) {
    stages.sort((a, b) => {
      if (ascending) {
        return a.value - b.value;
      }
      return b.value - a.value;  // Descending is default for funnels
    });
  }
  
  // Calculate percentages and dropoff
  if (stages.length > 0) {
    const maxValue = Math.max(...stages.map(s => s.value));
    
    stages.forEach((stage, index) => {
      // Calculate percentage of max value
      stage.percentage = (stage.value / maxValue) * 100;
      
      // Calculate drop percentage from previous stage
      if (index > 0 && calculateDropoff) {
        const prevValue = stages[index - 1].value;
        stage.dropPercentage = prevValue > 0 
          ? ((prevValue - stage.value) / prevValue) * 100
          : 0;
      }
    });
  }
  
  // Format labels with percentages if requested
  const labels = stages.map((stage, index) => {
    let label = stage.label;
    
    if (showPercentages) {
      // For first stage, show percentage of max (100%)
      if (index === 0) {
        label = `${label} (100%)`;
      } 
      // For other stages, show percentage of max and drop from previous
      else if (stage.dropPercentage !== undefined) {
        label = `${label} (${formatPercentage(stage.percentage! / 100)}, -${formatPercentage(stage.dropPercentage / 100)})`;
      }
      // Fallback if no drop percentage
      else {
        label = `${label} (${formatPercentage(stage.percentage! / 100)})`;
      }
    }
    
    return label;
  });
  
  // Get colors for stages
  const colors = stages.map((_, index) => {
    // Use provided colors or generate from color scheme
    if (rawData.colors && rawData.colors[index]) {
      return rawData.colors[index];
    }
    
    const schemeColors = chartColorSchemes[colorScheme as keyof typeof chartColorSchemes] || 
                         chartColorSchemes.pipeline;
    return schemeColors[index % schemeColors.length];
  });
  
  // Format data for funnel chart - typically implemented as an inverted bar chart
  return {
    labels,
    datasets: [{
      data: stages.map(stage => stage.value),
      backgroundColor: colors,
      borderWidth: 0,
      // Additional properties for funnel plugin if being used
      hoverBackgroundColor: colors.map(color => `${color}d0`),
      // Store additional metadata for custom rendering
      percentages: stages.map(stage => stage.percentage),
      dropPercentages: stages.map(stage => stage.dropPercentage)
    }]
  };
};

/**
 * Aggregates time series data by specified time interval
 * 
 * @param data - Array of time series data points
 * @param interval - Time interval for aggregation (hour, day, week, month)
 * @param aggregationMethod - Method to aggregate values (sum, avg, min, max, count)
 * @returns Aggregated time series data
 */
export const aggregateTimeSeriesData = (
  data: any[],
  interval: string = 'day',
  aggregationMethod: string = 'sum'
): any[] => {
  // Validate input data
  if (!Array.isArray(data) || data.length === 0) {
    return [];
  }
  
  // Group data points by time interval
  const groupedData: {[key: string]: number[]} = {};
  
  data.forEach(item => {
    // Extract timestamp from data
    const timestamp = item.timestamp || item.date || item.x;
    if (!timestamp) return;
    
    const date = new Date(timestamp);
    let groupKey = '';
    
    // Generate group key based on interval
    switch (interval.toLowerCase()) {
      case 'hour':
        groupKey = format(date, 'yyyy-MM-dd-HH');
        break;
      case 'day':
        groupKey = format(date, 'yyyy-MM-dd');
        break;
      case 'week':
        // Get the start of the week (Sunday) and format
        const startOfWeek = new Date(date);
        startOfWeek.setDate(date.getDate() - date.getDay());
        groupKey = format(startOfWeek, 'yyyy-MM-dd');
        break;
      case 'month':
        groupKey = format(date, 'yyyy-MM');
        break;
      default:
        // Default to day if interval not recognized
        groupKey = format(date, 'yyyy-MM-dd');
    }
    
    // Extract value from data
    const value = item.value !== undefined ? item.value : (
      item.y !== undefined ? item.y : 0
    );
    
    // Add to group
    if (!groupedData[groupKey]) {
      groupedData[groupKey] = [];
    }
    
    groupedData[groupKey].push(value);
  });
  
  // Apply aggregation method to each group
  const result: any[] = [];
  
  Object.keys(groupedData).forEach(groupKey => {
    const values = groupedData[groupKey];
    let aggregatedValue = 0;
    
    // Apply aggregation method
    switch (aggregationMethod.toLowerCase()) {
      case 'sum':
        aggregatedValue = values.reduce((sum, val) => sum + val, 0);
        break;
      case 'avg':
      case 'average':
        aggregatedValue = values.reduce((sum, val) => sum + val, 0) / values.length;
        break;
      case 'min':
        aggregatedValue = Math.min(...values);
        break;
      case 'max':
        aggregatedValue = Math.max(...values);
        break;
      case 'count':
        aggregatedValue = values.length;
        break;
      default:
        // Default to sum if method not recognized
        aggregatedValue = values.reduce((sum, val) => sum + val, 0);
    }
    
    // Generate timestamp based on group key
    let timestamp = '';
    if (groupKey.length === 13) { // Hour format: yyyy-MM-dd-HH
      timestamp = groupKey.replace('-', 'T') + ':00:00Z';
    } else if (groupKey.length === 10) { // Day format: yyyy-MM-dd
      timestamp = groupKey + 'T00:00:00Z';
    } else if (groupKey.length === 7) { // Month format: yyyy-MM
      timestamp = groupKey + '-01T00:00:00Z';
    }
    
    // Add aggregated data point
    result.push({
      timestamp,
      value: aggregatedValue,
      count: values.length // Keep track of how many points were aggregated
    });
  });
  
  // Sort by timestamp
  return result.sort((a, b) => {
    return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
  });
};

/**
 * Calculates percentage change between consecutive data points
 * 
 * @param data - Array of data points
 * @param options - Calculation options
 * @returns Data with percentage change values
 */
export const calculatePercentageChange = (
  data: any[],
  options: {
    valueKey?: string;
    handleZero?: 'skip' | 'infinity' | 'zero';
    formatOutput?: boolean;
    precision?: number;
  } = {}
): any[] => {
  // Default options
  const {
    valueKey = 'value',
    handleZero = 'skip',
    formatOutput = false,
    precision = 1
  } = options;
  
  // Validate input data
  if (!Array.isArray(data) || data.length <= 1) {
    return data;
  }
  
  // Calculate percentage change for each data point
  return data.map((item, index) => {
    // Clone the item to avoid modifying original
    const result = { ...item };
    
    // Skip first item as there's no previous to compare with
    if (index === 0) {
      result.percentageChange = null;
      result.formattedChange = '';
      return result;
    }
    
    const currentValue = parseFloat(item[valueKey]);
    const previousValue = parseFloat(data[index - 1][valueKey]);
    
    // Handle cases with zero or undefined values
    if (
      isNaN(currentValue) || 
      isNaN(previousValue) || 
      (previousValue === 0 && handleZero === 'skip')
    ) {
      result.percentageChange = null;
      result.formattedChange = '';
      return result;
    }
    
    if (previousValue === 0) {
      // Handle division by zero based on option
      if (handleZero === 'infinity') {
        result.percentageChange = currentValue > 0 ? Infinity : -Infinity;
      } else { // 'zero'
        result.percentageChange = 0;
      }
    } else {
      // Standard percentage change calculation
      result.percentageChange = ((currentValue - previousValue) / Math.abs(previousValue)) * 100;
    }
    
    // Format the output if requested
    if (formatOutput && result.percentageChange !== null) {
      // Format with appropriate sign and precision
      const sign = result.percentageChange > 0 ? '+' : '';
      result.formattedChange = `${sign}${formatPercentage(result.percentageChange / 100, precision)}`;
    }
    
    return result;
  });
};

/**
 * Normalizes multiple data series to common scale for comparison
 * 
 * @param dataSeries - Array of data series
 * @param options - Normalization options
 * @returns Normalized data series
 */
export const normalizeDataSeries = (
  dataSeries: any[],
  options: {
    method?: 'min-max' | 'z-score' | 'percentage';
    preserveOriginal?: boolean;
    valueKey?: string;
  } = {}
): any[] => {
  // Default options
  const {
    method = 'min-max',
    preserveOriginal = true,
    valueKey = 'value'
  } = options;
  
  // Validate input data
  if (!Array.isArray(dataSeries) || dataSeries.length === 0) {
    return [];
  }
  
  // Handle different normalization methods
  switch (method) {
    case 'min-max': {
      // Find global min and max values across all series
      let globalMin = Number.MAX_VALUE;
      let globalMax = Number.MIN_VALUE;
      
      dataSeries.forEach(series => {
        series.forEach((point: any) => {
          const value = parseFloat(point[valueKey]);
          if (!isNaN(value)) {
            globalMin = Math.min(globalMin, value);
            globalMax = Math.max(globalMax, value);
          }
        });
      });
      
      // Avoid division by zero
      if (globalMax === globalMin) {
        return dataSeries.map(series => 
          series.map((point: any) => {
            const result = { ...point };
            result[preserveOriginal ? 'normalizedValue' : valueKey] = 0.5; // Set to middle of range
            return result;
          })
        );
      }
      
      // Normalize each series using min-max scaling
      return dataSeries.map(series => 
        series.map((point: any) => {
          const result = { ...point };
          const value = parseFloat(point[valueKey]);
          
          if (!isNaN(value)) {
            const normalizedValue = (value - globalMin) / (globalMax - globalMin);
            result[preserveOriginal ? 'normalizedValue' : valueKey] = normalizedValue;
          } else {
            result[preserveOriginal ? 'normalizedValue' : valueKey] = null;
          }
          
          return result;
        })
      );
    }
    
    case 'z-score': {
      // Calculate mean and standard deviation for each series
      return dataSeries.map(series => {
        // Calculate mean
        let sum = 0;
        let count = 0;
        
        series.forEach((point: any) => {
          const value = parseFloat(point[valueKey]);
          if (!isNaN(value)) {
            sum += value;
            count++;
          }
        });
        
        const mean = count > 0 ? sum / count : 0;
        
        // Calculate standard deviation
        let variance = 0;
        
        series.forEach((point: any) => {
          const value = parseFloat(point[valueKey]);
          if (!isNaN(value)) {
            variance += Math.pow(value - mean, 2);
          }
        });
        
        const stdDev = count > 0 ? Math.sqrt(variance / count) : 1;
        
        // Normalize using z-score
        return series.map((point: any) => {
          const result = { ...point };
          const value = parseFloat(point[valueKey]);
          
          if (!isNaN(value) && stdDev !== 0) {
            const normalizedValue = (value - mean) / stdDev;
            result[preserveOriginal ? 'normalizedValue' : valueKey] = normalizedValue;
          } else {
            result[preserveOriginal ? 'normalizedValue' : valueKey] = null;
          }
          
          return result;
        });
      });
    }
    
    case 'percentage': {
      // Normalize each value as percentage of series total
      return dataSeries.map(series => {
        // Calculate series total
        let total = 0;
        
        series.forEach((point: any) => {
          const value = parseFloat(point[valueKey]);
          if (!isNaN(value)) {
            total += Math.abs(value); // Use absolute value for percentage
          }
        });
        
        // Avoid division by zero
        if (total === 0) {
          return series.map((point: any) => {
            const result = { ...point };
            result[preserveOriginal ? 'normalizedValue' : valueKey] = 0;
            return result;
          });
        }
        
        // Normalize as percentage of total
        return series.map((point: any) => {
          const result = { ...point };
          const value = parseFloat(point[valueKey]);
          
          if (!isNaN(value)) {
            const normalizedValue = value / total;
            result[preserveOriginal ? 'normalizedValue' : valueKey] = normalizedValue;
          } else {
            result[preserveOriginal ? 'normalizedValue' : valueKey] = null;
          }
          
          return result;
        });
      });
    }
    
    default:
      // Return original data if method not recognized
      return dataSeries;
  }
};

/**
 * Applies moving average smoothing to time series data
 * 
 * @param data - Array of data points
 * @param windowSize - Size of the moving average window
 * @param method - Moving average method (simple, weighted, exponential)
 * @returns Smoothed data series
 */
export const applyMovingAverage = (
  data: any[],
  windowSize: number = 3,
  method: string = 'simple'
): any[] => {
  // Validate input data
  if (!Array.isArray(data) || data.length === 0) {
    return [];
  }
  
  // Validate window size
  if (windowSize < 2) {
    return data;
  }
  
  // Ensure window size doesn't exceed data length
  const effectiveWindowSize = Math.min(windowSize, data.length);
  
  // Apply appropriate smoothing method
  switch (method.toLowerCase()) {
    case 'simple': {
      // Simple moving average (equal weights)
      return data.map((item, index) => {
        const result = { ...item };
        
        // Calculate window bounds
        const windowStart = Math.max(0, index - effectiveWindowSize + 1);
        const windowEnd = index + 1; // exclusive
        const windowValues = [];
        
        // Collect values in window
        for (let i = windowStart; i < windowEnd; i++) {
          const value = parseFloat(data[i].value !== undefined ? data[i].value : data[i].y);
          if (!isNaN(value)) {
            windowValues.push(value);
          }
        }
        
        // Calculate simple average
        if (windowValues.length > 0) {
          const average = windowValues.reduce((sum, val) => sum + val, 0) / windowValues.length;
          result.smoothedValue = average;
        } else {
          result.smoothedValue = null;
        }
        
        return result;
      });
    }
    
    case 'weighted': {
      // Weighted moving average (higher weights for more recent values)
      return data.map((item, index) => {
        const result = { ...item };
        
        // Calculate window bounds
        const windowStart = Math.max(0, index - effectiveWindowSize + 1);
        const windowEnd = index + 1; // exclusive
        const windowValues = [];
        
        // Collect values in window
        for (let i = windowStart; i < windowEnd; i++) {
          const value = parseFloat(data[i].value !== undefined ? data[i].value : data[i].y);
          if (!isNaN(value)) {
            // Weight increases linearly with recency
            const weight = i - windowStart + 1;
            windowValues.push({ value, weight });
          }
        }
        
        // Calculate weighted average
        if (windowValues.length > 0) {
          const weightSum = windowValues.reduce((sum, item) => sum + item.weight, 0);
          const average = windowValues.reduce((sum, item) => sum + (item.value * item.weight), 0) / weightSum;
          result.smoothedValue = average;
        } else {
          result.smoothedValue = null;
        }
        
        return result;
      });
    }
    
    case 'exponential': {
      // Exponential moving average (alpha parameter determines weight of most recent value)
      const alpha = 2 / (effectiveWindowSize + 1); // Standard formula for EMA alpha
      
      // Initialize with first value
      let currentEMA = parseFloat(data[0].value !== undefined ? data[0].value : data[0].y);
      
      return data.map((item, index) => {
        const result = { ...item };
        
        if (index === 0) {
          // First point is just itself
          result.smoothedValue = currentEMA;
        } else {
          const value = parseFloat(item.value !== undefined ? item.value : item.y);
          
          if (!isNaN(value)) {
            // EMA formula: EMA_today = alpha * value_today + (1 - alpha) * EMA_yesterday
            currentEMA = (alpha * value) + ((1 - alpha) * currentEMA);
            result.smoothedValue = currentEMA;
          } else {
            // If value is not a number, keep previous EMA
            result.smoothedValue = currentEMA;
          }
        }
        
        return result;
      });
    }
    
    default:
      // Return original data if method not recognized
      return data;
  }
};