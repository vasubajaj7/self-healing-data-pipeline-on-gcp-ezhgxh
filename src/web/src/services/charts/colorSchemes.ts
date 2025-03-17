import chroma from 'chroma-js'; // ^2.4.2
import { chart, status, primary, secondary, error, warning, success, info } from '../../theme/colors';

/**
 * Generates a palette of colors based on a base color
 * 
 * @param baseColor - The hex color code to base the palette on
 * @param count - Number of colors to generate
 * @param options - Optional configuration for the palette generation
 * @returns Array of generated color hex codes
 */
export const generateColorPalette = (
  baseColor: string,
  count: number,
  options?: {
    lightnessRange?: [number, number];
    saturationRange?: [number, number];
    includeBase?: boolean;
  }
): string[] => {
  // Validate the base color
  if (!chroma.valid(baseColor)) {
    console.error(`Invalid color: ${baseColor}`);
    return Array(count).fill('#cccccc');
  }

  const {
    lightnessRange = [0.3, 0.8],
    saturationRange = [0.4, 0.8],
    includeBase = true
  } = options || {};

  // If we only need one color and includeBase is true, return the base color
  if (count === 1 && includeBase) {
    return [baseColor];
  }

  // Create a color palette
  const baseChroma = chroma(baseColor);
  const baseHue = baseChroma.get('hsl.h');

  // Generate an array of colors with variations in hue
  let palette: string[] = [];

  if (includeBase) {
    palette.push(baseColor);
  }

  // Calculate how many additional colors we need
  const additionalCount = includeBase ? count - 1 : count;

  if (additionalCount <= 0) {
    return palette;
  }

  // Generate colors with varying hue, keeping lightness and saturation within range
  for (let i = 0; i < additionalCount; i++) {
    // Create variations by shifting the hue
    const hueShift = (360 / additionalCount) * i;
    const newHue = (baseHue + hueShift) % 360;
    
    // Create a new color with the new hue and adjusted lightness/saturation
    const lightnessStep = (lightnessRange[1] - lightnessRange[0]) / (additionalCount || 1);
    const saturationStep = (saturationRange[1] - saturationRange[0]) / (additionalCount || 1);
    
    const lightness = lightnessRange[0] + (lightnessStep * i);
    const saturation = saturationRange[0] + (saturationStep * i);
    
    const color = chroma.hsl(newHue, saturation, lightness).hex();
    palette.push(color);
  }

  return palette;
};

/**
 * Returns a color scheme appropriate for a specific visualization context
 * 
 * @param context - The visualization context (e.g., 'pipeline', 'quality', etc.)
 * @param count - Number of colors needed
 * @returns Array of color hex codes for the specified context
 */
export const getContextColorScheme = (context: string, count: number = 5): string[] => {
  let baseScheme: string[] = [];
  
  switch (context.toLowerCase()) {
    case 'pipeline':
      baseScheme = pipelineColorScheme;
      break;
    case 'quality':
      baseScheme = qualityColorScheme;
      break;
    case 'healing':
      baseScheme = healingColorScheme;
      break;
    case 'alert':
      baseScheme = alertColorScheme;
      break;
    case 'performance':
      baseScheme = performanceColorScheme;
      break;
    default:
      // Default to pipeline color scheme
      baseScheme = pipelineColorScheme;
  }

  // If we need more colors than are in the base scheme, generate additional ones
  if (count <= baseScheme.length) {
    return baseScheme.slice(0, count);
  }

  // Generate additional colors based on the first color in the scheme
  const additionalColors = generateColorPalette(
    baseScheme[0],
    count - baseScheme.length,
    { includeBase: false }
  );

  return [...baseScheme, ...additionalColors];
};

/**
 * Returns a sequential color scheme for representing ordered data
 * 
 * @param baseColor - The base color for the sequential scheme
 * @param steps - Number of steps in the sequence
 * @returns Array of sequential color hex codes
 */
export const getSequentialColorScheme = (baseColor: string, steps: number = 5): string[] => {
  if (!chroma.valid(baseColor)) {
    console.error(`Invalid color: ${baseColor}`);
    return Array(steps).fill('#cccccc');
  }

  // Create a sequential scale from light to dark variants of the base color
  const base = chroma(baseColor);
  const lightVariant = base.luminance(0.8);
  const darkVariant = base.luminance(0.2);

  const scale = chroma.scale([lightVariant, base, darkVariant])
    .mode('lch')
    .colors(steps);

  return scale;
};

/**
 * Returns a diverging color scheme for representing data with a meaningful midpoint
 * 
 * @param startColor - Color for the low end of the scale
 * @param endColor - Color for the high end of the scale
 * @param midColor - Color for the midpoint (defaults to white)
 * @param steps - Number of steps in the scale
 * @returns Array of diverging color hex codes
 */
export const getDivergingColorScheme = (
  startColor: string,
  endColor: string,
  midColor: string = '#ffffff',
  steps: number = 9
): string[] => {
  if (!chroma.valid(startColor) || !chroma.valid(endColor) || !chroma.valid(midColor)) {
    console.error('Invalid color provided to diverging scheme');
    return Array(steps).fill('#cccccc');
  }

  // Ensure steps is odd to have a proper midpoint
  const actualSteps = steps % 2 === 0 ? steps + 1 : steps;

  // Create a diverging scale
  const scale = chroma.scale([startColor, midColor, endColor])
    .mode('lch')
    .colors(actualSteps);

  return scale;
};

/**
 * Returns a categorical color scheme for representing distinct categories
 * 
 * @param count - Number of categories
 * @param scheme - Name of the scheme to use
 * @returns Array of distinct color hex codes
 */
export const getCategoricalColorScheme = (count: number, scheme: string = 'default'): string[] => {
  // Use function from categoricalColorSchemes object
  if (scheme in categoricalColorSchemes) {
    return categoricalColorSchemes[scheme as keyof typeof categoricalColorSchemes](count);
  }
  
  // Default to standard scheme
  return categoricalColorSchemes.default(count);
};

/**
 * Returns colors for representing different status states
 * 
 * @returns Object mapping status names to color hex codes
 */
export const getStatusColorScheme = (): typeof statusColorScheme => {
  return statusColorScheme;
};

/**
 * Returns colors for representing different severity levels
 * 
 * @returns Object mapping severity levels to color hex codes
 */
export const getSeverityColorScheme = (): typeof severityColorScheme => {
  return severityColorScheme;
};

/**
 * Returns a color with specified opacity
 * 
 * @param color - The base color
 * @param opacity - Opacity value (0-1)
 * @returns Color with opacity in rgba format
 */
export const getColorWithOpacity = (color: string, opacity: number): string => {
  if (!chroma.valid(color)) {
    console.error(`Invalid color: ${color}`);
    return `rgba(204, 204, 204, ${opacity})`;
  }

  const rgb = chroma(color).rgb();
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${opacity})`;
};

// Define pre-configured color schemes for different visualization contexts

/**
 * Color scheme for pipeline-related visualizations
 */
export const pipelineColorScheme: string[] = [
  chart.blue,
  chart.cyan,
  chart.indigo,
  chart.teal,
  '#4fc3f7' // Light blue
];

/**
 * Color scheme for data quality visualizations
 */
export const qualityColorScheme: string[] = [
  chart.purple,
  '#7e57c2', // Deep purple
  '#5c6bc0', // Indigo
  '#3949ab', // Indigo dark
  '#9575cd' // Medium purple
];

/**
 * Color scheme for self-healing visualizations
 */
export const healingColorScheme: string[] = [
  chart.green,
  '#66bb6a', // Light green
  chart.teal,
  '#4db6ac', // Light teal
  '#81c784' // Medium green
];

/**
 * Color scheme for alert visualizations
 */
export const alertColorScheme: string[] = [
  chart.red,
  chart.orange,
  chart.amber,
  '#ff7043', // Deep orange
  '#ffb74d' // Light orange
];

/**
 * Color scheme for performance visualizations
 */
export const performanceColorScheme: string[] = [
  chart.blue,
  '#42a5f5', // Light blue
  chart.indigo,
  '#5c6bc0', // Indigo light
  '#29b6f6' // Light blue
];

/**
 * Colors for different status states
 */
export const statusColorScheme = {
  healthy: status.healthy,
  warning: status.warning,
  error: status.error,
  inactive: status.inactive,
  processing: status.processing
};

/**
 * Colors for different severity levels
 */
export const severityColorScheme = {
  critical: error.main,
  high: error.light,
  medium: warning.main,
  low: warning.light,
  info: info.main
};

/**
 * Functions to generate sequential color schemes
 */
export const sequentialColorSchemes = {
  blue: (steps: number = 5) => getSequentialColorScheme(chart.blue, steps),
  green: (steps: number = 5) => getSequentialColorScheme(chart.green, steps),
  red: (steps: number = 5) => getSequentialColorScheme(chart.red, steps),
  purple: (steps: number = 5) => getSequentialColorScheme(chart.purple, steps),
  orange: (steps: number = 5) => getSequentialColorScheme(chart.orange, steps)
};

/**
 * Functions to generate diverging color schemes
 */
export const divergingColorSchemes = {
  redToGreen: (steps: number = 9) => getDivergingColorScheme(chart.red, chart.green, '#ffffff', steps),
  blueToOrange: (steps: number = 9) => getDivergingColorScheme(chart.blue, chart.orange, '#ffffff', steps),
  purpleToGreen: (steps: number = 9) => getDivergingColorScheme(chart.purple, chart.green, '#ffffff', steps)
};

/**
 * Functions to generate categorical color schemes
 */
export const categoricalColorSchemes = {
  default: (count: number) => {
    const defaultColors = [
      chart.blue,
      chart.green,
      chart.purple,
      chart.orange,
      chart.red,
      chart.teal,
      chart.cyan,
      chart.lime,
      chart.amber,
      chart.indigo
    ];
    
    if (count <= defaultColors.length) {
      return defaultColors.slice(0, count);
    }
    
    // If we need more colors, generate additional ones
    const additionalColors = generateColorPalette(
      chart.blue,
      count - defaultColors.length,
      { includeBase: false }
    );
    
    return [...defaultColors, ...additionalColors];
  },
  
  pastel: (count: number) => {
    const pastelColors = [
      '#bbdefb', // Pastel blue
      '#c8e6c9', // Pastel green
      '#e1bee7', // Pastel purple
      '#ffe0b2', // Pastel orange
      '#ffcdd2', // Pastel red
      '#b2dfdb', // Pastel teal
      '#b3e5fc', // Pastel cyan
      '#f0f4c3', // Pastel lime
      '#ffecb3', // Pastel amber
      '#c5cae9'  // Pastel indigo
    ];
    
    if (count <= pastelColors.length) {
      return pastelColors.slice(0, count);
    }
    
    // If we need more colors, generate additional pastel ones
    const baseColor = pastelColors[0];
    const options = {
      lightnessRange: [0.7, 0.9] as [number, number],
      saturationRange: [0.15, 0.4] as [number, number],
      includeBase: false
    };
    
    const additionalColors = generateColorPalette(
      baseColor,
      count - pastelColors.length,
      options
    );
    
    return [...pastelColors, ...additionalColors];
  },
  
  bold: (count: number) => {
    const boldColors = [
      '#1565c0', // Bold blue
      '#2e7d32', // Bold green
      '#7b1fa2', // Bold purple
      '#e65100', // Bold orange
      '#c62828', // Bold red
      '#00695c', // Bold teal
      '#0277bd', // Bold light blue
      '#558b2f', // Bold lime
      '#ef6c00', // Bold amber
      '#283593'  // Bold indigo
    ];
    
    if (count <= boldColors.length) {
      return boldColors.slice(0, count);
    }
    
    // If we need more colors, generate additional bold ones
    const baseColor = boldColors[0];
    const options = {
      lightnessRange: [0.3, 0.5] as [number, number],
      saturationRange: [0.6, 0.8] as [number, number],
      includeBase: false
    };
    
    const additionalColors = generateColorPalette(
      baseColor,
      count - boldColors.length,
      options
    );
    
    return [...boldColors, ...additionalColors];
  }
};