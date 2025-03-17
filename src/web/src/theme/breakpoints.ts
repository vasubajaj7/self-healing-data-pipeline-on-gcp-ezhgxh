import { Breakpoints } from '@mui/material'; // @mui/material ^5.11.0

/**
 * Breakpoints for responsive design across different screen sizes.
 * These values align with Material UI's default breakpoints
 * but can be customized for specific application needs.
 */
const values = {
  xs: 0,    // Extra small devices (portrait phones)
  sm: 600,  // Small devices (landscape phones)
  md: 960,  // Medium devices (tablets)
  lg: 1280, // Large devices (desktops)
  xl: 1920, // Extra large devices (large desktops)
};

// Type definition for breakpoint keys
type BreakpointKey = keyof typeof values;

/**
 * Returns a media query string for screens wider than the specified breakpoint
 * @param key - Breakpoint key (xs, sm, md, lg, xl)
 * @returns Media query string for min-width
 */
const up = (key: BreakpointKey): string => {
  const value = values[key];
  return `@media (min-width: ${value}px)`;
};

/**
 * Returns a media query string for screens narrower than the specified breakpoint
 * @param key - Breakpoint key (xs, sm, md, lg, xl)
 * @returns Media query string for max-width
 */
const down = (key: BreakpointKey): string => {
  const value = values[key];
  // Subtract 0.05px to prevent collision with up() at exact breakpoint values
  return `@media (max-width: ${value - 0.05}px)`;
};

/**
 * Returns a media query string for screens between two specified breakpoints
 * @param start - Starting breakpoint key (xs, sm, md, lg, xl)
 * @param end - Ending breakpoint key (xs, sm, md, lg, xl)
 * @returns Media query string for min-width and max-width
 */
const between = (start: BreakpointKey, end: BreakpointKey): string => {
  const startValue = values[start];
  const endValue = values[end];
  // Subtract 0.05px from end value to prevent collision at exact breakpoint
  return `@media (min-width: ${startValue}px) and (max-width: ${endValue - 0.05}px)`;
};

/**
 * Breakpoints utility object for responsive design.
 * Provides standard breakpoint values and utility functions for creating media queries.
 * 
 * Example usage:
 * ```
 * import { breakpoints } from 'theme/breakpoints';
 * 
 * const styles = {
 *   container: {
 *     width: '100%',
 *     [breakpoints.up('md')]: {
 *       width: '960px',
 *     },
 *   },
 * };
 * ```
 */
export const breakpoints = {
  values,
  up,
  down,
  between,
} as Breakpoints;

export default breakpoints;