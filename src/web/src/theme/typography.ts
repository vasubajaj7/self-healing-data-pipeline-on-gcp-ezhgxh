/**
 * Typography System
 *
 * Defines the typography system for the application, including font families,
 * sizes, weights, and styles for different text elements to ensure consistent
 * typography across the UI.
 */

const typography = {
  // Base font family for the application
  fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif",
  
  // Font size scale using rem units for accessibility and responsiveness
  fontSize: {
    xs: '0.75rem',   // 12px
    sm: '0.875rem',  // 14px
    md: '1rem',      // 16px
    lg: '1.125rem',  // 18px
    xl: '1.25rem',   // 20px
    xxl: '1.5rem',   // 24px
  },
  
  // Font weight options
  fontWeight: {
    light: 300,
    regular: 400,
    medium: 500,
    bold: 700,
  },
  
  // Line height options
  lineHeight: {
    tight: 1.2,
    normal: 1.5,
    relaxed: 1.75,
  },
  
  // Heading styles
  h1: {
    fontSize: '2.5rem',      // 40px
    fontWeight: 700,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  h2: {
    fontSize: '2rem',        // 32px
    fontWeight: 700,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  h3: {
    fontSize: '1.75rem',     // 28px
    fontWeight: 500,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  h4: {
    fontSize: '1.5rem',      // 24px
    fontWeight: 500,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  h5: {
    fontSize: '1.25rem',     // 20px
    fontWeight: 500,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  h6: {
    fontSize: '1.125rem',    // 18px
    fontWeight: 500,
    lineHeight: 1.2,
    marginBottom: '0.5em',
  },
  
  // Body text styles
  body1: {
    fontSize: '1rem',        // 16px
    fontWeight: 400,
    lineHeight: 1.5,
  },
  
  body2: {
    fontSize: '0.875rem',    // 14px
    fontWeight: 400,
    lineHeight: 1.5,
  },
  
  // Other typography styles
  subtitle1: {
    fontSize: '1rem',        // 16px
    fontWeight: 500,
    lineHeight: 1.5,
  },
  
  subtitle2: {
    fontSize: '0.875rem',    // 14px
    fontWeight: 500,
    lineHeight: 1.5,
  },
  
  button: {
    fontSize: '0.875rem',    // 14px
    fontWeight: 500,
    lineHeight: 1.75,
    textTransform: 'uppercase',
  },
  
  caption: {
    fontSize: '0.75rem',     // 12px
    fontWeight: 400,
    lineHeight: 1.5,
  },
  
  overline: {
    fontSize: '0.75rem',     // 12px
    fontWeight: 400,
    lineHeight: 2.5,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
  },
};

// Export the typography configuration
export { typography };

// Default export for easier importing
export default typography;