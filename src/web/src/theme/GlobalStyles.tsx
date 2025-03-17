import React from 'react';
import { Global, css } from '@emotion/react'; // @emotion/react ^11.10.5
import { useThemeContext } from '../contexts/ThemeContext';
import { colors } from '../theme/colors';

/**
 * Component that renders global styles for the application
 * These styles provide a consistent foundation for all components and establish
 * theme-responsive behavior that adjusts based on light/dark mode.
 */
const AppGlobalStyles = () => {
  // Access the current theme context to determine mode
  const { mode } = useThemeContext();
  
  // Determine background and text colors based on current theme mode
  const backgroundColor = mode === 'light' ? colors.background.default : colors.background.dark;
  const textColor = mode === 'light' ? colors.text.primary : 'rgba(255, 255, 255, 0.87)';
  
  return (
    <Global
      styles={css`
        /* Reset styles */
        *, *::before, *::after {
          box-sizing: border-box;
        }
        
        body, h1, h2, h3, h4, h5, h6, p, ol, ul {
          margin: 0;
          padding: 0;
        }
        
        /* Base HTML & Body styles */
        html, body {
          font-family: 'Roboto', 'Helvetica', 'Arial', sans-serif;
          font-size: 16px;
          line-height: 1.5;
          background-color: ${backgroundColor};
          color: ${textColor};
          -webkit-font-smoothing: antialiased;
          -moz-osx-font-smoothing: grayscale;
          overflow-x: hidden;
          min-height: 100vh;
        }
        
        /* Responsive font size adjustments */
        @media (max-width: 600px) {
          html {
            font-size: 14px;
          }
        }
        
        /* Typography styles */
        h1, h2, h3, h4, h5, h6 {
          margin-bottom: 0.5em;
          font-weight: 700;
          line-height: 1.2;
        }
        
        h1 {
          font-size: 2.5rem;
        }
        
        h2 {
          font-size: 2rem;
        }
        
        h3 {
          font-size: 1.75rem;
        }
        
        h4 {
          font-size: 1.5rem;
        }
        
        h5 {
          font-size: 1.25rem;
        }
        
        h6 {
          font-size: 1.125rem;
        }
        
        /* Paragraph styling */
        p {
          margin-bottom: 1rem;
          line-height: 1.5;
        }
        
        /* Link styling */
        a {
          color: ${colors.primary.main};
          text-decoration: none;
          transition: color 0.2s ease;
        }
        
        a:hover {
          color: ${colors.primary.dark};
          text-decoration: underline;
        }
        
        a:focus {
          outline: 2px solid ${colors.primary.main};
          outline-offset: 2px;
        }
        
        /* List styling */
        ul, ol {
          margin-bottom: 1rem;
          padding-left: 1.5rem;
        }
        
        li {
          margin-bottom: 0.5rem;
        }
        
        /* Form element styling */
        input, textarea, select {
          font-family: inherit;
          font-size: inherit;
          padding: 0.5rem;
          border: 1px solid ${mode === 'light' ? colors.grey[300] : colors.grey[700]};
          border-radius: 4px;
          background-color: ${mode === 'light' ? colors.background.paper : '#1e1e1e'};
          color: ${textColor};
          transition: border-color 0.2s ease;
        }
        
        input:focus, textarea:focus, select:focus {
          outline: none;
          border-color: ${colors.primary.main};
          box-shadow: 0 0 0 2px ${colors.primary.light}40; /* 40 = 25% opacity */
        }
        
        button {
          font-family: inherit;
          font-size: inherit;
          cursor: pointer;
        }
        
        /* Accessibility focus styles */
        :focus-visible {
          outline: 2px solid ${colors.primary.main};
          outline-offset: 2px;
        }
        
        /* Custom scrollbar styling */
        ::-webkit-scrollbar {
          width: 8px;
          height: 8px;
        }
        
        ::-webkit-scrollbar-track {
          background: ${mode === 'light' ? colors.grey[100] : colors.grey[900]};
        }
        
        ::-webkit-scrollbar-thumb {
          background: ${mode === 'light' ? colors.grey[400] : colors.grey[700]};
          border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
          background: ${mode === 'light' ? colors.grey[500] : colors.grey[600]};
        }
        
        /* Text alignment utilities */
        .text-center { text-align: center !important; }
        .text-right { text-align: right !important; }
        .text-left { text-align: left !important; }
        .text-truncate {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        
        /* Spacing utilities */
        .m-0 { margin: 0 !important; }
        .mt-0 { margin-top: 0 !important; }
        .mr-0 { margin-right: 0 !important; }
        .mb-0 { margin-bottom: 0 !important; }
        .ml-0 { margin-left: 0 !important; }
        
        .m-1 { margin: 0.25rem !important; }
        .mt-1 { margin-top: 0.25rem !important; }
        .mr-1 { margin-right: 0.25rem !important; }
        .mb-1 { margin-bottom: 0.25rem !important; }
        .ml-1 { margin-left: 0.25rem !important; }
        
        .m-2 { margin: 0.5rem !important; }
        .mt-2 { margin-top: 0.5rem !important; }
        .mr-2 { margin-right: 0.5rem !important; }
        .mb-2 { margin-bottom: 0.5rem !important; }
        .ml-2 { margin-left: 0.5rem !important; }
        
        .m-3 { margin: 1rem !important; }
        .mt-3 { margin-top: 1rem !important; }
        .mr-3 { margin-right: 1rem !important; }
        .mb-3 { margin-bottom: 1rem !important; }
        .ml-3 { margin-left: 1rem !important; }
        
        .m-4 { margin: 1.5rem !important; }
        .mt-4 { margin-top: 1.5rem !important; }
        .mr-4 { margin-right: 1.5rem !important; }
        .mb-4 { margin-bottom: 1.5rem !important; }
        .ml-4 { margin-left: 1.5rem !important; }
        
        .m-5 { margin: 3rem !important; }
        .mt-5 { margin-top: 3rem !important; }
        .mr-5 { margin-right: 3rem !important; }
        .mb-5 { margin-bottom: 3rem !important; }
        .ml-5 { margin-left: 3rem !important; }
        
        .p-0 { padding: 0 !important; }
        .pt-0 { padding-top: 0 !important; }
        .pr-0 { padding-right: 0 !important; }
        .pb-0 { padding-bottom: 0 !important; }
        .pl-0 { padding-left: 0 !important; }
        
        .p-1 { padding: 0.25rem !important; }
        .pt-1 { padding-top: 0.25rem !important; }
        .pr-1 { padding-right: 0.25rem !important; }
        .pb-1 { padding-bottom: 0.25rem !important; }
        .pl-1 { padding-left: 0.25rem !important; }
        
        .p-2 { padding: 0.5rem !important; }
        .pt-2 { padding-top: 0.5rem !important; }
        .pr-2 { padding-right: 0.5rem !important; }
        .pb-2 { padding-bottom: 0.5rem !important; }
        .pl-2 { padding-left: 0.5rem !important; }
        
        .p-3 { padding: 1rem !important; }
        .pt-3 { padding-top: 1rem !important; }
        .pr-3 { padding-right: 1rem !important; }
        .pb-3 { padding-bottom: 1rem !important; }
        .pl-3 { padding-left: 1rem !important; }
        
        .p-4 { padding: 1.5rem !important; }
        .pt-4 { padding-top: 1.5rem !important; }
        .pr-4 { padding-right: 1.5rem !important; }
        .pb-4 { padding-bottom: 1.5rem !important; }
        .pl-4 { padding-left: 1.5rem !important; }
        
        .p-5 { padding: 3rem !important; }
        .pt-5 { padding-top: 3rem !important; }
        .pr-5 { padding-right: 3rem !important; }
        .pb-5 { padding-bottom: 3rem !important; }
        .pl-5 { padding-left: 3rem !important; }
        
        /* Display utilities */
        .d-none { display: none !important; }
        .d-inline { display: inline !important; }
        .d-inline-block { display: inline-block !important; }
        .d-block { display: block !important; }
        .d-flex { display: flex !important; }
        .d-grid { display: grid !important; }
        
        /* Flexbox utilities */
        .flex-row { flex-direction: row !important; }
        .flex-column { flex-direction: column !important; }
        .flex-wrap { flex-wrap: wrap !important; }
        .flex-nowrap { flex-wrap: nowrap !important; }
        
        .justify-content-start { justify-content: flex-start !important; }
        .justify-content-end { justify-content: flex-end !important; }
        .justify-content-center { justify-content: center !important; }
        .justify-content-between { justify-content: space-between !important; }
        .justify-content-around { justify-content: space-around !important; }
        
        .align-items-start { align-items: flex-start !important; }
        .align-items-end { align-items: flex-end !important; }
        .align-items-center { align-items: center !important; }
        .align-items-baseline { align-items: baseline !important; }
        .align-items-stretch { align-items: stretch !important; }
        
        /* Print-specific styles */
        @media print {
          body {
            background-color: #fff !important;
            color: #000 !important;
          }
          
          a {
            text-decoration: underline;
            color: #000 !important;
          }
          
          .no-print {
            display: none !important;
          }
          
          .page-break {
            page-break-before: always;
          }
        }
      `}
    />
  );
};

export default AppGlobalStyles;