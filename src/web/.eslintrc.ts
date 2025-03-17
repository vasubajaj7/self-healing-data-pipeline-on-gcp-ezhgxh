/**
 * ESLint configuration for the self-healing data pipeline web frontend
 * 
 * This configuration enforces code quality standards, consistent styling,
 * and best practices for TypeScript, React, and accessibility.
 * 
 * @see https://eslint.org/docs/user-guide/configuring
 */

import type { Linter } from 'eslint'; // eslint ^8.30.0

const config: Linter.Config = {
  // Stop looking for ESLint configurations in parent folders
  root: true,
  
  // Environment configurations
  env: {
    browser: true,
    es2021: true,
    node: true, // For build scripts and config files
    jest: true,  // For test files
  },
  
  // TypeScript configuration
  parser: '@typescript-eslint/parser', // @typescript-eslint/parser ^5.47.0
  parserOptions: {
    ecmaVersion: 2021,
    sourceType: 'module',
    ecmaFeatures: {
      jsx: true, // Enable JSX parsing
    },
    project: './tsconfig.json', // Link to TS configuration
  },
  
  // Extended configurations
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended', // TypeScript rules
    'plugin:react/recommended', // React rules
    'plugin:react-hooks/recommended', // React hooks rules
    'plugin:jsx-a11y/recommended', // Accessibility rules
    'plugin:import/errors', // Import validation
    'plugin:import/warnings',
    'plugin:import/typescript', // TypeScript import resolution
    'prettier', // Prettier integration (must be last)
  ],
  
  // Additional plugins
  plugins: [
    '@typescript-eslint', // @typescript-eslint/eslint-plugin ^5.47.0
    'react', // eslint-plugin-react ^7.31.11
    'react-hooks', // eslint-plugin-react-hooks ^4.6.0
    'jsx-a11y', // eslint-plugin-jsx-a11y ^6.6.1
    'import', // eslint-plugin-import ^2.26.0
    'prettier', // eslint-plugin-prettier ^4.2.1
  ],
  
  // Custom rule configurations
  rules: {
    // Prettier integration
    'prettier/prettier': 'error',
    
    // React specific rules
    'react/react-in-jsx-scope': 'off', // Not needed with React 17+
    'react/prop-types': 'off', // Using TypeScript for prop validation
    
    // TypeScript specific rules
    '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }], // Allow unused vars starting with _
    '@typescript-eslint/explicit-function-return-type': 'off', // Type inference is often sufficient
    '@typescript-eslint/explicit-module-boundary-types': 'off', // Type inference for exports
    '@typescript-eslint/no-explicit-any': 'warn', // Discourage 'any' type but don't error
    
    // General code quality rules
    'no-console': ['warn', { allow: ['warn', 'error', 'info'] }], // Allow specific console methods
    'prefer-const': 'error', // Enforce using const for variables that aren't reassigned
    'no-var': 'error', // Disallow var, prefer let/const
    'eqeqeq': ['error', 'always'], // Require strict equality comparisons
    'curly': ['error', 'all'], // Require curly braces for all control statements
    'quotes': ['error', 'single', { avoidEscape: true }], // Single quotes with exception for escaping
    
    // Import order rules
    'import/order': [
      'error',
      {
        'groups': [
          'builtin', // Node.js built-in modules
          'external', // npm modules
          'internal', // Internal modules
          'parent', // Modules from parent directories
          'sibling', // Modules from the same directory
          'index', // Main file in the same directory
        ],
        'newlines-between': 'always', // Require newlines between import groups
        'alphabetize': { 'order': 'asc', 'caseInsensitive': true }, // Sort import statements
      },
    ],
    
    // React hooks rules
    'react-hooks/rules-of-hooks': 'error', // Enforce Rules of Hooks
    'react-hooks/exhaustive-deps': 'warn', // Check effect dependencies
    
    // Accessibility rules
    'jsx-a11y/anchor-is-valid': 'warn', // Ensure anchors are valid
  },
  
  // Rule overrides for specific files
  overrides: [
    {
      // Test files
      files: [
        '**/*.test.ts',
        '**/*.test.tsx',
        '**/__tests__/**/*.ts',
        '**/__tests__/**/*.tsx',
      ],
      env: {
        jest: true, // Enable Jest globals
      },
      rules: {
        '@typescript-eslint/no-explicit-any': 'off', // Allow 'any' in test files
        'react/display-name': 'off', // No need for display names in tests
      },
    },
  ],
  
  // Plugin settings
  settings: {
    // React version detection
    react: {
      version: 'detect', // Auto-detect React version
    },
    // Import resolution
    'import/resolver': {
      typescript: {
        alwaysTryTypes: true, // Try using TypeScript type definitions
        project: './tsconfig.json',
      },
      node: {
        extensions: ['.js', '.jsx', '.ts', '.tsx'], // File extensions to resolve
      },
    },
  },
};

export default config;