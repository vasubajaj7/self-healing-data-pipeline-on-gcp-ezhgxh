import type { Config } from '@jest/types';

const createJestConfig = (): Config.InitialOptions => {
  return {
    // Use ts-jest as the preset for TypeScript handling
    preset: 'ts-jest',
    
    // Default test environment is Node.js
    testEnvironment: 'node',
    
    // Setup files that run after the environment is set up
    setupFilesAfterEnv: ['<rootDir>/setup.ts'],
    
    // Test matching patterns for identifying test files
    testMatch: [
      '**/__tests__/**/*.test.(ts|tsx)',
      '**/?(*.)+(spec|test).(ts|tsx)'
    ],
    
    // Configure TypeScript transformations
    transform: {
      '^.+\\.(ts|tsx)$': 'ts-jest'
    },
    
    // Module name mappers for path aliases and file mocks
    moduleNameMapper: {
      '@test/(.*)': '<rootDir>/$1',
      '@web/(.*)': '<rootDir>/../web/src/$1',
      '@utils/(.*)': '<rootDir>/utils/$1',
      '@fixtures/(.*)': '<rootDir>/fixtures/$1',
      '@mocks/(.*)': '<rootDir>/mock_data/$1',
      '\\.(css|less|scss|sass)$': 'identity-obj-proxy',
      '\\.(jpg|jpeg|png|gif|webp|svg)$': '<rootDir>/mock_data/fileMock.ts'
    },
    
    // File extensions to be processed
    moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
    
    // Files to collect coverage from
    collectCoverageFrom: [
      '../backend/**/*.{ts,js}',
      '../web/src/**/*.{ts,tsx}',
      '!**/node_modules/**',
      '!**/dist/**',
      '!**/build/**',
      '!**/*.d.ts'
    ],
    
    // Coverage thresholds by component
    coverageThreshold: {
      global: {
        statements: 85,
        branches: 85,
        functions: 85,
        lines: 85
      },
      './unit/backend/': {
        statements: 90,
        branches: 90,
        functions: 90,
        lines: 90
      },
      './unit/web/': {
        statements: 85,
        branches: 80,
        functions: 85,
        lines: 85
      }
    },
    
    // Coverage report formats
    coverageReporters: ['text', 'lcov', 'html', 'json', 'junit'],
    
    // Patterns to ignore during tests
    testPathIgnorePatterns: ['/node_modules/', '/dist/', '/build/'],
    
    // Watch plugins for interactive mode
    watchPlugins: [
      'jest-watch-typeahead/filename',
      'jest-watch-typeahead/testname'
    ],
    
    // Global configuration for ts-jest
    globals: {
      'ts-jest': {
        tsconfig: 'tsconfig.json',
        isolatedModules: true
      }
    },
    
    // Project configurations for different test types
    projects: [
      {
        displayName: 'unit',
        testMatch: ['<rootDir>/unit/**/*.test.(ts|tsx)'],
        testEnvironment: 'node'
      },
      {
        displayName: 'unit-web',
        testMatch: ['<rootDir>/unit/web/**/*.test.(ts|tsx)'],
        testEnvironment: 'jsdom',
        setupFilesAfterEnv: ['<rootDir>/setup.ts']
      },
      {
        displayName: 'integration',
        testMatch: ['<rootDir>/integration/**/*.test.(ts|tsx)'],
        testEnvironment: 'node'
      },
      {
        displayName: 'integration-web',
        testMatch: ['<rootDir>/integration/web/**/*.test.(ts|tsx)'],
        testEnvironment: 'jsdom',
        setupFilesAfterEnv: ['<rootDir>/setup.ts']
      }
    ],
    
    // Test report configuration
    reporters: [
      'default',
      ['jest-junit', {
        outputDirectory: './reports/junit',
        outputName: 'jest-junit.xml'
      }],
      ['jest-html-reporter', {
        outputPath: './reports/html/test-report.html'
      }]
    ],
    
    // Mock behavior configuration
    resetMocks: true,
    restoreMocks: true,
    clearMocks: true,
    
    // Verbose output for detailed test information
    verbose: true,
    
    // Test timeout in milliseconds
    testTimeout: 30000
  };
};

// Export the configuration
export default createJestConfig();