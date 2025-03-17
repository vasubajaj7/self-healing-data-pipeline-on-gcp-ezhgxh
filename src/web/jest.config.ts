import type { Config } from '@jest/types';

/**
 * Creates and returns the Jest configuration object
 * @returns Jest configuration object for the web frontend tests
 */
const createJestConfig = (): Config.InitialOptions => {
  return {
    // Define test environment as jsdom for browser-like environment
    testEnvironment: 'jsdom',
    
    // Configure test setup file paths
    setupFilesAfterEnv: ['<rootDir>/src/test/setup.ts'],
    
    // Set up test matching patterns
    testMatch: [
      '**/__tests__/**/*.test.(ts|tsx)',
      '**/?(*.)+(spec|test).(ts|tsx)'
    ],
    
    // Set up transform options for TypeScript files
    transform: {
      '^.+\\.(ts|tsx)$': 'ts-jest'
    },
    
    // Set up module name mapper for path aliases and file mocks
    moduleNameMapper: {
      '^@/(.*)$': '<rootDir>/src/$1',
      '\\.(css|less|scss|sass)$': 'identity-obj-proxy',
      '\\.(jpg|jpeg|png|gif|webp|svg)$': '<rootDir>/src/test/mocks/fileMock.ts'
    },
    
    // Supported file extensions
    moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
    
    // Configure coverage collection and reporting
    collectCoverageFrom: [
      'src/**/*.{ts,tsx}',
      '!src/**/*.d.ts',
      '!src/main.tsx',
      '!src/vite-env.d.ts',
      '!src/test/**/*',
      '!**/node_modules/**'
    ],
    
    // Coverage thresholds
    coverageThreshold: {
      global: {
        statements: 80,
        branches: 80,
        functions: 80,
        lines: 80
      }
    },
    
    // Coverage reporters
    coverageReporters: ['text', 'lcov', 'html'],
    
    // Paths to ignore during testing
    testPathIgnorePatterns: ['/node_modules/', '/dist/'],
    
    // Watch plugins for better developer experience
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
    
    // Reset, restore, and clear mocks between tests
    resetMocks: true,
    restoreMocks: true,
    clearMocks: true
  };
};

export default createJestConfig();