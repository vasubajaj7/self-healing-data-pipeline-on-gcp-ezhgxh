module.exports = {
  root: true,
  env: {
    node: true,
    browser: true,
    es2020: true,
    jest: true,
    'cypress/globals': true,
  },
  parser: '@typescript-eslint/parser', // @typescript-eslint/parser v5.47.0
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: 'module',
    project: './tsconfig.json',
    ecmaFeatures: {
      jsx: true,
    },
  },
  extends: [
    'eslint:recommended', // eslint v8.30.0
    'plugin:@typescript-eslint/recommended', // @typescript-eslint/eslint-plugin v5.47.0
    'plugin:jest/recommended', // eslint-plugin-jest v27.2.1
    'plugin:cypress/recommended', // eslint-plugin-cypress v2.13.3
    'plugin:playwright/recommended', // eslint-plugin-playwright v0.12.0
    'plugin:import/errors', // eslint-plugin-import v2.26.0
    'plugin:import/warnings',
    'plugin:import/typescript',
    'prettier', // eslint-config-prettier v8.5.0
  ],
  plugins: [
    '@typescript-eslint', // @typescript-eslint/eslint-plugin v5.47.0
    'jest', // eslint-plugin-jest v27.2.1
    'cypress', // eslint-plugin-cypress v2.13.3
    'playwright', // eslint-plugin-playwright v0.12.0
    'import', // eslint-plugin-import v2.26.0
    'prettier', // eslint-plugin-prettier v4.2.1
  ],
  rules: {
    'prettier/prettier': 'error',
    '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    '@typescript-eslint/explicit-function-return-type': 'off',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    '@typescript-eslint/no-explicit-any': 'off',
    'no-console': ['warn', { allow: ['warn', 'error', 'info'] }],
    'prefer-const': 'error',
    'no-var': 'error',
    'eqeqeq': ['error', 'always'],
    'curly': ['error', 'all'],
    'quotes': ['error', 'single', { avoidEscape: true }],
    'import/order': [
      'error',
      {
        groups: ['builtin', 'external', 'internal', 'parent', 'sibling', 'index'],
        'newlines-between': 'always',
        alphabetize: {
          order: 'asc',
          caseInsensitive: true,
        },
      },
    ],
    'jest/no-disabled-tests': 'warn',
    'jest/no-focused-tests': 'error',
    'jest/no-identical-title': 'error',
    'jest/valid-expect': 'error',
    'jest/expect-expect': 'warn',
  },
  overrides: [
    {
      files: ['**/*.test.ts', '**/*.test.tsx', '**/__tests__/**/*.ts', '**/__tests__/**/*.tsx'],
      env: {
        jest: true,
      },
      rules: {
        '@typescript-eslint/no-explicit-any': 'off',
      },
    },
    {
      files: ['**/e2e/cypress/**/*.ts', '**/e2e/cypress/**/*.js'],
      env: {
        'cypress/globals': true,
      },
      rules: {
        'cypress/no-unnecessary-waiting': 'warn',
        'cypress/assertion-before-screenshot': 'error',
      },
    },
    {
      files: ['**/e2e/playwright/**/*.ts', '**/e2e/playwright/**/*.js'],
      env: {
        node: true,
      },
      rules: {
        'playwright/no-wait-for-timeout': 'warn',
        'playwright/expect-expect': 'error',
      },
    },
    {
      files: ['**/performance/**/*.js'],
      rules: {
        '@typescript-eslint/no-var-requires': 'off',
        'no-undef': 'off',
      },
    },
  ],
  settings: {
    'import/resolver': {
      typescript: {
        alwaysTryTypes: true,
        project: './tsconfig.json',
      },
      node: {
        extensions: ['.js', '.jsx', '.ts', '.tsx'],
      },
    },
  },
};