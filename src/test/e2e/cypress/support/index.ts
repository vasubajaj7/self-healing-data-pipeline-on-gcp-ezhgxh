// Import custom commands
import './commands';

// Import code coverage plugin
import '@cypress/code-coverage/support';

/**
 * This file serves as the main entry point for Cypress support files.
 * It imports all custom commands defined in commands.ts and configures global
 * behavior for Cypress tests. The file is automatically loaded by Cypress before
 * test files are executed, making all custom commands available throughout the test suite.
 * It also sets up code coverage tracking and any other global configurations needed
 * for the testing environment.
 */

// Set default viewport size for consistent test environment
Cypress.config('viewportWidth', 1280);
Cypress.config('viewportHeight', 800);

// Configure global error handling for uncaught exceptions
// This prevents tests from failing when the application throws uncaught exceptions
// which is useful for testing error handling scenarios
Cypress.on('uncaught:exception', (err) => {
  // Log the error for debugging purposes
  console.error(err);
  
  // Return false to prevent the error from failing the test
  return false;
});

// Configure network request handling
// Note: Cypress.server() is deprecated in Cypress 10+, but we're keeping this
// comment as a reference. Use cy.intercept() in individual tests for network stubbing.
// Network requests are configured in commands.ts for specific API interactions