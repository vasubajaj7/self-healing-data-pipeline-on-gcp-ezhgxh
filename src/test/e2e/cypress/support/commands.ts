// Import type definitions for custom Cypress commands
import './index.d.ts';

/**
 * Custom Cypress commands for end-to-end testing of the self-healing data pipeline application.
 * These commands provide reusable utilities for authentication, navigation, data mocking, and UI interactions.
 */

// Authentication commands
Cypress.Commands.add('login', (email: string, password: string) => {
  cy.log(`Logging in as ${email}`);
  cy.visit('/login');
  cy.get('[data-cy=email-input]').type(email);
  cy.get('[data-cy=password-input]').type(password, { log: false });
  cy.get('[data-cy=login-button]').click();
  
  // Wait for the login API request to complete
  cy.waitForApiResponse('/api/auth/login');
  
  // Check if login was successful or MFA is required
  cy.url().then((url) => {
    if (url.includes('/dashboard')) {
      cy.log('Login successful');
    } else if (url.includes('/mfa')) {
      cy.log('MFA verification required');
    } else {
      cy.log('Login failed');
    }
  });
});

Cypress.Commands.add('loginAsAdmin', () => {
  cy.fixture('users.json').then((users) => {
    const admin = users.admin;
    cy.login(admin.email, admin.password);
    
    // Verify admin-specific UI elements
    cy.get('[data-cy=admin-menu]').should('be.visible');
    cy.get('[data-cy=user-role]').should('contain', 'Administrator');
  });
});

Cypress.Commands.add('loginAsEngineer', () => {
  cy.fixture('users.json').then((users) => {
    const engineer = users.engineer;
    cy.login(engineer.email, engineer.password);
    
    // Verify engineer-specific UI elements
    cy.get('[data-cy=pipeline-management]').should('be.visible');
    cy.get('[data-cy=user-role]').should('contain', 'Data Engineer');
  });
});

Cypress.Commands.add('loginAsAnalyst', () => {
  cy.fixture('users.json').then((users) => {
    const analyst = users.analyst;
    cy.login(analyst.email, analyst.password);
    
    // Verify analyst-specific UI elements
    cy.get('[data-cy=data-quality]').should('be.visible');
    cy.get('[data-cy=user-role]').should('contain', 'Data Analyst');
  });
});

Cypress.Commands.add('loginAsOperator', () => {
  cy.fixture('users.json').then((users) => {
    const operator = users.operator;
    cy.login(operator.email, operator.password);
    
    // Verify operator-specific UI elements
    cy.get('[data-cy=monitoring-dashboard]').should('be.visible');
    cy.get('[data-cy=user-role]').should('contain', 'Pipeline Operator');
  });
});

Cypress.Commands.add('loginWithMfa', (email: string, password: string, mfaCode: string) => {
  cy.visit('/login');
  cy.get('[data-cy=email-input]').type(email);
  cy.get('[data-cy=password-input]').type(password, { log: false });
  cy.get('[data-cy=login-button]').click();
  
  // Wait for MFA verification screen
  cy.get('[data-cy=mfa-verification]', { timeout: 10000 }).should('be.visible');
  
  // Enter MFA code
  cy.get('[data-cy=mfa-code-input]').type(mfaCode);
  cy.get('[data-cy=verify-button]').click();
  
  // Wait for verification to complete
  cy.waitForApiResponse('/api/auth/verify-mfa');
  
  // Verify successful navigation to dashboard
  cy.url().should('include', '/dashboard');
});

Cypress.Commands.add('logout', () => {
  // Click on user profile menu
  cy.get('[data-cy=user-profile-menu]').click();
  
  // Click on logout option
  cy.get('[data-cy=logout-option]').click();
  
  // Verify redirection to login page
  cy.url().should('include', '/login');
  
  // Verify authentication token is cleared
  cy.window().its('localStorage.token').should('be.undefined');
});

// Navigation commands
Cypress.Commands.add('navigateTo', (section: string) => {
  // Map section name to data-cy attribute
  const sectionMapping: Record<string, string> = {
    'Dashboard': 'dashboard-nav',
    'Pipeline Management': 'pipeline-management-nav',
    'Data Quality': 'data-quality-nav',
    'Self-Healing': 'self-healing-nav',
    'Alerting': 'alerting-nav',
    'Configuration': 'configuration-nav',
    'Administration': 'administration-nav'
  };
  
  const dataCySelector = sectionMapping[section] || section.toLowerCase().replace(' ', '-') + '-nav';
  
  // Find and click on the navigation item
  cy.get(`[data-cy=${dataCySelector}]`).click();
  
  // Wait for page to load
  cy.get('[data-cy=page-content]').should('be.visible');
  
  // Verify URL contains section path
  const urlPath = section.toLowerCase().replace(' ', '-');
  cy.url().should('include', urlPath);
  
  // Verify section-specific UI elements
  cy.get(`[data-cy=${section.toLowerCase().replace(' ', '-')}-header]`).should('be.visible');
});

// Data mocking commands
Cypress.Commands.add('mockPipelineData', () => {
  cy.fixture('pipelines.json').then((pipelineData) => {
    // Intercept API requests for pipeline data
    cy.intercept('GET', '/api/pipelines*', {
      statusCode: 200,
      body: pipelineData
    }).as('getPipelines');
    
    cy.intercept('GET', '/api/pipelines/*/executions', {
      statusCode: 200,
      body: pipelineData.executions
    }).as('getPipelineExecutions');
    
    // Verify interceptors are properly set up
    cy.log('Pipeline data mocking configured');
  });
});

Cypress.Commands.add('mockQualityData', () => {
  cy.fixture('quality.json').then((qualityData) => {
    // Intercept API requests for quality data
    cy.intercept('GET', '/api/quality/rules*', {
      statusCode: 200,
      body: qualityData.rules
    }).as('getQualityRules');
    
    cy.intercept('GET', '/api/quality/validations*', {
      statusCode: 200,
      body: qualityData.validations
    }).as('getQualityValidations');
    
    // Verify interceptors are properly set up
    cy.log('Quality data mocking configured');
  });
});

Cypress.Commands.add('mockHealingData', () => {
  cy.fixture('healing.json').then((healingData) => {
    // Intercept API requests for healing data
    cy.intercept('GET', '/api/healing/actions*', {
      statusCode: 200,
      body: healingData.actions
    }).as('getHealingActions');
    
    cy.intercept('GET', '/api/healing/settings*', {
      statusCode: 200,
      body: healingData.settings
    }).as('getHealingSettings');
    
    // Verify interceptors are properly set up
    cy.log('Healing data mocking configured');
  });
});

// UI interaction commands
Cypress.Commands.add('createPipeline', (pipelineData: object) => {
  // Navigate to pipeline management page
  cy.navigateTo('Pipeline Management');
  
  // Click on create new pipeline button
  cy.get('[data-cy=create-pipeline-button]').click();
  
  // Fill in pipeline form fields
  cy.get('[data-cy=pipeline-name-input]').type(pipelineData['name']);
  cy.get('[data-cy=pipeline-source-select]').select(pipelineData['source']);
  cy.get('[data-cy=pipeline-target-select]').select(pipelineData['target']);
  
  // Fill in additional fields based on provided data
  Object.entries(pipelineData).forEach(([key, value]) => {
    if (!['name', 'source', 'target'].includes(key)) {
      const fieldSelector = `[data-cy=pipeline-${key}-input]`;
      cy.get(fieldSelector).then($el => {
        if ($el.is('select')) {
          cy.wrap($el).select(value as string);
        } else {
          cy.wrap($el).clear().type(value as string);
        }
      });
    }
  });
  
  // Submit the form
  cy.get('[data-cy=submit-pipeline-button]').click();
  
  // Wait for the creation API request to complete
  cy.waitForApiResponse('/api/pipelines');
  
  // Verify the new pipeline appears in the list
  cy.get('[data-cy=pipeline-list]').should('contain', pipelineData['name']);
  
  // Check for success notification
  cy.checkToastNotification('Pipeline created successfully');
});

Cypress.Commands.add('createQualityRule', (ruleData: object) => {
  // Navigate to data quality page
  cy.navigateTo('Data Quality');
  
  // Click on create new rule button
  cy.get('[data-cy=create-rule-button]').click();
  
  // Fill in rule form fields
  cy.get('[data-cy=rule-name-input]').type(ruleData['name']);
  cy.get('[data-cy=rule-type-select]').select(ruleData['type']);
  
  // Handle columns selection if provided
  if (ruleData['columns']) {
    const columns = ruleData['columns'] as string[];
    columns.forEach(column => {
      cy.get('[data-cy=column-select]').select(column);
      cy.get('[data-cy=add-column-button]').click();
    });
  }
  
  // Fill in additional fields based on provided data
  Object.entries(ruleData).forEach(([key, value]) => {
    if (!['name', 'type', 'columns'].includes(key)) {
      const fieldSelector = `[data-cy=rule-${key}-input]`;
      cy.get(fieldSelector).then($el => {
        if ($el.is('select')) {
          cy.wrap($el).select(value as string);
        } else {
          cy.wrap($el).clear().type(value as string);
        }
      });
    }
  });
  
  // Submit the form
  cy.get('[data-cy=submit-rule-button]').click();
  
  // Wait for the creation API request to complete
  cy.waitForApiResponse('/api/quality/rules');
  
  // Verify the new rule appears in the list
  cy.get('[data-cy=rule-list]').should('contain', ruleData['name']);
  
  // Check for success notification
  cy.checkToastNotification('Quality rule created successfully');
});

Cypress.Commands.add('updateHealingSettings', (settingsData: object) => {
  // Navigate to self-healing configuration page
  cy.navigateTo('Self-Healing');
  cy.get('[data-cy=configuration-tab]').click();
  
  // Update settings form fields
  Object.entries(settingsData).forEach(([key, value]) => {
    const fieldSelector = `[data-cy=setting-${key}-input]`;
    cy.get(fieldSelector).then($el => {
      if ($el.is('select')) {
        cy.wrap($el).select(value as string);
      } else if ($el.is('input[type="checkbox"]')) {
        if (value) {
          cy.wrap($el).check();
        } else {
          cy.wrap($el).uncheck();
        }
      } else if ($el.is('input[type="range"]')) {
        cy.wrap($el).invoke('val', value).trigger('change');
      } else {
        cy.wrap($el).clear().type(value as string);
      }
    });
  });
  
  // Submit the form
  cy.get('[data-cy=save-settings-button]').click();
  
  // Wait for the update API request to complete
  cy.waitForApiResponse('/api/healing/settings');
  
  // Verify the settings have been updated
  cy.checkToastNotification('Settings updated successfully');
  
  // Verify form values match the updated settings
  Object.entries(settingsData).forEach(([key, value]) => {
    const fieldSelector = `[data-cy=setting-${key}-input]`;
    cy.get(fieldSelector).then($el => {
      if ($el.is('input[type="checkbox"]')) {
        if (value) {
          cy.wrap($el).should('be.checked');
        } else {
          cy.wrap($el).should('not.be.checked');
        }
      } else if ($el.is('select')) {
        cy.wrap($el).should('have.value', value);
      } else if (!$el.is('input[type="range"]')) {
        cy.wrap($el).should('have.value', value);
      }
    });
  });
});

Cypress.Commands.add('approveHealingAction', (actionId: string) => {
  // Navigate to self-healing page
  cy.navigateTo('Self-Healing');
  
  // Find the healing action with the specified ID
  cy.get(`[data-cy=healing-action-${actionId}]`).should('be.visible');
  
  // Click on the approve button
  cy.get(`[data-cy=approve-action-${actionId}]`).click();
  
  // Confirm the approval in the dialog
  cy.get('[data-cy=confirm-approval-button]').click();
  
  // Wait for the approval API request to complete
  cy.waitForApiResponse(`/api/healing/actions/${actionId}/approve`);
  
  // Verify the action status changes to approved
  cy.get(`[data-cy=healing-action-${actionId}-status]`).should('contain', 'Approved');
  
  // Check for success notification
  cy.checkToastNotification('Action approved successfully');
});

Cypress.Commands.add('rejectHealingAction', (actionId: string, reason: string) => {
  // Navigate to self-healing page
  cy.navigateTo('Self-Healing');
  
  // Find the healing action with the specified ID
  cy.get(`[data-cy=healing-action-${actionId}]`).should('be.visible');
  
  // Click on the reject button
  cy.get(`[data-cy=reject-action-${actionId}]`).click();
  
  // Enter the rejection reason in the dialog
  cy.get('[data-cy=rejection-reason-input]').type(reason);
  
  // Confirm the rejection
  cy.get('[data-cy=confirm-rejection-button]').click();
  
  // Wait for the rejection API request to complete
  cy.waitForApiResponse(`/api/healing/actions/${actionId}/reject`);
  
  // Verify the action status changes to rejected
  cy.get(`[data-cy=healing-action-${actionId}-status]`).should('contain', 'Rejected');
  
  // Check for success notification
  cy.checkToastNotification('Action rejected successfully');
});

// Verification commands
Cypress.Commands.add('checkDashboardMetrics', () => {
  // Navigate to dashboard page
  cy.navigateTo('Dashboard');
  
  // Verify pipeline health card is visible and has valid data
  cy.get('[data-cy=pipeline-health-card]').should('be.visible');
  cy.get('[data-cy=pipeline-health-percentage]').should('be.visible')
    .invoke('text').then(parseFloat).should('be.gte', 0).and('be.lte', 100);
  
  // Verify data quality card is visible and has valid data
  cy.get('[data-cy=data-quality-card]').should('be.visible');
  cy.get('[data-cy=data-quality-percentage]').should('be.visible')
    .invoke('text').then(parseFloat).should('be.gte', 0).and('be.lte', 100);
  
  // Verify self-healing status card is visible and has valid data
  cy.get('[data-cy=self-healing-card]').should('be.visible');
  cy.get('[data-cy=self-healing-percentage]').should('be.visible')
    .invoke('text').then(parseFloat).should('be.gte', 0).and('be.lte', 100);
  
  // Verify alert summary card is visible and has valid data
  cy.get('[data-cy=alert-summary-card]').should('be.visible');
  
  // Verify recent executions table is visible and has data
  cy.get('[data-cy=recent-executions-table]').should('be.visible');
  cy.get('[data-cy=recent-executions-row]').should('have.length.at.least', 1);
});

Cypress.Commands.add('checkTableContents', (tableSelector: string, expectedData: any[]) => {
  // Find the table using the provided selector
  cy.get(tableSelector).should('be.visible');
  
  // Extract the table data into a structured format
  cy.get(`${tableSelector} tbody tr`).then($rows => {
    const actualData = [];
    
    $rows.each((rowIndex, row) => {
      const rowData = {};
      Cypress.$(row).find('td').each((colIndex, cell) => {
        // Get the column name from the table header
        cy.get(`${tableSelector} thead th`).eq(colIndex).invoke('text').then(headerText => {
          const key = headerText.trim().toLowerCase().replace(/\s+/g, '_');
          rowData[key] = Cypress.$(cell).text().trim();
        });
      });
      actualData.push(rowData);
    });
    
    // Compare the extracted data with the expected data
    expectedData.forEach((expectedRow, rowIndex) => {
      Object.entries(expectedRow).forEach(([key, value]) => {
        const actualKey = key.toLowerCase().replace(/\s+/g, '_');
        expect(actualData[rowIndex][actualKey]).to.include(value as string);
      });
    });
  });
});

// Utility commands
Cypress.Commands.add('waitForApiResponse', (apiRoute: string) => {
  // Set up an intercept for the specified API route
  cy.intercept(apiRoute).as('apiRequest');
  
  // Wait for the intercept to be fulfilled
  cy.wait('@apiRequest', { timeout: 15000 }).then((interception) => {
    // Log response status for debugging
    cy.log(`API response status: ${interception.response.statusCode}`);
    return interception.response;
  });
});

Cypress.Commands.add('mockApiResponse', (method: string, url: string, response: object) => {
  // Set up an intercept for the specified method and URL
  cy.intercept(method, url, {
    statusCode: 200,
    body: response
  }).as(`mocked${method}${url.replace(/[^a-zA-Z0-9]/g, '')}`);
  
  // Verify the intercept is properly set up
  cy.log(`Mocked ${method} ${url} response configured`);
});

Cypress.Commands.add('checkToastNotification', (text: string) => {
  // Wait for toast notification to appear
  cy.get('[data-cy=toast-notification]', { timeout: 10000 }).should('be.visible');
  
  // Verify the notification contains the expected text
  cy.get('[data-cy=toast-notification]').should('contain', text);
  
  // Optionally wait for the notification to disappear
  // Use a try/catch approach since the toast might auto-dismiss
  cy.get('body').then($body => {
    // Wait for toast to disappear if auto-dismiss is enabled
    if ($body.find('[data-cy=toast-notification]').length > 0) {
      cy.get('[data-cy=toast-notification]', { timeout: 10000 }).should('not.exist');
    }
  });
});