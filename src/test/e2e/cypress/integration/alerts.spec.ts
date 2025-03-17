/// <reference types="cypress" />

describe('Alerts Page', () => {
  beforeEach(() => {
    // Log in as a pipeline operator
    cy.loginAsOperator();

    // Load fixtures and set up mocks
    cy.fixture('alerts.json').then((alertsData) => {
      // Mock API responses for the alerts page
      cy.intercept('GET', '/api/alerts', { 
        statusCode: 200, 
        body: { items: alertsData.alerts } 
      }).as('getAlerts');
      
      cy.intercept('GET', '/api/alerts/statistics', { 
        statusCode: 200, 
        body: alertsData.statistics 
      }).as('getAlertStatistics');
      
      cy.intercept('GET', '/api/alerts/trend', { 
        statusCode: 200, 
        body: alertsData.trend 
      }).as('getAlertTrend');
      
      cy.intercept('GET', '/api/alerts/channels', { 
        statusCode: 200, 
        body: alertsData.channels 
      }).as('getNotificationChannels');
    });

    // Visit the alerts page
    cy.visit('/alerts');
    
    // Wait for API responses
    cy.wait(['@getAlerts', '@getAlertStatistics', '@getAlertTrend', '@getNotificationChannels']);
  });

  it('should display all alert dashboard components correctly', () => {
    // Verify all main components are visible
    cy.get("[data-testid='alerts-page-title']").should('be.visible');
    cy.get("[data-testid='active-alerts-table']").should('be.visible');
    cy.get("[data-testid='alert-stats-card']").should('be.visible');
    cy.get("[data-testid='alert-trend-chart']").should('be.visible');
    cy.get("[data-testid='notification-channels-card']").should('be.visible');
  });

  it('should display active alerts correctly', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertsCount = alertsData.alerts.length;
      
      // Verify alerts table shows the correct number of alerts
      cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', alertsCount);
      
      // Verify first alert details
      const firstAlert = alertsData.alerts[0];
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().within(() => {
        // Check severity indicator
        cy.get("[data-testid='severity-indicator']").should('have.class', firstAlert.severity.toLowerCase());
        
        // Check alert description
        cy.get("[data-testid='alert-description']").should('contain', firstAlert.description);
        
        // Check timestamp
        cy.get("[data-testid='alert-timestamp']").should('exist');
        
        // Check status
        cy.get("[data-testid='alert-status']").should('contain', firstAlert.status);
      });
    });
  });

  it('should display alert statistics correctly', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const stats = alertsData.statistics;
      
      // Verify alert statistics
      cy.get("[data-testid='alert-stats-card']").within(() => {
        cy.get("[data-testid='high-priority-count']").should('contain', stats.high);
        cy.get("[data-testid='medium-priority-count']").should('contain', stats.medium);
        cy.get("[data-testid='low-priority-count']").should('contain', stats.low);
        cy.get("[data-testid='total-count']").should('contain', stats.total);
      });
    });
  });

  it('should display alert trend chart correctly', () => {
    // Verify chart is displayed
    cy.get("[data-testid='alert-trend-chart']").should('be.visible');
    
    // Verify chart has appropriate elements
    cy.get("[data-testid='alert-trend-chart']").within(() => {
      cy.get('.recharts-cartesian-axis').should('exist');
      cy.get('.recharts-line').should('exist');
      cy.get('.recharts-legend-item').should('exist');
    });
  });

  it('should display notification channels correctly', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const channels = alertsData.channels;
      
      // Verify notification channels display
      cy.get("[data-testid='notification-channels-card']").within(() => {
        cy.get("[data-testid='teams-status']").should('contain', channels.teams.sent);
        cy.get("[data-testid='email-status']").should('contain', channels.email.sent);
        cy.get("[data-testid='sms-status']").should('contain', channels.sms.sent);
        cy.get("[data-testid='pending-status']").should('contain', channels.pending);
      });
    });
  });

  it('should display alert details when an alert is selected', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API response for alert details
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      
      // Wait for the API call to complete
      cy.wait('@getAlertDetails');
      
      // Verify alert details display
      cy.get("[data-testid='alert-details-card']").should('be.visible');
      cy.get("[data-testid='alert-details-card']").within(() => {
        const alert = alertsData.alerts[0];
        cy.get("[data-testid='alert-title']").should('contain', alert.description);
        cy.get("[data-testid='alert-severity']").should('contain', alert.severity);
        cy.get("[data-testid='alert-timestamp']").should('exist');
        cy.get("[data-testid='alert-component']").should('contain', alert.component);
        cy.get("[data-testid='alert-status']").should('contain', alert.status);
      });
    });
  });

  it('should display related alerts when an alert is selected', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('GET', `/api/alerts/${alertId}/related`, {
        statusCode: 200,
        body: { items: alertsData.relatedAlerts }
      }).as('getRelatedAlerts');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      
      // Wait for the API calls to complete
      cy.wait(['@getAlertDetails', '@getRelatedAlerts']);
      
      // Verify related alerts display
      cy.get("[data-testid='related-alerts-card']").should('be.visible');
      cy.get("[data-testid='related-alerts-card'] [data-testid='related-alert-item']")
        .should('have.length', alertsData.relatedAlerts.length);
    });
  });

  it('should display suggested actions when an alert is selected', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('GET', `/api/alerts/${alertId}/actions`, {
        statusCode: 200,
        body: { items: alertsData.suggestedActions }
      }).as('getSuggestedActions');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      
      // Wait for the API calls to complete
      cy.wait(['@getAlertDetails', '@getSuggestedActions']);
      
      // Verify suggested actions display
      cy.get("[data-testid='suggested-actions-card']").should('be.visible');
      cy.get("[data-testid='suggested-actions-card'] [data-testid='action-item']")
        .should('have.length', alertsData.suggestedActions.length);
    });
  });

  it('should filter alerts by severity', () => {
    // Mock the API response for filtered alerts
    cy.intercept('GET', '/api/alerts?severity=High', {
      statusCode: 200,
      body: {
        items: [
          { alert_id: '1', severity: 'High', description: 'High severity alert', status: 'Active' }
        ]
      }
    }).as('getFilteredAlerts');
    
    // Select High severity from filter dropdown
    cy.get("[data-testid='severity-filter']").click();
    cy.get("[data-testid='severity-option-high']").click();
    
    // Wait for the API call
    cy.wait('@getFilteredAlerts');
    
    // Verify only high severity alerts are shown
    cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', 1);
    cy.get("[data-testid='active-alerts-table'] tbody tr").first()
      .find("[data-testid='severity-indicator']").should('have.class', 'high');
  });

  it('should filter alerts by status', () => {
    // Mock the API response for filtered alerts
    cy.intercept('GET', '/api/alerts?status=Acknowledged', {
      statusCode: 200,
      body: {
        items: [
          { alert_id: '2', severity: 'Medium', description: 'Acknowledged alert', status: 'Acknowledged' }
        ]
      }
    }).as('getFilteredAlerts');
    
    // Select Acknowledged status from filter dropdown
    cy.get("[data-testid='status-filter']").click();
    cy.get("[data-testid='status-option-acknowledged']").click();
    
    // Wait for the API call
    cy.wait('@getFilteredAlerts');
    
    // Verify only acknowledged alerts are shown
    cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', 1);
    cy.get("[data-testid='active-alerts-table'] tbody tr").first()
      .find("[data-testid='alert-status']").should('contain', 'Acknowledged');
  });

  it('should filter alerts by component', () => {
    // Mock the API response for filtered alerts
    cy.intercept('GET', '/api/alerts?component=BigQuery', {
      statusCode: 200,
      body: {
        items: [
          { alert_id: '3', severity: 'High', description: 'BigQuery alert', component: 'BigQuery', status: 'Active' }
        ]
      }
    }).as('getFilteredAlerts');
    
    // Select BigQuery component from filter dropdown
    cy.get("[data-testid='component-filter']").click();
    cy.get("[data-testid='component-option-bigquery']").click();
    
    // Wait for the API call
    cy.wait('@getFilteredAlerts');
    
    // Verify only BigQuery alerts are shown
    cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', 1);
    cy.get("[data-testid='active-alerts-table'] tbody tr").first()
      .find("[data-testid='alert-component']").should('contain', 'BigQuery');
  });

  it('should filter alerts by time range', () => {
    // Mock the API response for filtered alerts
    cy.intercept('GET', '/api/alerts?timeRange=7d', {
      statusCode: 200,
      body: {
        items: [
          { alert_id: '4', severity: 'Medium', description: 'Recent alert', status: 'Active' }
        ]
      }
    }).as('getFilteredAlerts');
    
    // Select Last 7 Days from time range dropdown
    cy.get("[data-testid='time-range-filter']").click();
    cy.get("[data-testid='time-range-option-7d']").click();
    
    // Wait for the API call
    cy.wait('@getFilteredAlerts');
    
    // Verify filtered alerts are shown
    cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', 1);
    cy.get("[data-testid='time-range-indicator']").should('contain', 'Last 7 Days');
  });

  it('should acknowledge an alert successfully', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('POST', `/api/alerts/${alertId}/acknowledge`, {
        statusCode: 200,
        body: { success: true }
      }).as('acknowledgeAlert');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      cy.wait('@getAlertDetails');
      
      // Click the acknowledge button
      cy.get("[data-testid='acknowledge-button']").click();
      
      // Fill in the acknowledgment form
      cy.get("[data-testid='acknowledge-form']").should('be.visible');
      cy.get("[data-testid='notes-input']").type('Investigating the issue');
      cy.get("[data-testid='submit-button']").click();
      
      // Verify the API call was made with correct data
      cy.wait('@acknowledgeAlert').its('request.body').should('deep.include', {
        notes: 'Investigating the issue'
      });
      
      // Verify success notification
      cy.get("[data-testid='success-notification']").should('be.visible')
        .and('contain', 'Alert acknowledged successfully');
        
      // Verify the alert status has updated
      cy.get("[data-testid='alert-details-card'] [data-testid='alert-status']")
        .should('contain', 'Acknowledged');
    });
  });

  it('should escalate an alert successfully', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('POST', `/api/alerts/${alertId}/escalate`, {
        statusCode: 200,
        body: { success: true }
      }).as('escalateAlert');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      cy.wait('@getAlertDetails');
      
      // Click the escalate button
      cy.get("[data-testid='escalate-button']").click();
      
      // Fill in the escalation form
      cy.get("[data-testid='escalate-form']").should('be.visible');
      cy.get("[data-testid='escalation-level']").click();
      cy.get("[data-testid='escalation-level-option-manager']").click();
      cy.get("[data-testid='notes-input']").type('Requires urgent attention');
      cy.get("[data-testid='submit-button']").click();
      
      // Verify the API call was made with correct data
      cy.wait('@escalateAlert').its('request.body').should('deep.include', {
        escalationLevel: 'manager',
        notes: 'Requires urgent attention'
      });
      
      // Verify success notification
      cy.get("[data-testid='success-notification']").should('be.visible')
        .and('contain', 'Alert escalated successfully');
    });
  });

  it('should resolve an alert successfully', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('POST', `/api/alerts/${alertId}/resolve`, {
        statusCode: 200,
        body: { success: true }
      }).as('resolveAlert');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      cy.wait('@getAlertDetails');
      
      // Click the resolve button
      cy.get("[data-testid='resolve-button']").click();
      
      // Fill in the resolution form
      cy.get("[data-testid='resolve-form']").should('be.visible');
      cy.get("[data-testid='notes-input']").type('Issue has been fixed');
      cy.get("[data-testid='submit-button']").click();
      
      // Verify the API call was made with correct data
      cy.wait('@resolveAlert').its('request.body').should('deep.include', {
        notes: 'Issue has been fixed'
      });
      
      // Verify success notification
      cy.get("[data-testid='success-notification']").should('be.visible')
        .and('contain', 'Alert resolved successfully');
        
      // Verify the alert status has updated
      cy.get("[data-testid='alert-details-card'] [data-testid='alert-status']")
        .should('contain', 'Resolved');
    });
  });

  it('should suppress similar alerts successfully', () => {
    cy.fixture('alerts.json').then((alertsData) => {
      const alertId = alertsData.alerts[0].alert_id;
      
      // Mock the API responses
      cy.intercept('GET', `/api/alerts/${alertId}`, {
        statusCode: 200,
        body: alertsData.alerts[0]
      }).as('getAlertDetails');
      
      cy.intercept('POST', '/api/alerts/suppress', {
        statusCode: 200,
        body: { success: true }
      }).as('suppressAlerts');
      
      // Mock the updated alerts list after suppression
      cy.intercept('GET', '/api/alerts', {
        statusCode: 200,
        body: { items: [alertsData.alerts[1]] } // Only return other alerts
      }).as('getUpdatedAlerts');
      
      // Click on the first alert
      cy.get("[data-testid='active-alerts-table'] tbody tr").first().click();
      cy.wait('@getAlertDetails');
      
      // Click the suppress similar button
      cy.get("[data-testid='suppress-similar-button']").click();
      
      // Fill in the suppress form
      cy.get("[data-testid='suppress-form']").should('be.visible');
      cy.get("[data-testid='duration-input']").type('24');
      cy.get("[data-testid='notes-input']").type('Known issue, will be fixed in next release');
      cy.get("[data-testid='submit-button']").click();
      
      // Verify the API call was made with correct data
      cy.wait('@suppressAlerts').its('request.body').should('deep.include', {
        pattern: alertsData.alerts[0].pattern,
        duration: 24,
        notes: 'Known issue, will be fixed in next release'
      });
      
      // Verify success notification
      cy.get("[data-testid='success-notification']").should('be.visible')
        .and('contain', 'Similar alerts suppressed successfully');
        
      // Verify the alerts list has been updated
      cy.wait('@getUpdatedAlerts');
      cy.get("[data-testid='active-alerts-table'] tbody tr").should('have.length', 1);
    });
  });

  it('should navigate to alert history page', () => {
    // Mock the alert history API response
    cy.intercept('GET', '/api/alerts/history', {
      statusCode: 200,
      body: { items: [] }
    }).as('getAlertHistory');
    
    // Click on the alert history tab
    cy.get("[data-testid='alert-history-tab']").click();
    
    // Verify navigation to alert history page
    cy.url().should('include', '/alerts/history');
    cy.get("[data-testid='alert-history-title']").should('be.visible');
    cy.wait('@getAlertHistory');
    cy.get("[data-testid='alert-history-table']").should('be.visible');
  });

  it('should navigate to notification config page', () => {
    // Mock the notification config API response
    cy.intercept('GET', '/api/alerts/notification-config', {
      statusCode: 200,
      body: { channels: {} }
    }).as('getNotificationConfig');
    
    // Click on the notification config tab
    cy.get("[data-testid='notification-config-tab']").click();
    
    // Verify navigation to notification config page
    cy.url().should('include', '/alerts/notification-config');
    cy.get("[data-testid='notification-config-title']").should('be.visible');
    cy.wait('@getNotificationConfig');
    cy.get("[data-testid='notification-settings-form']").should('be.visible');
  });

  it('should show loading state while fetching alerts data', () => {
    // First visit without waiting for responses to see loading state
    cy.visit('/alerts', {
      onBeforeLoad: (win) => {
        // Stub fetch to delay responses
        cy.stub(win, 'fetch').callsFake(() => {
          return new Promise(resolve => {
            setTimeout(() => {
              resolve({
                ok: true,
                json: () => Promise.resolve({ items: [] })
              });
            }, 1000);
          });
        });
      }
    });
    
    // Verify loading indicators are displayed
    cy.get("[data-testid='loading-indicator']").should('be.visible');
    
    // After loading completes, verify indicators are gone
    cy.get("[data-testid='loading-indicator']", { timeout: 5000 }).should('not.exist');
  });

  it('should handle API errors gracefully', () => {
    // Mock API error responses
    cy.intercept('GET', '/api/alerts', {
      statusCode: 500,
      body: { error: 'Internal Server Error' }
    }).as('getAlertsError');
    
    // Visit the alerts page
    cy.visit('/alerts');
    
    // Wait for the API call to complete
    cy.wait('@getAlertsError');
    
    // Verify error state is displayed
    cy.get("[data-testid='error-alert']").should('be.visible')
      .and('contain', 'Failed to load alerts');
    
    // Verify retry button is available
    cy.get("[data-testid='retry-button']").should('be.visible');
    
    // Test retry functionality
    cy.intercept('GET', '/api/alerts', {
      statusCode: 200,
      body: { items: [] }
    }).as('getAlertsRetry');
    
    cy.get("[data-testid='retry-button']").click();
    cy.wait('@getAlertsRetry');
    
    // Verify error is no longer displayed
    cy.get("[data-testid='error-alert']").should('not.exist');
  });
});