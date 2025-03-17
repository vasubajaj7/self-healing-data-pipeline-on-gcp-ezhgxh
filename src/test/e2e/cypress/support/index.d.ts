/// <reference types="cypress" />

/**
 * This file contains type definitions for custom Cypress commands
 * used in end-to-end testing of the self-healing data pipeline application.
 * 
 * These type definitions extend the Cypress namespace to provide proper
 * TypeScript support and IntelliSense in test files.
 */

declare namespace Cypress {
  interface Chainable<Subject = any> {
    /**
     * Custom command to log in to the application with specified credentials
     * @example cy.login('user@example.com', 'password123')
     */
    login(email: string, password: string): Chainable<Element>;

    /**
     * Custom command to log in as an admin user using predefined credentials
     * @example cy.loginAsAdmin()
     */
    loginAsAdmin(): Chainable<Element>;

    /**
     * Custom command to log in as a data engineer using predefined credentials
     * @example cy.loginAsEngineer()
     */
    loginAsEngineer(): Chainable<Element>;

    /**
     * Custom command to log in as a data analyst using predefined credentials
     * @example cy.loginAsAnalyst()
     */
    loginAsAnalyst(): Chainable<Element>;

    /**
     * Custom command to log in as a pipeline operator using predefined credentials
     * @example cy.loginAsOperator()
     */
    loginAsOperator(): Chainable<Element>;

    /**
     * Custom command to log in with MFA verification
     * @example cy.loginWithMfa('user@example.com', 'password123', '123456')
     */
    loginWithMfa(email: string, password: string, mfaCode: string): Chainable<Element>;

    /**
     * Custom command to log out from the application
     * @example cy.logout()
     */
    logout(): Chainable<Element>;
    
    /**
     * Custom command to navigate to a specific section of the application
     * @example cy.navigateTo('Dashboard')
     */
    navigateTo(section: string): Chainable<Element>;
    
    /**
     * Custom command to mock pipeline data API responses
     * @example cy.mockPipelineData()
     */
    mockPipelineData(): Chainable<Element>;

    /**
     * Custom command to mock quality data API responses
     * @example cy.mockQualityData()
     */
    mockQualityData(): Chainable<Element>;

    /**
     * Custom command to mock self-healing data API responses
     * @example cy.mockHealingData()
     */
    mockHealingData(): Chainable<Element>;
    
    /**
     * Custom command to create a new pipeline through the UI
     * @example cy.createPipeline({ name: 'Test Pipeline', source: 'GCS', target: 'BigQuery' })
     */
    createPipeline(pipelineData: object): Chainable<Element>;

    /**
     * Custom command to create a new quality rule through the UI
     * @example cy.createQualityRule({ name: 'Not Null Check', type: 'schema', columns: ['id'] })
     */
    createQualityRule(ruleData: object): Chainable<Element>;

    /**
     * Custom command to update self-healing settings through the UI
     * @example cy.updateHealingSettings({ threshold: 85, autoApprove: true })
     */
    updateHealingSettings(settingsData: object): Chainable<Element>;

    /**
     * Custom command to approve a self-healing action through the UI
     * @example cy.approveHealingAction('action-123')
     */
    approveHealingAction(actionId: string): Chainable<Element>;

    /**
     * Custom command to reject a self-healing action through the UI
     * @example cy.rejectHealingAction('action-123', 'Requires manual verification')
     */
    rejectHealingAction(actionId: string, reason: string): Chainable<Element>;
    
    /**
     * Custom command to verify dashboard metrics are displayed correctly
     * @example cy.checkDashboardMetrics()
     */
    checkDashboardMetrics(): Chainable<Element>;

    /**
     * Custom command to verify table contents match expected data
     * @example cy.checkTableContents('table.pipeline-list', [{ name: 'Pipeline 1', status: 'Active' }])
     */
    checkTableContents(tableSelector: string, expectedData: any[]): Chainable<Element>;
    
    /**
     * Custom command to wait for a specific API response
     * @example cy.waitForApiResponse('/api/pipelines')
     */
    waitForApiResponse(apiRoute: string): Chainable<Element>;

    /**
     * Custom command to mock a specific API response
     * @example cy.mockApiResponse('GET', '/api/pipelines', { data: [] })
     */
    mockApiResponse(method: string, url: string, response: object): Chainable<Element>;
    
    /**
     * Custom command to check for a toast notification with specific text
     * @example cy.checkToastNotification('Pipeline created successfully')
     */
    checkToastNotification(text: string): Chainable<Element>;
  }
}