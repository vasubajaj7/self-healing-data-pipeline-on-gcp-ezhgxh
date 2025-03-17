/// <reference types="cypress" />

describe('Dashboard Page', () => {
  // Selectors for dashboard components
  const dashboardTitle = "h1:contains('Dashboard')";
  const pipelineHealthCard = "[data-testid='pipeline-health-card']";
  const dataQualityCard = "[data-testid='data-quality-card']";
  const selfHealingStatusCard = "[data-testid='self-healing-status-card']";
  const alertSummaryCard = "[data-testid='alert-summary-card']";
  const recentExecutionsTable = "[data-testid='recent-executions-table']";
  const systemStatusCard = "[data-testid='system-status-card']";
  const aiInsightsCard = "[data-testid='ai-insights-card']";
  const quickStatsCard = "[data-testid='quick-stats-card']";
  const timeRangeSelector = "[data-testid='time-range-selector']";
  const refreshButton = "[data-testid='refresh-button']";
  const lastRefreshed = "[data-testid='last-refreshed']";
  const viewAllAlerts = "[data-testid='view-all-alerts']";
  const viewAllExecutions = "[data-testid='view-all-executions']";
  const loadingIndicator = '.MuiCircularProgress-root';
  const errorAlert = "[data-testid='error-alert']";

  beforeEach(() => {
    // Login as a user with dashboard access
    cy.loginAsEngineer();
    
    // Mock API responses for dashboard data
    cy.fixture('pipeline_data.json').then((pipelineData) => {
      const { pipelineListResponse, pipelineExecutionListResponse } = pipelineData;
      
      cy.intercept('GET', '/api/pipelines', {
        statusCode: 200,
        body: { items: pipelineListResponse.items }
      }).as('getPipelines');
      
      cy.intercept('GET', '/api/pipelines/executions', {
        statusCode: 200,
        body: { items: pipelineExecutionListResponse.items }
      }).as('getPipelineExecutions');
    });
    
    cy.fixture('quality_data.json').then((qualityData) => {
      const { datasetQualitySummaries, qualityStatistics } = qualityData;
      
      cy.intercept('GET', '/api/quality/datasets', {
        statusCode: 200,
        body: { items: datasetQualitySummaries }
      }).as('getQualityDatasets');
      
      cy.intercept('GET', '/api/quality/statistics', {
        statusCode: 200,
        body: qualityStatistics
      }).as('getQualityStatistics');
    });
    
    cy.fixture('healing_data.json').then((healingData) => {
      const { healingStatistics, healingIssues } = healingData;
      
      cy.intercept('GET', '/api/healing/statistics', {
        statusCode: 200,
        body: healingStatistics
      }).as('getHealingStatistics');
      
      cy.intercept('GET', '/api/healing/issues', {
        statusCode: 200,
        body: { items: healingIssues }
      }).as('getHealingIssues');
    });
    
    cy.fixture('system_status.json').then((systemStatus) => {
      cy.intercept('GET', '/api/system/status', {
        statusCode: 200,
        body: systemStatus
      }).as('getSystemStatus');
    });
    
    cy.fixture('alerts.json').then((alerts) => {
      cy.intercept('GET', '/api/alerts', {
        statusCode: 200,
        body: { items: alerts.items }
      }).as('getAlerts');
    });
    
    cy.fixture('ai_insights.json').then((insights) => {
      cy.intercept('GET', '/api/insights', {
        statusCode: 200,
        body: { items: insights.items }
      }).as('getInsights');
    });
    
    cy.intercept('GET', '/api/dashboard/metrics', {
      statusCode: 200,
      body: {
        activePipelines: 12,
        pendingJobs: 3,
        alertRateTrend: -15
      }
    }).as('getDashboardMetrics');
    
    // Visit the dashboard page
    cy.visit('/dashboard');
    
    // Wait for initial data to load
    cy.wait([
      '@getPipelines',
      '@getPipelineExecutions',
      '@getQualityDatasets',
      '@getQualityStatistics',
      '@getHealingStatistics',
      '@getHealingIssues',
      '@getSystemStatus',
      '@getAlerts',
      '@getInsights',
      '@getDashboardMetrics'
    ]);
  });

  it('should display all dashboard components correctly', () => {
    cy.get(dashboardTitle).should('be.visible');
    cy.get(pipelineHealthCard).should('be.visible');
    cy.get(dataQualityCard).should('be.visible');
    cy.get(selfHealingStatusCard).should('be.visible');
    cy.get(alertSummaryCard).should('be.visible');
    cy.get(recentExecutionsTable).should('be.visible');
    cy.get(systemStatusCard).should('be.visible');
    cy.get(aiInsightsCard).should('be.visible');
    cy.get(quickStatsCard).should('be.visible');
  });

  it('should display correct pipeline health metrics', () => {
    cy.get(pipelineHealthCard).within(() => {
      cy.get('[data-testid="health-percentage"]').should('contain', '98%');
      cy.get('[data-testid="health-progress"]').should('have.attr', 'aria-valuenow', '98');
      cy.get('[data-testid="health-status"]').should('have.class', 'status-healthy');
    });
  });

  it('should display correct data quality metrics', () => {
    cy.get(dataQualityCard).within(() => {
      cy.get('[data-testid="quality-percentage"]').should('contain', '94%');
      cy.get('[data-testid="quality-progress"]').should('have.attr', 'aria-valuenow', '94');
      cy.get('[data-testid="quality-status"]').should('have.class', 'status-good');
    });
  });

  it('should display correct self-healing metrics', () => {
    cy.get(selfHealingStatusCard).within(() => {
      cy.get('[data-testid="healing-percentage"]').should('contain', '87%');
      cy.get('[data-testid="healing-progress"]').should('have.attr', 'aria-valuenow', '87');
      cy.get('[data-testid="healing-status"]').should('have.class', 'status-good');
    });
  });

  it('should display active alerts correctly', () => {
    cy.get(alertSummaryCard).within(() => {
      cy.get('[data-testid="alert-item"]').should('have.length.at.least', 1);
      cy.get('[data-testid="alert-severity-high"]').should('be.visible');
      cy.get('[data-testid="alert-description"]').first().should('contain', 'BQ Load Failed');
      cy.get('[data-testid="alert-time"]').first().should('contain', 'min ago');
    });
  });

  it('should display recent executions correctly', () => {
    cy.get(recentExecutionsTable).within(() => {
      cy.get('[data-testid="execution-row"]').should('have.length.at.least', 3);
      cy.get('[data-testid="execution-name"]').first().should('contain', 'analytics_daily');
      cy.get('[data-testid="execution-status-completed"]').should('be.visible');
      cy.get('[data-testid="execution-time"]').first().should('contain', 'AM');
    });
  });

  it('should display system status correctly', () => {
    cy.get(systemStatusCard).within(() => {
      cy.get('[data-testid="component-status-row"]').should('have.length.at.least', 4);
      cy.get('[data-testid="component-name"]').first().should('contain', 'GCS Connector');
      cy.get('[data-testid="status-ok"]').should('be.visible');
      cy.get('[data-testid="status-warn"]').should('be.visible');
    });
  });

  it('should display AI insights correctly', () => {
    cy.get(aiInsightsCard).within(() => {
      cy.get('[data-testid="insight-item"]').should('have.length.at.least', 2);
      cy.get('[data-testid="insight-icon"]').should('be.visible');
      cy.get('[data-testid="insight-description"]').first().should('contain', 'Predicted slowdown');
    });
  });

  it('should display quick stats correctly', () => {
    cy.get(quickStatsCard).within(() => {
      cy.get('[data-testid="active-pipelines"]').should('contain', '12');
      cy.get('[data-testid="pending-jobs"]').should('contain', '3');
      cy.get('[data-testid="alert-rate"]').should('contain', '-15%');
    });
  });

  it('should update data when time range filter is changed', () => {
    // Intercept API calls that will be made when filter changes
    cy.intercept('GET', '/api/dashboard/metrics*').as('getDashboardMetrics');
    cy.intercept('GET', '/api/pipelines/executions*').as('getPipelineExecutions');
    
    // Change time range filter
    cy.get(timeRangeSelector).click();
    cy.get('.MuiMenuItem-root').contains('Last 7 Days').click();
    
    // Verify API calls were made with correct parameters
    cy.wait('@getDashboardMetrics').its('request.url').should('include', 'timeRange=7d');
    cy.wait('@getPipelineExecutions').its('request.url').should('include', 'timeRange=7d');
    
    // Verify last refreshed timestamp updated
    cy.get(lastRefreshed).should('contain', 'Last refreshed');
  });

  it('should refresh data when refresh button is clicked', () => {
    // Intercept API calls that will be made when refresh button is clicked
    cy.intercept('GET', '/api/dashboard/metrics*').as('getDashboardMetrics');
    cy.intercept('GET', '/api/pipelines/executions*').as('getPipelineExecutions');
    cy.intercept('GET', '/api/quality/datasets*').as('getQualityData');
    cy.intercept('GET', '/api/healing/statistics*').as('getHealingStats');
    
    // Click refresh button
    cy.get(refreshButton).click();
    
    // Verify loading indicator is displayed
    cy.get(loadingIndicator).should('be.visible');
    
    // Wait for API calls to complete
    cy.wait(['@getDashboardMetrics', '@getPipelineExecutions', '@getQualityData', '@getHealingStats']);
    
    // Verify loading indicator is gone
    cy.get(loadingIndicator).should('not.exist');
    
    // Verify last refreshed timestamp updated
    cy.get(lastRefreshed).should('contain', 'Last refreshed');
  });

  it('should navigate to pipeline details when clicking on a pipeline', () => {
    // Get the first pipeline name element
    cy.get(`${recentExecutionsTable} [data-testid="execution-name"]`).first().click();
    
    // Verify URL navigation
    cy.url().should('include', '/pipelines/');
    
    // Verify pipeline details page loaded
    cy.get('[data-testid="pipeline-details-title"]').should('be.visible');
  });

  it('should navigate to alert details when clicking on an alert', () => {
    // Get the first alert element
    cy.get(`${alertSummaryCard} [data-testid="alert-item"]`).first().click();
    
    // Verify URL navigation
    cy.url().should('include', '/alerts/');
    
    // Verify alert details page loaded
    cy.get('[data-testid="alert-details-title"]').should('be.visible');
  });

  it('should expand alert section when clicking view all button', () => {
    cy.get(viewAllAlerts).click();
    
    // Verify navigation to alerts page
    cy.url().should('include', '/alerts');
    
    // Verify alerts page loaded
    cy.get('[data-testid="alerts-page-title"]').should('be.visible');
  });

  it('should expand executions section when clicking view all button', () => {
    cy.get(viewAllExecutions).click();
    
    // Verify navigation to pipelines page
    cy.url().should('include', '/pipelines');
    
    // Verify pipelines page loaded
    cy.get('[data-testid="pipelines-page-title"]').should('be.visible');
  });

  it('should show loading state while fetching dashboard data', () => {
    // Set up interceptors with delayed response
    cy.intercept('GET', '/api/dashboard/metrics', (req) => {
      req.reply((res) => {
        res.delay = 1000; // Delay response by 1 second
        return res;
      });
    }).as('slowDashboardMetrics');
    
    // Reload page to see initial loading state
    cy.reload();
    
    // Verify loading indicators are displayed
    cy.get(loadingIndicator).should('be.visible');
    
    // Wait for data to load
    cy.wait('@slowDashboardMetrics');
    cy.get(pipelineHealthCard, { timeout: 10000 }).should('be.visible');
    
    // Verify loading indicators are gone
    cy.get(loadingIndicator).should('not.exist');
  });

  it('should handle API errors gracefully', () => {
    // Mock API errors
    cy.intercept('GET', '/api/dashboard/metrics', {
      statusCode: 500,
      body: { error: 'Internal Server Error' }
    }).as('getDashboardMetricsError');
    
    // Reload the page to trigger new API calls
    cy.reload();
    
    // Wait for error response
    cy.wait('@getDashboardMetricsError');
    
    // Verify error state is displayed
    cy.get(errorAlert).should('be.visible');
    
    // Verify error message is informative
    cy.get(errorAlert).should('contain', 'Error loading dashboard data');
    
    // Verify retry button is available
    cy.get('[data-testid="retry-button"]').should('be.visible');
    
    // Test retry functionality
    cy.intercept('GET', '/api/dashboard/metrics', {
      statusCode: 200,
      body: {
        activePipelines: 12,
        pendingJobs: 3,
        alertRateTrend: -15
      }
    }).as('retryDashboardMetrics');
    
    cy.get('[data-testid="retry-button"]').click();
    cy.wait('@retryDashboardMetrics');
    
    // Verify error is gone after successful retry
    cy.get(errorAlert).should('not.exist');
  });
});