// Import Cypress types for TypeScript support
/// <reference types="cypress" />

// Selectors for Data Quality page elements
const qualityDashboardTitle = "h1:contains('Data Quality')";
const qualityScoreChart = "[data-testid='quality-score-chart']";
const qualityDimensionsCard = "[data-testid='quality-dimensions-card']";
const datasetQualityTable = "[data-testid='dataset-quality-table']";
const qualityTrendChart = "[data-testid='quality-trend-chart']";
const tabList = "[role='tablist']";
const overviewTab = "[role='tab']:contains('Overview')";
const datasetDetailTab = "[role='tab']:contains('Dataset Detail')";
const tableDetailTab = "[role='tab']:contains('Table Detail')";
const rulesTab = "[role='tab']:contains('Rules')";
const issuesTab = "[role='tab']:contains('Issues')";
const validationRulesTable = "[data-testid='validation-rules-table']";
const validationIssuesTable = "[data-testid='validation-issues-table']";
const addRuleButton = "[data-testid='add-rule-button']";
const editRuleButton = "[data-testid='edit-rule-button']";
const deleteRuleButton = "[data-testid='delete-rule-button']";
const ruleEditorModal = "[data-testid='rule-editor-modal']";
const issueDetailModal = "[data-testid='issue-detail-modal']";
const refreshButton = "[data-testid='refresh-button']";
const severityFilter = "[data-testid='severity-filter']";
const dimensionFilter = "[data-testid='dimension-filter']";
const statusFilter = "[data-testid='status-filter']";
const loadingIndicator = '.MuiCircularProgress-root';
const errorAlert = "[data-testid='error-alert']";
const successNotification = '.MuiAlert-standardSuccess';
const confirmationDialog = "[data-testid='confirmation-dialog']";
const confirmButton = "[data-testid='confirm-button']";
const cancelButton = "[data-testid='cancel-button']";
const saveButton = "[data-testid='save-button']";

describe('Data Quality Page', () => {
  beforeEach(() => {
    // Login as a user with data quality access
    cy.loginAsEngineer();
    
    // Mock API responses for quality data
    cy.mockQualityData();
    
    // Visit the data quality page
    cy.visit('/data-quality');
  });

  it('should display all data quality dashboard components correctly', () => {
    cy.get(qualityDashboardTitle).should('be.visible');
    cy.get(qualityScoreChart).should('be.visible');
    cy.get(qualityDimensionsCard).should('be.visible');
    cy.get(datasetQualityTable).should('be.visible');
    cy.get(qualityTrendChart).should('be.visible');
    cy.get(tabList).should('be.visible');
    cy.get(overviewTab).should('be.visible');
    cy.get(rulesTab).should('be.visible');
    cy.get(issuesTab).should('be.visible');
  });

  it('should display correct quality metrics', () => {
    cy.wait('@getQualityStatistics');
    
    cy.get(qualityScoreChart).should('contain', '94%');
    
    cy.get(qualityDimensionsCard).within(() => {
      cy.contains('Completeness').parent().should('contain', '96%');
      cy.contains('Accuracy').parent().should('contain', '92%');
      cy.contains('Consistency').parent().should('contain', '94%');
      cy.contains('Timeliness').parent().should('contain', '99%');
    });
    
    cy.get(qualityTrendChart).should('be.visible');
    cy.get(qualityTrendChart).find('path.recharts-curve').should('exist');
  });

  it('should display dataset quality table with correct data', () => {
    cy.wait('@getDatasetQuality');
    
    cy.get(datasetQualityTable).should('be.visible');
    
    // Verify table headers
    cy.get(datasetQualityTable).find('th').should('have.length.at.least', 5);
    cy.get(datasetQualityTable).find('th').contains('Dataset').should('be.visible');
    cy.get(datasetQualityTable).find('th').contains('Quality').should('be.visible');
    cy.get(datasetQualityTable).find('th').contains('Trend').should('be.visible');
    cy.get(datasetQualityTable).find('th').contains('Issues').should('be.visible');
    
    // Verify first row data
    cy.get(datasetQualityTable).find('tbody tr').first().within(() => {
      cy.get('td').eq(0).should('contain', 'customer_data');
      cy.get('td').eq(1).should('contain', '98%');
      cy.get('td').eq(2).find('svg').should('be.visible'); // Trend indicator
      cy.get('td').eq(3).should('contain', '2'); // Issue count
    });
    
    // Verify number of rows matches mock data
    cy.fixture('quality_data.json').then((qualityData) => {
      const { datasetQualitySummaries } = qualityData;
      cy.get(datasetQualityTable).find('tbody tr').should('have.length', datasetQualitySummaries.length);
    });
  });

  it('should navigate to dataset detail view when clicking on a dataset', () => {
    // Intercept API calls that will be made when selecting a dataset
    cy.intercept('GET', '/api/quality/datasets/*').as('getDatasetDetail');
    
    // Find and click on a dataset in the table
    cy.get(datasetQualityTable).find('tbody tr').first().click();
    
    // Verify dataset detail tab is active
    cy.get(datasetDetailTab).should('have.attr', 'aria-selected', 'true');
    
    // Verify API call was made with correct dataset ID
    cy.wait('@getDatasetDetail');
    
    // Verify URL was updated
    cy.url().should('include', 'dataset=');
    
    // Verify dataset name is displayed in the detail view
    cy.get(`[aria-labelledby="${datasetDetailTab.replace(/^\[|\]$/g, '')}"]`).find('h2').should('contain', 'Dataset:');
    
    // Verify dataset-specific quality metrics are displayed
    cy.get(`[aria-labelledby="${datasetDetailTab.replace(/^\[|\]$/g, '')}"]`).find(qualityScoreChart).should('be.visible');
    
    // Verify tables within the dataset are listed
    cy.get(`[aria-labelledby="${datasetDetailTab.replace(/^\[|\]$/g, '')}"]`).find('table').should('be.visible');
    cy.get(`[aria-labelledby="${datasetDetailTab.replace(/^\[|\]$/g, '')}"]`).find('table tbody tr').should('have.length.at.least', 1);
  });

  it('should navigate to table detail view when clicking on a table', () => {
    // Intercept API calls that will be made when selecting a dataset and table
    cy.intercept('GET', '/api/quality/datasets/*').as('getDatasetDetail');
    cy.intercept('GET', '/api/quality/tables/*').as('getTableDetail');
    
    // Navigate to dataset detail view first
    cy.get(datasetQualityTable).find('tbody tr').first().click();
    cy.wait('@getDatasetDetail');
    
    // Find and click on a table within the dataset
    cy.get(`[aria-labelledby="${datasetDetailTab.replace(/^\[|\]$/g, '')}"]`).find('table tbody tr').first().click();
    
    // Verify table detail tab is active
    cy.get(tableDetailTab).should('have.attr', 'aria-selected', 'true');
    
    // Verify API call was made with correct table ID
    cy.wait('@getTableDetail');
    
    // Verify URL was updated
    cy.url().should('include', 'table=');
    
    // Verify table name is displayed in the detail view
    cy.get(`[aria-labelledby="${tableDetailTab.replace(/^\[|\]$/g, '')}"]`).find('h2').should('contain', 'Table:');
    
    // Verify table-specific quality metrics are displayed
    cy.get(`[aria-labelledby="${tableDetailTab.replace(/^\[|\]$/g, '')}"]`).find(qualityScoreChart).should('be.visible');
    
    // Verify quality issues for the table are listed
    cy.get(`[aria-labelledby="${tableDetailTab.replace(/^\[|\]$/g, '')}"]`).find('h3').contains('Quality Issues').should('be.visible');
    cy.get(`[aria-labelledby="${tableDetailTab.replace(/^\[|\]$/g, '')}"]`).find('table').should('be.visible');
  });

  it('should display validation rules correctly', () => {
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Verify rules tab is active
    cy.get(rulesTab).should('have.attr', 'aria-selected', 'true');
    
    // Verify validation rules table is displayed
    cy.get(validationRulesTable).should('be.visible');
    
    // Verify table headers
    cy.get(validationRulesTable).find('th').should('have.length.at.least', 5);
    cy.get(validationRulesTable).find('th').contains('Rule Name').should('be.visible');
    cy.get(validationRulesTable).find('th').contains('Type').should('be.visible');
    cy.get(validationRulesTable).find('th').contains('Dimension').should('be.visible');
    cy.get(validationRulesTable).find('th').contains('Severity').should('be.visible');
    cy.get(validationRulesTable).find('th').contains('Status').should('be.visible');
    
    // Verify first row data
    cy.get(validationRulesTable).find('tbody tr').first().within(() => {
      cy.get('td').eq(0).should('be.visible'); // Rule name
      cy.get('td').eq(1).should('be.visible'); // Type
      cy.get('td').eq(2).should('be.visible'); // Dimension
      cy.get('td').eq(3).find('span').should('be.visible'); // Severity indicator
      cy.get('td').eq(4).should('be.visible'); // Status
    });
    
    // Verify number of rows matches mock data
    cy.fixture('quality_data.json').then((qualityData) => {
      const { qualityRules } = qualityData;
      cy.get(validationRulesTable).find('tbody tr').should('have.length', qualityRules.length);
    });
  });

  it('should display quality issues correctly', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Verify issues tab is active
    cy.get(issuesTab).should('have.attr', 'aria-selected', 'true');
    
    // Verify quality issues table is displayed
    cy.get(validationIssuesTable).should('be.visible');
    
    // Verify table headers
    cy.get(validationIssuesTable).find('th').should('have.length.at.least', 6);
    cy.get(validationIssuesTable).find('th').contains('Issue').should('be.visible');
    cy.get(validationIssuesTable).find('th').contains('Severity').should('be.visible');
    cy.get(validationIssuesTable).find('th').contains('Status').should('be.visible');
    cy.get(validationIssuesTable).find('th').contains('Affected').should('be.visible');
    cy.get(validationIssuesTable).find('th').contains('Detected').should('be.visible');
    cy.get(validationIssuesTable).find('th').contains('Self-Healing').should('be.visible');
    
    // Verify first row data
    cy.get(validationIssuesTable).find('tbody tr').first().within(() => {
      cy.get('td').eq(0).should('be.visible'); // Issue description
      cy.get('td').eq(1).find('span').should('be.visible'); // Severity indicator
      cy.get('td').eq(2).should('be.visible'); // Status
      cy.get('td').eq(3).should('be.visible'); // Affected dataset/table
      cy.get('td').eq(4).should('be.visible'); // Detection timestamp
      cy.get('td').eq(5).should('be.visible'); // Self-healing status
    });
    
    // Verify number of rows matches mock data
    cy.fixture('quality_data.json').then((qualityData) => {
      const { qualityIssues } = qualityData;
      cy.get(validationIssuesTable).find('tbody tr').should('have.length', qualityIssues.length);
    });
  });

  it('should filter quality issues by severity', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Intercept the filtered API call
    cy.intercept('GET', '/api/quality/issues*severity=high*').as('getHighSeverityIssues');
    
    // Open the severity filter dropdown
    cy.get(severityFilter).click();
    
    // Select High severity
    cy.get('.MuiMenu-list').contains('High').click();
    
    // Verify API call was made with correct parameters
    cy.wait('@getHighSeverityIssues');
    
    // Verify URL is updated with severity parameter
    cy.url().should('include', 'severity=high');
    
    // Verify all issues shown have High severity
    cy.get(validationIssuesTable).find('tbody tr').each(($row) => {
      cy.wrap($row).find('td').eq(1).find('span').should('have.class', 'high-severity');
    });
  });

  it('should filter quality issues by dimension', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Intercept the filtered API call
    cy.intercept('GET', '/api/quality/issues*dimension=completeness*').as('getCompletenessDimensionIssues');
    
    // Open the dimension filter dropdown
    cy.get(dimensionFilter).click();
    
    // Select Completeness dimension
    cy.get('.MuiMenu-list').contains('Completeness').click();
    
    // Verify API call was made with correct parameters
    cy.wait('@getCompletenessDimensionIssues');
    
    // Verify URL is updated with dimension parameter
    cy.url().should('include', 'dimension=completeness');
    
    // Verify issues table is updated
    cy.get(validationIssuesTable).should('be.visible');
    cy.get(validationIssuesTable).find('tbody tr').should('have.length.at.least', 1);
  });

  it('should filter quality issues by status', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Intercept the filtered API call
    cy.intercept('GET', '/api/quality/issues*status=open*').as('getOpenStatusIssues');
    
    // Open the status filter dropdown
    cy.get(statusFilter).click();
    
    // Select Open status
    cy.get('.MuiMenu-list').contains('Open').click();
    
    // Verify API call was made with correct parameters
    cy.wait('@getOpenStatusIssues');
    
    // Verify URL is updated with status parameter
    cy.url().should('include', 'status=open');
    
    // Verify all issues shown have Open status
    cy.get(validationIssuesTable).find('tbody tr').each(($row) => {
      cy.wrap($row).find('td').eq(2).should('contain', 'Open');
    });
  });

  it('should open rule editor when clicking add rule button', () => {
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Verify Add Rule button is displayed
    cy.get(addRuleButton).should('be.visible');
    
    // Click on the Add Rule button
    cy.get(addRuleButton).click();
    
    // Verify rule editor modal is displayed
    cy.get(ruleEditorModal).should('be.visible');
    
    // Verify rule editor form is displayed with empty fields
    cy.get(ruleEditorModal).find('input[name="name"]').should('be.visible').and('have.value', '');
    
    // Verify rule type dropdown contains available options
    cy.get(ruleEditorModal).find('label').contains('Rule Type').should('be.visible');
    cy.get(ruleEditorModal).find('select[name="type"]').click();
    cy.get('.MuiMenu-list').find('li').should('have.length.at.least', 3);
    
    // Verify dimension dropdown contains available options
    cy.get(ruleEditorModal).find('label').contains('Dimension').should('be.visible');
    cy.get(ruleEditorModal).find('select[name="dimension"]').click();
    cy.get('.MuiMenu-list').find('li').should('have.length.at.least', 4);
    
    // Verify severity dropdown contains available options
    cy.get(ruleEditorModal).find('label').contains('Severity').should('be.visible');
    cy.get(ruleEditorModal).find('select[name="severity"]').click();
    cy.get('.MuiMenu-list').find('li').should('have.length.at.least', 3);
  });

  it('should open rule editor with rule data when clicking edit rule button', () => {
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Verify Edit button is displayed for the first rule
    cy.get(validationRulesTable).find('tbody tr').first().find(editRuleButton).should('be.visible');
    
    // Click on the Edit button for the first rule
    cy.get(validationRulesTable).find('tbody tr').first().find(editRuleButton).click();
    
    // Verify rule editor modal is displayed
    cy.get(ruleEditorModal).should('be.visible');
    
    // Verify rule editor form is displayed with rule data pre-filled
    cy.fixture('quality_data.json').then((qualityData) => {
      const { qualityRules } = qualityData;
      const firstRule = qualityRules[0];
      
      cy.get(ruleEditorModal).find('input[name="name"]').should('have.value', firstRule.name);
      cy.get(ruleEditorModal).find('select[name="type"]').should('have.value', firstRule.type);
      cy.get(ruleEditorModal).find('select[name="dimension"]').should('have.value', firstRule.dimension);
      cy.get(ruleEditorModal).find('select[name="severity"]').should('have.value', firstRule.severity);
      
      if (firstRule.dataset) {
        cy.get(ruleEditorModal).find('input[name="dataset"]').should('have.value', firstRule.dataset);
      }
      
      if (firstRule.table) {
        cy.get(ruleEditorModal).find('input[name="table"]').should('have.value', firstRule.table);
      }
    });
  });

  it('should create a new rule successfully', () => {
    // Mock rule creation API response
    cy.intercept('POST', '/api/quality/rules', {
      statusCode: 201,
      body: {
        id: 'new-rule-123',
        name: 'New Test Rule',
        type: 'not_null',
        dimension: 'completeness',
        severity: 'high',
        dataset: 'test_dataset',
        table: 'test_table',
        column: 'test_column',
        status: 'active'
      }
    }).as('createRule');
    
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Click on the Add Rule button
    cy.get(addRuleButton).click();
    
    // Fill in rule form fields
    cy.get(ruleEditorModal).find('input[name="name"]').type('New Test Rule');
    cy.get(ruleEditorModal).find('select[name="type"]').select('not_null');
    cy.get(ruleEditorModal).find('select[name="dimension"]').select('completeness');
    cy.get(ruleEditorModal).find('select[name="severity"]').select('high');
    cy.get(ruleEditorModal).find('input[name="dataset"]').type('test_dataset');
    cy.get(ruleEditorModal).find('input[name="table"]').type('test_table');
    cy.get(ruleEditorModal).find('input[name="column"]').type('test_column');
    
    // Click Save button
    cy.get(saveButton).click();
    
    // Verify API call with rule data
    cy.wait('@createRule').its('request.body').should('deep.include', {
      name: 'New Test Rule',
      type: 'not_null',
      dimension: 'completeness',
      severity: 'high',
      dataset: 'test_dataset',
      table: 'test_table',
      column: 'test_column'
    });
    
    // Verify success notification
    cy.get(successNotification).should('be.visible').and('contain', 'Rule created successfully');
    
    // Verify rule editor modal is closed
    cy.get(ruleEditorModal).should('not.exist');
    
    // Verify rules table is updated (intercept the refresh call)
    cy.intercept('GET', '/api/quality/rules', (req) => {
      req.reply((res) => {
        const body = { ...res.body };
        if (body.items && Array.isArray(body.items)) {
          body.items.unshift({
            id: 'new-rule-123',
            name: 'New Test Rule',
            type: 'not_null',
            dimension: 'completeness',
            severity: 'high',
            dataset: 'test_dataset',
            table: 'test_table',
            column: 'test_column',
            status: 'active'
          });
        }
        res.send(body);
      });
    }).as('refreshRules');
    
    // The table should refresh automatically after successful creation
    cy.wait('@refreshRules');
    
    // Verify new rule appears in the table
    cy.get(validationRulesTable).find('tbody tr').first().should('contain', 'New Test Rule');
  });

  it('should update an existing rule successfully', () => {
    // Mock rule update API response
    cy.intercept('PUT', '/api/quality/rules/*', {
      statusCode: 200,
      body: {
        id: 'rule-123',
        name: 'Updated Rule Name',
        type: 'not_null',
        dimension: 'completeness',
        severity: 'medium',
        dataset: 'test_dataset',
        table: 'test_table',
        column: 'test_column',
        status: 'active'
      }
    }).as('updateRule');
    
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Click on the Edit button for the first rule
    cy.get(validationRulesTable).find('tbody tr').first().find(editRuleButton).click();
    
    // Modify rule form fields
    cy.get(ruleEditorModal).find('input[name="name"]').clear().type('Updated Rule Name');
    cy.get(ruleEditorModal).find('select[name="severity"]').select('medium');
    
    // Click Save button
    cy.get(saveButton).click();
    
    // Verify API call with updated rule data
    cy.wait('@updateRule').its('request.body').should('deep.include', {
      name: 'Updated Rule Name',
      severity: 'medium'
    });
    
    // Verify success notification
    cy.get(successNotification).should('be.visible').and('contain', 'Rule updated successfully');
    
    // Verify rule editor modal is closed
    cy.get(ruleEditorModal).should('not.exist');
    
    // Verify rules table is updated (intercept the refresh call)
    cy.intercept('GET', '/api/quality/rules', (req) => {
      req.reply((res) => {
        const body = { ...res.body };
        if (body.items && Array.isArray(body.items) && body.items.length > 0) {
          body.items[0].name = 'Updated Rule Name';
          body.items[0].severity = 'medium';
        }
        res.send(body);
      });
    }).as('refreshRules');
    
    // The table should refresh automatically after successful update
    cy.wait('@refreshRules');
    
    // Verify modified rule appears in the table
    cy.get(validationRulesTable).find('tbody tr').first().should('contain', 'Updated Rule Name');
  });

  it('should delete a rule successfully', () => {
    // Mock rule deletion API response
    cy.intercept('DELETE', '/api/quality/rules/*', {
      statusCode: 200,
      body: { success: true }
    }).as('deleteRule');
    
    // Click on the Rules tab
    cy.get(rulesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityRules');
    
    // Store the number of rules before deletion
    let initialRuleCount;
    cy.get(validationRulesTable).find('tbody tr').then(rows => {
      initialRuleCount = rows.length;
    });
    
    // Store the name of the first rule (for verification)
    let firstRuleName;
    cy.get(validationRulesTable).find('tbody tr').first().find('td').eq(0).invoke('text').then(text => {
      firstRuleName = text;
    });
    
    // Click on the Delete button for the first rule
    cy.get(validationRulesTable).find('tbody tr').first().find(deleteRuleButton).click();
    
    // Confirm deletion in the confirmation dialog
    cy.get(confirmationDialog).should('be.visible');
    cy.get(confirmationDialog).find(confirmButton).click();
    
    // Verify API call for rule deletion
    cy.wait('@deleteRule');
    
    // Verify success notification
    cy.get(successNotification).should('be.visible').and('contain', 'Rule deleted successfully');
    
    // Verify rule is removed from the table (intercept the refresh call)
    cy.intercept('GET', '/api/quality/rules', (req) => {
      req.reply((res) => {
        const body = { ...res.body };
        if (body.items && Array.isArray(body.items) && body.items.length > 0) {
          // Remove the first rule
          body.items.shift();
        }
        res.send(body);
      });
    }).as('refreshRules');
    
    // The table should refresh automatically after successful deletion
    cy.wait('@refreshRules');
    
    // Verify the number of rules has decreased by 1
    cy.get(validationRulesTable).find('tbody tr').should('have.length', initialRuleCount - 1);
    
    // Verify the deleted rule is no longer in the table
    cy.get(validationRulesTable).find('tbody tr').first().find('td').eq(0).invoke('text').should('not.equal', firstRuleName);
  });

  it('should display issue details when clicking on an issue', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Intercept the issue details API call
    cy.intercept('GET', '/api/quality/issues/*').as('getIssueDetails');
    
    // Click on an issue in the issues table
    cy.get(validationIssuesTable).find('tbody tr').first().click();
    
    // Verify issue details API call
    cy.wait('@getIssueDetails');
    
    // Verify issue details modal is displayed
    cy.get(issueDetailModal).should('be.visible');
    
    // Verify issue details modal contains expected information
    cy.get(issueDetailModal).within(() => {
      // Issue description
      cy.get('h2').should('be.visible');
      
      // Affected dataset and table
      cy.contains('Affected Dataset:').should('be.visible');
      cy.contains('Affected Table:').should('be.visible');
      
      // Detection timestamp
      cy.contains('Detected:').should('be.visible');
      
      // Severity level
      cy.contains('Severity:').should('be.visible').next().find('span').should('be.visible');
      
      // Affected rows count
      cy.contains('Affected Rows:').should('be.visible');
      
      // Self-healing status
      cy.contains('Self-Healing:').should('be.visible');
    });
  });

  it('should update issue status successfully', () => {
    // Mock issue update API response
    cy.intercept('PUT', '/api/quality/issues/*', {
      statusCode: 200,
      body: { success: true }
    }).as('updateIssue');
    
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Intercept the issue details API call
    cy.intercept('GET', '/api/quality/issues/*').as('getIssueDetails');
    
    // Click on an issue in the issues table
    cy.get(validationIssuesTable).find('tbody tr').first().click();
    
    // Verify issue details API call
    cy.wait('@getIssueDetails');
    
    // Verify issue details modal is displayed
    cy.get(issueDetailModal).should('be.visible');
    
    // Change issue status in the details modal
    cy.get(issueDetailModal).find('select[name="status"]').select('Resolved');
    
    // Save the status change
    cy.get(issueDetailModal).find(saveButton).click();
    
    // Verify API call with updated status
    cy.wait('@updateIssue').its('request.body').should('deep.include', {
      status: 'Resolved'
    });
    
    // Verify success notification
    cy.get(successNotification).should('be.visible').and('contain', 'Issue updated successfully');
    
    // Verify issue details modal is closed
    cy.get(issueDetailModal).should('not.exist');
    
    // Verify issues table is updated (intercept the refresh call)
    cy.intercept('GET', '/api/quality/issues', (req) => {
      req.reply((res) => {
        const body = { ...res.body };
        if (body.items && Array.isArray(body.items) && body.items.length > 0) {
          body.items[0].status = 'Resolved';
        }
        res.send(body);
      });
    }).as('refreshIssues');
    
    // The table should refresh automatically after successful update
    cy.wait('@refreshIssues');
    
    // Verify issue status is updated in the table
    cy.get(validationIssuesTable).find('tbody tr').first().find('td').eq(2).should('contain', 'Resolved');
  });

  it('should refresh data when refresh button is clicked', () => {
    // Intercept API calls that will be refreshed
    cy.intercept('GET', '/api/quality/datasets').as('refreshDatasets');
    cy.intercept('GET', '/api/quality/statistics').as('refreshStatistics');
    cy.intercept('GET', '/api/quality/timeseries').as('refreshTimeSeries');
    
    // Click the refresh button
    cy.get(refreshButton).click();
    
    // Verify loading indicators are displayed during refresh
    cy.get(loadingIndicator).should('be.visible');
    
    // Verify API calls are made to refresh data
    cy.wait(['@refreshDatasets', '@refreshStatistics', '@refreshTimeSeries']);
    
    // Verify loading indicators are no longer displayed
    cy.get(loadingIndicator).should('not.exist');
    
    // Verify data is displayed after refresh
    cy.get(qualityScoreChart).should('be.visible');
    cy.get(qualityDimensionsCard).should('be.visible');
    cy.get(datasetQualityTable).should('be.visible');
    cy.get(qualityTrendChart).should('be.visible');
  });

  it('should handle API errors gracefully', () => {
    // Mock API error responses
    cy.intercept('GET', '/api/quality/statistics', {
      statusCode: 500,
      body: { error: 'Internal Server Error' }
    }).as('statisticsError');
    
    // Refresh the page to trigger the API calls with errors
    cy.reload();
    
    // Verify error states are displayed
    cy.wait('@statisticsError');
    
    // Verify error alert is displayed
    cy.get(errorAlert).should('be.visible');
    cy.get(errorAlert).should('contain', 'Error loading quality statistics');
    
    // Verify retry option is provided
    cy.get(errorAlert).find('button').contains('Retry').should('be.visible');
    
    // Test retry functionality
    cy.intercept('GET', '/api/quality/statistics', {
      statusCode: 200,
      body: {
        overallScore: 94,
        dimensions: {
          completeness: 96,
          accuracy: 92,
          consistency: 94,
          timeliness: 99
        }
      }
    }).as('statisticsRetry');
    
    // Click retry button
    cy.get(errorAlert).find('button').contains('Retry').click();
    
    // Verify retry API call is made
    cy.wait('@statisticsRetry');
    
    // Verify error alert is no longer displayed
    cy.get(errorAlert).should('not.exist');
    
    // Verify data is loaded
    cy.get(qualityScoreChart).should('be.visible');
  });

  it('should persist filters in URL and restore state on page reload', () => {
    // Click on the Issues tab
    cy.get(issuesTab).click();
    
    // Wait for the API call to complete
    cy.wait('@getQualityIssues');
    
    // Apply severity filter
    cy.intercept('GET', '/api/quality/issues*severity=high*').as('highSeverityFilter');
    cy.get(severityFilter).click();
    cy.get('.MuiMenu-list').contains('High').click();
    cy.wait('@highSeverityFilter');
    
    // Apply dimension filter
    cy.intercept('GET', '/api/quality/issues*dimension=completeness*').as('completenessDimensionFilter');
    cy.get(dimensionFilter).click();
    cy.get('.MuiMenu-list').contains('Completeness').click();
    cy.wait('@completenessDimensionFilter');
    
    // Apply status filter
    cy.intercept('GET', '/api/quality/issues*status=open*').as('openStatusFilter');
    cy.get(statusFilter).click();
    cy.get('.MuiMenu-list').contains('Open').click();
    cy.wait('@openStatusFilter');
    
    // Verify URL contains all filter parameters
    cy.url().should('include', 'tab=issues');
    cy.url().should('include', 'severity=high');
    cy.url().should('include', 'dimension=completeness');
    cy.url().should('include', 'status=open');
    
    // Mock API responses for reload
    cy.intercept('GET', '/api/quality/issues*severity=high*dimension=completeness*status=open*', {
      statusCode: 200,
      body: {
        items: [
          {
            id: 'issue-1',
            description: 'Filtered Issue',
            severity: 'high',
            status: 'open',
            dimension: 'completeness',
            dataset: 'test_dataset',
            table: 'test_table',
            detectedAt: '2023-06-15T10:30:00Z',
            selfHealing: 'in_progress'
          }
        ]
      }
    }).as('filteredIssues');
    
    // Reload the page
    cy.reload();
    
    // Verify filters are restored from URL
    cy.wait('@filteredIssues');
    
    // Verify issues tab is active
    cy.get(issuesTab).should('have.attr', 'aria-selected', 'true');
    
    // Verify filter values are restored
    cy.get(severityFilter).should('contain', 'High');
    cy.get(dimensionFilter).should('contain', 'Completeness');
    cy.get(statusFilter).should('contain', 'Open');
    
    // Verify filtered data is displayed
    cy.get(validationIssuesTable).should('be.visible');
    cy.get(validationIssuesTable).find('tbody tr').should('have.length', 1);
    cy.get(validationIssuesTable).find('tbody tr').first().should('contain', 'Filtered Issue');
  });
});