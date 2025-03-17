import './commands';

// Configuration Page test suite
describe('Configuration Page', () => {
  it('should navigate to configuration page', () => {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.get('h1').should('contain', 'Configuration');
    cy.get('[role="tab"]').should('have.length', 4);
    cy.get('[role="tab"]').eq(0).should('contain', 'Data Sources');
    cy.get('[role="tab"]').eq(1).should('contain', 'Pipelines');
    cy.get('[role="tab"]').eq(2).should('contain', 'Validation Rules');
    cy.get('[role="tab"]').eq(3).should('contain', 'Notifications');
  });

  it('should restrict access for unauthorized users', () => {
    cy.loginAsAnalyst();
    cy.visit('/configuration');
    cy.url().should('include', '/dashboard');
    cy.get('.MuiAlert-root').should('contain', 'You do not have permission to access this page');
  });

  it('should switch between configuration tabs', () => {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');

    cy.get('[role="tab"]').eq(1).click();
    cy.get('h2').should('contain', 'Pipeline Configurations');

    cy.get('[role="tab"]').eq(2).click();
    cy.get('h2').should('contain', 'Validation Rules');

    cy.get('[role="tab"]').eq(3).click();
    cy.get('h2').should('contain', 'Notification Settings');

    cy.get('[role="tab"]').eq(0).click();
    cy.get('h2').should('contain', 'Data Sources');
  });
});

// Data Sources Configuration test suite
describe('Data Sources Configuration', () => {
  beforeEach(function() {
    // Initialize mock data
    cy.fixture('mockData').then((mockData) => {
      this.dataSources = mockData.dataSources;
      this.pipelineConfigs = mockData.pipelineConfigs;
      this.validationRules = mockData.validationRules;
      this.notificationConfig = mockData.notificationConfig;
    });
  });

  it('should display data sources table', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.get('h2').should('contain', 'Data Sources');
    cy.wait('@getDataSources');

    cy.get('table').should('be.visible');
    cy.get('th').should('contain', 'Type');
    cy.get('th').should('contain', 'Name');
    cy.get('th').should('contain', 'Source Type');
    cy.get('th').should('contain', 'Status');
    cy.get('th').should('contain', 'Description');
    cy.get('th').should('contain', 'Last Updated');
    cy.get('th').should('contain', 'Actions');

    cy.get('tbody tr').should('have.length', 3);
    cy.get('tbody tr').eq(0).should('contain', 'Sales Data GCS');
    cy.get('tbody tr').eq(1).should('contain', 'Customer Database');
    cy.get('tbody tr').eq(2).should('contain', 'Product API');
  });

  it('should filter data sources', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.wait('@getDataSources');

    // Open filter panel
    cy.get('[aria-label="Filter"]').click();

    // Filter by source type
    cy.get('[data-testid="source-type-filter"]').click();
    cy.get('[role="listbox"] [data-value="GCS"]').click();

    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources).filter(src => src.sourceType === 'GCS'),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 1,
          totalPages: 1
        }
      }
    }).as('getFilteredDataSources');

    cy.wait('@getFilteredDataSources');
    cy.get('tbody tr').should('have.length', 1);
    cy.get('tbody tr').eq(0).should('contain', 'Sales Data GCS');

    // Add status filter
    cy.get('[data-testid="status-filter"]').click();
    cy.get('[role="listbox"] [data-value="OK"]').click();

    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources).filter(src => src.sourceType === 'GCS' && src.status === 'OK'),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 1,
          totalPages: 1
        }
      }
    }).as('getFilteredDataSources2');

    cy.wait('@getFilteredDataSources2');
    cy.get('tbody tr').should('have.length', 1);

    // Search by name
    cy.get('[data-testid="search-input"]').type('Sales');

    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources).filter(src => 
          src.sourceType === 'GCS' && 
          src.status === 'OK' && 
          src.name.includes('Sales')
        ),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 1,
          totalPages: 1
        }
      }
    }).as('getSearchedDataSources');

    cy.wait('@getSearchedDataSources');
    cy.get('tbody tr').should('have.length', 1);
    cy.get('tbody tr').eq(0).should('contain', 'Sales Data GCS');
  });

  it('should add a new GCS data source', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.wait('@getDataSources');

    // Click add button
    cy.get('[data-testid="add-source-button"]').click();

    // Verify form is displayed
    cy.get('[data-testid="source-form"]').should('be.visible');

    // Fill the form
    cy.get('[data-testid="source-type-select"]').click();
    cy.get('[role="listbox"] [data-value="GCS"]').click();
    cy.get('[data-testid="source-name-input"]').type('Test GCS Source');
    cy.get('[data-testid="source-description-input"]').type('Test GCS source for e2e testing');
    cy.get('[data-testid="bucket-name-input"]').type('test-bucket');
    cy.get('[data-testid="path-input"]').type('test-path/');

    // Mock the API response for creating a source
    cy.intercept('POST', '/api/config/data-sources', {
      statusCode: 201,
      body: {
        data: {
          sourceId: 'src-new',
          name: 'Test GCS Source',
          sourceType: 'GCS',
          connectionDetails: {
            bucketName: 'test-bucket',
            path: 'test-path/'
          },
          description: 'Test GCS source for e2e testing',
          isActive: true,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          status: 'OK'
        },
        message: 'Data source created successfully'
      }
    }).as('createDataSource');

    // Mock the refreshed data sources list
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: [
          {
            sourceId: 'src-new',
            name: 'Test GCS Source',
            sourceType: 'GCS',
            connectionDetails: {
              bucketName: 'test-bucket',
              path: 'test-path/'
            },
            description: 'Test GCS source for e2e testing',
            isActive: true,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            status: 'OK'
          },
          ...Cypress._.cloneDeep(this.dataSources)
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 4,
          totalPages: 1
        }
      }
    }).as('getUpdatedDataSources');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@createDataSource');
    cy.get('.MuiSnackbar-root').should('contain', 'Data source created successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedDataSources');
    cy.get('tbody tr').should('have.length', 4);
    cy.get('tbody tr').eq(0).should('contain', 'Test GCS Source');
  });

  it('should edit an existing data source', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.wait('@getDataSources');

    // Click edit button for the first source
    cy.get('tbody tr').eq(0).find('[data-testid="edit-button"]').click();

    // Verify form is displayed with pre-filled values
    cy.get('[data-testid="source-form"]').should('be.visible');
    cy.get('[data-testid="source-name-input"]').should('have.value', 'Sales Data GCS');
    cy.get('[data-testid="source-description-input"]').should('have.value', 'GCS bucket containing sales data files');

    // Modify the description
    cy.get('[data-testid="source-description-input"]').clear().type('Updated GCS bucket description');

    // Mock the API response for updating a source
    cy.intercept('PUT', '/api/config/data-sources/src-001', {
      statusCode: 200,
      body: {
        data: {
          sourceId: 'src-001',
          name: 'Sales Data GCS',
          sourceType: 'GCS',
          connectionDetails: {
            bucketName: 'sales-data-bucket',
            path: 'sales/'
          },
          description: 'Updated GCS bucket description',
          isActive: true,
          updatedAt: new Date().toISOString(),
          status: 'OK'
        },
        message: 'Data source updated successfully'
      }
    }).as('updateDataSource');

    // Mock the refreshed data sources list
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: [
          {
            ...Cypress._.cloneDeep(this.dataSources[0]),
            description: 'Updated GCS bucket description',
            updatedAt: new Date().toISOString()
          },
          ...Cypress._.cloneDeep(this.dataSources.slice(1))
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getUpdatedDataSources');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updateDataSource');
    cy.get('.MuiSnackbar-root').should('contain', 'Data source updated successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedDataSources');
    cy.get('tbody tr').eq(0).should('contain', 'Updated GCS bucket description');
  });

  it('should test a data source connection', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.wait('@getDataSources');

    // Mock the test connection API response
    cy.intercept('POST', '/api/config/data-sources/src-001/test-connection', {
      statusCode: 200,
      body: {
        data: {
          success: true,
          message: 'Connection successful'
        }
      }
    }).as('testConnection');

    // Click test connection button for the first source
    cy.get('tbody tr').eq(0).find('[data-testid="test-connection-button"]').click();

    // Verify loading indicator and then success notification
    cy.get('[role="progressbar"]').should('exist');
    cy.wait('@testConnection');
    cy.get('.MuiSnackbar-root').should('contain', 'Connection successful');
  });

  it('should delete a data source', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    cy.wait('@getDataSources');

    // Click delete button for the first source
    cy.get('tbody tr').eq(0).find('[data-testid="delete-button"]').click();

    // Verify confirmation dialog is displayed
    cy.get('[role="dialog"]').should('be.visible');
    cy.get('[role="dialog"]').should('contain', 'Delete Data Source');

    // Click cancel button
    cy.get('[data-testid="cancel-button"]').click();

    // Verify dialog is closed and source still exists
    cy.get('[role="dialog"]').should('not.exist');
    cy.get('tbody tr').should('have.length', 3);

    // Click delete button again
    cy.get('tbody tr').eq(0).find('[data-testid="delete-button"]').click();

    // Mock the delete API response
    cy.intercept('DELETE', '/api/config/data-sources/src-001', {
      statusCode: 200,
      body: {
        data: {
          success: true
        },
        message: 'Data source deleted successfully'
      }
    }).as('deleteDataSource');

    // Mock the refreshed data sources list
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources.slice(1)),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 2,
          totalPages: 1
        }
      }
    }).as('getUpdatedDataSources');

    // Click confirm button
    cy.get('[data-testid="confirm-button"]').click();

    // Verify API call and success notification
    cy.wait('@deleteDataSource');
    cy.get('.MuiSnackbar-root').should('contain', 'Data source deleted successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedDataSources');
    cy.get('tbody tr').should('have.length', 2);
    cy.get('tbody tr').eq(0).should('contain', 'Customer Database');
  });
});

// Pipeline Configuration test suite
describe('Pipeline Configuration', () => {
  beforeEach(function() {
    // Initialize mock data
    cy.fixture('mockData').then((mockData) => {
      this.dataSources = mockData.dataSources;
      this.pipelineConfigs = mockData.pipelineConfigs;
      this.validationRules = mockData.validationRules;
      this.notificationConfig = mockData.notificationConfig;
    });
  });

  it('should display pipeline configurations table', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Pipelines tab
    cy.get('[role="tab"]').eq(1).click();

    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.pipelineConfigs),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getPipelines');

    cy.wait('@getPipelines');

    // Verify the pipeline configurations table is displayed
    cy.get('h2').should('contain', 'Pipeline Configurations');
    cy.get('table').should('be.visible');
    cy.get('th').should('contain', 'Name');
    cy.get('th').should('contain', 'Source');
    cy.get('th').should('contain', 'Target');
    cy.get('th').should('contain', 'Status');
    cy.get('th').should('contain', 'Schedule');
    cy.get('th').should('contain', 'Last Updated');
    cy.get('th').should('contain', 'Actions');

    // Verify data is loaded in the table
    cy.get('tbody tr').should('have.length', 3);
    cy.get('tbody tr').eq(0).should('contain', 'Daily Sales Import');
    cy.get('tbody tr').eq(1).should('contain', 'Customer Data Sync');
    cy.get('tbody tr').eq(2).should('contain', 'Product Catalog Update');
  });

  it('should add a new pipeline configuration', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Pipelines tab
    cy.get('[role="tab"]').eq(1).click();

    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.pipelineConfigs),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getPipelines');

    cy.wait('@getPipelines');

    // Click add button
    cy.get('[data-testid="add-pipeline-button"]').click();

    // Verify form is displayed
    cy.get('[data-testid="pipeline-form"]').should('be.visible');

    // Fill the form
    cy.get('[data-testid="pipeline-name-input"]').type('Test Pipeline');
    cy.get('[data-testid="pipeline-source-select"]').click();
    cy.get('[role="listbox"] [data-value="src-001"]').click();
    cy.get('[data-testid="pipeline-dataset-input"]').type('test_dataset');
    cy.get('[data-testid="pipeline-table-input"]').type('test_table');
    cy.get('[data-testid="pipeline-description-input"]').type('Test pipeline for e2e testing');
    cy.get('[data-testid="pipeline-schedule-input"]').type('0 4 * * *');

    // Mock the API response for creating a pipeline
    cy.intercept('POST', '/api/config/pipelines', {
      statusCode: 201,
      body: {
        data: {
          pipelineId: 'pipe-new',
          name: 'Test Pipeline',
          sourceId: 'src-001',
          targetDataset: 'test_dataset',
          targetTable: 'test_table',
          description: 'Test pipeline for e2e testing',
          schedule: '0 4 * * *',
          configuration: {},
          isActive: true,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          status: 'HEALTHY'
        },
        message: 'Pipeline configuration created successfully'
      }
    }).as('createPipeline');

    // Mock the refreshed pipelines list
    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: [
          {
            pipelineId: 'pipe-new',
            name: 'Test Pipeline',
            sourceId: 'src-001',
            targetDataset: 'test_dataset',
            targetTable: 'test_table',
            description: 'Test pipeline for e2e testing',
            schedule: '0 4 * * *',
            configuration: {},
            isActive: true,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            status: 'HEALTHY'
          },
          ...Cypress._.cloneDeep(this.pipelineConfigs)
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 4,
          totalPages: 1
        }
      }
    }).as('getUpdatedPipelines');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@createPipeline');
    cy.get('.MuiSnackbar-root').should('contain', 'Pipeline configuration created successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedPipelines');
    cy.get('tbody tr').should('have.length', 4);
    cy.get('tbody tr').eq(0).should('contain', 'Test Pipeline');
  });

  it('should edit an existing pipeline configuration', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Pipelines tab
    cy.get('[role="tab"]').eq(1).click();

    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.pipelineConfigs),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getPipelines');

    cy.wait('@getPipelines');

    // Click edit button for the first pipeline
    cy.get('tbody tr').eq(0).find('[data-testid="edit-button"]').click();

    // Verify form is displayed with pre-filled values
    cy.get('[data-testid="pipeline-form"]').should('be.visible');
    cy.get('[data-testid="pipeline-name-input"]').should('have.value', 'Daily Sales Import');
    cy.get('[data-testid="pipeline-description-input"]').should('have.value', 'Daily import of sales data from GCS');

    // Modify the description
    cy.get('[data-testid="pipeline-description-input"]').clear().type('Updated pipeline description');

    // Mock the API response for updating a pipeline
    cy.intercept('PUT', '/api/config/pipelines/pipe-001', {
      statusCode: 200,
      body: {
        data: {
          pipelineId: 'pipe-001',
          name: 'Daily Sales Import',
          sourceId: 'src-001',
          targetDataset: 'sales_data',
          targetTable: 'daily_sales',
          description: 'Updated pipeline description',
          schedule: '0 5 * * *',
          configuration: {
            filePattern: '*.csv',
            incrementalField: 'date'
          },
          isActive: true,
          updatedAt: new Date().toISOString(),
          status: 'HEALTHY'
        },
        message: 'Pipeline configuration updated successfully'
      }
    }).as('updatePipeline');

    // Mock the refreshed pipelines list
    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: [
          {
            ...Cypress._.cloneDeep(this.pipelineConfigs[0]),
            description: 'Updated pipeline description',
            updatedAt: new Date().toISOString()
          },
          ...Cypress._.cloneDeep(this.pipelineConfigs.slice(1))
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getUpdatedPipelines');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updatePipeline');
    cy.get('.MuiSnackbar-root').should('contain', 'Pipeline configuration updated successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedPipelines');
    cy.get('tbody tr').eq(0).should('contain', 'Updated pipeline description');
  });

  it('should delete a pipeline configuration', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Pipelines tab
    cy.get('[role="tab"]').eq(1).click();

    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.pipelineConfigs),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getPipelines');

    cy.wait('@getPipelines');

    // Click delete button for the first pipeline
    cy.get('tbody tr').eq(0).find('[data-testid="delete-button"]').click();

    // Verify confirmation dialog is displayed
    cy.get('[role="dialog"]').should('be.visible');
    cy.get('[role="dialog"]').should('contain', 'Delete Pipeline');

    // Mock the delete API response
    cy.intercept('DELETE', '/api/config/pipelines/pipe-001', {
      statusCode: 200,
      body: {
        data: {
          success: true
        },
        message: 'Pipeline configuration deleted successfully'
      }
    }).as('deletePipeline');

    // Mock the refreshed pipelines list
    cy.intercept('GET', '/api/config/pipelines*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.pipelineConfigs.slice(1)),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 2,
          totalPages: 1
        }
      }
    }).as('getUpdatedPipelines');

    // Click confirm button
    cy.get('[data-testid="confirm-button"]').click();

    // Verify API call and success notification
    cy.wait('@deletePipeline');
    cy.get('.MuiSnackbar-root').should('contain', 'Pipeline configuration deleted successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedPipelines');
    cy.get('tbody tr').should('have.length', 2);
    cy.get('tbody tr').eq(0).should('contain', 'Customer Data Sync');
  });
});

// Validation Rules Configuration test suite
describe('Validation Rules Configuration', () => {
  beforeEach(function() {
    // Initialize mock data
    cy.fixture('mockData').then((mockData) => {
      this.dataSources = mockData.dataSources;
      this.pipelineConfigs = mockData.pipelineConfigs;
      this.validationRules = mockData.validationRules;
      this.notificationConfig = mockData.notificationConfig;
    });
  });

  it('should display validation rules table', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Validation Rules tab
    cy.get('[role="tab"]').eq(2).click();

    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.validationRules),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getValidationRules');

    cy.wait('@getValidationRules');

    // Verify the validation rules table is displayed
    cy.get('h2').should('contain', 'Validation Rules');
    cy.get('table').should('be.visible');
    cy.get('th').should('contain', 'Name');
    cy.get('th').should('contain', 'Dataset');
    cy.get('th').should('contain', 'Table');
    cy.get('th').should('contain', 'Rule Type');
    cy.get('th').should('contain', 'Severity');
    cy.get('th').should('contain', 'Status');
    cy.get('th').should('contain', 'Actions');

    // Verify data is loaded in the table
    cy.get('tbody tr').should('have.length', 3);
    cy.get('tbody tr').eq(0).should('contain', 'Sales Amount Not Null');
    cy.get('tbody tr').eq(1).should('contain', 'Customer Email Format');
    cy.get('tbody tr').eq(2).should('contain', 'Product Price Range');
  });

  it('should add a new validation rule', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Validation Rules tab
    cy.get('[role="tab"]').eq(2).click();

    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.validationRules),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getValidationRules');

    cy.wait('@getValidationRules');

    // Click add button
    cy.get('[data-testid="add-rule-button"]').click();

    // Verify form is displayed
    cy.get('[data-testid="rule-form"]').should('be.visible');

    // Fill the form
    cy.get('[data-testid="rule-name-input"]').type('Test Rule');
    cy.get('[data-testid="rule-dataset-input"]').type('test_dataset');
    cy.get('[data-testid="rule-table-input"]').type('test_table');
    cy.get('[data-testid="rule-type-select"]').click();
    cy.get('[role="listbox"] [data-value="NOT_NULL"]').click();
    cy.get('[data-testid="rule-column-input"]').type('test_column');
    cy.get('[data-testid="rule-severity-select"]').click();
    cy.get('[role="listbox"] [data-value="HIGH"]').click();
    cy.get('[data-testid="rule-description-input"]').type('Test validation rule for e2e testing');

    // Mock the API response for creating a validation rule
    cy.intercept('POST', '/api/config/validation-rules', {
      statusCode: 201,
      body: {
        data: {
          ruleId: 'rule-new',
          name: 'Test Rule',
          targetDataset: 'test_dataset',
          targetTable: 'test_table',
          ruleType: 'NOT_NULL',
          expectationType: 'expect_column_values_to_not_be_null',
          ruleDefinition: {
            column: 'test_column'
          },
          severity: 'HIGH',
          isActive: true,
          description: 'Test validation rule for e2e testing',
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString()
        },
        message: 'Validation rule created successfully'
      }
    }).as('createValidationRule');

    // Mock the refreshed validation rules list
    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: [
          {
            ruleId: 'rule-new',
            name: 'Test Rule',
            targetDataset: 'test_dataset',
            targetTable: 'test_table',
            ruleType: 'NOT_NULL',
            expectationType: 'expect_column_values_to_not_be_null',
            ruleDefinition: {
              column: 'test_column'
            },
            severity: 'HIGH',
            isActive: true,
            description: 'Test validation rule for e2e testing',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
          },
          ...Cypress._.cloneDeep(this.validationRules)
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 4,
          totalPages: 1
        }
      }
    }).as('getUpdatedValidationRules');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@createValidationRule');
    cy.get('.MuiSnackbar-root').should('contain', 'Validation rule created successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedValidationRules');
    cy.get('tbody tr').should('have.length', 4);
    cy.get('tbody tr').eq(0).should('contain', 'Test Rule');
  });

  it('should edit an existing validation rule', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Validation Rules tab
    cy.get('[role="tab"]').eq(2).click();

    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.validationRules),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getValidationRules');

    cy.wait('@getValidationRules');

    // Click edit button for the first rule
    cy.get('tbody tr').eq(0).find('[data-testid="edit-button"]').click();

    // Verify form is displayed with pre-filled values
    cy.get('[data-testid="rule-form"]').should('be.visible');
    cy.get('[data-testid="rule-name-input"]').should('have.value', 'Sales Amount Not Null');
    cy.get('[data-testid="rule-severity-select"]').should('have.value', 'HIGH');

    // Modify the severity
    cy.get('[data-testid="rule-severity-select"]').click();
    cy.get('[role="listbox"] [data-value="MEDIUM"]').click();

    // Mock the API response for updating a validation rule
    cy.intercept('PUT', '/api/config/validation-rules/rule-001', {
      statusCode: 200,
      body: {
        data: {
          ruleId: 'rule-001',
          name: 'Sales Amount Not Null',
          targetDataset: 'sales_data',
          targetTable: 'daily_sales',
          ruleType: 'NOT_NULL',
          expectationType: 'expect_column_values_to_not_be_null',
          ruleDefinition: {
            column: 'sales_amount'
          },
          severity: 'MEDIUM',
          isActive: true,
          description: 'Validates that sales amount is never null',
          updatedAt: new Date().toISOString()
        },
        message: 'Validation rule updated successfully'
      }
    }).as('updateValidationRule');

    // Mock the refreshed validation rules list
    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: [
          {
            ...Cypress._.cloneDeep(this.validationRules[0]),
            severity: 'MEDIUM',
            updatedAt: new Date().toISOString()
          },
          ...Cypress._.cloneDeep(this.validationRules.slice(1))
        ],
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getUpdatedValidationRules');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updateValidationRule');
    cy.get('.MuiSnackbar-root').should('contain', 'Validation rule updated successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedValidationRules');
    cy.get('tbody tr').eq(0).should('contain', 'MEDIUM');
  });

  it('should delete a validation rule', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Validation Rules tab
    cy.get('[role="tab"]').eq(2).click();

    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.validationRules),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getValidationRules');

    cy.wait('@getValidationRules');

    // Click delete button for the first rule
    cy.get('tbody tr').eq(0).find('[data-testid="delete-button"]').click();

    // Verify confirmation dialog is displayed
    cy.get('[role="dialog"]').should('be.visible');
    cy.get('[role="dialog"]').should('contain', 'Delete Validation Rule');

    // Mock the delete API response
    cy.intercept('DELETE', '/api/config/validation-rules/rule-001', {
      statusCode: 200,
      body: {
        data: {
          success: true
        },
        message: 'Validation rule deleted successfully'
      }
    }).as('deleteValidationRule');

    // Mock the refreshed validation rules list
    cy.intercept('GET', '/api/config/validation-rules*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.validationRules.slice(1)),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 2,
          totalPages: 1
        }
      }
    }).as('getUpdatedValidationRules');

    // Click confirm button
    cy.get('[data-testid="confirm-button"]').click();

    // Verify API call and success notification
    cy.wait('@deleteValidationRule');
    cy.get('.MuiSnackbar-root').should('contain', 'Validation rule deleted successfully');

    // Verify the table is updated
    cy.wait('@getUpdatedValidationRules');
    cy.get('tbody tr').should('have.length', 2);
    cy.get('tbody tr').eq(0).should('contain', 'Customer Email Format');
  });
});

// Notification Configuration test suite
describe('Notification Configuration', () => {
  beforeEach(function() {
    // Initialize mock data
    cy.fixture('mockData').then((mockData) => {
      this.dataSources = mockData.dataSources;
      this.pipelineConfigs = mockData.pipelineConfigs;
      this.validationRules = mockData.validationRules;
      this.notificationConfig = mockData.notificationConfig;
    });
  });

  it('should display notification settings form', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Notifications tab
    cy.get('[role="tab"]').eq(3).click();

    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.notificationConfig)
      }
    }).as('getNotificationConfig');

    cy.wait('@getNotificationConfig');

    // Verify the notification settings form is displayed
    cy.get('h2').should('contain', 'Notification Settings');

    // Verify form sections are present
    cy.get('[data-testid="teams-webhook-section"]').should('be.visible');
    cy.get('[data-testid="email-settings-section"]').should('be.visible');
    cy.get('[data-testid="enabled-channels-section"]').should('be.visible');
    cy.get('[data-testid="alert-thresholds-section"]').should('be.visible');

    // Verify form fields are populated
    cy.get('[data-testid="teams-webhook-url"]').should('have.value', 'https://outlook.office.com/webhook/example-webhook-url');
    cy.get('[data-testid="email-recipients"]').should('have.value', 'alerts@example.com, operations@example.com');
    cy.get('[data-testid="email-cc-recipients"]').should('have.value', 'management@example.com');
    cy.get('[data-testid="email-subject-prefix"]').should('have.value', '[DATA PIPELINE]');

    // Verify channel toggles
    cy.get('[data-testid="teams-channel-toggle"]').find('input').should('be.checked');
    cy.get('[data-testid="email-channel-toggle"]').find('input').should('be.checked');
    cy.get('[data-testid="in-app-channel-toggle"]').find('input').should('be.checked');
  });

  it('should update Microsoft Teams webhook URL', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Notifications tab
    cy.get('[role="tab"]').eq(3).click();

    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.notificationConfig)
      }
    }).as('getNotificationConfig');

    cy.wait('@getNotificationConfig');

    // Update the Teams webhook URL
    cy.get('[data-testid="teams-webhook-url"]').clear().type('https://outlook.office.com/webhook/updated-webhook-url');

    // Mock the update API response
    cy.intercept('PUT', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          teamsWebhookUrl: 'https://outlook.office.com/webhook/updated-webhook-url',
          updatedAt: new Date().toISOString()
        },
        message: 'Notification configuration updated successfully'
      }
    }).as('updateNotificationConfig');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updateNotificationConfig');
    cy.get('.MuiSnackbar-root').should('contain', 'Notification configuration updated successfully');

    // Mock the refreshed config for page reload
    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          teamsWebhookUrl: 'https://outlook.office.com/webhook/updated-webhook-url',
          updatedAt: new Date().toISOString()
        }
      }
    }).as('getUpdatedNotificationConfig');

    // Reload the page to verify persistence
    cy.reload();
    cy.get('[role="tab"]').eq(3).click();
    cy.wait('@getUpdatedNotificationConfig');

    // Verify the updated webhook URL is persisted
    cy.get('[data-testid="teams-webhook-url"]').should('have.value', 'https://outlook.office.com/webhook/updated-webhook-url');
  });

  it('should update email notification settings', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Notifications tab
    cy.get('[role="tab"]').eq(3).click();

    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.notificationConfig)
      }
    }).as('getNotificationConfig');

    cy.wait('@getNotificationConfig');

    // Update email settings
    cy.get('[data-testid="email-recipients"]').clear().type('newalerts@example.com, newops@example.com');
    cy.get('[data-testid="email-cc-recipients"]').clear().type('newmanagement@example.com, director@example.com');
    cy.get('[data-testid="email-subject-prefix"]').clear().type('[NEW PREFIX]');

    // Mock the update API response
    cy.intercept('PUT', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          emailSettings: {
            recipients: ['newalerts@example.com', 'newops@example.com'],
            ccRecipients: ['newmanagement@example.com', 'director@example.com'],
            subjectPrefix: '[NEW PREFIX]'
          },
          updatedAt: new Date().toISOString()
        },
        message: 'Notification configuration updated successfully'
      }
    }).as('updateNotificationConfig');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updateNotificationConfig');
    cy.get('.MuiSnackbar-root').should('contain', 'Notification configuration updated successfully');

    // Mock the refreshed config for page reload
    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          emailSettings: {
            recipients: ['newalerts@example.com', 'newops@example.com'],
            ccRecipients: ['newmanagement@example.com', 'director@example.com'],
            subjectPrefix: '[NEW PREFIX]'
          },
          updatedAt: new Date().toISOString()
        }
      }
    }).as('getUpdatedNotificationConfig');

    // Reload the page to verify persistence
    cy.reload();
    cy.get('[role="tab"]').eq(3).click();
    cy.wait('@getUpdatedNotificationConfig');

    // Verify the updated email settings are persisted
    cy.get('[data-testid="email-recipients"]').should('have.value', 'newalerts@example.com, newops@example.com');
    cy.get('[data-testid="email-cc-recipients"]').should('have.value', 'newmanagement@example.com, director@example.com');
    cy.get('[data-testid="email-subject-prefix"]').should('have.value', '[NEW PREFIX]');
  });

  it('should toggle notification channels', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Notifications tab
    cy.get('[role="tab"]').eq(3).click();

    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.notificationConfig)
      }
    }).as('getNotificationConfig');

    cy.wait('@getNotificationConfig');

    // Toggle notification channels
    cy.get('[data-testid="teams-channel-toggle"]').click();
    cy.get('[data-testid="email-channel-toggle"]').click();

    // Mock the update API response
    cy.intercept('PUT', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          enabledChannels: {
            teams: false,
            email: false,
            inApp: true
          },
          updatedAt: new Date().toISOString()
        },
        message: 'Notification configuration updated successfully'
      }
    }).as('updateNotificationConfig');

    // Submit the form
    cy.get('[data-testid="save-button"]').click();

    // Verify API call and success notification
    cy.wait('@updateNotificationConfig');
    cy.get('.MuiSnackbar-root').should('contain', 'Notification configuration updated successfully');

    // Mock the refreshed config for page reload
    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: {
          ...Cypress._.cloneDeep(this.notificationConfig),
          enabledChannels: {
            teams: false,
            email: false,
            inApp: true
          },
          updatedAt: new Date().toISOString()
        }
      }
    }).as('getUpdatedNotificationConfig');

    // Reload the page to verify persistence
    cy.reload();
    cy.get('[role="tab"]').eq(3).click();
    cy.wait('@getUpdatedNotificationConfig');

    // Verify the toggled channels are persisted
    cy.get('[data-testid="teams-channel-toggle"]').find('input').should('not.be.checked');
    cy.get('[data-testid="email-channel-toggle"]').find('input').should('not.be.checked');
    cy.get('[data-testid="in-app-channel-toggle"]').find('input').should('be.checked');
  });

  it('should test notification channels', function() {
    cy.loginAsAdmin();
    cy.navigateTo('configuration');
    cy.intercept('GET', '/api/config/data-sources*', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.dataSources),
        pagination: {
          page: 0,
          pageSize: 10,
          totalItems: 3,
          totalPages: 1
        }
      }
    }).as('getDataSources');

    // Click on the Notifications tab
    cy.get('[role="tab"]').eq(3).click();

    cy.intercept('GET', '/api/config/notifications', {
      statusCode: 200,
      body: {
        data: Cypress._.cloneDeep(this.notificationConfig)
      }
    }).as('getNotificationConfig');

    cy.wait('@getNotificationConfig');

    // Mock the test notification API response for Teams
    cy.intercept('POST', '/api/config/notifications/test', {
      statusCode: 200,
      body: {
        data: {
          success: true,
          message: 'Test notification sent successfully'
        }
      }
    }).as('testNotification');

    // Click the Test Teams Notification button
    cy.get('[data-testid="test-teams-button"]').click();

    // Verify loading indicator and then success notification
    cy.get('[role="progressbar"]').should('exist');
    cy.wait('@testNotification');
    cy.get('.MuiSnackbar-root').should('contain', 'Test notification sent successfully');

    // Click the Test Email Notification button
    cy.get('[data-testid="test-email-button"]').click();

    // Verify loading indicator and then success notification
    cy.get('[role="progressbar"]').should('exist');
    cy.wait('@testNotification');
    cy.get('.MuiSnackbar-root').should('contain', 'Test notification sent successfully');
  });
});