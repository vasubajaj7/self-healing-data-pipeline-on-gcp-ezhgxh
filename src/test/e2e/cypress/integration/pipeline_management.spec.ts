import cypress from 'cypress';

describe('Pipeline Management', () => {
  beforeEach(() => {
    // Preserve cookies between tests
    cy.session('authenticated', () => {
      cy.login('data.engineer@example.com', 'password123');
    });
    
    // Mock pipeline API responses using fixture data
    cy.fixture('pipeline_data.json').then((data) => {
      // Mock pipeline list endpoint
      cy.intercept('GET', '/api/pipelines*', {
        statusCode: 200,
        body: data.pipelineListResponse
      }).as('getPipelines');
      
      // Mock single pipeline endpoint
      cy.intercept('GET', '/api/pipelines/*', (req) => {
        const pipelineId = req.url.split('/').pop().split('?')[0];
        const pipeline = data.pipelineListResponse.items.find(p => p.pipeline_id === pipelineId);
        
        if (pipeline) {
          req.reply({
            statusCode: 200,
            body: pipeline
          });
        } else {
          req.reply({
            statusCode: 404,
            body: { error: 'Not Found', message: 'Pipeline not found' }
          });
        }
      }).as('getPipeline');
      
      // Mock pipeline execution history
      cy.intercept('GET', '/api/pipelines/*/executions*', {
        statusCode: 200,
        body: data.pipelineExecutionListResponse
      }).as('getPipelineExecutions');
      
      // Mock execution details
      cy.intercept('GET', '/api/executions/*', (req) => {
        const executionId = req.url.split('/').pop().split('?')[0];
        const execution = data.pipelineExecutionListResponse.items.find(e => e.execution_id === executionId);
        
        if (execution) {
          req.reply({
            statusCode: 200,
            body: execution
          });
        } else {
          req.reply({
            statusCode: 404,
            body: { error: 'Not Found', message: 'Execution not found' }
          });
        }
      }).as('getExecution');
      
      // Mock task executions
      cy.intercept('GET', '/api/executions/*/tasks*', {
        statusCode: 200,
        body: data.taskExecutionListResponse
      }).as('getTaskExecutions');
    });
    
    // Login as a data engineer user
    cy.login('data.engineer@example.com', 'password123');
    
    // Navigate to the pipeline management page
    cy.visit('/pipelines');
    cy.wait('@getPipelines');
  });
  
  /**
   * Helper function to verify pipeline table data matches expected values
   */
  const verifyPipelineTableData = (expectedPipelines) => {
    cy.get('table.pipeline-table tbody tr').should('have.length', expectedPipelines.length);
    
    expectedPipelines.forEach((pipeline, index) => {
      cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-name`)
        .should('contain', pipeline.name);
      
      cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-status`)
        .should('contain', pipeline.status);
        
      // Verify status indicator color
      if (pipeline.status === 'HEALTHY') {
        cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-status .status-indicator`)
          .should('have.class', 'healthy');
      } else if (pipeline.status === 'ERROR') {
        cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-status .status-indicator`)
          .should('have.class', 'error');
      } else if (pipeline.status === 'WARNING') {
        cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-status .status-indicator`)
          .should('have.class', 'warning');
      }
      
      cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-source`)
        .should('contain', pipeline.source_id);
        
      cy.get(`table.pipeline-table tbody tr:eq(${index}) td.pipeline-target`)
        .should('contain', `${pipeline.target_dataset}.${pipeline.target_table}`);
    });
  };
  
  /**
   * Helper function to fill pipeline creation/edit form
   */
  const fillPipelineForm = (pipelineData) => {
    // Fill basic information
    cy.get('#pipeline-name').clear().type(pipelineData.name);
    cy.get('#pipeline-description').clear().type(pipelineData.description);
    cy.get('#source-select').select(pipelineData.source_id);
    cy.get('#target-dataset').clear().type(pipelineData.target_dataset);
    cy.get('#target-table').clear().type(pipelineData.target_table);
    
    // Fill transformation configuration
    cy.get('.transformation-tab').click();
    cy.get('#add-transformation').click();
    pipelineData.transformation_config.transformations.forEach((transform, index) => {
      if (index > 0) cy.get('#add-transformation').click();
      cy.get(`#transformation-type-${index}`).select(transform.type);
      cy.get(`#transformation-source-${index}`).clear().type(transform.source);
      cy.get(`#transformation-target-${index}`).clear().type(transform.target);
    });
    
    // Fill quality configuration
    cy.get('.quality-tab').click();
    cy.get('#quality-enabled').check();
    cy.get('#quality-threshold').clear().type(pipelineData.quality_config.threshold_score.toString());
    cy.get('#fail-on-error').check(pipelineData.quality_config.fail_on_validation_error);
    
    // Fill self-healing configuration
    cy.get('.healing-tab').click();
    cy.get('#healing-enabled').check();
    cy.get('#auto-correction').check(pipelineData.self_healing_config.auto_correction);
    cy.get('#confidence-threshold').clear().type(pipelineData.self_healing_config.confidence_threshold.toString());
    cy.get('#max-attempts').clear().type(pipelineData.self_healing_config.max_healing_attempts.toString());
    
    // Fill scheduling configuration
    cy.get('.scheduling-tab').click();
    cy.get('#schedule-interval').clear().type(pipelineData.scheduling_config.schedule_interval);
    cy.get('#start-date').clear().type(pipelineData.scheduling_config.start_date.split('T')[0]);
    cy.get('#retries').clear().type(pipelineData.scheduling_config.retries.toString());
    cy.get('#retry-delay').clear().type(pipelineData.scheduling_config.retry_delay.toString());
  };
  
  /**
   * Helper function to verify pipeline details page
   */
  const verifyPipelineDetails = (expectedPipeline) => {
    cy.get('.pipeline-details-header h1').should('contain', expectedPipeline.name);
    cy.get('.pipeline-details-description').should('contain', expectedPipeline.description);
    cy.get('.pipeline-details-status').should('contain', expectedPipeline.status);
    
    cy.get('.pipeline-details-info .source').should('contain', expectedPipeline.source_id);
    cy.get('.pipeline-details-info .target').should('contain', `${expectedPipeline.target_dataset}.${expectedPipeline.target_table}`);
    
    cy.get('.pipeline-details-scheduling').should('contain', expectedPipeline.scheduling_config.schedule_interval);
    
    if (expectedPipeline.last_execution) {
      cy.get('.last-execution-status').should('contain', expectedPipeline.last_execution.status);
    }
  };
  
  /**
   * Helper function to verify execution details page
   */
  const verifyExecutionDetails = (expectedExecution) => {
    cy.get('.execution-details-header h1').should('contain', expectedExecution.execution_id);
    cy.get('.execution-details-status').should('contain', expectedExecution.status);
    
    cy.get('.execution-details-metrics .records-processed').should('contain', expectedExecution.records_processed);
    cy.get('.execution-details-metrics .records-failed').should('contain', expectedExecution.records_failed);
    
    if (expectedExecution.quality_score !== null) {
      cy.get('.execution-details-metrics .quality-score').should('contain', expectedExecution.quality_score);
    }
    
    cy.get('.execution-details-timing .start-time')
      .should('contain', new Date(expectedExecution.start_time).toLocaleString());
    cy.get('.execution-details-timing .end-time')
      .should('contain', new Date(expectedExecution.end_time).toLocaleString());
    
    // Verify self-healing attempts if present
    if (expectedExecution.self_healing_attempts && expectedExecution.self_healing_attempts.length > 0) {
      cy.get('.self-healing-tab').click();
      cy.get('.self-healing-attempts-list').should('exist');
      cy.get('.self-healing-attempts-list .attempt')
        .should('have.length', expectedExecution.self_healing_attempts.length);
    }
  };
  
  it('should display pipeline inventory with correct data', () => {
    // Verify the page title is displayed
    cy.get('h1').should('contain', 'Pipeline Management');
    
    // Verify the create pipeline button is visible
    cy.get('.create-pipeline-button').should('be.visible');
    
    // Verify the search field is visible
    cy.get('.search-field').should('be.visible');
    
    // Verify the status filter is visible
    cy.get('.status-filter').should('be.visible');
    
    // Verify the pipeline table is displayed
    cy.get('table.pipeline-table').should('be.visible');
    
    cy.fixture('pipeline_data.json').then((data) => {
      const expectedPipelines = data.pipelineListResponse.items;
      
      // Verify the table contains the expected number of pipelines
      cy.get('table.pipeline-table tbody tr').should('have.length', expectedPipelines.length);
      
      // Verify pipeline data is displayed correctly
      verifyPipelineTableData(expectedPipelines);
    });
  });
  
  it('should filter pipelines by search term', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      // Type 'Sales' in the search field
      cy.get('.search-field input').clear().type('Sales');
      
      // Verify only pipelines with 'Sales' in the name are displayed
      const salesPipelines = data.pipelineListResponse.items.filter(
        p => p.name.includes('Sales') || p.description.includes('Sales')
      );
      verifyPipelineTableData(salesPipelines);
      
      // Clear the search field
      cy.get('.search-field input').clear();
      
      // Verify all pipelines are displayed again
      verifyPipelineTableData(data.pipelineListResponse.items);
      
      // Type 'API' in the search field
      cy.get('.search-field input').clear().type('API');
      
      // Verify only pipelines with 'API' in the name or description are displayed
      const apiPipelines = data.pipelineListResponse.items.filter(
        p => p.name.includes('API') || p.description.includes('API')
      );
      verifyPipelineTableData(apiPipelines);
      
      // Verify the filtered count matches expected results
      cy.get('.filtered-count').should('contain', `Showing ${apiPipelines.length} of ${data.pipelineListResponse.items.length} pipelines`);
    });
  });
  
  it('should filter pipelines by status', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      // Select 'HEALTHY' from the status filter dropdown
      cy.get('.status-filter select').select('HEALTHY');
      
      // Verify only pipelines with HEALTHY status are displayed
      const healthyPipelines = data.pipelineListResponse.items.filter(
        p => p.status === 'HEALTHY'
      );
      verifyPipelineTableData(healthyPipelines);
      
      // Select 'ERROR' from the status filter dropdown
      cy.get('.status-filter select').select('ERROR');
      
      // Verify only pipelines with ERROR status are displayed
      const errorPipelines = data.pipelineListResponse.items.filter(
        p => p.status === 'ERROR'
      );
      verifyPipelineTableData(errorPipelines);
      
      // Select 'ALL' from the status filter dropdown
      cy.get('.status-filter select').select('ALL');
      
      // Verify all pipelines are displayed again
      verifyPipelineTableData(data.pipelineListResponse.items);
    });
  });
  
  it('should navigate to pipeline details when clicking on a pipeline', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const firstPipeline = data.pipelineListResponse.items[0];
      
      // Click on the first pipeline in the table
      cy.get('table.pipeline-table tbody tr:first').click();
      
      // Verify URL contains the pipeline ID
      cy.url().should('include', `/pipelines/${firstPipeline.pipeline_id}`);
      
      // Verify pipeline details page is displayed
      cy.wait('@getPipeline');
      
      // Verify pipeline name is displayed in the header
      cy.get('.pipeline-details-header h1').should('contain', firstPipeline.name);
      
      // Verify pipeline details are displayed correctly
      verifyPipelineDetails(firstPipeline);
      
      // Verify the back button is visible
      cy.get('.back-button').should('be.visible');
    });
  });
  
  it('should create a new pipeline successfully', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      // Mock the create pipeline API endpoint
      cy.intercept('POST', '/api/pipelines', {
        statusCode: 201,
        body: data.apiResponses.createPipeline.success.body
      }).as('createPipeline');
      
      // Click on the create pipeline button
      cy.get('.create-pipeline-button').click();
      
      // Verify the create pipeline modal is displayed
      cy.get('.create-pipeline-modal').should('be.visible');
      
      // Fill in the required pipeline fields
      const newPipeline = {
        name: 'Test New Pipeline',
        description: 'Created during E2E testing',
        source_id: 'sales_gcs',
        target_dataset: 'test_dataset',
        target_table: 'test_table',
        transformation_config: {
          transformations: [
            {
              type: 'RENAME',
              source: 'old_column',
              target: 'new_column'
            }
          ]
        },
        quality_config: {
          threshold_score: 95,
          fail_on_validation_error: true
        },
        self_healing_config: {
          auto_correction: true,
          confidence_threshold: 85,
          max_healing_attempts: 3
        },
        scheduling_config: {
          schedule_interval: '0 0 * * *',
          start_date: '2023-01-01',
          retries: 3,
          retry_delay: 300
        }
      };
      
      fillPipelineForm(newPipeline);
      
      // Click the save button
      cy.get('.pipeline-form-submit').click();
      
      // Verify the API was called with the correct data
      cy.wait('@createPipeline').then((interception) => {
        expect(interception.request.body.name).to.equal(newPipeline.name);
        expect(interception.request.body.description).to.equal(newPipeline.description);
        expect(interception.request.body.source_id).to.equal(newPipeline.source_id);
        expect(interception.request.body.target_dataset).to.equal(newPipeline.target_dataset);
        expect(interception.request.body.target_table).to.equal(newPipeline.target_table);
      });
      
      // Verify success notification is displayed
      cy.get('.notification-success').should('be.visible');
      cy.get('.notification-success').should('contain', 'Pipeline created successfully');
      
      // Verify the new pipeline appears in the table
      cy.intercept('GET', '/api/pipelines*', (req) => {
        const updatedResponse = {...data.pipelineListResponse};
        updatedResponse.items = [
          data.apiResponses.createPipeline.success.body,
          ...data.pipelineListResponse.items
        ];
        req.reply({
          statusCode: 200,
          body: updatedResponse
        });
      }).as('getUpdatedPipelines');
      
      cy.reload();
      cy.wait('@getUpdatedPipelines');
      
      cy.get('table.pipeline-table tbody tr:first td.pipeline-name')
        .should('contain', newPipeline.name);
    });
  });
  
  it('should edit an existing pipeline successfully', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const pipelineToEdit = data.pipelineListResponse.items[0];
      const updatedPipeline = {
        ...pipelineToEdit,
        name: 'Updated Pipeline Name',
        description: 'Updated during E2E testing',
        scheduling_config: {
          ...pipelineToEdit.scheduling_config,
          schedule_interval: '0 12 * * *'
        }
      };
      
      // Mock the update pipeline API endpoint
      cy.intercept('PUT', `/api/pipelines/${pipelineToEdit.pipeline_id}`, {
        statusCode: 200,
        body: updatedPipeline
      }).as('updatePipeline');
      
      // Find a pipeline in the table
      cy.get('table.pipeline-table tbody tr:first .edit-button').click();
      
      // Verify the edit pipeline modal is displayed
      cy.get('.edit-pipeline-modal').should('be.visible');
      
      // Verify the form is pre-filled with the pipeline's data
      cy.get('#pipeline-name').should('have.value', pipelineToEdit.name);
      cy.get('#pipeline-description').should('have.value', pipelineToEdit.description);
      
      // Change the pipeline name and description
      cy.get('#pipeline-name').clear().type(updatedPipeline.name);
      cy.get('#pipeline-description').clear().type(updatedPipeline.description);
      
      // Update the scheduling configuration
      cy.get('.scheduling-tab').click();
      cy.get('#schedule-interval').clear().type(updatedPipeline.scheduling_config.schedule_interval);
      
      // Click the save button
      cy.get('.pipeline-form-submit').click();
      
      // Verify the API was called with the correct data
      cy.wait('@updatePipeline').then((interception) => {
        expect(interception.request.body.name).to.equal(updatedPipeline.name);
        expect(interception.request.body.description).to.equal(updatedPipeline.description);
        expect(interception.request.body.scheduling_config.schedule_interval)
          .to.equal(updatedPipeline.scheduling_config.schedule_interval);
      });
      
      // Verify success notification is displayed
      cy.get('.notification-success').should('be.visible');
      cy.get('.notification-success').should('contain', 'Pipeline updated successfully');
      
      // Verify the pipeline in the table shows the updated information
      cy.intercept('GET', '/api/pipelines*', (req) => {
        const updatedResponse = {...data.pipelineListResponse};
        updatedResponse.items[0] = updatedPipeline;
        req.reply({
          statusCode: 200,
          body: updatedResponse
        });
      }).as('getUpdatedPipelines');
      
      cy.reload();
      cy.wait('@getUpdatedPipelines');
      
      cy.get('table.pipeline-table tbody tr:first td.pipeline-name')
        .should('contain', updatedPipeline.name);
    });
  });
  
  it('should delete a pipeline after confirmation', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const pipelineToDelete = data.pipelineListResponse.items[0];
      
      // Mock the delete pipeline API endpoint
      cy.intercept('DELETE', `/api/pipelines/${pipelineToDelete.pipeline_id}`, {
        statusCode: 200,
        body: { success: true }
      }).as('deletePipeline');
      
      // Find a pipeline in the table
      cy.get('table.pipeline-table tbody tr:first .delete-button').click();
      
      // Verify the delete confirmation dialog is displayed
      cy.get('.delete-confirmation-dialog').should('be.visible');
      
      // Click the cancel button
      cy.get('.delete-confirmation-dialog .cancel-button').click();
      
      // Verify the dialog is closed and pipeline still exists
      cy.get('.delete-confirmation-dialog').should('not.exist');
      cy.get('table.pipeline-table tbody tr:first td.pipeline-name')
        .should('contain', pipelineToDelete.name);
      
      // Click the delete button again
      cy.get('table.pipeline-table tbody tr:first .delete-button').click();
      
      // Click the confirm button in the dialog
      cy.get('.delete-confirmation-dialog .confirm-button').click();
      
      // Verify the API was called with the correct pipeline ID
      cy.wait('@deletePipeline').then((interception) => {
        expect(interception.request.url).to.include(pipelineToDelete.pipeline_id);
      });
      
      // Verify success notification is displayed
      cy.get('.notification-success').should('be.visible');
      cy.get('.notification-success').should('contain', 'Pipeline deleted successfully');
      
      // Verify the pipeline is removed from the table
      cy.intercept('GET', '/api/pipelines*', (req) => {
        const updatedResponse = {...data.pipelineListResponse};
        updatedResponse.items = data.pipelineListResponse.items.slice(1);
        req.reply({
          statusCode: 200,
          body: updatedResponse
        });
      }).as('getUpdatedPipelines');
      
      cy.reload();
      cy.wait('@getUpdatedPipelines');
      
      cy.get('table.pipeline-table tbody tr').should('have.length', data.pipelineListResponse.items.length - 1);
      cy.get('table.pipeline-table tbody tr:first td.pipeline-name')
        .should('not.contain', pipelineToDelete.name);
    });
  });
  
  it('should view pipeline execution history', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const pipeline = data.pipelineListResponse.items[0];
      
      // Mock the pipeline execution history API endpoint
      cy.intercept('GET', `/api/pipelines/${pipeline.pipeline_id}/executions*`, {
        statusCode: 200,
        body: data.pipelineExecutionListResponse
      }).as('getPipelineExecutions');
      
      // Find a pipeline in the table
      cy.get('table.pipeline-table tbody tr:first .history-button').click();
      
      // Verify URL contains the pipeline ID and history indicator
      cy.url().should('include', `/pipelines/${pipeline.pipeline_id}/history`);
      
      // Verify execution history page is displayed
      cy.wait('@getPipelineExecutions');
      
      cy.get('h1').should('contain', 'Execution History');
      cy.get('h2').should('contain', pipeline.name);
      
      // Verify the execution history table is displayed
      cy.get('table.execution-history-table').should('be.visible');
      cy.get('table.execution-history-table tbody tr').should('have.length', 
        data.pipelineExecutionListResponse.items.length);
      
      // Verify execution data is displayed correctly
      data.pipelineExecutionListResponse.items.forEach((execution, index) => {
        cy.get(`table.execution-history-table tbody tr:eq(${index}) td.execution-status`)
          .should('contain', execution.status);
          
        cy.get(`table.execution-history-table tbody tr:eq(${index}) td.execution-start-time`)
          .should('contain', new Date(execution.start_time).toLocaleString());
          
        if (execution.quality_score !== null) {
          cy.get(`table.execution-history-table tbody tr:eq(${index}) td.execution-quality-score`)
            .should('contain', execution.quality_score);
        }
      });
      
      // Verify pagination works for execution history
      if (data.pipelineExecutionListResponse.pagination.total_pages > 1) {
        // Mock next page response
        cy.intercept('GET', `/api/pipelines/${pipeline.pipeline_id}/executions?page=2*`, {
          statusCode: 200,
          body: {
            ...data.pipelineExecutionListResponse,
            pagination: {
              ...data.pipelineExecutionListResponse.pagination,
              page: 2
            }
          }
        }).as('getExecutionsPage2');
        
        // Click next page
        cy.get('.pagination-next').click();
        
        // Verify next page was loaded
        cy.wait('@getExecutionsPage2');
        cy.get('.pagination-current').should('contain', '2');
      }
      
      // Verify the back button returns to pipeline details
      cy.get('.back-button').click();
      cy.url().should('include', `/pipelines/${pipeline.pipeline_id}`);
    });
  });
  
  it('should view pipeline execution details', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const pipeline = data.pipelineListResponse.items[0];
      
      // Mock the pipeline execution API endpoints
      cy.intercept('GET', `/api/pipelines/${pipeline.pipeline_id}/executions*`, {
        statusCode: 200,
        body: data.pipelineExecutionListResponse
      }).as('getPipelineExecutions');
      
      // Mock the pipeline execution details API endpoint
      cy.intercept('GET', '/api/executions/*', (req) => {
        const executionId = req.url.split('/').pop().split('?')[0];
        const execution = data.pipelineExecutionListResponse.items.find(e => e.execution_id === executionId);
        
        if (execution) {
          req.reply({
            statusCode: 200,
            body: execution
          });
        } else {
          req.reply({
            statusCode: 404,
            body: { error: 'Not Found', message: 'Execution not found' }
          });
        }
      }).as('getExecution');
      
      // Mock the task executions API endpoint
      cy.intercept('GET', '/api/executions/*/tasks*', {
        statusCode: 200,
        body: data.taskExecutionListResponse
      }).as('getTaskExecutions');
      
      // Navigate to execution history for a pipeline
      cy.get('table.pipeline-table tbody tr:first .history-button').click();
      cy.wait('@getPipelineExecutions');
      
      // Click on an execution in the history table
      cy.get('table.execution-history-table tbody tr:first').click();
      
      // Verify URL contains the execution ID
      const firstExecution = data.pipelineExecutionListResponse.items[0];
      cy.url().should('include', `/executions/${firstExecution.execution_id}`);
      
      // Verify execution details page is displayed
      cy.wait('@getExecution');
      
      cy.get('h1').should('contain', 'Execution Details');
      
      // Verify execution status and metrics are displayed correctly
      verifyExecutionDetails(firstExecution);
      
      // Verify the task executions tab shows task data
      cy.get('.tasks-tab').click();
      cy.wait('@getTaskExecutions');
      
      cy.get('table.task-execution-table').should('be.visible');
      cy.get('table.task-execution-table tbody tr').should('have.length', 
        data.taskExecutionListResponse.items.length);
      
      // Verify the DAG visualization tab displays correctly
      cy.get('.dag-tab').click();
      cy.get('.dag-visualization').should('be.visible');
      
      // Verify self-healing attempts are displayed if present
      if (firstExecution.self_healing_attempts && firstExecution.self_healing_attempts.length > 0) {
        cy.get('.self-healing-tab').click();
        cy.get('.self-healing-attempts-list').should('be.visible');
        cy.get('.self-healing-attempts-list .attempt').should('have.length', 
          firstExecution.self_healing_attempts.length);
          
        firstExecution.self_healing_attempts.forEach((attempt, index) => {
          cy.get(`.self-healing-attempts-list .attempt:eq(${index}) .issue-type`)
            .should('contain', attempt.issue_type);
          cy.get(`.self-healing-attempts-list .attempt:eq(${index}) .action-taken`)
            .should('contain', attempt.action_taken);
          cy.get(`.self-healing-attempts-list .attempt:eq(${index}) .confidence-score`)
            .should('contain', attempt.details.confidence_score);
        });
      }
      
      // Verify the back button returns to execution history
      cy.get('.back-button').click();
      cy.url().should('include', `/pipelines/${pipeline.pipeline_id}/history`);
    });
  });
  
  it('should run a pipeline manually', () => {
    cy.fixture('pipeline_data.json').then((data) => {
      const pipeline = data.pipelineListResponse.items[0];
      
      // Mock the run pipeline API endpoint
      cy.intercept('POST', `/api/pipelines/${pipeline.pipeline_id}/run`, {
        statusCode: 200,
        body: data.apiResponses.runPipeline.success.body
      }).as('runPipeline');
      
      // Navigate to pipeline details for a pipeline
      cy.get('table.pipeline-table tbody tr:first').click();
      cy.wait('@getPipeline');
      
      // Click the run now button
      cy.get('.run-pipeline-button').click();
      
      // Verify the API was called with the correct pipeline ID
      cy.wait('@runPipeline').then((interception) => {
        expect(interception.request.url).to.include(pipeline.pipeline_id);
      });
      
      // Verify success notification is displayed
      cy.get('.notification-success').should('be.visible');
      cy.get('.notification-success').should('contain', 'Pipeline execution started');
      
      // Verify the pipeline status updates to show running state
      cy.get('.pipeline-details-status').should('contain', 'RUNNING');
    });
  });
  
  it('should handle API errors gracefully', () => {
    // Mock the pipeline API to return an error
    cy.intercept('GET', '/api/pipelines*', {
      statusCode: 500,
      body: {
        error: 'Internal Server Error',
        message: 'An unexpected error occurred while fetching pipelines'
      }
    }).as('getPipelinesError');
    
    // Attempt to load the pipeline management page
    cy.reload();
    cy.wait('@getPipelinesError');
    
    // Verify error notification is displayed
    cy.get('.notification-error').should('be.visible');
    cy.get('.notification-error').should('contain', 'An unexpected error occurred');
    
    // Verify the UI handles the error gracefully
    cy.get('.error-state').should('be.visible');
    cy.get('.retry-button').should('be.visible');
    
    // Reset the mock to return valid data
    cy.fixture('pipeline_data.json').then((data) => {
      cy.intercept('GET', '/api/pipelines*', {
        statusCode: 200,
        body: data.pipelineListResponse
      }).as('getPipelines');
    });
    
    // Mock the create pipeline API to return a validation error
    cy.intercept('POST', '/api/pipelines', {
      statusCode: 400,
      body: {
        error: 'Validation Error',
        message: 'Invalid pipeline data',
        details: ['Pipeline name is required', 'Invalid source configuration']
      }
    }).as('createPipelineError');
    
    // Click the retry button
    cy.get('.retry-button').click();
    cy.wait('@getPipelines');
    
    // Attempt to create a pipeline with invalid data
    cy.get('.create-pipeline-button').click();
    cy.get('.pipeline-form-submit').click();
    
    // Wait for the API call
    cy.wait('@createPipelineError');
    
    // Verify validation error messages are displayed
    cy.get('.form-validation-error').should('be.visible');
    cy.get('.form-validation-error').should('contain', 'Pipeline name is required');
    cy.get('.form-validation-error').should('contain', 'Invalid source configuration');
    
    // Verify the form remains open with entered data
    cy.get('.create-pipeline-modal').should('be.visible');
  });
});