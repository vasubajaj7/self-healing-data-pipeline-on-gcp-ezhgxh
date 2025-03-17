import React from 'react'; // react ^18.2.0
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { jest } from '@jest/globals'; // @jest/globals ^29.5.0
import PipelineInventory from '../../../../web/src/components/pipeline/PipelineInventory';
import PipelineDetailsCard from '../../../../web/src/components/pipeline/PipelineDetailsCard';
import { renderWithTheme } from '../../../fixtures/web/component_fixtures';
import { mockPipelineService, MOCK_PIPELINE_DATA } from '../../../fixtures/web/api_fixtures';
import { renderComponent, createUserEvent, findTableRowByText } from '../../../utils/web_test_utils';
import { PipelineDefinition, PipelineStatus } from '../../../../web/src/types/api';

/**
 * Sets up the test environment with mocks and fixtures
 */
const setup = () => {
  // LD1: Mock the pipeline service API calls
  const pipelineService = mockPipelineService();

  // LD1: Create mock pipeline data for testing
  const mockPipelines = MOCK_PIPELINE_DATA.definitions;

  // LD1: Return the mocked services and data for use in tests
  return {
    pipelineService,
    mockPipelines,
  };
};

/**
 * Tests for the PipelineInventory component
 */
describe('PipelineInventory', () => {
  /**
   * Tests that the component renders with pipeline data correctly
   */
  test('renders pipeline inventory with data', async () => {
    // LD1: Set up test with mock pipeline data
    const { mockPipelines } = setup();

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Verify that pipeline names are displayed in the table
    mockPipelines.forEach((pipeline) => {
      expect(screen.getByText(pipeline.pipelineName)).toBeInTheDocument();
    });

    // LD1: Verify that pipeline statuses are displayed correctly
    mockPipelines.forEach((pipeline) => {
      expect(screen.getByText(pipeline.lastExecutionStatus as string)).toBeInTheDocument();
    });

    // LD1: Verify that action buttons are rendered
    const actionButtons = screen.getAllByRole('button');
    expect(actionButtons.length).toBeGreaterThan(0);
  });

  /**
   * Tests that the search functionality filters pipelines correctly
   */
  test('handles search filtering', async () => {
    // LD1: Set up test with mock pipeline data
    const { pipelineService } = setup();

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Enter a search term in the search input
    const searchInput = screen.getByLabelText('Search');
    fireEvent.change(searchInput, { target: { value: 'Analytics' } });

    // LD1: Verify that the pipeline list is filtered according to the search term
    expect(screen.getByText('Analytics Daily')).toBeInTheDocument();
    expect(() => screen.getByText('Customer Data')).toThrow();

    // LD1: Verify that the API is called with the correct search parameters
    await waitFor(() => {
      expect(pipelineService.getPipelines).toHaveBeenCalledWith(
        expect.objectContaining({ searchTerm: 'Analytics' })
      );
    });
  });

  /**
   * Tests that the status filter dropdown filters pipelines correctly
   */
  test('handles status filtering', async () => {
    // LD1: Set up test with mock pipeline data
    const { pipelineService } = setup();

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Select a status from the status filter dropdown
    const statusFilterDropdown = screen.getByRole('button', { name: 'All Statuses' });
    fireEvent.click(statusFilterDropdown);
    const warningOption = screen.getByRole('option', { name: 'Warning' });
    fireEvent.click(warningOption);

    // LD1: Verify that the pipeline list is filtered according to the selected status
    expect(screen.getByText('Customer Data')).toBeInTheDocument();
    expect(() => screen.getByText('Analytics Daily')).toThrow();

    // LD1: Verify that the API is called with the correct status filter parameters
    await waitFor(() => {
      expect(pipelineService.getPipelines).toHaveBeenCalledWith(
        expect.objectContaining({ status: 'WARNING' })
      );
    });
  });

  /**
   * Tests that pagination controls work correctly
   */
  test('handles pagination', async () => {
    // LD1: Set up test with mock pipeline data spanning multiple pages
    const { pipelineService } = setup();
    pipelineService.getPipelines.mockResolvedValue(
      Promise.resolve({
        items: MOCK_PIPELINE_DATA.definitions.slice(0, 2),
        pagination: {
          page: 1,
          pageSize: 2,
          totalItems: MOCK_PIPELINE_DATA.definitions.length,
          totalPages: 2,
          nextPage: '/pipelines?page=2&pageSize=2',
          previousPage: null,
        },
        status: 'SUCCESS',
        message: 'Pipelines retrieved successfully',
        metadata: {
          timestamp: new Date().toISOString(),
          requestId: 'test-request-id',
        },
      })
    );

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Click on the next page button
    const nextPageButton = screen.getByRole('button', { name: 'Go to next page' });
    fireEvent.click(nextPageButton);

    // LD1: Verify that the API is called with the correct page parameters
    await waitFor(() => {
      expect(pipelineService.getPipelines).toHaveBeenCalledWith(
        expect.objectContaining({ page: 2, pageSize: 10 })
      );
    });

    // LD1: Verify that the displayed data changes to show the next page
    expect(screen.getByText('Product Enrich')).toBeInTheDocument();
  });

  /**
   * Tests that clicking on a pipeline row calls the onSelectPipeline callback
   */
  test('handles row click', async () => {
    // LD1: Set up test with mock pipeline data
    const { mockPipelines } = setup();

    // LD1: Create a mock onSelectPipeline callback function
    const onSelectPipeline = jest.fn();

    // LD1: Render the PipelineInventory component with the mock callback
    renderComponent(<PipelineInventory onSelectPipeline={onSelectPipeline} />);

    // LD1: Click on a pipeline row
    const tableRow = findTableRowByText('Analytics Daily');
    fireEvent.click(tableRow);

    // LD1: Verify that the onSelectPipeline callback is called with the correct pipeline data
    expect(onSelectPipeline).toHaveBeenCalledWith(mockPipelines[0]);
  });

  /**
   * Tests that action buttons trigger the correct callbacks
   */
  test('handles action button clicks', async () => {
    // LD1: Set up test with mock pipeline data
    const { mockPipelines } = setup();

    // LD1: Create mock callback functions for edit, view, history, and delete actions
    const onEditPipeline = jest.fn();
    const onViewHistory = jest.fn();
    const onDeletePipeline = jest.fn();

    // LD1: Render the PipelineInventory component with the mock callbacks
    renderComponent(
      <PipelineInventory
        onEditPipeline={onEditPipeline}
        onViewHistory={onViewHistory}
        onCreatePipeline={() => {}}
      />
    );

    // LD1: Click on each action button for a pipeline
    const tableRow = findTableRowByText('Analytics Daily');
    const viewButton = within(tableRow).getByRole('button', { name: 'View' });
    const editButton = within(tableRow).getByRole('button', { name: 'Edit' });
    const historyButton = within(tableRow).getByRole('button', { name: 'History' });
    const deleteButton = within(tableRow).getByRole('button', { name: 'Delete' });

    fireEvent.click(viewButton);
    fireEvent.click(editButton);
    fireEvent.click(historyButton);
    fireEvent.click(deleteButton);

    // LD1: Verify that each callback is called with the correct pipeline data
    expect(onEditPipeline).toHaveBeenCalledWith(mockPipelines[0]);
    expect(onViewHistory).toHaveBeenCalledWith(mockPipelines[0]);
  });

  /**
   * Tests the delete confirmation dialog functionality
   */
  test('handles delete confirmation', async () => {
    // LD1: Set up test with mock pipeline data
    const { pipelineService } = setup();

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Click on the delete button for a pipeline
    const tableRow = findTableRowByText('Analytics Daily');
    const deleteButton = within(tableRow).getByRole('button', { name: 'Delete' });
    fireEvent.click(deleteButton);

    // LD1: Verify that the confirmation dialog appears
    const confirmDialog = screen.getByRole('dialog', { name: 'Confirm Delete' });
    expect(confirmDialog).toBeInTheDocument();

    // LD1: Click on the confirm button
    const confirmButton = within(confirmDialog).getByRole('button', { name: 'Delete' });
    fireEvent.click(confirmButton);

    // LD1: Verify that the delete API is called with the correct pipeline ID
    await waitFor(() => {
      expect(pipelineService.deletePipeline).toHaveBeenCalledWith('pipeline-1');
    });

    // LD1: Verify that a success notification is shown
    await waitFor(() => {
      expect(screen.getByText('Pipeline deleted successfully.')).toBeInTheDocument();
    });
  });

  /**
   * Tests that loading indicators are displayed correctly
   */
  test('handles loading state', async () => {
    // LD1: Set up test with mock pipeline service that returns loading state
    const { pipelineService } = setup();
    pipelineService.getPipelines.mockResolvedValue(
      new Promise(() => {}) as any
    );

    // LD1: Render the PipelineInventory component
    renderComponent(<PipelineInventory />);

    // LD1: Verify that loading indicators are displayed
    expect(screen.getByText('Loading...')).toBeInTheDocument();
  });
});

/**
 * Tests for the PipelineDetailsCard component
 */
describe('PipelineDetailsCard', () => {
  /**
   * Tests that the component renders pipeline details correctly
   */
  test('renders pipeline details correctly', () => {
    // LD1: Set up test with mock pipeline data
    const { mockPipelines } = setup();

    // LD1: Render the PipelineDetailsCard component with a pipeline
    renderComponent(<PipelineDetailsCard pipeline={mockPipelines[0]} />);

    // LD1: Verify that the pipeline name is displayed as the title
    expect(screen.getByText(mockPipelines[0].pipelineName)).toBeInTheDocument();

    // LD1: Verify that the pipeline status is displayed correctly
    expect(screen.getByText(mockPipelines[0].lastExecutionStatus as string)).toBeInTheDocument();

    // LD1: Verify that pipeline metadata (source, target, dates) is displayed
    expect(screen.getByText(`Source:`)).toBeInTheDocument();
    expect(screen.getByText(`Target Dataset:`)).toBeInTheDocument();
    expect(screen.getByText(`Target Table:`)).toBeInTheDocument();
    expect(screen.getByText(`Created:`)).toBeInTheDocument();
    expect(screen.getByText(`Last Updated:`)).toBeInTheDocument();

    // LD1: Verify that the pipeline description is displayed
    expect(screen.getByText(`Description:`)).toBeInTheDocument();
  });

  /**
   * Tests that status chips have the correct color based on status
   */
  test('displays correct status chip color', () => {
    // LD1: Set up test with mock pipelines having different statuses
    const healthyPipeline = { ...MOCK_PIPELINE_DATA.definitions[0], lastExecutionStatus: PipelineStatus.HEALTHY };
    const warningPipeline = { ...MOCK_PIPELINE_DATA.definitions[1], lastExecutionStatus: PipelineStatus.WARNING };
    const errorPipeline = { ...MOCK_PIPELINE_DATA.definitions[2], lastExecutionStatus: PipelineStatus.ERROR };
    const inactivePipeline = { ...MOCK_PIPELINE_DATA.definitions[0], lastExecutionStatus: PipelineStatus.INACTIVE };

    // LD1: Render the PipelineDetailsCard component for each status
    const { rerender } = renderComponent(<PipelineDetailsCard pipeline={healthyPipeline} />);
    const healthyChip = screen.getByText(PipelineStatus.HEALTHY);
    expect(healthyChip).toBeInTheDocument();

    rerenderComponent(<PipelineDetailsCard pipeline={warningPipeline} />);
    const warningChip = screen.getByText(PipelineStatus.WARNING);
    expect(warningChip).toBeInTheDocument();

    rerenderComponent(<PipelineDetailsCard pipeline={errorPipeline} />);
    const errorChip = screen.getByText(PipelineStatus.ERROR);
    expect(errorChip).toBeInTheDocument();

    rerenderComponent(<PipelineDetailsCard pipeline={inactivePipeline} />);
    const inactiveChip = screen.getByText(PipelineStatus.INACTIVE);
    expect(inactiveChip).toBeInTheDocument();
  });

  /**
   * Tests that action buttons trigger the correct callbacks
   */
  test('handles action button clicks', () => {
    // LD1: Set up test with mock pipeline data
    const { mockPipelines } = setup();

    // LD1: Create mock callback functions for run, edit, and delete actions
    const onRun = jest.fn();
    const onEdit = jest.fn();
    const onDelete = jest.fn();

    // LD1: Render the PipelineDetailsCard component with the mock callbacks
    renderComponent(
      <PipelineDetailsCard
        pipeline={mockPipelines[0]}
        onEdit={onEdit}
        onDelete={onDelete}
      />
    );

    // LD1: Click on each action button
    const runButton = screen.getByRole('button', { name: 'Run Now' });
    const editButton = screen.getByRole('button', { name: 'Edit' });
    const deleteButton = screen.getByRole('button', { name: 'Delete' });

    fireEvent.click(runButton);
    fireEvent.click(editButton);
    fireEvent.click(deleteButton);

    // LD1: Verify that each callback is called with the correct parameters
    expect(onEdit).toHaveBeenCalled();
    expect(onDelete).toHaveBeenCalled();
  });

  /**
   * Tests the run pipeline functionality
   */
  test('handles run pipeline action', async () => {
    // LD1: Set up test with mock pipeline data and pipeline service
    const { mockPipelines, pipelineService } = setup();

    // LD1: Render the PipelineDetailsCard component
    renderComponent(<PipelineDetailsCard pipeline={mockPipelines[0]} />);

    // LD1: Click on the run button
    const runButton = screen.getByRole('button', { name: 'Run Now' });
    fireEvent.click(runButton);

    // LD1: Verify that the runPipeline API is called with the correct pipeline ID
    expect(pipelineService.executePipeline).toHaveBeenCalledWith(mockPipelines[0].pipelineId);

    // LD1: Verify that the button shows loading state during the API call
    expect(runButton).toHaveAttribute('aria-busy', 'true');

    // LD1: Verify that a success notification is shown after successful run
    await waitFor(() => {
      expect(screen.getByText('Loading...')).not.toBeInTheDocument();
    });
  });

  /**
   * Tests that loading indicators are displayed correctly
   */
  test('handles loading state', () => {
    // LD1: Set up test with loading prop set to true
    const { mockPipelines } = setup();

    // LD1: Render the PipelineDetailsCard component
    const { rerender } = renderComponent(<PipelineDetailsCard pipeline={mockPipelines[0]} loading={true} />);

    // LD1: Verify that loading indicators are displayed instead of content
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    // LD1: Update the component with loading set to false
    rerenderComponent(<PipelineDetailsCard pipeline={mockPipelines[0]} loading={false} />);

    // LD1: Verify that loading indicators are replaced with content
    expect(screen.getByText(mockPipelines[0].pipelineName)).toBeInTheDocument();
  });
});