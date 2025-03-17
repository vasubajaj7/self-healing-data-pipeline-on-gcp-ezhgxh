import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Table from '../Table';
import { renderWithProviders } from '../../../test/utils/renderWithProviders';

describe('Table', () => {
  // Test data setup
  const testColumns = [
    { id: 'id', label: 'ID', numeric: true, sortable: true },
    { id: 'name', label: 'Name', sortable: true },
    { id: 'status', label: 'Status' },
    { id: 'date', label: 'Date', sortable: true },
  ];

  const testData = [
    { id: 1, name: 'Item 1', status: 'Active', date: '2023-01-01' },
    { id: 2, name: 'Item 2', status: 'Inactive', date: '2023-01-02' },
    { id: 3, name: 'Item 3', status: 'Active', date: '2023-01-03' },
  ];

  // Mock function setup
  let mockOnSortChange;
  let mockOnPageChange;
  let mockOnSelectionChange;
  let mockOnRowClick;
  let mockFormat;
  let mockRenderCell;

  beforeEach(() => {
    // Reset mock functions before each test
    mockOnSortChange = jest.fn();
    mockOnPageChange = jest.fn();
    mockOnSelectionChange = jest.fn();
    mockOnRowClick = jest.fn();
    mockFormat = jest.fn(value => `Formatted: ${value}`);
    mockRenderCell = jest.fn((value, row, index) => (
      <span data-testid={`custom-${row.id}`}>{value}</span>
    ));
  });

  it('renders table with columns and data', () => {
    renderWithProviders(<Table columns={testColumns} data={testData} />);
    
    // Verify table is in the document
    const table = screen.getByRole('table');
    expect(table).toBeInTheDocument();
    
    // Verify column headers
    testColumns.forEach(column => {
      expect(screen.getByText(column.label)).toBeInTheDocument();
    });
    
    // Verify rows - check each cell in each row
    testData.forEach(row => {
      Object.values(row).forEach(cellValue => {
        expect(screen.getByText(cellValue.toString())).toBeInTheDocument();
      });
    });
  });

  it('renders table with empty data', () => {
    renderWithProviders(<Table columns={testColumns} data={[]} />);
    
    // Verify empty message is displayed
    expect(screen.getByText(/no data available/i)).toBeInTheDocument();
    
    // Verify table is not rendered
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it('renders table with custom empty message', () => {
    const customEmptyMessage = 'No items found';
    renderWithProviders(<Table columns={testColumns} data={[]} emptyMessage={customEmptyMessage} />);
    
    // Verify custom empty message is displayed
    expect(screen.getByText(customEmptyMessage)).toBeInTheDocument();
  });

  it('renders table with loading state', () => {
    renderWithProviders(<Table columns={testColumns} data={testData} loading={true} />);
    
    // Verify loading spinner is displayed
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
    
    // Verify table is not rendered while loading
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it('renders table with error state', () => {
    const errorMessage = 'Error loading data';
    renderWithProviders(<Table columns={testColumns} data={testData} error={errorMessage} />);
    
    // Verify error message is displayed
    expect(screen.getByText(errorMessage)).toBeInTheDocument();
    
    // Verify table is not rendered when error occurs
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it('handles sorting when column is clicked', async () => {
    renderWithProviders(<Table columns={testColumns} data={testData} />);
    
    // Get the Name column header which is sortable
    const nameColumnHeader = screen.getByText('Name');
    
    // Click to sort ascending
    fireEvent.click(nameColumnHeader);
    
    // Check data order (should be ascending by name)
    const rows = screen.getAllByRole('row');
    // Skip header row (index 0)
    expect(within(rows[1]).getByText('Item 1')).toBeInTheDocument();
    expect(within(rows[2]).getByText('Item 2')).toBeInTheDocument();
    expect(within(rows[3]).getByText('Item 3')).toBeInTheDocument();
    
    // Click again to sort descending
    fireEvent.click(nameColumnHeader);
    
    // Wait for re-render
    await waitFor(() => {
      const updatedRows = screen.getAllByRole('row');
      // Data should now be in reverse order
      expect(within(updatedRows[1]).getByText('Item 3')).toBeInTheDocument();
      expect(within(updatedRows[2]).getByText('Item 2')).toBeInTheDocument();
      expect(within(updatedRows[3]).getByText('Item 1')).toBeInTheDocument();
    });
  });

  it('handles custom sort function', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData} 
        onSortChange={mockOnSortChange}
      />
    );
    
    // Get the Name column header which is sortable
    const nameColumnHeader = screen.getByText('Name');
    
    // Click to sort
    fireEvent.click(nameColumnHeader);
    
    // Verify onSortChange was called with correct parameters
    expect(mockOnSortChange).toHaveBeenCalledWith('name', 'asc');
    
    // Click again to sort descending
    fireEvent.click(nameColumnHeader);
    
    // Verify onSortChange was called with updated parameters
    expect(mockOnSortChange).toHaveBeenCalledWith('name', 'desc');
  });

  it('handles pagination', () => {
    // Create larger test data to test pagination
    const largeTestData = Array.from({ length: 25 }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      status: i % 2 === 0 ? 'Active' : 'Inactive',
      date: `2023-01-${String(i + 1).padStart(2, '0')}`,
    }));
    
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={largeTestData}
        initialPageSize={10}
      />
    );
    
    // Check that pagination controls are displayed
    expect(screen.getByText(/1-10 of 25/i)).toBeInTheDocument();
    
    // Verify only first 10 items are displayed
    expect(screen.getByText('Item 1')).toBeInTheDocument();
    expect(screen.getByText('Item 10')).toBeInTheDocument();
    expect(screen.queryByText('Item 11')).not.toBeInTheDocument();
    
    // Navigate to next page
    fireEvent.click(screen.getByRole('button', { name: /next page/i }));
    
    // Verify items 11-20 are now displayed
    expect(screen.getByText(/11-20 of 25/i)).toBeInTheDocument();
    expect(screen.getByText('Item 11')).toBeInTheDocument();
    expect(screen.getByText('Item 20')).toBeInTheDocument();
    expect(screen.queryByText('Item 10')).not.toBeInTheDocument();
  });

  it('handles page size change', async () => {
    // Create larger test data to test pagination
    const largeTestData = Array.from({ length: 30 }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      status: i % 2 === 0 ? 'Active' : 'Inactive',
      date: `2023-01-${String(i + 1).padStart(2, '0')}`,
    }));
    
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={largeTestData}
        initialPageSize={10}
        pageSizeOptions={[5, 10, 20, 30]}
      />
    );
    
    // Check current page size display
    expect(screen.getByText(/1-10 of 30/i)).toBeInTheDocument();
    
    // Find the page size select element
    const select = screen.getByLabelText(/items per page/i);
    
    // Change page size to 20
    fireEvent.change(select, { target: { value: '20' } });
    
    // Wait for the table to update
    await waitFor(() => {
      expect(screen.getByText(/1-20 of 30/i)).toBeInTheDocument();
    });
    
    // Verify 20 items are now displayed
    expect(screen.getByText('Item 1')).toBeInTheDocument();
    expect(screen.getByText('Item 20')).toBeInTheDocument();
    expect(screen.queryByText('Item 21')).not.toBeInTheDocument();
  });

  it('handles custom pagination', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        onPageChange={mockOnPageChange}
        totalItems={100}
      />
    );
    
    // Navigate to next page
    fireEvent.click(screen.getByRole('button', { name: /next page/i }));
    
    // Verify onPageChange was called with correct parameters
    expect(mockOnPageChange).toHaveBeenCalledWith(2, 10); // Default page size is 10
  });

  it('handles row selection', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        selectable={true}
        onSelectionChange={mockOnSelectionChange}
      />
    );
    
    // Verify checkbox in header is displayed
    const headerCheckbox = screen.getAllByRole('checkbox')[0];
    expect(headerCheckbox).toBeInTheDocument();
    
    // Verify checkboxes in rows are displayed
    const checkboxes = screen.getAllByRole('checkbox');
    expect(checkboxes.length).toBe(testData.length + 1); // +1 for header checkbox
    
    // Select a row
    fireEvent.click(checkboxes[1]); // First row checkbox
    
    // Verify onSelectionChange was called with selected row
    expect(mockOnSelectionChange).toHaveBeenCalledWith([testData[0]]);
    
    // Select all rows
    fireEvent.click(headerCheckbox);
    
    // Verify onSelectionChange was called with all rows
    expect(mockOnSelectionChange).toHaveBeenCalledWith(testData);
  });

  it('renders with custom cell formatting', () => {
    const columnsWithFormat = [
      ...testColumns.slice(0, -1),
      { ...testColumns[3], format: mockFormat } // Add format function to date column
    ];
    
    renderWithProviders(<Table columns={columnsWithFormat} data={testData} />);
    
    // Verify formatted cells are displayed
    testData.forEach(row => {
      expect(screen.getByText(`Formatted: ${row.date}`)).toBeInTheDocument();
    });
    
    // Verify format function was called for each row with correct arguments
    testData.forEach(row => {
      expect(mockFormat).toHaveBeenCalledWith(row.date, row);
    });
  });

  it('renders with custom cell rendering', () => {
    const columnsWithRenderCell = [
      ...testColumns.slice(0, -1),
      { ...testColumns[3], renderCell: mockRenderCell } // Add renderCell function to date column
    ];
    
    renderWithProviders(<Table columns={columnsWithRenderCell} data={testData} />);
    
    // Verify custom rendered cells are displayed
    testData.forEach(row => {
      expect(screen.getByTestId(`custom-${row.id}`)).toBeInTheDocument();
      expect(screen.getByTestId(`custom-${row.id}`).textContent).toBe(row.date);
    });
    
    // Verify renderCell function was called for each row with correct arguments
    testData.forEach((row, index) => {
      expect(mockRenderCell).toHaveBeenCalledWith(row.date, row, index);
    });
  });

  it('handles row click events', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        onRowClick={mockOnRowClick}
      />
    );
    
    // Get table rows (skip header row at index 0)
    const rows = screen.getAllByRole('row').slice(1);
    
    // Click on first row
    fireEvent.click(rows[0]);
    
    // Verify onRowClick was called with correct row data and index
    expect(mockOnRowClick).toHaveBeenCalledWith(testData[0], 0);
  });

  it('renders with sticky header', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        stickyHeader={true}
      />
    );
    
    // Get the table element
    const table = screen.getByRole('table');
    
    // Verify stickyHeader attribute is set
    expect(table).toHaveAttribute('stickyHeader', 'true');
  });

  it('renders with dense layout', () => {
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        dense={true}
      />
    );
    
    // The pagination component should have the compact class when dense is true
    const paginationContainer = screen.getByText(/1-3 of 3/i).closest('div');
    expect(paginationContainer).toHaveClass('compact');
    
    // Additionally we could check the rows have reduced padding but this is harder to test
  });

  it('renders with title and subtitle', () => {
    const title = 'Test Table';
    const subtitle = 'Table subtitle';
    
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        title={title}
        subtitle={subtitle}
      />
    );
    
    // Verify title and subtitle are displayed
    expect(screen.getByText(title)).toBeInTheDocument();
    expect(screen.getByText(subtitle)).toBeInTheDocument();
  });

  it('renders with custom actions', () => {
    const mockActions = <button data-testid="custom-action">Custom Action</button>;
    
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        actions={mockActions}
      />
    );
    
    // Verify custom actions are displayed in the toolbar
    expect(screen.getByTestId('custom-action')).toBeInTheDocument();
  });

  it('handles keyboard navigation', async () => {
    const user = userEvent.setup();
    
    renderWithProviders(
      <Table 
        columns={testColumns} 
        data={testData}
        selectable={true}
      />
    );
    
    // Initially, focus is on the document body
    expect(document.body).toHaveFocus();
    
    // Tab to first focusable element (header checkbox)
    await user.tab();
    const firstCheckbox = screen.getAllByRole('checkbox')[0];
    expect(firstCheckbox).toHaveFocus();
    
    // Tab to the first sortable column header
    await user.tab();
    const sortableHeaders = screen.getAllByRole('button').filter(
      button => button.getAttribute('aria-sort') !== null
    );
    expect(sortableHeaders[0]).toHaveFocus();
    
    // Tab to the next sortable column header
    await user.tab();
    expect(sortableHeaders[1]).toHaveFocus();
    
    // Tab to pagination controls
    for (let i = 0; i < 3; i++) {
      await user.tab();
    }
    
    // Should now focus on previous page button
    const prevPageButton = screen.getByRole('button', { name: /previous page/i });
    expect(prevPageButton).toHaveFocus();
    
    // Tab to next page button
    await user.tab();
    const nextPageButton = screen.getByRole('button', { name: /next page/i });
    expect(nextPageButton).toHaveFocus();
  });
});