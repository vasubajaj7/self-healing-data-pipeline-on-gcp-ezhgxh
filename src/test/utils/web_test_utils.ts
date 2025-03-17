import { ReactElement } from 'react'; // react ^18.2.0
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import { QueryClient } from 'react-query'; // react-query ^3.39.2
import {
  renderWithProviders,
  mockResizeObserver,
  mockIntersectionObserver,
  createMockMediaQuery,
} from '../web/src/test/utils/testUtils';

/**
 * Sets up all necessary browser API mocks for testing web components
 */
export const setupBrowserMocks = (): void => {
  // LD1: Call mockResizeObserver to mock the ResizeObserver API
  mockResizeObserver();
  // LD1: Call mockIntersectionObserver to mock the IntersectionObserver API
  mockIntersectionObserver();

  // LD1: Mock window.matchMedia using createMockMediaQuery
  window.matchMedia = createMockMediaQuery(false);

  // LD1: Set up any other browser APIs that need mocking for tests
};

/**
 * Options for rendering components in tests
 */
interface RenderOptions {
  route?: string;
  queryClient?: QueryClient;
  mockResponses?: Record<string, any>;
  providerProps?: object;
}

/**
 * Renders a React component with all necessary providers and browser mocks
 * @param ui - The React component to render
 * @param options - Additional render options
 * @returns Render result with additional helper methods
 */
export const renderComponent = (ui: ReactElement, options: RenderOptions = {}) => {
  // LD1: Set up browser mocks by calling setupBrowserMocks
  setupBrowserMocks();

  // LD1: Use renderWithProviders to render the component with all necessary context providers
  const renderResult = renderWithProviders(ui, options);

  // LD1: Return the render result with all testing utilities
  return renderResult;
};

/**
 * Creates a user event instance for simulating user interactions
 * @param options - Options for user event configuration
 * @returns UserEvent - User event instance for simulating interactions
 */
export const createUserEvent = (options: object = {}) => {
  // LD1: Create and return a userEvent instance with the provided options
  return userEvent.setup(options);
};

/**
 * Map of field names to values for filling forms
 */
interface FormFields {
  [fieldName: string]: string | number | boolean;
}

/**
 * Fills multiple form fields with provided values
 * @param fields - Map of field names to values
 * @returns Promise<void> - Promise that resolves when all fields are filled
 */
export const fillFormFields = async (fields: FormFields): Promise<void> => {
  // LD1: Create a user event instance
  const user = createUserEvent();

  // LD1: For each field in the fields object:
  for (const fieldName in fields) {
    if (fields.hasOwnProperty(fieldName)) {
      const value = fields[fieldName];

      // LD1: Find the input element by label text or test ID
      const inputElement = screen.getByRole('textbox', { name: fieldName });

      // LD1: Clear the existing value if any
      await user.clear(inputElement);

      // LD1: Type the new value using userEvent
      await user.type(inputElement, String(value));

      // LD1: Wait for the value to be applied
      await waitFor(() => expect(inputElement).toHaveValue(String(value)));
    }
  }
};

/**
 * Selects an option from a dropdown menu
 * @param dropdownLabel - Label text of the dropdown
 * @param optionText - Text of the option to select
 * @returns Promise<void> - Promise that resolves when option is selected
 */
export const selectDropdownOption = async (dropdownLabel: string, optionText: string): Promise<void> => {
  // LD1: Create a user event instance
  const user = createUserEvent();

  // LD1: Find the dropdown element by label text
  const dropdownElement = screen.getByLabelText(dropdownLabel);

  // LD1: Click the dropdown to open it
  await user.click(dropdownElement);

  // LD1: Find the option with the specified text
  const optionElement = screen.getByText(optionText);

  // LD1: Click the option to select it
  await user.click(optionElement);

  // LD1: Wait for the selection to be applied
  await waitFor(() => expect(dropdownElement).toHaveTextContent(optionText));
};

/**
 * Waits for loading indicators to disappear from the UI
 * @returns Promise<void> - Promise that resolves when loading is complete
 */
export const waitForLoadingToComplete = async (): Promise<void> => {
  // LD1: Look for common loading indicators (spinners, loading text, etc.)
  // LD1: Wait for them to be removed from the DOM
  // LD1: Return a promise that resolves when loading is complete
  await waitFor(() => {
    const loadingIndicators = screen.queryAllByText(/Loading|Please wait/i);
    expect(loadingIndicators).toHaveLength(0);
  });
};

/**
 * Finds a table row that contains the specified text
 * @param text - The text to search for within the table row
 * @param container - The container element to search within (optional)
 * @returns HTMLElement - The found table row element
 */
export const findTableRowByText = (text: string, container?: HTMLElement): HTMLElement => {
  // LD1: Search for table rows within the container (or document if not provided)
  const tableRows = (container || document).querySelectorAll('tr');

  // LD1: Filter rows to find those containing the specified text
  const matchingRow = Array.from(tableRows).find(row => row.textContent?.includes(text));

  // LD1: Return the first matching row or throw an error if none found
  if (!matchingRow) {
    throw new Error(`Table row with text "${text}" not found`);
  }

  return matchingRow;
};

/**
 * Finds a table cell at the specified column and row position
 * @param rowIndex - The index of the row (0-based)
 * @param columnIndex - The index of the column (0-based)
 * @param tableElement - The table element to search within
 * @returns HTMLElement - The found table cell element
 */
export const findTableCellByColumnAndRow = (rowIndex: number, columnIndex: number, tableElement: HTMLElement): HTMLElement => {
  // LD1: Get all rows in the table
  const rows = tableElement.querySelectorAll('tr');

  // LD1: Get the specified row by index
  const row = rows[rowIndex];

  // LD1: Get all cells in that row
  const cells = row.querySelectorAll('td, th');

  // LD1: Return the cell at the specified column index
  const cell = cells[columnIndex];
  return cell as HTMLElement;
};

/**
 * Creates a configured QueryClient for testing with predefined responses
 * @param mockResponses - Mock responses for API calls
 * @returns QueryClient - Configured QueryClient instance
 */
export const createMockQueryClient = (mockResponses: Record<string, any> = {}): QueryClient => {
  // LD1: Create a new QueryClient instance
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        // LD1: Configure default options for queries and mutations
        retry: false, // LD1: Set retry to false to avoid retries in tests
        cacheTime: 0, // LD1: Set cacheTime to a low value for testing
      },
      mutations: {
        retry: false, // LD1: Set retry to false to avoid retries in tests
      },
    },
  });

  // LD1: If mockResponses provided, set up query cache with predefined data
  Object.entries(mockResponses).forEach(([queryKey, data]) => {
    queryClient.setQueryData(queryKey, data);
  });

  // LD1: Return the configured QueryClient
  return queryClient;
};

/**
 * Creates a consistent test ID selector for finding elements
 * @param id - The base ID for the test element
 * @returns string - Data-testid selector string
 */
export const createTestId = (id: string): string => {
  // LD1: Format the provided ID into a data-testid selector string
  // LD1: Return the formatted selector
  return `[data-testid="${id}"]`;
};

// IE3: Re-export utility from web test utils for convenience
export { renderWithProviders };

// IE3: Re-export utility from web test utils for convenience
export { mockResizeObserver };

// IE3: Re-export utility from web test utils for convenience
export { mockIntersectionObserver };

// IE3: Re-export utility from web test utils for convenience
export { createMockMediaQuery };