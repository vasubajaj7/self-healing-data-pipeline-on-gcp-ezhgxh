import React, { ReactElement } from 'react'; // React ^18.2.0
import { render, RenderOptions, RenderResult, waitForElementToBeRemoved as tlWaitForElementToBeRemoved } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { ThemeContextProvider } from '../../contexts/ThemeContext';

/**
 * Renders a React component with theme context for testing
 * @param ui - The React component to render
 * @param options - Additional render options
 * @returns The render result with additional helper methods
 */
export const renderWithTheme = (
  ui: ReactElement,
  options?: Partial<RenderOptions>
): RenderResult => {
  return render(
    <ThemeContextProvider>{ui}</ThemeContextProvider>,
    options
  );
};

/**
 * Mocks the ResizeObserver API for testing environments
 */
export const mockResizeObserver = (): void => {
  global.ResizeObserver = class ResizeObserver {
    observe = jest.fn();
    unobserve = jest.fn();
    disconnect = jest.fn();
  };
};

/**
 * Mocks the IntersectionObserver API for testing environments
 */
export const mockIntersectionObserver = (): void => {
  global.IntersectionObserver = class IntersectionObserver {
    observe = jest.fn();
    unobserve = jest.fn();
    disconnect = jest.fn();
    root = null;
    rootMargin = '';
    thresholds = [];
  };
};

/**
 * Creates a mock implementation of window.matchMedia for testing
 * @param matches - Whether the media query should match
 * @returns A mock implementation of matchMedia
 */
export const createMockMediaQuery = (matches: boolean): Function => {
  return (query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: jest.fn(), // deprecated but needed for some tests
    removeListener: jest.fn(), // deprecated but needed for some tests
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  });
};

/**
 * Utility wrapper around Testing Library's waitForElementToBeRemoved with default options
 * @param callback - Element or callback that returns an element
 * @param options - Options for waitForElementToBeRemoved
 * @returns Promise that resolves when element is removed
 */
export const waitForElementToBeRemoved = (
  callback: Element | (() => Element),
  options?: object
): Promise<void> => {
  return tlWaitForElementToBeRemoved(callback, {
    timeout: 5000, // 5 seconds default timeout
    ...options,
  });
};