import React, { ReactNode, useEffect } from 'react'; // react ^18.2.0
import { render, screen, waitFor, act } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { renderHook, waitForNextUpdate } from '@testing-library/react-hooks'; // @testing-library/react-hooks ^8.0.1
import { jest } from 'jest'; // jest ^29.5.0
import { useApi, useApiRequest } from '../../../../web/src/hooks/useApi';
import apiClient from '../../../../web/src/services/api/apiClient';
import { isAuthenticationError } from '../../../../web/src/utils/errorHandling';
import { renderWithProviders } from '../../../../web/src/test/utils/renderWithProviders';

// TestComponent: A test component that uses the useApi hook for testing
interface TestComponentProps {
  endpoint: string;
  method: 'get' | 'post' | 'put' | 'patch' | 'delete';
}

const TestComponent: React.FC<TestComponentProps> = ({ endpoint, method }) => {
  // Destructure endpoint and method from props
  // Get the API methods, loading state, and error state from useApi hook
  const { get, post, put, patch, delete: del, loading, error } = useApi();
  const [data, setData] = React.useState<any>(null);

  // Use React.useEffect to make the API call when the component mounts
  useEffect(() => {
    const fetchData = async () => {
      try {
        let response;
        switch (method) {
          case 'get':
            response = await get(endpoint);
            break;
          case 'post':
            response = await post(endpoint, { test: 'data' });
            break;
          case 'put':
            response = await put(endpoint, { test: 'data' });
            break;
          case 'patch':
            response = await patch(endpoint, { test: 'data' });
            break;
          case 'delete':
            response = await del(endpoint);
            break;
          default:
            throw new Error('Invalid method');
        }
        setData(response);
      } catch (e) {
        // Error is already handled by useApi hook
      }
    };

    fetchData();
  }, [endpoint, method, get, post, put, patch, del]);

  // Return a div that displays loading state, error message, and response data
  return (
    <div>
      {loading && <div data-testid="loading">Loading...</div>}
      {error && <div data-testid="error">{error.message}</div>}
      {data && <div data-testid="data">{JSON.stringify(data)}</div>}
    </div>
  );
};

// TestApiRequestComponent: A test component that uses the useApiRequest hook for testing
interface TestApiRequestComponentProps {
  endpoint: string;
  method: 'get' | 'post' | 'put' | 'patch' | 'delete';
}

const TestApiRequestComponent: React.FC<TestApiRequestComponentProps> = ({ endpoint, method }) => {
  // Destructure endpoint and method from props
  // Get the executeRequest function, data, loading state, error state, and reset function from useApiRequest hook
  const { executeRequest, data, loading, error, reset } = useApiRequest<any>();

  // Use React.useEffect to make the API call when the component mounts
  useEffect(() => {
    const fetchData = async () => {
      try {
        let requestFn;
        switch (method) {
          case 'get':
            requestFn = apiClient.get;
            break;
          case 'post':
            requestFn = apiClient.post;
            break;
          case 'put':
            requestFn = apiClient.put;
            break;
          case 'patch':
            requestFn = apiClient.patch;
            break;
          case 'delete':
            requestFn = apiClient.delete;
            break;
          default:
            throw new Error('Invalid method');
        }
        await executeRequest(requestFn, endpoint, { test: 'data' });
      } catch (e) {
        // Error is already handled by useApiRequest hook
      }
    };

    fetchData();
  }, [endpoint, method, executeRequest]);

  // Return a div that displays loading state, error message, response data, and a button to reset the state
  return (
    <div>
      {loading && <div data-testid="loading">Loading...</div>}
      {error && <div data-testid="error">{error.message}</div>}
      {data && <div data-testid="data">{JSON.stringify(data)}</div>}
      <button data-testid="reset-button" onClick={reset}>Reset</button>
    </div>
  );
};

// useApi hook: Tests for the useApi hook functionality
describe('useApi hook', () => {
  // should make successful GET request
  it('should make successful GET request', async () => {
    // Mock apiClient.get to return a successful response
    jest.spyOn(apiClient, 'get').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestComponent with GET method and test endpoint
    render(<TestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.get was called with the correct parameters
    expect(apiClient.get).toHaveBeenCalledWith('/test', undefined);
  });

  // should make successful POST request
  it('should make successful POST request', async () => {
    // Mock apiClient.post to return a successful response
    jest.spyOn(apiClient, 'post').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestComponent with POST method and test endpoint
    render(<TestComponent endpoint="/test" method="post" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.post was called with the correct parameters
    expect(apiClient.post).toHaveBeenCalledWith('/test', { test: 'data' }, undefined);
  });

  // should make successful PUT request
  it('should make successful PUT request', async () => {
    // Mock apiClient.put to return a successful response
    jest.spyOn(apiClient, 'put').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestComponent with PUT method and test endpoint
    render(<TestComponent endpoint="/test" method="put" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.put was called with the correct parameters
    expect(apiClient.put).toHaveBeenCalledWith('/test', { test: 'data' }, undefined);
  });

  // should make successful PATCH request
  it('should make successful PATCH request', async () => {
    // Mock apiClient.patch to return a successful response
    jest.spyOn(apiClient, 'patch').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestComponent with PATCH method and test endpoint
    render(<TestComponent endpoint="/test" method="patch" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.patch was called with the correct parameters
    expect(apiClient.patch).toHaveBeenCalledWith('/test', { test: 'data' }, undefined);
  });

  // should make successful DELETE request
  it('should make successful DELETE request', async () => {
    // Mock apiClient.delete to return a successful response
    jest.spyOn(apiClient, 'delete').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestComponent with DELETE method and test endpoint
    render(<TestComponent endpoint="/test" method="delete" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.delete was called with the correct parameters
    expect(apiClient.delete).toHaveBeenCalledWith('/test', undefined);
  });

  // should handle API error correctly
  it('should handle API error correctly', async () => {
    // Mock apiClient.get to reject with an error
    jest.spyOn(apiClient, 'get').mockRejectedValue(new Error('API Error'));
    // Render TestComponent with GET method and test endpoint
    render(<TestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('error'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the error message is displayed correctly
    expect(screen.getByTestId('error')).toHaveTextContent('API Error');
    // Verify that apiClient.get was called with the correct parameters
    expect(apiClient.get).toHaveBeenCalledWith('/test', undefined);
  });

  // should handle authentication error correctly
  it('should handle authentication error correctly', async () => {
    // Mock apiClient.get to reject with an error
    jest.spyOn(apiClient, 'get').mockRejectedValue(new Error('Authentication Error'));
    // Mock isAuthenticationError to return true
    jest.spyOn(isAuthenticationError, 'prototype', 'get').mockReturnValue(true);
    // Mock useAuth hook's logout function
    const logoutMock = jest.fn();
    jest.spyOn(require('../../../../web/src/hooks/useAuth'), 'useAuth').mockReturnValue({
      isAuthenticated: false,
      user: null,
      permissions: [],
      loading: false,
      error: null,
      login: jest.fn(),
      logout: logoutMock,
      verifyMfa: jest.fn(),
      refreshToken: jest.fn(),
      getUserProfile: jest.fn(),
      checkPermission: jest.fn(),
      checkRole: jest.fn(),
    });
    // Render TestComponent with GET method and test endpoint
    render(<TestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('error'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the error message is displayed correctly
    expect(screen.getByTestId('error')).toHaveTextContent('Authentication Error');
    // Verify that logout function was called
    expect(logoutMock).toHaveBeenCalled();
  });
});

// useApiRequest hook: Tests for the useApiRequest hook functionality
describe('useApiRequest hook', () => {
  // should make successful request when executeRequest is called
  it('should make successful request when executeRequest is called', async () => {
    // Mock apiClient.get to return a successful response
    jest.spyOn(apiClient, 'get').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestApiRequestComponent with GET method and test endpoint
    render(<TestApiRequestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Verify that apiClient.get was called with the correct parameters
    expect(apiClient.get).toHaveBeenCalledWith('/test', { test: 'data' });
  });

  // should handle error correctly
  it('should handle error correctly', async () => {
    // Mock apiClient.get to reject with an error
    jest.spyOn(apiClient, 'get').mockRejectedValue(new Error('API Error'));
    // Render TestApiRequestComponent with GET method and test endpoint
    render(<TestApiRequestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('error'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the error message is displayed correctly
    expect(screen.getByTestId('error')).toHaveTextContent('API Error');
    // Verify that apiClient.get was called with the correct parameters
    expect(apiClient.get).toHaveBeenCalledWith('/test', { test: 'data' });
  });

  // should reset state when reset function is called
  it('should reset state when reset function is called', async () => {
    // Mock apiClient.get to return a successful response
    jest.spyOn(apiClient, 'get').mockResolvedValue({ data: { message: 'Success' } });
    // Render TestApiRequestComponent with GET method and test endpoint
    render(<TestApiRequestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('data'));
    // Verify that the response data is displayed correctly
    expect(screen.getByTestId('data')).toHaveTextContent('{"message":"Success"}');
    // Click the reset button
    act(() => {
      screen.getByTestId('reset-button').click();
    });
    // Verify that data, loading, and error states are reset to initial values
    expect(screen.queryByTestId('data')).toBeNull();
    expect(screen.queryByTestId('loading')).toBeNull();
    expect(screen.queryByTestId('error')).toBeNull();
  });

  // should handle authentication error correctly
  it('should handle authentication error correctly', async () => {
    // Mock apiClient.get to reject with an error
    jest.spyOn(apiClient, 'get').mockRejectedValue(new Error('Authentication Error'));
    // Mock isAuthenticationError to return true
    jest.spyOn(isAuthenticationError, 'prototype', 'get').mockReturnValue(true);
    // Mock useAuth hook's logout function
    const logoutMock = jest.fn();
    jest.spyOn(require('../../../../web/src/hooks/useAuth'), 'useAuth').mockReturnValue({
      isAuthenticated: false,
      user: null,
      permissions: [],
      loading: false,
      error: null,
      login: jest.fn(),
      logout: logoutMock,
      verifyMfa: jest.fn(),
      refreshToken: jest.fn(),
      getUserProfile: jest.fn(),
      checkPermission: jest.fn(),
      checkRole: jest.fn(),
    });
    // Render TestApiRequestComponent with GET method and test endpoint
    render(<TestApiRequestComponent endpoint="/test" method="get" />);
    // Wait for the API call to complete
    await waitFor(() => screen.getByTestId('error'));
    // Verify that loading state changes correctly
    expect(screen.queryByTestId('loading')).toBeNull();
    // Verify that the error message is displayed correctly
    expect(screen.getByTestId('error')).toHaveTextContent('Authentication Error');
    // Verify that logout function was called
    expect(logoutMock).toHaveBeenCalled();
  });
});