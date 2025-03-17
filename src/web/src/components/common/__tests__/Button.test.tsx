import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import Button from '../../common/Button';
import { renderWithTheme } from '../../../test/utils/testUtils';

describe('Button component', () => {
  it('renders button with text', () => {
    renderWithTheme(<Button>Click me</Button>);
    const button = screen.getByRole('button', { name: /click me/i });
    expect(button).toBeInTheDocument();
    expect(button).toHaveTextContent('Click me');
  });

  it('renders button with different variants', () => {
    const { rerender } = renderWithTheme(<Button variant="contained">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-contained');

    rerender(<Button variant="outlined">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-outlined');

    rerender(<Button variant="text">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-text');
  });

  it('renders button with different colors', () => {
    const { rerender } = renderWithTheme(<Button color="primary">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedPrimary');

    rerender(<Button color="secondary">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedSecondary');

    rerender(<Button color="error">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedError');

    rerender(<Button color="warning">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedWarning');

    rerender(<Button color="info">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedInfo');

    rerender(<Button color="success">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-containedSuccess');
  });

  it('renders button with different sizes', () => {
    const { rerender } = renderWithTheme(<Button size="small">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-sizeSmall');

    rerender(<Button size="medium">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-sizeMedium');

    rerender(<Button size="large">Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-sizeLarge');
  });

  it('handles click events', () => {
    const mockOnClick = jest.fn();
    renderWithTheme(<Button onClick={mockOnClick}>Click me</Button>);
    
    fireEvent.click(screen.getByRole('button'));
    expect(mockOnClick).toHaveBeenCalledTimes(1);
  });

  it('renders disabled state', () => {
    const mockOnClick = jest.fn();
    renderWithTheme(
      <Button disabled onClick={mockOnClick}>
        Disabled Button
      </Button>
    );
    
    const button = screen.getByRole('button');
    expect(button).toBeDisabled();
    expect(button).toHaveClass('Mui-disabled');
    
    fireEvent.click(button);
    expect(mockOnClick).not.toHaveBeenCalled();
  });

  it('renders loading state', () => {
    const mockOnClick = jest.fn();
    renderWithTheme(
      <Button loading onClick={mockOnClick}>
        Loading Button
      </Button>
    );
    
    const button = screen.getByRole('button');
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute('aria-busy', 'true');
    
    const loadingIndicator = screen.getByRole('progressbar');
    expect(loadingIndicator).toBeInTheDocument();
    
    fireEvent.click(button);
    expect(mockOnClick).not.toHaveBeenCalled();
  });

  it('renders loading with different positions', () => {
    const { rerender } = renderWithTheme(
      <Button loading loadingPosition="start">
        Start Loading
      </Button>
    );
    
    let loadingIndicator = screen.getByRole('progressbar');
    expect(loadingIndicator).toBeInTheDocument();
    expect(screen.getByText('Start Loading')).toBeInTheDocument();
    
    rerender(
      <Button loading loadingPosition="end">
        End Loading
      </Button>
    );
    
    loadingIndicator = screen.getByRole('progressbar');
    expect(loadingIndicator).toBeInTheDocument();
    expect(screen.getByText('End Loading')).toBeInTheDocument();
    
    rerender(
      <Button loading loadingPosition="center">
        Center Loading
      </Button>
    );
    
    loadingIndicator = screen.getByRole('progressbar');
    expect(loadingIndicator).toBeInTheDocument();
    expect(screen.queryByText('Center Loading')).not.toBeInTheDocument();
  });

  it('renders with custom loading size', () => {
    renderWithTheme(
      <Button loading loadingSize={32}>
        Custom Loading Size
      </Button>
    );
    
    const loadingIndicator = screen.getByRole('progressbar');
    expect(loadingIndicator).toHaveAttribute('style', expect.stringContaining('width: 32px'));
    expect(loadingIndicator).toHaveAttribute('style', expect.stringContaining('height: 32px'));
  });

  it('renders with fullWidth prop', () => {
    renderWithTheme(<Button fullWidth>Full Width Button</Button>);
    expect(screen.getByRole('button')).toHaveClass('MuiButton-fullWidth');
  });

  it('renders with startIcon and endIcon', () => {
    const TestIcon = () => <span data-testid="test-icon">Icon</span>;
    
    const { rerender } = renderWithTheme(
      <Button startIcon={<TestIcon />}>Button with Start Icon</Button>
    );
    
    expect(screen.getByTestId('test-icon')).toBeInTheDocument();
    expect(screen.getByText('Button with Start Icon')).toBeInTheDocument();
    
    rerender(
      <Button endIcon={<TestIcon />}>Button with End Icon</Button>
    );
    
    expect(screen.getByTestId('test-icon')).toBeInTheDocument();
    expect(screen.getByText('Button with End Icon')).toBeInTheDocument();
  });

  it('applies custom className', () => {
    renderWithTheme(
      <Button className="custom-button">Custom Class Button</Button>
    );
    
    expect(screen.getByRole('button')).toHaveClass('custom-button');
  });
});