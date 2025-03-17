import React from 'react';
import { render, RenderResult, RenderOptions } from '@testing-library/react'; // @testing-library/react ^13.4.0
import { ThemeProvider } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import CssBaseline from '@mui/material/CssBaseline'; // @mui/material ^5.11.0
import { ThemeContextProvider, useTheme } from '../../web/src/contexts/ThemeContext';
import { lightTheme, darkTheme } from '../../web/src/theme/theme';

// Interfaces
/**
 * Common props interface for mock components
 */
interface MockComponentProps {
  children?: React.ReactNode;
  className?: string;
  'data-testid'?: string;
}

/**
 * Props for the ThemeWrapper component
 */
interface ThemeWrapperProps {
  children: React.ReactNode;
  theme?: typeof lightTheme | typeof darkTheme;
}

/**
 * Options for the renderWithTheme function
 */
interface RenderWithThemeOptions {
  theme?: typeof lightTheme | typeof darkTheme;
  renderOptions?: RenderOptions;
}

// Constants
/**
 * Prefix for data-testid attributes
 */
const TEST_ID_PREFIX = 'test';

/**
 * Default props for mock components
 */
const MOCK_COMPONENT_PROPS = {
  button: {
    variant: 'contained',
    color: 'primary',
    disabled: false,
    loading: false,
    fullWidth: false,
    size: 'medium',
    type: 'button',
    onClick: jest.fn(),
  },
  card: {
    'data-testid': 'mock-card',
  },
  table: {
    data: [],
    columns: [],
    loading: false,
    onRowClick: jest.fn(),
    'data-testid': 'mock-table',
  },
  modal: {
    open: false,
    onClose: jest.fn(),
    maxWidth: 'sm',
    fullWidth: true,
    'data-testid': 'mock-modal',
  },
  chart: {
    data: [],
    type: 'line',
    height: 300,
    width: 500,
    'data-testid': 'mock-chart',
  },
};

/**
 * Creates a consistent data-testid attribute value for testing
 */
const createDataTestId = (componentName: string, elementName: string): string => {
  return `${TEST_ID_PREFIX}-${componentName}-${elementName}`.toLowerCase().replace(/\s+/g, '-');
};

/**
 * Creates a mock component for testing that renders its children with additional props
 */
const createMockComponent = <P extends Record<string, any>>(
  displayName: string,
  defaultProps?: Partial<P>
): React.FC<P & MockComponentProps> => {
  const MockComponent: React.FC<P & MockComponentProps> = (props) => {
    const combinedProps = { ...defaultProps, ...props };
    return (
      <div 
        data-testid={combinedProps['data-testid'] || `mock-${displayName.toLowerCase()}`} 
        data-mock-component={displayName}
        {...combinedProps}
      >
        {props.children}
      </div>
    );
  };
  
  MockComponent.displayName = `Mock${displayName}`;
  return MockComponent;
};

/**
 * Renders a React component with the specified theme for testing
 */
const renderWithTheme = (
  ui: React.ReactElement,
  options?: RenderWithThemeOptions
): RenderResult => {
  const { theme = lightTheme, renderOptions = {} } = options || {};
  
  const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <ThemeContextProvider>
      {children}
    </ThemeContextProvider>
  );
  
  return render(ui, { wrapper: Wrapper, ...renderOptions });
};

/**
 * Wrapper component that provides theme context for testing
 */
const ThemeWrapper: React.FC<ThemeWrapperProps> = ({ children, theme = lightTheme }) => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      {children}
    </ThemeProvider>
  );
};

/**
 * Mock implementation of the Button component for testing
 */
const MockButton: React.FC<{
  children?: React.ReactNode;
  onClick?: () => void;
  variant?: 'text' | 'outlined' | 'contained';
  color?: 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning' | 'inherit' | string;
  disabled?: boolean;
  loading?: boolean;
  fullWidth?: boolean;
  size?: 'small' | 'medium' | 'large';
  startIcon?: React.ReactNode;
  endIcon?: React.ReactNode;
  className?: string;
  type?: 'button' | 'submit' | 'reset';
  'data-testid'?: string;
}> = (props) => {
  const {
    children,
    onClick = MOCK_COMPONENT_PROPS.button.onClick,
    variant = MOCK_COMPONENT_PROPS.button.variant,
    color = MOCK_COMPONENT_PROPS.button.color,
    disabled = MOCK_COMPONENT_PROPS.button.disabled,
    loading = MOCK_COMPONENT_PROPS.button.loading,
    fullWidth = MOCK_COMPONENT_PROPS.button.fullWidth,
    size = MOCK_COMPONENT_PROPS.button.size,
    startIcon,
    endIcon,
    className,
    type = MOCK_COMPONENT_PROPS.button.type,
    'data-testid': dataTestId = 'mock-button',
  } = props;

  return (
    <button
      type={type}
      className={`mock-button mock-button-${variant} mock-button-${color} mock-button-${size} ${className || ''} ${
        fullWidth ? 'mock-button-fullwidth' : ''
      } ${disabled ? 'mock-button-disabled' : ''}`}
      onClick={onClick}
      disabled={disabled || loading}
      data-testid={dataTestId}
      data-mock-component="Button"
      style={{
        padding: '8px 16px',
        border: variant === 'outlined' ? '1px solid' : 'none',
        backgroundColor: variant === 'contained' ? (color === 'primary' ? '#1976d2' : '#9c27b0') : 'transparent',
        color: variant === 'contained' ? '#fff' : (color === 'primary' ? '#1976d2' : '#9c27b0'),
        borderRadius: '4px',
        cursor: disabled || loading ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.5 : 1,
        width: fullWidth ? '100%' : 'auto',
        fontSize: size === 'small' ? '0.8rem' : size === 'large' ? '1.2rem' : '1rem',
      }}
    >
      {startIcon && <span className="mock-button-start-icon" style={{ marginRight: '8px' }}>{startIcon}</span>}
      {loading ? <span className="mock-button-loading">Loading...</span> : children}
      {endIcon && <span className="mock-button-end-icon" style={{ marginLeft: '8px' }}>{endIcon}</span>}
    </button>
  );
};

/**
 * Mock implementation of the Card component for testing
 */
const MockCard: React.FC<{
  children?: React.ReactNode;
  title?: string;
  subtitle?: string;
  actions?: React.ReactNode;
  className?: string;
  'data-testid'?: string;
}> = (props) => {
  const {
    children,
    title,
    subtitle,
    actions,
    className,
    'data-testid': dataTestId = MOCK_COMPONENT_PROPS.card['data-testid'],
  } = props;

  return (
    <div 
      className={`mock-card ${className || ''}`} 
      data-testid={dataTestId}
      data-mock-component="Card"
      style={{
        border: '1px solid #e0e0e0',
        borderRadius: '4px',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        overflow: 'hidden',
        backgroundColor: '#fff',
      }}
    >
      {(title || subtitle) && (
        <div className="mock-card-header" style={{ padding: '16px', borderBottom: title ? '1px solid #e0e0e0' : 'none' }}>
          {title && <div className="mock-card-title" style={{ fontSize: '1.25rem', fontWeight: 500 }}>{title}</div>}
          {subtitle && <div className="mock-card-subtitle" style={{ fontSize: '0.875rem', color: 'rgba(0,0,0,0.6)' }}>{subtitle}</div>}
        </div>
      )}
      <div className="mock-card-content" style={{ padding: '16px' }}>{children}</div>
      {actions && (
        <div className="mock-card-actions" style={{ padding: '8px 16px', borderTop: '1px solid #e0e0e0' }}>
          {actions}
        </div>
      )}
    </div>
  );
};

/**
 * Mock implementation of the Table component for testing
 */
const MockTable: React.FC<{
  data?: any[];
  columns?: Array<{
    field?: string;
    name?: string;
    header?: string;
  }>;
  loading?: boolean;
  pagination?: {
    page?: number;
    totalCount?: number;
    pageSize?: number;
    onPageChange?: (page: number) => void;
  };
  onRowClick?: (row: any) => void;
  className?: string;
  'data-testid'?: string;
}> = (props) => {
  const {
    data = MOCK_COMPONENT_PROPS.table.data,
    columns = MOCK_COMPONENT_PROPS.table.columns,
    loading = MOCK_COMPONENT_PROPS.table.loading,
    pagination,
    onRowClick = MOCK_COMPONENT_PROPS.table.onRowClick,
    className,
    'data-testid': dataTestId = MOCK_COMPONENT_PROPS.table['data-testid'],
  } = props;

  return (
    <div 
      className={`mock-table-container ${className || ''}`} 
      data-testid={dataTestId}
      data-mock-component="Table"
      style={{ width: '100%', overflowX: 'auto' }}
    >
      {loading ? (
        <div className="mock-table-loading" style={{ padding: '16px', textAlign: 'center' }}>Loading...</div>
      ) : (
        <table className="mock-table" style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {columns.map((column, index) => (
                <th 
                  key={index} 
                  className="mock-table-header"
                  style={{ 
                    padding: '12px 16px', 
                    textAlign: 'left', 
                    borderBottom: '2px solid #e0e0e0',
                    backgroundColor: '#f5f5f5',
                    fontWeight: 500
                  }}
                >
                  {column.header || column.name || column.field || `Column ${index}`}
                </th>
              ))}
              {columns.length === 0 && <th style={{ padding: '12px 16px' }}>No columns defined</th>}
            </tr>
          </thead>
          <tbody>
            {data.length > 0 ? (
              data.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className="mock-table-row"
                  onClick={() => onRowClick && onRowClick(row)}
                  data-testid={`${dataTestId}-row-${rowIndex}`}
                  style={{ cursor: onRowClick ? 'pointer' : 'default' }}
                >
                  {columns.map((column, colIndex) => (
                    <td 
                      key={colIndex} 
                      className="mock-table-cell"
                      style={{ 
                        padding: '12px 16px', 
                        borderBottom: '1px solid #e0e0e0',
                        borderTop: rowIndex === 0 ? 'none' : '1px solid #e0e0e0'
                      }}
                    >
                      {row[column.field || column.name || `col${colIndex}`]}
                    </td>
                  ))}
                  {columns.length === 0 && <td style={{ padding: '12px 16px' }}>{JSON.stringify(row).substring(0, 50)}</td>}
                </tr>
              ))
            ) : (
              <tr>
                <td 
                  colSpan={columns.length || 1} 
                  className="mock-table-empty"
                  style={{ padding: '16px', textAlign: 'center', color: 'rgba(0,0,0,0.6)' }}
                >
                  No data available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
      {pagination && (
        <div 
          className="mock-table-pagination" 
          data-testid={`${dataTestId}-pagination`}
          style={{ 
            padding: '16px', 
            display: 'flex', 
            justifyContent: 'flex-end',
            borderTop: '1px solid #e0e0e0'
          }}
        >
          <span>
            Page {pagination.page || 1} of {Math.ceil((pagination.totalCount || 0) / (pagination.pageSize || 10)) || 1}
          </span>
        </div>
      )}
    </div>
  );
};

/**
 * Mock implementation of the Modal component for testing
 */
const MockModal: React.FC<{
  children?: React.ReactNode;
  open?: boolean;
  onClose?: () => void;
  title?: string;
  actions?: React.ReactNode;
  maxWidth?: 'xs' | 'sm' | 'md' | 'lg' | 'xl' | string;
  fullWidth?: boolean;
  className?: string;
  'data-testid'?: string;
}> = (props) => {
  const {
    children,
    open = MOCK_COMPONENT_PROPS.modal.open,
    onClose = MOCK_COMPONENT_PROPS.modal.onClose,
    title,
    actions,
    maxWidth = MOCK_COMPONENT_PROPS.modal.maxWidth,
    fullWidth = MOCK_COMPONENT_PROPS.modal.fullWidth,
    className,
    'data-testid': dataTestId = MOCK_COMPONENT_PROPS.modal['data-testid'],
  } = props;

  if (!open) {
    return null;
  }

  const getMaxWidthValue = (): string => {
    const widthMap: Record<string, string> = {
      xs: '444px',
      sm: '600px',
      md: '900px',
      lg: '1200px',
      xl: '1536px'
    };
    return widthMap[maxWidth as string] || '600px';
  };

  return (
    <div
      className={`mock-modal mock-modal-${maxWidth} ${fullWidth ? 'mock-modal-fullwidth' : ''} ${className || ''}`}
      data-testid={dataTestId}
      data-mock-component="Modal"
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
    >
      <div 
        className="mock-modal-backdrop" 
        onClick={onClose} 
        data-testid={`${dataTestId}-backdrop`}
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0,0,0,0.5)',
        }}
      />
      <div 
        className="mock-modal-content"
        style={{
          position: 'relative',
          backgroundColor: '#fff',
          borderRadius: '4px',
          boxShadow: '0 4px 8px rgba(0,0,0,0.2)',
          width: fullWidth ? '90%' : getMaxWidthValue(),
          maxWidth: getMaxWidthValue(),
          maxHeight: '90vh',
          overflow: 'auto',
          zIndex: 1001,
        }}
      >
        {title && (
          <div 
            className="mock-modal-header"
            style={{
              padding: '16px 24px',
              borderBottom: '1px solid #e0e0e0',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <div className="mock-modal-title" style={{ fontSize: '1.25rem', fontWeight: 500 }}>{title}</div>
            <button 
              className="mock-modal-close" 
              onClick={onClose} 
              data-testid={`${dataTestId}-close`}
              style={{
                background: 'none',
                border: 'none',
                fontSize: '1.5rem',
                cursor: 'pointer',
                padding: '4px',
              }}
            >
              &times;
            </button>
          </div>
        )}
        <div className="mock-modal-body" style={{ padding: '16px 24px' }}>{children}</div>
        {actions && (
          <div 
            className="mock-modal-actions"
            style={{
              padding: '8px 24px 16px',
              borderTop: '1px solid #e0e0e0',
              display: 'flex',
              justifyContent: 'flex-end',
              gap: '8px',
            }}
          >
            {actions}
          </div>
        )}
      </div>
    </div>
  );
};

/**
 * Mock implementation of chart components for testing
 */
const MockChart: React.FC<{
  data?: any[];
  type?: 'line' | 'bar' | 'pie' | 'area' | 'scatter' | string;
  options?: {
    xAxis?: {
      title?: string;
      type?: string;
    };
    yAxis?: {
      title?: string;
      type?: string;
    };
    title?: string;
    legend?: boolean;
    [key: string]: any;
  };
  height?: number;
  width?: number;
  className?: string;
  'data-testid'?: string;
}> = (props) => {
  const {
    data = MOCK_COMPONENT_PROPS.chart.data,
    type = MOCK_COMPONENT_PROPS.chart.type,
    options,
    height = MOCK_COMPONENT_PROPS.chart.height,
    width = MOCK_COMPONENT_PROPS.chart.width,
    className,
    'data-testid': dataTestId = MOCK_COMPONENT_PROPS.chart['data-testid'],
  } = props;

  const chartColors = {
    line: '#1976d2',
    bar: '#2e7d32',
    pie: '#9c27b0',
    area: '#ed6c02',
    scatter: '#d32f2f',
  };

  const chartColor = chartColors[type as keyof typeof chartColors] || '#1976d2';

  return (
    <div
      className={`mock-chart mock-chart-${type} ${className || ''}`}
      style={{ 
        height: `${height}px`, 
        width: `${width}px`,
        border: '1px solid #e0e0e0',
        borderRadius: '4px',
        padding: '16px',
        backgroundColor: '#fff',
        overflow: 'hidden',
      }}
      data-testid={dataTestId}
      data-mock-component="Chart"
    >
      {options?.title && (
        <div 
          className="mock-chart-title" 
          style={{ 
            fontSize: '1.25rem', 
            fontWeight: 500, 
            marginBottom: '16px',
            textAlign: 'center',
          }}
        >
          {options.title}
        </div>
      )}
      <div 
        className="mock-chart-info"
        style={{
          fontSize: '0.875rem',
          color: 'rgba(0,0,0,0.6)',
          marginBottom: '16px',
        }}
      >
        <div>Chart Type: <span style={{ color: chartColor }}>{type}</span></div>
        <div>Data Points: {data.length}</div>
        <div>
          {options?.xAxis?.title && <span>X-Axis: {options.xAxis.title}</span>}
          {options?.yAxis?.title && <span> • Y-Axis: {options.yAxis.title}</span>}
        </div>
      </div>
      <div 
        className="mock-chart-visualization"
        style={{
          height: `${height - 120}px`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          border: '1px dashed #e0e0e0',
          borderRadius: '4px',
          backgroundColor: '#f5f5f5',
        }}
      >
        {data.length > 0 ? (
          <div className="mock-chart-data" style={{ width: '100%', padding: '16px' }}>
            <div style={{ 
              color: chartColor,
              textAlign: 'center',
              fontWeight: 500,
              marginBottom: '8px',
            }}>
              [Mock {type.charAt(0).toUpperCase() + type.slice(1)} Chart]
            </div>
            {data.slice(0, 3).map((item, index) => (
              <div 
                key={index} 
                className="mock-chart-data-point"
                style={{
                  padding: '4px 8px',
                  margin: '4px 0',
                  borderLeft: `3px solid ${chartColor}`,
                  backgroundColor: 'rgba(0,0,0,0.03)',
                }}
              >
                Data Point {index + 1}: {JSON.stringify(item).substring(0, 50)}
                {JSON.stringify(item).length > 50 ? '...' : ''}
              </div>
            ))}
            {data.length > 3 && (
              <div 
                className="mock-chart-data-more"
                style={{
                  padding: '4px 8px',
                  textAlign: 'center',
                  fontSize: '0.875rem',
                  color: 'rgba(0,0,0,0.6)',
                }}
              >
                ...and {data.length - 3} more points
              </div>
            )}
          </div>
        ) : (
          <div 
            className="mock-chart-empty"
            style={{
              padding: '16px',
              textAlign: 'center',
              color: 'rgba(0,0,0,0.6)',
            }}
          >
            No data to display
          </div>
        )}
      </div>
    </div>
  );
};

// Exports
export {
  renderWithTheme,
  createMockComponent,
  createDataTestId,
  MockButton,
  MockCard,
  MockTable,
  MockModal,
  MockChart,
  ThemeWrapper,
};