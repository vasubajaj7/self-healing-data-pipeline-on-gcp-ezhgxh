import React from 'react';
import { Tabs, TabsProps, Tab, TabProps, Box, Badge } from '@mui/material'; // @mui/material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import useMediaQuery from '@mui/material/useMediaQuery'; // @mui/material ^5.11.0
import { lightTheme } from '../../theme/theme';

/**
 * Interface for individual tab configuration
 */
interface TabItem {
  id: string;
  label: string | React.ReactNode;
  content: React.ReactNode;
  icon?: React.ReactNode;
  disabled?: boolean;
  badgeContent?: number | string;
  badgeColor?: string;
}

/**
 * Extended props interface for the Tabs component
 */
interface CustomTabsProps extends Omit<TabsProps, 'children'> {
  tabs: TabItem[];
  activeTab: number;
  onTabChange: (index: number) => void;
  variant?: 'standard' | 'fullWidth' | 'scrollable';
  orientation?: 'horizontal' | 'vertical';
  contentClassName?: string;
  tabClassName?: string;
  showContent?: boolean;
  tabProps?: Partial<TabProps>;
}

/**
 * Props for the TabPanel component
 */
interface TabPanelProps {
  children: React.ReactNode;
  value: number;
  index: number;
  className?: string;
}

/**
 * Generates accessibility props for tabs and tab panels
 * @param index - Tab index
 * @returns Object containing id and aria-controls attributes
 */
function a11yProps(index: number) {
  return {
    id: `tab-${index}`,
    'aria-controls': `tabpanel-${index}`,
  };
}

/**
 * Styled version of Material-UI Tabs with custom styling
 */
const StyledTabs = styled(Tabs)(({ theme, orientation }) => ({
  borderBottom: orientation === 'horizontal' ? `1px solid ${theme.palette.divider}` : 'none',
  minHeight: '48px',
  '& .MuiTabs-indicator': {
    backgroundColor: theme.palette.primary.main,
    height: orientation === 'horizontal' ? '3px' : undefined,
    width: orientation === 'vertical' ? '3px' : undefined,
  },
  '&.MuiTabs-vertical': {
    borderRight: `1px solid ${theme.palette.divider}`,
    minWidth: '160px',
  },
  // Styling for scrollable tabs
  '&.MuiTabs-scrollable': {
    '& .MuiTabs-scrollButtons': {
      '&.Mui-disabled': {
        opacity: 0.3,
      },
    },
  },
}));

/**
 * Styled version of Material-UI Tab with custom styling
 */
const StyledTab = styled(Tab)(({ theme }) => ({
  textTransform: 'none',
  fontWeight: 500,
  fontSize: '0.875rem',
  minHeight: '48px',
  padding: '12px 16px',
  transition: 'color 0.2s ease-in-out, background-color 0.2s ease-in-out',
  '&.Mui-selected': {
    color: theme.palette.primary.main,
    fontWeight: 600,
  },
  '&:hover': {
    opacity: 0.8,
    backgroundColor: theme.palette.action.hover,
  },
  '&.MuiTab-labelIcon': {
    minHeight: '72px',
    paddingTop: '8px',
  },
  [theme.breakpoints.down('sm')]: {
    padding: '8px 12px',
    minWidth: 'auto',
    fontSize: '0.8125rem',
  },
}));

/**
 * Component to render the content of a tab
 */
const TabPanel = (props: TabPanelProps) => {
  const { children, value, index, className, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      className={className}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 2, transition: 'opacity 0.3s ease-in-out' }}>
          {children}
        </Box>
      )}
    </div>
  );
};

/**
 * Main tabs component with custom styling and additional functionality.
 * Provides a consistent tab navigation pattern across the application
 * with support for badges, icons, and responsive behavior.
 */
const CustomTabs: React.FC<CustomTabsProps> = ({
  tabs,
  activeTab,
  onTabChange,
  variant = 'standard',
  orientation = 'horizontal',
  contentClassName,
  tabClassName,
  showContent = true,
  tabProps,
  ...props
}) => {
  const isSmallScreen = useMediaQuery(lightTheme.breakpoints.down('sm'));

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    onTabChange(newValue);
  };

  // Determine appropriate variant based on screen size and provided variant
  const effectiveVariant = isSmallScreen && variant === 'standard' ? 'scrollable' : variant;
  
  // Determine if scroll buttons should be shown
  const showScrollButtons = effectiveVariant === 'scrollable' ? 'auto' : false;

  return (
    <React.Fragment>
      <StyledTabs 
        value={activeTab}
        onChange={handleTabChange}
        variant={effectiveVariant}
        orientation={orientation}
        scrollButtons={showScrollButtons}
        allowScrollButtonsMobile
        aria-label="navigation tabs"
        {...props}
      >
        {tabs.map((tab, index) => {
          // If tab has a badge, wrap the label in a Badge component
          const label = tab.badgeContent ? (
            <Badge 
              badgeContent={tab.badgeContent} 
              color={(tab.badgeColor as any) || 'primary'}
            >
              {tab.label}
            </Badge>
          ) : tab.label;

          return (
            <StyledTab
              key={tab.id}
              label={label}
              icon={tab.icon}
              iconPosition="start"
              disabled={tab.disabled}
              className={tabClassName}
              {...a11yProps(index)}
              {...tabProps}
            />
          );
        })}
      </StyledTabs>
      
      {/* Only render tab panels if showContent is true */}
      {showContent && tabs.map((tab, index) => (
        <TabPanel 
          key={tab.id} 
          value={activeTab} 
          index={index}
          className={contentClassName}
        >
          {tab.content}
        </TabPanel>
      ))}
    </React.Fragment>
  );
};

export default CustomTabs;