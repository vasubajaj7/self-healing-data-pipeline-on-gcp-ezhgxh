import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Grid,
  Paper,
  Card,
  CardContent,
  CardHeader,
  Divider,
  Tabs,
  Tab,
  CircularProgress,
  Alert,
  Chip,
  Tooltip,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  SpeedOutlined,
  TrendingUpOutlined,
  StorageOutlined,
  AttachMoneyOutlined,
  MemoryOutlined,
  DataUsageOutlined,
  TrendingUp,
  TrendingDown,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import { styled } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { format, subDays } from 'date-fns'; // date-fns ^2.30.0

import QueryPerformanceTable from './QueryPerformanceTable';
import OptimizationRecommendations from './OptimizationRecommendations';
import ResourceUtilizationChart from './ResourceUtilizationChart';
import CostAnalysisChart from './CostAnalysisChart';
import SchemaOptimizationCard from './SchemaOptimizationCard';
import optimizationService from '../../services/api/optimizationService';
import useApi from '../../hooks/useApi';
import { OptimizationType, OptimizationSummary } from '../../types/optimization';
import useNotification from '../../hooks/useNotification';

/**
 * Interface for the props of the OptimizationDashboard component
 */
interface OptimizationDashboardProps {
  className?: string;
}

/**
 * Interface for the TabPanel component props
 */
interface TabPanelProps {
  children?: React.ReactNode;
  value: number;
  index: number;
}

/**
 * Formats a number as a currency value with dollar sign
 * @param value The number to format
 * @returns Formatted currency string
 */
const formatCurrency = (value: number): string => {
  return `$${value.toFixed(2)}`;
};

/**
 * Formats a number as a percentage with sign
 * @param value The number to format
 * @returns Formatted percentage string
 */
const formatPercentage = (value: number): string => {
  return `${value > 0 ? '+' : ''}${value.toFixed(1)}%`;
};

/**
 * Styled component for the tabs container
 */
const TabsContainer = styled(Box)(({ theme }) => ({
  borderBottom: `1px solid ${theme.palette.divider}`,
  marginBottom: theme.spacing(3),
}));

/**
 * Styled grid container for summary cards
 */
const SummaryGrid = styled(Grid)(({ theme }) => ({
  marginBottom: theme.spacing(3),
}));

/**
 * Styled container for tab panel content
 */
const ContentContainer = styled(Box)(({ theme }) => ({
  marginTop: theme.spacing(2),
}));

/**
 * Styled Card component for summary metrics
 */
const SummaryCardStyled = styled(Card)(({ theme }) => ({
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
}));

/**
 * Styled container for card icons
 */
const IconContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: '48px',
  height: '48px',
  borderRadius: '50%',
  marginBottom: theme.spacing(2),
}));

/**
 * Styled container for trend indicators
 */
const TrendContainer = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  marginLeft: theme.spacing(1),
}));

/**
 * Component for rendering tab panel content
 */
function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`optimization-tabpanel-${index}`}
      aria-labelledby={`optimization-tab-${index}`}
      {...other}
    >
      {value === index && (
        <ContentContainer>
          {children}
        </ContentContainer>
      )}
    </div>
  );
}

/**
 * Main dashboard component for performance optimization features
 */
const OptimizationDashboard: React.FC<OptimizationDashboardProps> = ({ className }) => {
  // Define state variables
  const [activeTab, setActiveTab] = useState<number>(0);
  const [timeRange, setTimeRange] = useState<{ startDate: string; endDate: string }>({
    startDate: format(subDays(new Date(), 7), 'yyyy-MM-dd'),
    endDate: format(new Date(), 'yyyy-MM-dd'),
  });
  const [summary, setSummary] = useState<OptimizationSummary | null>(null);
  const [selectedResourceType, setSelectedResourceType] = useState<string>('bigquery_slots');

  // Use the useApi hook for API calls
  const { get, loading, error } = useApi();

  // Use the useNotification hook for displaying notifications
  const { showSuccess, showInfo } = useNotification();

  /**
   * Load optimization summary data from the API
   */
  const loadOptimizationSummary = useCallback(async () => {
    try {
      const response = await get<OptimizationSummary>(optimizationService.getOptimizationSummary());
      setSummary(response);
    } catch (error) {
      console.error('Failed to load optimization summary:', error);
    }
  }, [get]);

  /**
   * Handle tab change
   * @param event The event object
   * @param newValue The new tab value
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  /**
   * Handle successful application of an optimization recommendation
   */
  const handleRecommendationApplied = () => {
    showSuccess('Recommendation applied successfully!');
    loadOptimizationSummary();
  };

  /**
   * Handle rejection of an optimization recommendation
   */
  const handleRecommendationRejected = () => {
    showInfo('Recommendation rejected.');
    loadOptimizationSummary();
  };

  /**
   * Handle resource type change for resource utilization chart
   * @param resourceType The new resource type
   */
  const handleResourceTypeChange = (resourceType: string) => {
    setSelectedResourceType(resourceType);
  };

  // Load optimization summary when component mounts
  useEffect(() => {
    loadOptimizationSummary();
  }, [loadOptimizationSummary]);

  return (
    <Box className={className}>
      <TabsContainer>
        <Tabs
          value={activeTab}
          onChange={handleTabChange}
          aria-label="optimization tabs"
        >
          <Tab label="Query Optimization" id="optimization-tab-0" aria-controls="optimization-tabpanel-0" />
          <Tab label="Schema Optimization" id="optimization-tab-1" aria-controls="optimization-tabpanel-1" />
          <Tab label="Resource Optimization" id="optimization-tab-2" aria-controls="optimization-tabpanel-2" />
          <Tab label="Cost Analysis" id="optimization-tab-3" aria-controls="optimization-tabpanel-3" />
        </Tabs>
      </TabsContainer>

      {loading && <Box display="flex" justifyContent="center"><CircularProgress /></Box>}
      {error && <Alert severity="error">Error loading data: {error.message}</Alert>}

      {summary && (
        <SummaryGrid container spacing={3}>
          <Grid item xs={12} sm={6} md={3}>
            <SummaryCard
              title="Total Recommendations"
              value={summary.total_recommendations}
              icon={<SpeedOutlined color="primary" />}
              color="primary.main"
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <SummaryCard
              title="Applied Recommendations"
              value={summary.applied_recommendations}
              icon={<TrendingUpOutlined color="success" />}
              color="success.main"
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <SummaryCard
              title="Total Savings"
              value={formatCurrency(summary.total_savings)}
              icon={<AttachMoneyOutlined color="success" />}
              color="success.main"
            />
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <SummaryCard
              title="Potential Savings"
              value={formatCurrency(summary.potential_savings)}
              icon={<AttachMoneyOutlined color="warning" />}
              color="warning.main"
            />
          </Grid>
        </SummaryGrid>
      )}

      <TabPanel value={activeTab} index={0}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <QueryPerformanceTable
              timeRange={timeRange}
              onQuerySelect={(queryId) => console.log('Selected query:', queryId)}
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <OptimizationRecommendations
              type={OptimizationType.QUERY}
              onRecommendationApplied={handleRecommendationApplied}
              onRecommendationRejected={handleRecommendationRejected}
            />
          </Grid>
        </Grid>
      </TabPanel>

      <TabPanel value={activeTab} index={1}>
        <SchemaOptimizationCard
          maxHeight={600}
          onRecommendationApplied={handleRecommendationApplied}
          onRecommendationRejected={handleRecommendationRejected}
        />
      </TabPanel>

      <TabPanel value={activeTab} index={2}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <ResourceUtilizationChart
              initialResourceType={selectedResourceType}
              onResourceTypeChange={handleResourceTypeChange}
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <OptimizationRecommendations
              type={OptimizationType.RESOURCE}
              onRecommendationApplied={handleRecommendationApplied}
              onRecommendationRejected={handleRecommendationRejected}
            />
          </Grid>
        </Grid>
      </TabPanel>

      <TabPanel value={activeTab} index={3}>
        <CostAnalysisChart />
      </TabPanel>
    </Box>
  );
};

/**
 * Props for the SummaryCard component
 */
interface SummaryCardProps {
  title: string;
  value: string | number;
  icon: React.ReactNode;
  color: string;
  subtitle?: string;
  trend?: number | null;
}

/**
 * Card component for displaying summary metrics
 */
const SummaryCard: React.FC<SummaryCardProps> = ({ title, value, icon, color, subtitle, trend }) => {
  return (
    <SummaryCardStyled>
      <CardContent>
        <IconContainer style={{ backgroundColor: color + '1A' }}>
          {React.cloneElement(icon, { style: { color } })}
        </IconContainer>
        <Typography variant="h6" component="div">
          {value}
        </Typography>
        <Typography variant="subtitle2" color="textSecondary">
          {title}
          {trend !== null && (
            <TrendContainer>
              {trend > 0 ? <TrendingUp color="success" /> : <TrendingDown color="error" />}
              <Typography variant="caption" color={trend > 0 ? "success.main" : "error.main"}>
                {formatPercentage(trend)}
              </Typography>
            </TrendContainer>
          )}
        </Typography>
        {subtitle && (
          <Typography variant="caption" color="textSecondary">
            {subtitle}
          </Typography>
        )}
      </CardContent>
    </SummaryCardStyled>
  );
};

export default OptimizationDashboard;