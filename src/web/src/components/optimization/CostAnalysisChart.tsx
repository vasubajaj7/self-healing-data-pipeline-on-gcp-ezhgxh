import React, { useState, useEffect, useCallback, useMemo } from 'react'; // version: ^18.2.0
import {
  Card,
  CardContent,
  CardHeader,
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  Alert,
  ToggleButtonGroup,
  ToggleButton,
  Divider,
  Tooltip
} from '@mui/material'; // version: ^5.11.0
import { AttachMoney, TrendingUp, TrendingDown, Info } from '@mui/icons-material'; // version: ^5.11.0
import { format, subDays, startOfDay, endOfDay, parseISO } from 'date-fns'; // version: ^2.30.0
import { PieChart, Pie, Tooltip as RechartsTooltip, Legend } from 'recharts'; // version: ^2.5.0

import LineChart from '../charts/LineChart';
import optimizationService from '../../services/api/optimizationService';
import useApi from '../../hooks/useApi';
import { transformTimeSeriesData } from '../../services/charts/dataTransformers';
import { formatCurrency, formatPercentage } from '../../services/charts/chartUtils';
import { CostAnalysisData } from '../../types/optimization';

// Define the props for the CostAnalysisChart component
interface CostAnalysisChartProps {
  height?: string | number;
  title?: string;
  className?: string;
  initialTimePeriod?: string;
  onTimePeriodChange?: (timePeriod: string) => void;
}

// Define the type for time period options
interface TimePeriod {
  id: string;
  name: string;
  days: number;
}

// Define a styled Card component with consistent height and padding
const StyledCard = ({ children, ...props }: any) => (
  <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }} {...props}>
    {children}
  </Card>
);

// Define a container for the chart with appropriate sizing
const ChartContainer = ({ children }: any) => (
  <Box sx={{ height: 'calc(100% - 80px)', width: '100%', position: 'relative' }}>
    {children}
  </Box>
);

// Define a container for cost summary information
const SummaryContainer = ({ children }: any) => (
  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
    {children}
  </Box>
);

// Define styled Typography for cost values
const CostValue = ({ children }: any) => (
  <Typography variant="h5" component="div" sx={{ fontSize: '1.5rem', fontWeight: 'bold' }}>
    {children}
  </Typography>
);

// Define styled component for trend indicators
const TrendIndicator = ({ children }: any) => (
  <Box sx={{ display: 'flex', alignItems: 'center', marginLeft: '8px' }}>
    {children}
  </Box>
);

/**
 * Formats cost change percentage with appropriate color and icon
 * @param percentage The cost change percentage
 * @returns Object with formatted text, color, and icon
 */
const formatCostChange = (percentage: number) => {
  const formattedPercentage = formatPercentage(percentage);
  const color = percentage < 0 ? 'success.main' : 'error.main';
  const icon = percentage < 0 ? <TrendingDown color="success" /> : <TrendingUp color="error" />;

  return {
    text: formattedPercentage,
    color: color,
    icon: icon,
  };
};

/**
 * A component that displays cost analysis data with trends and breakdowns
 */
const CostAnalysisChart: React.FC<CostAnalysisChartProps> = (props) => {
  // Define state variables
  const [timePeriod, setTimePeriod] = useState<string>(props.initialTimePeriod || 'last30days');
  const [costData, setCostData] = useState<CostAnalysisData | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | null>(null);

  // Define available time periods for filtering
  const timePeriods: TimePeriod[] = useMemo(() => [
    { id: 'last7days', name: 'Last 7 Days', days: 7 },
    { id: 'last30days', name: 'Last 30 Days', days: 30 },
    { id: 'last90days', name: 'Last 90 Days', days: 90 },
  ], []);

  // Use the useApi hook for API calls
  const { get } = useApi();

  /**
   * Load cost analysis data
   */
  const loadCostData = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      // Calculate date range based on timePeriod
      const endDate = new Date();
      const startDate = subDays(endDate, timePeriods.find(p => p.id === timePeriod)?.days || 30);

      // Format dates as ISO strings
      const endDateStr = format(endDate, 'yyyy-MM-dd');
      const startDateStr = format(startDate, 'yyyy-MM-dd');

      // Fetch cost data from API
      const response = await get<CostAnalysisData>(
        `/optimization/recommendations/cost-analysis?startDate=${startDateStr}&endDate=${endDateStr}`
      );

      // Update state with cost data
      setCostData(response);
    } catch (err: any) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [timePeriod, get, timePeriods]);

  // Load cost data when time period changes
  useEffect(() => {
    loadCostData();
  }, [loadCostData]);

  /**
   * Handle time period selection change
   * @param event The event object
   * @param newValue The new time period value
   */
  const handleTimePeriodChange = (event: React.MouseEvent<HTMLElement>, newValue: string | null) => {
    if (newValue) {
      setTimePeriod(newValue);
      if (props.onTimePeriodChange) {
        props.onTimePeriodChange(newValue);
      }
    }
  };

  /**
   * Format cost trend data for the line chart
   * @param costData The cost analysis data
   * @returns The formatted cost trend data
   */
  const formatTrendData = useCallback((costData: CostAnalysisData) => {
    return transformTimeSeriesData(
      {
        data: costData.costTrend,
        name: 'Cost',
      },
      {
        dateFormat: 'MMM d',
      }
    );
  }, []);

  /**
   * Format cost breakdown data for the pie chart
   * @param costData The cost analysis data
   * @returns The formatted cost breakdown data
   */
  const formatBreakdownData = useCallback((costData: CostAnalysisData) => {
    const breakdownData = Object.entries(costData.costBreakdown).map(([name, value]) => ({
      name,
      value,
    }));
    return breakdownData;
  }, []);

  return (
    <StyledCard className={props.className}>
      <CardHeader
        title={props.title || 'Cost Analysis'}
        action={
          <ToggleButtonGroup
            value={timePeriod}
            exclusive
            onChange={handleTimePeriodChange}
            size="small"
          >
            {timePeriods.map((period) => (
              <ToggleButton key={period.id} value={period.id}>
                {period.name}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
        }
      />
      <CardContent sx={{ flexGrow: 1, padding: '16px', display: 'flex', flexDirection: 'column' }}>
        {loading && (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
          </Box>
        )}
        {error && <Alert severity="error">Error loading cost analysis data: {error.message}</Alert>}
        {!loading && !error && !costData && (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <Typography color="text.secondary">No cost data available for the selected time period.</Typography>
          </Box>
        )}
        {!loading && !error && costData && (
          <>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '16px' }}>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Total Cost
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Typography variant="h5" component="div">
                    {formatCurrency(costData.totalCost)}
                  </Typography>
                  {costData.costChangePercentage !== 0 && (
                    <Box sx={{ display: 'flex', alignItems: 'center', ml: 1 }}>
                      <Typography variant="body2" color={formatCostChange(costData.costChangePercentage).color} sx={{ display: 'flex', alignItems: 'center' }}>
                        {formatCostChange(costData.costChangePercentage).icon} {formatCostChange(costData.costChangePercentage).text}
                      </Typography>
                    </Box>
                  )}
                  <Typography variant="body2" color="text.secondary">
                    vs. previous period
                  </Typography>
                </Box>
              </Box>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Potential Savings
                </Typography>
                <Typography variant="h5" component="div" color="success.main">
                  {formatCurrency(costData.potentialSavings)}
                </Typography>
              </Box>
            </Box>
            <Box sx={{ height: '200px', mb: 3 }}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Cost Trend
              </Typography>
              <LineChart
                data={formatTrendData(costData)}
                height="100%"
                width="100%"
                options={{
                  maintainAspectRatio: false,
                  scales: {
                    y: {
                      beginAtZero: true,
                      title: {
                        display: true,
                        text: 'Cost ($)',
                      },
                    },
                    x: {
                      title: {
                        display: true,
                        text: 'Date',
                      },
                    },
                  },
                  plugins: {
                    tooltip: {
                      callbacks: {
                        label: (context) => `Cost: ${formatCurrency(context.parsed.y)}`,
                      },
                    },
                  },
                }}
              />
            </Box>
            <Divider sx={{ my: 2 }} />
            <Box sx={{ display: 'flex', flexDirection: { xs: 'column', md: 'row' }, justifyContent: 'space-between' }}>
              <Box sx={{ width: { xs: '100%', md: '48%' }, mb: { xs: 2, md: 0 } }}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Cost Breakdown
                </Typography>
                <Box sx={{ height: '200px' }}>
                  <PieChart width="100%" height="100%" data={formatBreakdownData(costData)} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
                    <Pie dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} fill="#8884d8" label={({ name, value }) => `${name}: ${value}`} />
                    <RechartsTooltip formatter={(value) => formatCurrency(value)} />
                    <Legend layout="vertical" align="right" verticalAlign="middle" />
                  </PieChart>
                </Box>
              </Box>
              <Box sx={{ width: { xs: '100%', md: '48%' } }}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  Savings Opportunities
                </Typography>
                <Box sx={{ maxHeight: '200px', overflowY: 'auto' }}>
                  {!costData.savingsOpportunities || costData.savingsOpportunities.length === 0 ? (
                    <Typography color="text.secondary">No savings opportunities identified.</Typography>
                  ) : (
                    costData.savingsOpportunities.map((opportunity, index) => (
                      <Box key={index} sx={{ mb: 1, p: 1, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                          <Typography variant="body2">{opportunity.category}</Typography>
                          <Typography variant="body2" color="success.main">{formatCurrency(opportunity.amount)}</Typography>
                        </Box>
                        <Typography variant="body2" color="text.secondary">{opportunity.description}</Typography>
                      </Box>
                    ))
                  )}
                </Box>
              </Box>
            </Box>
          </>
        )}
      </CardContent>
    </StyledCard>
  );
};

export default CostAnalysisChart;