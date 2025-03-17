import React, { useState, useEffect, useMemo } from 'react'; // react ^18.2.0
import {
  Grid,
  Box,
  Typography,
  Divider,
  Tabs,
  Tab,
  Paper,
  Chip,
  Tooltip,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  CheckCircleOutline,
  Warning,
  Error,
  AccessTime,
  Speed,
  Timeline,
  Settings,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Card from '../common/Card';
import ActiveIssuesTable from './ActiveIssuesTable';
import HealingActionsTable from './HealingActionsTable';
import SuccessRateChart from './SuccessRateChart';
import ModelPerformanceCard from './ModelPerformanceCard';
import ModelHealthCard from './ModelHealthCard';
import HealingSettingsForm from './HealingSettingsForm';
import { useApi } from '../../hooks/useApi';
import healingService from '../../services/api/healingService';
import {
  HealingDashboardData,
  AIModel,
  ModelType,
} from '../../types/selfHealing';
import { formatNumber } from '../../utils/formatting';
import { formatDuration } from '../../utils/date';

/**
 * Interface defining the props for the HealingDashboard component
 */
interface HealingDashboardProps {
  /**
   * Additional CSS class for styling
   */
  className?: string;
  /**
   * Date range for filtering dashboard data
   */
  dateRange?: { startDate: string; endDate: string };
  /**
   * Interval in milliseconds to refresh dashboard data
   */
  refreshInterval?: number;
  /**
   * Whether to show the settings section
   */
  showSettings?: boolean;
}

/**
 * Interface defining the props for the MetricCard component
 */
interface MetricCardProps {
  /**
   * Title of the metric
   */
  title: string;
  /**
   * Value of the metric
   */
  value: number;
  /**
   * Type of metric for formatting
   */
  metricType: string;
  /**
   * Icon to display with the metric
   */
  icon?: React.ReactNode;
  /**
   * Description of the metric
   */
  description?: string;
}

/**
 * Formats a success rate value as a percentage with appropriate styling
 * @param rate The success rate number
 * @returns Formatted success rate with color-coded styling
 */
const formatSuccessRate = (rate: number): React.ReactNode => {
  if (rate === undefined || rate === null) {
    return '-';
  }

  const percentage = (rate * 100).toFixed(1);
  let color = 'success';

  if (rate < 50) {
    color = 'error';
  } else if (rate < 80) {
    color = 'warning';
  }

  return (
    <Typography variant="body2" color={color}>
      {percentage}%
    </Typography>
  );
};

/**
 * Formats a metric value with appropriate styling based on the metric type
 * @param metricType The type of metric
 * @param value The value of the metric
 * @returns Formatted metric value with appropriate styling
 */
const formatMetricValue = (metricType: string, value: number): React.ReactNode => {
  if (value === undefined || value === null) {
    return '-';
  }

  switch (metricType) {
    case 'successRate':
      return formatSuccessRate(value);
    case 'resolutionTime':
      return formatDuration(value);
    default:
      return <Typography variant="body1">{formatNumber(value)}</Typography>;
  }
};

/**
 * Returns the appropriate icon for a metric type
 * @param metricType The type of metric
 * @returns Icon component for the metric type
 */
const getMetricIcon = (metricType: string): React.ReactNode => {
  switch (metricType) {
    case 'successRate':
      return <CheckCircleOutline />;
    case 'resolutionTime':
      return <AccessTime />;
    case 'issuesDetected':
      return <Warning />;
    case 'activeIssues':
      return <Error />;
    default:
      return null;
  }
};

/**
 * Card component for displaying a single metric with title, value, and icon
 */
const MetricCard: React.FC<MetricCardProps> = ({ title, value, metricType, icon, description }) => {
  const formattedValue = formatMetricValue(metricType, value);

  return (
    <Paper elevation={3} style={{ padding: '16px', borderRadius: '8px' }}>
      <Box display="flex" alignItems="center" marginBottom="8px">
        {icon && <Box marginRight="8px">{icon}</Box>}
        <Typography variant="h6" component="div">
          {title}
        </Typography>
      </Box>
      <Typography variant="h4" component="div">
        {formattedValue}
      </Typography>
      {description && (
        <Typography variant="body2" color="textSecondary">
          {description}
        </Typography>
      )}
    </Paper>
  );
};

/**
 * Main dashboard component for the self-healing functionality
 */
const HealingDashboard: React.FC<HealingDashboardProps> = ({ className, dateRange, refreshInterval = 60000, showSettings = false }) => {
  const [dashboardData, setDashboardData] = useState<HealingDashboardData | null>(null);
  const [models, setModels] = useState<AIModel[]>([]);
  const [activeTab, setActiveTab] = useState(0);

  const { get, loading, error } = useApi();

  const fetchDashboardData = async () => {
    try {
      const response = await get<HealingDashboardData>(healingService.getDashboardData, dateRange);
      setDashboardData(response);
    } catch (err) {
      console.error('Failed to fetch dashboard data:', err);
    }
  };

  const fetchModels = async () => {
    try {
      const response = await get<AIModel[]>(healingService.getAIModels, {});
      setModels(response);
    } catch (err) {
      console.error('Failed to fetch AI models:', err);
    }
  };

  useEffect(() => {
    fetchDashboardData();
    fetchModels();

    const intervalId = setInterval(() => {
      fetchDashboardData();
      fetchModels();
    }, refreshInterval);

    return () => clearInterval(intervalId);
  }, [dateRange, refreshInterval, get]);

  const aiDetectionModels = useMemo(() => models.filter(model => model.modelType === ModelType.DETECTION), [models]);
  const aiCorrectionModels = useMemo(() => models.filter(model => model.modelType === ModelType.CORRECTION), [models]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  return (
    <Grid container spacing={3} className={className}>
      {/* Metrics Section */}
      <Grid item xs={12}>
        <Typography variant="h4" gutterBottom>
          Self-Healing Overview
        </Typography>
        <Grid container spacing={3}>
          <Grid item xs={12} md={4}>
            <Card title="Total Issues Detected" value={dashboardData?.totalIssuesDetected || 0} metricType="issuesDetected" icon={<Warning />} description="Total number of issues detected" />
          </Grid>
          <Grid item xs={12} md={4}>
            <Card title="Issues Resolved Automatically" value={dashboardData?.issuesResolvedAutomatically || 0} metricType="issuesResolved" icon={<CheckCircleOutline />} description="Number of issues resolved without manual intervention" />
          </Grid>
          <Grid item xs={12} md={4}>
            <Card title="Overall Success Rate" value={dashboardData?.overallSuccessRate || 0} metricType="successRate" icon={<Speed />} description="Percentage of issues successfully resolved" />
          </Grid>
          <Grid item xs={12} md={4}>
            <Card title="Average Resolution Time" value={dashboardData?.averageResolutionTime || 0} metricType="resolutionTime" icon={<AccessTime />} description="Average time taken to resolve an issue" />
          </Grid>
        </Grid>
      </Grid>

      {/* Tabs Section */}
      <Grid item xs={12}>
        <Paper>
          <Tabs value={activeTab} onChange={handleTabChange} aria-label="self-healing dashboard tabs">
            <Tab label="Issues" />
            <Tab label="Actions" />
            <Tab label="Performance" />
            <Tab label="Models" />
            {showSettings && <Tab label="Settings" />}
          </Tabs>
        </Paper>
      </Grid>

      {/* Tab Content */}
      <Grid item xs={12}>
        {activeTab === 0 && (
          <ActiveIssuesTable
            onIssueSelect={(issue) => console.log('Selected issue:', issue)}
          />
        )}
        {activeTab === 1 && (
          <HealingActionsTable
            onActionSelect={(action) => console.log('Selected action:', action)}
            onActionEdit={(action) => console.log('Edit action:', action)}
            onActionDelete={(action) => console.log('Delete action:', action)}
          />
        )}
        {activeTab === 2 && (
          <SuccessRateChart dateRange={dateRange} />
        )}
        {activeTab === 3 && (
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <ModelPerformanceCard modelId="1" model={aiDetectionModels[0]} title="Detection Model Performance" />
            </Grid>
            <Grid item xs={12} md={6}>
              <ModelHealthCard modelId="1" model={aiDetectionModels[0]} title="Detection Model Health" />
            </Grid>
          </Grid>
        )}
        {activeTab === 4 && showSettings && (
          <HealingSettingsForm />
        )}
      </Grid>
    </Grid>
  );
};

export default HealingDashboard;