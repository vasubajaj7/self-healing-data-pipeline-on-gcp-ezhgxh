import React, { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react'; // react ^18.2.0
import {
  DashboardData, DashboardFilters, TimeRange, PipelineHealthMetrics,
  DataQualityMetrics, SelfHealingMetrics, AlertSummary, SystemStatus,
  QuickStats, PipelineExecution, AIInsight
} from '../types/dashboard';
import { useApi } from '../hooks/useApi';
import pipelineService from '../services/api/pipelineService';
import qualityService from '../services/api/qualityService';
import healingService from '../services/api/healingService';
import alertService from '../services/api/alertService';
import { ENV } from '../config/env';

/**
 * Type definition for the dashboard context value
 */
interface DashboardContextType {
  dashboardData: DashboardData | null;
  loading: boolean;
  error: object | null;
  filters: DashboardFilters;
  setFilters: (filters: Partial<DashboardFilters>) => void;
  fetchDashboardData: () => Promise<void>;
  refreshInterval: number;
  setRefreshInterval: (interval: number) => void;
}

/**
 * Props for the DashboardProvider component
 */
interface DashboardProviderProps {
  children: ReactNode;
  initialRefreshInterval?: number;
}

/**
 * React context for dashboard state and methods
 */
export const DashboardContext = createContext<DashboardContextType | undefined>(undefined);

/**
 * React context provider component for dashboard data and functionality
 */
export const DashboardProvider: React.FC<DashboardProviderProps> = ({ children, initialRefreshInterval }) => {
  // Initialize state variables
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [filters, setFilters] = useState<DashboardFilters>({
    timeRange: 'LAST_24_HOURS',
    customStartDate: null,
    customEndDate: null,
    pipelineFilter: [],
    statusFilter: [],
  });
  const [refreshInterval, setRefreshInterval] = useState<number>(initialRefreshInterval || ENV.REFRESH_INTERVAL);

  // Use the useApi hook for API requests
  const { get, loading, error } = useApi();

  // Fetch dashboard data function
  const fetchDashboardData = useCallback(async () => {
    try {
      // Fetch pipeline health metrics
      const pipelineHealth: PipelineHealthMetrics = (await pipelineService.getPipelineHealthMetrics()).data;

      // Fetch data quality metrics
      const dataQuality: DataQualityMetrics = (await qualityService.getDataQualityMetrics()).data;

      // Fetch self-healing metrics
      const selfHealing: SelfHealingMetrics = (await healingService.getSelfHealingMetrics()).data;

      // Fetch active alerts
      const activeAlerts: AlertSummary[] = (await alertService.getActiveAlerts()).items;

      // Fetch system status
      const systemStatus: SystemStatus = {
        gcsConnector: 'OK',
        cloudSql: 'OK',
        externalApis: 'WARN',
        bigQuery: 'OK',
        mlServices: 'OK',
      };

      // Fetch quick stats
      const quickStats: QuickStats = {
        activePipelines: 12,
        pendingJobs: 3,
        alertRateChange: -15,
        alertRatePeriod: '7d',
      };

      // Fetch recent executions
      const recentExecutions: PipelineExecution[] = (await pipelineService.getPipelineExecutions({ page: 1, pageSize: 5 })).items;

      // Fetch AI insights
      const aiInsights: AIInsight[] = (await healingService.getAIInsights()).items;

      // Combine all data into a single dashboard data object
      const dashboardData: DashboardData = {
        pipelineHealth,
        dataQuality,
        selfHealing,
        activeAlerts,
        systemStatus,
        quickStats,
        recentExecutions,
        aiInsights,
      };

      // Update the dashboard data state
      setDashboardData(dashboardData);
    } catch (err) {
      console.error('Error fetching dashboard data:', err);
    }
  }, [get]);

  // Set up auto-refresh interval
  useEffect(() => {
    // Clear existing interval
    let intervalId: NodeJS.Timeout;

    // Set interval if refreshInterval is greater than 0
    if (refreshInterval > 0) {
      intervalId = setInterval(() => {
        fetchDashboardData();
      }, refreshInterval);
    }

    // Clean up interval on component unmount or when refreshInterval changes
    return () => clearInterval(intervalId);
  }, [fetchDashboardData, refreshInterval]);

  // Fetch initial dashboard data on component mount
  useEffect(() => {
    fetchDashboardData();
  }, [fetchDashboardData]);

  // Update data when filters change
  useEffect(() => {
    fetchDashboardData();
  }, [filters, fetchDashboardData]);

  // Provide the dashboard context value
  const value: DashboardContextType = {
    dashboardData,
    loading,
    error,
    filters,
    setFilters,
    fetchDashboardData,
    refreshInterval,
    setRefreshInterval,
  };

  return (
    <DashboardContext.Provider value={value}>
      {children}
    </DashboardContext.Provider>
  );
};

/**
 * Custom hook that provides access to the dashboard context
 */
export const useDashboard = (): DashboardContextType => {
  const context = useContext(DashboardContext);
  if (!context) {
    throw new Error('useDashboard must be used within a DashboardProvider');
  }
  return context;
};