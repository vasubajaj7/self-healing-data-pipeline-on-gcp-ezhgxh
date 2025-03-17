import React, { createContext, useContext, useState, useEffect, useCallback, useMemo, ReactNode } from 'react';
import {
  DatasetQualitySummary, QualityIssue, QualityRuleDetails, QualityValidationResult,
  QualityTimeSeries, ColumnQualityDetails, QualityStatistics, QualityDashboardFilters,
  QualityDimension, QualityRuleType, QualityIssueStatus
} from '../types/quality';
import { QualityRule, QualityValidation, QualityScore, ValidationStatus } from '../types/api';
import qualityService from '../services/api/qualityService';
import { useApi } from '../hooks/useApi';
import { useAuth } from './AuthContext';

/**
 * Type definition for the quality context value
 */
interface QualityContextType {
  // State
  datasets: DatasetQualitySummary[];
  rules: QualityRule[];
  issues: QualityIssue[];
  statistics: QualityStatistics | null;
  timeSeries: QualityTimeSeries | null;
  filters: QualityDashboardFilters;
  loading: boolean;
  error: string | null;
  
  // Methods
  fetchQualityData: () => Promise<void>;
  fetchDatasets: () => Promise<DatasetQualitySummary[]>;
  fetchRules: () => Promise<QualityRule[]>;
  fetchIssues: () => Promise<QualityIssue[]>;
  fetchStatistics: () => Promise<QualityStatistics>;
  fetchTimeSeries: () => Promise<QualityTimeSeries>;
  createRule: (rule: Partial<QualityRule>) => Promise<QualityRule>;
  updateRule: (ruleId: string, rule: Partial<QualityRule>) => Promise<QualityRule>;
  deleteRule: (ruleId: string) => Promise<boolean>;
  updateIssueStatus: (issueId: string, status: QualityIssueStatus, comment?: string) => Promise<QualityIssue>;
  runValidation: (dataset: string, table: string, ruleIds?: string[]) => Promise<string>;
  setFilters: (filters: Partial<QualityDashboardFilters>) => void;
  setRefreshInterval: (interval: number) => void;
}

/**
 * Props for the QualityProvider component
 */
interface QualityProviderProps {
  children: ReactNode;
  initialFilters?: Partial<QualityDashboardFilters>;
  refreshInterval?: number;
}

// Create the context with a default undefined value
const QualityContext = createContext<QualityContextType | undefined>(undefined);

/**
 * Custom hook that provides access to the quality context
 */
export const useQuality = (): QualityContextType => {
  const context = useContext(QualityContext);
  if (context === undefined) {
    throw new Error('useQuality must be used within a QualityProvider');
  }
  return context;
};

/**
 * Hook that implements the quality provider functionality
 */
const useQualityProvider = () => {
  // Initialize state for quality data
  const [datasets, setDatasets] = useState<DatasetQualitySummary[]>([]);
  const [rules, setRules] = useState<QualityRule[]>([]);
  const [issues, setIssues] = useState<QualityIssue[]>([]);
  const [statistics, setStatistics] = useState<QualityStatistics | null>(null);
  const [timeSeries, setTimeSeries] = useState<QualityTimeSeries | null>(null);
  
  // Loading and error states
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  
  // Initialize filters with default values
  const [filters, setFiltersState] = useState<QualityDashboardFilters>({
    dataset: undefined,
    table: undefined,
    dimension: undefined,
    ruleType: undefined,
    severity: undefined,
    status: undefined,
    timeRange: 'LAST_30_DAYS',
    startDate: undefined,
    endDate: undefined,
    minScore: undefined,
    maxScore: undefined,
    searchTerm: undefined
  });
  
  // State for refresh interval
  const [refreshInterval, setRefreshIntervalState] = useState<number>(60000); // Default 1 minute
  
  // Get API methods
  const api = useApi();
  
  // Get auth context for permission checking
  const { checkPermission } = useAuth();
  
  // Fetch all quality data based on current filters
  const fetchQualityData = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Fetch data in parallel to improve performance
      const [datasetsData, rulesData, issuesData, statsData] = await Promise.all([
        fetchDatasets(),
        fetchRules(),
        fetchIssues(),
        fetchStatistics()
      ]);
      
      // Update state with fetched data
      setDatasets(datasetsData);
      setRules(rulesData);
      setIssues(issuesData);
      setStatistics(statsData);
      
      // Fetch time series if dataset filter is set
      if (filters.dataset) {
        try {
          const timeSeriesData = await fetchTimeSeries();
          setTimeSeries(timeSeriesData);
        } catch (err) {
          // Non-critical error, just log it
          console.error('Failed to load time series data:', err);
        }
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load quality data';
      setError(errorMsg);
    } finally {
      setLoading(false);
    }
  }, [filters]);
  
  // Fetch dataset quality summaries
  const fetchDatasets = useCallback(async (): Promise<DatasetQualitySummary[]> => {
    try {
      const response = await qualityService.getDatasetQualitySummaries({
        dataset: filters.dataset
      });
      
      return response.items;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load dataset quality summaries';
      setError(errorMsg);
      return [];
    }
  }, [filters.dataset]);
  
  // Fetch quality rules
  const fetchRules = useCallback(async (): Promise<QualityRule[]> => {
    try {
      const response = await qualityService.getQualityRules({
        page: 1,
        pageSize: 100,
        dataset: filters.dataset,
        table: filters.table,
        ruleType: filters.ruleType,
        isActive: true
      });
      
      return response.items;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load quality rules';
      setError(errorMsg);
      return [];
    }
  }, [filters.dataset, filters.table, filters.ruleType]);
  
  // Fetch quality issues
  const fetchIssues = useCallback(async (): Promise<QualityIssue[]> => {
    try {
      const response = await qualityService.getQualityIssues({
        page: 1,
        pageSize: 100,
        dataset: filters.dataset,
        table: filters.table,
        dimension: filters.dimension,
        severity: filters.severity,
        status: filters.status as QualityIssueStatus,
        startDate: filters.startDate,
        endDate: filters.endDate
      });
      
      return response.items;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load quality issues';
      setError(errorMsg);
      return [];
    }
  }, [filters]);
  
  // Fetch quality statistics
  const fetchStatistics = useCallback(async (): Promise<QualityStatistics> => {
    try {
      const response = await qualityService.getQualityStatistics();
      return response.data;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load quality statistics';
      setError(errorMsg);
      throw err;
    }
  }, []);
  
  // Fetch quality time series data
  const fetchTimeSeries = useCallback(async (): Promise<QualityTimeSeries> => {
    if (!filters.dataset) {
      throw new Error('Dataset must be specified for time series data');
    }
    
    try {
      const response = await qualityService.getQualityTimeSeries({
        dataset: filters.dataset,
        table: filters.table,
        startDate: filters.startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // Default to last 30 days
        endDate: filters.endDate || new Date().toISOString().split('T')[0] // Default to today
      });
      
      return response.data;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load quality time series';
      setError(errorMsg);
      throw err;
    }
  }, [filters.dataset, filters.table, filters.startDate, filters.endDate]);
  
  // Create a new quality rule
  const createRule = useCallback(async (rule: Partial<QualityRule>): Promise<QualityRule> => {
    try {
      // Check permissions
      if (!checkPermission('MANAGE_QUALITY')) {
        throw new Error('You do not have permission to create quality rules');
      }
      
      const response = await qualityService.createQualityRule(rule);
      
      // Update rules state with the new rule
      setRules(prevRules => [...prevRules, response.data]);
      
      // Refresh to ensure we have updated data
      fetchQualityData();
      
      return response.data;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to create quality rule';
      setError(errorMsg);
      throw err;
    }
  }, [checkPermission, fetchQualityData]);
  
  // Update an existing quality rule
  const updateRule = useCallback(async (ruleId: string, rule: Partial<QualityRule>): Promise<QualityRule> => {
    try {
      // Check permissions
      if (!checkPermission('MANAGE_QUALITY')) {
        throw new Error('You do not have permission to update quality rules');
      }
      
      const response = await qualityService.updateQualityRule(ruleId, rule);
      
      // Update rules state with the updated rule
      setRules(prevRules => prevRules.map(r => r.ruleId === ruleId ? response.data : r));
      
      return response.data;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to update quality rule';
      setError(errorMsg);
      throw err;
    }
  }, [checkPermission]);
  
  // Delete a quality rule
  const deleteRule = useCallback(async (ruleId: string): Promise<boolean> => {
    try {
      // Check permissions
      if (!checkPermission('MANAGE_QUALITY')) {
        throw new Error('You do not have permission to delete quality rules');
      }
      
      const response = await qualityService.deleteQualityRule(ruleId);
      
      // Update rules state by removing the deleted rule
      setRules(prevRules => prevRules.filter(r => r.ruleId !== ruleId));
      
      // Refresh data to reflect the deletion
      fetchQualityData();
      
      return response.data.success;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to delete quality rule';
      setError(errorMsg);
      throw err;
    }
  }, [checkPermission, fetchQualityData]);
  
  // Update status of a quality issue
  const updateIssueStatus = useCallback(async (
    issueId: string, 
    status: QualityIssueStatus, 
    comment?: string
  ): Promise<QualityIssue> => {
    try {
      // Check permissions
      if (!checkPermission('MANAGE_QUALITY')) {
        throw new Error('You do not have permission to update issue status');
      }
      
      const response = await qualityService.updateQualityIssueStatus(issueId, { 
        status, 
        comment 
      });
      
      // Update issues state with the updated issue
      setIssues(prevIssues => prevIssues.map(i => i.issueId === issueId ? response.data : i));
      
      return response.data;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to update issue status';
      setError(errorMsg);
      throw err;
    }
  }, [checkPermission]);
  
  // Run a manual quality validation
  const runValidation = useCallback(async (
    dataset: string, 
    table: string, 
    ruleIds?: string[]
  ): Promise<string> => {
    try {
      // Check permissions
      if (!checkPermission('MANAGE_QUALITY')) {
        throw new Error('You do not have permission to run quality validations');
      }
      
      const response = await qualityService.runQualityValidation({
        dataset,
        table,
        ruleIds
      });
      
      // Refresh data after validation is triggered
      fetchQualityData();
      
      return response.data.validationId;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to run quality validation';
      setError(errorMsg);
      throw err;
    }
  }, [checkPermission, fetchQualityData]);
  
  // Update filters and trigger data refresh
  const setFilters = useCallback((newFilters: Partial<QualityDashboardFilters>) => {
    setFiltersState(prevFilters => {
      const updatedFilters = { ...prevFilters, ...newFilters };
      return updatedFilters;
    });
  }, []);
  
  // Update refresh interval
  const setRefreshInterval = useCallback((interval: number) => {
    setRefreshIntervalState(interval);
  }, []);
  
  // Set up automatic refresh
  useEffect(() => {
    // Initial data fetch
    fetchQualityData();
    
    // Set up interval for refreshing data
    const intervalId = setInterval(() => {
      fetchQualityData();
    }, refreshInterval);
    
    // Clean up interval on unmount or refresh interval change
    return () => clearInterval(intervalId);
  }, [fetchQualityData, refreshInterval]);
  
  // Combine state and methods into a context value
  return {
    // State
    datasets,
    rules,
    issues,
    statistics,
    timeSeries,
    filters,
    loading,
    error,
    
    // Methods
    fetchQualityData,
    fetchDatasets,
    fetchRules,
    fetchIssues,
    fetchStatistics,
    fetchTimeSeries,
    createRule,
    updateRule,
    deleteRule,
    updateIssueStatus,
    runValidation,
    setFilters,
    setRefreshInterval
  };
};

/**
 * React context provider component for quality data and operations
 */
export const QualityProvider: React.FC<QualityProviderProps> = ({ 
  children, 
  initialFilters, 
  refreshInterval 
}) => {
  // Get the quality context value from the provider hook
  const qualityContext = useQualityProvider();
  
  // Set initial filters and refresh interval if provided
  useEffect(() => {
    if (initialFilters) {
      qualityContext.setFilters(initialFilters);
    }
    
    if (refreshInterval) {
      qualityContext.setRefreshInterval(refreshInterval);
    }
  }, []);
  
  return (
    <QualityContext.Provider value={qualityContext}>
      {children}
    </QualityContext.Provider>
  );
};

export { QualityContext };