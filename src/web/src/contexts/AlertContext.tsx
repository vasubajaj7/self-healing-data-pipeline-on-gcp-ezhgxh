import React, { 
  createContext, 
  useContext, 
  ReactNode, 
  useState, 
  useEffect, 
  useCallback,
  useMemo
} from 'react';

import { 
  Alert, 
  AlertFilter, 
  AlertStats, 
  AlertAcknowledgement, 
  AlertEscalation, 
  AlertResolution, 
  AlertSuppression, 
  SuggestedAction,
  AlertStatus,
  AlertType,
  AlertSeverity
} from '../types/alerts';

import alertService from '../services/api/alertService';
import { useInterval } from '../hooks/useInterval';
import { PaginatedResponse } from '../types/api';

// Interface defining the alert context shape
interface AlertContextType {
  alerts: Alert[];
  selectedAlert: Alert | null;
  alertStats: AlertStats | null;
  loading: boolean;
  error: string | null;
  filters: AlertFilter;
  pagination: {
    page: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
  };
  refreshInterval: number;
  lastUpdated: Date | null;
  fetchAlerts: () => Promise<void>;
  fetchAlertById: (alertId: string) => Promise<Alert | null>;
  fetchAlertStats: (timeRange?: string) => Promise<AlertStats | null>;
  setFilters: (filters: Partial<AlertFilter>) => void;
  setPage: (page: number) => void;
  setPageSize: (pageSize: number) => void;
  setRefreshInterval: (intervalMs: number) => void;
  selectAlert: (alert: Alert | null) => void;
  acknowledgeAlert: (alertId: string, acknowledgement: AlertAcknowledgement) => Promise<boolean>;
  escalateAlert: (alertId: string, escalation: AlertEscalation) => Promise<boolean>;
  resolveAlert: (alertId: string, resolution: AlertResolution) => Promise<boolean>;
  suppressSimilarAlerts: (alertId: string, suppression: AlertSuppression) => Promise<boolean>;
  getRelatedAlerts: (alertId: string) => Promise<Alert[]>;
  getSuggestedActions: (alertId: string) => Promise<SuggestedAction[]>;
}

// Props for the AlertProvider component
interface AlertProviderProps {
  children: ReactNode;
}

// State interface for the alert context
interface AlertState {
  alerts: Alert[];
  selectedAlert: Alert | null;
  alertStats: AlertStats | null;
  loading: boolean;
  error: string | null;
  filters: AlertFilter;
  page: number;
  pageSize: number;
  totalItems: number;
  totalPages: number;
  refreshInterval: number;
  lastUpdated: Date | null;
}

// Create the context with a default value of undefined
const AlertContext = createContext<AlertContextType | undefined>(undefined);

// Provider component that makes alert data available throughout the application
const AlertProvider: React.FC<AlertProviderProps> = ({ children }) => {
  // Initialize alert state
  const [alertState, setAlertState] = useState<AlertState>({
    alerts: [],
    selectedAlert: null,
    alertStats: null,
    loading: false,
    error: null,
    filters: {
      severity: undefined,
      status: undefined,
      type: undefined,
      source: undefined,
      component: undefined,
      startDate: undefined,
      endDate: undefined,
      search: undefined
    },
    page: 1,
    pageSize: 10,
    totalItems: 0,
    totalPages: 0,
    refreshInterval: 60000, // Default: 1 minute
    lastUpdated: null
  });

  // Fetches alerts based on current filters and pagination
  const fetchAlerts = useCallback(async () => {
    setAlertState(prev => ({ ...prev, loading: true, error: null }));
    
    try {
      const { filters, page, pageSize } = alertState;
      const response = await alertService.getAlerts(filters, page, pageSize);
      
      setAlertState(prev => ({
        ...prev,
        alerts: response.items,
        totalItems: response.pagination.totalItems,
        totalPages: response.pagination.totalPages,
        loading: false,
        lastUpdated: new Date(),
        error: null
      }));
    } catch (error) {
      console.error('Error fetching alerts:', error);
      setAlertState(prev => ({
        ...prev,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to fetch alerts'
      }));
    }
  }, [alertState.filters, alertState.page, alertState.pageSize]);

  // Fetches a specific alert by ID
  const fetchAlertById = useCallback(async (alertId: string): Promise<Alert | null> => {
    setAlertState(prev => ({ ...prev, loading: true, error: null }));
    
    try {
      const alert = await alertService.getAlertById(alertId);
      setAlertState(prev => ({ ...prev, loading: false }));
      return alert;
    } catch (error) {
      console.error('Error fetching alert by ID:', error);
      setAlertState(prev => ({
        ...prev,
        loading: false,
        error: error instanceof Error ? error.message : `Failed to fetch alert ${alertId}`
      }));
      return null;
    }
  }, []);

  // Fetches alert statistics for dashboards
  const fetchAlertStats = useCallback(async (timeRange: string = '24h'): Promise<AlertStats | null> => {
    setAlertState(prev => ({ ...prev, loading: true, error: null }));
    
    try {
      const stats = await alertService.getAlertStats(timeRange);
      setAlertState(prev => ({
        ...prev,
        alertStats: stats,
        loading: false,
        error: null
      }));
      return stats;
    } catch (error) {
      console.error('Error fetching alert statistics:', error);
      setAlertState(prev => ({
        ...prev,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to fetch alert statistics'
      }));
      return null;
    }
  }, []);

  // Updates alert filters and refreshes data
  const setFilters = useCallback((newFilters: Partial<AlertFilter>) => {
    setAlertState(prev => {
      // Merge new filters with existing filters
      const updatedFilters = { ...prev.filters, ...newFilters };
      
      // Reset to page 1 when filters change
      return {
        ...prev,
        filters: updatedFilters,
        page: 1
      };
    });
    
    // Fetch alerts with new filters (in the next tick to ensure state is updated)
    setTimeout(() => fetchAlerts(), 0);
  }, [fetchAlerts]);

  // Updates the current page and refreshes data
  const setPage = useCallback((page: number) => {
    setAlertState(prev => ({
      ...prev,
      page
    }));
    
    // Fetch alerts with new page (in the next tick to ensure state is updated)
    setTimeout(() => fetchAlerts(), 0);
  }, [fetchAlerts]);

  // Updates the page size and refreshes data
  const setPageSize = useCallback((pageSize: number) => {
    setAlertState(prev => ({
      ...prev,
      pageSize,
      page: 1 // Reset to first page when page size changes
    }));
    
    // Fetch alerts with new page size (in the next tick to ensure state is updated)
    setTimeout(() => fetchAlerts(), 0);
  }, [fetchAlerts]);

  // Updates the automatic refresh interval for alert data
  const setRefreshInterval = useCallback((intervalMs: number) => {
    setAlertState(prev => ({
      ...prev,
      refreshInterval: intervalMs
    }));
  }, []);

  // Sets the currently selected alert
  const selectAlert = useCallback((alert: Alert | null) => {
    setAlertState(prev => ({
      ...prev,
      selectedAlert: alert
    }));
  }, []);

  // Acknowledges an alert to indicate it's being handled
  const acknowledgeAlert = useCallback(async (
    alertId: string, 
    acknowledgement: AlertAcknowledgement
  ): Promise<boolean> => {
    try {
      await alertService.acknowledgeAlert(
        alertId, 
        acknowledgement.acknowledgedBy, 
        acknowledgement.comments
      );
      
      // Refresh alerts after acknowledgement
      fetchAlerts();
      
      // If the acknowledged alert is the currently selected alert, update it
      if (alertState.selectedAlert && alertState.selectedAlert.alertId === alertId) {
        const updatedAlert = await fetchAlertById(alertId);
        if (updatedAlert) {
          selectAlert(updatedAlert);
        }
      }
      
      return true;
    } catch (error) {
      console.error('Error acknowledging alert:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to acknowledge alert ${alertId}`
      }));
      return false;
    }
  }, [alertState.selectedAlert, fetchAlertById, fetchAlerts, selectAlert]);

  // Escalates an alert to a higher priority level or team
  const escalateAlert = useCallback(async (
    alertId: string, 
    escalation: AlertEscalation
  ): Promise<boolean> => {
    try {
      await alertService.escalateAlert(
        alertId, 
        escalation.escalatedBy, 
        escalation.escalationReason, 
        escalation.escalationLevel
      );
      
      // Refresh alerts after escalation
      fetchAlerts();
      
      // If the escalated alert is the currently selected alert, update it
      if (alertState.selectedAlert && alertState.selectedAlert.alertId === alertId) {
        const updatedAlert = await fetchAlertById(alertId);
        if (updatedAlert) {
          selectAlert(updatedAlert);
        }
      }
      
      return true;
    } catch (error) {
      console.error('Error escalating alert:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to escalate alert ${alertId}`
      }));
      return false;
    }
  }, [alertState.selectedAlert, fetchAlertById, fetchAlerts, selectAlert]);

  // Marks an alert as resolved
  const resolveAlert = useCallback(async (
    alertId: string, 
    resolution: AlertResolution
  ): Promise<boolean> => {
    try {
      await alertService.resolveAlert(
        alertId, 
        resolution.resolvedBy, 
        resolution.resolutionNotes
      );
      
      // Refresh alerts after resolution
      fetchAlerts();
      
      // If the resolved alert is the currently selected alert, update it
      if (alertState.selectedAlert && alertState.selectedAlert.alertId === alertId) {
        const updatedAlert = await fetchAlertById(alertId);
        if (updatedAlert) {
          selectAlert(updatedAlert);
        }
      }
      
      return true;
    } catch (error) {
      console.error('Error resolving alert:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to resolve alert ${alertId}`
      }));
      return false;
    }
  }, [alertState.selectedAlert, fetchAlertById, fetchAlerts, selectAlert]);

  // Suppresses similar alerts to reduce alert noise
  const suppressSimilarAlerts = useCallback(async (
    alertId: string, 
    suppression: AlertSuppression
  ): Promise<boolean> => {
    try {
      await alertService.suppressSimilarAlerts(
        alertId, 
        suppression.suppressedBy, 
        suppression.durationMinutes, 
        suppression.suppressionReason
      );
      
      // Refresh alerts after suppression
      fetchAlerts();
      
      return true;
    } catch (error) {
      console.error('Error suppressing similar alerts:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to suppress similar alerts ${alertId}`
      }));
      return false;
    }
  }, [fetchAlerts]);

  // Retrieves alerts related to a specific alert
  const getRelatedAlerts = useCallback(async (alertId: string): Promise<Alert[]> => {
    try {
      const relatedAlerts = await alertService.getRelatedAlerts(alertId);
      return relatedAlerts;
    } catch (error) {
      console.error('Error fetching related alerts:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to fetch related alerts for ${alertId}`
      }));
      return [];
    }
  }, []);

  // Retrieves AI-suggested actions for an alert
  const getSuggestedActions = useCallback(async (alertId: string): Promise<SuggestedAction[]> => {
    try {
      const { actions } = await alertService.getSuggestedActions(alertId);
      return actions;
    } catch (error) {
      console.error('Error fetching suggested actions:', error);
      setAlertState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : `Failed to fetch suggested actions for ${alertId}`
      }));
      return [];
    }
  }, []);

  // Fetch initial alerts on component mount
  useEffect(() => {
    fetchAlerts();
    fetchAlertStats();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Set up periodic refresh using useInterval hook with refreshInterval
  useInterval(() => {
    fetchAlerts();
  }, alertState.refreshInterval);

  // Create context value object with state and alert methods
  const contextValue = useMemo(() => ({
    // Alert data
    alerts: alertState.alerts,
    selectedAlert: alertState.selectedAlert,
    alertStats: alertState.alertStats,
    
    // UI state
    loading: alertState.loading,
    error: alertState.error,
    
    // Filtering and pagination
    filters: alertState.filters,
    pagination: {
      page: alertState.page,
      pageSize: alertState.pageSize,
      totalItems: alertState.totalItems,
      totalPages: alertState.totalPages
    },
    
    // Refresh configuration
    refreshInterval: alertState.refreshInterval,
    lastUpdated: alertState.lastUpdated,
    
    // Methods
    fetchAlerts,
    fetchAlertById,
    fetchAlertStats,
    setFilters,
    setPage,
    setPageSize,
    setRefreshInterval,
    selectAlert,
    acknowledgeAlert,
    escalateAlert,
    resolveAlert,
    suppressSimilarAlerts,
    getRelatedAlerts,
    getSuggestedActions
  }), [
    alertState.alerts,
    alertState.selectedAlert,
    alertState.alertStats,
    alertState.loading,
    alertState.error,
    alertState.filters,
    alertState.page,
    alertState.pageSize,
    alertState.totalItems,
    alertState.totalPages,
    alertState.refreshInterval,
    alertState.lastUpdated,
    fetchAlerts,
    fetchAlertById,
    fetchAlertStats,
    setFilters,
    setPage,
    setPageSize,
    setRefreshInterval,
    selectAlert,
    acknowledgeAlert,
    escalateAlert,
    resolveAlert,
    suppressSimilarAlerts,
    getRelatedAlerts,
    getSuggestedActions
  ]);

  return (
    <AlertContext.Provider value={contextValue}>
      {children}
    </AlertContext.Provider>
  );
};

// Custom hook that provides access to the AlertContext
function useAlertContext(): AlertContextType {
  const context = useContext(AlertContext);
  
  if (context === undefined) {
    throw new Error('useAlertContext must be used within an AlertProvider');
  }
  
  return context;
}

export { AlertContext, AlertProvider, useAlertContext };