import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Grid,
  Divider,
  Tabs,
  Tab,
  CircularProgress,
  Alert,
  Chip,
  Tooltip,
  Button
} from '@mui/material';
import {
  SpeedOutlined,
  TrendingUpOutlined,
  StorageOutlined,
  AttachMoneyOutlined,
  CodeOutlined,
  HistoryOutlined
} from '@mui/icons-material';
import { format, subDays } from 'date-fns';
import Prism from 'prismjs';
import { useParams, useNavigate } from 'react-router-dom';

import PageContainer from '../../components/layout/PageContainer';
import Card from '../../components/common/Card';
import QueryPerformanceTable from '../../components/optimization/QueryPerformanceTable';
import OptimizationRecommendations from '../../components/optimization/OptimizationRecommendations';
import useApi from '../../hooks/useApi';
import { getQueryDetails } from '../../services/api/optimizationService';
import useNotification from '../../hooks/useNotification';
import { OptimizationType, QueryDetails } from '../../types/optimization';

// Props for the TabPanel component
interface TabPanelProps {
  children: React.ReactNode;
  value: number;
  index: number;
}

// TabPanel component to handle tab content display
const TabPanel = ({ children, value, index }: TabPanelProps) => {
  return (
    <Box
      role="tabpanel"
      hidden={value !== index}
      id={`optimization-tabpanel-${index}`}
      aria-labelledby={`optimization-tab-${index}`}
      sx={{ pt: 2 }}
    >
      {value === index && children}
    </Box>
  );
};

// Main component for query optimization page
const QueryOptimization: React.FC = () => {
  // Get queryId from URL params
  const { queryId } = useParams<{ queryId?: string }>();
  const navigate = useNavigate();
  
  // State for selected query details
  const [selectedQuery, setSelectedQuery] = useState<QueryDetails | null>(null);
  // State for active tab
  const [activeTab, setActiveTab] = useState(0);
  // State for time range filter
  const [timeRange, setTimeRange] = useState({
    startDate: format(subDays(new Date(), 7), 'yyyy-MM-dd'),
    endDate: format(new Date(), 'yyyy-MM-dd')
  });
  
  // API hooks
  const { get, loading, error } = useApi();
  const { showSuccess, showInfo, showError } = useNotification();
  
  // Load query details when queryId changes
  const loadQueryDetails = useCallback(async () => {
    if (!queryId) return;
    
    try {
      const response = await getQueryDetails(queryId);
      setSelectedQuery(response.data);
    } catch (err) {
      console.error('Failed to load query details:', err);
      showError('Failed to load query details');
    }
  }, [queryId, showError]);
  
  // Effect to load query details when queryId changes
  useEffect(() => {
    if (queryId) {
      loadQueryDetails();
    }
  }, [queryId, loadQueryDetails]);
  
  // Effect to initialize Prism syntax highlighting when a query is selected
  useEffect(() => {
    if (selectedQuery && Prism) {
      setTimeout(() => Prism.highlightAll(), 0);
    }
  }, [selectedQuery, activeTab]);
  
  // Handle tab change
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };
  
  // Handle query selection from the performance table
  const handleQuerySelect = (queryId: string) => {
    navigate(`/optimization/${queryId}`);
  };
  
  // Handle optimization recommendation application
  const handleRecommendationApplied = () => {
    showSuccess('Optimization successfully applied');
    if (queryId) {
      loadQueryDetails();
    }
  };
  
  // Handle optimization recommendation rejection
  const handleRecommendationRejected = () => {
    showInfo('Optimization recommendation rejected');
    if (queryId) {
      loadQueryDetails();
    }
  };
  
  // Handle navigation back to the query list
  const handleBackToList = () => {
    setSelectedQuery(null);
    navigate('/optimization');
  };
  
  // Render query details view
  const renderQueryDetails = () => {
    if (!selectedQuery) return null;
    
    return (
      <Box>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs value={activeTab} onChange={handleTabChange} aria-label="query details tabs">
            <Tab icon={<SpeedOutlined />} iconPosition="start" label="Overview" />
            <Tab icon={<TrendingUpOutlined />} iconPosition="start" label="Recommendations" />
            <Tab icon={<CodeOutlined />} iconPosition="start" label="Execution Plan" />
            <Tab icon={<HistoryOutlined />} iconPosition="start" label="History" />
          </Tabs>
        </Box>
        
        <TabPanel value={activeTab} index={0}>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6} lg={3}>
              <Card>
                <Box display="flex" flexDirection="column" p={2}>
                  <Typography variant="subtitle2" color="textSecondary">Execution Time</Typography>
                  <Box display="flex" alignItems="center">
                    <SpeedOutlined color="primary" sx={{ mr: 1 }} />
                    <Typography variant="h6">
                      {selectedQuery.executionTime > 60000 
                        ? `${Math.round(selectedQuery.executionTime / 60000)}m ${Math.round((selectedQuery.executionTime % 60000) / 1000)}s`
                        : `${(selectedQuery.executionTime / 1000).toFixed(2)}s`}
                    </Typography>
                  </Box>
                </Box>
              </Card>
            </Grid>
            
            <Grid item xs={12} md={6} lg={3}>
              <Card>
                <Box display="flex" flexDirection="column" p={2}>
                  <Typography variant="subtitle2" color="textSecondary">Bytes Processed</Typography>
                  <Box display="flex" alignItems="center">
                    <StorageOutlined color="primary" sx={{ mr: 1 }} />
                    <Typography variant="h6">
                      {selectedQuery.bytesProcessed >= 1073741824 
                        ? `${(selectedQuery.bytesProcessed / 1073741824).toFixed(2)} GB`
                        : selectedQuery.bytesProcessed >= 1048576 
                          ? `${(selectedQuery.bytesProcessed / 1048576).toFixed(2)} MB`
                          : `${(selectedQuery.bytesProcessed / 1024).toFixed(2)} KB`}
                    </Typography>
                  </Box>
                </Box>
              </Card>
            </Grid>
            
            <Grid item xs={12} md={6} lg={3}>
              <Card>
                <Box display="flex" flexDirection="column" p={2}>
                  <Typography variant="subtitle2" color="textSecondary">Slot Milliseconds</Typography>
                  <Box display="flex" alignItems="center">
                    <TrendingUpOutlined color="primary" sx={{ mr: 1 }} />
                    <Typography variant="h6">
                      {selectedQuery.slotMilliseconds.toLocaleString()}
                    </Typography>
                  </Box>
                </Box>
              </Card>
            </Grid>
            
            <Grid item xs={12} md={6} lg={3}>
              <Card>
                <Box display="flex" flexDirection="column" p={2}>
                  <Typography variant="subtitle2" color="textSecondary">Estimated Cost</Typography>
                  <Box display="flex" alignItems="center">
                    <AttachMoneyOutlined color="primary" sx={{ mr: 1 }} />
                    <Typography variant="h6">
                      ${selectedQuery.estimatedCost.toFixed(2)}
                    </Typography>
                  </Box>
                </Box>
              </Card>
            </Grid>
          </Grid>
          
          <Box mt={3}>
            <Typography variant="h6" gutterBottom>Query Metadata</Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={6}>
                <Typography variant="body2" color="textSecondary">
                  <strong>Execution Date:</strong> {format(new Date(selectedQuery.executionDateTime), 'MMM dd, yyyy HH:mm:ss')}
                </Typography>
              </Grid>
              <Grid item xs={12} sm={6}>
                <Typography variant="body2" color="textSecondary">
                  <strong>User:</strong> {selectedQuery.user}
                </Typography>
              </Grid>
              <Grid item xs={12} sm={6}>
                <Typography variant="body2" color="textSecondary">
                  <strong>Project:</strong> {selectedQuery.project}
                </Typography>
              </Grid>
              <Grid item xs={12} sm={6}>
                <Typography variant="body2" color="textSecondary">
                  <strong>Referenced Tables:</strong> {selectedQuery.referencedTables?.map(t => `${t.datasetId}.${t.tableId}`).join(', ') || 'None'}
                </Typography>
              </Grid>
            </Grid>
          </Box>
        </TabPanel>
        
        <TabPanel value={activeTab} index={1}>
          <Box>
            {selectedQuery.optimizationRecommendations && selectedQuery.optimizationRecommendations.length > 0 ? (
              <OptimizationRecommendations 
                type={OptimizationType.QUERY}
                title="Query-Specific Recommendations"
                maxItems={10}
                onRecommendationApplied={handleRecommendationApplied}
                onRecommendationRejected={handleRecommendationRejected}
              />
            ) : (
              <Alert severity="info">No specific recommendations for this query.</Alert>
            )}
          </Box>
        </TabPanel>
        
        <TabPanel value={activeTab} index={2}>
          <Box>
            <Typography variant="h6" gutterBottom>Execution Plan</Typography>
            <Typography variant="body2" color="textSecondary" paragraph>
              The visualization below shows how BigQuery executes this query, including the execution stages and their relationships.
            </Typography>
            <Box sx={{
              backgroundColor: '#f5f5f5',
              p: 2,
              borderRadius: 1,
              overflowX: 'auto',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              maxHeight: '400px',
              overflowY: 'auto'
            }}>
              <pre>
                <code className="language-json">{JSON.stringify(selectedQuery.executionPlan, null, 2)}</code>
              </pre>
            </Box>
            <Box mt={4}>
              <Typography variant="h6" gutterBottom>Execution Statistics</Typography>
              <Box sx={{
                backgroundColor: '#f5f5f5',
                p: 2,
                borderRadius: 1,
                overflowX: 'auto',
                fontFamily: 'monospace',
                fontSize: '0.875rem',
                maxHeight: '400px',
                overflowY: 'auto'
              }}>
                <pre>
                  <code className="language-json">{JSON.stringify(selectedQuery.executionStats, null, 2)}</code>
                </pre>
              </Box>
            </Box>
          </Box>
        </TabPanel>
        
        <TabPanel value={activeTab} index={3}>
          <Box>
            <Typography variant="h6" gutterBottom>Query History</Typography>
            <Typography variant="body2" color="textSecondary" paragraph>
              Historical performance of similar queries over time.
            </Typography>
            
            <Alert severity="info" sx={{ mb: 2 }}>
              Historical data is being gathered. Check back later for more insights.
            </Alert>
            
            {/* Historical charts would be added here */}
          </Box>
        </TabPanel>
      </Box>
    );
  };
  
  return (
    <PageContainer>
      {queryId ? (
        // Query details view
        <Box>
          <Box display="flex" alignItems="center" mb={3}>
            <Button 
              variant="outlined" 
              onClick={handleBackToList} 
              sx={{ mr: 2 }}
              startIcon={<ArrowUpward sx={{ transform: 'rotate(-90deg)' }} />}
            >
              Back to Query List
            </Button>
            <Typography variant="h5">
              Query Details
            </Typography>
          </Box>
          
          <Card>
            {loading ? (
              <Box display="flex" justifyContent="center" alignItems="center" height="300px">
                <CircularProgress />
              </Box>
            ) : error ? (
              <Alert severity="error">
                {error instanceof Error ? error.message : 'An error occurred loading query details'}
              </Alert>
            ) : !selectedQuery ? (
              <Alert severity="info">No query found with the provided ID.</Alert>
            ) : (
              <Box>
                <Box p={2}>
                  <Typography variant="subtitle1" fontWeight="bold">Query Text</Typography>
                  <Box sx={{
                    backgroundColor: '#f5f5f5',
                    p: 2,
                    borderRadius: 1,
                    overflowX: 'auto',
                    fontFamily: 'monospace',
                    fontSize: '0.875rem',
                    maxHeight: '200px',
                    overflowY: 'auto'
                  }}>
                    <pre>
                      <code className="language-sql">{selectedQuery.queryText}</code>
                    </pre>
                  </Box>
                </Box>
                
                {renderQueryDetails()}
              </Box>
            )}
          </Card>
        </Box>
      ) : (
        // Query list view
        <Box>
          <Typography variant="h5" gutterBottom>Query Optimization</Typography>
          <Typography variant="body1" color="textSecondary" paragraph>
            Analyze and optimize your BigQuery queries for better performance and lower costs.
          </Typography>
          
          <QueryPerformanceTable 
            timeRange={timeRange}
            onQuerySelect={handleQuerySelect}
          />
          
          <Box mt={4}>
            <OptimizationRecommendations 
              type={OptimizationType.QUERY}
              title="General Query Optimization Recommendations"
              maxItems={5}
              onRecommendationApplied={handleRecommendationApplied}
              onRecommendationRejected={handleRecommendationRejected}
            />
          </Box>
        </Box>
      )}
    </PageContainer>
  );
};

export default QueryOptimization;