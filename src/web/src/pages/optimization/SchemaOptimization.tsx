import React, { useState, useEffect, useCallback } from 'react';
import { 
  Typography, 
  Box, 
  Paper, 
  Breadcrumbs, 
  Link, 
  Divider, 
  Grid, 
  Button, 
  CircularProgress, 
  Alert 
} from '@mui/material';
import { 
  StorageOutlined, 
  NavigateNext, 
  Refresh 
} from '@mui/icons-material';

import PageContainer from '../../components/layout/PageContainer';
import SchemaOptimizationCard from '../../components/optimization/SchemaOptimizationCard';
import useNotification from '../../hooks/useNotification';
import optimizationService from '../../services/api/optimizationService';

/**
 * Schema Optimization page component
 * Provides interface for viewing and managing BigQuery schema optimization recommendations
 * such as partitioning and clustering to improve query performance and reduce costs
 */
const SchemaOptimization: React.FC = () => {
  // Initialize notification hook for displaying success/error messages
  const { showSuccess, showInfo } = useNotification();
  
  // State for refresh mechanism and loading indicator
  const [refreshTrigger, setRefreshTrigger] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(false);

  // Handle successful application of a schema optimization recommendation
  const handleRecommendationApplied = useCallback(() => {
    showSuccess('Schema optimization successfully applied');
    // Refresh recommendations to show updated status
    setRefreshTrigger(prev => prev + 1);
  }, [showSuccess]);

  // Handle rejection of a schema optimization recommendation
  const handleRecommendationRejected = useCallback(() => {
    showInfo('Schema optimization recommendation rejected');
    // Refresh recommendations to show updated status
    setRefreshTrigger(prev => prev + 1);
  }, [showInfo]);

  // Handle manual refresh button click
  const handleRefresh = useCallback(() => {
    setLoading(true);
    setRefreshTrigger(prev => prev + 1);
    
    // Add a small delay for better UX
    setTimeout(() => {
      setLoading(false);
    }, 500);
  }, []);

  return (
    <PageContainer>
      {/* Header with title, breadcrumbs, and refresh button */}
      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center', 
        marginBottom: '24px' 
      }}>
        <Box sx={{ display: 'flex', flexDirection: 'column' }}>
          <Typography variant="h4" component="h1" gutterBottom>
            Schema Optimization
          </Typography>
          <Breadcrumbs separator={<NavigateNext fontSize="small" />}>
            <Link color="inherit" href="/dashboard">Dashboard</Link>
            <Link color="inherit" href="/optimization">Optimization</Link>
            <Typography color="textPrimary">Schema</Typography>
          </Breadcrumbs>
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Button 
            variant="outlined" 
            startIcon={loading ? <CircularProgress size={20} /> : <Refresh />} 
            onClick={handleRefresh}
            disabled={loading}
          >
            {loading ? 'Refreshing...' : 'Refresh'}
          </Button>
        </Box>
      </Box>

      {/* Informational text about schema optimization */}
      <Box sx={{ marginBottom: '24px' }}>
        <Typography variant="body1" paragraph>
          Schema optimization recommendations help you improve BigQuery query performance and reduce costs by 
          implementing optimal partitioning, clustering, and column organization strategies for your tables.
        </Typography>
        <Typography variant="body2" color="textSecondary">
          Each recommendation includes estimated performance improvements, cost savings, and an implementation
          script that can be applied automatically or reviewed before implementation.
        </Typography>
      </Box>

      {/* Main content - Schema optimization recommendations */}
      <SchemaOptimizationCard 
        key={refreshTrigger} // Key changes force re-render on refresh
        onRecommendationApplied={handleRecommendationApplied}
        onRecommendationRejected={handleRecommendationRejected}
      />
    </PageContainer>
  );
};

export default SchemaOptimization;