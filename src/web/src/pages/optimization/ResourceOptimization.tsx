import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Grid,
  Typography,
  Box,
  Paper,
  Card,
  CardContent,
  CardHeader,
  Divider,
  CircularProgress,
  Alert,
  Chip,
  Stack,
  useTheme,
  useMediaQuery,
} from '@mui/material'; // @mui/material ^5.11.0
import { Storage, Memory, CloudQueue, DataUsage, TrendingUp, AttachMoney } from '@mui/icons-material'; // @mui/icons-material ^5.11.0

import ResourceUtilizationChart from '../../components/optimization/ResourceUtilizationChart';
import CostAnalysisChart from '../../components/optimization/CostAnalysisChart';
import OptimizationRecommendations from '../../components/optimization/OptimizationRecommendations';
import MainLayout from '../../components/layout/MainLayout';
import optimizationService from '../../services/api/optimizationService';
import useApi from '../../hooks/useApi';
import { OptimizationType, OptimizationSummary } from '../../types/optimization';
import { formatCurrency } from '../../utils/formatting';

/**
 * Main component for the Resource Optimization page
 * @returns Rendered Resource Optimization page
 */
const ResourceOptimization: React.FC = () => {
  // LD1: Initialize state for optimization summary data
  const [optimizationSummary, setOptimizationSummary] = useState<OptimizationSummary | null>(null);

  // LD1: Use useApi hook to manage loading and error states
  const { get, loading, error } = useApi();

  // LD1: Fetch optimization summary data on component mount
  useEffect(() => {
    // LD1: Define an async function to load the optimization summary
    const loadOptimizationSummary = async () => {
      // LD1: Set loading state to true before fetching data
      setLoading(true);
      try {
        // LD1: Fetch the optimization summary from the API
        const response = await get<OptimizationSummary>(optimizationService.getOptimizationSummary());
        // LD1: Update the state with the fetched data
        setOptimizationSummary(response);
      } catch (err: any) {
        // LD1: Set the error state if data fetching fails
        setError(err);
      } finally {
        // LD1: Set loading state to false after fetching is complete
        setLoading(false);
      }
    };

    // LD1: Call the loadOptimizationSummary function when the component mounts
    loadOptimizationSummary();
  }, [get]);

  // LD1: Handle refresh of optimization data when recommendations are applied or rejected
  const handleRecommendationAction = useCallback(() => {
    // LD1: Define an async function to reload the optimization summary
    const loadOptimizationSummary = async () => {
      // LD1: Set loading state to true before fetching data
      setLoading(true);
      try {
        // LD1: Fetch the optimization summary from the API
        const response = await get<OptimizationSummary>(optimizationService.getOptimizationSummary());
        // LD1: Update the state with the fetched data
        setOptimizationSummary(response);
      } catch (err: any) {
        // LD1: Set the error state if data fetching fails
        setError(err);
      } finally {
        // LD1: Set loading state to false after fetching is complete
        setLoading(false);
      }
    };

    // LD1: Call the loadOptimizationSummary function when the component mounts
    loadOptimizationSummary();
  }, [get]);

  // LD1: Render the page layout with grid structure
  return (
    <MainLayout>
      {/* LD1: Render page title and description */}
      <Box sx={{ p: 3 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          Resource Optimization
        </Typography>
        <Typography variant="body1" color="text.secondary" paragraph>
          Monitor resource utilization, analyze costs, and implement optimization recommendations to improve efficiency and reduce costs.
        </Typography>

        {/* LD1: Render Grid container with spacing */}
        <Grid container spacing={3}>
          {/* LD1: First row: ResourceUtilizationChart and CostAnalysisChart in two columns */}
          <Grid item xs={12} md={6}>
            <ResourceUtilizationChart height="400px" />
          </Grid>
          <Grid item xs={12} md={6}>
            <CostAnalysisChart height="400px" />
          </Grid>

          {/* LD1: Second row: Optimization summary card and OptimizationRecommendations component */}
          <Grid item xs={12} md={4}>
            <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <CardHeader title="Optimization Summary" />
              <CardContent sx={{ flexGrow: 1, p: 2 }}>
                {/* LD1: Show loading indicators while data is being fetched */}
                {loading && (
                  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
                    <CircularProgress />
                  </Box>
                )}
                {/* LD1: Show error messages if data fetching fails */}
                {error && <Alert severity="error">Error loading optimization summary: {error.message}</Alert>}
                {/* LD1: Display optimization summary if data is available */}
                {!loading && !error && optimizationSummary && (
                  <Grid container spacing={2}>
                    <Grid item xs={6}>
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', p: 1 }}>
                        <Typography variant="h4" component="div">
                          {optimizationSummary.recommendations_by_type.RESOURCE || 0}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Total Recommendations
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={6}>
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', p: 1 }}>
                        <Typography variant="h4" component="div">
                          {optimizationSummary.applied_recommendations}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Applied
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12}>
                      <Divider sx={{ my: 2 }} />
                    </Grid>
                    <Grid item xs={12}>
                      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', p: 1 }}>
                        <Typography variant="h4" component="div" color="success.main">
                          {formatCurrency(optimizationSummary.savings_by_type.RESOURCE || 0)}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Potential Monthly Savings
                        </Typography>
                      </Box>
                    </Grid>
                    <Grid item xs={12}>
                      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 2 }}>
                        <Chip
                          icon={<TrendingUp />}
                          label={`${optimizationSummary.performance_improvement}% Performance Improvement`}
                          color="primary"
                          variant="outlined"
                        />
                      </Box>
                    </Grid>
                  </Grid>
                )}
              </CardContent>
            </Card>
          </Grid>
          {/* LD1: Display resource optimization recommendations filtered by type RESOURCE */}
          <Grid item xs={12} md={8}>
            <OptimizationRecommendations
              type={OptimizationType.RESOURCE}
              title="Resource Optimization Recommendations"
              maxItems={5}
              onRecommendationApplied={handleRecommendationAction}
              onRecommendationRejected={handleRecommendationAction}
            />
          </Grid>
        </Grid>
      </Box>
    </MainLayout>
  );
};

// IE3: Export the ResourceOptimization component as the default export
export default ResourceOptimization;