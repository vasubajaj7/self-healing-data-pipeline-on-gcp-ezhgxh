import React, { useEffect } from 'react'; // react ^18.2.0
import { Box, Typography } from '@mui/material'; // @mui/material ^5.11.0
import { useParams, useSearchParams } from 'react-router-dom'; // react-router-dom ^6.6.1
import MainLayout from '../components/layout/MainLayout';
import QualityDashboard from '../components/quality/QualityDashboard';
import { useQuality } from '../contexts/QualityContext';
import { QualityDashboardFilters } from '../types/quality';

/**
 * Main component for the Data Quality page
 */
const DataQuality: React.FC = () => {
  // Get URL parameters and query parameters using React Router hooks
  const params = useParams<{ dataset?: string; table?: string; tab?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();

  // Initialize state for initial tab based on URL parameters
  const initialTab = params.tab ? parseInt(params.tab, 10) : 0;

  // Access quality context using useQuality hook
  const { setFilters } = useQuality();

  // Set up effect to apply URL filters to quality context
  useEffect(() => {
    const urlFilters: QualityDashboardFilters = {
      dataset: params.dataset,
      table: params.table,
      dimension: searchParams.get('dimension') || undefined,
      severity: searchParams.get('severity') || undefined,
      status: searchParams.get('status') || undefined,
      timeRange: searchParams.get('timeRange') || undefined,
      startDate: undefined,
      endDate: undefined,
      minScore: undefined,
      maxScore: undefined,
      searchTerm: undefined,
    };
    setFilters(urlFilters);
  }, [params, searchParams, setFilters]);

  // Render MainLayout component as the page container
  return (
    <MainLayout>
      {/* Render page title and description */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          Data Quality
        </Typography>
        <Typography variant="body1">
          Monitor and manage the quality of your data pipelines.
        </Typography>
      </Box>

      {/* Render QualityDashboard component with appropriate props */}
      <QualityDashboard initialTab={initialTab} />
    </MainLayout>
  );
};

export default DataQuality;