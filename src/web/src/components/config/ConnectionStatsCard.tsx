import React from 'react';
import { Box, Grid, Typography } from '@mui/material'; // ^5.11.0
import { useTheme } from '@mui/material/styles'; // ^5.11.0
import { Storage, Database, Api, Code, CheckCircle, Warning, Error } from '@mui/icons-material'; // ^5.11.0
import Card from '../common/Card';
import { SourceSystem } from '../../types/config';

/**
 * Props for the ConnectionStatsCard component
 */
interface ConnectionStatsCardProps {
  /** Array of data sources to calculate statistics from */
  dataSources: SourceSystem[];
  /** Whether the data is currently loading */
  loading?: boolean;
  /** Error message if data fetching failed */
  error?: string | null;
  /** Additional CSS class for styling */
  className?: string;
}

/**
 * Interface for connection statistics
 */
interface ConnectionStats {
  total: number;
  active: number;
  inactive: number;
  typeStats: Record<string, number>;
  statusStats: Record<string, number>;
}

/**
 * Calculates statistics from the provided data sources
 * 
 * @param dataSources Array of source systems
 * @returns Object containing calculated statistics
 */
const calculateStats = (dataSources: SourceSystem[]): ConnectionStats => {
  const stats: ConnectionStats = {
    total: 0,
    active: 0,
    inactive: 0,
    typeStats: {},
    statusStats: {}
  };

  // Count total sources
  stats.total = dataSources.length;

  // Process each data source
  dataSources.forEach(source => {
    // Count active/inactive
    if (source.isActive) {
      stats.active++;
    } else {
      stats.inactive++;
    }

    // Count by source type
    if (!stats.typeStats[source.sourceType]) {
      stats.typeStats[source.sourceType] = 0;
    }
    stats.typeStats[source.sourceType]++;

    // Count by status
    const status = source.status || 'UNKNOWN';
    if (!stats.statusStats[status]) {
      stats.statusStats[status] = 0;
    }
    stats.statusStats[status]++;
  });

  return stats;
};

/**
 * Component that displays a single statistic with label and icon
 */
const StatItem: React.FC<{
  label: string;
  value: number;
  icon?: React.ReactNode;
  color?: string;
}> = ({ label, value, icon, color }) => {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
      {icon && (
        <Box sx={{ marginRight: '8px', display: 'flex', alignItems: 'center', color }}>
          {icon}
        </Box>
      )}
      <Box>
        <Typography variant="h5" sx={{ fontWeight: 'bold' }}>
          {value}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {label}
        </Typography>
      </Box>
    </Box>
  );
};

/**
 * A React component that displays statistics about data source connections
 * in the configuration section of the self-healing data pipeline application.
 */
const ConnectionStatsCard: React.FC<ConnectionStatsCardProps> = ({
  dataSources,
  loading = false,
  error = null,
  className
}) => {
  const theme = useTheme();
  const stats = calculateStats(dataSources);

  // Define color schemes for different statuses and types
  const statusColors = {
    OK: theme.palette.success.main,
    WARNING: theme.palette.warning.main,
    ERROR: theme.palette.error.main,
    UNKNOWN: theme.palette.text.disabled
  };

  const typeColors = {
    GCS: theme.palette.primary.main,
    CLOUD_SQL: theme.palette.secondary.main,
    API: theme.palette.info.main,
    CUSTOM: theme.palette.text.primary
  };

  // Helper function to get the appropriate icon for each source type
  const getTypeIcon = (type: string): React.ReactNode => {
    switch (type) {
      case 'GCS':
        return <Storage />;
      case 'CLOUD_SQL':
        return <Database />;
      case 'API':
        return <Api />;
      default:
        return <Code />;
    }
  };

  // Helper function to get the appropriate icon for each status
  const getStatusIcon = (status: string): React.ReactNode => {
    switch (status) {
      case 'OK':
        return <CheckCircle />;
      case 'WARNING':
        return <Warning />;
      case 'ERROR':
        return <Error />;
      default:
        return <CheckCircle />;
    }
  };

  return (
    <Card 
      title="Connection Statistics"
      loading={loading}
      error={error}
      className={className}
      minHeight="220px"
      sx={{ marginBottom: '16px' }}
    >
      <Grid container spacing={2}>
        {/* Total connections */}
        <Grid item xs={12} sm={4}>
          <StatItem 
            label="Total Connections" 
            value={stats.total} 
            icon={<Storage />} 
          />
        </Grid>
        
        {/* Active connections */}
        <Grid item xs={12} sm={4}>
          <StatItem 
            label="Active" 
            value={stats.active} 
            icon={<CheckCircle />} 
            color={theme.palette.success.main} 
          />
        </Grid>
        
        {/* Inactive connections */}
        <Grid item xs={12} sm={4}>
          <StatItem 
            label="Inactive" 
            value={stats.inactive} 
            icon={<Error />} 
            color={theme.palette.text.disabled} 
          />
        </Grid>

        {/* Connection Type Distribution */}
        {Object.keys(stats.typeStats).length > 0 && (
          <Grid item xs={12}>
            <Typography variant="subtitle1" gutterBottom>
              By Type
            </Typography>
            <Grid container spacing={2}>
              {Object.keys(stats.typeStats).map(type => (
                <Grid item xs={6} sm={3} key={type}>
                  <StatItem 
                    label={type} 
                    value={stats.typeStats[type]} 
                    icon={getTypeIcon(type)}
                    color={typeColors[type as keyof typeof typeColors] || theme.palette.text.primary}
                  />
                </Grid>
              ))}
            </Grid>
          </Grid>
        )}

        {/* Status Distribution */}
        {Object.keys(stats.statusStats).length > 0 && (
          <Grid item xs={12}>
            <Typography variant="subtitle1" gutterBottom>
              By Status
            </Typography>
            <Grid container spacing={2}>
              {Object.keys(stats.statusStats).map(status => (
                <Grid item xs={6} sm={3} key={status}>
                  <StatItem 
                    label={status} 
                    value={stats.statusStats[status]} 
                    icon={getStatusIcon(status)}
                    color={statusColors[status as keyof typeof statusColors] || theme.palette.text.primary}
                  />
                </Grid>
              ))}
            </Grid>
          </Grid>
        )}
      </Grid>
    </Card>
  );
};

export default ConnectionStatsCard;