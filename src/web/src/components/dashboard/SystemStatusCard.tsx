import React from 'react'; // react ^18.2.0
import Card from '../common/Card';
import StatusIndicator from '../charts/StatusIndicator';
import { useDashboard } from '../../contexts/DashboardContext';
import { SystemComponentStatus, SystemStatus } from '../../types/dashboard';
import { Box, Typography, Grid, Divider } from '@mui/material'; // @mui/material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0

/**
 * Interface defining the props for the SystemStatusCard component.
 */
interface SystemStatusCardProps {
  className?: string;
  loading?: boolean;
  error?: Error | null;
  systemStatus?: SystemStatus;
  onClick?: () => void;
}

/**
 * Internal interface for system component display data
 */
interface SystemComponentItem {
  name: string;
  key: keyof SystemStatus;
  description: string;
}

/**
 * Card component that displays the status of various system components with status indicators.
 */
const SystemStatusCard: React.FC<SystemStatusCardProps> = ({
  className,
  loading = false,
  error = null,
  systemStatus,
  onClick,
}) => {
  // Access dashboard data including system status
  const { dashboardData } = useDashboard();

  // Use the theme to access theme colors for styling
  const theme = useTheme();

  // Use useNavigate hook for navigation to configuration page on click
  const navigate = useNavigate();

  // Define click handler to navigate to configuration page
  const handleCardClick = () => {
    navigate('/configuration');
  };

  // Define system components list with display names and keys
  const systemComponents: SystemComponentItem[] = [
    { name: 'GCS Connector', key: 'gcsConnector', description: 'Google Cloud Storage Connector' },
    { name: 'Cloud SQL', key: 'cloudSql', description: 'Cloud SQL Database' },
    { name: 'External APIs', key: 'externalApis', description: 'External API Integrations' },
    { name: 'BigQuery', key: 'bigQuery', description: 'BigQuery Data Warehouse' },
    { name: 'ML Services', key: 'mlServices', description: 'Machine Learning Services' },
  ];

  // Map SystemComponentStatus enum values to status strings for StatusIndicator
  const mapStatusToString = (status: SystemComponentStatus): string => {
    switch (status) {
      case SystemComponentStatus.OK:
        return 'healthy';
      case SystemComponentStatus.WARN:
        return 'warning';
      case SystemComponentStatus.ERROR:
        return 'error';
      default:
        return 'inactive';
    }
  };

  // Generate tooltip text based on component name and status
  const getStatusTooltip = (componentName: string, status: SystemComponentStatus): string => {
    switch (status) {
      case SystemComponentStatus.OK:
        return `${componentName} is operating normally`;
      case SystemComponentStatus.WARN:
        return `${componentName} is experiencing issues`;
      case SystemComponentStatus.ERROR:
        return `${componentName} is not functioning properly`;
      default:
        return `Status for ${componentName} is unavailable`;
    }
  };

  // Define card styles
  const cardStyles = {
    height: '100%',
    cursor: 'pointer',
  };

  // Define component grid styles
  const componentGrid = {
    marginTop: '8px',
  };

  // Define component item styles
  const componentItem = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '8px 0',
  };

  // Define component name styles
  const componentName = {
    fontSize: '0.875rem',
  };

  // Define divider styles
  const divider = {
    margin: '4px 0',
  };

  // Render the SystemStatusCard component
  return (
    <Card
      title="System Status"
      className={className}
      loading={loading}
      error={error}
      onClick={handleCardClick}
      style={cardStyles}
    >
      <Grid container direction="column" style={componentGrid}>
        {systemComponents.map((component, index) => {
          const status = systemStatus ? systemStatus[component.key] : dashboardData?.systemStatus?.[component.key];
          const statusString = status ? mapStatusToString(status) : 'inactive';
          const tooltipText = status ? getStatusTooltip(component.name, status) : `Status for ${component.name} is unavailable`;

          return (
            <React.Fragment key={component.key}>
              <Grid item style={componentItem}>
                <Typography variant="body2" style={componentName}>
                  {component.name}
                </Typography>
                <StatusIndicator status={statusString} tooltip={tooltipText} />
              </Grid>
              {index < systemComponents.length - 1 && <Divider style={divider} />}
            </React.Fragment>
          );
        })}
      </Grid>
    </Card>
  );
};

export default SystemStatusCard;