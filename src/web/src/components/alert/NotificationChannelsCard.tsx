import React, { useState, useEffect } from 'react';
import { Box, Typography, Grid, Divider, Badge } from '@mui/material'; // @mui/material ^5.11.0
import { CheckCircle, Cancel, Notifications } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Card from '../common/Card';
import { alertService } from '../../services/api/alertService';
import { useApi } from '../../hooks/useApi';
import { NotificationChannelStatus, NotificationChannel } from '../../types/alerts';

/**
 * Props for the NotificationChannelsCard component
 */
interface NotificationChannelsCardProps {
  /** Additional CSS class for styling */
  className?: string;
  /** Height of the card */
  height?: number | string;
  /** Callback function when the configure button is clicked */
  onConfigClick?: () => void;
}

/**
 * Internal interface for channel display data
 */
interface ChannelData {
  /** Channel identifier */
  id: string;
  /** Display name of the channel */
  name: string;
  /** Whether the channel is configured */
  configured: boolean;
  /** Whether the channel is active */
  active: boolean;
  /** Number of notifications sent through this channel */
  sentCount: number;
}

/**
 * A component that displays notification channel status in a card format.
 * Shows which channels are configured and active, along with sent notification counts for each channel.
 */
const NotificationChannelsCard: React.FC<NotificationChannelsCardProps> = ({
  className,
  height,
  onConfigClick
}) => {
  // State for notification channel status
  const [channelStatus, setChannelStatus] = useState<NotificationChannelStatus | null>(null);
  const [channelData, setChannelData] = useState<ChannelData[]>([]);
  
  // Use the API hook for loading and error handling
  const { loading, error } = useApi();
  
  // Fetch notification channel status on component mount
  useEffect(() => {
    fetchChannelStatus();
  }, []);
  
  /**
   * Fetches notification channel status from the API
   */
  const fetchChannelStatus = async () => {
    try {
      const status = await alertService.getNotificationChannels();
      setChannelStatus(status);
      
      // Process the data for display
      if (status) {
        const processedData = processChannelData(status);
        setChannelData(processedData);
      }
    } catch (error) {
      console.error('Error fetching notification channels:', error);
    }
  };
  
  /**
   * Processes raw channel status into displayable data
   */
  const processChannelData = (status: NotificationChannelStatus): ChannelData[] => {
    // Create channel data objects
    return [
      {
        id: 'teams',
        name: 'Microsoft Teams',
        configured: status.configured.includes('TEAMS'),
        active: !!status.teams,
        sentCount: 4 // This would come from the API in a real implementation
      },
      {
        id: 'email',
        name: 'Email',
        configured: status.configured.includes('EMAIL'),
        active: !!status.email,
        sentCount: 5 // This would come from the API in a real implementation
      },
      {
        id: 'sms',
        name: 'SMS',
        configured: status.configured.includes('SMS'),
        active: !!status.sms,
        sentCount: 0 // This would come from the API in a real implementation
      }
    ];
  };
  
  /**
   * Handles click on the configure button
   */
  const handleConfigClick = () => {
    if (onConfigClick) {
      onConfigClick();
    }
  };
  
  return (
    <Card 
      title="Notification Channels"
      loading={loading}
      error={error ? error.error?.message : null}
      minHeight={height || 300}
      className={className}
      action={
        <Typography 
          variant="body2" 
          color="primary" 
          sx={{ 
            cursor: 'pointer',
            fontWeight: 'medium',
            '&:hover': {
              textDecoration: 'underline'
            }
          }} 
          onClick={handleConfigClick}
        >
          Configure
        </Typography>
      }
    >
      <Box sx={{ p: 1 }}>
        {channelData.map((channel, index) => (
          <React.Fragment key={channel.id}>
            <ChannelItem channel={channel} />
            {index < channelData.length - 1 && <Divider sx={{ my: 1 }} />}
          </React.Fragment>
        ))}
        
        {channelData.length === 0 && !loading && (
          <Box sx={{ py: 2, display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
            <Typography variant="body2" color="text.secondary" align="center">
              No notification channels configured
            </Typography>
          </Box>
        )}
      </Box>
    </Card>
  );
};

/**
 * A component that displays a single notification channel item
 */
const ChannelItem: React.FC<{ channel: ChannelData }> = ({ channel }) => {
  return (
    <Box sx={{ 
      display: 'flex', 
      alignItems: 'center', 
      justifyContent: 'space-between', 
      padding: '8px 0'
    }}>
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: '8px'
      }}>
        {channel.configured ? (
          <CheckCircle sx={{ color: 'success.main', fontSize: '1rem' }} />
        ) : (
          <Cancel sx={{ color: 'error.main', fontSize: '1rem' }} />
        )}
        <Typography 
          variant="body2" 
          sx={{ opacity: channel.configured ? 1 : 0.7 }}
        >
          {channel.name}
        </Typography>
      </Box>
      
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: '4px'
      }}>
        <Badge 
          badgeContent={channel.sentCount} 
          color="primary"
          sx={{ marginLeft: '8px' }}
          max={99}
          showZero={channel.configured}
        >
          <Notifications 
            sx={{ 
              color: channel.configured ? 'primary.main' : 'text.disabled', 
              fontSize: '1.25rem' 
            }} 
          />
        </Badge>
      </Box>
    </Box>
  );
};

export default NotificationChannelsCard;