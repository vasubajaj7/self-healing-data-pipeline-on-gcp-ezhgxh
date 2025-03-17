import React, { useState, useEffect } from 'react'; // react ^18.2.0
import Card from '../common/Card';
import Button from '../common/Button';
import { useDashboard } from '../../contexts/DashboardContext';
import { AIInsight, AiInsightsCardProps } from '../../types/dashboard';
import { Box, Typography, List, ListItem, ListItemText, ListItemIcon, Divider, Tooltip } from '@mui/material'; // @mui/material ^5.11.0
import InfoOutlined from '@mui/icons-material/InfoOutlined'; // @mui/icons-material ^5.11.0
import ArrowForward from '@mui/icons-material/ArrowForward'; // @mui/icons-material ^5.11.0
import { useTheme } from '@mui/material/styles'; // @mui/material/styles ^5.11.0
import { useNavigate } from 'react-router-dom'; // react-router-dom ^6.8.0

/**
 * Formats the timestamp of an insight into a human-readable format
 * @param timestamp The timestamp string
 * @returns The formatted time string
 */
const formatInsightTime = (timestamp: string): string => {
  // Create a new Date object from the timestamp string
  const date = new Date(timestamp);

  // Format the date using toLocaleString or similar method
  const formattedDate = date.toLocaleString();

  // Return the formatted date string
  return formattedDate;
};

/**
 * Returns a color based on the confidence level of an insight
 * @param confidence The confidence level
 * @returns The color code based on confidence level
 */
const getConfidenceColor = (confidence: number): string => {
  // If confidence is >= 90, return success color (green)
  if (confidence >= 90) {
    return 'success.main';
  }

  // If confidence is >= 70, return warning color (amber)
  if (confidence >= 70) {
    return 'warning.main';
  }

  // Otherwise return error color (red)
  return 'error.main';
};

/**
 * Navigates to the detailed view of an insight
 * @param insight The AIInsight object
 */
const navigateToInsightDetails = (insight: AIInsight, navigate: any): void => {
  // Use navigate function to redirect to the self-healing page with the insight ID as a parameter
  navigate(`/healing/${insight.id}`, { state: { insight } });

  // Include any additional context in the navigation state
};

/**
 * Card component that displays AI-generated insights about the data pipeline
 */
const AiInsightsCard: React.FC<AiInsightsCardProps> = ({ className, maxItems = 3, loading = false, onClick }) => {
  // Use useDashboard hook to get dashboard data
  const { dashboardData } = useDashboard();

  // Use useTheme hook to get theme values for styling
  const theme = useTheme();

  // Use useNavigate hook for navigation
  const navigate = useNavigate();

  // Extract aiInsights from dashboardData or use empty array if not available
  const aiInsights = dashboardData?.aiInsights || [];

  // Limit the number of insights to display based on maxItems prop
  const limitedInsights = aiInsights.slice(0, maxItems);

  // Define styling for the list container
  const listContainerStyle = {
    maxHeight: '300px',
    overflow: 'auto',
  };

  // Define styling for the list item
  const listItemStyle = {
    padding: '8px 0',
  };

  // Define styling for the insight icon
  const insightIconStyle = {
    minWidth: '36px',
  };

  // Define styling for the confidence chip
  const confidenceChipStyle = {
    fontSize: '0.75rem',
    padding: '2px 8px',
    borderRadius: '10px',
    marginLeft: '8px',
    display: 'inline-block',
  };

  // Define styling for the view all button
  const viewAllButtonStyle = {
    marginTop: '8px',
    textAlign: 'center',
  };

  // Render Card component with title 'AI Insights'
  return (
    <Card title="AI Insights" className={className} loading={loading} onClick={onClick}>
      {/* Render List component containing insights */}
      <List style={listContainerStyle}>
        {/* For each insight, render ListItem with InfoOutlined icon */}
        {limitedInsights.map((insight, index) => (
          <React.Fragment key={insight.id}>
            <ListItem style={listItemStyle} alignItems="flex-start">
              <ListItemIcon style={insightIconStyle}>
                <InfoOutlined />
              </ListItemIcon>
              <ListItemText
                primary={
                  <React.Fragment>
                    <Typography variant="body2" color="textPrimary">
                      {insight.description}
                      <span
                        style={{
                          ...confidenceChipStyle,
                          backgroundColor: theme.palette[getConfidenceColor(insight.confidence)].light,
                          color: theme.palette[getConfidenceColor(insight.confidence)].dark,
                        }}
                      >
                        {insight.confidence}% Confidence
                      </span>
                    </Typography>
                  </React.Fragment>
                }
                secondary={
                  <React.Fragment>
                    <Tooltip title={`Timestamp: ${formatInsightTime(insight.timestamp)} | Related Entity: ${insight.relatedEntity}`}>
                      <Typography
                        variant="caption"
                        color="textSecondary"
                        sx={{ display: 'block' }}
                      >
                        {formatInsightTime(insight.timestamp)}
                      </Typography>
                    </Tooltip>
                  </React.Fragment>
                }
                onClick={() => navigateToInsightDetails(insight, navigate)}
                style={{ cursor: 'pointer' }}
              />
            </ListItem>
            {/* Add divider between list items */}
            {index < limitedInsights.length - 1 && <Divider />}
          </React.Fragment>
        ))}
      </List>
      {/* Add 'View All' button at the bottom if there are more insights than maxItems */}
      {aiInsights.length > maxItems && (
        <Box style={viewAllButtonStyle}>
          <Button
            endIcon={<ArrowForward />}
            onClick={() => navigate('/healing')}
          >
            View All
          </Button>
        </Box>
      )}
    </Card>
  );
};

export default AiInsightsCard;