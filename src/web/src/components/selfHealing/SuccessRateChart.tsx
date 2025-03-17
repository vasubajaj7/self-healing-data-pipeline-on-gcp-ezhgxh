import React, { useState, useEffect } from 'react'; // react ^18.2.0
import { Box, Typography } from '@mui/material'; // @mui/material ^5.11.0
import LineChart from '../charts/LineChart';
import Card from '../common/Card';
import Spinner from '../common/Spinner';
import NoData from '../common/NoData';
import healingService from '../../services/api/healingService';
import { useApi } from '../../hooks/useApi';
import { formatDateShort } from '../../utils/date';
import { colors } from '../../theme/colors';

/**
 * @mui/material v5.11.0
 * react v18.2.0
 */

/**
 * Props for the SuccessRateChart component
 */
interface SuccessRateChartProps {
  /**
   * Title to display in the card header
   * @default 'Self-Healing Success Rate'
   */
  title?: string;
  /**
   * Height of the chart
   * @default 300
   */
  height?: number | string;
  /**
   * Date range for filtering the success rate data
   */
  dateRange?: { startDate: string; endDate: string };
  /**
   * Additional CSS class for styling
   */
  className?: string;
}

/**
 * Transforms the success rate trend data from the API into the format expected by the LineChart component
 * @param successRateTrend Array of objects with date and rate properties
 * @returns Formatted chart data object for LineChart component
 */
const transformSuccessRateData = (successRateTrend: Array<{ date: string; rate: number }>) => {
  // Map the success rate trend data to the format expected by LineChart
  const labels = successRateTrend.map((item) => formatDateShort(item.date)); // Format dates using formatDateShort utility
  const data = successRateTrend.map((item) => item.rate * 100); // Convert rates from decimal to percentage values

  // Return an object with labels array and datasets array containing the formatted data
  return {
    labels: labels,
    datasets: [
      {
        label: 'Success Rate',
        data: data,
        borderColor: colors.chart.green,
        backgroundColor: 'transparent',
      },
    ],
  };
};

/**
 * Component that displays a line chart showing the success rate of self-healing actions over time
 * @param props SuccessRateChartProps
 * @returns React component
 */
const SuccessRateChart: React.FC<SuccessRateChartProps> = ({
  title = 'Self-Healing Success Rate',
  height = 300,
  dateRange,
  className,
}) => {
  // Use useApi hook to get API methods and loading/error states
  const { get, loading, error } = useApi();
  // Define state for chart data
  const [chartData, setChartData] = useState<object | null>(null);

  // Fetch success rate data when component mounts or dateRange changes
  useEffect(() => {
    const fetchData = async () => {
      try {
        // Retrieve dashboard data using healingService.getDashboardData
        const response = await get(healingService.getDashboardData, dateRange);

        if (response && response.data && response.data.successRateTrend) {
          // Transform the success rate trend data using transformSuccessRateData function
          const transformedData = transformSuccessRateData(response.data.successRateTrend);
          setChartData(transformedData);
        } else {
          setChartData(null); // Set chartData to null if no data is available
        }
      } catch (e) {
        console.error('Failed to fetch success rate data', e);
        setChartData(null); // Set chartData to null if there's an error
      }
    };

    fetchData();
  }, [dateRange, get]);

  // Define chart options for LineChart component
  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        beginAtZero: true,
        max: 100,
        title: {
          display: true,
          text: 'Success Rate (%)',
        },
      },
      x: {
        title: {
          display: true,
          text: 'Date',
        },
      },
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: (context: any) => {
            // Custom function to format tooltip with percentage
            let label = context.dataset.label || '';
            if (label) {
              label += ': ';
            }
            if (context.parsed.y !== null) {
              label += context.parsed.y + '%';
            }
            return label;
          },
        },
      },
      legend: {
        position: 'top',
      },
    },
  };

  // Render Card component with the title prop
  return (
    <Card title={title} height={height} className={className}>
      {loading ? (
        // If loading, render Spinner component
        <Spinner overlay />
      ) : error ? (
        // If error, render error message
        <Typography color="error">Error fetching data.</Typography>
      ) : !chartData || !chartData.labels || chartData.labels.length === 0 ? (
        // If no data available, render NoData component
        <NoData message="No success rate data available." />
      ) : (
        // If data is available, render LineChart component with the transformed data
        <LineChart data={chartData} options={chartOptions} />
      )}
    </Card>
  );
};

export default SuccessRateChart;