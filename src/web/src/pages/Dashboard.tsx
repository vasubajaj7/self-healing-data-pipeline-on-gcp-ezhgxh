import React from 'react'; // react ^18.2.0
import DashboardOverview from '../components/dashboard/DashboardOverview';
import { DashboardProvider } from '../contexts/DashboardContext';
import MainLayout from '../components/layout/MainLayout';

/**
 * Main dashboard page component that wraps the dashboard overview with the dashboard context provider
 * @returns {JSX.Element} Rendered dashboard page with context provider and layout
 */
const Dashboard: React.FC = () => {
  // LD1: Render MainLayout as the outer container for consistent page layout
  return (
    <MainLayout>
      {/* LD2: Wrap DashboardOverview with DashboardProvider to provide data and state management */}
      <DashboardProvider initialRefreshInterval={60000}>
        {/* LD3: Set appropriate refresh interval for dashboard data (e.g., 60000ms or 1 minute) */}
        <DashboardOverview />
      </DashboardProvider>
      {/* LD4: Return the composed component structure */}
    </MainLayout>
  );
};

export default Dashboard;