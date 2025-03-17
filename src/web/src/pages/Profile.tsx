import React, { useEffect } from 'react';
import { Navigate } from 'react-router-dom'; // react-router-dom ^6.6.1
import MainLayout from '../components/layout/MainLayout';
import UserProfile from '../components/authentication/UserProfile';
import { useAuth } from '../contexts/AuthContext';

/**
 * Profile page component that renders the user profile within the main layout
 * @returns Rendered profile page component
 */
const ProfilePage: React.FC = () => {
  // LD1: Get authentication state using useAuth hook
  const { isAuthenticated } = useAuth();

  // LD2: If user is not authenticated, redirect to login page
  if (!isAuthenticated) {
    return <Navigate to="/login" />;
  }

  // LD3: Render MainLayout component with UserProfile component as children
  return (
    <MainLayout>
      <UserProfile />
    </MainLayout>
  );
};

// IE3: Export the ProfilePage component as the default export
export default ProfilePage;