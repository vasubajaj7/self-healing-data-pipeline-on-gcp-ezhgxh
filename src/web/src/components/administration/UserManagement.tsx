import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Grid,
  Paper,
  IconButton,
  Tooltip,
  Chip,
  Alert,
  Snackbar,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Edit,
  Delete,
  Visibility,
  VisibilityOff,
  PersonAdd,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11
import Table from '../common/Table';
import Modal from '../common/Modal';
import Form from '../common/Form';
import Input from '../common/Input';
import Select from '../common/Select';
import Button from '../common/Button';
import adminService from '../../services/api/adminService';
import { User, UserRole, UserPermission } from '../../types/user';
import { useAuth } from '../../contexts/AuthContext';
import { CreateUserFormValues, EditUserFormValues } from '../../interfaces/UserManagement';

/**
 * Formats a date string into a readable format
 * @param dateString 
 * @returns Formatted date string
 */
const formatDate = (dateString: string): string => {
  const date = new Date(dateString);
  return date.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
};

/**
 * Main component for user management functionality
 */
const UserManagement: React.FC = () => {
  // Authentication context for permission checking
  const { checkPermission } = useAuth();

  // State variables
  const [users, setUsers] = useState<User[]>([]);
  const [roles, setRoles] = useState<UserRole[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [totalUsers, setTotalUsers] = useState<number>(0);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [roleFilter, setRoleFilter] = useState<string>('');
  const [activeOnly, setActiveOnly] = useState<boolean>(false);
  const [isCreateModalOpen, setCreateModalOpen] = useState<boolean>(false);
  const [isEditModalOpen, setEditModalOpen] = useState<boolean>(false);
  const [isDeleteModalOpen, setDeleteModalOpen] = useState<boolean>(false);
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [notification, setNotification] = useState<{ open: boolean; message: string; type: 'success' | 'error' | 'info' | 'warning' }>({ open: false, message: '', type: 'info' });

  // Fetch users from the API with pagination and filtering
  const fetchUsers = useCallback(async () => {
    setLoading(true);
    try {
      const response = await adminService.getUsers(page, pageSize, searchTerm, roleFilter, activeOnly);
      setUsers(response.items);
      setTotalUsers(response.pagination.totalItems);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch users');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, searchTerm, roleFilter, activeOnly]);

  // Fetch available roles from the API
  const fetchRoles = useCallback(async () => {
    try {
      const response = await adminService.getRoles();
      setRoles(response.data);
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch roles');
    }
  }, []);

  // Load initial data on component mount
  useEffect(() => {
    fetchUsers();
    fetchRoles();
  }, [fetchUsers, fetchRoles]);

  // Reload users when pagination or filters change
  useEffect(() => {
    fetchUsers();
  }, [page, pageSize, searchTerm, roleFilter, activeOnly, fetchUsers]);

  // Handle user creation form submission
  const handleCreateUser = async (values: any, helpers: FormikHelpers<any>) => {
    try {
      await adminService.createUser(values);
      closeCreateModal();
      showNotification('User created successfully', 'success');
      await fetchUsers();
      helpers.resetForm();
    } catch (err: any) {
      showNotification(err.message || 'Failed to create user', 'error');
    }
  };

  // Handle user update form submission
  const handleUpdateUser = async (values: any, helpers: FormikHelpers<any>) => {
    try {
      if (!selectedUser?.id) {
        showNotification('No user selected for update', 'error');
        return;
      }
      await adminService.updateUser(selectedUser.id, values);
      closeEditModal();
      showNotification('User updated successfully', 'success');
      await fetchUsers();
      helpers.resetForm();
    } catch (err: any) {
      showNotification(err.message || 'Failed to update user', 'error');
    }
  };

  // Handle user deletion confirmation
  const handleDeleteUser = async () => {
    try {
      if (!selectedUser?.id) {
        showNotification('No user selected for deletion', 'error');
        return;
      }
      await adminService.deleteUser(selectedUser.id);
      closeDeleteModal();
      showNotification('User deleted successfully', 'success');
      await fetchUsers();
    } catch (err: any) {
      showNotification(err.message || 'Failed to delete user', 'error');
    }
  };

  // Handle pagination page change
  const handlePageChange = (newPage: number, newPageSize: number) => {
    setPage(newPage);
    setPageSize(newPageSize);
  };

  // Handle search input change
  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(event.target.value);
    setPage(0);
  };

  // Handle role filter change
  const handleRoleFilterChange = (value: string) => {
    setRoleFilter(value);
    setPage(0);
  };

  // Handle active filter toggle
  const handleActiveFilterChange = (checked: boolean) => {
    setActiveOnly(checked);
    setPage(0);
  };

  // Open the create user modal
  const openCreateModal = () => {
    setCreateModalOpen(true);
  };

  // Close the create user modal
  const closeCreateModal = () => {
    setCreateModalOpen(false);
  };

  // Open the edit user modal with selected user
  const openEditModal = (user: User) => {
    setSelectedUser(user);
    setEditModalOpen(true);
  };

  // Close the edit user modal
  const closeEditModal = () => {
    setEditModalOpen(false);
    setSelectedUser(null);
  };

  // Open the delete user modal with selected user
  const openDeleteModal = (user: User) => {
    setSelectedUser(user);
    setDeleteModalOpen(true);
  };

  // Close the delete user modal
  const closeDeleteModal = () => {
    setDeleteModalOpen(false);
    setSelectedUser(null);
  };

  // Toggle password visibility in forms
  const togglePasswordVisibility = () => {
    setShowPassword(!showPassword);
  };

  // Show a notification message
  const showNotification = (message: string, type: 'success' | 'error' | 'info' | 'warning') => {
    setNotification({ open: true, message, type });
  };

  // Close the notification snackbar
  const closeNotification = () => {
    setNotification({ ...notification, open: false });
  };

  // Check if user has MANAGE_USERS permission
  if (!checkPermission(UserPermission.MANAGE_USERS)) {
    return (
      <Box p={3}>
        <Typography variant="h6" color="error">
          Access Denied: You do not have permission to manage users.
        </Typography>
      </Box>
    );
  }

  // Define table columns for user data display
  const columns = [
    { id: 'username', label: 'Username', sortable: true },
    { id: 'email', label: 'Email', sortable: true },
    { id: 'firstName', label: 'First Name', sortable: true },
    { id: 'lastName', label: 'Last Name', sortable: true },
    {
      id: 'role',
      label: 'Role',
      sortable: true,
      format: (value: UserRole) => value.replace('_', ' '),
    },
    {
      id: 'isActive',
      label: 'Active',
      sortable: true,
      format: (value: boolean) => (value ? 'Yes' : 'No'),
    },
    {
      id: 'lastLogin',
      label: 'Last Login',
      sortable: true,
      format: (value: string) => (value ? formatDate(value) : 'Never'),
    },
    {
      id: 'actions',
      label: 'Actions',
      renderCell: (value: any, row: User) => (
        <>
          <Tooltip title="Edit User">
            <IconButton onClick={() => openEditModal(row)} size="small">
              <Edit />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete User">
            <IconButton onClick={() => openDeleteModal(row)} size="small">
              <Delete />
            </IconButton>
          </Tooltip>
        </>
      ),
    },
  ];

  // Prepare role options for select components
  const roleOptions = roles.map((role) => ({
    value: role,
    label: role.replace('_', ' '),
  }));

  // Create validation schema for user forms using yup
  const validationSchema = yup.object().shape({
    username: yup.string().required('Username is required'),
    email: yup.string().email('Invalid email').required('Email is required'),
    firstName: yup.string().required('First Name is required'),
    lastName: yup.string().required('Last Name is required'),
    password: yup.string().min(8, 'Password must be at least 8 characters'),
    confirmPassword: yup
      .string()
      .oneOf([yup.ref('password'), null], 'Passwords must match')
      .required('Confirm Password is required'),
    role: yup.string().required('Role is required'),
    isActive: yup.boolean(),
    mfaEnabled: yup.boolean(),
  });

  // Render the component
  return (
    <Box p={3}>
      <Grid container justifyContent="space-between" alignItems="center" mb={2}>
        <Grid item>
          <Typography variant="h5">User Management</Typography>
        </Grid>
        <Grid item>
          <Button variant="contained" startIcon={<PersonAdd />} onClick={openCreateModal}>
            Create User
          </Button>
        </Grid>
      </Grid>

      <Grid container spacing={2} alignItems="center" mb={2}>
        <Grid item>
          <Input
            label="Search Users"
            type="text"
            value={searchTerm}
            onChange={handleSearchChange}
            size="small"
          />
        </Grid>
        <Grid item>
          <Select
            label="Filter by Role"
            value={roleFilter}
            onChange={handleRoleFilterChange}
            options={roleOptions}
            size="small"
          />
        </Grid>
        <Grid item>
          <Box display="flex" alignItems="center">
            <Typography variant="body2" mr={1}>
              Active Only:
            </Typography>
            <Input
              type="checkbox"
              checked={activeOnly}
              onChange={(e) => handleActiveFilterChange(e.target.checked)}
            />
          </Box>
        </Grid>
      </Grid>

      <Table
        columns={columns}
        data={users}
        loading={loading}
        error={error}
        totalItems={totalUsers}
        page={page}
        pageSize={pageSize}
        onPageChange={handlePageChange}
      />

      <Modal
        open={isCreateModalOpen}
        onClose={closeCreateModal}
        title="Create New User"
      >
        <Form
          initialValues={{
            username: '',
            email: '',
            firstName: '',
            lastName: '',
            password: '',
            confirmPassword: '',
            role: roleOptions.length > 0 ? roleOptions[0].value : '',
            isActive: true,
            mfaEnabled: false,
          }}
          validationSchema={validationSchema}
          onSubmit={handleCreateUser}
        >
          {(formik) => (
            <>
              <Input
                label="Username"
                type="text"
                value={formik.values.username}
                onChange={(value) => formik.setFieldValue('username', value)}
                error={formik.touched.username && !!formik.errors.username}
                helperText={formik.touched.username && formik.errors.username}
              />
              <Input
                label="Email"
                type="email"
                value={formik.values.email}
                onChange={(value) => formik.setFieldValue('email', value)}
                error={formik.touched.email && !!formik.errors.email}
                helperText={formik.touched.email && formik.errors.email}
              />
              <Input
                label="First Name"
                type="text"
                value={formik.values.firstName}
                onChange={(value) => formik.setFieldValue('firstName', value)}
                error={formik.touched.firstName && !!formik.errors.firstName}
                helperText={formik.touched.firstName && formik.errors.firstName}
              />
              <Input
                label="Last Name"
                type="text"
                value={formik.values.lastName}
                onChange={(value) => formik.setFieldValue('lastName', value)}
                error={formik.touched.lastName && !!formik.errors.lastName}
                helperText={formik.touched.lastName && formik.errors.lastName}
              />
              <Input
                label="Password"
                type={showPassword ? 'text' : 'password'}
                value={formik.values.password}
                onChange={(value) => formik.setFieldValue('password', value)}
                error={formik.touched.password && !!formik.errors.password}
                helperText={formik.touched.password && formik.errors.password}
                endAdornment={
                  <IconButton onClick={togglePasswordVisibility} edge="end">
                    {showPassword ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                }
              />
              <Input
                label="Confirm Password"
                type={showPassword ? 'text' : 'password'}
                value={formik.values.confirmPassword}
                onChange={(value) => formik.setFieldValue('confirmPassword', value)}
                error={formik.touched.confirmPassword && !!formik.errors.confirmPassword}
                helperText={formik.touched.confirmPassword && formik.errors.confirmPassword}
                endAdornment={
                  <IconButton onClick={togglePasswordVisibility} edge="end">
                    {showPassword ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                }
              />
              <Select
                label="Role"
                value={formik.values.role}
                onChange={(value) => formik.setFieldValue('role', value)}
                options={roleOptions}
                error={formik.touched.role && !!formik.errors.role}
                helperText={formik.touched.role && formik.errors.role}
              />
              <Box display="flex" alignItems="center">
                <Typography variant="body2" mr={1}>
                  Active:
                </Typography>
                <Input
                  type="checkbox"
                  checked={formik.values.isActive}
                  onChange={(e) => formik.setFieldValue('isActive', e.target.checked)}
                />
              </Box>
              <Box display="flex" alignItems="center">
                <Typography variant="body2" mr={1}>
                  MFA Enabled:
                </Typography>
                <Input
                  type="checkbox"
                  checked={formik.values.mfaEnabled}
                  onChange={(e) => formik.setFieldValue('mfaEnabled', e.target.checked)}
                />
              </Box>
              <Box display="flex" justifyContent="flex-end">
                <Button onClick={closeCreateModal}>Cancel</Button>
                <Button type="submit" loading={formik.isSubmitting}>
                  Create
                </Button>
              </Box>
            </>
          )}
        </Form>
      </Modal>

      <Modal
        open={isEditModalOpen}
        onClose={closeEditModal}
        title="Edit User"
      >
        <Form
          initialValues={selectedUser
            ? {
                username: selectedUser.username,
                email: selectedUser.email,
                firstName: selectedUser.firstName,
                lastName: selectedUser.lastName,
                password: '',
                confirmPassword: '',
                role: selectedUser.role,
                isActive: selectedUser.isActive,
                mfaEnabled: selectedUser.mfaEnabled,
              }
            : {
                username: '',
                email: '',
                firstName: '',
                lastName: '',
                password: '',
                confirmPassword: '',
                role: roleOptions.length > 0 ? roleOptions[0].value : '',
                isActive: true,
                mfaEnabled: false,
              }}
          validationSchema={validationSchema}
          onSubmit={handleUpdateUser}
          enableReinitialize
        >
          {(formik) => (
            <>
              <Input
                label="Username"
                type="text"
                value={formik.values.username}
                onChange={(value) => formik.setFieldValue('username', value)}
                error={formik.touched.username && !!formik.errors.username}
                helperText={formik.touched.username && formik.errors.username}
              />
              <Input
                label="Email"
                type="email"
                value={formik.values.email}
                onChange={(value) => formik.setFieldValue('email', value)}
                error={formik.touched.email && !!formik.errors.email}
                helperText={formik.touched.email && formik.errors.email}
              />
              <Input
                label="First Name"
                type="text"
                value={formik.values.firstName}
                onChange={(value) => formik.setFieldValue('firstName', value)}
                error={formik.touched.firstName && !!formik.errors.firstName}
                helperText={formik.touched.firstName && formik.errors.firstName}
              />
              <Input
                label="Last Name"
                type="text"
                value={formik.values.lastName}
                onChange={(value) => formik.setFieldValue('lastName', value)}
                error={formik.touched.lastName && !!formik.errors.lastName}
                helperText={formik.touched.lastName && formik.errors.lastName}
              />
              <Input
                label="Password"
                type={showPassword ? 'text' : 'password'}
                value={formik.values.password}
                onChange={(value) => formik.setFieldValue('password', value)}
                error={formik.touched.password && !!formik.errors.password}
                helperText={formik.touched.password && formik.errors.password}
                placeholder="Leave blank to keep current password"
                endAdornment={
                  <IconButton onClick={togglePasswordVisibility} edge="end">
                    {showPassword ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                }
              />
              <Input
                label="Confirm Password"
                type={showPassword ? 'text' : 'password'}
                value={formik.values.confirmPassword}
                onChange={(value) => formik.setFieldValue('confirmPassword', value)}
                error={formik.touched.confirmPassword && !!formik.errors.confirmPassword}
                helperText={formik.touched.confirmPassword && formik.errors.confirmPassword}
                placeholder="Leave blank to keep current password"
                endAdornment={
                  <IconButton onClick={togglePasswordVisibility} edge="end">
                    {showPassword ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                }
              />
              <Select
                label="Role"
                value={formik.values.role}
                onChange={(value) => formik.setFieldValue('role', value)}
                options={roleOptions}
                error={formik.touched.role && !!formik.errors.role}
                helperText={formik.touched.role && formik.errors.role}
              />
              <Box display="flex" alignItems="center">
                <Typography variant="body2" mr={1}>
                  Active:
                </Typography>
                <Input
                  type="checkbox"
                  checked={formik.values.isActive}
                  onChange={(e) => formik.setFieldValue('isActive', e.target.checked)}
                />
              </Box>
              <Box display="flex" alignItems="center">
                <Typography variant="body2" mr={1}>
                  MFA Enabled:
                </Typography>
                <Input
                  type="checkbox"
                  checked={formik.values.mfaEnabled}
                  onChange={(e) => formik.setFieldValue('mfaEnabled', e.target.checked)}
                />
              </Box>
              <Box display="flex" justifyContent="flex-end">
                <Button onClick={closeEditModal}>Cancel</Button>
                <Button type="submit" loading={formik.isSubmitting}>
                  Update
                </Button>
              </Box>
            </>
          )}
        </Form>
      </Modal>

      <Modal
        open={isDeleteModalOpen}
        onClose={closeDeleteModal}
        title="Confirm Delete"
        actions={
          <>
            <Button onClick={closeDeleteModal}>Cancel</Button>
            <Button variant="contained" color="error" onClick={handleDeleteUser}>
              Delete
            </Button>
          </>
        }
      >
        <Typography>
          Are you sure you want to delete user "{selectedUser?.username}"?
        </Typography>
      </Modal>

      <Snackbar
        open={notification.open}
        autoHideDuration={5000}
        onClose={closeNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={closeNotification} severity={notification.type} sx={{ width: '100%' }}>
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default UserManagement;