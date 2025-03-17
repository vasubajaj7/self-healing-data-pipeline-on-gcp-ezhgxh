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
  FormGroup,
  FormControlLabel,
  Checkbox,
  Divider,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Edit,
  Delete,
  Security,
  AdminPanelSettings,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11
import Table from '../common/Table';
import Modal from '../common/Modal';
import Form from '../common/Form';
import Input from '../common/Input';
import Button from '../common/Button';
import adminService from '../../services/api/adminService';
import { UserRole, UserPermission } from '../../types/user';
import { useAuth } from '../../contexts/AuthContext';
import { RoleFormValues } from '../../types/user';
import { PermissionGroup } from '../../types/user';

/**
 * Functional component for managing roles and permissions.
 * Displays a table of existing roles and provides options to create, edit, and delete roles.
 */
const RoleManagement: React.FC = () => {
  // Authentication context to check user permissions
  const { checkPermission } = useAuth();

  // State variables for managing roles, permissions, loading, and errors
  const [roles, setRoles] = useState<UserRole[]>([]);
  const [permissions, setPermissions] = useState<UserPermission[]>(Object.values(UserPermission));
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // State variables for managing modal visibility and selected role
  const [isCreateModalOpen, setIsCreateModalOpen] = useState<boolean>(false);
  const [isEditModalOpen, setIsEditModalOpen] = useState<boolean>(false);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState<boolean>(false);
  const [selectedRole, setSelectedRole] = useState<UserRole | null>(null);

  // State for notification snackbar
  const [notification, setNotification] = useState<{
    open: boolean;
    message: string;
    type: 'success' | 'error' | 'info' | 'warning';
  }>({ open: false, message: '', type: 'info' });

  /**
   * Effect to load roles when the component mounts.
   */
  useEffect(() => {
    fetchRoles();
  }, []);

  /**
   * Fetches roles from the API and updates the state.
   */
  const fetchRoles = useCallback(async () => {
    setLoading(true);
    try {
      const response = await adminService.getRoles();
      setRoles(response.data);
      setLoading(false);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch roles.');
      setLoading(false);
    }
  }, []);

  /**
   * Handles the creation of a new role.
   * @param values - The form values for the new role.
   * @param helpers - Formik helpers for managing the form state.
   */
  const handleCreateRole = async (values: RoleFormValues, helpers: FormikHelpers<any>) => {
    try {
      await adminService.createRole(values);
      closeCreateModal();
      showNotification('Role created successfully', 'success');
      await fetchRoles();
      helpers.resetForm();
    } catch (err: any) {
      showNotification(err.message || 'Failed to create role', 'error');
    }
  };

  /**
   * Handles the update of an existing role.
   * @param values - The form values for the updated role.
   * @param helpers - Formik helpers for managing the form state.
   */
  const handleUpdateRole = async (values: RoleFormValues, helpers: FormikHelpers<any>) => {
    try {
      if (selectedRole?.id) {
        await adminService.updateRole(selectedRole.id, values);
        closeEditModal();
        showNotification('Role updated successfully', 'success');
        await fetchRoles();
        helpers.resetForm();
      }
    } catch (err: any) {
      showNotification(err.message || 'Failed to update role', 'error');
    }
  };

  /**
   * Handles the deletion of a role.
   */
  const handleDeleteRole = async () => {
    try {
      if (selectedRole?.id) {
        await adminService.deleteRole(selectedRole.id);
        closeDeleteModal();
        showNotification('Role deleted successfully', 'success');
        await fetchRoles();
      }
    } catch (err: any) {
      showNotification(err.message || 'Failed to delete role', 'error');
    }
  };

  /**
   * Opens the create role modal.
   */
  const openCreateModal = () => setIsCreateModalOpen(true);

  /**
   * Closes the create role modal.
   */
  const closeCreateModal = () => setIsCreateModalOpen(false);

  /**
   * Opens the edit role modal with the specified role.
   * @param role - The role to edit.
   */
  const openEditModal = (role: UserRole) => {
    setSelectedRole(role);
    setIsEditModalOpen(true);
  };

  /**
   * Closes the edit role modal.
   */
  const closeEditModal = () => {
    setIsEditModalOpen(false);
    setSelectedRole(null);
  };

  /**
   * Opens the delete role modal with the specified role.
   * @param role - The role to delete.
   */
  const openDeleteModal = (role: UserRole) => {
    setSelectedRole(role);
    setIsDeleteModalOpen(true);
  };

  /**
   * Closes the delete role modal.
   */
  const closeDeleteModal = () => {
    setIsDeleteModalOpen(false);
    setSelectedRole(null);
  };

  /**
   * Shows a notification message.
   * @param message - The message to display.
   * @param type - The type of notification (success, error, info, warning).
   */
  const showNotification = (message: string, type: 'success' | 'error' | 'info' | 'warning') => {
    setNotification({ open: true, message, type });
  };

  /**
   * Closes the notification snackbar.
   */
  const closeNotification = () => {
    setNotification({ ...notification, open: false });
  };

  /**
   * Groups permissions by their category for better organization in the UI.
   */
  const groupPermissionsByCategory = useCallback(() => {
    const grouped: Record<string, UserPermission[]> = {};

    permissions.forEach(permission => {
      const category = permission.split('_')[1]; // e.g., VIEW_DASHBOARD -> DASHBOARD
      if (!grouped[category]) {
        grouped[category] = [];
      }
      grouped[category].push(permission);
    });

    return grouped;
  }, [permissions]);

  // Check if the user has the required permission to manage roles
  if (!checkPermission(UserPermission.MANAGE_ROLES)) {
    return (
      <Alert severity="error">
        You do not have permission to manage roles.
      </Alert>
    );
  }

  // Define the columns for the roles table
  const columns = React.useMemo(
    () => [
      { id: 'name', label: 'Name', sortable: true },
      { id: 'description', label: 'Description' },
      {
        id: 'actions',
        label: 'Actions',
        sortable: false,
        renderCell: (_value: any, row: UserRole) => (
          <>
            <Tooltip title="Edit">
              <IconButton onClick={() => openEditModal(row)} aria-label="edit">
                <Edit />
              </IconButton>
            </Tooltip>
            <Tooltip title="Delete">
              <IconButton onClick={() => openDeleteModal(row)} aria-label="delete">
                <Delete />
              </IconButton>
            </Tooltip>
          </>
        ),
      },
    ],
    [openEditModal, openDeleteModal]
  );

  // Group permissions by category for the permission selection UI
  const groupedPermissions = React.useMemo(() => groupPermissionsByCategory(), [groupPermissionsByCategory]);

  // Define the validation schema for the role form
  const validationSchema = yup.object().shape({
    name: yup.string().required('Name is required'),
    description: yup.string(),
    permissions: yup.array().of(yup.string()).required('Permissions are required'),
  });

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h4" component="h1">
          Role Management
        </Typography>
        <Button variant="contained" startIcon={<Add />} onClick={openCreateModal}>
          Create Role
        </Button>
      </Box>

      <Table
        columns={columns}
        data={roles}
        loading={loading}
        error={error}
        emptyMessage="No roles found."
        title="Roles"
      />

      <Modal
        open={isCreateModalOpen}
        onClose={closeCreateModal}
        title="Create Role"
      >
        <Form
          initialValues={{ name: '', description: '', permissions: [] }}
          validationSchema={validationSchema}
          onSubmit={handleCreateRole}
        >
          {({ values, handleChange, handleSubmit, errors, touched, setFieldValue }) => (
            <Box>
              <Input
                label="Name"
                name="name"
                value={values.name}
                onChange={handleChange}
                error={touched.name && !!errors.name}
                helperText={touched.name && errors.name}
                fullWidth
              />
              <Input
                label="Description"
                name="description"
                value={values.description}
                onChange={handleChange}
                fullWidth
                multiline
                rows={3}
              />
              <Typography variant="subtitle1">Permissions</Typography>
              <FormGroup>
                {Object.entries(groupedPermissions).map(([category, perms]) => (
                  <Paper key={category} sx={{ p: 2, mb: 1 }}>
                    <Typography variant="subtitle2">{category}</Typography>
                    <Divider sx={{ my: 1 }} />
                    <Grid container spacing={1}>
                      {perms.map(permission => (
                        <Grid item xs={12} sm={6} md={4} key={permission}>
                          <FormControlLabel
                            control={
                              <Checkbox
                                checked={values.permissions.includes(permission)}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setFieldValue('permissions', [...values.permissions, permission]);
                                  } else {
                                    setFieldValue('permissions', values.permissions.filter((p: string) => p !== permission));
                                  }
                                }}
                                name={permission}
                              />
                            }
                            label={permission}
                          />
                        </Grid>
                      ))}
                    </Grid>
                  </Paper>
                ))}
              </FormGroup>
              <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
                <Button onClick={closeCreateModal} sx={{ mr: 1 }}>
                  Cancel
                </Button>
                <Button variant="contained" onClick={handleSubmit}>
                  Create
                </Button>
              </Box>
            </Box>
          )}
        </Form>
      </Modal>

      <Modal
        open={isEditModalOpen}
        onClose={closeEditModal}
        title="Edit Role"
      >
        <Form
          initialValues={{
            name: selectedRole?.name || '',
            description: selectedRole?.description || '',
            permissions: getPermissionsForRole(selectedRole?.name as UserRole) || [],
          }}
          validationSchema={validationSchema}
          onSubmit={handleUpdateRole}
          enableReinitialize
        >
          {({ values, handleChange, handleSubmit, errors, touched, setFieldValue }) => (
            <Box>
              <Input
                label="Name"
                name="name"
                value={values.name}
                onChange={handleChange}
                error={touched.name && !!errors.name}
                helperText={touched.name && errors.name}
                fullWidth
              />
              <Input
                label="Description"
                name="description"
                value={values.description}
                onChange={handleChange}
                fullWidth
                multiline
                rows={3}
              />
              <Typography variant="subtitle1">Permissions</Typography>
              <FormGroup>
                {Object.entries(groupedPermissions).map(([category, perms]) => (
                  <Paper key={category} sx={{ p: 2, mb: 1 }}>
                    <Typography variant="subtitle2">{category}</Typography>
                    <Divider sx={{ my: 1 }} />
                    <Grid container spacing={1}>
                      {perms.map(permission => (
                        <Grid item xs={12} sm={6} md={4} key={permission}>
                          <FormControlLabel
                            control={
                              <Checkbox
                                checked={values.permissions.includes(permission)}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setFieldValue('permissions', [...values.permissions, permission]);
                                  } else {
                                    setFieldValue('permissions', values.permissions.filter((p: string) => p !== permission));
                                  }
                                }}
                                name={permission}
                              />
                            }
                            label={permission}
                          />
                        </Grid>
                      ))}
                    </Grid>
                  </Paper>
                ))}
              </FormGroup>
              <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
                <Button onClick={closeEditModal} sx={{ mr: 1 }}>
                  Cancel
                </Button>
                <Button variant="contained" onClick={handleSubmit}>
                  Update
                </Button>
              </Box>
            </Box>
          )}
        </Form>
      </Modal>

      <Modal
        open={isDeleteModalOpen}
        onClose={closeDeleteModal}
        title="Delete Role"
      >
        <Box sx={{ p: 3 }}>
          <Typography>
            Are you sure you want to delete role "{selectedRole?.name}"?
          </Typography>
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'flex-end' }}>
            <Button onClick={closeDeleteModal} sx={{ mr: 1 }}>
              Cancel
            </Button>
            <Button variant="contained" color="error" onClick={handleDeleteRole}>
              Delete
            </Button>
          </Box>
        </Box>
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

export default RoleManagement;