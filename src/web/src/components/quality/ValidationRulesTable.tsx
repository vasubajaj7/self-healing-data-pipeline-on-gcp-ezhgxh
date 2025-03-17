import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import { Box, Grid, Typography, Tooltip, IconButton } from '@mui/material'; // @mui/material ^5.11.0
import { Edit, Delete, FilterList, Search, Add, Refresh } from '@mui/icons-material'; // @mui/icons-material ^5.11.0

import Table from '../common/Table';
import Button from '../common/Button';
import Select from '../common/Select';
import Input from '../common/Input';
import Badge from '../common/Badge';
import Modal from '../common/Modal';
import { useApi } from '../../hooks/useApi';
import qualityService from '../../services/api/qualityService';
import { QualityRule } from '../../types/api';
import { QualityRuleType, QualityDimension } from '../../types/quality';
import { AlertSeverity } from '../../types/global.d';
import { useNotification } from '../../hooks/useNotification';

/**
 * Interface for the ValidationRulesTable component properties
 */
interface ValidationRulesTableProps {
  dataset?: string;
  table?: string;
  onRuleSelect?: (rule: QualityRule) => void;
  onRuleEdit?: (rule: QualityRule) => void;
  onRuleAdd?: () => void;
  refreshInterval?: number;
  showFilters?: boolean;
  showActions?: boolean;
  className?: string;
}

/**
 * Interface for the rule filters state
 */
interface RuleFilters {
  ruleType?: string;
  dimension?: string;
  severity?: string;
  isActive?: boolean;
  searchTerm?: string;
}

/**
 * Converts QualityRuleType enum to select options
 */
const getRuleTypeOptions = () => {
  return Object.values(QualityRuleType).map(ruleType => ({
    value: ruleType,
    label: formatRuleType(ruleType),
  }));
};

/**
 * Converts QualityDimension enum to select options
 */
const getDimensionOptions = () => {
  return Object.values(QualityDimension).map(dimension => ({
    value: dimension,
    label: dimension.replace('_', ' '),
  }));
};

/**
 * Converts AlertSeverity enum to select options
 */
const getSeverityOptions = () => {
  return Object.values(AlertSeverity).map(severity => ({
    value: severity,
    label: severity,
  }));
};

/**
 * Formats rule type for display
 */
const formatRuleType = (ruleType: string) => {
  const formatted = ruleType.replace(/_/g, ' ');
  return formatted.charAt(0).toUpperCase() + formatted.slice(1);
};

/**
 * Returns the appropriate color for a severity level
 */
const getSeverityColor = (severity: AlertSeverity) => {
  switch (severity) {
    case AlertSeverity.CRITICAL:
      return 'error';
    case AlertSeverity.HIGH:
      return 'warning';
    case AlertSeverity.MEDIUM:
      return 'info';
    case AlertSeverity.LOW:
      return 'default';
    default:
      return 'default';
  }
};

/**
 * Functional component for filter controls
 */
const FilterSection: React.FC<{
  filters: RuleFilters;
  onFilterChange: (filters: Partial<RuleFilters>) => void;
  onRefresh: () => void;
}> = ({ filters, onFilterChange, onRefresh }) => {
  const handleFilterChange = (field: keyof RuleFilters, value: any) => {
    onFilterChange({ ...filters, [field]: value });
  };

  return (
    <Grid container spacing={2} alignItems="center">
      <Grid item xs={12} sm={6} md={3}>
        <Select
          label="Rule Type"
          value={filters.ruleType || ''}
          onChange={(value) => handleFilterChange('ruleType', value)}
          options={getRuleTypeOptions()}
          fullWidth
        />
      </Grid>
      <Grid item xs={12} sm={6} md={3}>
        <Select
          label="Dimension"
          value={filters.dimension || ''}
          onChange={(value) => handleFilterChange('dimension', value)}
          options={getDimensionOptions()}
          fullWidth
        />
      </Grid>
      <Grid item xs={12} sm={6} md={3}>
        <Select
          label="Severity"
          value={filters.severity || ''}
          onChange={(value) => handleFilterChange('severity', value)}
          options={getSeverityOptions()}
          fullWidth
        />
      </Grid>
      <Grid item xs={12} sm={6} md={3}>
        <Input
          label="Search"
          value={filters.searchTerm || ''}
          onChange={(value) => handleFilterChange('searchTerm', value)}
          placeholder="Search rules..."
          fullWidth
          InputProps={{
            endAdornment: (
              <IconButton onClick={onRefresh}>
                <Refresh />
              </IconButton>
            ),
          }}
        />
      </Grid>
    </Grid>
  );
};

/**
 * Functional component for action buttons in each row
 */
const ActionButtons: React.FC<{
  rule: QualityRule;
  onEdit: (rule: QualityRule) => void;
  onDelete: (rule: QualityRule) => void;
}> = ({ rule, onEdit, onDelete }) => {
  return (
    <Box display="flex" justifyContent="flex-end">
      <Tooltip title="Edit Rule">
        <IconButton onClick={() => onEdit(rule)}>
          <Edit />
        </IconButton>
      </Tooltip>
      <Tooltip title="Delete Rule">
        <IconButton onClick={() => onDelete(rule)}>
          <Delete />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

/**
 * Functional component for displaying severity levels
 */
const SeverityBadge: React.FC<{ severity: AlertSeverity }> = ({ severity }) => {
  const color = getSeverityColor(severity);
  return <Badge label={severity} color={color} />;
};

/**
 * Functional component for displaying active status
 */
const ActiveStatusBadge: React.FC<{ isActive: boolean }> = ({ isActive }) => {
  return <Badge label={isActive ? 'Active' : 'Inactive'} color={isActive ? 'success' : 'default'} />;
};

/**
 * Functional component for delete confirmation modal
 */
const DeleteConfirmationModal: React.FC<{
  rule: QualityRule | null;
  open: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}> = ({ rule, open, onConfirm, onCancel }) => {
  return (
    <Modal open={open} onClose={onCancel} title="Confirm Delete">
      <Typography>
        Are you sure you want to delete rule <strong>{rule?.ruleName}</strong>?
      </Typography>
      <Box display="flex" justifyContent="flex-end" mt={3}>
        <Button onClick={onCancel} color="primary">
          Cancel
        </Button>
        <Button onClick={onConfirm} color="error">
          Delete
        </Button>
      </Box>
    </Modal>
  );
};

/**
 * Main functional component for the ValidationRulesTable
 */
const ValidationRulesTable: React.FC<ValidationRulesTableProps> = ({
  dataset,
  table,
  onRuleSelect,
  onRuleEdit,
  onRuleAdd,
  refreshInterval = 60000,
  showFilters = true,
  showActions = true,
  className,
}) => {
  // State variables
  const [rules, setRules] = useState<QualityRule[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<RuleFilters>({
    ruleType: '',
    dimension: '',
    severity: '',
    isActive: undefined,
    searchTerm: '',
  });
  const [pagination, setPagination] = useState({ page: 0, pageSize: 10, totalItems: 0 });
  const [deleteRule, setDeleteRule] = useState<QualityRule | null>(null);
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState<boolean>(false);

  // API hooks
  const { get } = useApi();
  const { delete: apiDelete } = useApi();
  const { showSuccess, showError } = useNotification();

  // Fetch rules data
  useEffect(() => {
    const fetchRules = async () => {
      setLoading(true);
      setError(null);
      try {
        const params: any = {
          page: pagination.page + 1,
          pageSize: pagination.pageSize,
          dataset: dataset,
          table: table,
          ruleType: filters.ruleType,
          dimension: filters.dimension,
          severity: filters.severity,
          searchTerm: filters.searchTerm,
        };
        if (filters.isActive !== undefined) {
          params.isActive = filters.isActive;
        }
        const response = await get<any>(qualityService.getQualityRules, params);
        setRules(response.items);
        setPagination({
          ...pagination,
          totalItems: response.pagination.totalItems,
        });
      } catch (err: any) {
        setError(err.message || 'Failed to fetch rules');
      } finally {
        setLoading(false);
      }
    };

    fetchRules();
  }, [dataset, table, filters, pagination.page, pagination.pageSize, get]);

  // Set up refresh interval
  useEffect(() => {
    if (refreshInterval > 0) {
      const intervalId = setInterval(() => {
        setRules([]); // Clear existing rules to trigger refresh
      }, refreshInterval);
      return () => clearInterval(intervalId);
    }
    return undefined;
  }, [refreshInterval]);

  // Filter change handler
  const handleFilterChange = (newFilters: Partial<RuleFilters>) => {
    setFilters({ ...filters, ...newFilters });
    setPagination({ ...pagination, page: 0 }); // Reset to first page
  };

  // Search change handler (debounced)
  const handleSearchChange = useCallback((searchTerm: string) => {
    handleFilterChange({ searchTerm });
  }, [handleFilterChange]);

  // Refresh handler
  const handleRefresh = () => {
    setRules([]); // Clear existing rules to trigger refresh
  };

  // Rule click handler
  const handleRuleClick = (rule: QualityRule) => {
    if (onRuleSelect) {
      onRuleSelect(rule);
    }
  };

  // Edit click handler
  const handleEditClick = (rule: QualityRule) => {
    if (onRuleEdit) {
      onRuleEdit(rule);
    }
  };

  // Delete click handler
  const handleDeleteClick = (rule: QualityRule) => {
    setDeleteRule(rule);
    setShowDeleteConfirmation(true);
  };

  // Delete confirmation handler
  const handleDeleteConfirm = async () => {
    setShowDeleteConfirmation(false);
    if (deleteRule) {
      try {
        await apiDelete(qualityService.deleteQualityRule, deleteRule.ruleId);
        setRules(rules.filter((rule) => rule.ruleId !== deleteRule.ruleId));
        showSuccess('Rule deleted successfully');
      } catch (err: any) {
        showError(err.message || 'Failed to delete rule');
      } finally {
        setDeleteRule(null);
      }
    }
  };

  // Page change handler
  const handlePageChange = (newPage: number) => {
    setPagination({ ...pagination, page: newPage });
  };

  // Table columns configuration
  const columns = useMemo(() => [
    { id: 'ruleName', label: 'Rule Name', sortable: true, minWidth: 150 },
    { id: 'targetDataset', label: 'Dataset', sortable: true, minWidth: 120 },
    { id: 'targetTable', label: 'Table', sortable: true, minWidth: 120 },
    { id: 'ruleType', label: 'Rule Type', sortable: true, minWidth: 120, format: formatRuleType },
    { id: 'severity', label: 'Severity', sortable: true, minWidth: 100, renderCell: (severity: AlertSeverity) => <SeverityBadge severity={severity} /> },
    { id: 'isActive', label: 'Status', sortable: true, minWidth: 100, renderCell: (isActive: boolean) => <ActiveStatusBadge isActive={isActive} /> },
    { id: 'updatedAt', label: 'Last Updated', sortable: true, minWidth: 150, format: (date: string) => new Date(date).toLocaleDateString() },
    {
      id: 'actions',
      label: 'Actions',
      sortable: false,
      minWidth: 100,
      renderCell: (value: any, row: QualityRule) => (
        <ActionButtons rule={row} onEdit={handleEditClick} onDelete={handleDeleteClick} />
      ),
    },
  ], [handleEditClick, handleDeleteClick]);

  return (
    <Box className={className}>
      {showFilters && (
        <FilterSection
          filters={filters}
          onFilterChange={handleFilterChange}
          onRefresh={handleRefresh}
        />
      )}
      <Table
        title="Validation Rules"
        data={rules}
        columns={columns}
        loading={loading}
        error={error}
        pagination
        onPageChange={handlePageChange}
        totalItems={pagination.totalItems}
        onRowClick={handleRuleClick}
        actions={
          showActions && (
            <Button variant="contained" color="primary" onClick={onRuleAdd}>
              Add Rule
            </Button>
          )
        }
      />
      <DeleteConfirmationModal
        open={showDeleteConfirmation}
        rule={deleteRule}
        onConfirm={handleDeleteConfirm}
        onCancel={() => setShowDeleteConfirmation(false)}
      />
    </Box>
  );
};

export default ValidationRulesTable;