import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Paper,
  Divider,
  IconButton,
  Tooltip,
  Grid,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Edit,
  Delete,
  FilterList,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0

import Table from '../common/Table';
import Button from '../common/Button';
import Modal from '../common/Modal';
import ValidationRuleEditor from '../quality/ValidationRuleEditor';
import { useApi } from '../../hooks/useApi';
import { useNotification } from '../../hooks/useNotification';
import qualityService from '../../services/api/qualityService';
import {
  QualityRule,
  QualityRuleType,
  QualityDimension,
  AlertSeverity,
} from '../../types/api';

/**
 * Interface defining the props for the ValidationRulesConfig component
 */
interface ValidationRulesConfigProps {
  /**
   * Value that changes to trigger a refresh of the rules list
   */
  refreshTrigger?: number;
  /**
   * Callback to notify parent component that a refresh occurred
   */
  onRefresh?: () => void;
  /**
   * Additional CSS class for styling
   */
  className?: string;
}

/**
 * Interface defining the state for filtering validation rules
 */
interface FilterState {
  /**
   * Filter by dataset name
   */
  dataset?: string;
  /**
   * Filter by table name
   */
  table?: string;
  /**
   * Filter by rule type
   */
  ruleType?: QualityRuleType;
  /**
   * Filter by quality dimension
   */
  dimension?: QualityDimension;
  /**
   * Filter by severity level
   */
  severity?: AlertSeverity;
  /**
   * Filter by active status
   */
  isActive?: boolean;
}

/**
 * Component for filtering validation rules
 */
const FilterControls: React.FC<{
  filters: FilterState;
  onFilterChange: (filters: FilterState) => void;
}> = ({ filters, onFilterChange }) => {
  return (
    <Box>
      {/* Render filter inputs for dataset, table, rule type, dimension, severity, and active status */}
      {/* Handle filter input changes */}
      {/* Provide clear filters button */}
    </Box>
  );
};

/**
 * Formats rule type enum value for display
 */
const formatRuleType = (ruleType: string) => {
  const formatted = ruleType.replace(/_/g, ' ');
  return formatted.charAt(0).toUpperCase() + formatted.slice(1).toLowerCase();
};

/**
 * Formats severity enum value for display
 */
const formatSeverity = (severity: string) => {
  return severity.charAt(0).toUpperCase() + severity.slice(1).toLowerCase();
};

/**
 * Formats quality dimension enum value for display
 */
const formatDimension = (dimension: string) => {
  return dimension.charAt(0).toUpperCase() + dimension.slice(1).toLowerCase();
};

/**
 * Component for managing data quality validation rules
 */
const ValidationRulesConfig: React.FC<ValidationRulesConfigProps> = ({
  refreshTrigger,
  onRefresh,
  className,
}) => {
  // Initialize state for rules list, pagination, loading, and error
  const [rules, setRules] = useState<QualityRule[]>([]);
  const [totalRules, setTotalRules] = useState<number>(0);
  const [page, setPage] = useState<number>(0);
  const [pageSize, setPageSize] = useState<number>(10);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // Initialize state for selected rule, modal visibility, and filters
  const [selectedRule, setSelectedRule] = useState<QualityRule | null>(null);
  const [showRuleModal, setShowRuleModal] = useState<boolean>(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState<boolean>(false);
  const [filters, setFilters] = useState<FilterState>({});

  // Define table columns for displaying rules
  const tableColumns = React.useMemo(
    () => [
      { id: 'ruleName', label: 'Rule Name', sortable: true },
      { id: 'targetDataset', label: 'Dataset', sortable: true },
      { id: 'targetTable', label: 'Table', sortable: true },
      {
        id: 'ruleType',
        label: 'Rule Type',
        sortable: true,
        format: formatRuleType,
      },
      {
        id: 'dimension',
        label: 'Dimension',
        sortable: true,
        format: formatDimension,
      },
      {
        id: 'severity',
        label: 'Severity',
        sortable: true,
        format: formatSeverity,
      },
      {
        id: 'isActive',
        label: 'Status',
        sortable: true,
        format: (value: any) => (value ? 'Active' : 'Inactive'),
      },
      {
        id: 'actions',
        label: 'Actions',
        sortable: false,
        renderCell: (value: any, row: QualityRule) => (
          <>
            <Tooltip title="Edit Rule">
              <IconButton
                size="small"
                onClick={() => handleEditRule(row)}
              >
                <Edit />
              </IconButton>
            </Tooltip>
            <Tooltip title="Delete Rule">
              <IconButton
                size="small"
                onClick={() => handleDeleteRule(row)}
              >
                <Delete />
              </IconButton>
            </Tooltip>
          </>
        ),
      },
    ],
    []
  );

  // Implement API calls for fetching, creating, updating, and deleting rules
  const { executeRequest: fetchRulesRequest } = useApi();
  const { executeRequest: deleteQualityRuleRequest } = useApi();

  // Notification hook for displaying messages
  const { showSuccess, showError } = useNotification();

  // Define callback functions for rule management
  const fetchRules = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchRulesRequest(qualityService.getQualityRules, {
        page: page + 1,
        pageSize,
        ...filters,
      });
      setRules(response?.items || []);
      setTotalRules(response?.pagination?.totalItems || 0);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch rules');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, filters, fetchRulesRequest]);

  const handleAddRule = useCallback(() => {
    setSelectedRule(null);
    setShowRuleModal(true);
  }, []);

  const handleEditRule = useCallback((rule: QualityRule) => {
    setSelectedRule(rule);
    setShowRuleModal(true);
  }, []);

  const handleDeleteRule = useCallback((rule: QualityRule) => {
    setSelectedRule(rule);
    setShowDeleteConfirm(true);
  }, []);

  const confirmDeleteRule = useCallback(async () => {
    if (!selectedRule) return;

    try {
      await deleteQualityRuleRequest(qualityService.deleteQualityRule, selectedRule.ruleId);
      showSuccess('Rule deleted successfully');
      setShowDeleteConfirm(false);
      setSelectedRule(null);
      fetchRules();
    } catch (err: any) {
      showError(err.message || 'Failed to delete rule');
    }
  }, [selectedRule, deleteQualityRuleRequest, showSuccess, showError, fetchRules]);

  const handleRuleSave = useCallback((rule: QualityRule) => {
    setShowRuleModal(false);
    setSelectedRule(null);
    fetchRules();
    onRefresh?.();
  }, [fetchRules, onRefresh]);

  const handleModalClose = useCallback(() => {
    setShowRuleModal(false);
    setSelectedRule(null);
  }, []);

  const handlePageChange = useCallback((newPage: number, newPageSize: number) => {
    setPage(newPage - 1);
    setPageSize(newPageSize);
  }, []);

  const handleFilterChange = useCallback((newFilters: FilterState) => {
    setFilters(newFilters);
    setPage(0);
  }, []);

  // Fetch rules on component mount and when filters or pagination change
  useEffect(() => {
    fetchRules();
  }, [refreshTrigger, page, pageSize, filters, fetchRules]);

  return (
    <Box sx={{ padding: (theme) => theme.spacing(2), backgroundColor: (theme) => theme.palette.background.paper, borderRadius: (theme) => theme.shape.borderRadius, boxShadow: (theme) => theme.shadows[1] }} className={className}>
      {/* Render header with title and add button */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: (theme) => theme.spacing(2) }}>
        <Typography variant="h6" sx={{ fontWeight: '500' }}>
          Validation Rules
        </Typography>
        <Button variant="contained" startIcon={<Add />} onClick={handleAddRule}>
          Add Rule
        </Button>
      </Box>

      {/* Render filter controls for dataset, table, rule type, etc. */}
      <Box sx={{ marginBottom: (theme) => theme.spacing(2), padding: (theme) => theme.spacing(2), backgroundColor: (theme) => theme.palette.background.default, borderRadius: (theme) => theme.shape.borderRadius }}>
        <FilterControls filters={filters} onFilterChange={handleFilterChange} />
      </Box>

      {/* Render Table component with rules data */}
      <Table
        columns={tableColumns}
        data={rules}
        loading={loading}
        error={error}
        totalItems={totalRules}
        page={page + 1}
        pageSize={pageSize}
        onPageChange={handlePageChange}
      />

      {/* Render Modal with ValidationRuleEditor for creating/editing rules */}
      <Modal open={showRuleModal} onClose={handleModalClose} title={selectedRule ? 'Edit Validation Rule' : 'Create Validation Rule'}>
        <ValidationRuleEditor
          rule={selectedRule}
          onSave={handleRuleSave}
          onCancel={handleModalClose}
        />
      </Modal>

      {/* Render confirmation dialog for rule deletion */}
      {showDeleteConfirm && (
        <Modal open={showDeleteConfirm} onClose={() => setShowDeleteConfirm(false)} title="Confirm Delete">
          <Typography>Are you sure you want to delete this rule?</Typography>
          <Box display="flex" justifyContent="flex-end" mt={2}>
            <Button onClick={() => setShowDeleteConfirm(false)} variant="outlined">
              Cancel
            </Button>
            <Button onClick={confirmDeleteRule} variant="contained" color="error">
              Delete
            </Button>
          </Box>
        </Modal>
      )}
    </Box>
  );
};

export default ValidationRulesConfig;