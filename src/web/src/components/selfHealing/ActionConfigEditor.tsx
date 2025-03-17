import React, { useState, useEffect, useCallback } from 'react';
import * as yup from 'yup';

// MUI Components
import {
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Switch,
  Button,
  CircularProgress,
  Divider,
  Alert,
  Grid,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import { ExpandMore, PlayArrow, Save } from '@mui/icons-material';

// JSON Editor
import JSONEditor from 'jsoneditor-react';

// Internal Components
import Form from '../common/Form';
import Card from '../common/Card';

// Services and Types
import healingService from '../../services/api/healingService';
import { HealingAction, ActionType, IssueType } from '../../types/selfHealing';

// Hooks
import useNotification from '../../hooks/useNotification';
import useApi from '../../hooks/useApi';

/**
 * Props interface for the ActionConfigEditor component
 */
interface ActionConfigEditorProps {
  actionId?: string;
  patternId: string;
  initialAction?: Partial<HealingAction>;
  onSave?: (action: HealingAction) => void;
  onCancel?: () => void;
  className?: string;
}

/**
 * Interface for the form values used in the action editor
 */
interface ActionFormValues {
  name: string;
  actionType: ActionType;
  description: string;
  actionDefinition: object;
  isActive: boolean;
  metadata?: object;
}

/**
 * Interface for action test results
 */
interface TestResult {
  success: boolean;
  message: string;
  data?: object;
  executionTime?: number;
}

/**
 * Returns an array of options for action type selection
 */
const getActionTypeOptions = () => {
  return Object.values(ActionType).map(type => ({
    value: type,
    label: type.replace(/_/g, ' ')
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ')
  }));
};

/**
 * Returns a default action definition object based on the selected action type
 */
const getDefaultActionDefinition = (actionType: ActionType): object => {
  switch (actionType) {
    case ActionType.DATA_CORRECTION:
      return {
        correctionStrategy: 'imputation', // imputation, removal, fixed_value
        parameters: {
          method: 'ml', // ml, statistical, fixed
          confidence_threshold: 85,
          fields: []
        }
      };
      
    case ActionType.PARAMETER_ADJUSTMENT:
      return {
        parameters: {
          // Parameter names and values will go here
        },
        adjustmentStrategy: 'override', // override, increment, percentage
        maxAdjustmentAmount: 200, // percentage or absolute value
        minValue: 0,
        maxValue: null // null for no upper limit
      };
      
    case ActionType.RESOURCE_OPTIMIZATION:
      return {
        resourceType: 'bigquery_slots', // bigquery_slots, memory, workers
        optimizationStrategy: 'auto_scale', // auto_scale, fixed_allocation, throttling
        parameters: {
          min_allocation: 50,
          max_allocation: 200,
          scale_factor: 1.5,
          cooldown_period_minutes: 10
        }
      };
      
    case ActionType.RETRY:
      return {
        maxAttempts: 3,
        backoffStrategy: 'exponential', // none, linear, exponential
        initialDelayMs: 1000,
        maxDelayMs: 60000,
        jitterMs: 100
      };
      
    case ActionType.SCHEMA_CORRECTION:
      return {
        correctionType: 'auto_adapt', // auto_adapt, enforce_schema, flexible_schema
        schemaValidationLevel: 'lenient', // strict, lenient
        allowedChanges: ['add_nullable_column', 'type_conversion_safe'],
        typeConversions: {
          // e.g., "string_to_int": true
        }
      };
      
    default:
      return {};
  }
};

/**
 * Form component for creating and editing healing actions
 */
const ActionConfigEditor: React.FC<ActionConfigEditorProps> = ({
  actionId,
  patternId,
  initialAction,
  onSave,
  onCancel,
  className
}) => {
  // State
  const [action, setAction] = useState<Partial<HealingAction>>(
    initialAction || { patternId, isActive: true, actionDefinition: {} }
  );
  const [loading, setLoading] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [testLoading, setTestLoading] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testData, setTestData] = useState<object>({});

  // Hooks
  const { showSuccess, showError } = useNotification();

  // Effects
  useEffect(() => {
    // If actionId is provided, fetch action data
    if (actionId) {
      fetchAction();
    }
  }, [actionId]);

  useEffect(() => {
    // Update action state when initialAction changes
    if (initialAction) {
      setAction(initialAction);
    }
  }, [initialAction]);

  // Fetch action data
  const fetchAction = async () => {
    if (!actionId) return;

    setLoading(true);
    setError(null);

    try {
      const response = await healingService.getHealingAction(actionId);
      setAction(response.data);
    } catch (err) {
      setError('Failed to fetch action data');
      showError('Failed to fetch action data. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  // Handle form submission
  const handleSubmit = async (values: ActionFormValues, formHelpers: any) => {
    setSaving(true);
    setError(null);

    try {
      // Prepare action data
      const actionData: Partial<HealingAction> = {
        ...values,
        patternId,
      };

      let response;

      if (actionId) {
        // Update existing action
        response = await healingService.updateHealingAction(actionId, actionData);
      } else {
        // Create new action
        response = await healingService.createHealingAction(actionData);
      }

      showSuccess(`Action successfully ${actionId ? 'updated' : 'created'}`);
      
      // Call onSave callback if provided
      if (onSave && response.data) {
        onSave(response.data);
      }
    } catch (err) {
      setError('Failed to save action');
      showError('Failed to save action. Please check your inputs and try again.');
      
      // Set form error if applicable
      if (err.message) {
        formHelpers.setStatus({ error: err.message });
      }
    } finally {
      setSaving(false);
    }
  };

  // Handle action type change
  const handleActionTypeChange = (newType: ActionType, formikProps: any) => {
    const defaultDefinition = getDefaultActionDefinition(newType);
    formikProps.setValues({
      ...formikProps.values,
      actionType: newType,
      actionDefinition: defaultDefinition
    });

    // Mark fields as touched to ensure validation runs
    formikProps.setFieldTouched('actionType', true);
    formikProps.setFieldTouched('actionDefinition', true);
  };

  // Test the action
  const handleTestAction = async (values: ActionFormValues) => {
    setTestLoading(true);
    setTestResult(null);
    
    try {
      // Prepare test parameters
      const testParams = {
        actionId: actionId,
        patternId: patternId,
        testData: testData
      };

      // Call service to test the rule
      const response = await healingService.testHealingRule(testParams);
      
      // Set test result
      setTestResult({
        success: response.data.actionSuccess || false,
        message: response.data.actionSuccess 
                  ? 'Action test successful' 
                  : 'Action test failed',
        data: response.data.actionResult || {},
        executionTime: response.data.executionTime
      });
    } catch (err) {
      setTestResult({
        success: false,
        message: err.message || 'Test failed due to an error'
      });
    } finally {
      setTestLoading(false);
    }
  };

  // Validate the action form values
  const validateActionForm = (values: ActionFormValues) => {
    const errors: Record<string, string> = {};
    
    if (!values.name || values.name.trim() === '') {
      errors.name = 'Name is required';
    }
    
    if (!values.description || values.description.trim() === '') {
      errors.description = 'Description is required';
    }
    
    if (!values.actionDefinition || Object.keys(values.actionDefinition).length === 0) {
      errors.actionDefinition = 'Action definition is required';
    } else {
      try {
        // Ensure we can stringify the object to validate it
        JSON.stringify(values.actionDefinition);
      } catch (err) {
        errors.actionDefinition = 'Invalid JSON structure';
      }
    }
    
    return errors;
  };

  return (
    <Card
      title={actionId ? 'Edit Healing Action' : 'Create Healing Action'}
      loading={loading}
      error={error}
      className={className}
    >
      <Form
        initialValues={{
          name: action.name || '',
          actionType: action.actionType || ActionType.DATA_CORRECTION,
          description: action.description || '',
          actionDefinition: action.actionDefinition || getDefaultActionDefinition(action.actionType || ActionType.DATA_CORRECTION),
          isActive: action.isActive !== undefined ? action.isActive : true,
          metadata: action.metadata || {}
        }}
        validate={validateActionForm}
        onSubmit={handleSubmit}
        enableReinitialize
      >
        {(formik) => (
          <Box>
            <Grid container spacing={3}>
              {/* Action Name */}
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  id="name"
                  name="name"
                  label="Action Name"
                  value={formik.values.name}
                  onChange={formik.handleChange}
                  onBlur={formik.handleBlur}
                  error={formik.touched.name && Boolean(formik.errors.name)}
                  helperText={formik.touched.name && formik.errors.name}
                  disabled={saving}
                  required
                />
              </Grid>

              {/* Action Type */}
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel id="actionType-label" required>Action Type</InputLabel>
                  <Select
                    labelId="actionType-label"
                    id="actionType"
                    name="actionType"
                    value={formik.values.actionType}
                    onChange={(e) => handleActionTypeChange(e.target.value as ActionType, formik)}
                    onBlur={formik.handleBlur}
                    error={formik.touched.actionType && Boolean(formik.errors.actionType)}
                    disabled={saving || Boolean(actionId)} // Lock action type when editing
                    label="Action Type"
                    required
                  >
                    {getActionTypeOptions().map(option => (
                      <MenuItem key={option.value} value={option.value}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>

              {/* Active Status */}
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <Typography component="div" variant="body1">Active</Typography>
                    <Switch
                      id="isActive"
                      name="isActive"
                      checked={formik.values.isActive}
                      onChange={formik.handleChange}
                      disabled={saving}
                      color="primary"
                    />
                  </Box>
                </FormControl>
              </Grid>

              {/* Description */}
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  id="description"
                  name="description"
                  label="Description"
                  value={formik.values.description}
                  onChange={formik.handleChange}
                  onBlur={formik.handleBlur}
                  error={formik.touched.description && Boolean(formik.errors.description)}
                  helperText={formik.touched.description && formik.errors.description}
                  disabled={saving}
                  multiline
                  rows={3}
                  required
                />
              </Grid>

              {/* Action Definition (JSON Editor) */}
              <Grid item xs={12}>
                <Typography variant="subtitle1" gutterBottom>
                  Action Definition
                  {formik.touched.actionDefinition && formik.errors.actionDefinition && (
                    <Typography variant="caption" color="error" sx={{ ml: 1 }}>
                      {formik.errors.actionDefinition}
                    </Typography>
                  )}
                </Typography>
                <Box sx={{ border: 1, borderColor: 'grey.300', borderRadius: 1, p: 1, height: 300 }}>
                  <JSONEditor
                    value={formik.values.actionDefinition}
                    onChange={(newValue) => formik.setFieldValue('actionDefinition', newValue)}
                    mode="code"
                  />
                </Box>
              </Grid>

              {/* Test Configuration */}
              <Grid item xs={12}>
                <Accordion>
                  <AccordionSummary
                    expandIcon={<ExpandMore />}
                    aria-controls="test-configuration-content"
                    id="test-configuration-header"
                  >
                    <Typography>Test Configuration</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    <Box>
                      <Typography variant="subtitle2" gutterBottom>
                        Test Data
                      </Typography>
                      <Box sx={{ border: 1, borderColor: 'grey.300', borderRadius: 1, p: 1, height: 200, mb: 2 }}>
                        <JSONEditor
                          value={testData}
                          onChange={(newValue) => setTestData(newValue)}
                          mode="code"
                        />
                      </Box>
                      <Button
                        variant="outlined"
                        color="primary"
                        startIcon={testLoading ? <CircularProgress size={20} /> : <PlayArrow />}
                        onClick={() => handleTestAction(formik.values)}
                        disabled={testLoading || !formik.isValid || formik.isSubmitting}
                      >
                        {testLoading ? 'Testing...' : 'Test Action'}
                      </Button>

                      {/* Test Results */}
                      {testResult && (
                        <Box sx={{ mt: 2 }}>
                          <Alert severity={testResult.success ? 'success' : 'error'}>
                            {testResult.message}
                            {testResult.executionTime && (
                              <Typography variant="caption" display="block">
                                Execution time: {testResult.executionTime}ms
                              </Typography>
                            )}
                          </Alert>
                          {testResult.data && Object.keys(testResult.data).length > 0 && (
                            <Box sx={{ mt: 1, maxHeight: 200, overflow: 'auto' }}>
                              <Typography variant="subtitle2">Result Data:</Typography>
                              <pre style={{ 
                                backgroundColor: '#f5f5f5', 
                                padding: '8px', 
                                borderRadius: '4px', 
                                overflow: 'auto' 
                              }}>
                                {JSON.stringify(testResult.data, null, 2)}
                              </pre>
                            </Box>
                          )}
                        </Box>
                      )}
                    </Box>
                  </AccordionDetails>
                </Accordion>
              </Grid>
            </Grid>

            {/* Form Actions */}
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 4, gap: 2 }}>
              {onCancel && (
                <Button
                  variant="outlined"
                  onClick={onCancel}
                  disabled={saving}
                >
                  Cancel
                </Button>
              )}
              <Button
                type="submit"
                variant="contained"
                color="primary"
                startIcon={saving ? <CircularProgress size={20} /> : <Save />}
                disabled={saving || !formik.isValid || formik.isSubmitting}
              >
                {saving ? 'Saving...' : 'Save Action'}
              </Button>
            </Box>
          </Box>
        )}
      </Form>
    </Card>
  );
};

export default ActionConfigEditor;