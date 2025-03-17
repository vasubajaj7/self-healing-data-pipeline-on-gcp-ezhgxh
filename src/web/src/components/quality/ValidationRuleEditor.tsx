import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Divider,
  Paper,
  CircularProgress,
} from '@mui/material'; // @mui/material ^5.11.0
import Save from '@mui/icons-material/Save'; // @mui/icons-material ^5.11.0
import Cancel from '@mui/icons-material/Cancel'; // @mui/icons-material ^5.11.0
import Add from '@mui/icons-material/Add'; // @mui/icons-material ^5.11.0
import Edit from '@mui/icons-material/Edit'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11

import Form from '../common/Form';
import Select from '../common/Select';
import Input from '../common/Input';
import Button from '../common/Button';
import { useApi } from '../../hooks/useApi';
import { useNotification } from '../../hooks/useNotification';
import qualityService from '../../services/api/qualityService';
import {
  QualityRule,
  QualityRuleTemplate,
} from '../../types/api';
import {
  QualityRuleType,
  QualityDimension,
} from '../../types/quality';
import { AlertSeverity } from '../../types/global';

/**
 * Interface defining the props for the ValidationRuleEditor component
 */
interface ValidationRuleEditorProps {
  rule?: QualityRule | null;
  dataset?: string;
  table?: string;
  onSave?: (rule: QualityRule) => void;
  onCancel?: () => void;
  className?: string;
}

/**
 * Interface defining the form values for the rule editor
 */
interface RuleFormValues {
  ruleName: string;
  targetDataset: string;
  targetTable: string;
  ruleType: string;
  expectationType: string;
  severity: string;
  description?: string;
  isActive: boolean;
  ruleDefinition: object;
}

/**
 * Functional component for rendering fields specific to schema validation rules
 */
const SchemaValidationFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Column"
        value={values.ruleDefinition?.column as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.column', value)}
        required
      />
      <Select
        label="Expected Type"
        options={[
          { value: 'string', label: 'String' },
          { value: 'number', label: 'Number' },
          { value: 'boolean', label: 'Boolean' },
          { value: 'date', label: 'Date' },
        ]}
        value={values.ruleDefinition?.expectedType as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.expectedType', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to null check validation rules
 */
const NullCheckFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Column"
        value={values.ruleDefinition?.column as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.column', value)}
        required
      />
      <Input
        label="Threshold (%)"
        type="number"
        value={values.ruleDefinition?.threshold as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.threshold', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to value range validation rules
 */
const ValueRangeFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Column"
        value={values.ruleDefinition?.column as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.column', value)}
        required
      />
      <Input
        label="Minimum Value"
        type="number"
        value={values.ruleDefinition?.min as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.min', value)}
        required
      />
      <Input
        label="Maximum Value"
        type="number"
        value={values.ruleDefinition?.max as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.max', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to referential integrity validation rules
 */
const ReferentialFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Source Column"
        value={values.ruleDefinition?.sourceColumn as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.sourceColumn', value)}
        required
      />
      <Input
        label="Reference Dataset"
        value={values.ruleDefinition?.referenceDataset as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.referenceDataset', value)}
        required
      />
      <Input
        label="Reference Table"
        value={values.ruleDefinition?.referenceTable as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.referenceTable', value)}
        required
      />
      <Input
        label="Reference Column"
        value={values.ruleDefinition?.referenceColumn as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.referenceColumn', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to pattern matching validation rules
 */
const PatternMatchFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Column"
        value={values.ruleDefinition?.column as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.column', value)}
        required
      />
      <Input
        label="Pattern"
        value={values.ruleDefinition?.pattern as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.pattern', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to statistical validation rules
 */
const StatisticalFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="Column"
        value={values.ruleDefinition?.column as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.column', value)}
        required
      />
      <Input
        label="Method"
        value={values.ruleDefinition?.method as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.method', value)}
        required
      />
      <Input
        label="Threshold"
        type="number"
        value={values.ruleDefinition?.threshold as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.threshold', value)}
        required
      />
    </>
  );
};

/**
 * Functional component for rendering fields specific to custom validation rules
 */
const CustomRuleFields: React.FC<{
  values: RuleFormValues;
  setFieldValue: (field: string, value: any) => void;
}> = ({ values, setFieldValue }) => {
  return (
    <>
      <Input
        label="SQL Expression"
        value={values.ruleDefinition?.sqlExpression as string || ''}
        onChange={(value) => setFieldValue('ruleDefinition.sqlExpression', value)}
        required
      />
    </>
  );
};

/**
 * Converts QualityRuleType enum to select options
 */
const getRuleTypeOptions = () => {
  return Object.values(QualityRuleType).map((ruleType) => ({
    value: ruleType,
    label: formatRuleType(ruleType),
  }));
};

/**
 * Converts QualityDimension enum to select options
 */
const getDimensionOptions = () => {
  return Object.values(QualityDimension).map((dimension) => ({
    value: dimension,
    label: dimension,
  }));
};

/**
 * Converts AlertSeverity enum to select options
 */
const getSeverityOptions = () => {
  return Object.values(AlertSeverity).map((severity) => ({
    value: severity,
    label: severity,
  }));
};

/**
 * Formats rule type for display
 */
const formatRuleType = (ruleType: string) => {
  const formatted = ruleType.replace(/_/g, ' ');
  return formatted.charAt(0).toUpperCase() + formatted.slice(1).toLowerCase();
};

/**
 * Gets initial rule definition based on rule type
 */
const getInitialRuleDefinition = (ruleType: QualityRuleType) => {
  switch (ruleType) {
    case QualityRuleType.SCHEMA:
      return { column: '', expectedType: '' };
    case QualityRuleType.NULL_CHECK:
      return { column: '', threshold: 0 };
    case QualityRuleType.VALUE_RANGE:
      return { column: '', min: 0, max: 100 };
    default:
      return {};
  }
};

/**
 * Creates validation schema based on rule type
 */
const getValidationSchema = (ruleType: QualityRuleType) => {
  const baseSchema = yup.object().shape({
    ruleName: yup.string().required('Rule name is required'),
    targetDataset: yup.string().required('Dataset is required'),
    targetTable: yup.string().required('Table is required'),
    ruleType: yup.string().required('Rule type is required'),
    expectationType: yup.string().required('Expectation type is required'),
    severity: yup.string().required('Severity is required'),
    description: yup.string(),
    isActive: yup.boolean(),
  });

  switch (ruleType) {
    case QualityRuleType.SCHEMA:
      return baseSchema.shape({
        ruleDefinition: yup.object().shape({
          column: yup.string().required('Column is required'),
          expectedType: yup.string().required('Expected type is required'),
        }),
      });
    case QualityRuleType.NULL_CHECK:
      return baseSchema.shape({
        ruleDefinition: yup.object().shape({
          column: yup.string().required('Column is required'),
          threshold: yup.number().min(0).max(100).required('Threshold is required'),
        }),
      });
    case QualityRuleType.VALUE_RANGE:
      return baseSchema.shape({
        ruleDefinition: yup.object().shape({
          column: yup.string().required('Column is required'),
          min: yup.number().required('Minimum value is required'),
          max: yup.number().required('Maximum value is required'),
        }),
      });
    default:
      return baseSchema;
  }
};

/**
 * Form component for creating and editing data quality validation rules
 */
const ValidationRuleEditor: React.FC<ValidationRuleEditorProps> = ({
  rule,
  dataset,
  table,
  onSave,
  onCancel,
  className,
}) => {
  // State for rule templates, selected rule type, and loading states
  const [ruleTemplates, setRuleTemplates] = useState<QualityRuleTemplate[]>([]);
  const [selectedRuleType, setSelectedRuleType] = useState<QualityRuleType>(
    rule ? (rule.ruleType as QualityRuleType) : QualityRuleType.SCHEMA
  );
  const [loadingTemplates, setLoadingTemplates] = useState(false);

  // API hooks for fetching templates and saving rules
  const { executeRequest: fetchTemplates } = useApiRequest<QualityRuleTemplate[]>();
  const { executeRequest: createRuleRequest } = useApiRequest<QualityRule>();
  const { executeRequest: updateRuleRequest } = useApiRequest<QualityRule>();

  // Notification hook for success/error messages
  const { showSuccess, showError } = useNotification();

  // Define initial form values based on existing rule or defaults
  const initialValues: RuleFormValues = useMemo(() => ({
    ruleName: rule?.ruleName || '',
    targetDataset: rule?.targetDataset || dataset || '',
    targetTable: rule?.targetTable || table || '',
    ruleType: rule?.ruleType || QualityRuleType.SCHEMA,
    expectationType: rule?.expectationType || '',
    severity: rule?.severity || AlertSeverity.MEDIUM,
    description: rule?.description || '',
    isActive: rule?.isActive !== undefined ? rule.isActive : true,
    ruleDefinition: rule?.ruleDefinition || getInitialRuleDefinition(selectedRuleType),
  }), [rule, dataset, table, selectedRuleType]);

  // Define validation schema based on rule type
  const validationSchema = useMemo(() => getValidationSchema(selectedRuleType), [selectedRuleType]);

  // Define form submission handler to create or update rule
  const handleSubmit = useCallback(
    async (values: RuleFormValues) => {
      try {
        const ruleData: QualityRule = {
          ruleId: rule?.ruleId || '',
          ruleName: values.ruleName,
          targetDataset: values.targetDataset,
          targetTable: values.targetTable,
          ruleType: values.ruleType,
          expectationType: values.expectationType,
          severity: values.severity as AlertSeverity,
          description: values.description,
          isActive: values.isActive,
          ruleDefinition: values.ruleDefinition,
          createdAt: rule?.createdAt || '',
          updatedAt: rule?.updatedAt || '',
        };

        if (rule) {
          // Update existing rule
          await updateRuleRequest(qualityService.updateQualityRule, rule.ruleId, ruleData);
          showSuccess('Rule updated successfully');
        } else {
          // Create new rule
          await createRuleRequest(qualityService.createQualityRule, ruleData);
          showSuccess('Rule created successfully');
        }

        onSave?.(ruleData);
      } catch (error: any) {
        showError(error.message || 'Failed to save rule');
      }
    },
    [createRuleRequest, updateRuleRequest, showSuccess, showError, onSave, rule]
  );

  // Fetch rule templates on component mount
  useEffect(() => {
    const fetchRuleTemplatesData = async () => {
      setLoadingTemplates(true);
      try {
        const templates = await fetchTemplates(qualityService.getQualityRuleTemplates, {
          ruleType: selectedRuleType,
        });
        setRuleTemplates(templates || []);
      } catch (error: any) {
        showError(error.message || 'Failed to load rule templates');
      } finally {
        setLoadingTemplates(false);
      }
    };

    fetchRuleTemplatesData();
  }, [fetchTemplates, selectedRuleType, showError]);

  return (
    <Form
      initialValues={initialValues}
      validationSchema={validationSchema}
      onSubmit={handleSubmit}
      title={rule ? 'Edit Validation Rule' : 'Create Validation Rule'}
      className={className}
    >
      {({ values, setFieldValue }) => (
        <Grid container spacing={2}>
          <Grid item xs={12} md={6}>
            <Input label="Rule Name" value={values.ruleName} onChange={(value) => setFieldValue('ruleName', value)} required />
          </Grid>
          <Grid item xs={12} md={6}>
            <Input label="Target Dataset" value={values.targetDataset} onChange={(value) => setFieldValue('targetDataset', value)} required />
          </Grid>
          <Grid item xs={12} md={6}>
            <Input label="Target Table" value={values.targetTable} onChange={(value) => setFieldValue('targetTable', value)} required />
          </Grid>
          <Grid item xs={12} md={6}>
            <Select
              label="Rule Type"
              options={getRuleTypeOptions()}
              value={values.ruleType}
              onChange={(value) => {
                setFieldValue('ruleType', value);
                setSelectedRuleType(value as QualityRuleType);
                setFieldValue('ruleDefinition', getInitialRuleDefinition(value as QualityRuleType));
              }}
              required
            />
          </Grid>

          {/* Render rule-specific parameter fields based on selected rule type */}
          {values.ruleType === QualityRuleType.SCHEMA && (
            <SchemaValidationFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.NULL_CHECK && (
            <NullCheckFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.VALUE_RANGE && (
            <ValueRangeFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.REFERENTIAL && (
            <ReferentialFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.PATTERN_MATCH && (
            <PatternMatchFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.STATISTICAL && (
            <StatisticalFields values={values} setFieldValue={setFieldValue} />
          )}
          {values.ruleType === QualityRuleType.CUSTOM && (
            <CustomRuleFields values={values} setFieldValue={setFieldValue} />
          )}

          <Grid item xs={12} md={6}>
            <Select
              label="Severity"
              options={getSeverityOptions()}
              value={values.severity}
              onChange={(value) => setFieldValue('severity', value)}
              required
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <Select
              label="Is Active"
              options={[
                { value: true, label: 'Yes' },
                { value: false, label: 'No' },
              ]}
              value={values.isActive}
              onChange={(value) => setFieldValue('isActive', value)}
              required
            />
          </Grid>
          <Grid item xs={12}>
            <Input label="Description" value={values.description || ''} onChange={(value) => setFieldValue('description', value)} />
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end" gap={2}>
              <Button variant="outlined" color="primary" startIcon={<Cancel />} onClick={onCancel}>
                Cancel
              </Button>
              <Button type="submit" variant="contained" color="primary" startIcon={<Save />}>
                Save
              </Button>
            </Box>
          </Grid>
        </Grid>
      )}
    </Form>
  );
};

export default ValidationRuleEditor;