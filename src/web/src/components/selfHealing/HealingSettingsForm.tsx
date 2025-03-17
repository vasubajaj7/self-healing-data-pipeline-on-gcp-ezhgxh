import React, { useState, useEffect } from 'react'; // react ^18.2.0
import {
  Box,
  Typography,
  Divider,
  Switch,
  FormControlLabel,
} from '@mui/material'; // @mui/material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11

import Form from '../common/Form';
import Input from '../common/Input';
import Select from '../common/Select';
import Button from '../common/Button';
import {
  HealingMode,
  HealingSettings,
} from '../../types/selfHealing';
import healingService from '../../services/api/healingService';
import { useApi } from '../../hooks/useApi';
import {
  validateRequired,
  validateMinValue,
  validateMaxValue,
} from '../../utils/validation';
import { useAlertContext } from '../../contexts/AlertContext';

/**
 * Interface defining the props for the HealingSettingsForm component
 */
interface HealingSettingsFormProps {
  /**
   * Callback function called when settings are successfully updated
   */
  onSettingsUpdated?: () => void;
}

/**
 * Enum-like array for healing mode options
 */
const healingModeOptions = [
  { value: HealingMode.AUTOMATIC, label: 'Automatic' },
  { value: HealingMode.SEMI_AUTOMATIC, label: 'Semi-Automatic' },
  { value: HealingMode.MANUAL, label: 'Manual' },
];

/**
 * Default settings for the healing configuration
 */
const defaultSettings: HealingSettings = {
  healingMode: HealingMode.SEMI_AUTOMATIC,
  globalConfidenceThreshold: 85,
  maxRetryAttempts: 3,
  approvalRequiredHighImpact: true,
  learningModeActive: true,
  additionalSettings: null,
  updatedAt: '',
  updatedBy: '',
};

/**
 * Validation schema for the healing settings form using yup
 */
const validationSchema = yup.object().shape({
  healingMode: yup.string().required('Healing Mode is required'),
  globalConfidenceThreshold: yup
    .number()
    .required('Confidence Threshold is required')
    .min(0, 'Confidence Threshold must be at least 0')
    .max(100, 'Confidence Threshold must be at most 100'),
  maxRetryAttempts: yup
    .number()
    .required('Max Retry Attempts is required')
    .min(0, 'Max Retry Attempts must be at least 0')
    .max(10, 'Max Retry Attempts must be at most 10'),
});

/**
 * Validates the healing settings form values
 * @param values The form values to validate
 * @returns An object containing validation errors for each field
 */
const validateSettings = (values: HealingSettings): Record<string, string | null> => {
  const errors: Record<string, string | null> = {};

  // Validate healingMode is required
  errors.healingMode = validateRequired(values.healingMode);

  // Validate globalConfidenceThreshold is required and between 0 and 100
  errors.globalConfidenceThreshold = validateRequired(String(values.globalConfidenceThreshold)) ||
    validateMinValue(values.globalConfidenceThreshold, 0) ||
    validateMaxValue(values.globalConfidenceThreshold, 100);

  // Validate maxRetryAttempts is required and between 0 and 10
  errors.maxRetryAttempts = validateRequired(String(values.maxRetryAttempts)) ||
    validateMinValue(values.maxRetryAttempts, 0) ||
    validateMaxValue(values.maxRetryAttempts, 10);

  return errors;
};

/**
 * Form component for configuring self-healing settings
 * @param props - Component properties including an optional callback for settings updates
 * @returns A React functional component for configuring self-healing settings
 */
const HealingSettingsForm: React.FC<HealingSettingsFormProps> = ({ onSettingsUpdated }) => {
  // Initialize state for settings using useState hook
  const [settings, setSettings] = useState<HealingSettings>(defaultSettings);

  // Initialize loading state using useApi hook
  const { loading, error, get, put } = useApi<HealingSettings>();

  // Get alert context for displaying notifications
  const { displaySuccess, displayError } = useAlertContext();

  /**
   * Fetches the current healing settings from the API when the component mounts
   */
  useEffect(() => {
    const fetchSettings = async () => {
      try {
        const response = await get<HealingSettings>(`/healing/settings`);
        if (response) {
          setSettings(response);
        }
      } catch (e: any) {
        displayError(`Failed to load healing settings: ${e.message}`);
      }
    };

    fetchSettings();
  }, [get, displayError]);

  /**
   * Handles form submission to update settings
   * @param values - The form values to submit
   */
  const handleSubmit = async (values: HealingSettings) => {
    try {
      // Update settings via API
      const updatedSettings = await put<HealingSettings>(`/healing/settings`, values);

      if (updatedSettings) {
        setSettings(updatedSettings);
        displaySuccess('Healing settings updated successfully!');
        if (onSettingsUpdated) {
          onSettingsUpdated();
        }
      }
    } catch (e: any) {
      displayError(`Failed to update healing settings: ${e.message}`);
    }
  };

  return (
    <Form
      initialValues={settings}
      validationSchema={validationSchema}
      onSubmit={handleSubmit}
      title="Self-Healing Configuration"
      subtitle="Configure the behavior of the self-healing system."
    >
      {({ values, handleChange, errors, touched, handleBlur }) => (
        <>
          {/* Healing Mode Selection */}
          <Select
            label="Healing Mode"
            name="healingMode"
            value={values.healingMode}
            onChange={handleChange}
            options={healingModeOptions}
            error={touched.healingMode && !!errors.healingMode}
            helperText={touched.healingMode && errors.healingMode}
            fullWidth
            required
          />

          {/* Confidence Threshold Input */}
          <Input
            label="Confidence Threshold (%)"
            name="globalConfidenceThreshold"
            type="number"
            value={values.globalConfidenceThreshold}
            onChange={handleChange}
            onBlur={handleBlur}
            error={touched.globalConfidenceThreshold && !!errors.globalConfidenceThreshold}
            helperText={touched.globalConfidenceThreshold && errors.globalConfidenceThreshold}
            fullWidth
            required
            endAdornment="%"
          />

          {/* Max Retry Attempts Input */}
          <Input
            label="Max Retry Attempts"
            name="maxRetryAttempts"
            type="number"
            value={values.maxRetryAttempts}
            onChange={handleChange}
            onBlur={handleBlur}
            error={touched.maxRetryAttempts && !!errors.maxRetryAttempts}
            helperText={touched.maxRetryAttempts && errors.maxRetryAttempts}
            fullWidth
            required
          />

          {/* Approval Required Toggle */}
          <FormControlLabel
            control={
              <Switch
                name="approvalRequiredHighImpact"
                checked={values.approvalRequiredHighImpact}
                onChange={handleChange}
                onBlur={handleBlur}
                color="primary"
              />
            }
            label="Approval Required for High Impact Fixes"
          />

          {/* Learning Mode Toggle */}
          <FormControlLabel
            control={
              <Switch
                name="learningModeActive"
                checked={values.learningModeActive}
                onChange={handleChange}
                onBlur={handleBlur}
                color="primary"
              />
            }
            label="Learning Mode Active"
          />

          {/* Save Button */}
          <Button type="submit" loading={loading}>
            Save Settings
          </Button>
        </>
      )}
    </Form>
  );
};

export default HealingSettingsForm;