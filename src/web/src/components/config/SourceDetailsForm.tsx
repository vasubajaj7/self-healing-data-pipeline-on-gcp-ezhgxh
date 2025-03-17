import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import Form, { FormikHelpers } from '../common/Form';
import Card from '../common/Card';
import Button from '../common/Button';
import SchemaMapping from './SchemaMapping';
import { SourceSystem, SourceSystemType } from '../../types/config';
import configService from '../../services/api/configService';
import useNotification from '../../hooks/useNotification';
import {
  Box,
  Grid,
  Typography,
  Divider,
  TextField,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
  FormHelperText,
  Switch,
  FormControlLabel,
  Tabs,
  Tab,
  styled,
} from '@mui/material'; // @mui/material ^5.11.0
import { Storage, Database, Api, Code, Check } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import * as yup from 'yup'; // yup ^0.32.11

/**
 * Interface for the props of the SourceDetailsForm component.
 * Defines the structure for passing data and callbacks to the form.
 */
interface SourceDetailsFormProps {
  sourceId: string | null;
  onSave: (source: SourceSystem) => void;
  onCancel: () => void;
}

/**
 * Interface for Google Cloud Storage connection details.
 * Defines the structure for GCS-specific connection parameters.
 */
interface GCSConnectionDetails {
  bucketName: string;
  path?: string;
  filePattern?: string;
  fileFormat: string;
}

/**
 * Interface for Cloud SQL connection details.
 * Defines the structure for Cloud SQL-specific connection parameters.
 */
interface CloudSQLConnectionDetails {
  instanceName: string;
  database: string;
  username: string;
  password: string;
  table?: string;
  query?: string;
}

/**
 * Interface for API connection details.
 * Defines the structure for API-specific connection parameters.
 */
interface APIConnectionDetails {
  endpoint: string;
  method: string;
  authType: string;
  apiKey?: string;
  username?: string;
  password?: string;
  oauthConfig?: object;
  headers?: Record<string, string>;
}

/**
 * Interface for Custom connection details.
 * Defines the structure for Custom-specific connection parameters.
 */
interface CustomConnectionDetails {
  connectionType: string;
  connectionString?: string;
  customConfig?: Record<string, any>;
}

/**
 * Interface for Extraction Settings
 * Defines the structure for data extraction settings
 */
interface ExtractionSettings {
  incrementalField?: string;
  batchSize?: number;
  extractionMethod?: string;
  customSettings?: Record<string, any>;
}

/**
 * Returns the appropriate validation schema based on source type
 * @param sourceType - The source type to get the validation schema for
 * @returns Yup validation schema for the selected source type
 */
function getValidationSchema(sourceType: string): yup.ObjectSchema<any> {
  // Create base validation schema for common fields
  const baseSchema = yup.object().shape({
    name: yup.string().required('Name is required').max(100, 'Name must be at most 100 characters'),
    sourceType: yup.string().required('Source type is required').oneOf(['GCS', 'CLOUD_SQL', 'API', 'CUSTOM'], 'Invalid source type'),
    description: yup.string().nullable().max(500, 'Description must be at most 500 characters'),
    isActive: yup.boolean(),
  });

  // Create specific validation schema for GCS source type
  const gcsSchema = yup.object().shape({
    connectionDetails: yup.object().shape({
      bucketName: yup.string().required('Bucket name is required'),
      path: yup.string().nullable(),
      filePattern: yup.string().nullable(),
      fileFormat: yup.string().required('File format is required').oneOf(['CSV', 'JSON', 'AVRO', 'PARQUET'], 'Invalid file format'),
    }),
  });

  // Create specific validation schema for Cloud SQL source type
  const cloudSQLSchema = yup.object().shape({
    connectionDetails: yup.object().shape({
      instanceName: yup.string().required('Instance name is required'),
      database: yup.string().required('Database name is required'),
      username: yup.string().required('Username is required'),
      password: yup.string().required('Password is required'),
      table: yup.string().nullable(),
      query: yup.string().nullable(),
    }),
  });

  // Create specific validation schema for API source type
  const apiSchema = yup.object().shape({
    connectionDetails: yup.object().shape({
      endpoint: yup.string().required('Endpoint is required').url('Must be a valid URL'),
      method: yup.string().required('HTTP method is required').oneOf(['GET', 'POST', 'PUT', 'DELETE'], 'Invalid HTTP method'),
      authType: yup.string().required('Authentication type is required').oneOf(['NONE', 'API_KEY', 'OAUTH', 'BASIC'], 'Invalid authentication type'),
      apiKey: yup.string().when('connectionDetails.authType', { is: 'API_KEY', then: yup.string().required('API key is required') }),
      username: yup.string().when('connectionDetails.authType', { is: 'BASIC', then: yup.string().required('Username is required') }),
      password: yup.string().when('connectionDetails.authType', { is: 'BASIC', then: yup.string().required('Password is required') }),
    }),
  });

  // Create specific validation schema for Custom source type
  const customSchema = yup.object().shape({
    connectionDetails: yup.object().shape({
      connectionType: yup.string().required('Connection type is required'),
      connectionString: yup.string().nullable(),
      customConfig: yup.string().nullable().test('is-json', 'Must be valid JSON', value => !value || isValidJson(value)),
    }),
  });

  // Create extraction settings validation schema
  const extractionSettingsSchema = yup.object().shape({
    extractionSettings: yup.object().shape({
      extractionMethod: yup.string().nullable().oneOf(['FULL', 'INCREMENTAL', 'CDC'], 'Invalid extraction method'),
      incrementalField: yup.string().when('extractionSettings.extractionMethod', { is: 'INCREMENTAL', then: yup.string().required('Incremental field is required for incremental extraction') }),
      batchSize: yup.number().nullable().positive('Batch size must be positive'),
    }),
  });

  // Return the appropriate schema based on sourceType parameter
  switch (sourceType) {
    case 'GCS':
      return baseSchema.concat(gcsSchema).concat(extractionSettingsSchema);
    case 'CLOUD_SQL':
      return baseSchema.concat(cloudSQLSchema).concat(extractionSettingsSchema);
    case 'API':
      return baseSchema.concat(apiSchema).concat(extractionSettingsSchema);
    case 'CUSTOM':
      return baseSchema.concat(customSchema).concat(extractionSettingsSchema);
    default:
      return baseSchema;
  }
}

/**
 * Helper function to validate if a string is a valid JSON
 * @param str - The string to validate
 * @returns True if the string is a valid JSON, false otherwise
 */
function isValidJson(str: string): boolean {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}

/**
 * Functional component that renders an icon based on the source type
 */
const SourceTypeIcon: React.FC<{ type: SourceSystemType }> = ({ type }) => {
  switch (type) {
    case 'GCS':
      return <Storage />;
    case 'CLOUD_SQL':
      return <Database />;
    case 'API':
      return <Api />;
    case 'CUSTOM':
      return <Code />;
    default:
      return <Check />;
  }
};

/**
 * Props for the SourceDetailsForm component
 */
interface SourceDetailsFormProps {
  sourceId: string | null;
  onSave: (source: SourceSystem) => void;
  onCancel: () => void;
}

/**
 * Main form component for creating and editing data sources
 */
const SourceDetailsForm: React.FC<SourceDetailsFormProps> = ({ sourceId, onSave, onCancel }) => {
  // Define state variables
  const [source, setSource] = useState<SourceSystem | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [testing, setTesting] = useState<boolean>(false);
  const [activeTab, setActiveTab] = useState<number>(0);
  const [sourceType, setSourceType] = useState<SourceSystemType>('GCS');

  const { showSuccess, showError } = useNotification();

  /**
   * Fetch details of an existing source
   */
  const fetchSourceDetails = useCallback(async () => {
    if (!sourceId) return;
    setLoading(true);
    try {
      const response = await configService.getDataSource(sourceId);
      setSource(response.data);
      setSourceType(response.data.sourceType as SourceSystemType);
    } catch (error: any) {
      showError(`Failed to fetch source details: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [sourceId, configService.getDataSource, showError]);

  /**
   * Handle form submission
   */
  const handleSubmit = async (values: any, helpers: FormikHelpers<any>) => {
    setSaving(true);
    try {
      const sourceData: SourceSystem = {
        ...values,
        sourceId: sourceId || 'new',
        sourceType: values.sourceType,
        connectionDetails: values.connectionDetails,
        extractionSettings: values.extractionSettings,
        isActive: values.isActive !== false,
        createdAt: source?.createdAt || new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        name: values.name,
      };

      if (sourceId) {
        await configService.updateDataSource(sourceId, sourceData);
        showSuccess('Source updated successfully!');
      } else {
        await configService.createDataSource(sourceData);
        showSuccess('Source created successfully!');
      }
      onSave(sourceData);
    } catch (error: any) {
      showError(`Failed to save source: ${error.message}`);
      helpers.setStatus({ error: error.message });
    } finally {
      setSaving(false);
    }
  };

  /**
   * Test the connection to the source
   */
  const handleTestConnection = async (values: any) => {
    setTesting(true);
    try {
      const sourceData: SourceSystem = {
        ...values,
        sourceId: sourceId || 'new',
        sourceType: values.sourceType,
        connectionDetails: values.connectionDetails,
        extractionSettings: values.extractionSettings,
        isActive: values.isActive !== false,
        createdAt: source?.createdAt || new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        name: values.name,
      };
      const response = await configService.testDataSourceConnection(sourceData.sourceId);
      if (response.data.success) {
        showSuccess('Connection successful!');
      } else {
        showError(`Connection failed: ${response.data.message}`);
      }
    } catch (error: any) {
      showError(`Connection test failed: ${error.message}`);
    } finally {
      setTesting(false);
    }
  };

  /**
   * Handle tab change
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  useEffect(() => {
    if (sourceId) {
      fetchSourceDetails();
    }
  }, [sourceId, fetchSourceDetails]);

  const initialValues = useMemo(() => ({
    name: source?.name || '',
    sourceType: source?.sourceType || 'GCS',
    description: source?.description || '',
    isActive: source?.isActive !== false,
    connectionDetails: source?.connectionDetails || {},
    extractionSettings: source?.extractionSettings || {},
  }), [source]);

  const validationSchema = useMemo(() => getValidationSchema(sourceType), [sourceType]);

  return (
    <Form
      initialValues={initialValues}
      validationSchema={validationSchema}
      onSubmit={handleSubmit}
      title={sourceId ? 'Edit Source' : 'Create Source'}
      subtitle="Define the details for your data source"
    >
      {({ values, handleChange, handleBlur, touched, errors }) => (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Card title="Basic Information">
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Source Name"
                    name="name"
                    value={values.name}
                    onChange={handleChange}
                    onBlur={handleBlur}
                    error={touched.name && !!errors.name}
                    helperText={touched.name && errors.name}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth required>
                    <InputLabel id="sourceType-label">Source Type</InputLabel>
                    <Select
                      labelId="sourceType-label"
                      id="sourceType"
                      name="sourceType"
                      value={values.sourceType}
                      label="Source Type"
                      onChange={(e) => {
                        handleChange(e);
                        setSourceType(e.target.value as SourceSystemType);
                      }}
                      onBlur={handleBlur}
                      error={touched.sourceType && !!errors.sourceType}
                    >
                      <MenuItem value="GCS">Google Cloud Storage</MenuItem>
                      <MenuItem value="CLOUD_SQL">Cloud SQL</MenuItem>
                      <MenuItem value="API">API</MenuItem>
                      <MenuItem value="CUSTOM">Custom</MenuItem>
                    </Select>
                    {touched.sourceType && errors.sourceType && (
                      <FormHelperText error>{errors.sourceType}</FormHelperText>
                    )}
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <TextField
                    fullWidth
                    label="Description"
                    name="description"
                    value={values.description}
                    onChange={handleChange}
                    onBlur={handleBlur}
                    error={touched.description && !!errors.description}
                    helperText={touched.description && errors.description}
                    multiline
                    rows={3}
                  />
                </Grid>
                <Grid item xs={12}>
                  <FormControlLabel
                    control={<Switch name="isActive" checked={values.isActive} onChange={handleChange} onBlur={handleBlur} />}
                    label="Active"
                  />
                </Grid>
              </Grid>
            </Card>
          </Grid>

          <Grid item xs={12}>
            <Card title="Connection Details">
              {sourceType === 'GCS' && (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Bucket Name"
                      name="connectionDetails.bucketName"
                      value={(values.connectionDetails as GCSConnectionDetails)?.bucketName || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.bucketName && !!errors.connectionDetails?.bucketName}
                      helperText={touched.connectionDetails?.bucketName && errors.connectionDetails?.bucketName}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Path"
                      name="connectionDetails.path"
                      value={(values.connectionDetails as GCSConnectionDetails)?.path || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="File Pattern"
                      name="connectionDetails.filePattern"
                      value={(values.connectionDetails as GCSConnectionDetails)?.filePattern || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControl fullWidth required>
                      <InputLabel id="fileFormat-label">File Format</InputLabel>
                      <Select
                        labelId="fileFormat-label"
                        id="connectionDetails.fileFormat"
                        name="connectionDetails.fileFormat"
                        value={(values.connectionDetails as GCSConnectionDetails)?.fileFormat || ''}
                        label="File Format"
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.connectionDetails?.fileFormat && !!errors.connectionDetails?.fileFormat}
                      >
                        <MenuItem value="CSV">CSV</MenuItem>
                        <MenuItem value="JSON">JSON</MenuItem>
                        <MenuItem value="AVRO">Avro</MenuItem>
                        <MenuItem value="PARQUET">Parquet</MenuItem>
                      </Select>
                      {touched.connectionDetails?.fileFormat && errors.connectionDetails?.fileFormat && (
                        <FormHelperText error>{errors.connectionDetails?.fileFormat}</FormHelperText>
                      )}
                    </FormControl>
                  </Grid>
                </Grid>
              )}
              {sourceType === 'CLOUD_SQL' && (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Instance Name"
                      name="connectionDetails.instanceName"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.instanceName || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.instanceName && !!errors.connectionDetails?.instanceName}
                      helperText={touched.connectionDetails?.instanceName && errors.connectionDetails?.instanceName}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Database"
                      name="connectionDetails.database"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.database || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.database && !!errors.connectionDetails?.database}
                      helperText={touched.connectionDetails?.database && errors.connectionDetails?.database}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Username"
                      name="connectionDetails.username"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.username || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.username && !!errors.connectionDetails?.username}
                      helperText={touched.connectionDetails?.username && errors.connectionDetails?.username}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Password"
                      name="connectionDetails.password"
                      type="password"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.password || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.password && !!errors.connectionDetails?.password}
                      helperText={touched.connectionDetails?.password && errors.connectionDetails?.password}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Table"
                      name="connectionDetails.table"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.table || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Custom Query"
                      name="connectionDetails.query"
                      value={(values.connectionDetails as CloudSQLConnectionDetails)?.query || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      multiline
                      rows={3}
                    />
                  </Grid>
                </Grid>
              )}
              {sourceType === 'API' && (
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="API Endpoint"
                      name="connectionDetails.endpoint"
                      value={(values.connectionDetails as APIConnectionDetails)?.endpoint || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.endpoint && !!errors.connectionDetails?.endpoint}
                      helperText={touched.connectionDetails?.endpoint && errors.connectionDetails?.endpoint}
                      required
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControl fullWidth required>
                      <InputLabel id="method-label">HTTP Method</InputLabel>
                      <Select
                        labelId="method-label"
                        id="connectionDetails.method"
                        name="connectionDetails.method"
                        value={(values.connectionDetails as APIConnectionDetails)?.method || ''}
                        label="HTTP Method"
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.connectionDetails?.method && !!errors.connectionDetails?.method}
                      >
                        <MenuItem value="GET">GET</MenuItem>
                        <MenuItem value="POST">POST</MenuItem>
                        <MenuItem value="PUT">PUT</MenuItem>
                        <MenuItem value="DELETE">DELETE</MenuItem>
                      </Select>
                      {touched.connectionDetails?.method && errors.connectionDetails?.method && (
                        <FormHelperText error>{errors.connectionDetails?.method}</FormHelperText>
                      )}
                    </FormControl>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <FormControl fullWidth required>
                      <InputLabel id="authType-label">Authentication Type</InputLabel>
                      <Select
                        labelId="authType-label"
                        id="connectionDetails.authType"
                        name="connectionDetails.authType"
                        value={(values.connectionDetails as APIConnectionDetails)?.authType || ''}
                        label="Authentication Type"
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.connectionDetails?.authType && !!errors.connectionDetails?.authType}
                      >
                        <MenuItem value="NONE">None</MenuItem>
                        <MenuItem value="API_KEY">API Key</MenuItem>
                        <MenuItem value="OAUTH">OAuth 2.0</MenuItem>
                        <MenuItem value="BASIC">Basic</MenuItem>
                      </Select>
                      {touched.connectionDetails?.authType && errors.connectionDetails?.authType && (
                        <FormHelperText error>{errors.connectionDetails?.authType}</FormHelperText>
                      )}
                    </FormControl>
                  </Grid>
                  {values.connectionDetails.authType === 'API_KEY' && (
                    <Grid item xs={12}>
                      <TextField
                        fullWidth
                        label="API Key"
                        name="connectionDetails.apiKey"
                        type="password"
                        value={(values.connectionDetails as APIConnectionDetails)?.apiKey || ''}
                        onChange={handleChange}
                        onBlur={handleBlur}
                        error={touched.connectionDetails?.apiKey && !!errors.connectionDetails?.apiKey}
                        helperText={touched.connectionDetails?.apiKey && errors.connectionDetails?.apiKey}
                        required
                      />
                    </Grid>
                  )}
                  {values.connectionDetails.authType === 'BASIC' && (
                    <>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Username"
                          name="connectionDetails.username"
                          value={(values.connectionDetails as APIConnectionDetails)?.username || ''}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          error={touched.connectionDetails?.username && !!errors.connectionDetails?.username}
                          helperText={touched.connectionDetails?.username && errors.connectionDetails?.username}
                          required
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Password"
                          name="connectionDetails.password"
                          type="password"
                          value={(values.connectionDetails as APIConnectionDetails)?.password || ''}
                          onChange={handleChange}
                          onBlur={handleBlur}
                          error={touched.connectionDetails?.password && !!errors.connectionDetails?.password}
                          helperText={touched.connectionDetails?.password && errors.connectionDetails?.password}
                          required
                        />
                      </Grid>
                    </>
                  )}
                </Grid>
              )}
              {sourceType === 'CUSTOM' && (
                <Grid container spacing={2}>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Connection Type"
                      name="connectionDetails.connectionType"
                      value={(values.connectionDetails as CustomConnectionDetails)?.connectionType || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.connectionDetails?.connectionType && !!errors.connectionDetails?.connectionType}
                      helperText={touched.connectionDetails?.connectionType && errors.connectionDetails?.connectionType}
                      required
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Connection String"
                      name="connectionDetails.connectionString"
                      value={(values.connectionDetails as CustomConnectionDetails)?.connectionString || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      multiline
                      rows={3}
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Custom Configuration (JSON)"
                      name="connectionDetails.customConfig"
                      value={(values.connectionDetails as CustomConnectionDetails)?.customConfig || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      multiline
                      rows={3}
                      helperText="Enter custom configuration parameters as a JSON object"
                      error={touched.connectionDetails?.customConfig && !!errors.connectionDetails?.customConfig}
                    />
                  </Grid>
                </Grid>
              )}
            </Card>
          </Grid>

          <Grid item xs={12}>
            <Card title="Extraction Settings">
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel id="extractionMethod-label">Extraction Method</InputLabel>
                    <Select
                      labelId="extractionMethod-label"
                      id="extractionSettings.extractionMethod"
                      name="extractionSettings.extractionMethod"
                      value={(values.extractionSettings as ExtractionSettings)?.extractionMethod || ''}
                      label="Extraction Method"
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.extractionSettings?.extractionMethod && !!errors.extractionSettings?.extractionMethod}
                    >
                      <MenuItem value="FULL">Full</MenuItem>
                      <MenuItem value="INCREMENTAL">Incremental</MenuItem>
                      <MenuItem value="CDC">CDC</MenuItem>
                    </Select>
                    {touched.extractionSettings?.extractionMethod && errors.extractionSettings?.extractionMethod && (
                      <FormHelperText error>{errors.extractionSettings?.extractionMethod}</FormHelperText>
                    )}
                  </FormControl>
                </Grid>
                {values.extractionSettings?.extractionMethod === 'INCREMENTAL' && (
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Incremental Field"
                      name="extractionSettings.incrementalField"
                      value={(values.extractionSettings as ExtractionSettings)?.incrementalField || ''}
                      onChange={handleChange}
                      onBlur={handleBlur}
                      error={touched.extractionSettings?.incrementalField && !!errors.extractionSettings?.incrementalField}
                      helperText={touched.extractionSettings?.incrementalField && errors.extractionSettings?.incrementalField}
                      required
                    />
                  </Grid>
                )}
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Batch Size"
                    name="extractionSettings.batchSize"
                    type="number"
                    value={(values.extractionSettings as ExtractionSettings)?.batchSize || ''}
                    onChange={handleChange}
                    onBlur={handleBlur}
                    error={touched.extractionSettings?.batchSize && !!errors.extractionSettings?.batchSize}
                    helperText="Number of records to extract in each batch"
                  />
                </Grid>
              </Grid>
            </Card>
          </Grid>

          <Grid item xs={12} sx={{ mt: 3, textAlign: 'right' }}>
            <Button onClick={onCancel}>Cancel</Button>
            <Button
              variant="contained"
              color="primary"
              type="submit"
              disabled={saving}
              loading={saving}
              sx={{ ml: 2 }}
              onClick={() => handleTestConnection(values)}
            >
              Test Connection
            </Button>
            <Button
              variant="contained"
              color="primary"
              type="submit"
              disabled={saving}
              loading={saving}
              sx={{ ml: 2 }}
            >
              {sourceId ? 'Update' : 'Create'}
            </Button>
          </Grid>
        </Grid>
      )}
    </Form>
  );
};

export default SourceDetailsForm;