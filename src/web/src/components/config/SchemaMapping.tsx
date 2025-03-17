import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import Table from '../common/Table';
import Card from '../common/Card';
import Button from '../common/Button';
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
  IconButton,
  Tooltip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Delete,
  Edit,
  Refresh,
  ArrowForward,
  SwapHoriz,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import {
  DragDropContext,
  Droppable,
  Draggable,
  DropResult,
} from 'react-beautiful-dnd'; // react-beautiful-dnd ^13.1.1
import { SourceSystem } from '../../types/config';
import configService from '../../services/api/configService';
import useNotification from '../../hooks/useNotification';

/**
 * Interface for a schema field
 */
interface SchemaField {
  name: string;
  type: string;
  description?: string;
  nullable?: boolean;
  mode?: string;
  defaultValue?: any;
}

/**
 * Interface for a field mapping between source and target
 */
interface FieldMapping {
  id: string;
  sourceField: string;
  sourceType: string;
  targetField: string;
  targetType: string;
  transformation?: string;
  description?: string;
  mode?: string;
  defaultValue?: any;
}

/**
 * Interface for the complete schema mapping
 */
interface SchemaMapping {
  sourceId: string;
  targetDataset: string;
  targetTable: string;
  mappings: FieldMapping[];
  createdAt?: string;
  updatedAt?: string;
}

/**
 * Props for the SchemaMapping component
 */
interface SchemaMappingProps {
  sourceId: string;
  targetDataset: string;
  targetTable: string;
  existingMapping?: SchemaMapping | null;
  onSave?: (mapping: SchemaMapping) => void;
  readOnly?: boolean;
}

/**
 * Columns for the source fields table
 */
const sourceFieldsColumns = {
  columns: [
    { id: 'name', label: 'Field Name', width: '40%' },
    { id: 'type', label: 'Data Type', width: '30%' },
    { id: 'nullable', label: 'Nullable', width: '15%', format: (value: any) => value ? 'Yes' : 'No' },
    {
      id: 'actions',
      label: 'Actions',
      width: '15%',
      renderCell: (_: any, row: any) => <Button onClick={() => handleQuickMap(row)}>Map</Button>
    }
  ]
};

/**
 * Columns for the mappings table
 */
const mappingsColumns = {
  columns: [
    { id: 'sourceField', label: 'Source Field', width: '20%' },
    { id: 'sourceType', label: 'Source Type', width: '15%' },
    { id: 'targetField', label: 'Target Field', width: '20%' },
    { id: 'targetType', label: 'Target Type', width: '15%' },
    { id: 'transformation', label: 'Transformation', width: '15%' },
    {
      id: 'actions',
      label: 'Actions',
      width: '15%',
      renderCell: (_: any, row: any) => (
        <Box>
          <IconButton onClick={() => handleEditMapping(row)}>
            <Edit />
          </IconButton>
          <IconButton onClick={() => handleDeleteMapping(row.id)}>
            <Delete />
          </IconButton>
        </Box>
      )
    }
  ]
};

/**
 * Dialog for creating or editing a field mapping
 */
const FieldMappingDialog: React.FC<{
  open: boolean;
  onClose: () => void;
  onSave: (mapping: FieldMapping) => void;
  mapping: FieldMapping | null;
  sourceFields: SchemaField[];
  existingTargetFields: string[];
}> = ({ open, onClose, onSave, mapping, sourceFields, existingTargetFields }) => {
  const [sourceField, setSourceField] = useState(mapping?.sourceField || '');
  const [targetField, setTargetField] = useState(mapping?.targetField || '');
  const [targetType, setTargetType] = useState(mapping?.targetType || 'STRING');
  const [transformation, setTransformation] = useState(mapping?.transformation || '');
  const [description, setDescription] = useState(mapping?.description || '');
  const [mode, setMode] = useState(mapping?.mode || 'NULLABLE');
  const [defaultValue, setDefaultValue] = useState(mapping?.defaultValue || null);

  const availableSourceFields = useMemo(() => {
    return sourceFields.filter(field => !existingTargetFields.includes(field.name) || field.name === sourceField);
  }, [sourceFields, existingTargetFields, sourceField]);

  const handleSubmit = () => {
    if (!sourceField || !targetField || !targetType) {
      // TODO: Implement proper form validation
      alert('Please fill in all required fields.');
      return;
    }

    const newMapping = {
      id: mapping?.id || Math.random().toString(36).substring(7),
      sourceField,
      sourceType: sourceFields.find(f => f.name === sourceField)?.type || 'STRING',
      targetField,
      targetType,
      transformation,
      description,
      mode,
      defaultValue
    };
    onSave(newMapping);
    onClose();
  };

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{mapping ? 'Edit Mapping' : 'Add Mapping'}</DialogTitle>
      <DialogContent>
        <Grid container spacing={2} mt={1}>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth required>
              <InputLabel id="source-field-label">Source Field</InputLabel>
              <Select
                labelId="source-field-label"
                value={sourceField}
                label="Source Field"
                onChange={(e) => setSourceField(e.target.value)}
              >
                {availableSourceFields.map((field) => (
                  <MenuItem key={field.name} value={field.name}>
                    {field.name} ({field.type})
                  </MenuItem>
                ))}
              </Select>
              <FormHelperText>Select the source field to map</FormHelperText>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Target Field"
              value={targetField}
              onChange={(e) => setTargetField(e.target.value)}
              required
              helperText="Enter the target field name"
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth required>
              <InputLabel id="target-type-label">Target Type</InputLabel>
              <Select
                labelId="target-type-label"
                value={targetType}
                label="Target Type"
                onChange={(e) => setTargetType(e.target.value)}
              >
                <MenuItem value="STRING">STRING</MenuItem>
                <MenuItem value="INT64">INT64</MenuItem>
                <MenuItem value="FLOAT64">FLOAT64</MenuItem>
                <MenuItem value="BOOLEAN">BOOLEAN</MenuItem>
                <MenuItem value="DATE">DATE</MenuItem>
                <MenuItem value="TIMESTAMP">TIMESTAMP</MenuItem>
              </Select>
              <FormHelperText>Select the target data type</FormHelperText>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Transformation"
              value={transformation}
              onChange={(e) => setTransformation(e.target.value)}
              helperText="Enter transformation expression (optional)"
            />
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              helperText="Enter a description for this mapping (optional)"
            />
          </Grid>
        </Grid>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSubmit}>Save</Button>
      </DialogActions>
    </Dialog>
  );
};

/**
 * Main component for schema mapping configuration
 */
const SchemaMapping: React.FC<SchemaMappingProps> = ({
  sourceId,
  targetDataset,
  targetTable,
  existingMapping,
  onSave,
  readOnly = false,
}) => {
  // Initialize state for source fields, mappings, and UI state
  const [sourceFields, setSourceFields] = useState<SchemaField[]>([]);
  const [mappings, setMappings] = useState<FieldMapping[]>(existingMapping?.mappings || []);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [dialogOpen, setDialogOpen] = useState<boolean>(false);
  const [currentMapping, setCurrentMapping] = useState<FieldMapping | null>(null);
  const [saving, setSaving] = useState<boolean>(false);

  const { showSuccess, showError } = useNotification();

  /**
   * Detect schema from the source system
   */
  const detectSchema = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await configService.detectSourceSchema(sourceId);
      if (response.data) {
        // Transform the response into SchemaField[] format
        const schemaFields: SchemaField[] = Object.entries(response.data).map(([name, type]) => ({
          name,
          type: type as string,
        }));
        setSourceFields(schemaFields);
      } else {
        setError('Failed to detect schema: No data returned.');
      }
    } catch (err: any) {
      showError(`Failed to detect schema: ${err.message}`);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [sourceId, setSourceFields, setError, setLoading, showError]);

  /**
   * Infer BigQuery data type from source field type
   */
  const inferDataType = useCallback((sourceType: string): string => {
    // Map common source data types to BigQuery types
    const typeMap: { [key: string]: string } = {
      string: 'STRING',
      varchar: 'STRING',
      text: 'STRING',
      numeric: 'FLOAT64',
      decimal: 'FLOAT64',
      float: 'FLOAT64',
      integer: 'INT64',
      int: 'INT64',
      boolean: 'BOOLEAN',
      date: 'DATE',
      time: 'TIME',
      datetime: 'TIMESTAMP',
      timestamp: 'TIMESTAMP',
    };

    return typeMap[sourceType] || 'STRING'; // Default to STRING
  }, []);

  /**
   * Generates a suggested target field name from source field name
   */
  const generateTargetName = useCallback((sourceName: string): string => {
    // Convert source name to snake_case if it's in camelCase
    const snakeCaseName = sourceName.replace(/([A-Z])/g, '_$1').toLowerCase();

    // Replace spaces with underscores
    const formattedName = snakeCaseName.replace(/\s+/g, '_');

    // Remove special characters
    const cleanName = formattedName.replace(/[^a-z0-9_]/g, '');

    return cleanName.toLowerCase();
  }, []);

  /**
   * Detect schema from the source system
   */
  useEffect(() => {
    if (sourceId) {
      handleDetectSchema();
    }
  }, [sourceId, handleDetectSchema]);

  /**
   * Initialize mappings from existingMapping prop
   */
  useEffect(() => {
    if (existingMapping) {
      setMappings(existingMapping.mappings);
    }
  }, [existingMapping]);

  /**
   * Detect schema from the source system
   */
  const handleDetectSchema = useCallback(() => {
    detectSchema();
  }, [detectSchema]);

  /**
   * Open dialog to add a new field mapping
   */
  const handleAddMapping = useCallback(() => {
    setDialogOpen(true);
    setCurrentMapping(null);
  }, [setDialogOpen, setCurrentMapping]);

  /**
   * Open dialog to edit an existing field mapping
   */
  const handleEditMapping = useCallback((mapping: FieldMapping) => {
    setDialogOpen(true);
    setCurrentMapping(mapping);
  }, [setDialogOpen, setCurrentMapping]);

  /**
   * Delete a field mapping
   */
  const handleDeleteMapping = useCallback((mappingId: string) => {
    setMappings(prevMappings => prevMappings.filter(m => m.id !== mappingId));
  }, [setMappings]);

  /**
   * Save a field mapping from the dialog
   */
  const handleSaveMapping = useCallback((mapping: FieldMapping) => {
    setMappings(prevMappings => {
      const existingIndex = prevMappings.findIndex(m => m.id === mapping.id);
      if (existingIndex !== -1) {
        // Update existing mapping
        const newMappings = [...prevMappings];
        newMappings[existingIndex] = mapping;
        return newMappings;
      } else {
        // Add new mapping
        return [...prevMappings, mapping];
      }
    });
    setDialogOpen(false);
    setCurrentMapping(null);
  }, [setMappings, setDialogOpen, setCurrentMapping]);

  /**
   * Automatically generate mappings for all unmapped fields
   */
  const handleAutoMap = useCallback(() => {
    setMappings(prevMappings => {
      const existingTargetFields = prevMappings.map(m => m.targetField);
      const newMappings = sourceFields.filter(field => !existingTargetFields.includes(field.name)).map(field => ({
        id: Math.random().toString(36).substring(7),
        sourceField: field.name,
        sourceType: field.type,
        targetField: generateTargetName(field.name),
        targetType: inferDataType(field.type),
        transformation: '',
        description: `Auto-generated mapping for ${field.name}`,
        mode: field.nullable ? 'NULLABLE' : 'REQUIRED',
        defaultValue: null
      }));
      return [...prevMappings, ...newMappings];
    });
  }, [sourceFields, setMappings, generateTargetName, inferDataType]);

  /**
   * Save the complete schema mapping
   */
  const handleSave = useCallback(async () => {
    setSaving(true);
    try {
      const schemaMapping: SchemaMapping = {
        sourceId,
        targetDataset,
        targetTable,
        mappings,
      };
      await configService.saveSchemaMapping(schemaMapping);
      showSuccess('Schema mapping saved successfully!');
      onSave?.(schemaMapping);
    } catch (err: any) {
      showError(`Failed to save schema mapping: ${err.message}`);
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }, [sourceId, targetDataset, targetTable, mappings, configService.saveSchemaMapping, onSave, setSaving, showError, setError]);

  /**
   * Handle the end of a drag operation for reordering
   */
  const handleDragEnd = useCallback((result: DropResult) => {
    if (!result.destination) {
      return;
    }

    const reorderedMappings = Array.from(mappings);
    const [movedMapping] = reorderedMappings.splice(result.source.index, 1);
    reorderedMappings.splice(result.destination.index, 0, movedMapping);

    setMappings(reorderedMappings);
  }, [mappings, setMappings]);

  const handleQuickMap = (row: any) => {
    setMappings(prevMappings => {
      const existingTargetFields = prevMappings.map(m => m.targetField);
      if (existingTargetFields.includes(row.name)) {
        return prevMappings;
      }
      const newMapping = {
        id: Math.random().toString(36).substring(7),
        sourceField: row.name,
        sourceType: row.type,
        targetField: generateTargetName(row.name),
        targetType: inferDataType(row.type),
        transformation: '',
        description: `Auto-generated mapping for ${row.name}`,
        mode: row.nullable ? 'NULLABLE' : 'REQUIRED',
        defaultValue: null
      };
      return [...prevMappings, newMapping];
    });
  };

  const existingTargetFields = useMemo(() => mappings.map(m => m.targetField), [mappings]);

  return (
    <Card title="Schema Mapping"
      subheader={`Configure field mappings for ${sourceId} to ${targetDataset}.${targetTable}`}
      action={<Button onClick={handleDetectSchema} disabled={loading} startIcon={<Refresh />}>Detect Schema</Button>}
      loading={loading}
      error={error}
    >
      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <Typography variant="h6">Source Fields</Typography>
          <Table
            columns={sourceFieldsColumns.columns}
            data={sourceFields}
            loading={loading}
            emptyMessage="No source fields detected. Please detect schema."
            selectable={false}
          />
        </Grid>
        <Grid item xs={12} md={6}>
          <Typography variant="h6">Mappings</Typography>
          <DragDropContext onDragEnd={handleDragEnd}>
            <Droppable droppableId="mappings">
              {(provided) => (
                <Box {...provided.droppableProps} ref={provided.innerRef}>
                  <Table
                    columns={mappingsColumns.columns}
                    data={mappings}
                    loading={loading}
                    emptyMessage="No mappings defined. Please add a mapping."
                    selectable={false}
                    getRowId={(row) => row.id}
                  />
                  {provided.placeholder}
                </Box>
              )}
            </Droppable>
          </DragDropContext>
          <Box mt={2} display="flex" justifyContent="space-between">
            <Button onClick={handleAddMapping} disabled={readOnly} startIcon={<Add />}>
              Add Mapping
            </Button>
            <Button onClick={handleAutoMap} disabled={readOnly || loading} startIcon={<SwapHoriz />}>
              Auto Map
            </Button>
          </Box>
        </Grid>
      </Grid>
      <Divider sx={{ my: 2 }} />
      <Box display="flex" justifyContent="flex-end">
        <Button variant="contained" color="primary" onClick={handleSave} disabled={saving || readOnly} loading={saving}>
          Save Mapping
        </Button>
      </Box>

      <FieldMappingDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        onSave={handleSaveMapping}
        mapping={currentMapping}
        sourceFields={sourceFields}
        existingTargetFields={existingTargetFields}
      />
    </Card>
  );
};

SchemaMapping.defaultProps = {
  readOnly: false,
};

export default SchemaMapping;