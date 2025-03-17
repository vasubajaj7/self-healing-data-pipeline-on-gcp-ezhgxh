import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Button,
  Tabs,
  Tab,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  MenuItem,
  Select,
  FormControl,
  InputLabel,
  Divider,
  CircularProgress,
} from '@mui/material'; // @mui/material ^5.11.0
import {
  Add,
  Refresh,
  PlayArrow,
  Stop,
  Delete,
  Edit,
  Info,
  CheckCircle,
  Warning,
  Error,
  HourglassEmpty,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0

import Card from '../common/Card';
import Table from '../common/Table';
import ModelHealthCard from './ModelHealthCard';
import ModelPerformanceCard from './ModelPerformanceCard';
import { useApi } from '../../hooks/useApi';
import healingService from '../../services/api/healingService';
import {
  AIModel,
  ModelHealth,
  ModelType,
  ModelStatus,
  ModelTrainingRequest,
} from '../../types/selfHealing';
import { formatDate } from '../../utils/date';

/**
 * Interface defining the props for the ModelManagementPanel component.
 */
interface ModelManagementPanelProps {
  className?: string;
  refreshInterval?: number;
}

/**
 * Interface for the ModelTrainingDialog component
 */
interface ModelTrainingDialogProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (trainingRequest: ModelTrainingRequest) => void;
  existingModel?: AIModel;
}

/**
 * Returns a human-readable label for a model type
 * @param modelType The model type
 * @returns Human-readable label for the model type
 */
const getModelTypeLabel = (modelType: ModelType): string => {
  switch (modelType) {
    case ModelType.DETECTION:
      return 'Detection';
    case ModelType.IMPUTATION:
      return 'Imputation';
    case ModelType.CORRECTION:
      return 'Correction';
    case ModelType.PREDICTION:
      return 'Prediction';
    case ModelType.SCHEMA:
      return 'Schema';
    default:
      return modelType;
  }
};

/**
 * Returns a styled chip component for model status
 * @param status The model status
 * @returns Chip component with appropriate color and icon
 */
const getModelStatusChip = (status: ModelStatus): React.ReactNode => {
  let color: 'success' | 'warning' | 'error' | 'default' = 'default';
  let icon: React.ReactNode = <Info />;

  if (status === ModelStatus.ACTIVE) {
    color = 'success';
    icon = <CheckCircle />;
  } else if (status === ModelStatus.TRAINING) {
    color = 'warning';
    icon = <HourglassEmpty />;
  } else if (status === ModelStatus.FAILED) {
    color = 'error';
    icon = <Error />;
  }

  return <Chip label={status} color={color} icon={icon} size="small" />;
};

/**
 * Formats model size in bytes to a human-readable format
 * @param sizeString The size string
 * @returns Formatted size string (e.g., '45 MB')
 */
const formatModelSize = (sizeString: string): string => {
  const size = Number(sizeString);
  if (size >= 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  } else if (size >= 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  } else {
    return `${size} bytes`;
  }
};

/**
 * Dialog component for creating or retraining AI models
 */
const ModelTrainingDialog: React.FC<ModelTrainingDialogProps> = ({ open, onClose, onSubmit, existingModel }) => {
  const [modelName, setModelName] = useState(existingModel?.name || '');
  const [modelType, setModelType] = useState<ModelType>(existingModel?.modelType || ModelType.DETECTION);
  const [modelDescription, setModelDescription] = useState(existingModel?.description || '');

  useEffect(() => {
    if (existingModel) {
      setModelName(existingModel.name || '');
      setModelType(existingModel.modelType || ModelType.DETECTION);
      setModelDescription(existingModel.description || '');
    }
  }, [existingModel]);

  const handleSubmit = () => {
    const trainingRequest: ModelTrainingRequest = {
      modelId: existingModel?.modelId,
      modelType: modelType,
      name: modelName,
      description: modelDescription,
      trainingConfig: {},
    };
    onSubmit(trainingRequest);
  };

  return (
    <Dialog open={open} onClose={onClose} aria-labelledby="model-training-dialog-title">
      <DialogTitle id="model-training-dialog-title">
        {existingModel ? 'Retrain Model' : 'New Model'}
      </DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          margin="dense"
          id="model-name"
          label="Model Name"
          type="text"
          fullWidth
          variant="outlined"
          value={modelName}
          onChange={(e) => setModelName(e.target.value)}
        />
        <FormControl fullWidth margin="dense">
          <InputLabel id="model-type-label">Model Type</InputLabel>
          <Select
            labelId="model-type-label"
            id="model-type"
            value={modelType}
            label="Model Type"
            onChange={(e) => setModelType(e.target.value as ModelType)}
          >
            <MenuItem value={ModelType.DETECTION}>Detection</MenuItem>
            <MenuItem value={ModelType.IMPUTATION}>Imputation</MenuItem>
            <MenuItem value={ModelType.CORRECTION}>Correction</MenuItem>
            <MenuItem value={ModelType.PREDICTION}>Prediction</MenuItem>
            <MenuItem value={ModelType.SCHEMA}>Schema</MenuItem>
          </Select>
        </FormControl>
        <TextField
          margin="dense"
          id="model-description"
          label="Description"
          type="text"
          fullWidth
          multiline
          rows={4}
          variant="outlined"
          value={modelDescription}
          onChange={(e) => setModelDescription(e.target.value)}
        />
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button onClick={handleSubmit}>Submit</Button>
      </DialogActions>
    </Dialog>
  );
};

/**
 * Main component for managing AI models used in the self-healing pipeline
 */
const ModelManagementPanel: React.FC<ModelManagementPanelProps> = ({ className, refreshInterval = 60000 }) => {
  const [models, setModels] = useState<AIModel[]>([]);
  const [selectedModel, setSelectedModel] = useState<AIModel | null>(null);
  const [modelHealth, setModelHealth] = useState<ModelHealth | null>(null);
  const [isTrainingDialogOpen, setIsTrainingDialogOpen] = useState(false);
  const [activeTab, setActiveTab] = useState(0);

  const { executeRequest: executeGetModels, loading: loadingModels, error: errorModels } = useApi();
  const { executeRequest: executeGetModelHealth, loading: loadingModelHealth, error: errorModelHealth } = useApi();
  const { executeRequest: executeTrainModel } = useApi();
  const { executeRequest: executeActivateModel } = useApi();
  const { executeRequest: executeDeactivateModel } = useApi();

  /**
   * Fetches the list of AI models from the API
   */
  const fetchModels = useCallback(async () => {
    try {
      const response = await executeGetModels(healingService.getAIModels, { page: 1, pageSize: 100 });
      if (response && response.items) {
        setModels(response.items);
      }
    } catch (error) {
      console.error('Failed to fetch models:', error);
    }
  }, [executeGetModels]);

  /**
   * Handles selection of a model for detailed view
   */
  const handleModelSelect = useCallback(async (model: AIModel) => {
    setSelectedModel(model);
    try {
      const response = await executeGetModelHealth(healingService.getModelHealth, model.modelId);
      if (response) {
        setModelHealth(response.data);
      }
    } catch (error) {
      console.error('Failed to fetch model health:', error);
      setModelHealth(null);
    }
  }, [executeGetModelHealth]);

  /**
   * Handles initiating model training
   */
  const handleTrainModel = useCallback(async (trainingRequest: ModelTrainingRequest) => {
    try {
      await executeTrainModel(healingService.trainModel, trainingRequest);
      alert('Model training initiated successfully!');
      await fetchModels();
      setIsTrainingDialogOpen(false);
    } catch (error) {
      console.error('Failed to train model:', error);
      alert('Failed to initiate model training. See console for details.');
    }
  }, [executeTrainModel, fetchModels]);

  /**
   * Handles activating a model for use in self-healing
   */
  const handleActivateModel = useCallback(async (modelId: string) => {
    try {
      await executeActivateModel(healingService.activateModel, modelId);
      alert('Model activated successfully!');
      await fetchModels();
    } catch (error) {
      console.error('Failed to activate model:', error);
      alert('Failed to activate model. See console for details.');
    }
  }, [executeActivateModel, fetchModels]);

  /**
   * Handles deactivating a model from use in self-healing
   */
  const handleDeactivateModel = useCallback(async (modelId: string) => {
    try {
      await executeDeactivateModel(healingService.deactivateModel, modelId);
      alert('Model deactivated successfully!');
      await fetchModels();
    } catch (error) {
      console.error('Failed to deactivate model:', error);
      alert('Failed to deactivate model. See console for details.');
    }
  }, [executeDeactivateModel, fetchModels]);

  useEffect(() => {
    fetchModels();
    const intervalId = setInterval(fetchModels, refreshInterval);
    return () => clearInterval(intervalId);
  }, [fetchModels, refreshInterval]);

  const columns = React.useMemo(
    () => [
      { id: 'name', label: 'Name', sortable: true },
      { id: 'modelType', label: 'Type', format: (value: ModelType) => getModelTypeLabel(value) },
      { id: 'status', label: 'Status', format: (value: ModelStatus) => getModelStatusChip(value) },
      { id: 'accuracy', label: 'Accuracy', numeric: true, format: (value: number) => value ? value.toFixed(2) + '%' : 'N/A' },
      { id: 'lastTrainingDate', label: 'Last Trained', format: (value: string) => formatDate(value, 'MM/dd/yyyy') },
      { id: 'modelSize', label: 'Size', format: (value: string) => formatModelSize(value) },
      {
        id: 'actions',
        label: 'Actions',
        renderCell: (_, row: AIModel) => (
          <>
            <IconButton aria-label="edit" onClick={() => { setSelectedModel(row); setIsTrainingDialogOpen(true); }}>
              <Edit />
            </IconButton>
            {row.status === ModelStatus.ACTIVE ? (
              <IconButton aria-label="stop" onClick={() => handleDeactivateModel(row.modelId)}>
                <Stop />
              </IconButton>
            ) : (
              <IconButton aria-label="start" onClick={() => handleActivateModel(row.modelId)}>
                <PlayArrow />
              </IconButton>
            )}
          </>
        ),
      },
    ],
    [handleActivateModel, handleDeactivateModel]
  );

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  return (
    <Card title="AI Model Management" className={className} loading={loadingModels} error={errorModels}>
      <Box sx={{ width: '100%' }}>
        <Box sx={{ borderBottom: 1, borderColor: 'divider', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Tabs value={activeTab} onChange={handleTabChange} aria-label="ai model tabs">
            <Tab label="Models List" />
            <Tab label="Model Details" disabled={!selectedModel} />
          </Tabs>
          <Box sx={{ display: 'flex', gap: 1, p: 1 }}>
            <Button variant="contained" startIcon={<Add />} onClick={() => { setSelectedModel(null); setIsTrainingDialogOpen(true); }}>
              Add Model
            </Button>
            <IconButton aria-label="refresh" onClick={fetchModels}>
              <Refresh />
            </IconButton>
          </Box>
        </Box>
        {activeTab === 0 && (
          <Table
            columns={columns}
            data={models}
            loading={loadingModels}
            error={errorModels}
            onRowClick={handleModelSelect}
          />
        )}
        {activeTab === 1 && selectedModel && (
          <Grid container spacing={2} sx={{ p: 2 }}>
            <Grid item xs={12} md={6}>
              <ModelHealthCard modelId={selectedModel.modelId} model={selectedModel} />
            </Grid>
            <Grid item xs={12} md={6}>
              <ModelPerformanceCard modelId={selectedModel.modelId} model={selectedModel} />
            </Grid>
          </Grid>
        )}
      </Box>
      <ModelTrainingDialog
        open={isTrainingDialogOpen}
        onClose={() => setIsTrainingDialogOpen(false)}
        onSubmit={handleTrainModel}
        existingModel={selectedModel}
      />
    </Card>
  );
};

export default ModelManagementPanel;