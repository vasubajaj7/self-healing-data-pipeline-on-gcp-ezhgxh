import React, { useState, useEffect, useCallback, useMemo } from 'react'; // react ^18.2.0
import {
  Box,
  Grid,
  Typography,
  Tabs,
  Tab,
  Paper,
  Divider,
} from '@mui/material'; // @mui/material ^5.11.0
import { useParams, useNavigate, useLocation } from 'react-router-dom'; // react-router-dom ^6.6.1
import { Add, ArrowBack, Refresh } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import MainLayout from '../components/layout/MainLayout';
import PipelineInventory from '../components/pipeline/PipelineInventory';
import PipelineDetailsCard from '../components/pipeline/PipelineDetailsCard';
import ExecutionDetailsCard from '../components/pipeline/ExecutionDetailsCard';
import PipelineDag from '../components/pipeline/PipelineDag';
import TaskExecutionTable from '../components/pipeline/TaskExecutionTable';
import Button from '../components/common/Button';
import Modal from '../components/common/Modal';
import Spinner from '../components/common/Spinner';
import {
  PipelineDefinition,
  PipelineExecution,
  TaskExecution,
} from '../types/api';
import { pipelineService } from '../services/api/pipelineService';
import { useApi } from '../hooks/useApi';
import { useNotification } from '../hooks/useNotification';

/**
 * Enum for different view modes in the pipeline management page
 */
enum ViewMode {
  LIST = 'LIST',
  DETAILS = 'DETAILS',
  EXECUTION = 'EXECUTION',
  HISTORY = 'HISTORY',
}

/**
 * Main component for the pipeline management page
 */
const PipelineManagement: React.FC = () => {
  // Get route parameters using useParams hook
  const { pipelineId, executionId, showHistory } = useParams<{
    pipelineId: string;
    executionId: string;
    showHistory: string;
  }>();

  // Get navigation function using useNavigate hook
  const navigate = useNavigate();

  // Get location object using useLocation hook
  const location = useLocation();

  // Initialize notification hook for displaying messages
  const notification = useNotification();

  // Set up state for selected pipeline, execution, and view mode
  const [viewMode, setViewMode] = useState<ViewMode>(ViewMode.LIST);
  const [selectedPipeline, setSelectedPipeline] = useState<PipelineDefinition | null>(null);
  const [selectedExecution, setSelectedExecution] = useState<PipelineExecution | null>(null);

  // Set up state for task executions and pagination
  const [taskExecutions, setTaskExecutions] = useState<TaskExecution[]>([]);
  const [taskPage, setTaskPage] = useState(1);
  const [taskPageSize, setTaskPageSize] = useState(10);
  const [taskTotalItems, setTaskTotalItems] = useState(0);

  // Set up state for execution history and pagination
  const [executionHistory, setExecutionHistory] = useState<PipelineExecution[]>([]);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyPageSize, setHistoryPageSize] = useState(10);
  const [historyTotalItems, setHistoryTotalItems] = useState(0);

  // Set up state for create/edit modal visibility
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);

  // Set up state for selected tab in execution details view
  const [selectedTab, setSelectedTab] = useState(0);

  // Set up API hooks for fetching pipeline and execution data
  const pipelineApi = useApi<PipelineDefinition>();
  const executionApi = useApi<PipelineExecution>();
  const tasksApi = useApi<TaskExecution[]>();
  const historyApi = useApi<PipelineExecution[]>();

  // Implement handlers for pipeline selection, creation, editing, and deletion
  const handleSelectPipeline = (pipeline: PipelineDefinition) => {
    setSelectedPipeline(pipeline);
    setViewMode(ViewMode.DETAILS);
    navigate(`/pipelines/${pipeline.pipelineId}`);
    setSelectedExecution(null);
  };

  const handleCreatePipeline = () => {
    setCreateModalOpen(true);
  };

  const handleEditPipeline = (pipeline: PipelineDefinition) => {
    setSelectedPipeline(pipeline);
    setEditModalOpen(true);
  };

  const handleViewExecution = (executionId: string) => {
    executionApi.get(`/pipelines/executions/${executionId}`)
      .then((execution) => {
        setSelectedExecution(execution as any);
        setViewMode(ViewMode.EXECUTION);
        navigate(`/pipelines/${selectedPipeline?.pipelineId}/execution/${executionId}`);
        setTaskExecutions([]);
        setSelectedTab(0);
      })
      .catch((error) => {
        notification.showError(error.message || 'Failed to fetch execution details.');
      });
  };

  const handleViewHistory = (pipeline: PipelineDefinition) => {
    setSelectedPipeline(pipeline);
    setViewMode(ViewMode.HISTORY);
    navigate(`/pipelines/${pipeline.pipelineId}/history`);
  };

  const handleBackToList = () => {
    setSelectedPipeline(null);
    setSelectedExecution(null);
    setViewMode(ViewMode.LIST);
    navigate('/pipelines');
  };

  const handleBackToPipeline = () => {
    setSelectedExecution(null);
    setViewMode(ViewMode.DETAILS);
    navigate(`/pipelines/${selectedPipeline?.pipelineId}`);
  };

  // Implement handlers for refreshing data
  const handleRefreshPipeline = () => {
    if (selectedPipeline?.pipelineId) {
      pipelineApi.get(`/pipelines/${selectedPipeline.pipelineId}`)
        .then((pipeline) => {
          setSelectedPipeline(pipeline as any);
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to refresh pipeline details.');
        });
    }
  };

  const handleRefreshExecution = () => {
    if (selectedExecution?.executionId) {
      executionApi.get(`/pipelines/executions/${selectedExecution.executionId}`)
        .then((execution) => {
          setSelectedExecution(execution as any);
          if (execution) {
            tasksApi.get(`/pipelines/executions/${execution.executionId}/tasks`, {
              params: { page: taskPage, pageSize: taskPageSize },
            })
              .then((tasks) => {
                setTaskExecutions(tasks as any);
              })
              .catch((error) => {
                notification.showError(error.message || 'Failed to refresh task executions.');
              });
          }
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to refresh execution details.');
        });
    }
  };

  const handleRefreshHistory = () => {
    if (selectedPipeline?.pipelineId) {
      historyApi.get(`/pipelines/${selectedPipeline.pipelineId}/history`, {
        params: { page: historyPage, pageSize: historyPageSize },
      })
        .then((history) => {
          setExecutionHistory(history as any);
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to refresh execution history.');
        });
    }
  };

  // Implement handlers for pagination
  const handleTaskPageChange = (page: number, pageSize: number) => {
    setTaskPage(page);
    setTaskPageSize(pageSize);
    if (selectedExecution?.executionId) {
      tasksApi.get(`/pipelines/executions/${selectedExecution.executionId}/tasks`, {
        params: { page, pageSize },
      })
        .then((tasks) => {
          setTaskExecutions(tasks as any);
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to fetch task executions.');
        });
    }
  };

  const handleHistoryPageChange = (page: number, pageSize: number) => {
    setHistoryPage(page);
    setHistoryPageSize(pageSize);
    if (selectedPipeline?.pipelineId) {
      historyApi.get(`/pipelines/${selectedPipeline.pipelineId}/history`, {
        params: { page, pageSize },
      })
        .then((history) => {
          setExecutionHistory(history as any);
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to fetch execution history.');
        });
    }
  };

  // Implement handler for tab changes
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setSelectedTab(newValue);
  };

  // Implement handlers for modal close
  const handleCreateModalClose = () => {
    setCreateModalOpen(false);
  };

  const handleEditModalClose = () => {
    setEditModalOpen(false);
  };

  // Implement handlers for form submission
  const handleCreatePipelineSubmit = (pipelineData: Partial<PipelineDefinition>) => {
    pipelineService.createPipelineDefinition(pipelineData)
      .then(() => {
        notification.showSuccess('Pipeline created successfully.');
        setCreateModalOpen(false);
        navigate(`/pipelines/${pipelineData.pipelineId}`);
      })
      .catch((error) => {
        notification.showError(error.message || 'Failed to create pipeline.');
      });
  };

  const handleEditPipelineSubmit = (pipelineData: Partial<PipelineDefinition>) => {
    if (selectedPipeline?.pipelineId) {
      pipelineService.updatePipelineDefinition(selectedPipeline.pipelineId, pipelineData)
        .then(() => {
          notification.showSuccess('Pipeline updated successfully.');
          setEditModalOpen(false);
          handleRefreshPipeline();
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to update pipeline.');
        });
    }
  };

  // Implement handler for delete pipeline
  const handleDeletePipeline = (pipeline: PipelineDefinition) => {
    // TODO: Implement delete pipeline logic
    console.log('Deleting pipeline:', pipeline);
  };

    const handleViewLogs = () => {
        // TODO: Implement view logs logic
        console.log('Viewing logs');
    };

    const handleViewDataSamples = () => {
        // TODO: Implement view data samples logic
        console.log('Viewing data samples');
    };

  // Implement effect hooks for loading data based on route parameters
  useEffect(() => {
    if (pipelineId) {
      pipelineApi.get(`/pipelines/${pipelineId}`)
        .then((pipeline) => {
          setSelectedPipeline(pipeline as any);
          if (executionId) {
            executionApi.get(`/pipelines/executions/${executionId}`)
              .then((execution) => {
                setSelectedExecution(execution as any);
                setViewMode(ViewMode.EXECUTION);
              })
              .catch((error) => {
                notification.showError(error.message || 'Failed to fetch execution details.');
              });
          } else if (showHistory === 'true') {
            setViewMode(ViewMode.HISTORY);
          } else {
            setViewMode(ViewMode.DETAILS);
          }
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to fetch pipeline details.');
        });
    } else {
      setViewMode(ViewMode.LIST);
    }
  }, [pipelineId, executionId, showHistory, pipelineApi, executionApi, notification]);

  useEffect(() => {
    if (selectedExecution) {
      tasksApi.get(`/pipelines/executions/${selectedExecution.executionId}/tasks`, {
        params: { page: taskPage, pageSize: taskPageSize },
      })
        .then((tasks) => {
          setTaskExecutions(tasks as any);
        })
        .catch((error) => {
          notification.showError(error.message || 'Failed to fetch task executions.');
        });
    }
  }, [selectedExecution, taskPage, taskPageSize, tasksApi, notification]);

  // Render the page layout with appropriate components based on view mode
  return (
    <MainLayout>
      <Box className="container" sx={{ padding: 3 }}>
        {/* Page Header */}
        <Box className="header" sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
          {viewMode !== ViewMode.LIST && (
            <Button
              className="backButton"
              variant="outlined"
              startIcon={<ArrowBack />}
              onClick={viewMode === ViewMode.EXECUTION || viewMode === ViewMode.HISTORY ? handleBackToPipeline : handleBackToList}
              sx={{ mr: 2 }}
            >
              Back
            </Button>
          )}
          <Typography variant="h4" component="h1">
            {viewMode === ViewMode.LIST ? 'Pipeline Management' : selectedPipeline?.pipelineName}
          </Typography>
          {viewMode === ViewMode.DETAILS && (
            <Button
              className="refreshButton"
              variant="outlined"
              startIcon={<Refresh />}
              onClick={handleRefreshPipeline}
              sx={{ ml: 2 }}
            >
              Refresh
            </Button>
          )}
          {viewMode === ViewMode.EXECUTION && (
            <Button
              className="refreshButton"
              variant="outlined"
              startIcon={<Refresh />}
              onClick={handleRefreshExecution}
              sx={{ ml: 2 }}
            >
              Refresh
            </Button>
          )}
          {viewMode === ViewMode.HISTORY && (
            <Button
              className="refreshButton"
              variant="outlined"
              startIcon={<Refresh />}
              onClick={handleRefreshHistory}
              sx={{ ml: 2 }}
            >
              Refresh
            </Button>
          )}
        </Box>

        {/* Content Container */}
        <Box className="contentContainer" sx={{ width: '100%' }}>
          {viewMode === ViewMode.LIST ? (
            <PipelineInventory
              onSelectPipeline={handleSelectPipeline}
              onCreatePipeline={handleCreatePipeline}
              onEditPipeline={handleEditPipeline}
              onViewHistory={handleViewHistory}
            />
          ) : viewMode === ViewMode.DETAILS && selectedPipeline ? (
            <PipelineDetailsCard
              pipeline={selectedPipeline}
              onEdit={() => handleEditPipeline(selectedPipeline)}
              onDelete={() => handleDeletePipeline(selectedPipeline)}
              onViewHistory={() => handleViewHistory(selectedPipeline)}
            />
          ) : viewMode === ViewMode.EXECUTION && selectedExecution ? (
            <Box className="tabsContainer" sx={{ width: '100%' }}>
              <Tabs value={selectedTab} onChange={handleTabChange} aria-label="execution details tabs">
                <Tab label="Details" />
                <Tab label="DAG" />
                <Tab label="Tasks" />
              </Tabs>
              <Paper className="tabContent" sx={{ p: 2 }}>
                {selectedTab === 0 && (
                  <ExecutionDetailsCard
                    execution={selectedExecution}
                    onViewLogs={handleViewLogs}
                    onViewDataSamples={handleViewDataSamples}
                    onRefresh={handleRefreshExecution}
                  />
                )}
                {selectedTab === 1 && (
                  <PipelineDag executionId={selectedExecution.executionId} />
                )}
                {selectedTab === 2 && (
                  <TaskExecutionTable executionId={selectedExecution.executionId} />
                )}
              </Paper>
            </Box>
          ) : viewMode === ViewMode.HISTORY && selectedPipeline ? (
            <Box className="historyTable">
              {/* TODO: Implement execution history table */}
              <Typography>Execution History for {selectedPipeline.pipelineName}</Typography>
            </Box>
          ) : (
            <Box className="loadingContainer" sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 }}>
              <Spinner />
            </Box>
          )}
        </Box>
      </Box>

      {/* Create Pipeline Modal */}
      <Modal
        open={createModalOpen}
        onClose={handleCreateModalClose}
        title="Create New Pipeline"
      >
        {/* TODO: Implement create pipeline form */}
        <Typography>Create Pipeline Form</Typography>
      </Modal>

      {/* Edit Pipeline Modal */}
      <Modal
        open={editModalOpen}
        onClose={handleEditModalClose}
        title="Edit Pipeline"
      >
        {/* TODO: Implement edit pipeline form */}
        <Typography>Edit Pipeline Form</Typography>
      </Modal>
    </MainLayout>
  );
};

export default PipelineManagement;