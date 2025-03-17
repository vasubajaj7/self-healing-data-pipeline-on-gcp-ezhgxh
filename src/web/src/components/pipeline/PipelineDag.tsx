import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { 
  Box, 
  Typography, 
  Tooltip, 
  IconButton, 
  Paper 
} from '@mui/material';
import { styled } from '@mui/material/styles';
import { 
  ZoomIn, 
  ZoomOut, 
  RestartAlt, 
  FullscreenExit, 
  Fullscreen 
} from '@mui/icons-material';
import ReactFlow, { 
  Background, 
  Controls, 
  MiniMap, 
  useNodesState, 
  useEdgesState, 
  MarkerType 
} from 'reactflow';
import 'reactflow/dist/style.css';

import Card from '../common/Card';
import StatusIndicator from '../charts/StatusIndicator';
import Spinner from '../common/Spinner';
import pipelineService from '../../services/api/pipelineService';
import { PipelineExecution, TaskExecution } from '../../types/api';
import { PipelineStatus } from '../../types/dashboard';
import { colors } from '../../theme/colors';

/**
 * Props interface for the PipelineDag component
 */
interface PipelineDagProps {
  /**
   * ID of the pipeline execution to visualize
   */
  executionId: string;
  /**
   * Interval in milliseconds to refresh the DAG data
   */
  refreshInterval?: number;
  /**
   * Height of the DAG container
   */
  height?: number | string;
  /**
   * Width of the DAG container
   */
  width?: number | string;
  /**
   * Callback function when a task node is clicked
   */
  onTaskClick?: (task: TaskExecution) => void;
  /**
   * Whether to show zoom and reset controls
   */
  showControls?: boolean;
  /**
   * Whether to show the mini map
   */
  showMiniMap?: boolean;
  /**
   * Additional CSS class for styling
   */
  className?: string;
}

/**
 * Interface for task node data structure
 */
interface TaskNode {
  id: string;
  type: string;
  data: {
    label: string;
    status: string;
    taskType: string;
    isHealedTask: boolean;
    onClick?: (task: TaskExecution) => void;
    taskExecution: TaskExecution;
  };
  position: { x: number, y: number };
  style?: React.CSSProperties;
}

/**
 * Interface for edge between task nodes
 */
interface TaskEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  animated: boolean;
  style?: React.CSSProperties;
  markerEnd?: {
    type: MarkerType;
    color: string;
  };
}

/**
 * Maps pipeline status to corresponding color
 */
const getStatusColor = (status: string): string => {
  switch (status.toUpperCase()) {
    case PipelineStatus.HEALTHY:
    case 'SUCCESS':
    case 'SUCCEEDED': 
      return colors.status.healthy;
    case PipelineStatus.WARNING:
    case 'RUNNING':
      return colors.status.warning;
    case PipelineStatus.ERROR:
    case 'FAILED':
      return colors.status.error;
    case PipelineStatus.INACTIVE:
      return colors.status.inactive;
    case 'SELF_HEALING':
      return colors.info.main;
    default:
      return colors.grey[500];
  }
};

/**
 * Formats task ID for display by removing prefixes and formatting
 */
const formatTaskId = (taskId: string): string => {
  // Remove common prefixes like 'task_', 'operator_'
  let formattedId = taskId.replace(/^(task_|operator_)/, '');
  // Replace underscores with spaces and capitalize words
  return formattedId
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
};

/**
 * Styled container for the DAG visualization
 */
const DagContainer = styled(Box)<{ height?: number | string, width?: number | string }>(
  ({ height, width }) => ({
    height: typeof height === 'number' ? `${height}px` : height || '500px',
    width: typeof width === 'number' ? `${width}px` : width || '100%',
    position: 'relative',
    borderRadius: '4px',
    overflow: 'hidden',
  })
);

/**
 * Custom node component for rendering task nodes in the DAG
 */
const TaskNodeComponent: React.FC<{ data: any }> = ({ data }) => {
  const { label, status, taskType, isHealedTask, onClick, taskExecution } = data;
  
  const handleClick = () => {
    if (onClick && taskExecution) {
      onClick(taskExecution);
    }
  };

  return (
    <Paper
      elevation={2}
      sx={{
        p: 1.5,
        minWidth: 150,
        borderRadius: '4px',
        cursor: onClick ? 'pointer' : 'default',
        border: isHealedTask ? `2px solid ${colors.success.main}` : 'none',
        boxShadow: isHealedTask 
          ? `0 0 0 2px ${colors.success.main}, 0 2px 5px rgba(0,0,0,0.15)` 
          : '0 2px 5px rgba(0,0,0,0.15)',
        '&:hover': {
          boxShadow: isHealedTask 
            ? `0 0 0 2px ${colors.success.main}, 0 4px 8px rgba(0,0,0,0.2)` 
            : '0 4px 8px rgba(0,0,0,0.2)',
        }
      }}
      onClick={handleClick}
    >
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <StatusIndicator status={status.toLowerCase()} size={10} />
        <Typography variant="caption" sx={{ color: 'text.secondary' }}>
          {taskType}
        </Typography>
      </Box>
      <Typography variant="body2" sx={{ fontWeight: 500 }}>
        {label}
      </Typography>
      {isHealedTask && (
        <Box sx={{ mt: 1, display: 'flex', alignItems: 'center' }}>
          <Typography variant="caption" sx={{ color: colors.success.main, fontWeight: 500 }}>
            Auto-fixed by self-healing
          </Typography>
        </Box>
      )}
    </Paper>
  );
};

/**
 * Custom controls for the DAG visualization
 */
const ControlsPanel: React.FC<{
  onZoomIn: () => void;
  onZoomOut: () => void;
  onReset: () => void;
  onToggleFullscreen: () => void;
  isFullscreen: boolean;
}> = ({ onZoomIn, onZoomOut, onReset, onToggleFullscreen, isFullscreen }) => {
  return (
    <Paper
      elevation={2}
      sx={{
        position: 'absolute',
        top: 10,
        right: 10,
        display: 'flex',
        p: 0.5,
        zIndex: 5,
      }}
    >
      <Tooltip title="Zoom In">
        <IconButton onClick={onZoomIn} size="small">
          <ZoomIn fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title="Zoom Out">
        <IconButton onClick={onZoomOut} size="small">
          <ZoomOut fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title="Reset View">
        <IconButton onClick={onReset} size="small">
          <RestartAlt fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}>
        <IconButton onClick={onToggleFullscreen} size="small">
          {isFullscreen ? <FullscreenExit fontSize="small" /> : <Fullscreen fontSize="small" />}
        </IconButton>
      </Tooltip>
    </Paper>
  );
};

/**
 * Main component for rendering the pipeline DAG visualization
 * 
 * Visualizes pipeline execution as a Directed Acyclic Graph (DAG), showing the
 * relationships between tasks, their dependencies, and execution status.
 */
const PipelineDag: React.FC<PipelineDagProps> = ({
  executionId,
  refreshInterval = 30000,
  height = 500,
  width = '100%',
  onTaskClick,
  showControls = true,
  showMiniMap = true,
  className,
}) => {
  // States
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pipelineExecution, setPipelineExecution] = useState<PipelineExecution | null>(null);
  const [fullscreen, setFullscreen] = useState(false);
  
  const containerRef = useRef<HTMLDivElement>(null);
  const reactFlowInstance = useRef<any>(null);

  // Function to fetch pipeline execution and task data
  const fetchPipelineData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Fetch pipeline execution
      const executionResponse = await pipelineService.getPipelineExecution(executionId);
      const execution = executionResponse.data;
      setPipelineExecution(execution);
      
      // Fetch task executions
      const tasksResponse = await pipelineService.getTaskExecutions(executionId, { page: 1, pageSize: 100 });
      
      // Process the data to create nodes and edges
      const taskExecutions = tasksResponse.items;
      const { nodes, edges } = processTaskExecutions(taskExecutions, execution);
      
      setNodes(nodes);
      setEdges(edges);
      setLoading(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load pipeline data');
      setLoading(false);
    }
  }, [executionId]);

  // Process task executions into nodes and edges
  const processTaskExecutions = (
    taskExecutions: TaskExecution[], 
    execution: PipelineExecution
  ): { nodes: TaskNode[], edges: TaskEdge[] } => {
    // Map tasks to their dependencies based on taskId format or other metadata
    const taskMap = new Map<string, TaskExecution>();
    const dependencies = new Map<string, string[]>();
    
    // First pass: build task map and identify dependencies
    taskExecutions.forEach(task => {
      taskMap.set(task.taskId, task);
      
      // Extract dependencies from task parameters or other data
      const taskParams = task.taskParams || {};
      const upstreamTaskIds = Array.isArray(taskParams.upstream_task_ids) 
        ? taskParams.upstream_task_ids 
        : [];
      
      dependencies.set(task.taskId, upstreamTaskIds);
    });
    
    // Create nodes and calculate positions
    const nodes: TaskNode[] = [];
    const edges: TaskEdge[] = [];
    
    // Calculate DAG layout
    const levels = calculateDagLevels(taskMap, dependencies);
    const levelWidth = 250;
    const levelHeight = 150;
    
    // Create nodes with positions
    Array.from(taskMap.entries()).forEach(([taskId, task]) => {
      const level = levels.get(taskId) || 0;
      const tasksAtLevel = Array.from(levels.entries())
        .filter(([, l]) => l === level)
        .length;
      
      const tasksAtLevelBefore = Array.from(levels.entries())
        .filter(([id, l]) => l === level && id < taskId)
        .length;
      
      const x = level * levelWidth;
      const y = (tasksAtLevelBefore + 0.5) * levelHeight;
      
      // Check if this task was healed by looking at task parameters or status
      const isHealedTask = Boolean(task.taskParams?.healed_by_self_healing) || 
                          task.status === 'HEALED';
      
      nodes.push({
        id: taskId,
        type: 'default',
        data: {
          label: formatTaskId(taskId),
          status: task.status,
          taskType: task.taskType,
          isHealedTask,
          onClick: onTaskClick,
          taskExecution: task
        },
        position: { x, y },
        style: {
          width: 200,
        }
      });
    });
    
    // Create edges from dependencies
    Array.from(dependencies.entries()).forEach(([taskId, upstreamIds]) => {
      upstreamIds.forEach(upstreamId => {
        if (taskMap.has(upstreamId)) {
          const status = taskMap.get(taskId)?.status || '';
          const edgeColor = getStatusColor(status);
          
          edges.push({
            id: `${upstreamId}-${taskId}`,
            source: upstreamId,
            target: taskId,
            type: 'smoothstep',
            animated: status === 'RUNNING' || status === 'SELF_HEALING',
            style: { stroke: edgeColor },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: edgeColor,
            },
          });
        }
      });
    });
    
    return { nodes, edges };
  };

  // Helper function to calculate DAG levels for layout
  const calculateDagLevels = (
    taskMap: Map<string, TaskExecution>,
    dependencies: Map<string, string[]>
  ): Map<string, number> => {
    const levels = new Map<string, number>();
    const visited = new Set<string>();
    
    // Calculate level for a task recursively
    const calculateLevel = (taskId: string): number => {
      if (levels.has(taskId)) {
        return levels.get(taskId)!;
      }
      
      if (visited.has(taskId)) {
        // Cycle detected - handle appropriately
        return 0;
      }
      
      visited.add(taskId);
      
      const upstreamIds = dependencies.get(taskId) || [];
      if (upstreamIds.length === 0) {
        levels.set(taskId, 0);
        return 0;
      }
      
      let maxUpstreamLevel = -1;
      upstreamIds.forEach(upstreamId => {
        if (taskMap.has(upstreamId)) {
          const upstreamLevel = calculateLevel(upstreamId);
          maxUpstreamLevel = Math.max(maxUpstreamLevel, upstreamLevel);
        }
      });
      
      const level = maxUpstreamLevel + 1;
      levels.set(taskId, level);
      return level;
    };
    
    // Calculate levels for all tasks
    Array.from(taskMap.keys()).forEach(taskId => {
      if (!levels.has(taskId)) {
        calculateLevel(taskId);
      }
    });
    
    return levels;
  };

  // Zoom controls
  const handleZoomIn = () => {
    if (reactFlowInstance.current) {
      reactFlowInstance.current.zoomIn();
    }
  };

  const handleZoomOut = () => {
    if (reactFlowInstance.current) {
      reactFlowInstance.current.zoomOut();
    }
  };

  const handleReset = () => {
    if (reactFlowInstance.current) {
      reactFlowInstance.current.fitView();
    }
  };

  const toggleFullscreen = () => {
    if (containerRef.current) {
      if (!fullscreen) {
        if (containerRef.current.requestFullscreen) {
          containerRef.current.requestFullscreen();
        }
      } else {
        if (document.exitFullscreen) {
          document.exitFullscreen();
        }
      }
      setFullscreen(!fullscreen);
    }
  };

  // Setup refresh interval
  useEffect(() => {
    fetchPipelineData();
    
    // Setup interval for periodic refresh if refreshInterval is provided
    let intervalId: number | undefined;
    if (refreshInterval > 0) {
      intervalId = window.setInterval(fetchPipelineData, refreshInterval);
    }
    
    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [fetchPipelineData, refreshInterval]);

  // Fullscreen change event listener
  useEffect(() => {
    const handleFullscreenChange = () => {
      setFullscreen(!!document.fullscreenElement);
    };
    
    document.addEventListener('fullscreenchange', handleFullscreenChange);
    return () => {
      document.removeEventListener('fullscreenchange', handleFullscreenChange);
    };
  }, []);

  // Node types with custom component
  const nodeTypes = useMemo(() => ({ default: TaskNodeComponent }), []);

  // Render loading state
  if (loading && !nodes.length) {
    return (
      <Card loading={true} title="Pipeline Execution" height={height} width={width}>
        <Spinner label="Loading pipeline data..." />
      </Card>
    );
  }

  // Render error state
  if (error) {
    return (
      <Card error={error} title="Pipeline Execution" height={height} width={width}>
        <Box display="flex" justifyContent="center" alignItems="center" height="100%">
          <Typography color="error">
            {error}
          </Typography>
        </Box>
      </Card>
    );
  }

  return (
    <Card 
      title="Pipeline DAG" 
      height={height} 
      width={width}
      className={className}
    >
      <DagContainer ref={containerRef} height={height} width={width}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onInit={(instance) => {
            reactFlowInstance.current = instance;
            // Fit the view to show all nodes
            setTimeout(() => instance.fitView({ padding: 0.2 }), 100);
          }}
          nodeTypes={nodeTypes}
          fitView
          minZoom={0.1}
          maxZoom={2}
          defaultViewport={{ x: 0, y: 0, zoom: 0.5 }}
          nodesDraggable={false}
          nodesConnectable={false}
          attributionPosition="bottom-right"
        >
          <Background color="#f5f5f5" gap={16} />
          {showMiniMap && <MiniMap />}
          {showControls && !fullscreen && <Controls position="bottom-right" />}
          {showControls && (
            <ControlsPanel
              onZoomIn={handleZoomIn}
              onZoomOut={handleZoomOut}
              onReset={handleReset}
              onToggleFullscreen={toggleFullscreen}
              isFullscreen={fullscreen}
            />
          )}
        </ReactFlow>
        {loading && (
          <Box 
            position="absolute" 
            top={10} 
            left={10} 
            bgcolor="rgba(255,255,255,0.8)" 
            borderRadius={1} 
            p={1}
          >
            <Spinner size="small" label="Refreshing..." />
          </Box>
        )}
      </DagContainer>
    </Card>
  );
};

export default PipelineDag;