import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { Box, Typography, Paper, Divider } from '@mui/material'; // @mui/material ^5.11.0
import {
  Settings as SettingsIcon,
  Storage as StorageIcon,
  PipelineOutlined as PipelineIcon,
  RuleOutlined as RuleIcon,
  NotificationsOutlined as NotificationIcon,
} from '@mui/icons-material'; // @mui/icons-material ^5.11.0

import DataSourcesTable from './DataSourcesTable';
import SourceDetailsForm from './SourceDetailsForm';
import PipelineConfigTable from './PipelineConfigTable';
import ValidationRulesConfig from './ValidationRulesConfig';
import NotificationsConfig from './NotificationsConfig';
import Tabs from '../common/Tabs';
import PageContainer from '../layout/PageContainer';
import { SourceSystem, PipelineConfig } from '../../types/config';

/**
 * Main component for the configuration dashboard with tabbed interface
 * @returns {JSX.Element} The rendered configuration dashboard
 */
const ConfigDashboard: React.FC = () => {
  // LD1: Initialize state for active tab index
  const [activeTab, setActiveTab] = useState<number>(0);

  // LD1: Initialize state for selected source and pipeline for editing
  const [selectedSource, setSelectedSource] = useState<SourceSystem | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<PipelineConfig | null>(null);

  // LD1: Initialize state for showing source form and refresh triggers
  const [showSourceForm, setShowSourceForm] = useState<boolean>(false);
  const [showPipelineForm, setShowPipelineForm] = useState<boolean>(false);
  const [sourceRefreshTrigger, setSourceRefreshTrigger] = useState<number>(0);
  const [pipelineRefreshTrigger, setPipelineRefreshTrigger] = useState<number>(0);
  const [rulesRefreshTrigger, setRulesRefreshTrigger] = useState<number>(0);

  // LD1: Handle tab change by updating active tab state
  const handleTabChange = useCallback((newTabIndex: number) => {
    setActiveTab(newTabIndex);
    setSelectedSource(null);
    setSelectedPipeline(null);
    setShowSourceForm(false);
    setShowPipelineForm(false);
  }, [setActiveTab, setSelectedSource, setSelectedPipeline, setShowSourceForm, setShowPipelineForm]);

  // LD1: Handle source selection for editing
  const handleSourceSelect = useCallback((source: SourceSystem) => {
    setSelectedSource(source);
    setShowSourceForm(true);
  }, [setSelectedSource, setShowSourceForm]);

  const handleAddSource = useCallback(() => {
    setSelectedSource(null);
    setShowSourceForm(true);
  }, [setSelectedSource, setShowSourceForm]);

  // LD1: Handle pipeline selection for editing
  const handlePipelineSelect = useCallback((pipeline: PipelineConfig) => {
    setSelectedPipeline(pipeline);
    setShowPipelineForm(true);
  }, [setSelectedPipeline, setShowPipelineForm]);

  const handleAddPipeline = useCallback(() => {
    setSelectedPipeline(null);
    setShowPipelineForm(true);
  }, [setSelectedPipeline, setShowPipelineForm]);

  // LD1: Handle form submission and cancellation
  const handleSourceSave = useCallback((source: SourceSystem) => {
    setShowSourceForm(false);
    setSelectedSource(null);
    setSourceRefreshTrigger(prev => prev + 1);
  }, [setShowSourceForm, setSelectedSource, setSourceRefreshTrigger]);

  const handleSourceCancel = useCallback(() => {
    setShowSourceForm(false);
    setSelectedSource(null);
  }, [setShowSourceForm, setSelectedSource]);

  const handlePipelineSave = useCallback((pipeline: PipelineConfig) => {
    setShowPipelineForm(false);
    setSelectedPipeline(null);
    setPipelineRefreshTrigger(prev => prev + 1);
  }, [setShowPipelineForm, setSelectedPipeline, setPipelineRefreshTrigger]);

  const handlePipelineCancel = useCallback(() => {
    setShowPipelineForm(false);
    setSelectedPipeline(null);
  }, [setShowPipelineForm, setSelectedPipeline]);

  // LD1: Define tab configurations with icons and content components
  const tabs = React.useMemo(() => [
    {
      id: 'data-sources',
      label: 'Data Sources',
      icon: <StorageIcon />,
      content: (
        <DataSourcesTable
          onSourceSelect={handleSourceSelect}
          onAddSource={handleAddSource}
          refreshTrigger={sourceRefreshTrigger}
        />
      ),
    },
    {
      id: 'pipelines',
      label: 'Pipelines',
      icon: <PipelineIcon />,
      content: (
        <PipelineConfigTable
          onEditPipeline={handlePipelineSelect}
          onCreatePipeline={handleAddPipeline}
          onViewPipeline={handlePipelineSelect}
          refreshTrigger={pipelineRefreshTrigger}
        />
      ),
    },
    {
      id: 'validation-rules',
      label: 'Validation Rules',
      icon: <RuleIcon />,
      content: (
        <ValidationRulesConfig
          refreshTrigger={rulesRefreshTrigger}
        />
      ),
    },
    {
      id: 'notifications',
      label: 'Notifications',
      icon: <NotificationIcon />,
      content: <NotificationsConfig />,
    },
  ], [handleSourceSelect, handleAddSource, sourceRefreshTrigger, handlePipelineSelect, handleAddPipeline, pipelineRefreshTrigger, rulesRefreshTrigger]);

  // LD1: Render page container with title and description
  return (
    <PageContainer>
      <Typography variant="h4" component="h1" sx={{ fontWeight: '500', marginBottom: (theme) => theme.spacing(1) }}>
        Configuration
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ marginBottom: (theme) => theme.spacing(2) }}>
        Manage data sources, pipelines, validation rules, and notification settings for the self-healing data pipeline.
      </Typography>

      {/* LD1: Render tabs component with configured tabs */}
      <Tabs
        tabs={tabs}
        activeTab={activeTab}
        onTabChange={handleTabChange}
      />

      {/* LD1: Conditionally render source details form when editing a source */}
      {showSourceForm && (
        <Box sx={{ marginTop: (theme) => theme.spacing(2) }}>
          <SourceDetailsForm
            sourceId={selectedSource?.sourceId || null}
            onSave={handleSourceSave}
            onCancel={handleSourceCancel}
          />
        </Box>
      )}

      {/* LD1: Conditionally render pipeline form when editing a pipeline */}
      {showPipelineForm && (
        <Box sx={{ marginTop: (theme) => theme.spacing(2) }}>
          {/* <PipelineDetailsForm
            pipelineId={selectedPipeline?.pipelineId || null}
            onSave={handlePipelineSave}
            onCancel={handlePipelineCancel}
          /> */}
        </Box>
      )}
    </PageContainer>
  );
};

export default ConfigDashboard;