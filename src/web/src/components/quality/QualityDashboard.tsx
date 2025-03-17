import React, { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { Box, Grid, Typography, Button, Tabs, Tab, Divider } from '@mui/material'; // @mui/material ^5.11.0
import { Add, Refresh, FilterList } from '@mui/icons-material'; // @mui/icons-material ^5.11.0
import Card from '../common/Card';
import DatasetQualityTable from './DatasetQualityTable';
import QualityScoreChart from './QualityScoreChart';
import ValidationRulesTable from './ValidationRulesTable';
import ValidationIssuesTable from './ValidationIssuesTable';
import QualityDimensionsCard from './QualityDimensionsCard';
import ValidationRuleEditor from './ValidationRuleEditor';
import QualityTrendChart from './QualityTrendChart';
import { useQuality } from '../../contexts/QualityContext';
import { QualityRule, QualityIssue } from '../../types/quality';

/**
 * @description Props for the QualityDashboard component
 */
interface QualityDashboardProps {
  /**
   * @description Additional CSS class for styling
   */
  className?: string;
  /**
   * @description Initial active tab index
   */
  initialTab?: number;
  /**
   * @description Interval in milliseconds to refresh data
   */
  refreshInterval?: number;
}

/**
 * @description Main dashboard component that displays data quality metrics, validation results, and issues
 */
const QualityDashboard: React.FC<QualityDashboardProps> = ({
  className,
  initialTab = 0,
  refreshInterval = 60000,
}) => {
  // Access quality context
  const { statistics, loading, error, fetchQualityData, setFilters } = useQuality();

  // State variables
  const [activeTab, setActiveTab] = useState<number>(initialTab || 0);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedIssue, setSelectedIssue] = useState<QualityIssue | null>(null);
  const [editingRule, setEditingRule] = useState<QualityRule | null>(null);
  const [showRuleEditor, setShowRuleEditor] = useState<boolean>(false);

  // Effect to fetch quality data on component mount
  useEffect(() => {
    fetchQualityData();
  }, [fetchQualityData]);

  // Effect to set up refresh interval for quality data
  useEffect(() => {
    const intervalId = setInterval(() => {
      fetchQualityData();
    }, refreshInterval);

    // Clear interval on component unmount
    return () => clearInterval(intervalId);
  }, [refreshInterval, fetchQualityData]);

  /**
   * @description Handles selection of a dataset from the dataset quality table
   * @param {string} dataset
   * @returns {void} No return value
   */
  const handleDatasetSelect = (dataset: string) => {
    setSelectedDataset(dataset); // Set the selected dataset in component state
    setSelectedTable(null); // Clear the selected table state
    setFilters({ dataset, table: null }); // Update filters in the quality context to filter by the selected dataset
    setActiveTab(1); // Switch to the dataset detail tab
  };

  /**
   * @description Handles selection of a table from the dataset detail view
   * @param {string} table
   * @param {string} dataset
   * @returns {void} No return value
   */
  const handleTableSelect = (table: string, dataset: string) => {
    setSelectedTable(table); // Set the selected table in component state
    setFilters({ dataset, table }); // Update filters in the quality context to filter by the selected dataset and table
    setActiveTab(2); // Switch to the table detail tab
  };

  /**
   * @description Handles changing the active tab in the dashboard
   * @param {React.SyntheticEvent} event
   * @param {number} newValue
   * @returns {void} No return value
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue); // Set the active tab index in component state

    if (newValue === 0) {
      setSelectedDataset(null); // If switching to overview tab, clear selected dataset
      setSelectedTable(null);
    }

    setFilters({ dataset: null, table: null }); // Update filters in the quality context based on the selected tab
  };

  /**
   * @description Handles manual refresh of quality data
   * @returns {Promise<void>} Promise that resolves when refresh is complete
   */
  const handleRefresh = async () => {
    try {
      await fetchQualityData(); // Call fetchQualityData from the quality context to reload all quality data
    } catch (err) {
      console.error('Failed to refresh quality data:', err); // Handle any errors that occur during the refresh
    }
  };

  /**
   * @description Handles editing of a validation rule
   * @param {QualityRule} rule
   * @returns {void} No return value
   */
  const handleRuleEdit = (rule: QualityRule) => {
    setEditingRule(rule); // Set the rule to edit in component state
    setShowRuleEditor(true); // Open the rule editor modal
  };

  /**
   * @description Handles adding a new validation rule
   * @returns {void} No return value
   */
  const handleRuleAdd = () => {
    setEditingRule(null); // Set the rule to edit to null (indicating a new rule)
    setShowRuleEditor(true); // Open the rule editor modal
    // Pre-populate dataset and table if they are selected
  };

  /**
   * @description Handles closing the rule editor modal
   * @returns {void} No return value
   */
  const handleRuleEditorClose = () => {
    setShowRuleEditor(false); // Close the rule editor modal
    setEditingRule(null); // Clear the rule to edit state
    fetchQualityData(); // Refresh quality data to show any changes
  };

  /**
   * @description Handles selection of a quality issue
   * @param {QualityIssue} issue
   * @returns {void} No return value
   */
  const handleIssueSelect = (issue: QualityIssue) => {
    setSelectedIssue(issue); // Set the selected issue in component state
    // Open the issue detail modal or panel
  };

  return (
    <Box className={className}>
      {/* Dashboard header with title, refresh button, and add rule button */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="h4">Data Quality Dashboard</Typography>
        <Box>
          <Button variant="outlined" color="primary" onClick={handleRefresh} startIcon={<Refresh />}>
            Refresh
          </Button>
          <Button variant="contained" color="primary" onClick={handleRuleAdd} startIcon={<Add />} sx={{ ml: 2 }}>
            Add Rule
          </Button>
        </Box>
      </Box>

      {/* Tabs for Overview, Dataset Detail, Table Detail, Rules, and Issues */}
      <Tabs value={activeTab} onChange={handleTabChange} aria-label="data quality tabs">
        <Tab label="Overview" />
        {selectedDataset && <Tab label="Dataset Detail" />}
        {selectedTable && selectedDataset && <Tab label="Table Detail" />}
        <Tab label="Rules" />
        <Tab label="Issues" />
      </Tabs>
      <Divider sx={{ mb: 2 }} />

      {/* Render appropriate content based on active tab */}
      {activeTab === 0 && (
        // Overview tab: render overall quality score, quality dimensions, dataset table, and trend chart
        <Grid container spacing={2}>
          <Grid item xs={12} md={4}>
            <Card title="Overall Quality Score">
              <QualityScoreChart score={statistics?.overallScore || 0} isLoading={loading} />
            </Card>
          </Grid>
          <Grid item xs={12} md={8}>
            <QualityDimensionsCard dataset={selectedDataset} table={selectedTable} />
          </Grid>
          <Grid item xs={12}>
            <DatasetQualityTable onDatasetSelect={handleDatasetSelect} />
          </Grid>
          <Grid item xs={12}>
            <QualityTrendChart dataset={selectedDataset} table={selectedTable} />
          </Grid>
        </Grid>
      )}

      {activeTab === 1 && selectedDataset && (
        // Dataset Detail tab: render dataset-specific quality metrics and tables
        <Grid container spacing={2}>
          <Grid item xs={12} md={4}>
            <Card title={`${selectedDataset} Quality Score`}>
              <QualityScoreChart score={statistics?.overallScore || 0} isLoading={loading} />
            </Card>
          </Grid>
          <Grid item xs={12} md={8}>
            <QualityDimensionsCard dataset={selectedDataset} table={selectedTable} />
          </Grid>
          <Grid item xs={12}>
            <ValidationIssuesTable dataset={selectedDataset} onIssueSelect={handleIssueSelect} />
          </Grid>
        </Grid>
      )}

      {activeTab === 2 && selectedTable && selectedDataset && (
        // Table Detail tab: render table-specific quality metrics and issues
        <Grid container spacing={2}>
          <Grid item xs={12} md={4}>
            <Card title={`${selectedTable} Quality Score`}>
              <QualityScoreChart score={statistics?.overallScore || 0} isLoading={loading} />
            </Card>
          </Grid>
          <Grid item xs={12} md={8}>
            <QualityDimensionsCard dataset={selectedDataset} table={selectedTable} />
          </Grid>
          <Grid item xs={12}>
            <ValidationIssuesTable dataset={selectedDataset} table={selectedTable} onIssueSelect={handleIssueSelect} />
          </Grid>
        </Grid>
      )}

      {activeTab === 3 && (
        // Rules tab: render validation rules table with filtering
        <ValidationRulesTable dataset={selectedDataset} table={selectedTable} onRuleEdit={handleRuleEdit} onRuleAdd={handleRuleAdd} />
      )}

      {activeTab === 4 && (
        // Issues tab: render validation issues table with filtering
        <ValidationIssuesTable dataset={selectedDataset} table={selectedTable} onIssueSelect={handleIssueSelect} />
      )}

      {/* Rule editor modal */}
      <ValidationRuleEditor
        rule={editingRule}
        dataset={selectedDataset}
        table={selectedTable}
        onSave={handleRuleEditorClose}
        onCancel={handleRuleEditorClose}
        open={showRuleEditor}
      />
    </Box>
  );
};

export default QualityDashboard;