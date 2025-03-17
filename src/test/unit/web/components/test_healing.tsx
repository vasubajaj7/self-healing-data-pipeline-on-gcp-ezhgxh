# src/test/unit/web/components/test_healing.tsx
```typescript
import React from 'react'; // react ^18.2.0
import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals'; // @jest/globals ^29.5.0
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'; // @testing-library/react ^13.4.0
import userEvent from '@testing-library/user-event'; // @testing-library/user-event ^14.4.3
import HealingDashboard from '../../../../web/src/components/selfHealing/HealingDashboard';
import ActiveIssuesTable from '../../../../web/src/components/selfHealing/ActiveIssuesTable';
import HealingActionsTable from '../../../../web/src/components/selfHealing/HealingActionsTable';
import ModelPerformanceCard from '../../../../web/src/components/selfHealing/ModelPerformanceCard';
import HealingSettingsForm from '../../../../web/src/components/selfHealing/HealingSettingsForm';
import { renderComponent, setupBrowserMocks, createUserEvent, waitForLoadingToComplete, findTableRowByText } from '../../../utils/web_test_utils';
import healingService from '../../../../web/src/services/api/healingService';
import { HealingDashboardData, AIModel, HealingIssue, HealingAction, HealingSettings, HealingMode, ModelType, IssueType, ActionType, HealingStatus, ModelStatus } from '../../../../web/src/types/selfHealing';

// Mock the healingService
jest.mock('../../../../web/src/services/api/healingService');

// Helper function to create mock dashboard data
const createMockDashboardData = (overrides: Partial<HealingDashboardData> = {}): HealingDashboardData => {
  return {
    totalIssuesDetected: 150,
    issuesResolvedAutomatically: 120,
    activeIssues: 30,
    overallSuccessRate: 0.80,
    averageResolutionTime: 3600000,
    issuesByType: {
      DATA_QUALITY: 20,
      PIPELINE_FAILURE: 10,
      SYSTEM_FAILURE: 5,
      PERFORMANCE: 2
    },
    successRateByType: {
      DATA_QUALITY: 0.90,
      PIPELINE_FAILURE: 0.75,
      SYSTEM_FAILURE: 0.60,
      PERFORMANCE: 0.85
    },
    resolutionTimeByType: {
      DATA_QUALITY: 1800000,
      PIPELINE_FAILURE: 7200000,
      SYSTEM_FAILURE: 10800000,
      PERFORMANCE: 3600000
    },
    issuesTrend: [
      { date: '2023-08-01', count: 5 },
      { date: '2023-08-02', count: 8 },
      { date: '2023-08-03', count: 12 }
    ],
    successRateTrend: [
      { date: '2023-08-01', rate: 0.75 },
      { date: '2023-08-02', rate: 0.80 },
      { date: '2023-08-03', rate: 0.85 }
    ],
    modelPerformance: {
      accuracy: 0.95,
      precision: 0.92,
      recall: 0.98
    },
    recentActivities: [
      { activityId: '1', timestamp: '2023-08-03T10:00:00Z', activityType: 'ISSUE_DETECTED', description: 'Data quality issue detected', executionId: '123', healingId: '456', modelId: '789', userId: 'user1', details: {} },
      { activityId: '2', timestamp: '2023-08-03T11:00:00Z', activityType: 'HEALING_STARTED', description: 'Healing process started', executionId: '123', healingId: '456', modelId: '789', userId: 'user1', details: {} }
    ],
    ...overrides,
  };
};

// Helper function to create mock AI models
const createMockAIModels = (): AIModel[] => {
  return [
    {
      modelId: '1',
      name: 'Anomaly Detection Model',
      description: 'Detects anomalies in pipeline execution',
      modelType: ModelType.DETECTION,
      version: 'v1.0',
      status: ModelStatus.ACTIVE,
      accuracy: 0.95,
      lastTrainingDate: '2023-08-01T00:00:00Z',
      trainingDataSize: 10000,
      modelSize: '10MB',
      averageInferenceTime: 0.05,
      metadata: {},
      createdAt: '2023-07-01T00:00:00Z',
      updatedAt: '2023-08-01T00:00:00Z',
    },
    {
      modelId: '2',
      name: 'Data Correction Model',
      description: 'Corrects data quality issues',
      modelType: ModelType.CORRECTION,
      version: 'v2.0',
      status: ModelStatus.ACTIVE,
      accuracy: 0.98,
      lastTrainingDate: '2023-08-02T00:00:00Z',
      trainingDataSize: 20000,
      modelSize: '20MB',
      averageInferenceTime: 0.10,
      metadata: {},
      createdAt: '2023-07-02T00:00:00Z',
      updatedAt: '2023-08-02T00:00:00Z',
    },
  ];
};

// Helper function to create mock healing issues
const createMockHealingIssues = (): HealingIssue[] => {
  return [
    {
      issueId: '1',
      executionId: '123',
      pipelineId: '456',
      pipelineName: 'Customer Data Pipeline',
      issueType: IssueType.DATA_QUALITY,
      component: 'Validation',
      severity: AlertSeverity.HIGH,
      description: 'Null values in required fields',
      detectedAt: '2023-08-03T10:00:00Z',
      details: {},
      status: HealingStatus.IN_PROGRESS,
      healingId: '789',
      suggestedActions: [],
      confidence: 0.95,
    },
    {
      issueId: '2',
      executionId: '124',
      pipelineId: '457',
      pipelineName: 'Sales Data Pipeline',
      issueType: IssueType.PERFORMANCE,
      component: 'Transformation',
      severity: AlertSeverity.MEDIUM,
      description: 'Slow transformation performance',
      detectedAt: '2023-08-03T11:00:00Z',
      details: {},
      status: HealingStatus.COMPLETED,
      healingId: '790',
      suggestedActions: [],
      confidence: 0.80,
    },
  ];
};

// Helper function to create mock healing actions
const createMockHealingActions = (): HealingAction[] => {
  return [
    {
      actionId: '1',
      patternId: '101',
      name: 'Retry Pipeline',
      actionType: ActionType.RETRY,
      description: 'Retries the pipeline execution',
      actionDefinition: {},
      isActive: true,
      successRate: 0.90,
      metadata: {},
      createdAt: '2023-08-01T00:00:00Z',
      updatedAt: '2023-08-01T00:00:00Z',
    },
    {
      actionId: '2',
      patternId: '102',
      name: 'Apply Data Correction',
      actionType: ActionType.DATA_CORRECTION,
      description: 'Applies a data correction to fix data quality issues',
      actionDefinition: {},
      isActive: true,
      successRate: 0.95,
      metadata: {},
      createdAt: '2023-08-02T00:00:00Z',
      updatedAt: '2023-08-02T00:00:00Z',
    },
  ];
};

// Helper function to create mock healing settings
const createMockHealingSettings = (overrides: Partial<HealingSettings> = {}): HealingSettings => {
  return {
    healingMode: HealingMode.SEMI_AUTOMATIC,
    globalConfidenceThreshold: 85,
    maxRetryAttempts: 3,
    approvalRequiredHighImpact: true,
    learningModeActive: true,
    additionalSettings: {},
    updatedAt: '2023-08-03T00:00:00Z',
    updatedBy: 'user1',
    ...overrides,
  };
};

describe('HealingDashboard', () => {
  it('renders the dashboard with loading state initially', async () => {
    // Mock healingService.getDashboardData to return a promise that doesn't resolve immediately
    (healingService.getDashboardData as jest.Mock).mockReturnValue(new Promise(() => {}));
    // Mock healingService.getAIModels to return a promise that doesn't resolve immediately
    (healingService.getAIModels as jest.Mock).mockReturnValue(new Promise(() => {}));

    // Render the HealingDashboard component
    renderComponent(<HealingDashboard />);

    // Verify loading indicators are displayed
    expect(screen.getByText(/Loading/i)).toBeInTheDocument();
  });

  it('displays dashboard data when loaded', async () => {
    // Create mock dashboard data
    const mockDashboardData = createMockDashboardData();

    // Mock API services to resolve with mock data
    (healingService.getDashboardData as jest.Mock).mockResolvedValue(mockDashboardData);
    (healingService.getAIModels as jest.Mock).mockResolvedValue(createMockAIModels());

    // Render the HealingDashboard component
    renderComponent(<HealingDashboard />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Verify key metrics are displayed correctly
    expect(screen.getByText(/Total Issues Detected/i)).toBeInTheDocument();
    expect(screen.getByText(mockDashboardData.totalIssuesDetected.toString())).toBeInTheDocument();
    expect(screen.getByText(/Issues Resolved Automatically/i)).toBeInTheDocument();
    expect(screen.getByText(mockDashboardData.issuesResolvedAutomatically.toString())).toBeInTheDocument();
    expect(screen.getByText(/Overall Success Rate/i)).toBeInTheDocument();

    // Verify success rate is formatted properly
    expect(screen.getByText('80.0%')).toBeInTheDocument();

    // Verify resolution time is formatted properly
    expect(screen.getByText(/Average Resolution Time/i)).toBeInTheDocument();
    expect(screen.getByText('1h 0m 0s')).toBeInTheDocument();
  });

  it('handles tab switching correctly', async () => {
    // Create mock dashboard data and models
    const mockDashboardData = createMockDashboardData();
    const mockAIModels = createMockAIModels();

    // Mock API services to resolve with mock data
    (healingService.getDashboardData as jest.Mock).mockResolvedValue(mockDashboardData);
    (healingService.getAIModels as jest.Mock).mockResolvedValue(mockAIModels);

    // Render the HealingDashboard component
    renderComponent(<HealingDashboard />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Click on different tabs
    const issuesTab = screen.getByRole('tab', { name: /Issues/i });
    const actionsTab = screen.getByRole('tab', { name: /Actions/i });
    const performanceTab = screen.getByRole('tab', { name: /Performance/i });
    const modelsTab = screen.getByRole('tab', { name: /Models/i });

    await userEvent.click(issuesTab);
    expect(screen.getByText(/Active Issues/i)).toBeInTheDocument();

    await userEvent.click(actionsTab);
    expect(screen.getByText(/Healing Actions/i)).toBeInTheDocument();

    await userEvent.click(performanceTab);
    expect(screen.getByText(/Self-Healing Success Rate/i)).toBeInTheDocument();

    await userEvent.click(modelsTab);
    expect(screen.getByText(/Detection Model Performance/i)).toBeInTheDocument();
  });

  it('displays error state when API fails', async () => {
    // Mock healingService.getDashboardData to reject with an error
    (healingService.getDashboardData as jest.Mock).mockRejectedValue(new Error('API Error'));

    // Render the HealingDashboard component
    renderComponent(<HealingDashboard />);

    // Verify error message is displayed
    await waitFor(() => expect(screen.getByText(/Error fetching data/i)).toBeInTheDocument());
  });

  it('refreshes data at specified interval', async () => {
    // Mock Date.now for consistent timing
    jest.spyOn(Date, 'now').mockImplementation(() => 1672531200000);

    // Create mock dashboard data
    const mockDashboardData = createMockDashboardData();

    // Mock healingService.getDashboardData to resolve with mock data
    (healingService.getDashboardData as jest.Mock).mockResolvedValue(mockDashboardData);
    (healingService.getAIModels as jest.Mock).mockResolvedValue(createMockAIModels());

    // Render the HealingDashboard with a short refreshInterval
    renderComponent(<HealingDashboard refreshInterval={1000} />);

    // Wait for initial loading to complete
    await waitForLoadingToComplete();

    // Advance timers to trigger refresh
    jest.advanceTimersByTime(1000);

    // Verify getDashboardData is called again
    expect(healingService.getDashboardData).toHaveBeenCalledTimes(2);

    // Restore Date.now mock
    (Date.now as jest.Mock).mockRestore();
  });
});

describe('ActiveIssuesTable', () => {
  it('renders the table with loading state initially', async () => {
    // Mock healingService.getHealingIssues to return a promise that doesn't resolve immediately
    (healingService.getHealingIssues as jest.Mock).mockReturnValue(new Promise(() => {}));

    // Render the ActiveIssuesTable component
    renderComponent(<ActiveIssuesTable />);

    // Verify loading indicators are displayed
    expect(screen.getByText(/Loading/i)).toBeInTheDocument();
  });

  it('displays issues when loaded', async () => {
    // Create mock healing issues
    const mockHealingIssues = createMockHealingIssues();

    // Mock healingService.getHealingIssues to resolve with the mock issues
    (healingService.getHealingIssues as jest.Mock).mockResolvedValue({ items: mockHealingIssues, pagination: { page: 1, pageSize: 10, totalItems: mockHealingIssues.length, totalPages: 1 } });

    // Render the ActiveIssuesTable component
    renderComponent(<ActiveIssuesTable />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Verify issues are displayed in the table
    expect(screen.getByText(/Null values in required fields/i)).toBeInTheDocument();
    expect(screen.getByText(/Slow transformation performance/i)).toBeInTheDocument();

    // Verify issue details like type, severity, and status are displayed correctly
    expect(screen.getByText(/Data Quality/i)).toBeInTheDocument();
    expect(screen.getByText(/High/i)).toBeInTheDocument();
    expect(screen.getByText(/In Progress/i)).toBeInTheDocument();
  });

  it('handles pagination correctly', async () => {
    // Create many mock healing issues
    const mockHealingIssues = Array.from({ length: 25 }, (_, i) => ({
      issueId: `${i + 1}`,
      executionId: '123',
      pipelineId: '456',
      pipelineName: 'Test Pipeline',
      issueType: IssueType.DATA_QUALITY,
      component: 'Validation',
      severity: AlertSeverity.HIGH,
      description: `Test Issue ${i + 1}`,
      detectedAt: '2023-08-03T10:00:00Z',
      details: {},
      status: HealingStatus.IN_PROGRESS,
      healingId: '789',
      suggestedActions: [],
      confidence: 0.95,
    }));

    // Mock healingService.getHealingIssues to resolve with the mock issues
    (healingService.getHealingIssues as jest.Mock).mockResolvedValue({ items: mockHealingIssues.slice(0, 10), pagination: { page: 1, pageSize: 10, totalItems: mockHealingIssues.length, totalPages: 3 } });

    // Render the ActiveIssuesTable with showPagination=true
    renderComponent(<ActiveIssuesTable showPagination={true} />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Verify pagination controls are displayed
    expect(screen.getByRole('button', { name: /Go to next page/i })).toBeInTheDocument();

    // Click next page button
    await userEvent.click(screen.getByRole('button', { name: /Go to next page/i }));

    // Verify getHealingIssues is called with updated page parameter
    expect(healingService.getHealingIssues).toHaveBeenCalledWith(expect.objectContaining({ page: 2, pageSize: 10 }));
  });

  it('opens manual healing dialog when heal button is clicked', async () => {
    // Create mock healing issues with suggested actions
    const mockHealingIssues = [
      {
        issueId: '1',
        executionId: '123',
        pipelineId: '456',
        pipelineName: 'Customer Data Pipeline',
        issueType: IssueType.DATA_QUALITY,
        component: 'Validation',
        severity: AlertSeverity.HIGH,
        description: 'Null values in required fields',
        detectedAt: '2023-08-03T10:00:00Z',
        details: {},
        status: HealingStatus.IN_PROGRESS,
        healingId: '789',
        suggestedActions: [{ actionId: '1', name: 'Apply Imputation' } as any],
        confidence: 0.95,
      },
    ];

    // Mock healingService.getHealingIssues to resolve with the mock issues
    (healingService.getHealingIssues as jest.Mock).mockResolvedValue({ items: mockHealingIssues, pagination: { page: 1, pageSize: 10, totalItems: mockHealingIssues.length, totalPages: 1 } });

    // Render the ActiveIssuesTable component
    renderComponent(<ActiveIssuesTable />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Find and click the heal button for an issue
    const healButton = screen.getByRole('button', { name: /Heal Issue/i });
    await userEvent.click(healButton);

    // Verify the manual healing dialog is displayed
    expect(screen.getByRole('dialog', { name: /Manual Healing/i })).toBeInTheDocument();

    // Verify issue details are shown in the dialog
    expect(screen.getByText(/Null values in required fields/i)).toBeInTheDocument();
  });

  it('triggers manual healing when confirmed in dialog', async () => {
    // Create mock healing issues with suggested actions
    const mockHealingIssues = [
      {
        issueId: '1',
        executionId: '123',
        pipelineId: '456',
        pipelineName: 'Customer Data Pipeline',
        issueType: IssueType.DATA_QUALITY,
        component: 'Validation',
        severity: AlertSeverity.HIGH,
        description: 'Null values in required fields',
        detectedAt: '2023-08-03T10:00:00Z',
        details: {},
        status: HealingStatus.IN_PROGRESS,
        healingId: '789',
        suggestedActions: [{ actionId: '1', name: 'Apply Imputation' } as any],
        confidence: 0.95,
      },
    ];

    // Mock healingService.getHealingIssues to resolve with the mock issues
    (healingService.getHealingIssues as jest.Mock).mockResolvedValue({ items: mockHealingIssues, pagination: { page: 1, pageSize: 10, totalItems: mockHealingIssues.length, totalPages: 1 } });

    // Mock healingService.triggerManualHealing to resolve successfully
    (healingService.triggerManualHealing as jest.Mock).mockResolvedValue({});

    // Render the ActiveIssuesTable component
    renderComponent(<ActiveIssuesTable />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Find and click the heal button for an issue
    const healButton = screen.getByRole('button', { name: /Heal Issue/i });
    await userEvent.click(healButton);

    // Select an action in the dialog
    const actionSelect = screen.getByLabelText(/Select Healing Action/i);
    await userEvent.selectOptions(actionSelect, '1');

    // Click the confirm button
    const confirmButton = screen.getByRole('button', { name: /Confirm Healing/i });
    await userEvent.click(confirmButton);

    // Verify triggerManualHealing is called with correct parameters
    expect(healingService.triggerManualHealing).toHaveBeenCalledWith(expect.anything(), {
      issueId: '1',
      actionId: '1',
      parameters: {},
      notes: 'Triggered from Active Issues Table',
    });

    // Verify dialog is closed after successful healing
    await waitFor(() => expect(screen.queryByRole('dialog', { name: /Manual Healing/i })).not.toBeInTheDocument());
  });
});

describe('HealingSettingsForm', () => {
  it('renders the form with loading state initially', async () => {
    // Mock healingService.getHealingSettings to return a promise that doesn't resolve immediately
    (healingService.getHealingSettings as jest.Mock).mockReturnValue(new Promise(() => {}));

    // Render the HealingSettingsForm component
    renderComponent(<HealingSettingsForm />);

    // Verify loading indicators are displayed
    expect(screen.getByText(/Loading/i)).toBeInTheDocument();
  });

  it('displays settings when loaded', async () => {
    // Create mock healing settings
    const mockHealingSettings = createMockHealingSettings();

    // Mock healingService.getHealingSettings to resolve with the mock settings
    (healingService.getHealingSettings as jest.Mock).mockResolvedValue(mockHealingSettings);

    // Render the HealingSettingsForm component
    renderComponent(<HealingSettingsForm />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Verify form fields display the correct values from settings
    expect(screen.getByLabelText(/Confidence Threshold/i)).toHaveValue(mockHealingSettings.globalConfidenceThreshold);

    // Verify healing mode dropdown shows correct value
    const healingModeDropdown = screen.getByLabelText(/Healing Mode/i);
    expect(healingModeDropdown).toHaveTextContent(/Semi-Automatic/i);

    // Verify approval required checkbox is checked
    const approvalRequiredCheckbox = screen.getByLabelText(/Approval Required for High Impact Fixes/i);
    expect(approvalRequiredCheckbox).toBeChecked();
  });

  it('updates settings when form is submitted', async () => {
    // Create mock healing settings
    const mockHealingSettings = createMockHealingSettings();

    // Mock healingService.getHealingSettings to resolve with the mock settings
    (healingService.getHealingSettings as jest.Mock).mockResolvedValue(mockHealingSettings);

    // Mock healingService.updateHealingSettings to resolve successfully
    (healingService.updateHealingSettings as jest.Mock).mockResolvedValue(mockHealingSettings);

    // Render the HealingSettingsForm component
    renderComponent(<HealingSettingsForm />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Change healing mode dropdown value
    const healingModeDropdown = screen.getByLabelText(/Healing Mode/i);
    await userEvent.selectOptions(healingModeDropdown, HealingMode.AUTOMATIC);

    // Change confidence threshold slider value
    const confidenceThresholdInput = screen.getByLabelText(/Confidence Threshold/i);
    await fireEvent.change(confidenceThresholdInput, { target: { value: 90 } });

    // Toggle approval required checkbox
    const approvalRequiredCheckbox = screen.getByLabelText(/Approval Required for High Impact Fixes/i);
    await userEvent.click(approvalRequiredCheckbox);

    // Click save button
    const saveButton = screen.getByRole('button', { name: /Save Settings/i });
    await userEvent.click(saveButton);

    // Verify updateHealingSettings is called with updated values
    expect(healingService.updateHealingSettings).toHaveBeenCalledWith(expect.objectContaining({
      healingMode: HealingMode.AUTOMATIC,
      globalConfidenceThreshold: 90,
      approvalRequiredHighImpact: false,
    }));

    // Verify success message is displayed after saving
    await waitFor(() => expect(screen.getByText(/Healing settings updated successfully!/i)).toBeInTheDocument());
  });

  it('displays error when settings update fails', async () => {
    // Create mock healing settings
    const mockHealingSettings = createMockHealingSettings();

    // Mock healingService.getHealingSettings to resolve with the mock settings
    (healingService.getHealingSettings as jest.Mock).mockResolvedValue(mockHealingSettings);

    // Mock healingService.updateHealingSettings to reject with an error
    (healingService.updateHealingSettings as jest.Mock).mockRejectedValue(new Error('Update failed'));

    // Render the HealingSettingsForm component
    renderComponent(<HealingSettingsForm />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Make changes to the form
    const healingModeDropdown = screen.getByLabelText(/Healing Mode/i);
    await userEvent.selectOptions(healingModeDropdown, HealingMode.AUTOMATIC);

    // Click save button
    const saveButton = screen.getByRole('button', { name: /Save Settings/i });
    await userEvent.click(saveButton);

    // Verify error message is displayed
    await waitFor(() => expect(screen.getByText(/Failed to update healing settings/i)).toBeInTheDocument());
  });

  it('validates form inputs before submission', async () => {
    // Create mock healing settings
    const mockHealingSettings = createMockHealingSettings();

    // Mock healingService.getHealingSettings to resolve with the mock settings
    (healingService.getHealingSettings as jest.Mock).mockResolvedValue(mockHealingSettings);

    // Render the HealingSettingsForm component
    renderComponent(<HealingSettingsForm />);

    // Wait for loading to complete
    await waitForLoadingToComplete();

    // Set invalid values in form fields
    const confidenceThresholdInput = screen.getByLabelText(/Confidence Threshold/i);
    await fireEvent.change(confidenceThresholdInput, { target: { value: -10 } });

    // Click save button
    const saveButton = screen.getByRole('button', { name: /Save Settings/i });
    await userEvent.click(saveButton);

    // Verify validation error messages are displayed
    expect(screen.getByText(/Value must be at least 0/i)).toBeInTheDocument();

    // Verify updateHealingSettings is not called
    expect(healingService.updateHealingSettings).not.toHaveBeenCalled();
  });
});