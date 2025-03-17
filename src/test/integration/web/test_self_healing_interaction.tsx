import React from 'react'; // react ^18.2.0
import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals'; // @jest/globals ^29.5.0
import { screen, waitFor, within, fireEvent } from '@testing-library/react'; // @testing-library/react ^13.4.0
import {
  renderComponent,
  createUserEvent,
  waitForLoadingToComplete,
  findTableRowByText,
} from '../../utils/web_test_utils';
import { mockHealingService, MOCK_HEALING_DATA, createMockDataResponse } from '../fixtures/web/api_fixtures';
import HealingDashboard from '../../../web/src/components/selfHealing/HealingDashboard';
import HealingSettingsForm from '../../../web/src/components/selfHealing/HealingSettingsForm';
import healingService from '../../../web/src/services/api/healingService';
import { HealingMode } from '../../../web/src/types/selfHealing';

describe('Self-Healing Integration Tests', () => {
  let mockService: any;

  beforeEach(() => {
    mockService = mockHealingService();
    jest.spyOn(healingService, 'getAIModels').mockImplementation(mockService.getModels);
    jest.spyOn(healingService, 'getHealingSettings').mockImplementation(mockService.getSettings);
    jest.spyOn(healingService, 'updateHealingSettings').mockImplementation(mockService.updateSettings);
    jest.spyOn(healingService, 'getHealingIssues').mockImplementation(mockService.getIssues);
  });

  afterEach(() => {
    mockService.reset();
    jest.restoreAllMocks();
  });

  it('should load and display the healing dashboard with metrics', async () => {
    renderComponent(<HealingDashboard />);

    expect(screen.getByText('Loading...')).toBeInTheDocument();

    await waitForLoadingToComplete();

    expect(screen.getByText('Self-Healing Overview')).toBeInTheDocument();
    expect(screen.getByText(MOCK_HEALING_DATA.executions.length.toString())).toBeInTheDocument();
    expect(screen.getByText('92.5%')).toBeInTheDocument();
    expect(screen.getByText('Active Issues')).toBeInTheDocument();
  });

  it('should allow switching between dashboard tabs', async () => {
    renderComponent(<HealingDashboard />);

    await waitForLoadingToComplete();

    const issuesTab = screen.getByRole('tab', { name: /Issues/i });
    const actionsTab = screen.getByRole('tab', { name: /Actions/i });
    const performanceTab = screen.getByRole('tab', { name: /Performance/i });
    const modelsTab = screen.getByRole('tab', { name: /Models/i });

    await fireEvent.click(issuesTab);
    expect(screen.getByText('Active Issues')).toBeInTheDocument();

    await fireEvent.click(actionsTab);
    expect(screen.getByText('Healing Actions')).toBeInTheDocument();

    await fireEvent.click(performanceTab);
    expect(screen.getByText('Self-Healing Success Rate')).toBeInTheDocument();

    await fireEvent.click(modelsTab);
    expect(screen.getByText('AI Models Overview')).toBeInTheDocument();
  });

  it('should display issue details when clicking on an issue', async () => {
    renderComponent(<HealingDashboard />);

    await waitForLoadingToComplete();

    const issueRow = findTableRowByText('Negative price values in product catalog');
    await fireEvent.click(issueRow);

    await waitFor(() => {
      expect(screen.getByText('Issue Details')).toBeInTheDocument();
      expect(screen.getByText('Negative price values in product catalog')).toBeInTheDocument();
    });
  });

  it('should allow triggering manual healing for an issue', async () => {
    const triggerManualHealingMock = jest.spyOn(healingService, 'triggerManualHealing');
    renderComponent(<HealingDashboard />);

    await waitForLoadingToComplete();

    const issueRow = findTableRowByText('Negative price values in product catalog');
    await fireEvent.click(issueRow);

    const healButton = await screen.findByRole('button', { name: /Confirm Healing/i });
    await fireEvent.click(healButton);

    expect(triggerManualHealingMock).toHaveBeenCalled();
  });

  it('should load and display healing settings correctly', async () => {
    renderComponent(<HealingSettingsForm />);

    await waitForLoadingToComplete();

    expect(screen.getByLabelText('Healing Mode')).toBeInTheDocument();
    expect(screen.getByLabelText('Confidence Threshold (%)')).toBeInTheDocument();
    expect(screen.getByLabelText('Max Retry Attempts')).toBeInTheDocument();
  });

  it('should save updated healing settings', async () => {
    const updateHealingSettingsMock = jest.spyOn(healingService, 'updateHealingSettings');
    renderComponent(<HealingSettingsForm />);

    await waitForLoadingToComplete();

    const healingModeSelect = screen.getByLabelText('Healing Mode');
    await fireEvent.change(healingModeSelect, { target: { value: HealingMode.AUTOMATIC } });

    const confidenceThresholdInput = screen.getByLabelText('Confidence Threshold (%)');
    await fireEvent.change(confidenceThresholdInput, { target: { value: '90' } });

    const saveButton = screen.getByRole('button', { name: /Save Settings/i });
    await fireEvent.click(saveButton);

    expect(updateHealingSettingsMock).toHaveBeenCalled();
  });

  it('should display and manage AI models correctly', async () => {
    renderComponent(<HealingDashboard showSettings />);

    await waitForLoadingToComplete();

    const modelsTab = screen.getByRole('tab', { name: /Models/i });
    await fireEvent.click(modelsTab);

    expect(screen.getByText('AI Models Overview')).toBeInTheDocument();
    expect(screen.getByText('Email Correction Model')).toBeInTheDocument();
    expect(screen.getByText('Price Anomaly Detector')).toBeInTheDocument();
  });
});