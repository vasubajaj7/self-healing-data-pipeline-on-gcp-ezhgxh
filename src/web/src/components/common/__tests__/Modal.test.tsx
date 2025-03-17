import React from 'react';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, jest } from '@jest/globals';
import Modal from '../Modal';
import Button from '../Button';
import { renderWithProviders } from '../../../test/utils/renderWithProviders';

// Helper function to set up a rendered Modal with provided props
const setupModal = (props = {}) => {
  // Initialize userEvent for simulating user interactions
  const user = userEvent.setup();
  
  // Default props to use when rendering the Modal
  const defaultProps = {
    open: true,
    title: 'Test Modal',
    onClose: jest.fn(),
  };
  
  // Render modal with merged props using the utility for providing all required contexts
  const renderResult = renderWithProviders(
    <Modal {...defaultProps} {...props} />
  );
  
  return {
    ...renderResult,
    user,
    onClose: props.onClose || defaultProps.onClose,
  };
};

describe('Modal component', () => {
  it('renders correctly with default props', () => {
    const { getByText, getByRole } = setupModal();

    // Check that the modal title renders correctly
    expect(getByText('Test Modal')).toBeInTheDocument();
    
    // Check that the close button is rendered by default
    const closeButton = getByRole('button', { name: /close/i });
    expect(closeButton).toBeInTheDocument();
    
    // Verify the modal has the expected default width class (sm is default)
    const dialogPaper = document.querySelector('.MuiDialog-paper');
    expect(dialogPaper).toHaveClass('MuiDialog-paperWidthSm');
  });

  it('renders with custom title and content', () => {
    const { getByText } = setupModal({
      title: 'Custom Title',
      children: <div>Custom Content</div>
    });

    // Verify custom title is rendered
    expect(getByText('Custom Title')).toBeInTheDocument();
    
    // Verify custom content is rendered
    expect(getByText('Custom Content')).toBeInTheDocument();
  });

  it('renders with custom action buttons', async () => {
    const onSave = jest.fn();
    const onCancel = jest.fn();
    
    const { getByText, user } = setupModal({
      actions: (
        <>
          <Button onClick={onCancel}>Cancel</Button>
          <Button onClick={onSave}>Save</Button>
        </>
      )
    });

    // Verify buttons are rendered
    const saveButton = getByText('Save');
    const cancelButton = getByText('Cancel');
    expect(saveButton).toBeInTheDocument();
    expect(cancelButton).toBeInTheDocument();
    
    // Test button clicks
    await user.click(saveButton);
    expect(onSave).toHaveBeenCalledTimes(1);
    
    await user.click(cancelButton);
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('closes when the close button is clicked', async () => {
    const onClose = jest.fn();
    const { getByRole, user } = setupModal({ onClose });

    // Find and click the close button
    const closeButton = getByRole('button', { name: /close/i });
    await user.click(closeButton);
    
    // Check that onClose was called
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('closes when clicking on the backdrop if not disabled', async () => {
    const onClose = jest.fn();
    const { user } = setupModal({ onClose });

    // Find the backdrop and click it
    const backdrop = document.querySelector('.MuiBackdrop-root');
    expect(backdrop).toBeInTheDocument();
    await user.click(backdrop!);
    
    // Check that onClose was called
    expect(onClose).toHaveBeenCalledTimes(1);
    
    // Reset mock for the next test
    onClose.mockClear();
    
    // Render with disableBackdropClick=true
    const { user: user2 } = setupModal({ 
      onClose, 
      disableBackdropClick: true 
    });
    
    // Find the backdrop again and click it
    const backdrop2 = document.querySelector('.MuiBackdrop-root');
    expect(backdrop2).toBeInTheDocument();
    await user2.click(backdrop2!);
    
    // Check that onClose was NOT called
    expect(onClose).not.toHaveBeenCalled();
  });

  it('closes when pressing Escape key if not disabled', async () => {
    const onClose = jest.fn();
    const { user } = setupModal({ onClose });

    // Press escape key
    await user.keyboard('{Escape}');
    
    // Check that onClose was called
    expect(onClose).toHaveBeenCalledTimes(1);
    
    // Reset mock for the next test
    onClose.mockClear();
    
    // Render with disableEscapeKeyDown=true
    const { user: user2 } = setupModal({ 
      onClose, 
      disableEscapeKeyDown: true 
    });
    
    // Press escape key again
    await user2.keyboard('{Escape}');
    
    // Check that onClose was NOT called
    expect(onClose).not.toHaveBeenCalled();
  });

  it('applies custom maxWidth prop correctly', () => {
    // Test each width option
    const widths = ['xs', 'sm', 'md', 'lg', 'xl'] as const;
    
    widths.forEach(width => {
      const { unmount } = setupModal({ maxWidth: width });
      
      // Check for the specific width class
      const dialogPaper = document.querySelector('.MuiDialog-paper');
      expect(dialogPaper).toHaveClass(`MuiDialog-paperWidth${width.charAt(0).toUpperCase() + width.slice(1)}`);
      
      unmount();
    });
  });

  it('applies custom contentPadding prop correctly', () => {
    const customPadding = '40px';
    const { container } = setupModal({ contentPadding: customPadding });
    
    // Check that the content area has the correct padding
    const dialogContent = container.querySelector('.MuiDialogContent-root');
    expect(dialogContent).toHaveStyle(`padding: ${customPadding}`);
  });

  it('does not render close button when showCloseButton is false', () => {
    const { queryByRole } = setupModal({ showCloseButton: false });
    
    // Check that close button is not in the document
    const closeButton = queryByRole('button', { name: /close/i });
    expect(closeButton).not.toBeInTheDocument();
  });

  it('handles fullWidth prop correctly', () => {
    // Test with fullWidth=true (default)
    const { unmount } = setupModal();
    
    // Check for the fullWidth class
    let dialogPaper = document.querySelector('.MuiDialog-paper');
    expect(dialogPaper).toHaveClass('MuiDialog-paperFullWidth');
    
    unmount();
    
    // Test with fullWidth=false
    const { container } = setupModal({ fullWidth: false });
    
    // Check that the fullWidth class is not applied
    dialogPaper = document.querySelector('.MuiDialog-paper');
    expect(dialogPaper).not.toHaveClass('MuiDialog-paperFullWidth');
  });
});