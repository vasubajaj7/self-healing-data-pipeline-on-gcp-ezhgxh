import { useSnackbar, OptionsObject, SnackbarKey } from 'notistack'; // notistack ^3.0.0
import { useCallback } from 'react'; // react ^18.2.0

/**
 * Extended options for notifications beyond notistack defaults
 */
export interface NotificationOptions extends Partial<OptionsObject> {
  /**
   * Whether the notification should persist until manually closed
   */
  persist?: boolean;
  
  /**
   * Optional title for the notification
   */
  title?: string;
  
  /**
   * Whether to prevent duplicate notifications with the same message
   */
  preventDuplicate?: boolean;
}

/**
 * Custom hook that provides methods for displaying different types of notifications
 * @returns Object containing notification methods (showSuccess, showError, showWarning, showInfo, closeNotification)
 */
export const useNotification = () => {
  const { enqueueSnackbar, closeSnackbar } = useSnackbar();
  
  const defaultOptions: Partial<OptionsObject> = {
    autoHideDuration: 5000,
    anchorOrigin: {
      vertical: 'top',
      horizontal: 'right',
    },
    preventDuplicate: true,
    dense: false,
    disableWindowBlurListener: false,
  };
  
  /**
   * Displays a success notification
   * @param message Message to display in the notification
   * @param options Additional options for the notification
   * @returns Unique key for the displayed notification
   */
  const showSuccess = useCallback(
    (message: string, options?: NotificationOptions): SnackbarKey => {
      return enqueueSnackbar(message, {
        ...defaultOptions,
        ...options,
        variant: 'success',
      });
    },
    [enqueueSnackbar]
  );
  
  /**
   * Displays an error notification
   * @param message Error message to display in the notification
   * @param options Additional options for the notification
   * @returns Unique key for the displayed notification
   */
  const showError = useCallback(
    (message: string, options?: NotificationOptions): SnackbarKey => {
      return enqueueSnackbar(message, {
        ...defaultOptions,
        ...options,
        variant: 'error',
      });
    },
    [enqueueSnackbar]
  );
  
  /**
   * Displays a warning notification
   * @param message Warning message to display in the notification
   * @param options Additional options for the notification
   * @returns Unique key for the displayed notification
   */
  const showWarning = useCallback(
    (message: string, options?: NotificationOptions): SnackbarKey => {
      return enqueueSnackbar(message, {
        ...defaultOptions,
        ...options,
        variant: 'warning',
      });
    },
    [enqueueSnackbar]
  );
  
  /**
   * Displays an informational notification
   * @param message Informational message to display in the notification
   * @param options Additional options for the notification
   * @returns Unique key for the displayed notification
   */
  const showInfo = useCallback(
    (message: string, options?: NotificationOptions): SnackbarKey => {
      return enqueueSnackbar(message, {
        ...defaultOptions,
        ...options,
        variant: 'info',
      });
    },
    [enqueueSnackbar]
  );
  
  /**
   * Manually closes a specific notification or all notifications
   * @param key Key of the notification to close, or undefined to close all
   */
  const closeNotification = useCallback(
    (key?: SnackbarKey): void => {
      closeSnackbar(key);
    },
    [closeSnackbar]
  );
  
  return {
    showSuccess,
    showError,
    showWarning,
    showInfo,
    closeNotification,
  };
};

export default useNotification;