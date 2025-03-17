import numeral from 'numeral'; // numeral.js v2.0.6 - Number formatting library
import filesize from 'filesize'; // filesize v10.0.7 - File size formatting utility

/**
 * Formats a number with thousand separators and optional decimal places
 * @param value The number to format
 * @param decimalPlaces The number of decimal places to show
 * @returns Formatted number string or empty string if input is invalid
 */
export function formatNumber(value: number | string | null | undefined, decimalPlaces: number = 2): string {
  // Check if value is null, undefined, or NaN
  if (value === null || value === undefined || (typeof value === 'number' && isNaN(value))) {
    return '';
  }

  // Convert string to number if needed
  const numValue = typeof value === 'string' ? parseFloat(value) : value;
  
  // Check if conversion resulted in NaN
  if (isNaN(numValue)) {
    return '';
  }

  // Format with numeral.js using appropriate format based on decimal places
  const format = decimalPlaces === 0 ? '0,0' : `0,0.${'0'.repeat(decimalPlaces)}`;
  return numeral(numValue).format(format);
}

/**
 * Formats a number as an integer with thousand separators
 * @param value The number to format
 * @returns Formatted integer string or empty string if input is invalid
 */
export function formatInteger(value: number | string | null | undefined): string {
  return formatNumber(value, 0);
}

/**
 * Formats a number with 2 decimal places and thousand separators
 * @param value The number to format
 * @returns Formatted decimal string or empty string if input is invalid
 */
export function formatDecimal(value: number | string | null | undefined): string {
  return formatNumber(value, 2);
}

/**
 * Formats a number as a percentage with optional decimal places
 * @param value The number to format (0.75 or 75 will both display as 75%)
 * @param decimalPlaces The number of decimal places to show
 * @returns Formatted percentage string or empty string if input is invalid
 */
export function formatPercentage(value: number | string | null | undefined, decimalPlaces: number = 1): string {
  // Check if value is null, undefined, or NaN
  if (value === null || value === undefined || (typeof value === 'number' && isNaN(value))) {
    return '';
  }

  // Convert string to number if needed
  let numValue = typeof value === 'string' ? parseFloat(value) : value;
  
  // Check if conversion resulted in NaN
  if (isNaN(numValue)) {
    return '';
  }

  // If value is between 0 and 1, assume it's a decimal percentage (0.75 = 75%)
  if (numValue > 0 && numValue < 1) {
    numValue = numValue * 100;
  }

  // Format with numeral.js using appropriate format based on decimal places
  const format = decimalPlaces === 0 ? '0,0' : `0,0.${'0'.repeat(decimalPlaces)}`;
  return numeral(numValue).format(format) + '%';
}

/**
 * Formats a byte size into a human-readable file size string
 * @param bytes The size in bytes
 * @returns Human-readable file size or empty string if input is invalid
 */
export function formatFileSize(bytes: number | string | null | undefined): string {
  // Check if bytes is null, undefined, or NaN
  if (bytes === null || bytes === undefined || (typeof bytes === 'number' && isNaN(bytes))) {
    return '';
  }

  // Convert string to number if needed
  const numBytes = typeof bytes === 'string' ? parseFloat(bytes) : bytes;
  
  // Check if conversion resulted in NaN
  if (isNaN(numBytes)) {
    return '';
  }

  // Use filesize library to format bytes into human-readable format
  return filesize(numBytes, {standard: 'jedec', round: 1});
}

/**
 * Formats a duration in milliseconds into a human-readable string
 * @param milliseconds The duration in milliseconds
 * @returns Human-readable duration or empty string if input is invalid
 */
export function formatDuration(milliseconds: number | string | null | undefined): string {
  // Check if milliseconds is null, undefined, or NaN
  if (milliseconds === null || milliseconds === undefined || (typeof milliseconds === 'number' && isNaN(milliseconds))) {
    return '';
  }

  // Convert string to number if needed
  const ms = typeof milliseconds === 'string' ? parseFloat(milliseconds) : milliseconds;
  
  // Check if conversion resulted in NaN
  if (isNaN(ms)) {
    return '';
  }

  // Handle negative durations
  const absMs = Math.abs(ms);
  
  // Calculate hours, minutes, and seconds
  const hours = Math.floor(absMs / 3600000);
  const minutes = Math.floor((absMs % 3600000) / 60000);
  const seconds = Math.floor((absMs % 60000) / 1000);
  
  // Format the duration string
  let result = '';
  
  if (hours > 0) {
    result += `${hours}h `;
  }
  
  if (minutes > 0 || hours > 0) {
    result += `${minutes}m `;
  }
  
  result += `${seconds}s`;
  
  // Add negative sign if needed
  return ms < 0 ? `-${result}` : result;
}

/**
 * Formats a duration in milliseconds into a compact human-readable string (HH:MM:SS)
 * @param milliseconds The duration in milliseconds
 * @returns Compact human-readable duration or empty string if input is invalid
 */
export function formatDurationShort(milliseconds: number | string | null | undefined): string {
  // Check if milliseconds is null, undefined, or NaN
  if (milliseconds === null || milliseconds === undefined || (typeof milliseconds === 'number' && isNaN(milliseconds))) {
    return '';
  }

  // Convert string to number if needed
  const ms = typeof milliseconds === 'string' ? parseFloat(milliseconds) : milliseconds;
  
  // Check if conversion resulted in NaN
  if (isNaN(ms)) {
    return '';
  }

  // Handle negative durations
  const absMs = Math.abs(ms);
  
  // Calculate hours, minutes, and seconds
  const hours = Math.floor(absMs / 3600000);
  const minutes = Math.floor((absMs % 3600000) / 60000);
  const seconds = Math.floor((absMs % 60000) / 1000);
  
  // Format the duration string with leading zeros
  const formattedHours = hours.toString().padStart(2, '0');
  const formattedMinutes = minutes.toString().padStart(2, '0');
  const formattedSeconds = seconds.toString().padStart(2, '0');
  
  // Format as HH:MM:SS
  const result = `${formattedHours}:${formattedMinutes}:${formattedSeconds}`;
  
  // Add negative sign if needed
  return ms < 0 ? `-${result}` : result;
}

/**
 * Formats a number as currency with the specified currency symbol
 * @param value The number to format
 * @param currencySymbol The currency symbol to use (default: $)
 * @returns Formatted currency string or empty string if input is invalid
 */
export function formatCurrency(value: number | string | null | undefined, currencySymbol: string = '$'): string {
  // Check if value is null, undefined, or NaN
  if (value === null || value === undefined || (typeof value === 'number' && isNaN(value))) {
    return '';
  }

  // Convert string to number if needed
  const numValue = typeof value === 'string' ? parseFloat(value) : value;
  
  // Check if conversion resulted in NaN
  if (isNaN(numValue)) {
    return '';
  }

  // Format the number with 2 decimal places
  const formattedValue = formatNumber(numValue, 2);
  
  // Prepend the currency symbol
  return `${currencySymbol}${formattedValue}`;
}

/**
 * Formats a metric name for display in the UI by converting from snake_case or kebab-case to Title Case
 * @param metricName The metric name to format
 * @returns Formatted metric name
 */
export function formatMetricName(metricName: string): string {
  if (!metricName) {
    return '';
  }
  
  // Replace underscores and hyphens with spaces
  let formattedName = metricName.replace(/[_-]/g, ' ');
  
  // Capitalize each word
  formattedName = formattedName.replace(/\w\S*/g, (word) => {
    return word.charAt(0).toUpperCase() + word.substring(1).toLowerCase();
  });
  
  // Apply special formatting for specific metrics
  formattedName = formattedName
    .replace(/\bCpu\b/g, 'CPU')
    .replace(/\bIo\b/g, 'IO')
    .replace(/\bApi\b/g, 'API')
    .replace(/\bId\b/g, 'ID')
    .replace(/\bUrl\b/g, 'URL')
    .replace(/\bBq\b/g, 'BigQuery')
    .replace(/\bGcs\b/g, 'GCS')
    .replace(/\bSla\b/g, 'SLA')
    .replace(/\bSlo\b/g, 'SLO');
  
  return formattedName;
}

/**
 * Truncates text to a specified length and adds ellipsis if needed
 * @param text The text to truncate
 * @param maxLength The maximum length of the returned string (including ellipsis)
 * @returns Truncated text or empty string if input is invalid
 */
export function truncateText(text: string | null | undefined, maxLength: number = 100): string {
  // Check if text is null, undefined, or empty
  if (!text) {
    return '';
  }
  
  // If text is shorter than maxLength, return it as is
  if (text.length <= maxLength) {
    return text;
  }
  
  // Truncate text and add ellipsis
  return `${text.substring(0, maxLength - 3)}...`;
}

/**
 * Formats a status string for consistent display in the UI
 * @param status The status string to format
 * @returns Formatted status string or empty string if input is invalid
 */
export function formatStatus(status: string | null | undefined): string {
  // Check if status is null, undefined, or empty
  if (!status) {
    return '';
  }
  
  // Convert status to lowercase for consistent handling
  const statusLower = status.toLowerCase();
  
  // Map of status values to their formatted display versions
  const statusMap: Record<string, string> = {
    'running': 'Running',
    'completed': 'Completed',
    'succeeded': 'Succeeded',
    'failed': 'Failed',
    'error': 'Error',
    'pending': 'Pending',
    'waiting': 'Waiting',
    'cancelled': 'Cancelled',
    'timeout': 'Timeout',
    'skipped': 'Skipped',
    'warning': 'Warning',
    'success': 'Success',
  };
  
  // Return the mapped status or a capitalized version of the input
  return statusMap[statusLower] || 
    (statusLower.charAt(0).toUpperCase() + statusLower.slice(1));
}

/**
 * Formats a number with thousand separators (no decimal places)
 * @param value The number to format
 * @returns Formatted number with thousand separators or empty string if input is invalid
 */
export function formatThousands(value: number | string | null | undefined): string {
  return formatNumber(value, 0);
}

/**
 * Formats a rate value (e.g., requests per second) with the specified unit
 * @param value The rate value to format
 * @param unit The unit to append (e.g., '/s', '/min')
 * @returns Formatted rate string or empty string if input is invalid
 */
export function formatRate(value: number | string | null | undefined, unit: string = '/s'): string {
  // Check if value is null, undefined, or NaN
  if (value === null || value === undefined || (typeof value === 'number' && isNaN(value))) {
    return '';
  }

  // Convert string to number if needed
  const numValue = typeof value === 'string' ? parseFloat(value) : value;
  
  // Check if conversion resulted in NaN
  if (isNaN(numValue)) {
    return '';
  }
  
  // Format the number with appropriate decimal places (1 for small numbers, 0 for larger numbers)
  const formattedValue = numValue < 10 ? formatNumber(numValue, 1) : formatNumber(numValue, 0);
  
  // Append the unit
  return `${formattedValue}${unit}`;
}

/**
 * Formats a boolean value as 'Yes' or 'No'
 * @param value The boolean value to format
 * @returns 'Yes', 'No', or empty string if input is invalid
 */
export function formatBoolean(value: boolean | null | undefined): string {
  if (value === null || value === undefined) {
    return '';
  }
  
  return value ? 'Yes' : 'No';
}