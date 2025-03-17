/**
 * Date utility functions for the self-healing data pipeline web application.
 * This module provides comprehensive date handling capabilities including
 * parsing, formatting, comparison, and manipulation.
 */

import {
  format,
  parse,
  isValid,
  parseISO,
  differenceInMilliseconds,
  differenceInSeconds,
  differenceInMinutes,
  differenceInHours,
  differenceInDays,
  addDays,
  addHours,
  addMinutes,
  subDays,
  subHours,
  subMinutes,
  isAfter,
  isBefore,
  isEqual,
  startOfDay,
  endOfDay,
  startOfWeek,
  endOfWeek,
  startOfMonth,
  endOfMonth
} from 'date-fns'; // v2.30.0

import {
  DATE_FORMATS
} from '../utils/constants';

/**
 * Formats a date using the specified format pattern
 * @param date - The date to format
 * @param formatPattern - The format pattern to use
 * @returns Formatted date string or empty string if input is invalid
 */
export function formatDate(
  date: Date | string | number | null | undefined,
  formatPattern: string
): string {
  // Return empty string for null, undefined or invalid dates
  if (date === null || date === undefined) {
    return '';
  }

  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return '';
  }

  try {
    return format(parsedDate, formatPattern);
  } catch (error) {
    console.error('Error formatting date:', error);
    return '';
  }
}

/**
 * Parses a date string or timestamp into a Date object
 * @param date - The date to parse
 * @returns Date object or null if input is invalid
 */
export function parseDate(
  date: Date | string | number | null | undefined
): Date | null {
  if (date === null || date === undefined) {
    return null;
  }

  // If already a Date object, return it directly
  if (date instanceof Date) {
    return isValid(date) ? date : null;
  }

  // If it's a number (timestamp), create Date from it
  if (typeof date === 'number') {
    const dateObj = new Date(date);
    return isValid(dateObj) ? dateObj : null;
  }

  // If it's a string, try to parse it
  if (typeof date === 'string') {
    try {
      const dateObj = parseISO(date);
      return isValid(dateObj) ? dateObj : null;
    } catch (error) {
      return null;
    }
  }

  return null;
}

/**
 * Checks if a value is a valid date
 * @param value - The value to check
 * @returns True if the value is a valid date, false otherwise
 */
export function isValidDate(value: any): boolean {
  if (value === null || value === undefined) {
    return false;
  }

  if (value instanceof Date) {
    return isValid(value);
  }

  return parseDate(value) !== null;
}

/**
 * Formats a date using the short date format
 * @param date - The date to format
 * @returns Formatted date string or empty string if input is invalid
 */
export function formatDateShort(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.SHORT_DATE);
}

/**
 * Formats a date using the long date format
 * @param date - The date to format
 * @returns Formatted date string or empty string if input is invalid
 */
export function formatDateLong(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.LONG_DATE);
}

/**
 * Formats a date using the short time format
 * @param date - The date to format
 * @returns Formatted time string or empty string if input is invalid
 */
export function formatTimeShort(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.SHORT_TIME);
}

/**
 * Formats a date using the long time format
 * @param date - The date to format
 * @returns Formatted time string or empty string if input is invalid
 */
export function formatTimeLong(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.LONG_TIME);
}

/**
 * Formats a date using the short date and time format
 * @param date - The date to format
 * @returns Formatted date and time string or empty string if input is invalid
 */
export function formatDateTimeShort(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.SHORT_DATETIME);
}

/**
 * Formats a date using the long date and time format
 * @param date - The date to format
 * @returns Formatted date and time string or empty string if input is invalid
 */
export function formatDateTimeLong(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.LONG_DATETIME);
}

/**
 * Formats a date as an ISO 8601 date string (YYYY-MM-DD)
 * @param date - The date to format
 * @returns ISO date string or empty string if input is invalid
 */
export function formatISODate(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.ISO_DATE);
}

/**
 * Formats a date as an ISO 8601 date and time string
 * @param date - The date to format
 * @returns ISO date and time string or empty string if input is invalid
 */
export function formatISODateTime(
  date: Date | string | number | null | undefined
): string {
  return formatDate(date, DATE_FORMATS.ISO_DATETIME);
}

/**
 * Calculates the difference between two dates in the specified unit
 * @param startDate - The start date
 * @param endDate - The end date
 * @param unit - The unit to calculate the difference in ('milliseconds', 'seconds', 'minutes', 'hours', 'days')
 * @returns Difference in the specified unit or null if inputs are invalid
 */
export function getDateDifference(
  startDate: Date | string | number | null | undefined,
  endDate: Date | string | number | null | undefined,
  unit: 'milliseconds' | 'seconds' | 'minutes' | 'hours' | 'days'
): number | null {
  const parsedStartDate = parseDate(startDate);
  const parsedEndDate = parseDate(endDate);

  if (!parsedStartDate || !parsedEndDate) {
    return null;
  }

  switch (unit) {
    case 'milliseconds':
      return differenceInMilliseconds(parsedEndDate, parsedStartDate);
    case 'seconds':
      return differenceInSeconds(parsedEndDate, parsedStartDate);
    case 'minutes':
      return differenceInMinutes(parsedEndDate, parsedStartDate);
    case 'hours':
      return differenceInHours(parsedEndDate, parsedStartDate);
    case 'days':
      return differenceInDays(parsedEndDate, parsedStartDate);
    default:
      return null;
  }
}

/**
 * Adds a specified amount of time to a date
 * @param date - The date to add to
 * @param amount - The amount to add
 * @param unit - The unit of time to add ('days', 'hours', 'minutes')
 * @returns New date with the added time or null if input is invalid
 */
export function addToDate(
  date: Date | string | number | null | undefined,
  amount: number,
  unit: 'days' | 'hours' | 'minutes'
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  switch (unit) {
    case 'days':
      return addDays(parsedDate, amount);
    case 'hours':
      return addHours(parsedDate, amount);
    case 'minutes':
      return addMinutes(parsedDate, amount);
    default:
      return parsedDate;
  }
}

/**
 * Subtracts a specified amount of time from a date
 * @param date - The date to subtract from
 * @param amount - The amount to subtract
 * @param unit - The unit of time to subtract ('days', 'hours', 'minutes')
 * @returns New date with the subtracted time or null if input is invalid
 */
export function subtractFromDate(
  date: Date | string | number | null | undefined,
  amount: number,
  unit: 'days' | 'hours' | 'minutes'
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  switch (unit) {
    case 'days':
      return subDays(parsedDate, amount);
    case 'hours':
      return subHours(parsedDate, amount);
    case 'minutes':
      return subMinutes(parsedDate, amount);
    default:
      return parsedDate;
  }
}

/**
 * Compares two dates and returns a comparison result
 * @param dateA - The first date
 * @param dateB - The second date
 * @returns -1 if dateA is before dateB, 0 if equal, 1 if after, or null if inputs are invalid
 */
export function compareDates(
  dateA: Date | string | number | null | undefined,
  dateB: Date | string | number | null | undefined
): number | null {
  const parsedDateA = parseDate(dateA);
  const parsedDateB = parseDate(dateB);

  if (!parsedDateA || !parsedDateB) {
    return null;
  }

  if (isBefore(parsedDateA, parsedDateB)) {
    return -1;
  }

  if (isEqual(parsedDateA, parsedDateB)) {
    return 0;
  }

  return 1;
}

/**
 * Checks if the first date is before the second date
 * @param dateA - The first date
 * @param dateB - The second date
 * @returns True if dateA is before dateB, false otherwise or if inputs are invalid
 */
export function isDateBefore(
  dateA: Date | string | number | null | undefined,
  dateB: Date | string | number | null | undefined
): boolean {
  const parsedDateA = parseDate(dateA);
  const parsedDateB = parseDate(dateB);

  if (!parsedDateA || !parsedDateB) {
    return false;
  }

  return isBefore(parsedDateA, parsedDateB);
}

/**
 * Checks if the first date is after the second date
 * @param dateA - The first date
 * @param dateB - The second date
 * @returns True if dateA is after dateB, false otherwise or if inputs are invalid
 */
export function isDateAfter(
  dateA: Date | string | number | null | undefined,
  dateB: Date | string | number | null | undefined
): boolean {
  const parsedDateA = parseDate(dateA);
  const parsedDateB = parseDate(dateB);

  if (!parsedDateA || !parsedDateB) {
    return false;
  }

  return isAfter(parsedDateA, parsedDateB);
}

/**
 * Checks if two dates are equal
 * @param dateA - The first date
 * @param dateB - The second date
 * @returns True if dateA is equal to dateB, false otherwise or if inputs are invalid
 */
export function isDateEqual(
  dateA: Date | string | number | null | undefined,
  dateB: Date | string | number | null | undefined
): boolean {
  const parsedDateA = parseDate(dateA);
  const parsedDateB = parseDate(dateB);

  if (!parsedDateA || !parsedDateB) {
    return false;
  }

  return isEqual(parsedDateA, parsedDateB);
}

/**
 * Gets the start of the day for a given date
 * @param date - The date
 * @returns Date object representing the start of the day or null if input is invalid
 */
export function getStartOfDay(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return startOfDay(parsedDate);
}

/**
 * Gets the end of the day for a given date
 * @param date - The date
 * @returns Date object representing the end of the day or null if input is invalid
 */
export function getEndOfDay(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return endOfDay(parsedDate);
}

/**
 * Gets the start of the week for a given date
 * @param date - The date
 * @returns Date object representing the start of the week or null if input is invalid
 */
export function getStartOfWeek(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return startOfWeek(parsedDate);
}

/**
 * Gets the end of the week for a given date
 * @param date - The date
 * @returns Date object representing the end of the week or null if input is invalid
 */
export function getEndOfWeek(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return endOfWeek(parsedDate);
}

/**
 * Gets the start of the month for a given date
 * @param date - The date
 * @returns Date object representing the start of the month or null if input is invalid
 */
export function getStartOfMonth(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return startOfMonth(parsedDate);
}

/**
 * Gets the end of the month for a given date
 * @param date - The date
 * @returns Date object representing the end of the month or null if input is invalid
 */
export function getEndOfMonth(
  date: Date | string | number | null | undefined
): Date | null {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return null;
  }

  return endOfMonth(parsedDate);
}

/**
 * Gets a date range relative to the current date
 * @param rangeType - The type of range to get ('today', 'yesterday', 'last7Days', 'last30Days', 
 *                      'thisWeek', 'lastWeek', 'thisMonth', 'lastMonth')
 * @returns Object containing start and end dates for the specified range
 */
export function getRelativeDateRange(
  rangeType: 'today' | 'yesterday' | 'last7Days' | 'last30Days' | 'thisWeek' | 'lastWeek' | 'thisMonth' | 'lastMonth'
): { startDate: Date; endDate: Date } {
  const today = new Date();

  switch (rangeType) {
    case 'today':
      return {
        startDate: startOfDay(today),
        endDate: endOfDay(today)
      };
    case 'yesterday': {
      const yesterday = subDays(today, 1);
      return {
        startDate: startOfDay(yesterday),
        endDate: endOfDay(yesterday)
      };
    }
    case 'last7Days':
      return {
        startDate: startOfDay(subDays(today, 6)),
        endDate: endOfDay(today)
      };
    case 'last30Days':
      return {
        startDate: startOfDay(subDays(today, 29)),
        endDate: endOfDay(today)
      };
    case 'thisWeek':
      return {
        startDate: startOfWeek(today),
        endDate: endOfWeek(today)
      };
    case 'lastWeek': {
      const lastWeek = subDays(today, 7);
      return {
        startDate: startOfWeek(lastWeek),
        endDate: endOfWeek(lastWeek)
      };
    }
    case 'thisMonth':
      return {
        startDate: startOfMonth(today),
        endDate: endOfMonth(today)
      };
    case 'lastMonth': {
      // Go back to the previous month
      const lastMonth = subDays(startOfMonth(today), 1);
      return {
        startDate: startOfMonth(lastMonth),
        endDate: endOfMonth(lastMonth)
      };
    }
    default:
      return {
        startDate: startOfDay(today),
        endDate: endOfDay(today)
      };
  }
}

/**
 * Formats a date as a relative time string (e.g., '5 minutes ago')
 * @param date - The date to format
 * @returns Relative time string or empty string if input is invalid
 */
export function getTimeAgo(
  date: Date | string | number | null | undefined
): string {
  const parsedDate = parseDate(date);
  if (!parsedDate) {
    return '';
  }

  const now = new Date();
  const diffInSeconds = differenceInSeconds(now, parsedDate);
  const diffInMinutes = differenceInMinutes(now, parsedDate);
  const diffInHours = differenceInHours(now, parsedDate);
  const diffInDays = differenceInDays(now, parsedDate);

  if (diffInSeconds < 60) {
    return 'just now';
  }

  if (diffInMinutes < 60) {
    return `${diffInMinutes} ${diffInMinutes === 1 ? 'minute' : 'minutes'} ago`;
  }

  if (diffInHours < 24) {
    return `${diffInHours} ${diffInHours === 1 ? 'hour' : 'hours'} ago`;
  }

  if (diffInDays < 7) {
    return `${diffInDays} ${diffInDays === 1 ? 'day' : 'days'} ago`;
  }

  const diffInWeeks = Math.floor(diffInDays / 7);
  if (diffInWeeks < 4) {
    return `${diffInWeeks} ${diffInWeeks === 1 ? 'week' : 'weeks'} ago`;
  }

  const diffInMonths = Math.floor(diffInDays / 30);
  if (diffInMonths < 12) {
    return `${diffInMonths} ${diffInMonths === 1 ? 'month' : 'months'} ago`;
  }

  const diffInYears = Math.floor(diffInDays / 365);
  return `${diffInYears} ${diffInYears === 1 ? 'year' : 'years'} ago`;
}