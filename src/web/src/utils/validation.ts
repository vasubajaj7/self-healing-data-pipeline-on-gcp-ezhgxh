import { ERROR_MESSAGES } from './constants';

/**
 * Validates that a value is not empty
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateRequired = (value: string): string | null => {
  if (value === undefined || value === null || value === '') {
    return 'This field is required';
  }
  return null;
};

/**
 * Validates that a value is a properly formatted email address
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateEmail = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty (handled by required validation)
  
  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  if (!emailRegex.test(value)) {
    return 'Please enter a valid email address';
  }
  return null;
};

/**
 * Validates that a value is a properly formatted URL
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateUrl = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  try {
    new URL(value);
    return null;
  } catch (e) {
    return 'Please enter a valid URL';
  }
};

/**
 * Validates that a string value meets the minimum length requirement
 * @param value The value to validate
 * @param minLength The minimum length required
 * @returns Error message if validation fails, null otherwise
 */
export const validateMinLength = (value: string, minLength: number): string | null => {
  if (!value) return null; // Skip validation if empty
  
  if (value.length < minLength) {
    return `This field must be at least ${minLength} characters`;
  }
  return null;
};

/**
 * Validates that a string value does not exceed the maximum length
 * @param value The value to validate
 * @param maxLength The maximum length allowed
 * @returns Error message if validation fails, null otherwise
 */
export const validateMaxLength = (value: string, maxLength: number): string | null => {
  if (!value) return null; // Skip validation if empty
  
  if (value.length > maxLength) {
    return `This field cannot exceed ${maxLength} characters`;
  }
  return null;
};

/**
 * Validates that a value is a valid number
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateNumber = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  const num = Number(value);
  if (isNaN(num)) {
    return 'Please enter a valid number';
  }
  return null;
};

/**
 * Validates that a value is a valid integer
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateInteger = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  const num = Number(value);
  if (isNaN(num) || !Number.isInteger(num)) {
    return 'Please enter a valid integer';
  }
  return null;
};

/**
 * Validates that a numeric value is greater than or equal to the minimum value
 * @param value The value to validate
 * @param minValue The minimum allowed value
 * @returns Error message if validation fails, null otherwise
 */
export const validateMinValue = (value: string | number, minValue: number): string | null => {
  if (value === '' || value === undefined || value === null) return null; // Skip validation if empty
  
  const num = typeof value === 'string' ? Number(value) : value;
  if (isNaN(num)) {
    return 'Please enter a valid number';
  }
  
  if (num < minValue) {
    return `Value must be at least ${minValue}`;
  }
  return null;
};

/**
 * Validates that a numeric value is less than or equal to the maximum value
 * @param value The value to validate
 * @param maxValue The maximum allowed value
 * @returns Error message if validation fails, null otherwise
 */
export const validateMaxValue = (value: string | number, maxValue: number): string | null => {
  if (value === '' || value === undefined || value === null) return null; // Skip validation if empty
  
  const num = typeof value === 'string' ? Number(value) : value;
  if (isNaN(num)) {
    return 'Please enter a valid number';
  }
  
  if (num > maxValue) {
    return `Value must not exceed ${maxValue}`;
  }
  return null;
};

/**
 * Validates that a string value matches a specific regex pattern
 * @param value The value to validate
 * @param pattern The RegExp pattern to test against
 * @param errorMessage Custom error message to display if validation fails
 * @returns Error message if validation fails, null otherwise
 */
export const validatePattern = (value: string, pattern: RegExp, errorMessage: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  if (!pattern.test(value)) {
    return errorMessage;
  }
  return null;
};

/**
 * Validates that a value is a valid date string
 * @param value The value to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateDate = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  const date = new Date(value);
  if (isNaN(date.getTime())) {
    return 'Please enter a valid date';
  }
  return null;
};

/**
 * Validates that a date is within a specified range
 * @param value The date string to validate
 * @param minDate The minimum allowed date (or null for no minimum)
 * @param maxDate The maximum allowed date (or null for no maximum)
 * @returns Error message if validation fails, null otherwise
 */
export const validateDateRange = (value: string, minDate: Date | null, maxDate: Date | null): string | null => {
  if (!value) return null; // Skip validation if empty
  
  const dateValidation = validateDate(value);
  if (dateValidation) return dateValidation;
  
  const date = new Date(value);
  
  if (minDate && date < minDate) {
    return `Date must be on or after ${minDate.toLocaleDateString()}`;
  }
  
  if (maxDate && date > maxDate) {
    return `Date must be on or before ${maxDate.toLocaleDateString()}`;
  }
  
  return null;
};

/**
 * Validates that a string is valid JSON
 * @param value The string to validate
 * @returns Error message if validation fails, null otherwise
 */
export const validateJsonString = (value: string): string | null => {
  if (!value) return null; // Skip validation if empty
  
  try {
    JSON.parse(value);
    return null;
  } catch (e) {
    return 'Please enter valid JSON';
  }
};

/**
 * Validates an entire form using a validation schema
 * @param values Object containing form values
 * @param validationSchema Object containing validation functions for each field
 * @returns Object containing field names and error messages
 * 
 * When a form has validation errors, use ERROR_MESSAGES.VALIDATION_ERROR for user feedback
 */
export const validateForm = (
  values: Record<string, any>,
  validationSchema: Record<string, (value: any) => string | null>
): Record<string, string | null> => {
  const errors: Record<string, string | null> = {};
  
  Object.keys(validationSchema).forEach(field => {
    const validate = validationSchema[field];
    const fieldValue = values[field];
    errors[field] = validate(fieldValue);
  });
  
  return errors;
};

/**
 * Checks if a form is valid based on validation errors
 * @param errors Object containing validation errors
 * @returns True if form is valid, false otherwise
 */
export const isFormValid = (errors: Record<string, string | null>): boolean => {
  if (Object.keys(errors).length === 0) return true;
  return Object.values(errors).every(error => error === null);
};

/**
 * Validates an API response structure
 * @param response The API response to validate
 * @returns True if response is valid, false otherwise
 */
export const validateApiResponse = (response: any): boolean => {
  if (!response || typeof response !== 'object') return false;
  
  // Check for required fields in a standard API response
  const hasRequiredFields = 
    response.hasOwnProperty('status') && 
    response.hasOwnProperty('message') && 
    response.hasOwnProperty('metadata');
  
  return hasRequiredFields;
};