/**
 * Storage Utility Functions
 * 
 * This file provides utility functions for browser storage operations with error handling 
 * and type safety. It offers a consistent interface for interacting with localStorage and
 * sessionStorage, handling serialization/deserialization of data, and managing storage
 * availability.
 * 
 * Features:
 * - Type-safe storage operations
 * - Error handling for all storage operations
 * - Serialization/deserialization of complex data
 * - Storage availability checking
 * - Consistent interface for both localStorage and sessionStorage
 */

import { STORAGE_KEYS } from './constants';

/**
 * Checks if a specific storage type is available in the browser.
 * @param type - The type of storage to check ('localStorage' or 'sessionStorage')
 * @returns True if the storage is available, false otherwise
 */
export function isStorageAvailable(type: string): boolean {
  try {
    // Create a test key with a random value
    const testKey = `__storage_test_${Math.random()}`;
    const storage = window[type as keyof Window];
    
    if (!storage) {
      return false;
    }
    
    // Test the storage
    storage.setItem(testKey, 'test');
    const result = storage.getItem(testKey);
    storage.removeItem(testKey);
    
    return result === 'test';
  } catch (e) {
    // Various errors can occur:
    // - QuotaExceededError: Storage is full
    // - SecurityError: Browser has blocked access due to secure context requirements
    // - Other potential errors
    return false;
  }
}

/**
 * Safely stores a value in localStorage with JSON serialization.
 * @param key - The key to store the value under
 * @param value - The value to store
 * @returns True if the operation was successful, false otherwise
 */
export function setLocalStorageItem(key: string, value: any): boolean {
  if (!isStorageAvailable('localStorage')) {
    return false;
  }
  
  try {
    // Serialize the value to JSON
    const serializedValue = JSON.stringify(value);
    localStorage.setItem(key, serializedValue);
    return true;
  } catch (error) {
    console.error('Error setting localStorage item:', error);
    return false;
  }
}

/**
 * Safely retrieves and deserializes a value from localStorage.
 * @param key - The key to retrieve the value for
 * @param defaultValue - The default value to return if the key is not found or on error
 * @returns The retrieved value or defaultValue if not found or on error
 */
export function getLocalStorageItem<T>(key: string, defaultValue: T): T {
  if (!isStorageAvailable('localStorage')) {
    return defaultValue;
  }
  
  try {
    const item = localStorage.getItem(key);
    if (item === null) {
      return defaultValue;
    }
    
    return JSON.parse(item) as T;
  } catch (error) {
    console.error('Error getting localStorage item:', error);
    return defaultValue;
  }
}

/**
 * Safely removes an item from localStorage.
 * @param key - The key to remove
 * @returns True if the operation was successful, false otherwise
 */
export function removeLocalStorageItem(key: string): boolean {
  if (!isStorageAvailable('localStorage')) {
    return false;
  }
  
  try {
    localStorage.removeItem(key);
    return true;
  } catch (error) {
    console.error('Error removing localStorage item:', error);
    return false;
  }
}

/**
 * Safely clears all items from localStorage.
 * @returns True if the operation was successful, false otherwise
 */
export function clearLocalStorage(): boolean {
  if (!isStorageAvailable('localStorage')) {
    return false;
  }
  
  try {
    localStorage.clear();
    return true;
  } catch (error) {
    console.error('Error clearing localStorage:', error);
    return false;
  }
}

/**
 * Safely stores a value in sessionStorage with JSON serialization.
 * @param key - The key to store the value under
 * @param value - The value to store
 * @returns True if the operation was successful, false otherwise
 */
export function setSessionStorageItem(key: string, value: any): boolean {
  if (!isStorageAvailable('sessionStorage')) {
    return false;
  }
  
  try {
    // Serialize the value to JSON
    const serializedValue = JSON.stringify(value);
    sessionStorage.setItem(key, serializedValue);
    return true;
  } catch (error) {
    console.error('Error setting sessionStorage item:', error);
    return false;
  }
}

/**
 * Safely retrieves and deserializes a value from sessionStorage.
 * @param key - The key to retrieve the value for
 * @param defaultValue - The default value to return if the key is not found or on error
 * @returns The retrieved value or defaultValue if not found or on error
 */
export function getSessionStorageItem<T>(key: string, defaultValue: T): T {
  if (!isStorageAvailable('sessionStorage')) {
    return defaultValue;
  }
  
  try {
    const item = sessionStorage.getItem(key);
    if (item === null) {
      return defaultValue;
    }
    
    return JSON.parse(item) as T;
  } catch (error) {
    console.error('Error getting sessionStorage item:', error);
    return defaultValue;
  }
}

/**
 * Safely removes an item from sessionStorage.
 * @param key - The key to remove
 * @returns True if the operation was successful, false otherwise
 */
export function removeSessionStorageItem(key: string): boolean {
  if (!isStorageAvailable('sessionStorage')) {
    return false;
  }
  
  try {
    sessionStorage.removeItem(key);
    return true;
  } catch (error) {
    console.error('Error removing sessionStorage item:', error);
    return false;
  }
}

/**
 * Safely clears all items from sessionStorage.
 * @returns True if the operation was successful, false otherwise
 */
export function clearSessionStorage(): boolean {
  if (!isStorageAvailable('sessionStorage')) {
    return false;
  }
  
  try {
    sessionStorage.clear();
    return true;
  } catch (error) {
    console.error('Error clearing sessionStorage:', error);
    return false;
  }
}