import { useState, useEffect, useCallback } from 'react'; // react ^18.2.0
import { setLocalStorageItem, getLocalStorageItem, removeLocalStorageItem } from '../utils/storage';

/**
 * Type definition for the return value of useLocalStorage
 * Provides a tuple containing the current value, a setter function, and a removal function
 */
export type UseLocalStorageReturn<T> = [T, (value: T | ((val: T) => T)) => void, () => void];

/**
 * A custom React hook that synchronizes state with localStorage
 * 
 * This hook provides a stateful value that persists in localStorage across page reloads.
 * It offers an API similar to React's useState, with an additional function to remove
 * the item from storage.
 * 
 * @template T - The type of value being stored
 * @param key - The localStorage key to store the value under
 * @param initialValue - The initial value to use if no value exists in localStorage
 * @returns A tuple containing [storedValue, setValue, removeValue]
 * 
 * @example
 * // Store user preferences
 * const [preferences, setPreferences, removePreferences] = useLocalStorage('user_preferences', { theme: 'light' });
 * 
 * // Update a specific preference
 * setPreferences(prev => ({ ...prev, theme: 'dark' }));
 * 
 * // Clear preferences
 * removePreferences();
 */
export function useLocalStorage<T>(key: string, initialValue: T): UseLocalStorageReturn<T> {
  // Initialize state with the value from localStorage or the provided initialValue
  const [storedValue, setStoredValue] = useState<T>(() => {
    return getLocalStorageItem<T>(key, initialValue);
  });

  // Create a memoized setter function that updates both state and localStorage
  const setValue = useCallback((value: T | ((val: T) => T)) => {
    setStoredValue(prevValue => {
      // Handle both direct values and updater functions
      const newValue = value instanceof Function ? value(prevValue) : value;
      
      // Update localStorage with the new value
      setLocalStorageItem(key, newValue);
      
      // Return the new value to update state
      return newValue;
    });
  }, [key]);

  // Create a memoized remove function that clears the item from localStorage
  const removeValue = useCallback(() => {
    removeLocalStorageItem(key);
    setStoredValue(initialValue);
  }, [key, initialValue]);

  // Set up an effect to update state if localStorage changes in another tab/window
  useEffect(() => {
    const handleStorageChange = (e: StorageEvent) => {
      if (e.key === key) {
        if (e.newValue !== null) {
          try {
            // Parse the new value and update state
            const newValue = JSON.parse(e.newValue) as T;
            setStoredValue(newValue);
          } catch (error) {
            console.error('Error parsing localStorage change:', error);
          }
        } else {
          // If the item was removed, reset to initialValue
          setStoredValue(initialValue);
        }
      }
    };

    // Add event listener for storage events
    window.addEventListener('storage', handleStorageChange);
    
    // Clean up the event listener on unmount
    return () => {
      window.removeEventListener('storage', handleStorageChange);
    };
  }, [key, initialValue]);

  // Return a tuple with the current value, setter function, and remove function
  return [storedValue, setValue, removeValue];
}