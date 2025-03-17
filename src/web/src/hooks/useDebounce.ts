import { useState, useEffect, useRef } from 'react'; // ^18.2.0

/**
 * Type definition for a debounced function
 * Preserves the parameter types of the original function
 */
type DebouncedFunction<T extends (...args: any[]) => any> = (...args: Parameters<T>) => void;

/**
 * A hook that returns a debounced version of the provided value
 * that only updates after the specified delay has elapsed
 * 
 * @param value The value to debounce
 * @param delay The delay in milliseconds
 * @returns The debounced value
 * 
 * @example
 * // Using with a search input
 * const [searchText, setSearchText] = useState('');
 * const debouncedSearchText = useDebounce(searchText, 500);
 * 
 * // Effect only runs when debouncedSearchText changes, not on every keystroke
 * useEffect(() => {
 *   fetchSearchResults(debouncedSearchText);
 * }, [debouncedSearchText]);
 */
export function useDebounce<T>(value: T, delay: number): T {
  // State to store the debounced value
  const [debouncedValue, setDebouncedValue] = useState<T>(value);

  useEffect(() => {
    // Set up a timeout to update the debounced value after the specified delay
    const timeoutId = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);

    // Clean up the timeout if the value changes before the delay elapses
    // or when the component unmounts
    return () => {
      clearTimeout(timeoutId);
    };
  }, [value, delay]);

  return debouncedValue;
}

/**
 * A hook that returns a debounced version of the provided callback function
 * that only executes after the specified delay has elapsed
 * 
 * @param callback The function to debounce
 * @param delay The delay in milliseconds
 * @returns A debounced function
 * 
 * @example
 * // Using with a form input
 * const updateUser = async (userData) => {
 *   await api.updateUser(userData);
 * };
 * 
 * const debouncedUpdateUser = useDebouncedCallback(updateUser, 500);
 * 
 * // In the component
 * const handleChange = (e) => {
 *   const newUserData = { ...userData, [e.target.name]: e.target.value };
 *   setUserData(newUserData);
 *   debouncedUpdateUser(newUserData);
 * };
 */
export function useDebouncedCallback<T extends (...args: any[]) => any>(
  callback: T,
  delay: number
): DebouncedFunction<T> {
  // Store the timeout ID for cleanup
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);
  
  // Store the latest callback to avoid closure issues
  const callbackRef = useRef<T>(callback);
  
  // Update the callback ref when the callback changes
  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  // Return a memoized debounced function
  return useRef((...args: Parameters<T>): void => {
    // Clear any existing timeout
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    
    // Set a new timeout
    timeoutRef.current = setTimeout(() => {
      // Use the latest callback from the ref
      callbackRef.current(...args);
    }, delay);
  }).current;
}