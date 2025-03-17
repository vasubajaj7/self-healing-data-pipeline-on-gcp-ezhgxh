import { useEffect, useRef } from 'react'; // react@^18.2.0

/**
 * A custom hook that sets up an interval timer that calls a callback function
 * at a specified delay. The interval is cleared when the component unmounts
 * or when the delay changes. If delay is null, the interval is not started.
 * 
 * This hook is useful for implementing real-time data updates in dashboard
 * components while ensuring proper resource management and preventing memory leaks.
 *
 * @param callback - The function to call every interval
 * @param delay - The delay in milliseconds between each call, or null to disable
 */
export function useInterval(callback: () => void, delay: number | null): void {
  // Use a ref to store the callback to avoid recreating the interval
  // when only the callback changes
  const savedCallback = useRef<() => void>();

  // Remember the latest callback
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  // Set up the interval
  useEffect(() => {
    // Don't schedule if delay is null
    if (delay === null) {
      return;
    }

    // Set up the interval
    const id = setInterval(() => {
      if (savedCallback.current) {
        savedCallback.current();
      }
    }, delay);

    // Clean up on unmount or when delay changes
    return () => clearInterval(id);
  }, [delay]);
}