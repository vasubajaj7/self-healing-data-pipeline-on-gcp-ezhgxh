import { useEffect, useRef, RefObject } from 'react'; // ^18.2.0

/**
 * A hook that detects clicks outside of the specified element and calls the provided callback function.
 * Useful for closing dropdowns, modals, or other interactive elements when a user clicks elsewhere on the page.
 *
 * @param ref - React ref object pointing to the element to detect clicks outside of
 * @param callback - Function to call when a click outside is detected
 * @param enabled - Whether the outside click detection is enabled (defaults to true)
 * 
 * @example
 * ```tsx
 * const MyComponent = () => {
 *   const [isOpen, setIsOpen] = useState(false);
 *   const ref = useRef(null);
 *   
 *   useOutsideClick(ref, () => {
 *     if (isOpen) setIsOpen(false);
 *   });
 *   
 *   return (
 *     <div ref={ref}>
 *       {isOpen && <div>This will close when clicking outside</div>}
 *       <button onClick={() => setIsOpen(true)}>Open</button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useOutsideClick(
  ref: RefObject<HTMLElement | null>,
  callback: () => void,
  enabled: boolean = true
): void {
  // Use a ref for the callback to avoid issues with stale closures
  const callbackRef = useRef(callback);

  // Update the callback ref when the callback changes
  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!enabled) return;

    // Handler for when a click or touch occurs
    const handleOutsideClick = (event: MouseEvent | TouchEvent) => {
      const target = event.target as Node;

      // If the click is outside the referenced element, call the callback
      if (ref.current && !ref.current.contains(target)) {
        callbackRef.current();
      }
    };

    // Add event listeners for both mouse and touch events
    document.addEventListener('mousedown', handleOutsideClick);
    document.addEventListener('touchstart', handleOutsideClick);

    // Clean up event listeners on unmount or when dependencies change
    return () => {
      document.removeEventListener('mousedown', handleOutsideClick);
      document.removeEventListener('touchstart', handleOutsideClick);
    };
  }, [ref, enabled]);
}