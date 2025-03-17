import { logError } from '../../utils/errorHandling';

/**
 * Configuration options for the reconnection strategy
 */
export interface ReconnectionOptions {
  /**
   * Initial delay in milliseconds before the first reconnection attempt
   * Default: 1000ms (1 second)
   */
  initialDelay?: number;

  /**
   * Maximum delay in milliseconds between reconnection attempts
   * Default: 30000ms (30 seconds)
   */
  maxDelay?: number;

  /**
   * Maximum number of reconnection attempts before giving up
   * Default: 10 attempts
   */
  maxAttempts?: number;

  /**
   * Factor by which the delay increases with each attempt (exponential backoff)
   * Default: 2 (doubles the delay with each attempt)
   */
  backoffFactor?: number;

  /**
   * Whether to use random jitter to prevent reconnection thundering herd
   * Default: true
   */
  useJitter?: boolean;

  /**
   * Amount of jitter to apply as a fraction of the calculated delay
   * Default: 0.1 (10% jitter)
   */
  jitterFactor?: number;
}

/**
 * Implements reconnection strategy with exponential backoff for WebSocket connections.
 * 
 * This class provides a configurable reconnection mechanism that increases delay between
 * reconnection attempts exponentially, with optional jitter to prevent the "thundering herd"
 * problem when multiple clients reconnect simultaneously.
 * 
 * @example
 * ```typescript
 * // Create a strategy with default options
 * const reconnectionStrategy = new ReconnectionStrategy();
 * 
 * // Use in a WebSocket reconnection loop
 * function reconnect() {
 *   if (reconnectionStrategy.hasMoreAttempts()) {
 *     const delay = reconnectionStrategy.getDelay();
 *     setTimeout(() => {
 *       // Attempt to reconnect WebSocket...
 *     }, delay);
 *   } else {
 *     console.error('Maximum reconnection attempts reached');
 *   }
 * }
 * ```
 */
export class ReconnectionStrategy {
  private initialDelay: number;
  private maxDelay: number;
  private maxAttempts: number;
  private currentAttempt: number;
  private backoffFactor: number;
  private useJitter: boolean;
  private jitterFactor: number;

  /**
   * Initializes a new ReconnectionStrategy instance with configurable parameters
   * 
   * @param options - Configuration options for the reconnection strategy
   */
  constructor(options?: ReconnectionOptions) {
    this.initialDelay = options?.initialDelay ?? 1000; // Default: 1 second
    this.maxDelay = options?.maxDelay ?? 30000; // Default: 30 seconds
    this.maxAttempts = options?.maxAttempts ?? 10; // Default: 10 attempts
    this.backoffFactor = options?.backoffFactor ?? 2; // Default: Double the delay with each attempt
    this.useJitter = options?.useJitter ?? true; // Default: Use jitter
    this.jitterFactor = options?.jitterFactor ?? 0.1; // Default: 10% jitter
    this.currentAttempt = 0;
  }

  /**
   * Calculates the delay for the next reconnection attempt using exponential backoff
   * 
   * The delay increases exponentially with each attempt and is capped at a maximum value.
   * If jitter is enabled, a random variation is applied to prevent reconnection thundering herd.
   * 
   * @returns Delay in milliseconds for the next reconnection attempt
   */
  getDelay(): number {
    // Increment the current attempt counter
    this.currentAttempt++;

    // Calculate base delay using exponential backoff formula
    const baseDelay = this.initialDelay * Math.pow(this.backoffFactor, this.currentAttempt - 1);
    
    // Apply maximum delay cap to prevent excessive delays
    const cappedDelay = Math.min(baseDelay, this.maxDelay);
    
    // Apply jitter if enabled
    const finalDelay = this.useJitter ? this.applyJitter(cappedDelay) : cappedDelay;
    
    // Log the reconnection attempt with calculated delay
    logError(
      `WebSocket reconnection attempt ${this.currentAttempt}/${this.maxAttempts} with delay ${finalDelay}ms`,
      'ReconnectionStrategy'
    );
    
    return finalDelay;
  }

  /**
   * Resets the reconnection strategy to its initial state
   */
  reset(): void {
    this.currentAttempt = 0;
  }

  /**
   * Checks if more reconnection attempts are available
   * 
   * @returns True if more reconnection attempts are available, false otherwise
   */
  hasMoreAttempts(): boolean {
    return this.currentAttempt < this.maxAttempts;
  }

  /**
   * Gets the current reconnection attempt count
   * 
   * @returns Current reconnection attempt count
   */
  getCurrentAttempt(): number {
    return this.currentAttempt;
  }

  /**
   * Gets the maximum number of reconnection attempts
   * 
   * @returns Maximum number of reconnection attempts
   */
  getMaxAttempts(): number {
    return this.maxAttempts;
  }

  /**
   * Applies random jitter to a delay value to prevent reconnection thundering herd
   * 
   * @param delay - The base delay to apply jitter to
   * @returns Delay with jitter applied
   * @private
   */
  private applyJitter(delay: number): number {
    // Calculate jitter range based on delay and jitter factor
    const jitterRange = delay * this.jitterFactor;
    
    // Generate random jitter value within the range [-jitterRange/2, jitterRange/2]
    const randomJitter = (Math.random() - 0.5) * jitterRange;
    
    // Apply jitter to delay and ensure it doesn't go negative
    return Math.max(0, delay + randomJitter);
  }
}