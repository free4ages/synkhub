import { useEffect, useRef } from 'react';

interface UsePollingOptions {
  interval?: number;
  immediate?: boolean;
  enabled?: boolean;
}

export const usePolling = (
  callback: () => void | Promise<void>,
  options: UsePollingOptions = {}
) => {
  const {
    interval = 30000, // 30 seconds default
    immediate = true,
    enabled = true,
  } = options;

  const savedCallback = useRef(callback);
  const intervalId = useRef<NodeJS.Timeout | null>(null);

  // Update callback ref when callback changes
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!enabled) {
      if (intervalId.current) {
        clearInterval(intervalId.current);
        intervalId.current = null;
      }
      return;
    }

    const tick = async () => {
      try {
        await savedCallback.current();
      } catch (error) {
        console.error('Polling callback error:', error);
      }
    };

    // Execute immediately if requested
    if (immediate) {
      tick();
    }

    // Set up interval
    intervalId.current = setInterval(tick, interval);

    // Cleanup
    return () => {
      if (intervalId.current) {
        clearInterval(intervalId.current);
        intervalId.current = null;
      }
    };
  }, [interval, immediate, enabled]);

  // Manual trigger function
  const trigger = () => {
    savedCallback.current();
  };

  return { trigger };
};
