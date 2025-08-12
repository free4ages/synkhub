import { useState, useCallback } from 'react';
import { usePolling } from './usePolling';

interface UseAutoRefreshOptions {
  interval?: number;
  immediate?: boolean;
  enabled?: boolean;
}

export const useAutoRefresh = <T>(
  fetchFunction: () => Promise<T>,
  options: UseAutoRefreshOptions = {}
) => {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setError(null);
      const result = await fetchFunction();
      setData(result);
      setLastUpdated(new Date());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [fetchFunction]);

  const { trigger: refresh } = usePolling(fetchData, options);

  const manualRefresh = useCallback(async () => {
    setLoading(true);
    await fetchData();
  }, [fetchData]);

  return {
    data,
    loading,
    error,
    lastUpdated,
    refresh: manualRefresh,
    autoRefresh: refresh,
  };
};
