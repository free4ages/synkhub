import React, { useState, useEffect, useRef } from 'react';
import { 
  Terminal,
  Download,
  Filter,
  RefreshCw,
  AlertCircle
} from 'lucide-react';
import { LogEntry, LogsQueryParams, LOG_LEVELS } from '../../types/api';
import { LoadingSpinner } from '../ui/LoadingSpinner';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import apiService from '../../services/api';

interface LogConsoleProps {
  jobName: string;
  runId: string;
  className?: string;
}

export const LogConsole: React.FC<LogConsoleProps> = ({ 
  jobName, 
  runId, 
  className 
}) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [levelFilter, setLevelFilter] = useState<string>('');
  const [autoScroll, setAutoScroll] = useState(true);
  
  const logContainerRef = useRef<HTMLDivElement>(null);

  const fetchLogs = async () => {
    try {
      setError(null);
      const params: LogsQueryParams = {
        limit: 1000,
        ...(levelFilter && { level: levelFilter }),
      };

      const logsData = await apiService.getRunLogs(jobName, runId, params);
      setLogs(logsData);

      // Auto-scroll to bottom if enabled
      if (autoScroll && logContainerRef.current) {
        setTimeout(() => {
          logContainerRef.current?.scrollTo({
            top: logContainerRef.current.scrollHeight,
            behavior: 'smooth'
          });
        }, 100);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch logs');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    await fetchLogs();
  };

  const handleLevelFilter = async (level: string) => {
    setLevelFilter(level);
    setLoading(true);
    await fetchLogs();
  };

  const downloadLogs = () => {
    const logText = logs
      .map(log => `[${log.timestamp}] ${log.level}: ${log.message}`)
      .join('\n');
    
    const blob = new Blob([logText], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${jobName}-${runId}-logs.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const getLevelColor = (level: string) => {
    switch (level.toUpperCase()) {
      case 'DEBUG':
        return 'text-gray-600';
      case 'INFO':
        return 'text-primary-600';
      case 'WARNING':
        return 'text-warning-600';
      case 'ERROR':
        return 'text-error-600';
      default:
        return 'text-gray-600';
    }
  };

  useEffect(() => {
    fetchLogs();
  }, [jobName, runId, levelFilter]);

  return (
    <Card className={className}>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center">
            <Terminal className="h-5 w-5 mr-2" />
            Logs
          </CardTitle>
          
          <div className="flex items-center gap-2">
            {/* Level Filter */}
            <div className="flex items-center">
              <Filter className="h-4 w-4 text-gray-400 mr-2" />
              <select
                value={levelFilter}
                onChange={(e) => handleLevelFilter(e.target.value)}
                className="border border-gray-300 rounded px-2 py-1 text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              >
                <option value="">All levels</option>
                {Object.values(LOG_LEVELS).map(level => (
                  <option key={level} value={level}>{level}</option>
                ))}
              </select>
            </div>

            {/* Auto-scroll toggle */}
            <label className="flex items-center text-sm">
              <input
                type="checkbox"
                checked={autoScroll}
                onChange={(e) => setAutoScroll(e.target.checked)}
                className="mr-1"
              />
              Auto-scroll
            </label>

            {/* Download button */}
            <button
              onClick={downloadLogs}
              disabled={logs.length === 0}
              className="btn-secondary text-xs px-2 py-1"
              title="Download logs"
            >
              <Download className="h-4 w-4" />
            </button>

            {/* Refresh button */}
            <button
              onClick={handleRefresh}
              disabled={refreshing}
              className="btn-secondary text-xs px-2 py-1"
            >
              <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>
      </CardHeader>

      <CardContent className="p-0">
        {loading && logs.length === 0 ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-center">
              <LoadingSpinner size="lg" className="mx-auto mb-4" />
              <p className="text-gray-600">Loading logs...</p>
            </div>
          </div>
        ) : error ? (
          <div className="text-center py-12">
            <AlertCircle className="h-12 w-12 text-error-500 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Failed to load logs
            </h3>
            <p className="text-gray-600 mb-4">{error}</p>
            <button
              onClick={handleRefresh}
              className="btn-primary"
              disabled={refreshing}
            >
              {refreshing ? (
                <>
                  <LoadingSpinner size="sm" className="mr-2" />
                  Retrying...
                </>
              ) : (
                'Try Again'
              )}
            </button>
          </div>
        ) : logs.length === 0 ? (
          <div className="text-center py-12">
            <Terminal className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              No logs found
            </h3>
            <p className="text-gray-600">
              {levelFilter
                ? `No logs with level "${levelFilter}"`
                : 'No logs available for this run'}
            </p>
          </div>
        ) : (
          <div
            ref={logContainerRef}
            className="h-96 overflow-y-auto bg-gray-900 text-gray-100 font-mono text-sm"
          >
            <div className="p-4 space-y-1">
              {logs.map((log, index) => (
                <div key={index} className="flex items-start gap-3">
                  <span className="text-gray-500 text-xs whitespace-nowrap">
                    {new Date(log.timestamp).toLocaleTimeString()}
                  </span>
                  <span className={`text-xs font-medium uppercase whitespace-nowrap ${getLevelColor(log.level)}`}>
                    {log.level.padEnd(5)}
                  </span>
                  <span className="flex-1 break-words">
                    {log.message}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};
