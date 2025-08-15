import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { 
  Clock,
  User,
  Activity,
  ChevronRight,
  Filter,
  RefreshCw,
  AlertCircle
} from 'lucide-react';
import { RunSummary, RunsQueryParams, RUN_STATUS } from '../../types/api';
import { StatusBadge } from '../ui/StatusBadge';
import { LoadingSpinner } from '../ui/LoadingSpinner';
import { Card, CardContent } from '../ui/Card';
import apiService from '../../services/api';

interface RunsTableProps {
  jobName?: string;
  className?: string;
}

export const RunsTable: React.FC<RunsTableProps> = ({ jobName, className }) => {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [statusFilter, setStatusFilter] = useState<string>('');
  const [currentPage, setCurrentPage] = useState(0);
  const [hasMore, setHasMore] = useState(true);

  const PAGE_SIZE = 20;

  const fetchRuns = async (reset = false) => {
    try {
      setError(null);
      if (reset) {
        setCurrentPage(0);
      }

      const params: RunsQueryParams = {
        limit: PAGE_SIZE,
        offset: reset ? 0 : currentPage * PAGE_SIZE,
        ...(statusFilter && { status: statusFilter }),
      };

      const runsData = jobName
        ? await apiService.getJobRuns(jobName, params)
        : await apiService.getAllRuns(params);

      if (reset) {
        setRuns(runsData);
      } else {
        setRuns(prev => [...prev, ...runsData]);
      }

      setHasMore(runsData.length === PAGE_SIZE);
      
      if (!reset) {
        setCurrentPage(prev => prev + 1);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch runs');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    await fetchRuns(true);
  };

  const handleStatusFilter = async (status: string) => {
    setStatusFilter(status);
    setLoading(true);
    await fetchRuns(true);
  };

  const loadMore = () => {
    fetchRuns(false);
  };

  useEffect(() => {
    fetchRuns(true);
  }, [jobName, statusFilter]);

  if (loading && runs.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <LoadingSpinner size="lg" className="mx-auto mb-4" />
          <p className="text-gray-600">Loading runs...</p>
        </div>
      </div>
    );
  }

  if (error && runs.length === 0) {
    return (
      <Card>
        <CardContent className="text-center py-12">
          <AlertCircle className="h-12 w-12 text-error-500 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            Failed to load runs
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
        </CardContent>
      </Card>
    );
  }

  return (
    <div className={className}>
      {/* Filters and Controls */}
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="flex items-center">
            <Filter className="h-4 w-4 text-gray-400 mr-2" />
            <select
              value={statusFilter}
              onChange={(e) => handleStatusFilter(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
            >
              <option value="">All statuses</option>
              <option value={RUN_STATUS.RUNNING}>Running</option>
              <option value={RUN_STATUS.COMPLETED}>Completed</option>
              <option value={RUN_STATUS.FAILED}>Failed</option>
            </select>
          </div>
        </div>

        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="btn-secondary"
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Runs Table */}
      {runs.length === 0 ? (
        <Card>
          <CardContent className="text-center py-12">
            <Activity className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              No runs found
            </h3>
            <p className="text-gray-600">
              {statusFilter
                ? `No runs with status "${statusFilter}"`
                : 'No runs have been executed yet'}
            </p>
          </CardContent>
        </Card>
      ) : (
        <Card padding="none">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="table-header">Job</th>
                  <th className="table-header">Strategy</th>
                  <th className="table-header">Status</th>
                  <th className="table-header">Started</th>
                  <th className="table-header">Duration</th>
                  <th className="table-header">Rows</th>
                  <th className="table-header">Partitions</th>
                  <th className="table-header"></th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {runs.map((run) => (
                  <tr key={run.run_id} className="table-row">
                    <td className="table-cell">
                      <div className="flex items-center">
                        <User className="h-4 w-4 text-gray-400 mr-2" />
                        <div>
                          <div className="font-medium text-gray-900">
                            {run.job_name}
                          </div>
                          <div className="text-xs text-gray-500">
                            {run.run_id.slice(0, 8)}...
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="table-cell">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                        {run.strategy_name}
                      </span>
                    </td>
                    <td className="table-cell">
                      <StatusBadge status={run.status} />
                    </td>
                    <td className="table-cell">
                      <div className="flex items-center text-gray-600">
                        <Clock className="h-4 w-4 mr-1" />
                        <div>
                          <div className="text-sm">
                            {apiService.formatRelativeTime(run.start_time)}
                          </div>
                          <div className="text-xs text-gray-500">
                            {apiService.formatDateTime(run.start_time)}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="table-cell">
                      <span className="text-sm text-gray-900">
                        {apiService.formatDuration(run.duration_seconds)}
                      </span>
                    </td>
                    <td className="table-cell">
                      <div className="text-sm">
                        <div className="text-gray-900">
                          {run.rows_detected.toLocaleString()}
                        </div>
                        <div className="text-xs text-gray-500">
                          Fetched: {run.rows_fetched.toLocaleString()}
                        </div>
                        <div className="text-xs text-gray-500">
                          +{run.rows_inserted} -{run.rows_deleted} ~{run.rows_updated}
                        </div>
                      </div>
                    </td>
                    <td className="table-cell">
                      <div className="text-sm">
                        <div className="text-gray-900">
                          {run.successful_partitions}/{run.partition_count}
                        </div>
                        {run.failed_partitions > 0 && (
                          <div className="text-xs text-error-600">
                            {run.failed_partitions} failed
                          </div>
                        )}
                      </div>
                    </td>
                    <td className="table-cell">
                      <Link
                        to={`/runs/${run.job_name}/${run.run_id}`}
                        className="text-primary-600 hover:text-primary-700"
                      >
                        <ChevronRight className="h-4 w-4" />
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Load More */}
          {hasMore && (
            <div className="border-t border-gray-200 px-6 py-4">
              <button
                onClick={loadMore}
                disabled={loading}
                className="w-full btn-secondary"
              >
                {loading ? (
                  <>
                    <LoadingSpinner size="sm" className="mr-2" />
                    Loading more...
                  </>
                ) : (
                  'Load more runs'
                )}
              </button>
            </div>
          )}
        </Card>
      )}
    </div>
  );
};
