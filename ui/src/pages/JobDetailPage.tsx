import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  ArrowLeft,
  Database,
  Settings,
  Clock,
  Activity,
  RefreshCw,
  AlertCircle,
  CheckCircle,
  XCircle
} from 'lucide-react';
import { JobDetail, RunSummary } from '../types/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import { RunsTable } from '../components/runs/RunsTable';
import apiService from '../services/api';

export const JobDetailPage: React.FC = () => {
  const { jobName } = useParams<{ jobName: string }>();
  const [job, setJob] = useState<JobDetail | null>(null);
  const [recentRuns, setRecentRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloading, setReloading] = useState(false);

  const fetchJobDetail = async () => {
    if (!jobName) return;

    try {
      setError(null);
      const [jobData, runsData] = await Promise.all([
        apiService.getJobDetail(jobName),
        apiService.getJobRuns(jobName, { limit: 5 }),
      ]);

      setJob(jobData);
      setRecentRuns(runsData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load job details');
    } finally {
      setLoading(false);
    }
  };

  const handleReloadConfig = async () => {
    if (!jobName) return;

    setReloading(true);
    try {
      await apiService.reloadJobConfig(jobName);
      await fetchJobDetail(); // Refresh data after reload
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reload config');
    } finally {
      setReloading(false);
    }
  };

  useEffect(() => {
    fetchJobDetail();
  }, [jobName]);

  if (loading) {
    return (
      <div className="p-8">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <LoadingSpinner size="lg" className="mx-auto mb-4" />
            <p className="text-gray-600">Loading job details...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8">
        <Card>
          <CardContent className="text-center py-12">
            <AlertCircle className="h-12 w-12 text-error-500 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Failed to load job details
            </h3>
            <p className="text-gray-600 mb-4">{error}</p>
            <button
              onClick={fetchJobDetail}
              className="btn-primary"
            >
              Try Again
            </button>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="p-8">
        <Card>
          <CardContent className="text-center py-12">
            <Database className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Job not found
            </h3>
            <p className="text-gray-600 mb-4">
              The job "{jobName}" could not be found.
            </p>
            <Link to="/jobs" className="btn-primary">
              Back to Jobs
            </Link>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center mb-4">
          <Link
            to="/jobs"
            className="mr-4 p-2 text-gray-400 hover:text-gray-600 transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex-1">
            <h1 className="text-3xl font-bold text-gray-900">{job.name}</h1>
            <p className="text-gray-600 mt-2">{job.description}</p>
          </div>
          <button
            onClick={handleReloadConfig}
            disabled={reloading}
            className="btn-secondary"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${reloading ? 'animate-spin' : ''}`} />
            Reload Config
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Job Configuration */}
        <div className="lg:col-span-1 space-y-6">
          {/* Basic Info */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Database className="h-5 w-5 mr-2" />
                Configuration
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div>
                  <label className="text-sm font-medium text-gray-700">Partition Key</label>
                  <p className="text-sm text-gray-900 mt-1">{job.partition_column}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-700">Partition Step</label>
                  <p className="text-sm text-gray-900 mt-1">{job.partition_step.toLocaleString()}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-700">Max Concurrent Partitions</label>
                  <p className="text-sm text-gray-900 mt-1">{job.max_concurrent_partitions}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-700">Column Mappings</label>
                  <p className="text-sm text-gray-900 mt-1">{job.column_mappings.length} columns</p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Strategies */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Settings className="h-5 w-5 mr-2" />
                Strategies
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {job.strategies.map((strategy) => (
                  <div
                    key={strategy.name}
                    className="p-3 border border-gray-200 rounded-lg"
                  >
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="font-medium text-gray-900">{strategy.name}</h4>
                      <div className="flex items-center gap-2">
                        <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded uppercase">
                          {strategy.type}
                        </span>
                        {strategy.enabled ? (
                          <CheckCircle className="h-4 w-4 text-success-600" />
                        ) : (
                          <XCircle className="h-4 w-4 text-gray-400" />
                        )}
                      </div>
                    </div>
                    <div className="text-sm text-gray-600 space-y-1">
                      <div>Column: {strategy.column}</div>
                      <div>Sub-partition step: {strategy.sub_partition_step.toLocaleString()}</div>
                      <div>Page size: {strategy.page_size ? strategy.page_size.toLocaleString() : 'Not set'}</div>
                      {strategy.cron && (
                        <div className="flex items-center">
                          <Clock className="h-3 w-3 mr-1" />
                          {strategy.cron}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* Recent Runs Summary */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Activity className="h-5 w-5 mr-2" />
                Recent Activity
              </CardTitle>
            </CardHeader>
            <CardContent>
              {recentRuns.length === 0 ? (
                <div className="text-center py-4">
                  <Activity className="h-8 w-8 text-gray-400 mx-auto mb-2" />
                  <p className="text-gray-600">No recent runs</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {recentRuns.map((run) => (
                    <div key={run.run_id} className="flex items-center justify-between">
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">
                            {run.strategy_name}
                          </span>
                          <StatusBadge status={run.status} />
                        </div>
                        <div className="text-xs text-gray-500">
                          {apiService.formatRelativeTime(run.start_time)}
                        </div>
                      </div>
                      <div className="text-right text-xs text-gray-600">
                        <div>{run.rows_detected.toLocaleString()} rows detected</div>
                        <div>{apiService.formatDuration(run.duration_seconds)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div className="mt-4 pt-4 border-t border-gray-200">
                <Link
                  to={`/runs?job=${job.name}`}
                  className="text-sm text-primary-600 hover:text-primary-700 font-medium"
                >
                  View all runs →
                </Link>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Runs Table */}
        <div className="lg:col-span-2">
          <Card>
            <CardHeader>
              <CardTitle>All Runs</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              <RunsTable jobName={job.name} />
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};
