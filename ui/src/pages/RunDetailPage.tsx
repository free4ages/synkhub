import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  ArrowLeft,
  Clock,
  User,
  Activity,
  Database,
  AlertCircle,
  CheckCircle,
  XCircle,
  BarChart3
} from 'lucide-react';
import { RunDetail } from '../types/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import { LogConsole } from '../components/runs/LogConsole';
import apiService from '../services/api';

export const RunDetailPage: React.FC = () => {
  const { jobName, runId } = useParams<{ jobName: string; runId: string }>();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchRunDetail = async () => {
    if (!jobName || !runId) return;

    try {
      setError(null);
      const runData = await apiService.getRunDetail(jobName, runId);
      setRun(runData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load run details');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRunDetail();
  }, [jobName, runId]);

  if (loading) {
    return (
      <div className="p-8">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <LoadingSpinner size="lg" className="mx-auto mb-4" />
            <p className="text-gray-600">Loading run details...</p>
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
              Failed to load run details
            </h3>
            <p className="text-gray-600 mb-4">{error}</p>
            <button
              onClick={fetchRunDetail}
              className="btn-primary"
            >
              Try Again
            </button>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (!run) {
    return (
      <div className="p-8">
        <Card>
          <CardContent className="text-center py-12">
            <Activity className="h-12 w-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Run not found
            </h3>
            <p className="text-gray-600 mb-4">
              The run could not be found.
            </p>
            <Link to="/runs" className="btn-primary">
              Back to Runs
            </Link>
          </CardContent>
        </Card>
      </div>
    );
  }

  const successRate = run.partition_count > 0 
    ? ((run.successful_partitions / run.partition_count) * 100).toFixed(1)
    : '0';

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center mb-4">
          <Link
            to="/runs"
            className="mr-4 p-2 text-gray-400 hover:text-gray-600 transition-colors"
          >
            <ArrowLeft className="h-5 w-5" />
          </Link>
          <div className="flex-1">
            <h1 className="text-3xl font-bold text-gray-900">
              Run Details
            </h1>
            <div className="flex items-center gap-4 mt-2">
              <div className="flex items-center text-gray-600">
                <User className="h-4 w-4 mr-1" />
                <Link 
                  to={`/jobs/${run.job_name}`}
                  className="hover:text-primary-600 transition-colors"
                >
                  {run.job_name}
                </Link>
              </div>
              <div className="flex items-center text-gray-600">
                <Database className="h-4 w-4 mr-1" />
                {run.strategy_name}
              </div>
              <StatusBadge status={run.status} />
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Run Details */}
        <div className="lg:col-span-1 space-y-6">
          {/* Basic Info */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <Activity className="h-5 w-5 mr-2" />
                Run Information
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div>
                  <label className="text-sm font-medium text-gray-700">Run ID</label>
                  <p className="text-sm text-gray-900 mt-1 font-mono">
                    {run.run_id}
                  </p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-700">Started</label>
                  <p className="text-sm text-gray-900 mt-1">
                    {apiService.formatDateTime(run.start_time)}
                  </p>
                  <p className="text-xs text-gray-500">
                    {apiService.formatRelativeTime(run.start_time)}
                  </p>
                </div>
                {run.end_time && (
                  <div>
                    <label className="text-sm font-medium text-gray-700">Completed</label>
                    <p className="text-sm text-gray-900 mt-1">
                      {apiService.formatDateTime(run.end_time)}
                    </p>
                    <p className="text-xs text-gray-500">
                      {apiService.formatRelativeTime(run.end_time)}
                    </p>
                  </div>
                )}
                <div>
                  <label className="text-sm font-medium text-gray-700">Duration</label>
                  <p className="text-sm text-gray-900 mt-1">
                    {apiService.formatDuration(run.duration_seconds)}
                  </p>
                </div>
                {run.error_message && (
                  <div>
                    <label className="text-sm font-medium text-gray-700">Error</label>
                    <p className="text-sm text-error-600 mt-1 p-2 bg-error-50 rounded border">
                      {run.error_message}
                    </p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Statistics */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <BarChart3 className="h-5 w-5 mr-2" />
                Statistics
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="text-center p-3 bg-primary-50 rounded-lg">
                    <div className="text-2xl font-bold text-primary-600">
                      {run.rows_processed.toLocaleString()}
                    </div>
                    <div className="text-xs text-primary-700 font-medium">
                      Rows Processed
                    </div>
                  </div>
                  <div className="text-center p-3 bg-success-50 rounded-lg">
                    <div className="text-2xl font-bold text-success-600">
                      {run.rows_inserted.toLocaleString()}
                    </div>
                    <div className="text-xs text-success-700 font-medium">
                      Rows Inserted
                    </div>
                  </div>
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div className="text-center p-3 bg-warning-50 rounded-lg">
                    <div className="text-2xl font-bold text-warning-600">
                      {run.rows_updated.toLocaleString()}
                    </div>
                    <div className="text-xs text-warning-700 font-medium">
                      Rows Updated
                    </div>
                  </div>
                  <div className="text-center p-3 bg-error-50 rounded-lg">
                    <div className="text-2xl font-bold text-error-600">
                      {run.rows_deleted.toLocaleString()}
                    </div>
                    <div className="text-xs text-error-700 font-medium">
                      Rows Deleted
                    </div>
                  </div>
                </div>

                <div className="pt-4 border-t border-gray-200">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium text-gray-700">Partitions</span>
                    <span className="text-sm text-gray-600">
                      {run.successful_partitions}/{run.partition_count}
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div
                      className="bg-success-600 h-2 rounded-full"
                      style={{
                        width: `${successRate}%`
                      }}
                    />
                  </div>
                  <div className="flex items-center justify-between mt-1 text-xs text-gray-500">
                    <span>Success rate: {successRate}%</span>
                    {run.failed_partitions > 0 && (
                      <span className="text-error-600">
                        {run.failed_partitions} failed
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Logs */}
        <div className="lg:col-span-2">
          <LogConsole jobName={run.job_name} runId={run.run_id} />
        </div>
      </div>
    </div>
  );
};
