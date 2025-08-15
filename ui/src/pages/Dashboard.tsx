import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import {
  Activity,
  Database,
  Clock,
  AlertTriangle,
  TrendingUp,
  Users,
  CheckCircle,
  XCircle
} from 'lucide-react';
import { JobSummary, SystemStatus, RunSummary } from '../types/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/Card';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingSpinner } from '../components/ui/LoadingSpinner';
import apiService from '../services/api';

export const Dashboard: React.FC = () => {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [recentRuns, setRecentRuns] = useState<RunSummary[]>([]);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchDashboardData = async () => {
    try {
      setError(null);
      const [jobsData, runsData, statusData] = await Promise.all([
        apiService.getJobs(),
        apiService.getAllRuns({ limit: 10 }),
        apiService.getSystemStatus(),
      ]);

      setJobs(jobsData);
      setRecentRuns(runsData);
      setSystemStatus(statusData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDashboardData();
    
    // Refresh every 30 seconds
    const interval = setInterval(fetchDashboardData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="p-8">
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <LoadingSpinner size="lg" className="mx-auto mb-4" />
            <p className="text-gray-600">Loading dashboard...</p>
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
            <AlertTriangle className="h-12 w-12 text-error-500 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">
              Failed to load dashboard
            </h3>
            <p className="text-gray-600 mb-4">{error}</p>
            <button
              onClick={fetchDashboardData}
              className="btn-primary"
            >
              Try Again
            </button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const stats = systemStatus?.statistics || {
    total_jobs: 0,
    total_runs: 0,
    active_runs: 0,
    failed_runs: 0,
  };

  const completedRuns = stats.total_runs - stats.active_runs - stats.failed_runs;
  const successRate = stats.total_runs > 0 
    ? ((completedRuns / stats.total_runs) * 100).toFixed(1)
    : '0';

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-gray-600 mt-2">
          Monitor your sync jobs and system performance
        </p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <Card>
          <CardContent className="flex items-center">
            <div className="flex items-center justify-center w-12 h-12 bg-primary-100 rounded-lg mr-4">
              <Database className="h-6 w-6 text-primary-600" />
            </div>
            <div>
              <p className="text-sm font-medium text-gray-600">Total Jobs</p>
              <p className="text-2xl font-bold text-gray-900">{stats.total_jobs}</p>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="flex items-center">
            <div className="flex items-center justify-center w-12 h-12 bg-success-100 rounded-lg mr-4">
              <Activity className="h-6 w-6 text-success-600" />
            </div>
            <div>
              <p className="text-sm font-medium text-gray-600">Active Runs</p>
              <p className="text-2xl font-bold text-gray-900">{stats.active_runs}</p>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="flex items-center">
            <div className="flex items-center justify-center w-12 h-12 bg-error-100 rounded-lg mr-4">
              <XCircle className="h-6 w-6 text-error-600" />
            </div>
            <div>
              <p className="text-sm font-medium text-gray-600">Failed Runs</p>
              <p className="text-2xl font-bold text-gray-900">{stats.failed_runs}</p>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="flex items-center">
            <div className="flex items-center justify-center w-12 h-12 bg-warning-100 rounded-lg mr-4">
              <TrendingUp className="h-6 w-6 text-warning-600" />
            </div>
            <div>
              <p className="text-sm font-medium text-gray-600">Success Rate</p>
              <p className="text-2xl font-bold text-gray-900">{successRate}%</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Recent Jobs */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center">
                <Database className="h-5 w-5 mr-2" />
                Recent Jobs
              </CardTitle>
              <Link to="/jobs" className="text-sm text-primary-600 hover:text-primary-700">
                View all →
              </Link>
            </div>
          </CardHeader>
          <CardContent>
            {jobs.length === 0 ? (
              <div className="text-center py-8">
                <Database className="h-8 w-8 text-gray-400 mx-auto mb-2" />
                <p className="text-gray-600">No jobs configured</p>
              </div>
            ) : (
              <div className="space-y-4">
                {jobs.slice(0, 5).map((job) => (
                  <div
                    key={job.name}
                    className="flex items-center justify-between p-3 border border-gray-200 rounded-lg hover:bg-gray-50"
                  >
                    <div className="flex-1">
                      <h4 className="font-medium text-gray-900">{job.name}</h4>
                      <p className="text-sm text-gray-600 line-clamp-1">
                        {job.description}
                      </p>
                      <div className="flex items-center mt-1">
                        <span className="text-xs text-gray-500">
                          {job.enabled_strategies.length} of {job.strategies.length} strategies enabled
                        </span>
                      </div>
                    </div>
                    <div className="ml-4 text-right">
                      {job.last_status && (
                        <StatusBadge status={job.last_status} className="mb-1" />
                      )}
                      <div className="text-xs text-gray-500">
                        {job.total_runs} runs
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Recent Runs */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center">
                <Activity className="h-5 w-5 mr-2" />
                Recent Runs
              </CardTitle>
              <Link to="/runs" className="text-sm text-primary-600 hover:text-primary-700">
                View all →
              </Link>
            </div>
          </CardHeader>
          <CardContent>
            {recentRuns.length === 0 ? (
              <div className="text-center py-8">
                <Activity className="h-8 w-8 text-gray-400 mx-auto mb-2" />
                <p className="text-gray-600">No runs found</p>
              </div>
            ) : (
              <div className="space-y-4">
                {recentRuns.map((run) => (
                  <div
                    key={run.run_id}
                    className="flex items-center justify-between p-3 border border-gray-200 rounded-lg hover:bg-gray-50"
                  >
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <h4 className="font-medium text-gray-900">{run.job_name}</h4>
                        <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">
                          {run.strategy_name}
                        </span>
                      </div>
                      <div className="flex items-center gap-4 text-sm text-gray-600">
                        <div className="flex items-center">
                          <Clock className="h-3 w-3 mr-1" />
                          {apiService.formatRelativeTime(run.start_time)}
                        </div>
                        <div className="flex items-center">
                          <Users className="h-3 w-3 mr-1" />
                          {run.rows_detected.toLocaleString()} rows detected
                        </div>
                      </div>
                    </div>
                    <div className="ml-4 text-right">
                      <StatusBadge status={run.status} className="mb-1" />
                      <div className="text-xs text-gray-500">
                        {apiService.formatDuration(run.duration_seconds)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* System Status */}
      {systemStatus && (
        <div className="mt-8">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <CheckCircle className="h-5 w-5 mr-2 text-success-600" />
                System Status
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div>
                  <h4 className="font-medium text-gray-900 mb-2">Service</h4>
                  <div className="flex items-center">
                    <div className="w-2 h-2 bg-success-500 rounded-full mr-2"></div>
                    <span className="text-sm text-gray-600 capitalize">
                      {systemStatus.status}
                    </span>
                  </div>
                </div>
                <div>
                  <h4 className="font-medium text-gray-900 mb-2">Configuration</h4>
                  <div className="text-sm text-gray-600">
                    <div>Metrics: {systemStatus.config.metrics_dir}</div>
                    <div>Max runs per job: {systemStatus.config.max_runs_per_job}</div>
                  </div>
                </div>
                <div>
                  <h4 className="font-medium text-gray-900 mb-2">Performance</h4>
                  <div className="text-sm text-gray-600">
                    <div>Total runs: {stats.total_runs.toLocaleString()}</div>
                    <div>Success rate: {successRate}%</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
};
