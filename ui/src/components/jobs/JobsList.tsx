import React, { useState, useEffect } from 'react';
import { Search, RefreshCw, AlertCircle } from 'lucide-react';
import { JobSummary } from '../../types/api';
import { JobCard } from './JobCard';
import { LoadingSpinner } from '../ui/LoadingSpinner';
import { Card, CardContent } from '../ui/Card';
import apiService from '../../services/api';

export const JobsList: React.FC = () => {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [filteredJobs, setFilteredJobs] = useState<JobSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [refreshing, setRefreshing] = useState(false);

  const fetchJobs = async () => {
    try {
      setError(null);
      const jobsData = await apiService.getJobs();
      setJobs(jobsData);
      setFilteredJobs(jobsData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch jobs');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    await fetchJobs();
  };

  const handleSearch = (term: string) => {
    setSearchTerm(term);
    if (!term.trim()) {
      setFilteredJobs(jobs);
    } else {
      const filtered = jobs.filter(job =>
        job.name.toLowerCase().includes(term.toLowerCase()) ||
        job.description.toLowerCase().includes(term.toLowerCase()) ||
        job.strategies.some(strategy =>
          strategy.toLowerCase().includes(term.toLowerCase())
        )
      );
      setFilteredJobs(filtered);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <LoadingSpinner size="lg" className="mx-auto mb-4" />
          <p className="text-gray-600">Loading jobs...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <Card>
        <CardContent className="text-center py-12">
          <AlertCircle className="h-12 w-12 text-error-500 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            Failed to load jobs
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
    <div className="space-y-6">
      {/* Header with Search and Refresh */}
      <div className="flex items-center justify-between">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search jobs..."
            value={searchTerm}
            onChange={(e) => handleSearch(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          />
        </div>
        
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="btn-secondary ml-4"
        >
          <RefreshCw className={`h-4 w-4 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Jobs Count */}
      <div className="text-sm text-gray-600">
        {filteredJobs.length} of {jobs.length} jobs
        {searchTerm && ` matching "${searchTerm}"`}
      </div>

      {/* Jobs Grid */}
      {filteredJobs.length === 0 ? (
        <Card>
          <CardContent className="text-center py-12">
            <div className="text-gray-400 mb-4">
              {searchTerm ? (
                <>
                  <Search className="h-12 w-12 mx-auto mb-4" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    No jobs found
                  </h3>
                  <p>No jobs match your search criteria.</p>
                </>
              ) : (
                <>
                  <div className="h-12 w-12 mx-auto mb-4 bg-gray-100 rounded-full flex items-center justify-center">
                    <span className="text-2xl">📋</span>
                  </div>
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    No jobs configured
                  </h3>
                  <p>Add job configurations to get started.</p>
                </>
              )}
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredJobs.map((job) => (
            <JobCard key={job.name} job={job} />
          ))}
        </div>
      )}
    </div>
  );
};
