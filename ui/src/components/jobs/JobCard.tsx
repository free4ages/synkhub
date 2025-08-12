import React from 'react';
import { Link } from 'react-router-dom';
import { 
  Calendar,
  Clock,
  Activity,
  ChevronRight,
  Settings
} from 'lucide-react';
import { JobSummary } from '../../types/api';
import { Card, CardContent } from '../ui/Card';
import { StatusBadge } from '../ui/StatusBadge';
import { cn } from '../../utils/cn';
import apiService from '../../services/api';

interface JobCardProps {
  job: JobSummary;
  className?: string;
}

export const JobCard: React.FC<JobCardProps> = ({ job, className }) => {
  const hasRecentRun = job.last_run && job.last_status;

  return (
    <Card className={cn('hover:shadow-md transition-shadow', className)}>
      <CardContent className="p-0">
        <div className="p-6">
          {/* Header */}
          <div className="flex items-start justify-between mb-4">
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-gray-900 mb-1">
                {job.name}
              </h3>
              <p className="text-sm text-gray-600 line-clamp-2">
                {job.description}
              </p>
            </div>
            <Link
              to={`/jobs/${job.name}`}
              className="ml-4 p-2 text-gray-400 hover:text-gray-600 transition-colors"
            >
              <ChevronRight className="h-5 w-5" />
            </Link>
          </div>

          {/* Status and Last Run */}
          {hasRecentRun && (
            <div className="flex items-center justify-between mb-4">
              <StatusBadge status={job.last_status!} />
              <div className="flex items-center text-sm text-gray-500">
                <Clock className="h-4 w-4 mr-1" />
                {apiService.formatRelativeTime(job.last_run)}
              </div>
            </div>
          )}

          {/* Strategies */}
          <div className="mb-4">
            <div className="flex items-center mb-2">
              <Settings className="h-4 w-4 text-gray-400 mr-2" />
              <span className="text-sm font-medium text-gray-700">Strategies</span>
            </div>
            <div className="flex flex-wrap gap-2">
              {job.strategies.map((strategy) => (
                <span
                  key={strategy}
                  className={cn(
                    'inline-flex items-center px-2 py-1 rounded text-xs font-medium',
                    job.enabled_strategies.includes(strategy)
                      ? 'bg-primary-100 text-primary-800'
                      : 'bg-gray-100 text-gray-600'
                  )}
                >
                  {strategy}
                  {job.enabled_strategies.includes(strategy) && (
                    <span className="ml-1 text-xs">●</span>
                  )}
                </span>
              ))}
            </div>
          </div>

          {/* Footer Stats */}
          <div className="flex items-center justify-between pt-4 border-t border-gray-100">
            <div className="flex items-center text-sm text-gray-500">
              <Activity className="h-4 w-4 mr-1" />
              <span>{job.total_runs} runs</span>
            </div>
            
            <div className="flex items-center gap-4">
              <div className="text-xs text-gray-400">
                {job.enabled_strategies.length} of {job.strategies.length} enabled
              </div>
              <Link
                to={`/runs?job=${job.name}`}
                className="text-sm text-primary-600 hover:text-primary-700 font-medium"
              >
                View runs →
              </Link>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
