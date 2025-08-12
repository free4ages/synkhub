import React from 'react';
import { useSearchParams } from 'react-router-dom';
import { RunsTable } from '../components/runs/RunsTable';

export const RunsPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const jobFilter = searchParams.get('job');

  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">
          {jobFilter ? `Runs for ${jobFilter}` : 'All Runs'}
        </h1>
        <p className="text-gray-600 mt-2">
          {jobFilter 
            ? `View all runs for the ${jobFilter} job`
            : 'View and monitor all sync job runs across the system'
          }
        </p>
      </div>

      <RunsTable jobName={jobFilter || undefined} />
    </div>
  );
};
