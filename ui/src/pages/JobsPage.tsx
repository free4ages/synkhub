import React from 'react';
import { JobsList } from '../components/jobs/JobsList';

export const JobsPage: React.FC = () => {
  return (
    <div className="p-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Sync Jobs</h1>
        <p className="text-gray-600 mt-2">
          Manage and monitor your data synchronization jobs
        </p>
      </div>

      <JobsList />
    </div>
  );
};
