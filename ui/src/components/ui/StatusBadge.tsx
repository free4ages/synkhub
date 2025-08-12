import React from 'react';
import { cn } from '../../utils/cn';

interface StatusBadgeProps {
  status: string;
  className?: string;
}

export const StatusBadge: React.FC<StatusBadgeProps> = ({ status, className }) => {
  const getStatusStyles = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
        return 'bg-success-100 text-success-800';
      case 'running':
        return 'bg-primary-100 text-primary-800';
      case 'failed':
        return 'bg-error-100 text-error-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
        return '✓';
      case 'running':
        return '⟳';
      case 'failed':
        return '✗';
      default:
        return '○';
    }
  };

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium',
        getStatusStyles(status),
        className
      )}
    >
      <span className="text-xs">{getStatusIcon(status)}</span>
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </span>
  );
};
