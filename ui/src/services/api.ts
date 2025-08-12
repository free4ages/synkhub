import axios, { AxiosInstance, AxiosResponse } from 'axios';
import {
  JobSummary,
  JobDetail,
  RunSummary,
  RunDetail,
  LogEntry,
  ApiResponse,
  SystemStatus,
  RunsQueryParams,
  LogsQueryParams,
} from '../types/api';

class ApiService {
  private client: AxiosInstance;

  constructor(baseURL: string = '/api') {
    this.client = axios.create({
      baseURL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor for logging
    this.client.interceptors.request.use(
      (config) => {
        console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
        return config;
      },
      (error) => {
        console.error('API Request Error:', error);
        return Promise.reject(error);
      }
    );

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => {
        return response;
      },
      (error) => {
        console.error('API Response Error:', error);
        
        // Handle different error types
        if (error.response) {
          // Server responded with error status
          const status = error.response.status;
          const message = error.response.data?.detail || error.message;
          
          switch (status) {
            case 404:
              throw new Error(`Resource not found: ${message}`);
            case 500:
              throw new Error(`Server error: ${message}`);
            default:
              throw new Error(`API error (${status}): ${message}`);
          }
        } else if (error.request) {
          // Network error
          throw new Error('Network error: Unable to connect to server');
        } else {
          // Other error
          throw new Error(`Request error: ${error.message}`);
        }
      }
    );
  }

  // Job-related API calls
  async getJobs(): Promise<JobSummary[]> {
    const response: AxiosResponse<JobSummary[]> = await this.client.get('/jobs/');
    return response.data;
  }

  async getJobDetail(jobName: string): Promise<JobDetail> {
    const response: AxiosResponse<JobDetail> = await this.client.get(`/jobs/${jobName}`);
    return response.data;
  }

  async reloadJobConfig(jobName: string): Promise<ApiResponse> {
    const response: AxiosResponse<ApiResponse> = await this.client.post(`/jobs/${jobName}/reload`);
    return response.data;
  }

  // Run-related API calls
  async getAllRuns(params?: RunsQueryParams): Promise<RunSummary[]> {
    const response: AxiosResponse<RunSummary[]> = await this.client.get('/runs/', { params });
    return response.data;
  }

  async getJobRuns(jobName: string, params?: RunsQueryParams): Promise<RunSummary[]> {
    const response: AxiosResponse<RunSummary[]> = await this.client.get(`/runs/${jobName}`, { params });
    return response.data;
  }

  async getRunDetail(jobName: string, runId: string): Promise<RunDetail> {
    const response: AxiosResponse<RunDetail> = await this.client.get(`/runs/${jobName}/${runId}`);
    return response.data;
  }

  async getRunLogs(jobName: string, runId: string, params?: LogsQueryParams): Promise<LogEntry[]> {
    const response: AxiosResponse<LogEntry[]> = await this.client.get(`/runs/${jobName}/${runId}/logs`, { params });
    return response.data;
  }

  // System API calls
  async getSystemStatus(): Promise<SystemStatus> {
    const response: AxiosResponse<SystemStatus> = await this.client.get('/status');
    return response.data;
  }

  async getHealth(): Promise<{ status: string; service: string; version: string }> {
    const response = await this.client.get('/health');
    return response.data;
  }

  // Utility methods
  formatDuration(seconds?: number): string {
    if (!seconds) return 'N/A';
    
    if (seconds < 60) {
      return `${seconds.toFixed(1)}s`;
    } else if (seconds < 3600) {
      const minutes = Math.floor(seconds / 60);
      const remainingSeconds = Math.floor(seconds % 60);
      return `${minutes}m ${remainingSeconds}s`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      return `${hours}h ${minutes}m`;
    }
  }

  formatDateTime(dateString?: string): string {
    if (!dateString) return 'N/A';
    
    try {
      const date = new Date(dateString);
      return date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      });
    } catch {
      return dateString;
    }
  }

  formatRelativeTime(dateString?: string): string {
    if (!dateString) return 'N/A';
    
    try {
      const date = new Date(dateString);
      const now = new Date();
      const diffMs = now.getTime() - date.getTime();
      const diffMinutes = Math.floor(diffMs / (1000 * 60));
      
      if (diffMinutes < 1) {
        return 'Just now';
      } else if (diffMinutes < 60) {
        return `${diffMinutes}m ago`;
      } else if (diffMinutes < 1440) {
        const hours = Math.floor(diffMinutes / 60);
        return `${hours}h ago`;
      } else {
        const days = Math.floor(diffMinutes / 1440);
        return `${days}d ago`;
      }
    } catch {
      return dateString;
    }
  }

  getStatusColor(status: string): string {
    switch (status.toLowerCase()) {
      case 'completed':
        return 'text-success-600 bg-success-50';
      case 'running':
        return 'text-primary-600 bg-primary-50';
      case 'failed':
        return 'text-error-600 bg-error-50';
      default:
        return 'text-gray-600 bg-gray-50';
    }
  }

  getStatusIcon(status: string): string {
    switch (status.toLowerCase()) {
      case 'completed':
        return '✅';
      case 'running':
        return '🔄';
      case 'failed':
        return '❌';
      default:
        return '⚪';
    }
  }
}

// Export singleton instance
export const apiService = new ApiService();
export default apiService;
