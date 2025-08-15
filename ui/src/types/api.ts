// API response types matching the backend Pydantic models

export interface JobSummary {
  name: string;
  description: string;
  strategies: string[];
  enabled_strategies: string[];
  last_run?: string;
  last_status?: string;
  total_runs: number;
}

export interface StrategyDetail {
  name: string;
  type: string;
  enabled: boolean;
  column: string;
  cron?: string;
  sub_partition_step: number;
  page_size?: number;
}

export interface JobDetail {
  name: string;
  description: string;
  partition_key: string;
  partition_step: number;
  max_concurrent_partitions: number;
  strategies: StrategyDetail[];
  source_config: Record<string, any>;
  destination_config: Record<string, any>;
  column_mappings: Record<string, any>[];
}

export interface RunSummary {
  run_id: string;
  job_name: string;
  strategy_name: string;
  start_time: string;
  end_time?: string;
  status: string;
  duration_seconds?: number;
  rows_fetched: number;
  rows_detected: number;
  rows_inserted: number;
  rows_updated: number;
  rows_deleted: number;
  partition_count: number;
  successful_partitions: number;
  failed_partitions: number;
}

export interface RunDetail {
  run_id: string;
  job_name: string;
  strategy_name: string;
  start_time: string;
  end_time?: string;
  status: string;
  duration_seconds?: number;
  rows_fetched: number;
  rows_detected: number;
  rows_inserted: number;
  rows_updated: number;
  rows_deleted: number;
  error_message?: string;
  partition_count: number;
  successful_partitions: number;
  failed_partitions: number;
}

export interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
  run_id: string;
}

export interface ApiResponse<T = any> {
  success: boolean;
  message: string;
  data?: T;
}

export interface SystemStatus {
  status: string;
  statistics: {
    total_jobs: number;
    total_runs: number;
    active_runs: number;
    failed_runs: number;
  };
  config: {
    metrics_dir: string;
    max_runs_per_job: number;
  };
}

// Query parameters for API calls
export interface RunsQueryParams {
  limit?: number;
  offset?: number;
  status?: string;
}

export interface LogsQueryParams {
  limit?: number;
  level?: string;
}

// Status constants
export const RUN_STATUS = {
  RUNNING: 'running',
  COMPLETED: 'completed',
  FAILED: 'failed',
} as const;

export type RunStatus = typeof RUN_STATUS[keyof typeof RUN_STATUS];

export const LOG_LEVELS = {
  DEBUG: 'DEBUG',
  INFO: 'INFO',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
} as const;

export type LogLevel = typeof LOG_LEVELS[keyof typeof LOG_LEVELS];

// DataStore types
export interface DataStoreResponse {
  name: string;
  type: string;
  description?: string;
  tags: string[];
  connection_summary: Record<string, any>;
}

export interface DataStorageResponse {
  datastores: DataStoreResponse[];
  total_count: number;
}

export interface DataStoreTypesSummary {
  [type: string]: {
    count: number;
    datastores: string[];
  };
}

export interface DataStoreTagsSummary {
  [tag: string]: {
    count: number;
    datastores: string[];
  };
}

export interface ConnectionTestResponse {
  status: 'success' | 'error';
  message: string;
}

// DataStore constants
export const DATASTORE_TYPES = {
  POSTGRES: 'postgres',
  MYSQL: 'mysql',
  CLICKHOUSE: 'clickhouse',
  DUCKDB: 'duckdb',
  OBJECT_STORAGE: 'object_storage',
} as const;

export type DataStoreType = typeof DATASTORE_TYPES[keyof typeof DATASTORE_TYPES];
