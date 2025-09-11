export interface DatabaseConnection {
  type: 'postgres' | 'clickhouse' | 'starrocks' | 'mysql';
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  db_schema?: string;
}

export interface TableInfo {
  table_name: string;
  alias: string;
  schema_name?: string;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
  primary_key: boolean;
  unique: boolean;
  auto_increment: boolean;
  default_value?: string;
  max_length?: number;
  comment?: string;
  table_alias?: string; // For multi-table support
}

export interface ExtractedSchema {
  table_name: string;
  schema_name?: string;
  database_name?: string;
  columns: ColumnInfo[];
  primary_keys: string[];
  indexes: any[];
  // For multi-table support
  tables?: TableInfo[];
}

export interface ColumnMapping {
  name: string;
  src?: string;           // Source expression (e.g., "u.id", "p.email")
  dest?: string;          // Destination column name
  dtype?: string;         // Data type for the column
  unique_column: boolean;    // Used for identifying records during sync (upserts, conflict resolution)
  order_column: boolean;     // Used for sorting data during sync operations
  hash_key: boolean;      // Used for calculating row/block hashes for change detection
  insert: boolean;        // Whether this column should be inserted into destination
  direction?: string;     // Sort direction for order_column (asc/desc)
}

export interface FilterCondition {
  field: string;
  operator: '=' | '!=' | '>' | '<' | '>=' | '<=' | 'LIKE' | 'IN' | 'NOT IN' | 'IS NULL' | 'IS NOT NULL';
  value?: string | number | boolean | string[];
  table_alias?: string;
}

export interface JoinCondition {
  table: string;
  alias: string;
  on: string;
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
}

export interface EnrichmentTransformation {
  columns: string[];
  transform: string;
  dest: string;
  dtype: string;
}

export interface StrategyConfig {
  name: string;
  type: 'delta' | 'hash' | 'full';
  enabled: boolean;
  column?: string;
  sub_partition_step?: number;
  interval_reduction_factor?: number;
  intervals?: number[];
  prevent_update_unless_changed?: boolean;
  page_size?: number;
  cron?: string;
}

export interface MigrationConfig {
  name: string;
  description: string;
  source_provider: any;
  destination_provider: any;
  column_map: ColumnMapping[];
  partition_column?: string;    // Column used for partitioning data during sync
  partition_step?: number;   // Number of records per partition
  // New fields for enhanced functionality
  source_filters?: FilterCondition[];
  destination_filters?: FilterCondition[];
  joins?: JoinCondition[];
  strategies?: StrategyConfig[];
  enrichment?: {
    enabled: boolean;
    transformations: EnrichmentTransformation[];
  };
}

export interface SchemaExtractionResponse {
  success: boolean;
  schema: ExtractedSchema;
  suggested_config: MigrationConfig;
}

export interface DDLGenerationResponse {
  success: boolean;
  ddl: string;
  config_yaml: string;
  config_json: string;
}
