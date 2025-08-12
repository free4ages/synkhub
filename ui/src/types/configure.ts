export interface DatabaseConnection {
  type: 'postgres' | 'clickhouse' | 'starrocks' | 'mysql';
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  db_schema?: string;
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
}

export interface ExtractedSchema {
  table_name: string;
  schema_name?: string;
  database_name?: string;
  columns: ColumnInfo[];
  primary_keys: string[];
  indexes: any[];
}

export interface ColumnMapping {
  name: string;
  src?: string;           // Source expression (e.g., "u.id", "p.email")
  dest?: string;          // Destination column name
  dtype?: string;         // Data type for the column
  unique_key: boolean;    // Used for identifying records during sync (upserts, conflict resolution)
  order_key: boolean;     // Used for sorting data during sync operations
  hash_key: boolean;      // Used for calculating row/block hashes for change detection
  insert: boolean;        // Whether this column should be inserted into destination
  direction?: string;     // Sort direction for order_key (asc/desc)
}

export interface MigrationConfig {
  name: string;
  description: string;
  source_provider: any;
  destination_provider: any;
  column_map: ColumnMapping[];
  partition_key?: string;    // Column used for partitioning data during sync
  partition_step?: number;   // Number of records per partition
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
