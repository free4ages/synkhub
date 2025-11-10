# SyncHub (Work In Progress)

A powerful, production-ready data synchronization tool for modern data engineering workflows. SyncTool provides intelligent, incremental data synchronization between databases with advanced features like automatic DDL management, distributed scheduling, and pipeline-based processing.

https://github.com/user-attachments/assets/1b662f01-cae2-412a-a750-4a331aade886

## 🌟 Features

### Core Capabilities
- **Multiple Sync Strategies**: Full, Delta (timestamp-based), and Hash-based synchronization
- **Intelligent Strategy Selection**: Automatically selects the best sync strategy based on time gap and historical patterns
- **Resumable Syncs**: Automatically resume from last successful partition on failure - no data loss or duplication
- **Distributed Architecture**: Scale horizontally with Redis-backed job queue and multiple workers
- **Pipeline-Based Processing**: Modular, memory-efficient streaming data processing with configurable stages
- **Universal Schema Mapping**: Automatic type conversion between different database systems

### Database Support
- **PostgreSQL** - Full support with DDL generation
- **StarRocks** - High-performance OLAP database
- **MySQL** - Traditional RDBMS support
- **ClickHouse** - Columnar database (planned)
- **DuckDB** - Embedded analytics (planned)
- **S3/MinIO** - Object storage for data lakes

### Advanced Features
- **Automatic DDL Management**: Generate and track DDL changes, with safe deployment workflows
- **Build & Deployment System**: Validate, build, and deploy configurations with change detection
- **Data Enrichment**: Join and enrich data from multiple sources with caching
- **Partitioned Processing**: Efficient handling of large datasets with automatic partitioning
- **Execution Locking**: Prevent concurrent runs and DDL conflicts with Redis-based distributed locks
- **Retry Logic**: Slot-aware retry with exponential backoff for failed jobs
- **Monitoring & Observability**: Built-in metrics, logging, and HTTP API for monitoring
- **Scheduler & Cron**: Schedule pipelines with cron expressions
- **Configuration Management**: Store configs in files or databases with versioning

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Sync Modes](#-sync-modes)
  - [Row-Level Sync](#row-level-sync)
  - [Aggregate Sync with Change Detection](#aggregate-sync-with-change-detection)
  - [Column-Level Change Detection](#column-level-change-detection)
- [Usage](#-usage)
- [CLI Commands](#-cli-commands)
- [HTTP API](#-http-api)
- [Examples](#-examples)
- [Deployment](#-deployment)
- [Monitoring](#-monitoring)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

## 🏗️ Architecture

SyncTool uses a distributed, scalable architecture:

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Config Files  │────▶│   Scheduler  │────▶│   Redis Queue   │
│  (YAML/DB)      │     │  (Enhanced   │     │   (ARQ Jobs)    │
└─────────────────┘     │   ARQ)       │     └─────────────────┘
                        └──────────────┘              │
                                                      │
                        ┌──────────────┐              │
                        │   Workers    │◀─────────────┘
                        │  (Scalable)  │
                        └──────────────┘
                               │
                        ┌──────▼───────┐
                        │   Pipeline   │
                        │  Processor   │
                        └──────────────┘
```

### Key Components

1. **Scheduler**: Reads configurations, determines which strategies to run, enqueues jobs to Redis
2. **Workers**: Pull jobs from Redis queue, execute pipelines, report back to scheduler
3. **Pipeline Processor**: Executes sync jobs with configurable processing stages
4. **Config Manager**: Manages pipeline configurations with validation and versioning
5. **State Manager**: Tracks pipeline execution state and history

### Pipeline Architecture

Data flows through modular, configurable stages:

```
ChangeDetection → DataFetch → Transform → Enrich → Batcher → Populate
   (Required)     (Optional)  (Optional)  (Optional) (Optional) (Required)
```

Each stage can be independently enabled/disabled and configured for optimal performance.

## 🚀 Installation

### Prerequisites
- Python 3.8 or higher
- Redis 5.0 or higher
- PostgreSQL, MySQL, or other supported database

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/synctool.git
cd synctool

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Docker Installation

```bash
# Start all services (scheduler, workers, and Redis)
docker-compose -f docker-compose-arq.yml up -d

# Scale workers as needed
docker-compose -f docker-compose-arq.yml up -d --scale worker=5
```

## ⚡ Quick Start

### 1. Start Redis

```bash
# Using Docker
docker run -d -p 6379:6379 redis:7-alpine

# Or use Docker Compose
docker-compose -f docker-compose-arq.yml up -d redis
```

### 2. Configure Datastores

Create `examples/configs/datastores.yaml`:

```yaml
datastores:
  source_db:
    type: 'postgres'
    connection:
      host: 'localhost'
      port: 5432
      user: 'your_user'
      password: 'your_password'
      dbname: 'source_database'
      schema: 'public'
  
  dest_db:
    type: 'postgres'
    connection:
      host: 'localhost'
      port: 5432
      user: 'your_user'
      password: 'your_password'
      dbname: 'dest_database'
      schema: 'public'
```

### 3. Create a Pipeline Configuration

Create `examples/configs/pipelines/my_first_pipeline.yaml`:

```yaml
name: "users_sync"
description: "Sync users from source to destination"
partition_step: 10000
partition_column: "id"

source_provider:
  data_backend:
    type: "postgres"
    datastore_name: "source_db"
    table: "users"
    schema: "public"

destination_provider:
  data_backend:
    type: "postgres"
    datastore_name: "dest_db"
    table: "users_copy"
    schema: "public"

column_map:
  - source: "id"
    dest: "id"
    roles: ["unique_column", "partition_column"]
    data_type: "int"
  - source: "name"
    dest: "name"
    data_type: "string"
  - source: "email"
    dest: "email"
    data_type: "string"
  - source: "updated_at"
    dest: "updated_at"
    data_type: "timestamp"

strategies:
  - name: "hourly_delta"
    type: "delta"
    column: "updated_at"
    enabled: true
    cron: "0 * * * *"  # Run every hour
    page_size: 1000
```

### 4. Start Scheduler

```bash
python -m synctool.cli.arq_scheduler_cli start \
    --config-dir ./examples/configs/pipelines
```

### 5. Start Workers

```bash
# Start a single worker
python -m synctool.worker.worker_cli start

# Or start multiple workers
python -m synctool.worker.worker_cli start --max-jobs 4 &
python -m synctool.worker.worker_cli start --max-jobs 4 &
python -m synctool.worker.worker_cli start --max-jobs 4 &
```

### 6. Monitor Status

```bash
# Check pipeline status
python -m synctool.cli.arq_scheduler_cli status

# View pipeline history
python -m synctool.cli.arq_scheduler_cli history users_sync

# Check via HTTP API
curl http://localhost:8001/health
curl http://localhost:8001/api/pipelines/states | jq
```

## 🔧 Configuration

### Global Configuration

Edit `synctool/config/global_config.yaml`:

```yaml
redis:
  url: "redis://localhost:6379"
  db: 0

scheduler:
  enabled: true
  http_port: 8001
  check_interval: 60
  slot_definitions:
    high_priority: 5
    normal: 10
    low_priority: 3

worker:
  max_jobs: 4
  health_check_interval: 30

storage:
  state_dir: "./data/pipeline_states"
  logs_dir: "./data/logs"
  metrics_dir: "./data/metrics"

build_system:
  user_config_dir: "./examples/configs/pipelines"
  built_config_dir: "./examples/configs/built"
  datastores_path: "./examples/configs/datastores.yaml"
  auto_build_on_startup: true
  ddl_check_on_build: "required"  # Options: required, optional, skip
  on_build_failure: "keep_old"    # Options: keep_old, remove
```

### Pipeline Configuration

A complete pipeline configuration example:

```yaml
name: "advanced_sync_pipeline"
description: "Advanced pipeline with all features"
partition_step: 50000
partition_column: "id"
max_concurrent_partitions: 4

source_provider:
  data_backend:
    type: "postgres"
    datastore_name: "source_db"
    table: "orders"
    alias: "o"
    schema: "public"
    join:
      - table: "customers"
        alias: "c"
        on: "o.customer_id = c.id"
        type: "left"
    filters:
      - "o.status = 'active'"

destination_provider:
  data_backend:
    type: "postgres"
    datastore_name: "dest_db"
    table: "orders_enriched"
    schema: "analytics"
    supports_update: true

column_map:
  - source: "o.id"
    dest: "order_id"
    roles: ["unique_column", "partition_column"]
    data_type: "int"
  - source: "o.order_date"
    dest: "order_date"
    data_type: "timestamp"
  - source: "o.total"
    dest: "total_amount"
    data_type: "decimal"
    precision: 10
    scale: 2
  - source: "c.name"
    dest: "customer_name"
    data_type: "string"
  - source: "o.updated_at"
    dest: "updated_at"
    data_type: "timestamp"

strategies:
  - name: "delta_sync"
    type: "delta"
    column: "updated_at"
    enabled: true
    cron: "*/15 * * * *"  # Every 15 minutes
    page_size: 1000
    use_sub_partition: true
    sub_partition_step: 100
    
  - name: "hash_sync"
    type: "hash"
    enabled: true
    cron: "0 2 * * *"  # Daily at 2 AM
    page_size: 5000
    prevent_update_unless_changed: true
    
  - name: "full_sync"
    type: "full"
    enabled: true
    cron: "0 0 * * 0"  # Weekly on Sunday
    page_size: 10000

enrichment:
  enabled: true
  cache_backend: "redis"
  cache_config:
    maxsize: 100000
    ttl: 3600
  dimensions:
    - name: "customer_segment"
      join_key: "customer_id"
      source:
        type: "postgres"
        config:
          datastore_name: "analytics_db"
          table: "customer_segments"
      fields:
        - source: "segment"
          dest: "customer_segment"
        - source: "score"
          dest: "customer_score"
  transformations:
    - columns: ["total_amount"]
      transform: "lambda x: round(x['total_amount'] * 1.1, 2)"
      dest: "total_with_tax"
```

## 🔄 Sync Modes

SyncTool supports two powerful sync modes: **Row-Level Sync** for individual records and **Aggregate Sync** for pre-aggregated data with intelligent change detection.

### Row-Level Sync

Traditional row-by-row synchronization where each record is processed individually. Ideal for transactional data that needs to be replicated as-is.

**Configuration Example:**

```yaml
name: "users_row_sync"
sync_type: "row-level"  # Default mode

source_provider:
  data_backend:
    type: "postgres"
    datastore_name: "source_db"
    table: "users"
    schema: "public"

destination_provider:
  data_backend:
    type: "postgres"
    datastore_name: "dest_db"
    table: "users_copy"
    schema: "public"

column_map:
  - source: "id"
    dest: "id"
    roles: ["unique_column"]
    data_type: "int"
  - source: "name"
    dest: "name"
    data_type: "string"
  - source: "updated_at"
    dest: "updated_at"
    data_type: "timestamp"

strategies:
  - name: "delta_sync"
    type: "delta"
    primary_partitions:
      - column: "updated_at"
        step: 7200  # 2 hours in seconds
        data_type: "datetime"
    page_size: 1000
```

### Aggregate Sync with Change Detection

A revolutionary approach for syncing **pre-aggregated data** (e.g., daily summaries, rollups, totals) with intelligent change detection. Instead of re-computing entire aggregates, SyncTool identifies which source rows changed and only updates affected aggregates.

**Key Features:**

- **Delta Partitions**: Identifies changed source records and maps them to aggregate keys
- **Smart Re-aggregation**: Only recomputes aggregates for rows that changed
- **Multi-dimensional Partitioning**: Handles complex aggregations across multiple dimensions
- **Deduplication**: Automatically deduplicates within aggregation windows for delta syncs
- **Column-Level Expressions**: Use SQL aggregation functions (`SUM`, `COUNT`, `MAX`, `AVG`, etc.)

**How It Works:**

1. **Change Detection**: Identifies source rows that changed (e.g., orders with `updated_at > last_sync`)
2. **Delta Partitions**: Extracts unique combinations of aggregate keys (e.g., `user_id`, `date`)
3. **Re-aggregation**: For each affected aggregate key, re-runs the aggregation query on ALL relevant source rows
4. **Update Destination**: Updates or inserts the re-computed aggregates

**Configuration Example - Daily User Orders Aggregate:**

```yaml
name: "user_daily_orders_aggregate"
description: "Sync daily order aggregates per user with change detection"
sync_type: "aggregate"  # Enable aggregate mode
enabled: true
max_concurrent_partitions: 4

strategies:
  - name: "delta"
    type: "delta"
    enabled: true
    cron: "*/15 * * * *"  # Every 15 minutes
    
    # Primary partition: Detect changes in this time range
    primary_partitions:
      - column: "updated_at"
        step: 2  # Check last 2 hours
        step_unit: "hour"
        data_type: "datetime"
        bounded: true  # Only look at recent data
    
    # Delta partitions: Extract affected aggregate keys from changed rows
    delta_partitions:
      - column: "user_id"
        type: "value"  # Extract distinct values
        step: -1  # -1 means "extract all distinct values"
        data_type: "integer"
      - column: "order_date"
        step: 1
        step_unit: "day"
        data_type: "datetime"
    
    page_size: 1000
    use_pagination: true

backends:
  - name: "source_backend"
    type: "postgres"
    datastore_name: "local_postgres"
    table: "orders"
    schema: "public"
    columns:
      - name: "user_id"
        data_type: "integer"
      - name: "amount"
        data_type: "decimal"
      - name: "discount"
        data_type: "decimal"
      - name: "order_date"
        data_type: "datetime"
      - name: "updated_at"
        data_type: "datetime"
      - name: "checksum_int"
        data_type: "bigint"
        hash_key: true
        virtual: true

  - name: "destination_backend"
    type: "postgres"
    datastore_name: "local_postgres"
    table: "user_daily_orders_aggregate"
    schema: "analytics"
    columns:
      - name: "user_id"
        data_type: "integer"
        unique_column: true  # Part of composite key
      - name: "date"
        data_type: "date"
        unique_column: true  # Part of composite key
      - name: "amount"
        data_type: "decimal"
      - name: "discount"
        data_type: "decimal"
      - name: "num_orders"
        data_type: "integer"
      - name: "last_updated_at"
        data_type: "datetime"
      - name: "checksum_int"
        data_type: "bigint"
        hash_key: true
      - name: "order_date"
        expr: "date"  # Virtual column mapping
        data_type: "date"
        virtual: true

stages:
  - name: "partition_generator"
    type: "partition"
    enabled: true
    source:
      name: source_backend
    destination:
      name: destination_backend
  
  - name: "change_detection"
    type: "change_detection"
    enabled: true
    source:
      name: source_backend
    destination:
      name: destination_backend
  
  # Key stage: Defines aggregation logic
  - name: "data_fetch"
    type: "data_fetch"
    enabled: true
    source:
      name: source_backend
      columns:
        - name: "user_id"  # GROUP BY column
        - name: "date"
          expr: "CAST(order_date AS DATE)"  # GROUP BY column
        - name: "num_orders"
          expr: "count(1)"  # Aggregation
        - name: "amount"
          expr: "sum(amount)"  # Aggregation
        - name: "discount"
          expr: "sum(discount)"  # Aggregation
        - name: "last_updated_at"
          expr: "max(updated_at)"  # Track latest update
      group_by: ["user_id", "date"]  # Defines aggregation granularity
    destination:
      name: destination_backend
  
  # Important: Dedup for delta syncs (when multiple orders for same user/day change)
  - name: "dedup"
    type: "dedup"
    enabled: true
    applicable_on: "delta"  # Only applies to delta strategy
    config:
      dedup_keys: ["user_id", "date"]  # Ensure one aggregate per key
  
  - name: "data_batcher"
    type: "batcher"
    enabled: true
    config:
      target_batch_size: 100
  
  - name: "populate"
    type: "populate"
    enabled: true
    destination:
      name: destination_backend
```

**Example Flow:**

Imagine we have these orders:

```sql
-- Initial orders
order_id | user_id | amount | order_date | updated_at
---------|---------|--------|------------|------------
1        | 101     | 50.00  | 2024-01-15 | 2024-01-15 10:00
2        | 101     | 30.00  | 2024-01-15 | 2024-01-15 11:00
3        | 102     | 75.00  | 2024-01-15 | 2024-01-15 12:00
```

**Aggregate table after initial sync:**

```sql
user_id | date       | num_orders | amount | last_updated_at
--------|------------|------------|--------|----------------
101     | 2024-01-15 | 2          | 80.00  | 2024-01-15 11:00
102     | 2024-01-15 | 1          | 75.00  | 2024-01-15 12:00
```

**Now, order #2 is updated:**

```sql
UPDATE orders SET amount = 40.00, updated_at = '2024-01-15 14:00' WHERE order_id = 2;
```

**Delta sync detects change:**

1. **Primary partition check**: Find rows where `updated_at > last_sync` → finds order #2
2. **Extract delta partitions**: From changed row, extract `user_id=101` and `date=2024-01-15`
3. **Re-aggregate**: Run aggregation query for `user_id=101` AND `date=2024-01-15`:
   ```sql
   SELECT user_id, CAST(order_date AS DATE) as date, 
          COUNT(1) as num_orders, SUM(amount) as amount,
          MAX(updated_at) as last_updated_at
   FROM orders
   WHERE user_id = 101 AND CAST(order_date AS DATE) = '2024-01-15'
   GROUP BY user_id, date
   ```
4. **Update aggregate**: Result is `num_orders=2, amount=90.00` → updates existing aggregate

**Final aggregate table:**

```sql
user_id | date       | num_orders | amount | last_updated_at
--------|------------|------------|--------|----------------
101     | 2024-01-15 | 2          | 90.00  | 2024-01-15 14:00  ← Updated!
102     | 2024-01-15 | 1          | 75.00  | 2024-01-15 12:00  ← Unchanged
```

**Advanced: Nested Multi-Dimensional Aggregates**

For complex aggregations with multiple dimensions:

```yaml
strategies:
  - name: "hash"
    type: "hash"
    enabled: true
    
    # Multiple primary partition dimensions
    primary_partitions:
      - column: "order_date"
        step: 1
        step_unit: "month"
        data_type: "datetime"
      - column: "user_id"
        step: 50
        data_type: "integer"
    
    # Multiple secondary partition dimensions for finer granularity
    secondary_partitions:
      - column: "order_date"
        step: 1
        step_unit: "day"
        data_type: "datetime"
      - column: "user_id"
        step: 5
        data_type: "integer"
    
    page_size: 1000
```

This creates a hierarchical partitioning:
- Month → Day (for dates)
- 50 users → 5 users (for user IDs)

### Column-Level Change Detection

SyncTool tracks changes at the **column level** using hash-based detection, allowing for:

- **Selective Updates**: Only update rows where specific columns changed
- **Hash Columns**: Configure which columns contribute to the row hash
- **Virtual Columns**: Computed columns that don't exist in source (e.g., checksums)
- **Prevent Unnecessary Updates**: Skip updates when only non-tracked columns change

**Configuration:**

```yaml
backends:
  - name: "source_backend"
    columns:
      - name: "user_id"
        data_type: "integer"
        hash_column: true  # Include in hash
      - name: "name"
        data_type: "string"
        hash_column: true  # Include in hash
      - name: "email"
        data_type: "string"
        hash_column: true  # Include in hash
      - name: "last_login"
        data_type: "timestamp"
        hash_column: false  # Don't include in hash (changes don't trigger update)
      - name: "checksum"
        data_type: "varchar"
        hash_key: true  # This column stores the hash
        virtual: true   # Computed, not in source table
```

**Hash Algorithms:**

SyncTool supports multiple hash algorithms:

```yaml
# Global configuration
hash_algo: "md5_sum_hash"  # Options: md5_sum_hash, hash_md5_hash

strategies:
  - name: "hash_sync"
    type: "hash"
    # Hash sync compares checksums between source and destination
    # Only rows with different hashes are synced
```

**How Column-Level Detection Works:**

1. **Hash Computation**: For each row, compute hash from specified `hash_column=true` columns
2. **Comparison**: Compare source hash vs destination hash
3. **Change Detection**:
   - **New rows**: Hash exists in source, not in destination → INSERT
   - **Modified rows**: Hash differs between source and destination → UPDATE
   - **Deleted rows**: Hash exists in destination, not in source → DELETE
   - **Unchanged rows**: Hash matches → SKIP
4. **Selective Updates**: Only columns where `hash_column=true` and value changed trigger updates

**Benefits:**

- Reduces unnecessary database writes
- Improves sync performance for large datasets
- Allows tracking specific columns (e.g., ignore audit columns like `last_accessed_at`)
- Supports compliance requirements (e.g., only sync when PII columns change)

**Example: Ignore Audit Columns**

```yaml
columns:
  # Business columns - tracked
  - name: "product_id"
    hash_column: true
  - name: "name"
    hash_column: true
  - name: "price"
    hash_column: true
  - name: "category"
    hash_column: true
  
  # Audit columns - not tracked
  - name: "created_at"
    hash_column: false  # Don't trigger sync on creation timestamp changes
  - name: "created_by"
    hash_column: false  # Don't trigger sync on audit field changes
  - name: "last_viewed_at"
    hash_column: false  # Don't trigger sync when users view products
  - name: "view_count"
    hash_column: false  # Don't trigger sync on view counter increments
```

With this configuration:
- Changes to `name`, `price`, or `category` → Triggers sync
- Changes to `view_count` or `last_viewed_at` → Does NOT trigger sync

### Resumable Syncs

SyncTool syncs are **fully resumable** - if a sync job fails or is interrupted, it automatically resumes from the last successfully completed partition, ensuring no data loss or duplication.

**How Resumability Works:**

1. **Partition-Level Tracking**: Data is divided into partitions (time ranges, ID ranges, etc.)
2. **Progress Persistence**: Each completed partition is tracked in the pipeline state
3. **Automatic Resume**: On restart, the sync skips already-completed partitions
4. **No Duplication**: Idempotent operations ensure re-running the same partition doesn't create duplicates

**State Management:**

```bash
# Pipeline state files stored in data/pipeline_states/
data/pipeline_states/
├── my_pipeline_state.json          # Current sync state
├── my_pipeline_history.json        # Historical runs
└── my_pipeline_metrics.json        # Performance metrics
```

**Example State File:**

```json
{
  "pipeline_id": "users_sync",
  "strategy": "delta",
  "run_id": "20240115_140000_abc123",
  "status": "running",
  "started_at": "2024-01-15T14:00:00Z",
  "last_completed_partition": {
    "partition_id": "updated_at_2024-01-15T12:00:00_2024-01-15T14:00:00",
    "completed_at": "2024-01-15T14:15:30Z"
  },
  "progress": {
    "total_partitions": 24,
    "completed_partitions": 15,
    "failed_partitions": 0,
    "rows_processed": 150000
  }
}
```

**Resume Scenario Example:**

```
Initial Run:
├─ Partition 1 (IDs 1-10000)      ✓ Completed
├─ Partition 2 (IDs 10001-20000)  ✓ Completed
├─ Partition 3 (IDs 20001-30000)  ✓ Completed
├─ Partition 4 (IDs 30001-40000)  ✗ Failed (network error)
└─ Partition 5-10                 ⏸ Not started

Resume Run (automatic):
├─ Partition 1-3                  ⏭ Skipped (already completed)
├─ Partition 4 (IDs 30001-40000)  🔄 Retry from here
├─ Partition 5 (IDs 40001-50000)  ▶️ Continue
└─ Partition 6-10                 ▶️ Complete remaining
```

**Benefits:**

- **Fault Tolerance**: Network failures, database timeouts, or worker crashes don't lose progress
- **Cost Efficiency**: Don't re-process successfully synced data
- **Time Savings**: Large syncs can be split across multiple runs without starting over
- **Safe Retries**: Failed partitions can be retried without affecting completed ones

**Manual State Management:**

```bash
# View current state
python -m synctool.cli.state_cli show my_pipeline

# View state for specific strategy
python -m synctool.cli.state_cli show my_pipeline --strategy delta

# Reset state (start fresh)
python -m synctool.cli.state_cli reset my_pipeline

# Reset only specific strategy
python -m synctool.cli.state_cli reset my_pipeline --strategy delta
```

**Configuration Options:**

```yaml
strategies:
  - name: "delta_sync"
    type: "delta"
    
    # Partition configuration affects resumability granularity
    primary_partitions:
      - column: "updated_at"
        step: 7200  # 2-hour partitions = finer resume granularity
        data_type: "datetime"
    
    secondary_partitions:
      - column: "id"
        step: 10000  # 10K ID partitions = even finer granularity
        data_type: "integer"
    
    # Smaller partitions = more resume points, but more overhead
    # Larger partitions = fewer resume points, better performance
```

**Best Practices:**

1. **Partition Size**: Balance between resume granularity and overhead
   - Too small: Excessive state tracking overhead
   - Too large: More data to re-process on failure
   - Sweet spot: 1000-10000 rows per partition

2. **Monitoring**: Watch for frequently failing partitions
   ```bash
   # Check failed partitions
   curl http://localhost:8001/api/pipelines/my_pipeline/failed-partitions
   ```

3. **Cleanup**: Old state files are retained for history
   ```bash
   # Cleanup old states (keeps last 30 days by default)
   python -m synctool.cli.state_cli cleanup --days 30
   ```

4. **Concurrent Safety**: Multiple workers can process different partitions of the same pipeline simultaneously without conflicts

**Idempotency Guarantees:**

SyncTool ensures safe re-execution through:

- **Upsert Operations**: INSERT ON CONFLICT UPDATE for databases that support it
- **Delete-Then-Insert**: For databases without native upsert support
- **Hash-Based Detection**: Only update rows that actually changed
- **Transaction Boundaries**: Each partition processed in a transaction

**Example: Interrupted Sync Recovery**

```python
# Sync interrupted at partition 45 of 100
2024-01-15 14:30:15 | INFO | Processing partition 45/100
2024-01-15 14:30:18 | ERROR | Database connection timeout
2024-01-15 14:30:18 | INFO | Marking partition 44 as completed
2024-01-15 14:30:18 | INFO | Saving state to disk

# Automatic resume on next run
2024-01-15 14:35:00 | INFO | Loading previous state
2024-01-15 14:35:00 | INFO | Resuming from partition 45
2024-01-15 14:35:00 | INFO | Skipping 44 completed partitions
2024-01-15 14:35:01 | INFO | Processing partition 45/100
```

## 💻 Usage

### Run a One-Time Sync

```bash
# Using CLI
synctool run --config ./examples/configs/pipelines/my_pipeline.yaml --strategy delta

# Or using Python
python -m synctool.main ./examples/configs/pipelines/my_pipeline.yaml --strategy delta
```

### Using the Scheduler System

The recommended way to run SyncTool in production:

```bash
# Terminal 1: Start Scheduler
python -m synctool.cli.arq_scheduler_cli start \
    --config-dir ./examples/configs/pipelines \
    --redis-url redis://localhost:6379

# Terminal 2: Start Worker(s)
python -m synctool.worker.worker_cli start \
    --redis-url redis://localhost:6379 \
    --max-jobs 4

# Terminal 3: Monitor
python -m synctool.cli.arq_scheduler_cli status
```

### Using Docker Compose

```bash
# Start all services
docker-compose -f docker-compose-arq.yml up -d

# View logs
docker-compose -f docker-compose-arq.yml logs -f scheduler
docker-compose -f docker-compose-arq.yml logs -f worker

# Scale workers
docker-compose -f docker-compose-arq.yml up -d --scale worker=5

# Stop all services
docker-compose -f docker-compose-arq.yml down
```

### Programmatic Usage

```python
import asyncio
from synctool.config.config_loader import ConfigLoader
from synctool.sync.sync_job_manager import SyncJobManager

async def run_sync():
    # Load configuration
    config = ConfigLoader.load_from_yaml("./my_pipeline.yaml")
    
    # Validate
    issues = ConfigLoader.validate_config(config)
    if issues:
        print(f"Configuration issues: {issues}")
        return
    
    # Create job manager
    job_manager = SyncJobManager(max_concurrent_jobs=2)
    
    # Run sync
    result = await job_manager.run_sync_job(
        config=config,
        strategy_name="delta"
    )
    
    print(f"Sync completed: {result}")

# Run the sync
asyncio.run(run_sync())
```

## 🎯 CLI Commands

### Scheduler Commands

```bash
# Start scheduler
synctool scheduler start --config-dir <path> [--redis-url <url>] [--http-port <port>]

# Check status
synctool scheduler status [--global-config <path>]

# View pipeline history
synctool scheduler history <pipeline_name> [--limit 50]

# List all jobs
synctool scheduler list-jobs --config-dir <path>
```

### Worker Commands

```bash
# Start worker
synctool worker start [--redis-url <url>] [--max-jobs 4]

# Check worker health
synctool worker health
```

### Deployment Commands

```bash
# Check deployment status
synctool deploy status

# Deploy a specific config
synctool deploy config --config <pipeline_name> [--yes]

# Deploy all pending configs
synctool deploy all [--ddl-only] [--yes]

# Build configs without deploying
synctool build --config-dir <path>
```

### Configuration Commands

```bash
# Validate a configuration file
synctool config validate <config_file>

# Show resolved configuration (with datastore substitution)
synctool config show <config_file>

# List all configurations
synctool config list --config-dir <path>
```

### Redis/State Management Commands

```bash
# Clear Redis queue
synctool redis clear-queue [--queue-name <name>]

# View queue status
synctool redis queue-status

# Reset pipeline state
synctool state reset <pipeline_name> [--strategy <strategy_name>]

# View pipeline state
synctool state show <pipeline_name> [--strategy <strategy_name>]
```

## 🌐 HTTP API

The scheduler exposes an HTTP API (default port: 8001) for monitoring and control.

### Health & Status

```bash
# Health check
GET /health

# Scheduler status
GET /api/scheduler/status
```

### Pipeline Management

```bash
# List all pipelines
GET /api/pipelines/states

# Get pipeline details
GET /api/pipelines/{pipeline_id}/state

# Get pipeline strategies
GET /api/pipelines/{pipeline_id}/strategies

# Get strategy history
GET /api/pipelines/{pipeline_id}/strategies/{strategy_name}/history?limit=50

# Get logs for a run
GET /api/pipelines/{pipeline_id}/runs/{run_id}/logs
```

### Deployment API

```bash
# Get deployment status
GET /api/deploy/status

# Get pending configs
GET /api/deploy/pending

# Deploy a config
POST /api/deploy/config/{config_name}
{
  "apply_ddl": true,
  "if_not_exists": false
}

# Deploy all pending
POST /api/deploy/all
{
  "ddl_only": false
}
```

### Strategy & Execution

```bash
# List all strategies across all pipelines
GET /api/strategies/all

# Get strategy state
GET /api/strategies/{pipeline_id}/{strategy_name}

# Get execution locks
GET /api/locks/pipelines
GET /api/locks/tables
```

### Example API Usage

```python
import requests

# Check health
response = requests.get("http://localhost:8001/health")
print(response.json())

# Get all pipeline states
response = requests.get("http://localhost:8001/api/pipelines/states")
states = response.json()
print(f"Found {len(states)} pipelines")

# Get specific pipeline history
response = requests.get(
    "http://localhost:8001/api/pipelines/users_sync/history",
    params={"limit": 10}
)
history = response.json()
for run in history:
    print(f"Run {run['run_id']}: {run['status']} at {run['started_at']}")
```

## 📚 Examples

The `examples/` directory contains comprehensive examples:

### Python Examples

- **`example.py`**: Basic sync job with in-code configuration
- **`config_manager_usage.py`**: Using the ConfigManager for storing configs
- **`enhanced_scheduler_example.py`**: Advanced scheduler usage with state management
- **`datastore_usage_example.py`**: Working with datastores
- **`example_uid.py`**: Using UUID and virtual columns

### Configuration Examples

Located in `examples/configs/`:

- **`pipeline_example.yaml`**: Basic pipeline configuration
- **`enhanced_pipeline_example.yaml`**: Advanced pipeline with all features
- **`postgres_to_s3_delta_sync.yaml`**: Delta sync from PostgreSQL to S3
- **`postgres_to_s3_hash_sync.yaml`**: Hash-based sync to S3
- **`scheduled_postgres_sync.yaml`**: Scheduled sync with cron
- **`uuid_virtual_example.yaml`**: UUID and virtual column handling

### Pipeline Examples

Located in `examples/configs/pipelines/`:

- **`postgres_postgres.yaml`**: Simple PostgreSQL to PostgreSQL sync
- **`postgres_postgres_join.yaml`**: Sync with table joins
- **`postgres_postgres_aggregate_*.yaml`**: Syncs with aggregations
- **`postgres_postgres_md5sum.yaml`**: Hash-based sync with MD5
- **`postgres_starrocks*.yaml`**: PostgreSQL to StarRocks sync

### Running Examples

```bash
# Run basic example
python examples/example.py

# Run config manager example
python examples/config_manager_usage.py

# Run scheduler example (requires Redis)
python examples/enhanced_scheduler_example.py

# Run a pipeline configuration
python -m synctool.main examples/configs/pipelines/postgres_postgres.yaml
```

## 🚢 Deployment

### Production Deployment Checklist

1. **Configuration Management**
   - Use environment variables for secrets
   - Store configs in version control (exclude `built/` directory)
   - Set up proper Redis instance (not Docker for production)

2. **Resource Planning**
   - Calculate worker count based on pipeline concurrency needs
   - Size Redis appropriately for queue depth
   - Plan database connection pool sizes

3. **Monitoring Setup**
   - Configure log aggregation (e.g., ELK, Splunk)
   - Set up metrics collection (metrics stored in JSON files)
   - Create alerts for failed pipelines

4. **Security**
   - Use encrypted connections for databases
   - Secure Redis with password authentication
   - Use secrets management (e.g., Vault, AWS Secrets Manager)

### Kubernetes Deployment

Example Kubernetes manifests:

```yaml
# scheduler-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: synctool-scheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: synctool-scheduler
  template:
    metadata:
      labels:
        app: synctool-scheduler
    spec:
      containers:
      - name: scheduler
        image: synctool:latest
        command:
          - python
          - -m
          - synctool.cli.arq_scheduler_cli
          - start
          - --config-dir
          - /configs
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        volumeMounts:
        - name: configs
          mountPath: /configs
        - name: data
          mountPath: /app/data
      volumes:
      - name: configs
        configMap:
          name: synctool-configs
      - name: data
        persistentVolumeClaim:
          claimName: synctool-data
---
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: synctool-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: synctool-worker
  template:
    metadata:
      labels:
        app: synctool-worker
    spec:
      containers:
      - name: worker
        image: synctool:latest
        command:
          - python
          - -m
          - synctool.worker.worker_cli
          - start
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        volumeMounts:
        - name: data
          mountPath: /app/data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: synctool-data
```

### Docker Best Practices

```dockerfile
# Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .
RUN pip install -e .

# Create data directories
RUN mkdir -p /app/data/logs /app/data/metrics /app/data/pipeline_states

# Non-root user
RUN useradd -m synctool && chown -R synctool:synctool /app
USER synctool

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8001/health || exit 1

CMD ["python", "-m", "synctool.cli.arq_scheduler_cli", "start"]
```

## 📊 Monitoring

### Metrics

SyncTool tracks and stores metrics for each pipeline run:

- **Execution Metrics**: Start/end times, duration, status
- **Data Metrics**: Rows processed, inserted, updated, deleted
- **Performance Metrics**: Partition processing times, batch sizes
- **Error Metrics**: Failure counts, error messages

Metrics are stored in JSON format at `data/metrics/<pipeline>/<run_id>.json`:

```json
{
  "run_id": "20240101_120000_abc123",
  "pipeline": "users_sync",
  "strategy": "delta",
  "status": "completed",
  "started_at": "2024-01-01T12:00:00Z",
  "completed_at": "2024-01-01T12:05:30Z",
  "duration_seconds": 330,
  "rows_processed": 15000,
  "rows_inserted": 500,
  "rows_updated": 200,
  "rows_deleted": 10,
  "partitions_processed": 15,
  "errors": 0
}
```

### Logs

Logs are stored in JSON Lines format at `data/logs/<pipeline>/<run_id>.log`:

```json
{"timestamp": "2024-01-01T12:00:00Z", "level": "INFO", "message": "Starting sync", "run_id": "..."}
{"timestamp": "2024-01-01T12:00:05Z", "level": "INFO", "message": "Processing partition 1-10000", "partition": "1-10000"}
{"timestamp": "2024-01-01T12:00:15Z", "level": "INFO", "message": "Processed 5000 rows", "rows": 5000}
```

### Monitoring Best Practices

1. **Set up alerts** for:
   - Failed pipeline runs
   - Long-running jobs
   - High retry counts
   - DDL lock contention

2. **Track trends** for:
   - Data volume over time
   - Execution duration
   - Success/failure rates
   - Worker utilization

3. **Use the HTTP API** to build dashboards showing:
   - Current pipeline states
   - Recent run history
   - Active execution locks
   - Queue depth

## 🔧 Troubleshooting

### Common Issues

#### Pipeline Not Running

```bash
# Check if pipeline is enabled
grep "enabled" examples/configs/built/my_pipeline.yaml

# Check scheduler logs
tail -f data/logs/scheduler.log

# Check Redis connection
redis-cli ping

# Verify config was built
ls -la examples/configs/built/my_pipeline*
```

#### Build Failures

```bash
# Check build report in scheduler logs
grep "BUILD REPORT" data/logs/scheduler.log

# Validate configuration manually
python -m synctool.cli.config_cli validate examples/configs/pipelines/my_pipeline.yaml

# Check DDL requirements
python -m synctool.cli.deploy_cli status
```

#### Worker Not Processing Jobs

```bash
# Check worker logs
tail -f data/logs/worker.log

# Verify Redis queue
python -m synctool.cli.redis_cli queue-status

# Check for execution locks
curl http://localhost:8001/api/locks/pipelines
```

#### Performance Issues

```bash
# Check metrics for slow partitions
cat data/metrics/my_pipeline/latest_run.json | jq '.partition_times'

# Reduce partition size
# Edit config: partition_step: 5000  # was 50000

# Increase worker count
docker-compose -f docker-compose-arq.yml up -d --scale worker=5

# Enable pagination for large datasets
# Edit config: page_size: 1000
```

#### Memory Issues

```bash
# Reduce batch sizes in pipeline config
# Edit strategy config:
pipeline_config:
  batch_size: 500  # was 5000
  max_concurrent_batches: 5  # was 10

# Enable pagination
data_fetch:
  enabled: true
  use_pagination: true
  page_size: 500
```

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('synctool').setLevel(logging.DEBUG)
```

Or set environment variable:

```bash
export SYNCTOOL_LOG_LEVEL=DEBUG
python -m synctool.cli.arq_scheduler_cli start ...
```

### Getting Help

1. Check existing documentation in the repository:
   - `QUICKSTART_ARQ.md` - Quick start guide
   - `PIPELINE_ARCHITECTURE.md` - Pipeline system details
   - `BUILD_DEPLOYMENT_QUICKSTART.md` - Build system guide
   - `ENHANCED_SCHEDULER_GUIDE.md` - Scheduler documentation

2. Enable debug logging and check logs
3. Check metrics and state files in `data/` directory
4. Use HTTP API to inspect current state
5. Open an issue on GitHub with logs and configuration

## 📖 Additional Documentation

- **[Quick Start Guide](QUICKSTART_ARQ.md)** - Get started in 5 minutes
- **[Pipeline Architecture](PIPELINE_ARCHITECTURE.md)** - Deep dive into pipeline stages
- **[Build & Deployment](BUILD_DEPLOYMENT_QUICKSTART.md)** - Config build system
- **[Scheduler Guide](ENHANCED_SCHEDULER_GUIDE.md)** - Advanced scheduler features
- **[Retry Behavior](RETRY_BEHAVIOR_GUIDE.md)** - Understanding retry logic
- **[DDL Generation](INTERACTIVE_DDL_GUIDE.md)** - DDL management
- **[Lock Management](LOCK_REFACTORING_SUMMARY.md)** - Execution locks
- **[Precision & Scale](PRECISION_SCALE_LENGTH_GUIDE.md)** - Numeric type handling
- **[UUID & Virtual Columns](UUID_AND_VIRTUAL_COLUMNS_GUIDE.md)** - Special column types

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run tests (`pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Development Setup

```bash
# Clone repository
git clone https://github.com/yourusername/synctool.git
cd synctool

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install in development mode with all dependencies
pip install -e .
pip install -r requirements.txt

# Run tests
pytest

# Run specific test
pytest tests/test_sync_strategies.py

# Run with coverage
pytest --cov=synctool tests/
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with [ARQ](https://github.com/samuelcolvin/arq) for distributed task queue
- Uses [asyncpg](https://github.com/MagicStack/asyncpg) for PostgreSQL async operations
- Inspired by modern data engineering best practices

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/synctool/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/synctool/discussions)
- **Email**: developers@synctool.example

---

**Made with ❤️ for data engineers who value reliability and performance**

