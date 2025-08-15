# Centralized DataStores Configuration

This document explains how to use the centralized DataStores system in Synctool.

## Overview

The DataStores system separates database connection information from sync job configurations, providing:

- **Centralized Management**: All database connections are defined in one place
- **Security**: Sensitive information (passwords, secrets) uses environment variables
- **Reusability**: Multiple sync jobs can reference the same datastore
- **Environment-specific**: Different configurations for dev, staging, production

## File Structure

- `datastores.yaml` - Centralized datastore definitions
- `env.example` - Example environment variables file
- Sync job configs (e.g., `postgres_uuid_sync.yaml`) - Reference datastores by name

## Setting Up DataStores

### 1. Configure Environment Variables

Copy `env.example` to `.env` and fill in your actual values:

```bash
cp env.example .env
# Edit .env with your database credentials
```

### 2. Define DataStores

Edit `datastores.yaml` to define your database connections:

```yaml
datastores:
  local_postgres:
    type: 'postgres'
    description: 'Local PostgreSQL database for development'
    tags: ['development', 'postgres', 'local']
    connection:
      host: '${POSTGRES_HOST:localhost}'
      port: ${POSTGRES_PORT:5432}
      user: '${POSTGRES_USER:username}'
      password: '${POSTGRES_PASSWORD:}'
      dbname: '${POSTGRES_DB:synctool}'
```

### 3. Reference DataStores in Sync Jobs

In your sync job configurations, reference datastores by name:

```yaml
source_provider:
  data_backend:
    type: 'postgres'
    datastore_name: 'local_postgres'  # Reference by name
    table: 'users'
    schema: 'public'
```

## Environment Variable Syntax

Use the format `${VARIABLE_NAME:default_value}` in `datastores.yaml`:

- `${POSTGRES_HOST:localhost}` - Uses `POSTGRES_HOST` env var, defaults to `localhost`
- `${POSTGRES_PASSWORD}` - Uses `POSTGRES_PASSWORD` env var, no default (empty string)

## DataStore Types

### PostgreSQL
```yaml
postgres_db:
  type: 'postgres'
  connection:
    host: '${POSTGRES_HOST}'
    port: ${POSTGRES_PORT:5432}
    user: '${POSTGRES_USER}'
    password: '${POSTGRES_PASSWORD}'
    dbname: '${POSTGRES_DB}'
```

### MySQL
```yaml
mysql_db:
  type: 'mysql'
  connection:
    host: '${MYSQL_HOST}'
    port: ${MYSQL_PORT:3306}
    user: '${MYSQL_USER}'
    password: '${MYSQL_PASSWORD}'
    database: '${MYSQL_DB}'
```

### ClickHouse
```yaml
clickhouse_db:
  type: 'clickhouse'
  connection:
    host: '${CLICKHOUSE_HOST}'
    port: ${CLICKHOUSE_PORT:9000}
    user: '${CLICKHOUSE_USER}'
    password: '${CLICKHOUSE_PASSWORD}'
    database: '${CLICKHOUSE_DB}'
```

### DuckDB
```yaml
duckdb_local:
  type: 'duckdb'
  connection:
    database: '${DUCKDB_PATH:./data/synctool.duckdb}'
```

### S3 Object Storage
```yaml
s3_storage:
  type: 'object_storage'
  connection:
    s3_bucket: '${S3_BUCKET}'
    s3_prefix: '${S3_PREFIX:data/}'
    aws_access_key_id: '${AWS_ACCESS_KEY_ID}'
    aws_secret_access_key: '${AWS_SECRET_ACCESS_KEY}'
    region: '${AWS_REGION:us-east-1}'
```

## Migration from Old Format

### Before (Direct Connection)
```yaml
source_provider:
  data_backend:
    type: 'postgres'
    connection:
      host: 'localhost'
      port: 5432
      user: 'username'
      password: 'password'
      dbname: 'synctool'
```

### After (DataStore Reference)
```yaml
source_provider:
  data_backend:
    type: 'postgres'
    datastore_name: 'local_postgres'
```

## Benefits

1. **Security**: No hardcoded credentials in config files
2. **Environment Management**: Easy switching between dev/staging/prod
3. **Reusability**: One datastore definition used by multiple jobs
4. **Maintainability**: Centralized connection management
5. **Version Control Safe**: Sensitive data stays in environment variables

## Validation

The system validates:
- All referenced datastores exist
- Required connection parameters are present
- Environment variables are properly resolved
- DataStore types match backend requirements

## Error Handling

Common errors and solutions:

- **"Datastore 'name' not found"**: Check datastore name spelling in sync config
- **"Empty or invalid datastores YAML"**: Verify `datastores.yaml` syntax
- **"Must specify host"**: Ensure required environment variables are set
- **Connection failures**: Verify environment variable values are correct
