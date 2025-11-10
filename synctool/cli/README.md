# Synctool CLI Tools

This directory contains command-line interface tools for managing and operating Synctool.

## Available CLI Tools

### 1. Sync CLI (`sync_cli.py`)
Manual execution of sync jobs without the scheduler.

**Usage:**
```bash
# List all available jobs
python -m synctool.cli.sync_cli list

# Run a specific job
python -m synctool.cli.sync_cli run --job-name my_job --strategy delta

# Generate DDL for a job
python -m synctool.cli.sync_cli generate-ddl --job-name my_job --yes
```

**Documentation:** See `LOCK_REFACTORING_SUMMARY.md` for detailed usage

### 2. Redis CLI (`redis_cli.py`)
Manage Redis locks, monitor Redis state, and view ARQ job queues.

**Usage:**
```bash
# List all locks
python -m synctool.cli.redis_cli list

# View ARQ job queue
python -m synctool.cli.redis_cli jobs

# Clear locks for specific pipeline
python -m synctool.cli.redis_cli clear --pipeline-id my_pipeline --yes

# Check lock status
python -m synctool.cli.redis_cli check --pipeline-id my_pipeline

# Clear ARQ job queue
python -m synctool.cli.redis_cli clear-queue --yes

# View Redis info
python -m synctool.cli.redis_cli info
```

**Documentation:** See `REDIS_CLI_GUIDE.md` for detailed usage

### 3. ARQ Scheduler CLI (`arq_scheduler_cli.py`)
Start and manage the distributed scheduler using ARQ workers.

**Usage:**
```bash
# Start scheduler
python -m synctool.cli.arq_scheduler_cli start --config-dir ./configs

# View scheduler status
python -m synctool.cli.arq_scheduler_cli status

# View job history
python -m synctool.cli.arq_scheduler_cli history my_pipeline
```

**Documentation:** See `ENHANCED_SCHEDULER_GUIDE.md` and `QUICKSTART_ARQ.md`

### 4. Enhanced Scheduler CLI (`enhanced_scheduler_cli.py`)
Alternative scheduler implementation with enhanced features.

**Usage:**
```bash
# Start scheduler
python -m synctool.cli.enhanced_scheduler_cli start --config-dir ./configs

# View status
python -m synctool.cli.enhanced_scheduler_cli status
```

**Documentation:** See `ENHANCED_SCHEDULER_GUIDE.md`

### 5. Scheduler CLI (`scheduler_cli.py`)
Legacy scheduler for backwards compatibility.

**Usage:**
```bash
# Start scheduler
python -m synctool.cli.scheduler_cli start --config-dir ./configs

# List jobs
python -m synctool.cli.scheduler_cli list --config-dir ./configs
```

**Documentation:** See `SCHEDULER_README.md`

### 6. Config CLI (`config_cli.py`)
Manage pipeline configurations across different storage backends.

**Usage:**
```bash
# List configurations
python -m synctool.cli.config_cli list

# Load configuration
python -m synctool.cli.config_cli load my_config
```

**Documentation:** See `CONFIG_STORE_README.md`

## Common Workflows

### Development Workflow

1. **Validate Configuration**
   ```bash
   python -m synctool.cli.sync_cli list
   ```

2. **Generate DDL**
   ```bash
   python -m synctool.cli.sync_cli generate-ddl --job-name my_job --yes
   ```

3. **Test Job Manually**
   ```bash
   python -m synctool.cli.sync_cli run --job-name my_job --strategy delta
   ```

4. **Check Redis Locks**
   ```bash
   python -m synctool.cli.redis_cli check --pipeline-id my_job
   ```

### Production Workflow

1. **Start Scheduler**
   ```bash
   python -m synctool.cli.arq_scheduler_cli start --config-dir ./configs
   ```

2. **Start Workers**
   ```bash
   python -m synctool.worker.worker_cli start --max-jobs 4
   ```

3. **Monitor Status**
   ```bash
   python -m synctool.cli.arq_scheduler_cli status
   python -m synctool.cli.redis_cli info
   ```

4. **Troubleshoot Issues**
   ```bash
   # Check locks
   python -m synctool.cli.redis_cli list
   
   # Check ARQ job queue
   python -m synctool.cli.redis_cli jobs
   
   # Clear stuck locks
   python -m synctool.cli.redis_cli clear --pipeline-id my_job --yes
   
   # View job history
   python -m synctool.cli.arq_scheduler_cli history my_job
   ```

## Global Options

All CLI tools support these common options:

- `--global-config PATH`: Path to global configuration file
- `--log-level LEVEL`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Configuration

CLI tools use the global configuration file for common settings:

```yaml
# synctool/config/global_config.yaml

# Storage locations
storage:
  state_dir: "data/pipeline_states"
  metrics_dir: "data/metrics"
  logs_dir: "data/logs"
  max_runs_per_strategy: 50

# Redis connection
redis:
  url: "redis://localhost:6379/0"

# Scheduler settings
scheduler:
  lock_timeout: 3600
  check_interval: 60
  
# HTTP settings (for monitoring)
http:
  host: "0.0.0.0"
  port: 8001
```

## Help

Each CLI tool has built-in help:

```bash
# Main help
python -m synctool.cli.sync_cli --help

# Command-specific help
python -m synctool.cli.sync_cli run --help
python -m synctool.cli.redis_cli clear --help
```

## See Also

- [Main README](../../README.md) - Project overview
- [QUICKSTART_ARQ.md](../../QUICKSTART_ARQ.md) - Getting started guide
- [REDIS_CLI_GUIDE.md](../../REDIS_CLI_GUIDE.md) - Redis CLI documentation
- [ENHANCED_SCHEDULER_GUIDE.md](../../ENHANCED_SCHEDULER_GUIDE.md) - Scheduler documentation

