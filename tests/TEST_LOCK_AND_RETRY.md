# Lock and Retry Test Suite

This document describes the test suite for the lock management and slot-aware retry refactoring.

## Test Files

### 1. `test_slot_aware_retry.py`

Tests the slot-aware retry mechanism in `EnhancedStrategySelector`.

**Test Coverage:**
- ✅ Skipped jobs retry within current schedule slot
- ✅ Adaptive backoff calculation (50% of cron interval, 30s-5min)
- ✅ Slot boundary detection (abandon retry when new slot arrives)
- ✅ Failed jobs do NOT get automatic retry
- ✅ Different cron frequencies (2min, 15min, hourly, daily, weekly)
- ✅ Edge cases (no last_attempted_at, minimum backoff, etc.)

**Key Tests:**
- `test_skipped_job_retry_within_slot`: Verifies retry happens after backoff expires
- `test_skipped_job_too_soon_to_retry`: Verifies backoff is respected
- `test_skipped_job_new_slot_abandons_retry`: Verifies slot transition logic
- `test_failed_job_no_automatic_retry`: Confirms failed jobs wait for cron
- `test_daily_job_skipped_retries_all_day`: Daily jobs retry every 5 min
- `test_frequent_job_adaptive_backoff`: Fast jobs have shorter backoff

**Run Command:**
```bash
pytest tests/test_slot_aware_retry.py -v
```

### 2. `test_sync_job_manager_locks.py`

Tests the lock acquisition logic in `SyncJobManager`.

**Test Coverage:**
- ✅ Pipeline lock acquisition (success/failure)
- ✅ Table DDL lock checking
- ✅ Wait behavior for locks
- ✅ Skip behavior when locks unavailable
- ✅ Callback execution (on_locks_acquired, on_skip)
- ✅ Strategy config extraction
- ✅ Destination table info extraction

**Key Test Classes:**

#### `TestPipelineLockAcquisition`
- `test_no_lock_manager_runs_without_locking`: No lock manager = no locking
- `test_pipeline_lock_acquired_successfully`: Successful lock acquisition path
- `test_pipeline_lock_unavailable_skips_job`: Lock failure = skip with correct reason

#### `TestTableDDLLockCheck`
- `test_table_not_locked_proceeds`: Table available = proceed
- `test_table_locked_no_wait_skips`: Table locked + no wait = skip immediately
- `test_table_locked_wait_then_proceeds`: Table locked + wait = eventually succeed
- `test_table_locked_wait_timeout_skips`: Table locked + timeout = skip

#### `TestCallbacks`
- `test_on_locks_acquired_called_after_lock_success`: Callback timing verification
- `test_on_skip_called_when_lock_unavailable`: Skip callback verification

**Run Command:**
```bash
pytest tests/test_sync_job_manager_locks.py -v
```

### 3. `test_enhanced_strategy_selector.py` (Updated)

Existing test file updated with new logic for failed jobs.

**New/Updated Tests:**
- `test_retry_count_below_max_failed_status`: Failed jobs don't auto-retry

**Run Command:**
```bash
pytest tests/test_enhanced_strategy_selector.py -v
```

## Running All Tests

### Run All Lock and Retry Tests
```bash
pytest tests/test_slot_aware_retry.py tests/test_sync_job_manager_locks.py tests/test_enhanced_strategy_selector.py -v
```

### Run With Coverage
```bash
pytest tests/test_slot_aware_retry.py tests/test_sync_job_manager_locks.py tests/test_enhanced_strategy_selector.py --cov=synctool.scheduler --cov=synctool.sync --cov-report=html
```

### Run Specific Test
```bash
# Run a single test
pytest tests/test_slot_aware_retry.py::TestSlotAwareRetry::test_skipped_job_retry_within_slot -v

# Run a test class
pytest tests/test_sync_job_manager_locks.py::TestPipelineLockAcquisition -v
```

## Test Scenarios Covered

### Scenario 1: Daily Job Lock Conflict

**Setup:**
- Job scheduled daily at 2:00 AM
- Another job holds pipeline lock at 2:00 AM

**Expected Behavior:**
1. Job tries at 2:00 AM → skipped (lock unavailable)
2. Job retries at 2:05 AM (5 min backoff)
3. Job retries at 2:10 AM if still locked
4. Continues every 5 minutes until success or next day

**Tests:**
- `test_daily_job_skipped_retries_all_day`
- `test_skipped_job_retry_within_slot`

### Scenario 2: Frequent Job (Every 2 Min)

**Setup:**
- Job scheduled every 2 minutes
- Lock conflict at 10:00

**Expected Behavior:**
1. Job tries at 10:00 → skipped
2. Job retries at 10:01 (1 min backoff = 50% of 2 min)
3. New slot at 10:02 → abandon 10:00 retry, start fresh

**Tests:**
- `test_frequent_job_adaptive_backoff`
- `test_skipped_job_new_slot_abandons_retry`

### Scenario 3: Failed Job (Error Not Lock)

**Setup:**
- Job fails due to database error (not lock)
- Retry count below max

**Expected Behavior:**
1. Job fails at 10:00 → status = "failed"
2. At 10:05 → scheduler checks → no retry (failed != skipped)
3. At 11:00 → regular cron schedule → new attempt

**Tests:**
- `test_failed_job_no_automatic_retry`
- `test_retry_count_below_max_failed_status`
- `test_failed_job_max_retry_exceeded`

### Scenario 4: Table DDL Lock

**Setup:**
- Pipeline lock acquired
- Table has DDL lock (schema change in progress)

**Expected Behavior (no wait):**
1. Acquire pipeline lock → success
2. Check table DDL lock → locked
3. Skip job → status = "skipped"
4. Retry via slot-aware logic

**Expected Behavior (with wait):**
1. Acquire pipeline lock → success
2. Check table DDL lock → locked
3. Wait up to timeout for lock release
4. If released → proceed; if timeout → skip

**Tests:**
- `test_table_locked_no_wait_skips`
- `test_table_locked_wait_then_proceeds`
- `test_table_locked_wait_timeout_skips`

## Integration Test Scenarios

These are recommendations for manual/integration testing:

### Test 1: CLI with Lock Waiting
```bash
# Terminal 1: Start a long-running job
python -m synctool.cli.sync_cli run --job-name my_job --config-dir ./configs

# Terminal 2: Try to run same job (should skip or wait)
python -m synctool.cli.sync_cli run --job-name my_job --config-dir ./configs --lock-timeout 10
```

**Expected:**
- Second instance skips (or waits then skips)
- Clear error message about lock

### Test 2: Scheduler with Daily Job
```bash
# 1. Configure a daily job with short interval for testing
# 2. Manually hold the pipeline lock (e.g., run CLI job)
# 3. Watch scheduler logs for retry attempts
# 4. Release lock (stop CLI job)
# 5. Verify job succeeds on next retry
```

**Expected:**
- Retry attempts every 5 minutes
- Clear log messages: "Retrying skipped job within current slot"

### Test 3: Failed vs Skipped Distinction
```bash
# 1. Create a job that will fail (bad connection string)
# 2. Run it via scheduler
# 3. Watch that it does NOT retry within slot
# 4. Verify it waits for next cron schedule
```

**Expected:**
- No retry attempts between cron schedules
- Status remains "failed" (not "skipped")

## Debugging Failed Tests

### Common Issues

**Issue 1: `asyncio.contextmanager` not found**
```python
# Solution: Import from contextlib
from contextlib import asynccontextmanager
```

**Issue 2: Mock not returning async context manager correctly**
```python
# Solution: Use @asynccontextmanager decorator
@asyncio.contextmanager
async def mock_acquire_lock(*args, **kwargs):
    yield True
```

**Issue 3: Time-based tests flaky**
```python
# Solution: Use fixed datetime objects, not datetime.now()
current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
```

## Coverage Goals

**Target Coverage:**
- `enhanced_strategy_selector.py` _should_run_strategy: 95%+
- `sync_job_manager.py` run_sync_job: 90%+
- `enhanced_tasks.py` execute_pipeline_strategy: 85%+

**Current Coverage:**
Run coverage report to check:
```bash
pytest tests/test_slot_aware_retry.py tests/test_sync_job_manager_locks.py --cov=synctool --cov-report=term-missing
```

## Future Test Additions

Potential areas for additional testing:

1. **Concurrent Execution Tests**
   - Multiple workers trying same job
   - Race conditions in lock acquisition

2. **Network Failure Tests**
   - Worker can't communicate with scheduler
   - Redis connection failures

3. **State Corruption Tests**
   - Invalid timestamps in state
   - Missing required state fields

4. **Performance Tests**
   - Many frequent jobs (every 1 min)
   - Lock contention scenarios

## Contributing

When adding new lock/retry logic:

1. Add unit tests to appropriate file
2. Document expected behavior
3. Add integration test scenario
4. Update this document
5. Run full test suite before PR

## Related Documentation

- `SLOT_AWARE_RETRY_GUIDE.md` - User-facing guide
- `RETRY_BEHAVIOR_GUIDE.md` - Status distinction guide
- `LOCK_REFACTORING_SUMMARY.md` - Technical summary

