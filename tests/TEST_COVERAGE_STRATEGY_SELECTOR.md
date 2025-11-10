# Test Coverage for EnhancedStrategySelector

## Overview
Comprehensive test suite for the `EnhancedStrategySelector._should_run_strategy` method with 25 test cases covering all edge cases and scenarios.

## Implementation Changes

### Updated Logic for Missing State
Changed from checking "next scheduled time" to checking "previous scheduled time" for missing state scenarios:

**Before:**
```python
time_until_next = (next_scheduled_time - current_time).total_seconds()
if 0 <= time_until_next <= (self.safe_fallback_window_minutes * 60):
    return True, f"First run or missing state, next run in {time_until_next:.0f}s"
```

**After:**
```python
time_since_prev = (current_time - last_scheduled_time).total_seconds()
fallback_window_seconds = self.safe_fallback_window_minutes * 60
if 0 <= time_since_prev <= fallback_window_seconds:
    return True, f"First run or missing state — within {time_since_prev:.0f}s of schedule slot"
```

This ensures we catch up on missed schedules within the fallback window rather than waiting for the next scheduled time.

## Test Suite Structure

### TestShouldRunStrategy (20 tests)

#### 1. **Missing State Scenarios** (Tests 1-3)
- ✅ `test_missing_state_within_fallback_window`: First run within 30-minute window
- ✅ `test_missing_state_outside_fallback_window`: First run outside fallback window
- ✅ `test_state_exists_no_last_run_within_window`: State exists but no last_run

#### 2. **Safety Checks** (Tests 4-5)
- ✅ `test_max_retry_count_exceeded`: Blocks execution after max retries
- ✅ `test_strategy_already_running`: Prevents concurrent execution

#### 3. **Already Executed Checks** (Tests 6-7)
- ✅ `test_already_ran_for_schedule_slot`: Already ran after last scheduled time
- ✅ `test_already_scheduled_for_slot`: Already enqueued for this slot

#### 4. **Scheduling Windows** (Tests 8-9)
- ✅ `test_due_to_run_within_buffer`: Within 60-second schedule buffer
- ✅ `test_not_due_yet`: Before the schedule buffer window

#### 5. **Catch-Up Logic** (Tests 10-11)
- ✅ `test_overdue_catch_up`: Missed multiple intervals, should catch up
- ✅ `test_overdue_but_recently_ran`: Overdue but ran recently, don't duplicate

#### 6. **Timezone Handling** (Test 12)
- ✅ `test_timezone_handling_naive_datetime`: Converts naive datetimes to UTC

#### 7. **Different Cron Frequencies** (Tests 13-15, 18)
- ✅ `test_daily_cron_within_buffer`: Daily schedule (0 0 * * *)
- ✅ `test_frequent_cron_every_15_minutes`: High-frequency schedule (*/15 * * * *)
- ✅ `test_invalid_cron_expression`: Error handling for invalid cron
- ✅ `test_weekly_cron_schedule`: Weekly schedule (0 0 * * 0)

#### 8. **Edge Cases** (Tests 16-17, 19-20)
- ✅ `test_last_scheduled_at_outside_buffer`: Old last_scheduled_at value
- ✅ `test_retry_count_below_max`: Retry count less than maximum
- ✅ `test_first_run_cron_just_passed`: First run immediately after slot
- ✅ `test_exactly_at_schedule_buffer_limit`: Boundary condition at buffer limit

### TestCanEnqueueStrategy (5 tests)

#### Additional Enqueue Safety Checks
- ✅ `test_can_enqueue_no_state`: Allow first run
- ✅ `test_cannot_enqueue_already_running`: Block if already running
- ✅ `test_cannot_enqueue_recently_pending`: Block if recently pending (<5 min)
- ✅ `test_can_enqueue_stuck_pending`: Allow if stuck pending (>5 min)
- ✅ `test_can_enqueue_with_success_status`: Allow enqueue after success

## Test Results

```
25 passed in 0.06s
```

All tests pass successfully with no linter errors.

## Key Scenarios Covered

### 1. **First Run / Missing State**
- Within fallback window: ✅ RUN
- Outside fallback window: ❌ SKIP

### 2. **Safety Checks**
- Max retries exceeded: ❌ SKIP
- Already running: ❌ SKIP

### 3. **Schedule Adherence**
- Within schedule buffer (0-60s): ✅ RUN
- Already ran for slot: ❌ SKIP
- Already scheduled for slot: ❌ SKIP

### 4. **Catch-Up Behavior**
- Overdue (>1 interval since last run): ✅ RUN
- Overdue but recently ran: ❌ SKIP

### 5. **Cron Frequencies Tested**
- Every 15 minutes: `*/15 * * * *`
- Hourly: `0 * * * *`
- Daily: `0 0 * * *`
- Weekly: `0 0 * * 0`

## Configuration Parameters

The tests verify behavior with these default parameters:
- `safe_fallback_window_minutes`: 30 minutes
- `max_retry_count`: 3 attempts
- `schedule_buffer_seconds`: 60 seconds

## Running the Tests

```bash
# Run all tests
pytest tests/test_enhanced_strategy_selector.py -v

# Run specific test class
pytest tests/test_enhanced_strategy_selector.py::TestShouldRunStrategy -v

# Run specific test
pytest tests/test_enhanced_strategy_selector.py::TestShouldRunStrategy::test_missing_state_within_fallback_window -v
```

## Dependencies

- `pytest`: Testing framework
- `unittest.mock`: For mocking PipelineStateManager
- `croniter`: For cron expression parsing
- `datetime`: For timezone-aware datetime handling

## Future Enhancements

Potential additional test cases to consider:
1. Tests with different fallback window values
2. Tests with different schedule buffer values
3. Stress tests with rapid consecutive calls
4. Integration tests with real PipelineStateManager
5. Tests for DST transitions
6. Tests with leap years/seconds

