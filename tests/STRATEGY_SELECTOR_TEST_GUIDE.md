# Strategy Selector Test Guide

## Quick Reference for Test Scenarios

This guide helps you understand what each test validates and when the strategy should/shouldn't run.

## Decision Flow

```
┌─────────────────────────────────────────────────────┐
│ Should Strategy Run? (_should_run_strategy)         │
└─────────────────────────────────────────────────────┘
                      │
                      ▼
      ┌───────────────────────────────┐
      │ Is retry count >= max?        │
      └───────────────────────────────┘
                 │           │
                YES         NO
                 │           │
                 ▼           ▼
        ❌ Return False   Continue
      "Max retry exceeded"    │
                              ▼
              ┌────────────────────────────┐
              │ Is status "running"?       │
              └────────────────────────────┘
                     │           │
                    YES         NO
                     │           │
                     ▼           ▼
            ❌ Return False   Continue
          "Already running"      │
                                 ▼
              ┌─────────────────────────────┐
              │ Is state missing or null?   │
              └─────────────────────────────┘
                     │           │
                    YES         NO
                     │           │
                     ▼           ▼
      ┌──────────────────────┐  Continue
      │ Check fallback window│    │
      │ (last_scheduled_time)│    │
      └──────────────────────┘    │
             │           │         │
        Within 30m   Outside 30m   │
             │           │         │
             ▼           ▼         │
         ✅ RUN    ❌ SKIP       │
                                  ▼
                 ┌──────────────────────────────────┐
                 │ Has last_run >= last_scheduled?  │
                 └──────────────────────────────────┘
                            │           │
                           YES         NO
                            │           │
                            ▼           ▼
                   ❌ "Already ran" Continue
                                       │
                                       ▼
                    ┌────────────────────────────────────┐
                    │ last_scheduled_at >= last_scheduled│
                    └────────────────────────────────────┘
                               │           │
                              YES         NO
                               │           │
                               ▼           ▼
                   ❌ "Already scheduled" Continue
                                          │
                                          ▼
                      ┌─────────────────────────────────┐
                      │ Within schedule buffer (60s)?   │
                      └─────────────────────────────────┘
                             │           │
                            YES         NO
                             │           │
                             ▼           ▼
                         ✅ RUN      Check overdue
                                          │
                                          ▼
                         ┌───────────────────────────────┐
                         │ time_since_success >= interval│
                         └───────────────────────────────┘
                                │           │
                               YES         NO
                                │           │
                                ▼           ▼
                            ✅ RUN     ❌ SKIP
                          "Overdue"  "Not due yet"
```

## Test Scenarios by Category

### 🆕 First Run / Missing State

| Test | Scenario | Time After Schedule | Expected | Reason |
|------|----------|---------------------|----------|---------|
| test_missing_state_within_fallback_window | No state, 5 min after schedule | 5 minutes | ✅ RUN | Within 30-min fallback window |
| test_missing_state_outside_fallback_window | No state, 45 min after schedule | 45 minutes | ❌ SKIP | Outside fallback window |
| test_first_run_cron_just_passed | No state, 5 sec after schedule | 5 seconds | ✅ RUN | Just missed the slot |

**Key Learning:** On first run, only execute if we're within 30 minutes of a scheduled slot.

---

### 🛡️ Safety Checks

| Test | State Condition | Expected | Reason |
|------|----------------|----------|---------|
| test_max_retry_count_exceeded | retry_count = 3 (max) | ❌ SKIP | Prevent infinite retry loops |
| test_strategy_already_running | status = "running" | ❌ SKIP | Prevent concurrent execution |
| test_retry_count_below_max | retry_count = 2 (< max) | ✅ RUN | Can retry, not at limit |

**Key Learning:** Safety checks prevent concurrent runs and infinite retries.

---

### ⏰ Schedule Timing

| Test | Scenario | Time Gap | Expected | Reason |
|------|----------|----------|----------|---------|
| test_due_to_run_within_buffer | Normal schedule | 30 seconds | ✅ RUN | Within 60s buffer |
| test_exactly_at_schedule_buffer_limit | Edge case | 60 seconds | ✅ RUN | At buffer limit |
| test_not_due_yet | Too early | -120 seconds | ❌ SKIP | Before schedule |

**Key Learning:** 60-second buffer window around scheduled time for execution.

---

### 🚫 Already Executed

| Test | Last Action | Scheduled Time | Expected | Reason |
|------|-------------|----------------|----------|---------|
| test_already_ran_for_schedule_slot | Ran at 10:01 | 10:00 | ❌ SKIP | Already completed this slot |
| test_already_scheduled_for_slot | Scheduled at 10:02 | 10:00 | ❌ SKIP | Already enqueued |
| test_last_scheduled_at_outside_buffer | Scheduled at 9:00 | 10:00 (new) | ✅ RUN | Old slot, new schedule |

**Key Learning:** Check both execution and enqueue timestamps to prevent duplicates.

---

### 🔄 Catch-Up Behavior

| Test | Last Run | Current Interval | Expected | Reason |
|------|----------|------------------|----------|---------|
| test_overdue_catch_up | 2 hours ago | 1 hour | ✅ RUN | Missed 2+ intervals |
| test_overdue_but_recently_ran | 15 min ago | 1 hour | ❌ SKIP | Ran recently, don't duplicate |

**Key Learning:** Catch up on missed schedules, but avoid duplicate runs.

---

### 📅 Different Cron Schedules

| Test | Cron Expression | Frequency | Purpose |
|------|----------------|-----------|---------|
| test_frequent_cron_every_15_minutes | `*/15 * * * *` | Every 15 min | High-frequency sync |
| test_daily_cron_within_buffer | `0 0 * * *` | Daily at midnight | Daily batch |
| test_weekly_cron_schedule | `0 0 * * 0` | Sunday at midnight | Weekly reports |
| test_invalid_cron_expression | `invalid cron` | N/A | Error handling |

**Key Learning:** Same logic applies regardless of cron frequency.

---

### 🌍 Timezone Handling

| Test | Input Datetime | Expected Behavior |
|------|---------------|-------------------|
| test_timezone_handling_naive_datetime | No timezone info | Automatically convert to UTC |

**Key Learning:** All times internally converted to UTC for consistency.

---

### 🔐 Enqueue Safety (can_enqueue_strategy)

| Test | State | Expected | Reason |
|------|-------|----------|---------|
| test_can_enqueue_no_state | None | ✅ ENQUEUE | First run |
| test_cannot_enqueue_already_running | running | ❌ BLOCK | Still executing |
| test_cannot_enqueue_recently_pending | pending (2 min) | ❌ BLOCK | Just enqueued |
| test_can_enqueue_stuck_pending | pending (6 min) | ✅ ENQUEUE | Stuck, allow retry |
| test_can_enqueue_with_success_status | success | ✅ ENQUEUE | Ready for next run |

**Key Learning:** Additional safety check after acquiring enqueue lock.

---

## Common Patterns

### ✅ When Strategy WILL Run

1. **First run within fallback window**
   ```python
   state = None
   current_time = 5 minutes after last_scheduled_time
   # Result: RUN (within 30-min window)
   ```

2. **Due to run (within buffer)**
   ```python
   last_run = 9:00 AM
   current_time = 10:00:30 (30s after 10:00 schedule)
   # Result: RUN (within 60s buffer)
   ```

3. **Catch-up needed**
   ```python
   last_run = 8:00 AM (2 hours ago)
   cron_interval = 1 hour
   current_time = 10:30 AM
   # Result: RUN (missed multiple intervals)
   ```

### ❌ When Strategy WON'T Run

1. **Already ran for this slot**
   ```python
   last_run = 10:01 AM
   last_scheduled_time = 10:00 AM
   # Result: SKIP (already completed)
   ```

2. **Max retries exceeded**
   ```python
   retry_count = 3 (>= max_retry_count)
   # Result: SKIP (too many failures)
   ```

3. **Outside fallback window (first run)**
   ```python
   state = None
   current_time = 45 minutes after last_scheduled_time
   # Result: SKIP (outside 30-min window)
   ```

---

## Debugging Tips

### Check Test Output
```bash
# Run with verbose output
pytest tests/test_enhanced_strategy_selector.py -v -s

# Run single test with output
pytest tests/test_enhanced_strategy_selector.py::TestShouldRunStrategy::test_missing_state_within_fallback_window -v -s
```

### Add Custom Assertions
```python
# In your test
should_run, reason = strategy_selector._should_run_strategy(...)
print(f"Decision: {should_run}, Reason: {reason}")
```

### Test with Different Parameters
```python
# Override defaults
strategy_selector = EnhancedStrategySelector(
    state_manager=mock_state_manager,
    safe_fallback_window_minutes=15,  # Stricter fallback
    max_retry_count=5,                # More retries
    schedule_buffer_seconds=120       # Wider buffer
)
```

---

## Real-World Examples

### Example 1: Hourly Sync
```python
# Cron: "0 * * * *" (every hour)
# Schedule at: 10:00, 11:00, 12:00...

# Scenario A: Normal run at 10:00:15
should_run = True  # Within 60s buffer

# Scenario B: Scheduler down from 10:00-10:20
should_run = True  # Within 30-min fallback window

# Scenario C: Check at 10:45
should_run = False  # Outside fallback, will catch at 11:00
```

### Example 2: Daily Sync
```python
# Cron: "0 0 * * *" (daily at midnight)
# Schedule at: 00:00

# Scenario A: Runs at 00:00:10
should_run = True  # Within 60s buffer

# Scenario B: Scheduler starts at 00:25
should_run = True  # Within 30-min fallback

# Scenario C: Scheduler starts at 01:00
should_run = False  # Outside fallback, wait for tomorrow
```

### Example 3: Catch-Up After Downtime
```python
# Cron: "*/15 * * * *" (every 15 minutes)
# Downtime: 10:00 - 11:00
# Last run: 10:00
# Current: 11:05

# Check: time_since_last_run (65 min) >= interval (15 min)
should_run = True  # Catch up immediately
```

---

## Configuration Reference

### Default Values
```python
EnhancedStrategySelector(
    safe_fallback_window_minutes=30,  # First run grace period
    max_retry_count=3,                 # Max consecutive failures
    schedule_buffer_seconds=60         # Window around scheduled time
)
```

### Tuning Recommendations

| Parameter | Conservative | Balanced | Aggressive |
|-----------|-------------|----------|------------|
| safe_fallback_window_minutes | 15 | 30 | 60 |
| schedule_buffer_seconds | 30 | 60 | 120 |
| max_retry_count | 2 | 3 | 5 |

**Conservative:** Strict scheduling, less catch-up
**Balanced:** Default settings (recommended)
**Aggressive:** More forgiving, more catch-up attempts

---

## Troubleshooting

### Problem: Strategy never runs on first deployment
**Check:** Is the current time within 30 minutes of a schedule slot?
**Solution:** Either wait for next slot or increase `safe_fallback_window_minutes`

### Problem: Strategy runs twice for same schedule
**Check:** Are `last_run` and `last_scheduled_at` being updated?
**Solution:** Ensure state manager properly updates these fields

### Problem: Strategy stuck in retry loop
**Check:** Is `retry_count` being incremented on failure?
**Solution:** Verify state manager increments retry count, or lower `max_retry_count`

### Problem: Overdue catch-up not working
**Check:** Is `last_run` timestamp accurate?
**Solution:** Ensure `last_run` is updated only on successful completion

---

## Quick Test Commands

```bash
# Run all strategy selector tests
pytest tests/test_enhanced_strategy_selector.py -v

# Run only missing state tests
pytest tests/test_enhanced_strategy_selector.py -k "missing_state" -v

# Run only safety check tests  
pytest tests/test_enhanced_strategy_selector.py -k "retry_count or already_running" -v

# Run only cron schedule tests
pytest tests/test_enhanced_strategy_selector.py -k "cron" -v

# Run with coverage report
pytest tests/test_enhanced_strategy_selector.py --cov=synctool.scheduler.enhanced_strategy_selector --cov-report=html
```

