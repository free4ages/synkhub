"""
Test cases for EnhancedStrategySelector._should_run_strategy method.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch
from synctool.scheduler.enhanced_strategy_selector import EnhancedStrategySelector
from synctool.scheduler.pipeline_state_manager import StrategyRunState, PipelineStateManager


class TestShouldRunStrategy:
    """Test suite for _should_run_strategy method"""
    
    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock state manager"""
        manager = Mock(spec=PipelineStateManager)
        return manager
    
    @pytest.fixture
    def strategy_selector(self, mock_state_manager):
        """Create a strategy selector with mocked dependencies"""
        return EnhancedStrategySelector(
            state_manager=mock_state_manager,
            safe_fallback_window_minutes=30,
            max_retry_count=3,
            schedule_buffer_seconds=60
        )
    
    # Test 1: Missing state - within fallback window
    def test_missing_state_within_fallback_window(self, strategy_selector, mock_state_manager):
        """Test that strategy runs when state is missing and within fallback window"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 5 minutes after the hour (within 30 min fallback window)
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: No state exists
        mock_state_manager.get_current_state.return_value = None
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "First run or missing state" in reason
        assert "within" in reason.lower()
        mock_state_manager.get_current_state.assert_called_once_with(pipeline_id, strategy_name)
    
    # Test 2: Missing state - outside fallback window
    def test_missing_state_outside_fallback_window(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when state is missing and outside fallback window"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 45 minutes after the hour (outside 30 min fallback window)
        current_time = datetime(2025, 10, 15, 10, 45, 0, tzinfo=timezone.utc)
        
        # Mock: No state exists
        mock_state_manager.get_current_state.return_value = None
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "outside fallback window" in reason.lower()
        assert "missing" in reason.lower()
    
    # Test 3: State exists but last_run is None - within fallback window
    def test_state_exists_no_last_run_within_window(self, strategy_selector, mock_state_manager):
        """Test strategy runs when state exists but no last_run, within window"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: State exists but no last_run
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="ready",
            last_run=None,
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "First run or missing state" in reason
    
    # Test 4: Max retry count exceeded
    def test_max_retry_count_exceeded(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when max retry count is exceeded"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: State with max retries exceeded
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="failed",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=3  # Equals max_retry_count
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Max retry count" in reason
        assert "exceeded" in reason
    
    # Test 5: Strategy already running
    def test_strategy_already_running(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when already running"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: State with running status
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="running",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "already running" in reason.lower()
    
    # Test 6: Already ran for this schedule slot
    def test_already_ran_for_schedule_slot(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when already ran for current schedule slot"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 10:05, last scheduled was 10:00
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: Already ran at 10:01 (after the 10:00 schedule)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T10:01:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Already ran" in reason
    
    # Test 7: Already scheduled for this slot
    def test_already_scheduled_for_slot(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when already scheduled for current slot"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 10:05, last scheduled was 10:00
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: Last ran at 9:00, but already scheduled at 10:02
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="pending",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:02:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Already scheduled" in reason
    
    # Test 8: Due to run - within schedule buffer
    def test_due_to_run_within_buffer(self, strategy_selector, mock_state_manager):
        """Test that strategy runs when due and within schedule buffer"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 10:00:30 (30 seconds after schedule, within 60s buffer)
        current_time = datetime(2025, 10, 15, 10, 0, 30, tzinfo=timezone.utc)
        
        # Mock: Last ran at 9:00
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Due to run" in reason
        assert "gap" in reason.lower()
    
    # Test 9: Not due yet - before schedule buffer
    def test_not_due_yet(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when not due yet"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 9:58:00 (2 minutes before scheduled time)
        # Last scheduled time would be 9:00, next would be 10:00
        current_time = datetime(2025, 10, 15, 9, 58, 0, tzinfo=timezone.utc)
        
        # Mock: Last ran at 9:00
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:30+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Not due yet" in reason or "Already ran" in reason
    
    # Test 10: Overdue - catch up scenario
    def test_overdue_catch_up(self, strategy_selector, mock_state_manager):
        """Test that strategy runs when overdue (catch up)"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 10:30 (30 minutes past scheduled time at 10:00)
        current_time = datetime(2025, 10, 15, 10, 30, 0, tzinfo=timezone.utc)
        
        # Mock: Last ran at 8:00 (more than one interval ago)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T08:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Overdue" in reason or "overdue" in reason.lower()
    
    # Test 11: Overdue but recently ran - don't catch up
    def test_overdue_but_recently_ran(self, strategy_selector, mock_state_manager):
        """Test that strategy doesn't run when overdue but ran recently"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour at minute 0
        
        # Current time is 10:30 (30 minutes past scheduled time at 10:00)
        current_time = datetime(2025, 10, 15, 10, 30, 0, tzinfo=timezone.utc)
        
        # Mock: Last ran at 10:15 (recently, within one interval)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T10:15:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        # Should not run because we ran recently
    
    # Test 12: Timezone handling - naive datetime
    def test_timezone_handling_naive_datetime(self, strategy_selector, mock_state_manager):
        """Test that naive datetimes are converted to UTC"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        
        # Naive datetime (no timezone)
        current_time = datetime(2025, 10, 15, 10, 5, 0)  # No tzinfo
        
        # Mock: State with naive datetime
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:00",  # No timezone
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute - should not raise an error
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - should handle gracefully
        assert isinstance(should_run, bool)
        assert isinstance(reason, str)
    
    # Test 13: Daily cron - within buffer
    def test_daily_cron_within_buffer(self, strategy_selector, mock_state_manager):
        """Test daily cron schedule within buffer"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "daily_sync"
        cron_expr = "0 0 * * *"  # Daily at midnight
        
        # Current time is 00:00:30 (30 seconds after midnight)
        current_time = datetime(2025, 10, 15, 0, 0, 30, tzinfo=timezone.utc)
        
        # Mock: Last ran yesterday
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-14T00:00:30+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Due to run" in reason
    
    # Test 14: Every 15 minutes cron
    def test_frequent_cron_every_15_minutes(self, strategy_selector, mock_state_manager):
        """Test frequent cron schedule (every 15 minutes)"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "frequent_sync"
        cron_expr = "*/15 * * * *"  # Every 15 minutes
        
        # Current time is 10:15:10
        current_time = datetime(2025, 10, 15, 10, 15, 10, tzinfo=timezone.utc)
        
        # Mock: Last ran at 10:00
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T10:00:05+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Due to run" in reason
    
    # Test 15: Invalid cron expression
    def test_invalid_cron_expression(self, strategy_selector, mock_state_manager):
        """Test handling of invalid cron expression"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "invalid_sync"
        cron_expr = "invalid cron"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Mock: State exists
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Error" in reason or "error" in reason.lower()
    
    # Test 16: Last scheduled at but outside schedule buffer
    def test_last_scheduled_at_outside_buffer(self, strategy_selector, mock_state_manager):
        """Test that strategy can run again if last_scheduled_at is old"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 10:00:30
        current_time = datetime(2025, 10, 15, 10, 0, 30, tzinfo=timezone.utc)
        
        # Mock: Last scheduled at 9:00, last ran at 8:00
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T08:00:00+00:00",
            last_scheduled_at="2025-10-15T09:00:30+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Due to run" in reason
    
    # Test 17: Retry count below max - but failed status blocks retry
    def test_retry_count_below_max_failed_status(self, strategy_selector, mock_state_manager):
        """Test that failed jobs don't retry automatically even with retry count below max"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        current_time = datetime(2025, 10, 15, 10, 0, 30, tzinfo=timezone.utc)
        
        # Mock: State with retry count below max but failed status
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="failed",  # Failed status
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=2  # Below max of 3
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Failed jobs should NOT auto-retry
        assert should_run is False
        assert "failed" in reason.lower()
        assert "waiting for next regular schedule" in reason
    
    # Test 18: Weekly cron schedule
    def test_weekly_cron_schedule(self, strategy_selector, mock_state_manager):
        """Test weekly cron schedule"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "weekly_sync"
        cron_expr = "0 0 * * 0"  # Sunday at midnight
        
        # Current time is Sunday 00:00:30
        current_time = datetime(2025, 10, 19, 0, 0, 30, tzinfo=timezone.utc)  # Sunday
        
        # Mock: Last ran last Sunday
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-12T00:00:30+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Due to run" in reason
    
    # Test 19: First run with cron that just passed
    def test_first_run_cron_just_passed(self, strategy_selector, mock_state_manager):
        """Test first run when cron slot just passed (edge case)"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 10:00:05 (5 seconds after the hour)
        current_time = datetime(2025, 10, 15, 10, 0, 5, tzinfo=timezone.utc)
        
        # Mock: No state
        mock_state_manager.get_current_state.return_value = None
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "First run" in reason or "missing state" in reason.lower()
    
    # Test 20: Boundary test - exactly at schedule buffer limit
    def test_exactly_at_schedule_buffer_limit(self, strategy_selector, mock_state_manager):
        """Test edge case when exactly at schedule buffer limit"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        
        # Current time is exactly 60 seconds (buffer limit) after scheduled time
        current_time = datetime(2025, 10, 15, 10, 1, 0, tzinfo=timezone.utc)
        
        # Mock: Last ran at 9:00
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        # Should run because gap is <= buffer (0 <= 60 <= 60)
        assert should_run is True
        assert "Due to run" in reason


class TestCanEnqueueStrategy:
    """Test suite for can_enqueue_strategy method"""
    
    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock state manager"""
        manager = Mock(spec=PipelineStateManager)
        return manager
    
    @pytest.fixture
    def strategy_selector(self, mock_state_manager):
        """Create a strategy selector with mocked dependencies"""
        return EnhancedStrategySelector(
            state_manager=mock_state_manager,
            safe_fallback_window_minutes=30,
            max_retry_count=3,
            schedule_buffer_seconds=60
        )
    
    def test_can_enqueue_no_state(self, strategy_selector, mock_state_manager):
        """Test that strategy can be enqueued when no state exists"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        mock_state_manager.get_current_state.return_value = None
        
        # Execute
        can_enqueue, reason = strategy_selector.can_enqueue_strategy(
            pipeline_id, strategy_name, current_time
        )
        
        # Assert
        assert can_enqueue is True
        assert "first run" in reason.lower()
    
    def test_cannot_enqueue_already_running(self, strategy_selector, mock_state_manager):
        """Test that strategy cannot be enqueued when already running"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="running",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        can_enqueue, reason = strategy_selector.can_enqueue_strategy(
            pipeline_id, strategy_name, current_time
        )
        
        # Assert
        assert can_enqueue is False
        assert "already running" in reason.lower()
    
    def test_cannot_enqueue_recently_pending(self, strategy_selector, mock_state_manager):
        """Test that strategy cannot be enqueued when recently pending"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Pending for 2 minutes (less than 5 minute threshold)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="pending",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:03:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        can_enqueue, reason = strategy_selector.can_enqueue_strategy(
            pipeline_id, strategy_name, current_time
        )
        
        # Assert
        assert can_enqueue is False
        assert "already pending" in reason.lower()
    
    def test_can_enqueue_stuck_pending(self, strategy_selector, mock_state_manager):
        """Test that strategy can be enqueued when stuck in pending too long"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        # Pending for 6 minutes (more than 5 minute threshold - stuck)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="pending",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T09:59:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        can_enqueue, reason = strategy_selector.can_enqueue_strategy(
            pipeline_id, strategy_name, current_time
        )
        
        # Assert
        assert can_enqueue is True
        assert "can enqueue" in reason.lower()
    
    def test_can_enqueue_with_success_status(self, strategy_selector, mock_state_manager):
        """Test that strategy can be enqueued with success status"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        current_time = datetime(2025, 10, 15, 10, 5, 0, tzinfo=timezone.utc)
        
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="success",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        can_enqueue, reason = strategy_selector.can_enqueue_strategy(
            pipeline_id, strategy_name, current_time
        )
        
        # Assert
        assert can_enqueue is True
        assert "can enqueue" in reason.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

