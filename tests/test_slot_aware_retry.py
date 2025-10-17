"""
Test cases for slot-aware retry mechanism.
Tests the new logic that retries skipped jobs within their schedule slot.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
from synctool.scheduler.enhanced_strategy_selector import EnhancedStrategySelector
from synctool.scheduler.pipeline_state_manager import StrategyRunState, PipelineStateManager


class TestSlotAwareRetry:
    """Test suite for slot-aware retry logic"""
    
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
    
    # Test 1: Skipped job - within slot, backoff expired
    def test_skipped_job_retry_within_slot(self, strategy_selector, mock_state_manager):
        """Test that skipped job retries within current slot after backoff"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 10:06 (6 minutes after 10:00 slot)
        current_time = datetime(2025, 10, 15, 10, 6, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:01, backoff is 5 minutes (capped)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",  # Key: status is skipped
            last_run="2025-10-15T09:00:00+00:00",  # Last success at 9:00
            last_scheduled_at="2025-10-15T10:00:10+00:00",  # Scheduled at 10:00
            last_attempted_at="2025-10-15T10:01:00+00:00",  # Skipped at 10:01
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is True
        assert "Retrying skipped job" in reason
        assert "within current slot" in reason
    
    # Test 2: Skipped job - within slot, backoff NOT expired
    def test_skipped_job_too_soon_to_retry(self, strategy_selector, mock_state_manager):
        """Test that skipped job doesn't retry before backoff expires"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 10:03 (3 minutes after 10:00 slot)
        current_time = datetime(2025, 10, 15, 10, 3, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:01 (2 minutes ago), backoff is 5 minutes
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:00:10+00:00",
            last_attempted_at="2025-10-15T10:01:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Recently skipped" in reason
        assert "retry in" in reason
    
    # Test 3: Skipped job - moved to new slot
    def test_skipped_job_new_slot_abandons_retry(self, strategy_selector, mock_state_manager):
        """Test that skipped job is abandoned when new slot arrives"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 11:01 (new slot at 11:00 has arrived)
        current_time = datetime(2025, 10, 15, 11, 1, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:01 (old 10:00 slot)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:00:10+00:00",
            last_attempted_at="2025-10-15T10:01:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "Moved to new schedule slot" in reason or "abandoning" in reason.lower()
    
    # Test 4: Failed job - should NOT retry (not skipped)
    def test_failed_job_no_automatic_retry(self, strategy_selector, mock_state_manager):
        """Test that failed jobs don't get automatic slot-aware retry"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"  # Every hour
        
        # Current time is 10:06
        current_time = datetime(2025, 10, 15, 10, 6, 0, tzinfo=timezone.utc)
        
        # Mock: Failed at 10:01 (NOT skipped)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="failed",  # Key: status is failed, not skipped
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:00:10+00:00",
            last_attempted_at="2025-10-15T10:01:00+00:00",
            retry_count=1,
            error="Database connection failed"
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert
        assert should_run is False
        assert "failed" in reason.lower()
        assert "waiting for next regular schedule" in reason
    
    # Test 5: Daily job - skipped, retries throughout the day
    def test_daily_job_skipped_retries_all_day(self, strategy_selector, mock_state_manager):
        """Test that daily skipped job retries every 5 min throughout the day"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "daily_sync"
        cron_expr = "0 2 * * *"  # Daily at 2:00 AM
        
        # Current time is 2:30 AM (30 min after schedule)
        current_time = datetime(2025, 10, 15, 2, 30, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped at 2:25 AM (5 minutes ago)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-14T02:00:30+00:00",  # Yesterday
            last_scheduled_at="2025-10-15T02:00:10+00:00",
            last_attempted_at="2025-10-15T02:25:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry (still in 24-hour slot, backoff expired)
        assert should_run is True
        assert "Retrying skipped job" in reason
        assert "within current slot" in reason
    
    # Test 6: Every 2 minute job - fast backoff
    def test_frequent_job_adaptive_backoff(self, strategy_selector, mock_state_manager):
        """Test that frequent jobs have shorter backoff (50% of interval)"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "frequent_sync"
        cron_expr = "*/2 * * * *"  # Every 2 minutes
        
        # Current time is 10:01:10 (70 seconds after 10:00 schedule)
        current_time = datetime(2025, 10, 15, 10, 1, 10, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:00:10 (60 seconds ago)
        # Backoff should be ~60s (50% of 120s), so should retry now
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:58:00+00:00",
            last_scheduled_at="2025-10-15T10:00:05+00:00",
            last_attempted_at="2025-10-15T10:00:10+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry (backoff ~60s has passed)
        assert should_run is True
        assert "Retrying skipped job" in reason
    
    # Test 7: Every 15 minute job - capped backoff
    def test_fifteen_minute_job_capped_backoff(self, strategy_selector, mock_state_manager):
        """Test that 15-min job backoff is capped at 5 minutes"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "quarter_hourly"
        cron_expr = "*/15 * * * *"  # Every 15 minutes
        
        # Current time is 10:06 (6 minutes after 10:00 schedule)
        current_time = datetime(2025, 10, 15, 10, 6, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:01 (5 minutes ago)
        # Backoff should be capped at 5 min (not 7.5 min which is 50% of 15)
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:45:00+00:00",
            last_scheduled_at="2025-10-15T10:00:10+00:00",
            last_attempted_at="2025-10-15T10:01:00+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry (5 min backoff has passed)
        assert should_run is True
        assert "Retrying skipped job" in reason
    
    # Test 8: Skipped job without last_attempted_at
    def test_skipped_job_no_last_attempted_at(self, strategy_selector, mock_state_manager):
        """Test skipped job retry when last_attempted_at is missing"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        
        current_time = datetime(2025, 10, 15, 10, 6, 0, tzinfo=timezone.utc)
        
        # Mock: Skipped but no last_attempted_at timestamp
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:00:00+00:00",
            last_scheduled_at="2025-10-15T10:00:10+00:00",
            last_attempted_at=None,  # Missing
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry immediately (no timestamp to check)
        assert should_run is True
        assert "Retrying skipped job" in reason
    
    # Test 9: Weekly job - skipped, long slot
    def test_weekly_job_skipped_long_slot(self, strategy_selector, mock_state_manager):
        """Test weekly job has full week to retry"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "weekly_sync"
        cron_expr = "0 0 * * 0"  # Sunday at midnight
        
        # Current time is Monday 10:00 AM (34 hours after Sunday midnight)
        current_time = datetime(2025, 10, 20, 10, 0, 0, tzinfo=timezone.utc)  # Monday
        
        # Mock: Skipped Sunday at 00:05
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-12T00:00:30+00:00",  # Previous Sunday
            last_scheduled_at="2025-10-19T00:00:05+00:00",  # This Sunday
            last_attempted_at="2025-10-19T00:05:00+00:00",  # 34 hours ago
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry (still in 7-day slot, backoff passed)
        assert should_run is True
        assert "Retrying skipped job" in reason
    
    # Test 10: Success status - normal scheduling (not retry)
    def test_success_status_normal_scheduling(self, strategy_selector, mock_state_manager):
        """Test that success status uses normal scheduling, not retry logic"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        
        # Current time is 10:00:30
        current_time = datetime(2025, 10, 15, 10, 0, 30, tzinfo=timezone.utc)
        
        # Mock: Success status (not skipped)
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
        
        # Assert - Should run via normal scheduling
        assert should_run is True
        assert "Due to run" in reason
        assert "skipped" not in reason.lower()
    
    # Test 11: Failed job with high retry count
    def test_failed_job_max_retry_exceeded(self, strategy_selector, mock_state_manager):
        """Test that failed jobs are blocked after max retries"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "hourly_sync"
        cron_expr = "0 * * * *"
        
        current_time = datetime(2025, 10, 15, 10, 0, 30, tzinfo=timezone.utc)
        
        # Mock: Failed with max retries
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="failed",
            last_run="2025-10-15T09:00:00+00:00",
            retry_count=3,  # Equals max
            error="Persistent error"
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
    
    # Test 12: Minimum backoff (30 seconds) for very frequent jobs
    def test_minimum_backoff_30_seconds(self, strategy_selector, mock_state_manager):
        """Test that backoff has minimum of 30 seconds"""
        # Setup
        pipeline_id = "test_pipeline"
        strategy_name = "very_frequent"
        cron_expr = "* * * * *"  # Every minute
        
        # Current time is 10:00:40 (40 seconds after schedule)
        current_time = datetime(2025, 10, 15, 10, 0, 40, tzinfo=timezone.utc)
        
        # Mock: Skipped at 10:00:05 (35 seconds ago)
        # 50% of 60s = 30s, so should be at minimum
        mock_state = StrategyRunState(
            strategy=strategy_name,
            status="skipped",
            last_run="2025-10-15T09:59:05+00:00",
            last_scheduled_at="2025-10-15T10:00:02+00:00",
            last_attempted_at="2025-10-15T10:00:05+00:00",
            retry_count=0
        )
        mock_state_manager.get_current_state.return_value = mock_state
        
        # Execute
        should_run, reason = strategy_selector._should_run_strategy(
            pipeline_id, strategy_name, cron_expr, current_time
        )
        
        # Assert - Should retry (35s > 30s minimum backoff)
        assert should_run is True
        assert "Retrying skipped job" in reason


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

