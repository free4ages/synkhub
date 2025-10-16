"""
Enhanced strategy selector with robust scheduling logic.
Uses the original priority-based selection (picks ONE strategy with highest interval, not all).
"""

import logging
from typing import Optional, Tuple
from datetime import datetime, timezone
from croniter import croniter

from ..core.models import PipelineJobConfig, StrategyConfig
from .pipeline_state_manager import PipelineStateManager


class EnhancedStrategySelector:
    """
    Selects which strategy to run based on cron intervals with robust safety checks.
    If multiple strategies are due, select the one with the longest interval (highest priority).
    Daily > Hourly > Every 15 minutes, etc.
    """
    
    def __init__(
        self,
        state_manager: PipelineStateManager,
        safe_fallback_window_minutes: int = 30,
        max_retry_count: int = 3,
        schedule_buffer_seconds: int = 60
    ):
        """
        Initialize strategy selector.
        
        Args:
            state_manager: Pipeline state manager
            safe_fallback_window_minutes: If state is missing, only enqueue if next
                                         scheduled time is within this window
            max_retry_count: Maximum consecutive retry attempts before giving up
            schedule_buffer_seconds: Buffer window to consider a strategy "due"
        """
        self.state_manager = state_manager
        self.safe_fallback_window_minutes = safe_fallback_window_minutes
        self.max_retry_count = max_retry_count
        self.schedule_buffer_seconds = schedule_buffer_seconds
        self.logger = logging.getLogger(__name__)
    
    def select_strategy(self, config: PipelineJobConfig) -> Optional[Tuple[StrategyConfig, str]]:
        """
        Select strategy based on cron schedule.
        If multiple strategies are due, select the one with the longest interval (highest priority).
        Daily > Hourly > Every 15 minutes, etc.
        
        Returns (StrategyConfig, strategy_name) or None if nothing should run.
        """
        current_time = datetime.now(timezone.utc)
        pipeline_id = config.name
        
        eligible_strategies = []
        

        for strategy_config in config.strategies:
            if not strategy_config.enabled:
                continue
            
            if not strategy_config.cron:
                continue
            
            strategy_name = strategy_config.name
            
            # Check if this strategy should run based on cron and last run time
            should_run, reason = self._should_run_strategy(
                pipeline_id, strategy_name, strategy_config.cron, current_time
            )
            
            if should_run:
                # Calculate cron interval (time until next run)
                interval_seconds = self._get_cron_interval(strategy_config.cron, current_time)
                eligible_strategies.append((strategy_config, strategy_name, interval_seconds))
                self.logger.debug(
                    f"Strategy '{strategy_name}' is eligible: {reason} (interval: {interval_seconds}s)"
                )
            else:
                self.logger.debug(f"Strategy '{strategy_name}' not due: {reason}")
        
        # Select strategy with maximum interval (longest time between runs = highest priority)
        if eligible_strategies:
            # Sort by interval (descending) - daily strategies have higher priority than hourly
            eligible_strategies.sort(key=lambda x: x[2], reverse=True)
            selected = eligible_strategies[0]
            interval_str = self._format_interval(selected[2])
            self.logger.info(
                f"Selected strategy '{selected[1]}' for pipeline '{pipeline_id}' "
                f"(interval: {interval_str}, priority: highest)"
            )
            return (selected[0], selected[1])
        
        return None
    
    def _get_cron_interval(self, cron_expr: str, current_time: datetime) -> float:
        """
        Calculate the interval (in seconds) between consecutive runs of this cron expression.
        This represents the "frequency" - larger values mean less frequent runs.
        """
        try:
            cron = croniter(cron_expr, current_time)
            next_run = cron.get_next(datetime)
            following_run = cron.get_next(datetime)
            
            interval = (following_run - next_run).total_seconds()
            return interval
            
        except Exception as e:
            self.logger.error(f"Error calculating cron interval for '{cron_expr}': {e}")
            return 0.0
    
    def _format_interval(self, seconds: float) -> str:
        """Format interval in human-readable form"""
        if seconds >= 86400:
            days = seconds / 86400
            return f"{days:.1f} day(s)"
        elif seconds >= 3600:
            hours = seconds / 3600
            return f"{hours:.1f} hour(s)"
        elif seconds >= 60:
            minutes = seconds / 60
            return f"{minutes:.1f} minute(s)"
        else:
            return f"{seconds:.0f} second(s)"
    
    def _should_run_strategy(
        self,
        pipeline_id: str,
        strategy_name: str,
        cron_expr: str,
        current_time: datetime
    ) -> Tuple[bool, str]:
        """
        Determine if a strategy should run based on cron schedule and state.
        
        Logic:
        1. Compute next scheduled run time using cron expression and last_success_run
        2. Check last_scheduled_at to avoid duplicate enqueues
        3. Apply safe fallback window if state is missing
        4. Respect max retry count for failing strategies
        5. Use UTC consistently
        
        Args:
            pipeline_id: Pipeline identifier
            strategy_name: Strategy name
            cron_expr: Cron expression for scheduling
            current_time: Current UTC time
        
        Returns:
            (should_run: bool, reason: str)
        """
        try:
            # Ensure current_time is UTC
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
            
            # Get strategy state
            state = self.state_manager.get_current_state(pipeline_id, strategy_name)
            
            # Check retry limit (only applies to failed jobs, not skipped)
            if state and state.status == "failed" and state.retry_count >= self.max_retry_count:
                return False, f"Max retry count ({self.max_retry_count}) exceeded for failed job"
            
            # Check if strategy is currently running
            if state and state.status == "running":
                return False, "Strategy is already running"
            
            # Don't automatically retry failed jobs - let cron schedule handle new attempts
            if state and state.status == "failed":
                # Failed jobs wait for their next regular schedule slot
                # (they don't use slot-aware retry, only skipped jobs do)
                return False, "Job failed on last attempt, waiting for next regular schedule"
            
            # Initialize croniter
            cron = croniter(cron_expr, current_time)
            
            # Get the previous scheduled time (when it should have last run)
            last_scheduled_time = cron.get_prev(datetime)
            
            # Get the next scheduled time (when it should run next)
            next_scheduled_time = cron.get_next(datetime)
            
            # Check if we have state
            if state is None or state.last_run is None:
                # Graceful fallback: only run if this schedule slot is near (within fallback window)
                # Check if we're within the fallback window AFTER the last scheduled time
                time_since_prev = (current_time - last_scheduled_time).total_seconds()
                fallback_window_seconds = self.safe_fallback_window_minutes * 60
                
                # Only run if we're within the fallback window after the previous scheduled time
                if 0 <= time_since_prev <= fallback_window_seconds:
                    return True, f"First run or missing state — within {time_since_prev:.0f}s of schedule slot"
                else:
                    return False, f"State missing and outside fallback window (gap: {time_since_prev:.0f}s)"
            
            # Parse last success run time
            last_success_dt = datetime.fromisoformat(state.last_run)
            if last_success_dt.tzinfo is None:
                last_success_dt = last_success_dt.replace(tzinfo=timezone.utc)
            
            # Check if we've already run after the last scheduled time
            if last_success_dt >= last_scheduled_time:
                return False, f"Already ran at {last_success_dt.isoformat()}, last scheduled was {last_scheduled_time.isoformat()}"
            
            # Check last_scheduled_at to avoid duplicate enqueues
            if state.last_scheduled_at:
                last_scheduled_at_dt = datetime.fromisoformat(state.last_scheduled_at)
                if last_scheduled_at_dt.tzinfo is None:
                    last_scheduled_at_dt = last_scheduled_at_dt.replace(tzinfo=timezone.utc)
                
                # If we've already scheduled this strategy recently for this cron slot, skip
                if last_scheduled_at_dt >= last_scheduled_time:
                    # EXCEPTION: Allow retry if status is "skipped" AND we're still in current slot
                    if state.status == "skipped":
                        # Calculate next scheduled time to check if we're still in this slot
                        cron = croniter(cron_expr, current_time)
                        next_scheduled_time = cron.get_next(datetime)
                        
                        # Only retry if we're still within the current schedule slot
                        if current_time < next_scheduled_time:
                            # Calculate adaptive backoff based on cron interval
                            cron_interval = self._get_cron_interval(cron_expr, current_time)
                            # Use 50% of interval, min 30s, max 5min
                            retry_backoff_seconds = max(30, min(300, cron_interval * 0.5))
                            
                            # Check how long since last attempt
                            if state.last_attempted_at:
                                last_attempt_dt = datetime.fromisoformat(state.last_attempted_at)
                                if last_attempt_dt.tzinfo is None:
                                    last_attempt_dt = last_attempt_dt.replace(tzinfo=timezone.utc)
                                
                                time_since_skip = (current_time - last_attempt_dt).total_seconds()
                                
                                if time_since_skip >= retry_backoff_seconds:
                                    time_until_next = (next_scheduled_time - current_time).total_seconds()
                                    return True, (
                                        f"Retrying skipped job within current slot "
                                        f"(skipped {time_since_skip:.0f}s ago, "
                                        f"{time_until_next:.0f}s until next slot)"
                                    )
                                else:
                                    wait_time = retry_backoff_seconds - time_since_skip
                                    return False, f"Recently skipped, retry in {wait_time:.0f}s (within current slot)"
                            else:
                                # No last_attempted_at, safe to retry
                                return True, "Retrying skipped job within current slot"
                        else:
                            # We've moved to a new schedule slot - abandon old retry
                            self.logger.info(
                                f"Abandoning retry for {pipeline_id}:{strategy_name} - "
                                f"moved to new schedule slot"
                            )
                            return False, "Moved to new schedule slot, abandoning old retry"
                    
                    return False, f"Already scheduled at {last_scheduled_at_dt.isoformat()}"
            
            # Calculate time gap since last scheduled time
            time_gap = (current_time - last_scheduled_time).total_seconds()
            
            # Should run if within scheduling window
            if 0 <= time_gap <= self.schedule_buffer_seconds:
                return True, f"Due to run (last scheduled: {last_scheduled_time.isoformat()}, gap: {time_gap:.0f}s)"
            
            # Also check if we're overdue (missed a schedule)
            if time_gap > self.schedule_buffer_seconds:
                # We're past the scheduled time - check if we should catch up
                # Don't catch up if we've already successfully run recently
                time_since_success = (current_time - last_success_dt).total_seconds()
                cron_interval = self._get_cron_interval(cron_expr, current_time)
                
                # If we haven't run in over one interval, we should run now
                if time_since_success >= cron_interval:
                    return True, f"Overdue (gap: {time_gap:.0f}s, last success: {time_since_success:.0f}s ago)"
            
            return False, f"Not due yet (gap: {time_gap:.0f}s, buffer: {self.schedule_buffer_seconds}s)"
        
        except Exception as e:
            self.logger.error(
                f"Error checking if strategy {pipeline_id}:{strategy_name} should run: {e}",
                exc_info=True
            )
            return False, f"Error: {str(e)}"
    
    def can_enqueue_strategy(
        self,
        pipeline_id: str,
        strategy_name: str,
        current_time: Optional[datetime] = None
    ) -> Tuple[bool, str]:
        """
        Additional check before enqueuing to prevent duplicate enqueues.
        This is called after acquiring the enqueue lock.
        
        Returns:
            (can_enqueue: bool, reason: str)
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        state = self.state_manager.get_current_state(pipeline_id, strategy_name)
        
        if state is None:
            return True, "No state, first run"
        
        # Check if already running
        if state.status == "running":
            return False, "Already running"
        
        # Check if already pending (enqueued but not started)
        if state.status == "pending":
            # Check if it's been pending too long (stuck)
            if state.last_scheduled_at:
                last_scheduled_dt = datetime.fromisoformat(state.last_scheduled_at)
                if last_scheduled_dt.tzinfo is None:
                    last_scheduled_dt = last_scheduled_dt.replace(tzinfo=timezone.utc)
                
                time_pending = (current_time - last_scheduled_dt).total_seconds()
                if time_pending < 300:  # Less than 5 minutes
                    return False, f"Already pending for {time_pending:.0f}s"
        
        return True, "Can enqueue"
