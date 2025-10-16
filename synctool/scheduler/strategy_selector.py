from typing import Optional, List, Tuple
from datetime import datetime
import logging

from ..core.models import PipelineJobConfig, StrategyConfig
from .pipeline_state_manager import PipelineStateManager


class StrategySelector:
    """Selects which strategy to run based on cron intervals"""
    
    def __init__(self, state_manager: PipelineStateManager):
        self.state_manager = state_manager
        self.logger = logging.getLogger(__name__)
    
    def select_strategy(self, config: PipelineJobConfig) -> Optional[Tuple[StrategyConfig, str]]:
        """
        Select strategy based on cron schedule.
        If multiple strategies are due, select the one with the longest interval (highest priority).
        Daily > Hourly > Every 15 minutes, etc.
        
        Returns (StrategyConfig, strategy_name) or None if nothing should run.
        """
        current_time = datetime.now()
        pipeline_id = config.name
        
        eligible_strategies = []
        
        # Iterate through all stages and strategies
        for stage in config.stages:
            for strategy_config in stage.strategies:
                if not strategy_config.enabled:
                    continue
                
                if not strategy_config.cron:
                    continue
                
                strategy_name = strategy_config.name
                
                # Check if this strategy should run based on cron and last run time
                should_run = self._should_run_strategy(
                    pipeline_id, strategy_name, strategy_config.cron, current_time
                )
                
                if should_run:
                    # Calculate cron interval (time until next run)
                    interval_seconds = self._get_cron_interval(strategy_config.cron, current_time)
                    eligible_strategies.append((strategy_config, strategy_name, interval_seconds))
        
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
            from croniter import croniter
            
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
    ) -> bool:
        """
        Check if strategy should run based on cron schedule and last run time.
        Returns True if the strategy is due to run.
        """
        try:
            from croniter import croniter
            
            cron = croniter(cron_expr, current_time)
            last_scheduled = cron.get_prev(datetime)
            
            # Get last run time from state
            state = self.state_manager.get_current_state(pipeline_id)
            
            if state and state.strategy == strategy_name and state.last_run:
                last_run = datetime.fromisoformat(state.last_run)
                
                # Don't run if already ran after the last scheduled time
                if last_run >= last_scheduled:
                    return False
            
            # Calculate time gap since last scheduled time
            time_gap = (current_time - last_scheduled).total_seconds()
            
            # Should run if within scheduling window (e.g., 60 seconds)
            # This prevents missing scheduled runs if scheduler was down
            should_run = 0 <= time_gap < 60
            
            return should_run
            
        except Exception as e:
            self.logger.error(f"Error checking strategy {pipeline_id}:{strategy_name}: {e}")
            return False

