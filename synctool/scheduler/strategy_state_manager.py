"""
Strategy-level state management for pipeline execution.
Maintains per-strategy state including scheduling, execution, and retry information.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from threading import Lock


@dataclass
class StrategyState:
    """State for a single strategy within a pipeline"""
    pipeline_id: str
    strategy_name: str
    
    # Scheduling state
    last_scheduled_at: Optional[str] = None  # When strategy was last enqueued (ISO UTC)
    last_success_run: Optional[str] = None   # Last successful completion (ISO UTC)
    last_attempted_at: Optional[str] = None  # Last attempt start time (ISO UTC)
    
    # Status tracking
    status: str = "inactive"  # pending, running, failed, success, inactive, skipped
    
    # Retry management
    retry_count: int = 0  # Consecutive failed attempts
    
    # Execution metadata
    run_id: Optional[str] = None
    worker: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None
    updated_at: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StrategyState':
        return cls(**data)


@dataclass
class StrategyRunHistory:
    """Historical record of a strategy run"""
    pipeline_id: str
    strategy_name: str
    run_id: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    worker: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StrategyRunHistory':
        return cls(**data)


class StrategyStateManager:
    """
    Manages strategy-level state persistence to disk.
    Each strategy maintains independent state including scheduling and execution metadata.
    """
    
    def __init__(self, state_dir: str = "./data/strategy_states", max_history_per_strategy: int = 100):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.max_history_per_strategy = max_history_per_strategy
        self._locks: Dict[str, Lock] = {}
        self.logger = logging.getLogger(__name__)
    
    def _get_lock(self, pipeline_id: str, strategy_name: str) -> Lock:
        """Get or create a lock for a specific strategy"""
        lock_key = f"{pipeline_id}:{strategy_name}"
        if lock_key not in self._locks:
            self._locks[lock_key] = Lock()
        return self._locks[lock_key]
    
    def _get_strategy_dir(self, pipeline_id: str) -> Path:
        """Get directory for a pipeline's strategy states"""
        return self.state_dir / pipeline_id
    
    def _get_state_file(self, pipeline_id: str, strategy_name: str) -> Path:
        """Get state file path for a specific strategy"""
        strategy_dir = self._get_strategy_dir(pipeline_id)
        strategy_dir.mkdir(parents=True, exist_ok=True)
        return strategy_dir / f"{strategy_name}_state.json"
    
    def _get_history_file(self, pipeline_id: str, strategy_name: str) -> Path:
        """Get history file path for a specific strategy"""
        strategy_dir = self._get_strategy_dir(pipeline_id)
        strategy_dir.mkdir(parents=True, exist_ok=True)
        return strategy_dir / f"{strategy_name}_history.json"
    
    def get_strategy_state(self, pipeline_id: str, strategy_name: str) -> Optional[StrategyState]:
        """Get current state of a strategy"""
        state_file = self._get_state_file(pipeline_id, strategy_name)
        
        if not state_file.exists():
            return None
        
        with self._get_lock(pipeline_id, strategy_name):
            try:
                with open(state_file, 'r') as f:
                    data = json.load(f)
                return StrategyState.from_dict(data)
            except Exception as e:
                self.logger.error(f"Error reading state for {pipeline_id}:{strategy_name}: {e}")
                return None
    
    def update_strategy_state(self, state: StrategyState):
        """Update strategy state and optionally add to history"""
        state.updated_at = datetime.now(timezone.utc).isoformat()
        state_file = self._get_state_file(state.pipeline_id, state.strategy_name)
        
        with self._get_lock(state.pipeline_id, state.strategy_name):
            # Write current state
            with open(state_file, 'w') as f:
                json.dump(state.to_dict(), f, indent=2)
            
            self.logger.debug(f"Updated state for {state.pipeline_id}:{state.strategy_name} -> {state.status}")
    
    def add_to_history(
        self,
        pipeline_id: str,
        strategy_name: str,
        run_id: str,
        status: str,
        started_at: str,
        completed_at: Optional[str] = None,
        worker: Optional[str] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
        retry_count: int = 0
    ):
        """Add a run to strategy history"""
        history_entry = StrategyRunHistory(
            pipeline_id=pipeline_id,
            strategy_name=strategy_name,
            run_id=run_id,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            worker=worker,
            message=message,
            error=error,
            retry_count=retry_count
        )
        
        history_file = self._get_history_file(pipeline_id, strategy_name)
        
        with self._get_lock(pipeline_id, strategy_name):
            # Load existing history
            history = []
            if history_file.exists():
                try:
                    with open(history_file, 'r') as f:
                        history = json.load(f)
                except Exception:
                    history = []
            
            # Add new entry
            history.append(history_entry.to_dict())
            
            # Keep only last N runs
            history = history[-self.max_history_per_strategy:]
            
            # Write back
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2)
            
            self.logger.info(
                f"Added history entry for {pipeline_id}:{strategy_name} "
                f"run_id={run_id} status={status}"
            )
    
    def get_strategy_history(
        self,
        pipeline_id: str,
        strategy_name: str,
        limit: int = 20
    ) -> List[StrategyRunHistory]:
        """Get run history for a strategy"""
        history_file = self._get_history_file(pipeline_id, strategy_name)
        
        if not history_file.exists():
            return []
        
        with self._get_lock(pipeline_id, strategy_name):
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
                # Return most recent first
                return [
                    StrategyRunHistory.from_dict(h)
                    for h in history[-limit:]
                ][::-1]
            except Exception as e:
                self.logger.error(
                    f"Error reading history for {pipeline_id}:{strategy_name}: {e}"
                )
                return []
    
    def get_all_strategies_for_pipeline(self, pipeline_id: str) -> Dict[str, StrategyState]:
        """Get states for all strategies in a pipeline"""
        strategy_dir = self._get_strategy_dir(pipeline_id)
        
        if not strategy_dir.exists():
            return {}
        
        states = {}
        for state_file in strategy_dir.glob("*_state.json"):
            strategy_name = state_file.stem.replace("_state", "")
            state = self.get_strategy_state(pipeline_id, strategy_name)
            if state:
                states[strategy_name] = state
        
        return states
    
    def list_all_strategies(self) -> Dict[str, Dict[str, StrategyState]]:
        """List all strategies across all pipelines"""
        all_strategies = {}
        
        for pipeline_dir in self.state_dir.iterdir():
            if pipeline_dir.is_dir():
                pipeline_id = pipeline_dir.name
                all_strategies[pipeline_id] = self.get_all_strategies_for_pipeline(pipeline_id)
        
        return all_strategies
    
    def reset_strategy_retry_count(self, pipeline_id: str, strategy_name: str):
        """Reset retry count for a strategy (called on success)"""
        state = self.get_strategy_state(pipeline_id, strategy_name)
        if state:
            state.retry_count = 0
            self.update_strategy_state(state)
    
    def increment_retry_count(self, pipeline_id: str, strategy_name: str) -> int:
        """Increment retry count and return new value"""
        state = self.get_strategy_state(pipeline_id, strategy_name)
        if not state:
            state = StrategyState(
                pipeline_id=pipeline_id,
                strategy_name=strategy_name,
                retry_count=1
            )
        else:
            state.retry_count += 1
        
        self.update_strategy_state(state)
        return state.retry_count

