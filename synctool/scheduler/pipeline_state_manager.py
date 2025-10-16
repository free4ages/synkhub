import json
import os
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from threading import Lock
import uuid
import logging


@dataclass
class StrategyRunState:
    """State of a single strategy within a pipeline"""
    strategy: str
    status: str  # ready | running | waiting | skipped | failed | success | pending | inactive
    last_run: Optional[str] = None  # ISO timestamp of last successful run
    run_id: Optional[str] = None
    worker: Optional[str] = None
    message: Optional[str] = None
    updated_at: Optional[str] = None
    
    # Enhanced fields for strategy-level tracking
    last_scheduled_at: Optional[str] = None  # When strategy was last enqueued
    last_attempted_at: Optional[str] = None  # When worker last started execution
    retry_count: int = 0  # Consecutive failed attempts
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'StrategyRunState':
        return cls(**data)


@dataclass
class PipelineRunState:
    """State of a pipeline including all its strategies"""
    pipeline_id: str
    strategy: str  # Last selected/executed strategy
    status: str  # Overall pipeline status
    last_run: Optional[str] = None  # Last run timestamp
    run_id: Optional[str] = None
    worker: Optional[str] = None
    message: Optional[str] = None
    updated_at: Optional[str] = None
    
    # Dictionary of all strategy states
    strategy_states: Dict[str, StrategyRunState] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        # Convert strategy_states dict of objects to dict of dicts
        data['strategy_states'] = {
            name: state_obj.to_dict() if isinstance(state_obj, StrategyRunState) else state_obj
            for name, state_obj in self.strategy_states.items()
        }
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PipelineRunState':
        # Extract strategy_states and convert to StrategyRunState objects
        strategy_states_data = data.pop('strategy_states', {})
        strategy_states = {
            name: StrategyRunState.from_dict(state_dict) if isinstance(state_dict, dict) else state_dict
            for name, state_dict in strategy_states_data.items()
        }
        
        return cls(
            strategy_states=strategy_states,
            **data
        )
    
    def get_strategy_state(self, strategy_name: str) -> Optional[StrategyRunState]:
        """Get state for a specific strategy"""
        return self.strategy_states.get(strategy_name)
    
    def update_strategy_state(self, strategy_state: StrategyRunState):
        """Update state for a specific strategy"""
        strategy_state.updated_at = datetime.now(timezone.utc).isoformat()
        self.strategy_states[strategy_state.strategy] = strategy_state
        
        # Update pipeline-level fields when strategy updates
        self.strategy = strategy_state.strategy
        self.updated_at = strategy_state.updated_at


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


class PipelineStateManager:
    """
    Manages pipeline state persistence to disk with strategy-level granularity.
    
    Storage structure:
    - Single state file per pipeline: {pipeline}_state.json (contains pipeline + all strategies)
    - Separate history per strategy: {pipeline}_{strategy}_history.json (last N runs)
    """
    
    def __init__(self, state_dir: str = "./data/pipeline_states", max_runs_per_strategy: int = 50):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Create parallel history directory
        self.history_dir = self.state_dir.parent / "pipeline_history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_runs_per_strategy = max_runs_per_strategy
        self._locks: Dict[str, Lock] = {}
        self.logger = logging.getLogger(__name__)
    
    def _get_lock(self, pipeline_id: str) -> Lock:
        """Get or create a lock for a specific pipeline"""
        if pipeline_id not in self._locks:
            self._locks[pipeline_id] = Lock()
        return self._locks[pipeline_id]
    
    def _get_state_file(self, pipeline_id: str) -> Path:
        """Get state file path for a pipeline (contains pipeline + all strategies)"""
        return self.state_dir / f"{pipeline_id}_state.json"
    
    def _get_run_history_file(self, pipeline_id: str, strategy_name: str) -> Path:
        """Get run history file path for a specific strategy"""
        return self.history_dir / f"{pipeline_id}_{strategy_name}_history.json"
    
    def _load_pipeline_state(self, pipeline_id: str) -> Optional[PipelineRunState]:
        """Load complete pipeline state including all strategies"""
        state_file = self._get_state_file(pipeline_id)
        
        if not state_file.exists():
            return None
        
        try:
            with open(state_file, 'r') as f:
                data = json.load(f)
            return PipelineRunState.from_dict(data)
        except Exception as e:
            self.logger.error(f"Error reading state for {pipeline_id}: {e}")
            return None
    
    def _save_pipeline_state(self, pipeline_state: PipelineRunState):
        """Save complete pipeline state including all strategies"""
        state_file = self._get_state_file(pipeline_state.pipeline_id)
        
        try:
            pipeline_state.updated_at = datetime.now(timezone.utc).isoformat()
            with open(state_file, 'w') as f:
                json.dump(pipeline_state.to_dict(), f, indent=2)
        except Exception as e:
            self.logger.error(f"Error writing state for {pipeline_state.pipeline_id}: {e}")
    
    def get_current_state(self, pipeline_id: str, strategy_name: str = None) -> Optional[StrategyRunState]:
        """Get current state of a strategy. If strategy_name is None, gets last selected strategy state"""
        with self._get_lock(pipeline_id):
            pipeline_state = self._load_pipeline_state(pipeline_id)
            
            if not pipeline_state:
                return None
            
            if strategy_name is None:
                # Return state of last selected strategy
                if pipeline_state.strategy and pipeline_state.strategy in pipeline_state.strategy_states:
                    return pipeline_state.strategy_states[pipeline_state.strategy]
                # Or return first available strategy
                elif pipeline_state.strategy_states:
                    first_strategy = next(iter(pipeline_state.strategy_states.keys()))
                    return pipeline_state.strategy_states[first_strategy]
                return None
            
            return pipeline_state.get_strategy_state(strategy_name)
    
    def get_pipeline_state(self, pipeline_id: str) -> Optional[PipelineRunState]:
        """Get complete pipeline state including all strategies"""
        with self._get_lock(pipeline_id):
            return self._load_pipeline_state(pipeline_id)
    
    def update_state(self, strategy_state: StrategyRunState, pipeline_id: str):
        """
        Update strategy state within pipeline state.
        
        Automatically determines whether to update pipeline-level state by comparing run_ids:
        - If strategy's run_id matches current pipeline run_id → update both pipeline and strategy
        - If run_ids don't match → only update strategy state
        
        This prevents race conditions when:
        1. Scheduler enqueues Strategy B (pipeline run_id = B's run_id)
        2. Strategy A (still running) sends updates (A's run_id != pipeline run_id)
        3. Strategy A's updates only affect its strategy state, not pipeline state
        """
        with self._get_lock(pipeline_id):
            # Load existing pipeline state
            pipeline_state = self._load_pipeline_state(pipeline_id)
            
            if not pipeline_state:
                # Create new pipeline state
                pipeline_state = PipelineRunState(
                    pipeline_id=pipeline_id,
                    strategy=strategy_state.strategy,
                    status=strategy_state.status,
                    last_run=strategy_state.last_run,
                    run_id=strategy_state.run_id,
                    worker=strategy_state.worker,
                    message=strategy_state.message
                )
            
            # Always update the specific strategy state in the dictionary
            strategy_state.updated_at = datetime.now(timezone.utc).isoformat()
            pipeline_state.strategy_states[strategy_state.strategy] = strategy_state
            
            # Auto-detect if pipeline state should be updated
            # Update pipeline-level fields ONLY if run_ids match
            update_pipeline_level = (
                pipeline_state.run_id is None or 
                pipeline_state.run_id == strategy_state.run_id or 
                strategy_state.status == 'pending'
            )
            
            if update_pipeline_level:
                self.logger.debug(
                    f"Updating pipeline state for {pipeline_id} "
                    f"(run_id match: {strategy_state.run_id} or status: {strategy_state.status})"
                )
                pipeline_state.strategy = strategy_state.strategy
                pipeline_state.updated_at = strategy_state.updated_at
                
                # Update pipeline-level status based on strategy
                if strategy_state.status in ['running', 'pending']:
                    pipeline_state.status = strategy_state.status
                    pipeline_state.run_id = strategy_state.run_id
                    pipeline_state.worker = strategy_state.worker
                    pipeline_state.message = strategy_state.message
                elif strategy_state.status == 'success':
                    pipeline_state.status = 'success'
                    pipeline_state.last_run = strategy_state.last_run
                    pipeline_state.run_id = strategy_state.run_id
                    pipeline_state.worker = strategy_state.worker
                    pipeline_state.message = strategy_state.message
            else:
                self.logger.debug(
                    f"Skipping pipeline state update for {pipeline_id} "
                    f"(run_id mismatch: pipeline={pipeline_state.run_id}, "
                    f"strategy={strategy_state.run_id})"
                )
            
            # Save complete pipeline state
            self._save_pipeline_state(pipeline_state)
            
            # Append to history if status is terminal
            if strategy_state.status in ['success', 'failed', 'skipped']:
                self._append_to_history(pipeline_id, strategy_state)
    
    def _append_to_history(self, pipeline_id: str, strategy_state: StrategyRunState):
        """Append state to run history for a specific strategy and maintain max runs per strategy"""
        history_file = self._get_run_history_file(pipeline_id, strategy_state.strategy)
        
        # Load existing history for this strategy
        history = []
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
            except Exception:
                history = []
        
        # Create history entry
        history_entry = StrategyRunHistory(
            pipeline_id=pipeline_id,
            strategy_name=strategy_state.strategy,
            run_id=strategy_state.run_id or "unknown",
            status=strategy_state.status,
            started_at=strategy_state.last_attempted_at or strategy_state.updated_at,
            completed_at=strategy_state.updated_at,
            worker=strategy_state.worker,
            message=strategy_state.message,
            error=strategy_state.error,
            retry_count=strategy_state.retry_count
        )
        
        # Add new entry
        history.append(history_entry.to_dict())
        
        # Keep only last N runs for THIS strategy
        history = history[-self.max_runs_per_strategy:]
        
        # Write back to strategy-specific history file
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=2)
        
        self.logger.debug(
            f"Added history entry for {pipeline_id}:{strategy_state.strategy}, "
            f"total entries: {len(history)}"
        )
    
    def get_run_history(self, pipeline_id: str, strategy_name: str = None, limit: int = 10) -> List[StrategyRunHistory]:
        """Get run history for a strategy"""
        if strategy_name is None:
            # Get first strategy history for backward compat
            pattern = f"{pipeline_id}_*_history.json"
            history_files = list(self.history_dir.glob(pattern))
            if not history_files:
                return []
            history_file = history_files[0]
        else:
            history_file = self._get_run_history_file(pipeline_id, strategy_name)
        
        if not history_file.exists():
            return []
        
        with self._get_lock(pipeline_id):
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
                
                # Return most recent first
                return [StrategyRunHistory.from_dict(h) for h in history[-limit:]][::-1]
            except Exception as e:
                self.logger.error(f"Error reading history for {pipeline_id}:{strategy_name}: {e}")
                return []
    
    def list_all_states(self) -> Dict[str, Dict[str, StrategyRunState]]:
        """List current states of all pipelines and their strategies"""
        all_states = {}
        
        for state_file in self.state_dir.glob("*_state.json"):
            pipeline_id = state_file.stem.replace("_state", "")
            
            with self._get_lock(pipeline_id):
                pipeline_state = self._load_pipeline_state(pipeline_id)
                
                if pipeline_state and pipeline_state.strategy_states:
                    all_states[pipeline_id] = pipeline_state.strategy_states
        
        return all_states
    
    def get_all_strategies_for_pipeline(self, pipeline_id: str) -> Dict[str, StrategyRunState]:
        """Get states for all strategies in a pipeline"""
        with self._get_lock(pipeline_id):
            pipeline_state = self._load_pipeline_state(pipeline_id)
            
            if not pipeline_state:
                return {}
            
            return pipeline_state.strategy_states
    
    def reset_strategy_retry_count(self, pipeline_id: str, strategy_name: str):
        """Reset retry count for a strategy (called on success)"""
        strategy_state = self.get_current_state(pipeline_id, strategy_name)
        if strategy_state:
            strategy_state.retry_count = 0
            strategy_state.error = None
            self.update_state(strategy_state, pipeline_id)
    
    def increment_retry_count(self, pipeline_id: str, strategy_name: str) -> int:
        """Increment retry count and return new value"""
        strategy_state = self.get_current_state(pipeline_id, strategy_name)
        if not strategy_state:
            strategy_state = StrategyRunState(
                strategy=strategy_name,
                status="inactive",
                retry_count=1
            )
        else:
            strategy_state.retry_count += 1
        
        self.update_state(strategy_state, pipeline_id)
        return strategy_state.retry_count
