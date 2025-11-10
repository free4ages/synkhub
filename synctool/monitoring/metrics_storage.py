import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging

from ..core.models import JobRunMetrics


class MetricsStorage:
    """File-based storage for job run metrics"""
    
    def __init__(self, metrics_dir: str = "./data/metrics", max_runs_per_strategy: int = 50):
        self.metrics_dir = Path(metrics_dir)
        self.max_runs_per_strategy = max_runs_per_strategy
        self.logger = logging.getLogger(__name__)
        
        # Create metrics directory if it doesn't exist
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
    
    def _strategy_dir(self, job_name: str, strategy_name: str) -> Path:
        """Get directory for a specific strategy within a job"""
        strategy_dir = self.metrics_dir / job_name / strategy_name
        strategy_dir.mkdir(parents=True, exist_ok=True)
        return strategy_dir
    
    def save_metrics(self, metrics: JobRunMetrics) -> None:
        """Save job run metrics to disk"""
        strategy_name = metrics.strategy_name or "default"
        strategy_dir = self._strategy_dir(metrics.job_name, strategy_name)
        
        # Save metrics as JSON
        metrics_file = strategy_dir / f"{metrics.run_id}.json"
        metrics_data = {
            "job_name": metrics.job_name,
            "strategy_name": metrics.strategy_name,
            "run_id": metrics.run_id,
            "start_time": metrics.start_time.isoformat(),
            "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
            "status": metrics.status,
            "rows_fetched": metrics.rows_fetched,
            "rows_detected": metrics.rows_detected,
            "rows_inserted": metrics.rows_inserted,
            "rows_updated": metrics.rows_updated,
            "rows_deleted": metrics.rows_deleted,
            "error_message": metrics.error_message,
            "partition_count": metrics.partition_count,
            "successful_partitions": metrics.successful_partitions,
            "failed_partitions": metrics.failed_partitions,
            "duration_seconds": metrics.duration_seconds
        }
        
        with open(metrics_file, 'w') as f:
            json.dump(metrics_data, f, indent=2)
        
        # Clean up old runs for this strategy
        self._cleanup_old_runs(metrics.job_name, strategy_name)
    
    def get_job_runs(self, job_name: str, strategy_name: Optional[str] = None, limit: Optional[int] = None) -> List[JobRunMetrics]:
        """
        Get runs for a specific job.
        
        Args:
            job_name: Name of the job/pipeline
            strategy_name: Specific strategy name, or None to get all strategies
            limit: Maximum number of runs to return (per strategy if strategy_name is None)
        
        Returns:
            List of JobRunMetrics sorted by start time (newest first)
        """
        job_dir = self.metrics_dir / job_name
        if not job_dir.exists():
            return []
        
        runs = []
        
        if strategy_name:
            # Get runs for specific strategy
            strategy_dir = job_dir / strategy_name
            if strategy_dir.exists():
                runs.extend(self._load_strategy_runs(strategy_dir))
        else:
            # Get runs for all strategies
            for strategy_dir in job_dir.iterdir():
                if strategy_dir.is_dir():
                    runs.extend(self._load_strategy_runs(strategy_dir))
        
        # Sort by start time (newest first)
        runs.sort(key=lambda x: x.start_time, reverse=True)
        
        if limit:
            runs = runs[:limit]
        
        return runs
    
    def _load_strategy_runs(self, strategy_dir: Path) -> List[JobRunMetrics]:
        """Load all runs from a strategy directory"""
        runs = []
        for metrics_file in strategy_dir.glob("*.json"):
            try:
                with open(metrics_file, 'r') as f:
                    data = json.load(f)
                
                metrics = JobRunMetrics(
                    job_name=data["job_name"],
                    strategy_name=data["strategy_name"],
                    run_id=data["run_id"],
                    start_time=datetime.fromisoformat(data["start_time"]),
                    end_time=datetime.fromisoformat(data["end_time"]) if data["end_time"] else None,
                    status=data["status"],
                    rows_fetched=data.get("rows_fetched", 0),
                    rows_detected=data.get("rows_detected", 0),
                    rows_inserted=data["rows_inserted"],
                    rows_updated=data["rows_updated"],
                    rows_deleted=data["rows_deleted"],
                    error_message=data["error_message"],
                    partition_count=data["partition_count"],
                    successful_partitions=data["successful_partitions"],
                    failed_partitions=data["failed_partitions"]
                )
                runs.append(metrics)
            except Exception as e:
                self.logger.warning(f"Failed to load metrics from {metrics_file}: {e}")
        
        return runs
    
    def get_run_metrics(self, job_name: str, run_id: str, strategy_name: Optional[str] = None) -> Optional[JobRunMetrics]:
        """
        Get metrics for a specific run.
        
        Args:
            job_name: Name of the job/pipeline
            run_id: Run ID to fetch
            strategy_name: Strategy name (if None, searches all strategies)
        """
        job_dir = self.metrics_dir / job_name
        if not job_dir.exists():
            return None
        
        # If strategy_name provided, search directly
        if strategy_name:
            metrics_file = job_dir / strategy_name / f"{run_id}.json"
            if metrics_file.exists():
                return self._load_metrics_file(metrics_file)
            return None
        
        # Otherwise search all strategies
        for strategy_dir in job_dir.iterdir():
            if strategy_dir.is_dir():
                metrics_file = strategy_dir / f"{run_id}.json"
                if metrics_file.exists():
                    return self._load_metrics_file(metrics_file)
        
        return None
    
    def _load_metrics_file(self, metrics_file: Path) -> Optional[JobRunMetrics]:
        """Load a single metrics file"""
        try:
            with open(metrics_file, 'r') as f:
                data = json.load(f)
            
            return JobRunMetrics(
                job_name=data["job_name"],
                strategy_name=data["strategy_name"],
                run_id=data["run_id"],
                start_time=datetime.fromisoformat(data["start_time"]),
                end_time=datetime.fromisoformat(data["end_time"]) if data["end_time"] else None,
                status=data["status"],
                rows_fetched=data.get("rows_fetched", 0),
                rows_detected=data.get("rows_detected", 0),
                rows_inserted=data["rows_inserted"],
                rows_updated=data["rows_updated"],
                rows_deleted=data["rows_deleted"],
                error_message=data["error_message"],
                partition_count=data["partition_count"],
                successful_partitions=data["successful_partitions"],
                failed_partitions=data["failed_partitions"]
            )
        except Exception as e:
            self.logger.error(f"Failed to load metrics from {metrics_file}: {e}")
            return None
    
    def get_all_jobs(self) -> List[str]:
        """Get list of all job names that have metrics"""
        if not self.metrics_dir.exists():
            return []
        
        jobs = []
        for job_dir in self.metrics_dir.iterdir():
            if job_dir.is_dir():
                # Check if any strategy subdirectory has metrics
                has_metrics = any(
                    strategy_dir.is_dir() and any(strategy_dir.glob("*.json"))
                    for strategy_dir in job_dir.iterdir()
                )
                if has_metrics:
                    jobs.append(job_dir.name)
        
        return sorted(jobs)
    
    def _cleanup_old_runs(self, job_name: str, strategy_name: str) -> None:
        """Remove old runs to keep only the latest max_runs_per_strategy for this strategy"""
        strategy_dir = self._strategy_dir(job_name, strategy_name)
        
        # Get all run files sorted by modification time (newest first)
        run_files = sorted(
            strategy_dir.glob("*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        # Remove excess files
        if len(run_files) > self.max_runs_per_strategy:
            for old_file in run_files[self.max_runs_per_strategy:]:
                try:
                    old_file.unlink()
                    self.logger.debug(f"Removed old metrics file: {old_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove old metrics file {old_file}: {e}")
