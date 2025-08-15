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
    
    def __init__(self, metrics_dir: str = "./data/metrics", max_runs_per_job: int = 50):
        self.metrics_dir = Path(metrics_dir)
        self.max_runs_per_job = max_runs_per_job
        self.logger = logging.getLogger(__name__)
        
        # Create metrics directory if it doesn't exist
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
    
    def save_metrics(self, metrics: JobRunMetrics) -> None:
        """Save job run metrics to disk"""
        job_dir = self.metrics_dir / metrics.job_name
        job_dir.mkdir(exist_ok=True)
        
        # Save metrics as JSON
        metrics_file = job_dir / f"{metrics.run_id}.json"
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
        
        # Clean up old runs if needed
        self._cleanup_old_runs(metrics.job_name)
    
    def get_job_runs(self, job_name: str, limit: Optional[int] = None) -> List[JobRunMetrics]:
        """Get all runs for a specific job, sorted by start time (newest first)"""
        job_dir = self.metrics_dir / job_name
        if not job_dir.exists():
            return []
        
        runs = []
        for metrics_file in job_dir.glob("*.json"):
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
        
        # Sort by start time (newest first)
        runs.sort(key=lambda x: x.start_time, reverse=True)
        
        if limit:
            runs = runs[:limit]
        
        return runs
    
    def get_run_metrics(self, job_name: str, run_id: str) -> Optional[JobRunMetrics]:
        """Get metrics for a specific run"""
        metrics_file = self.metrics_dir / job_name / f"{run_id}.json"
        if not metrics_file.exists():
            return None
        
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
            self.logger.error(f"Failed to load metrics for run {run_id}: {e}")
            return None
    
    def get_all_jobs(self) -> List[str]:
        """Get list of all job names that have metrics"""
        if not self.metrics_dir.exists():
            return []
        
        jobs = []
        for job_dir in self.metrics_dir.iterdir():
            if job_dir.is_dir() and any(job_dir.glob("*.json")):
                jobs.append(job_dir.name)
        
        return sorted(jobs)
    
    def _cleanup_old_runs(self, job_name: str) -> None:
        """Remove old runs to keep only the latest max_runs_per_job"""
        job_dir = self.metrics_dir / job_name
        if not job_dir.exists():
            return
        
        # Get all run files sorted by modification time (newest first)
        run_files = sorted(
            job_dir.glob("*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        # Remove excess files
        if len(run_files) > self.max_runs_per_job:
            for old_file in run_files[self.max_runs_per_job:]:
                try:
                    old_file.unlink()
                    self.logger.debug(f"Removed old metrics file: {old_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove old metrics file {old_file}: {e}")
