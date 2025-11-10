import uuid
from datetime import datetime
from typing import Optional
import logging

from ..core.models import JobRunMetrics
from .metrics_storage import MetricsStorage


class MetricsCollector:
    """Collects and manages metrics during sync job execution"""
    
    def __init__(self, metrics_storage: MetricsStorage):
        self.storage = metrics_storage
        self.current_metrics: Optional[JobRunMetrics] = None
        self.logger = logging.getLogger(__name__)
    
    def start_job_run(self, job_name: str, strategy_name: str, run_id: Optional[str] = None) -> str:
        """Start tracking a new job run"""
        # Use provided run_id or generate new one
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        self.current_metrics = JobRunMetrics(
            job_name=job_name,
            strategy_name=strategy_name,
            run_id=run_id,
            start_time=datetime.now(),
            status="running"
        )
        
        # DON'T save initial metrics - only save at finish
        # self.storage.save_metrics(self.current_metrics)  # COMMENTED OUT
        self.logger.info(f"Started tracking metrics for job {job_name}, run {run_id}")
        
        return run_id
    
    def update_progress(self, **kwargs) -> None:
        """Update progress metrics"""
        if not self.current_metrics:
            return
        
        for key, value in kwargs.items():
            if hasattr(self.current_metrics, key):
                setattr(self.current_metrics, key, value)
        
        # DON'T save on every update - only save at finish
        # self.storage.save_metrics(self.current_metrics)  # COMMENTED OUT
    
    def finish_job_run(self, status: str = "completed", error_message: Optional[str] = None) -> None:
        """Finish tracking the current job run"""
        if not self.current_metrics:
            return
        
        self.current_metrics.end_time = datetime.now()
        self.current_metrics.status = status
        if error_message:
            self.current_metrics.error_message = error_message
        
        # ONLY save final metrics ONCE
        self.storage.save_metrics(self.current_metrics)
        self.logger.info(f"Finished tracking metrics for run {self.current_metrics.run_id}")
        
        self.current_metrics = None
    
    def get_current_run_id(self) -> Optional[str]:
        """Get the current run ID if a job is running"""
        return self.current_metrics.run_id if self.current_metrics else None
