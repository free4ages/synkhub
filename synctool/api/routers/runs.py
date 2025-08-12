from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pathlib import Path
import logging

from ..models.api_models import RunSummary, RunDetail, LogEntry, ApiResponse
from ...monitoring.metrics_storage import MetricsStorage
from ...core.models import SchedulerConfig

router = APIRouter(prefix="/api/runs", tags=["runs"])


def get_metrics_storage():
    """Dependency to get metrics storage instance"""
    from ..state import app_state
    metrics_storage = app_state.get("metrics_storage")
    if not metrics_storage:
        raise HTTPException(status_code=500, detail="Metrics storage not initialized")
    return metrics_storage


@router.get("/{job_name}", response_model=List[RunSummary])
async def list_job_runs(
    job_name: str,
    limit: int = Query(50, description="Maximum number of runs to return"),
    offset: int = Query(0, description="Number of runs to skip"),
    status: Optional[str] = Query(None, description="Filter by status"),
    metrics: MetricsStorage = Depends(get_metrics_storage)
):
    """List all runs for a specific job"""
    try:
        # Get all runs for the job
        all_runs = metrics.get_job_runs(job_name)
        
        # Filter by status if provided
        if status:
            all_runs = [run for run in all_runs if run.status == status]
        
        # Apply pagination
        paginated_runs = all_runs[offset:offset + limit]
        
        # Convert to RunSummary models
        run_summaries = []
        for run in paginated_runs:
            run_summary = RunSummary(
                run_id=run.run_id,
                job_name=run.job_name,
                strategy_name=run.strategy_name,
                start_time=run.start_time,
                end_time=run.end_time,
                status=run.status,
                duration_seconds=run.duration_seconds,
                rows_processed=run.rows_processed,
                rows_inserted=run.rows_inserted,
                rows_updated=run.rows_updated,
                rows_deleted=run.rows_deleted,
                partition_count=run.partition_count,
                successful_partitions=run.successful_partitions,
                failed_partitions=run.failed_partitions
            )
            run_summaries.append(run_summary)
        
        return run_summaries
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list runs: {str(e)}")


@router.get("/{job_name}/{run_id}", response_model=RunDetail)
async def get_run_detail(
    job_name: str,
    run_id: str,
    metrics: MetricsStorage = Depends(get_metrics_storage)
):
    """Get detailed information about a specific run"""
    try:
        run_metrics = metrics.get_run_metrics(job_name, run_id)
        if not run_metrics:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found for job '{job_name}'")
        
        run_detail = RunDetail(
            run_id=run_metrics.run_id,
            job_name=run_metrics.job_name,
            strategy_name=run_metrics.strategy_name,
            start_time=run_metrics.start_time,
            end_time=run_metrics.end_time,
            status=run_metrics.status,
            duration_seconds=run_metrics.duration_seconds,
            rows_processed=run_metrics.rows_processed,
            rows_inserted=run_metrics.rows_inserted,
            rows_updated=run_metrics.rows_updated,
            rows_deleted=run_metrics.rows_deleted,
            error_message=run_metrics.error_message,
            partition_count=run_metrics.partition_count,
            successful_partitions=run_metrics.successful_partitions,
            failed_partitions=run_metrics.failed_partitions
        )
        
        return run_detail
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get run detail: {str(e)}")


@router.get("/{job_name}/{run_id}/logs", response_model=List[LogEntry])
async def get_run_logs(
    job_name: str,
    run_id: str,
    limit: int = Query(1000, description="Maximum number of log entries to return"),
    level: Optional[str] = Query(None, description="Filter by log level"),
):
    """Get logs for a specific run"""
    try:
        # For now, return mock logs since we haven't implemented log storage yet
        # In a real implementation, this would read from log files or a log storage system
        
        mock_logs = [
            LogEntry(
                timestamp=datetime.now(),
                level="INFO",
                message=f"Started sync job: {job_name} with run_id: {run_id}",
                run_id=run_id
            ),
            LogEntry(
                timestamp=datetime.now(),
                level="INFO",
                message="Initializing providers and components",
                run_id=run_id
            ),
            LogEntry(
                timestamp=datetime.now(),
                level="DEBUG",
                message="Processing partitions with controlled concurrency",
                run_id=run_id
            ),
            LogEntry(
                timestamp=datetime.now(),
                level="INFO",
                message="Sync job completed successfully",
                run_id=run_id
            )
        ]
        
        # Filter by level if provided
        if level:
            mock_logs = [log for log in mock_logs if log.level == level.upper()]
        
        # Apply limit
        return mock_logs[:limit]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get logs: {str(e)}")


@router.get("/", response_model=List[RunSummary])
async def list_all_runs(
    limit: int = Query(100, description="Maximum number of runs to return"),
    offset: int = Query(0, description="Number of runs to skip"),
    status: Optional[str] = Query(None, description="Filter by status"),
    metrics: MetricsStorage = Depends(get_metrics_storage)
):
    """List runs from all jobs"""
    try:
        # Get all job names
        job_names = metrics.get_all_jobs()
        
        # Collect runs from all jobs
        all_runs = []
        for job_name in job_names:
            job_runs = metrics.get_job_runs(job_name)
            all_runs.extend(job_runs)
        
        # Sort by start time (newest first)
        all_runs.sort(key=lambda x: x.start_time, reverse=True)
        
        # Filter by status if provided
        if status:
            all_runs = [run for run in all_runs if run.status == status]
        
        # Apply pagination
        paginated_runs = all_runs[offset:offset + limit]
        
        # Convert to RunSummary models
        run_summaries = []
        for run in paginated_runs:
            run_summary = RunSummary(
                run_id=run.run_id,
                job_name=run.job_name,
                strategy_name=run.strategy_name,
                start_time=run.start_time,
                end_time=run.end_time,
                status=run.status,
                duration_seconds=run.duration_seconds,
                rows_processed=run.rows_processed,
                rows_inserted=run.rows_inserted,
                rows_updated=run.rows_updated,
                rows_deleted=run.rows_deleted,
                partition_count=run.partition_count,
                successful_partitions=run.successful_partitions,
                failed_partitions=run.failed_partitions
            )
            run_summaries.append(run_summary)
        
        return run_summaries
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list all runs: {str(e)}")
