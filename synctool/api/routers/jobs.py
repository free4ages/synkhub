from datetime import datetime
from typing import List, Optional
from dataclasses import asdict
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pathlib import Path
from pydantic import BaseModel

from ..models.api_models import JobSummary, JobDetail, StrategyDetail, ApiResponse
from ...scheduler.file_scheduler import FileBasedScheduler
from ...monitoring.metrics_storage import MetricsStorage
from ...core.models import SchedulerConfig
from ...utils.schema_manager import SchemaManager

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

def get_scheduler():
    """Dependency to get scheduler instance"""
    from ..state import app_state
    config = app_state.get("scheduler_config")
    if not config:
        raise HTTPException(status_code=500, detail="Scheduler not initialized")
    return FileBasedScheduler(config)


def get_metrics_storage():
    """Dependency to get metrics storage instance"""
    from ..state import app_state
    metrics_storage = app_state.get("metrics_storage")
    if not metrics_storage:
        raise HTTPException(status_code=500, detail="Metrics storage not initialized")
    return metrics_storage


@router.get("/", response_model=List[JobSummary])
async def list_jobs(
    scheduler: FileBasedScheduler = Depends(get_scheduler),
    metrics: MetricsStorage = Depends(get_metrics_storage)
):
    """List all available sync jobs"""
    try:
        # Load configs if not already loaded
        await scheduler.load_configs()
        job_configs = scheduler.get_job_configs()
        
        job_summaries = []
        for job_name, job_config in job_configs.items():
            # Get recent runs for this job
            recent_runs = metrics.get_job_runs(job_name, limit=1)
            last_run = recent_runs[0] if recent_runs else None
            total_runs = len(metrics.get_job_runs(job_name))
            
            # Get strategy information
            strategies = [s.get('name', '') for s in job_config.strategies]
            enabled_strategies = [s.get('name', '') for s in job_config.strategies if s.get('enabled', True)]
            
            job_summary = JobSummary(
                name=job_name,
                description=job_config.description,
                strategies=strategies,
                enabled_strategies=enabled_strategies,
                last_run=last_run.start_time if last_run else None,
                last_status=last_run.status if last_run else None,
                total_runs=total_runs
            )
            job_summaries.append(job_summary)
        
        return job_summaries
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@router.get("/{job_name}", response_model=JobDetail)
async def get_job_detail(
    job_name: str,
    scheduler: FileBasedScheduler = Depends(get_scheduler)
):
    """Get detailed information about a specific job"""
    try:
        # Load configs if not already loaded
        await scheduler.load_configs()
        job_config = scheduler.get_job_config(job_name)
        if not job_config:
            raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")
        
        # Convert strategies to StrategyDetail models
        strategy_details = []
        for strategy_dict in job_config.strategies:
            strategy_detail = StrategyDetail(
                name=strategy_dict.get('name', ''),
                type=strategy_dict.get('type', ''),
                enabled=strategy_dict.get('enabled', True),
                column=strategy_dict.get('column', ''),
                cron=strategy_dict.get('cron'),
                sub_partition_step=strategy_dict.get('sub_partition_step', 100),
                page_size=strategy_dict.get('page_size')
            )
            strategy_details.append(strategy_detail)
        
        job_detail = JobDetail(
            name=job_config.name,
            description=job_config.description,
            partition_column=job_config.partition_column,
            partition_step=job_config.partition_step,
            max_concurrent_partitions=job_config.max_concurrent_partitions,
            strategies=strategy_details,
            source_config=asdict(job_config.source_provider),
            destination_config=asdict(job_config.destination_provider),
            column_mappings=job_config.column_map
        )
        
        return job_detail
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job detail: {str(e)}")


@router.post("/{job_name}/reload", response_model=ApiResponse)
async def reload_job_config(
    job_name: str,
    scheduler: FileBasedScheduler = Depends(get_scheduler)
):
    """Reload configuration for a specific job"""
    try:
        await scheduler.load_configs()
        job_config = scheduler.get_job_config(job_name)
        
        if not job_config:
            return ApiResponse(
                success=False,
                message=f"Job '{job_name}' not found after reload"
            )
        
        return ApiResponse(
            success=True,
            message=f"Successfully reloaded configuration for job '{job_name}'"
        )
    
    except Exception as e:
        return ApiResponse(
            success=False,
            message=f"Failed to reload job config: {str(e)}"
        )


class GenerateDDLRequest(BaseModel):
    """Request model for DDL generation"""
    apply: bool = False
    if_not_exists: bool = False


@router.post("/{job_name}/generate-ddl")
async def generate_table_ddl(
    job_name: str,
    request: GenerateDDLRequest = Body(...),
    scheduler: FileBasedScheduler = Depends(get_scheduler)
):
    """Generate DDL for populate stage destination table (auto-detects populate stage)"""
    try:
        # Load configs to get job configuration
        await scheduler.load_configs()
        job_configs = scheduler.get_job_configs()
        
        if job_name not in job_configs:
            raise HTTPException(
                status_code=404,
                detail=f"Job '{job_name}' not found"
            )
        
        job_config = job_configs[job_name]
        
        # Get datastores from app state
        from ..state import app_state
        data_storage = app_state.get("data_storage")
        if not data_storage:
            raise HTTPException(
                status_code=500,
                detail="Data storage not initialized"
            )
        
        # Find the populate stage (auto-detect)
        populate_stages = [stage for stage in job_config.stages if stage.type == 'populate']
        
        if not populate_stages:
            raise HTTPException(
                status_code=404,
                detail=f"No populate stage found in job '{job_name}'"
            )
        
        if len(populate_stages) > 1:
            stage_names = [s.name for s in populate_stages]
            raise HTTPException(
                status_code=400,
                detail=f"Multiple populate stages found in job '{job_name}': {stage_names}. Please ensure only one populate stage per pipeline."
            )
        
        stage_config = populate_stages[0]
        
        if not stage_config.destination:
            raise HTTPException(
                status_code=400,
                detail=f"No destination configured for stage '{stage_config.name}'"
            )
        
        if not stage_config.destination.datastore_name:
            raise HTTPException(
                status_code=400,
                detail=f"No datastore configured for destination in stage '{stage_config.name}'"
            )
        
        # Get datastore
        datastore = data_storage.get_datastore(stage_config.destination.datastore_name)
        if not datastore:
            raise HTTPException(
                status_code=404,
                detail=f"Datastore '{stage_config.destination.datastore_name}' not found"
            )
        
        # Connect to datastore
        await datastore.connect()
        
        try:
            # Use SchemaManager to generate DDL
            schema_manager = SchemaManager()
            result = await schema_manager.ensure_table_schema(
                datastore=datastore,
                columns=stage_config.destination.columns,
                table_name=stage_config.destination.table,
                schema_name=stage_config.destination.schema,
                apply=request.apply,
                if_not_exists=request.if_not_exists
            )
            
            return {
                'success': True,
                'job_name': job_name,
                'stage_name': stage_config.name,
                'result': {
                    'table_name': result['table_name'],
                    'action': result['action'],
                    'table_exists': result['table_exists'],
                    'changes': [
                        {
                            'type': change['type'],
                            'description': change.get('description', change['type'])
                        }
                        for change in result['changes']
                    ],
                    'ddl_statements': result['ddl_statements'],
                    'applied': result['applied']
                }
            }
        finally:
            await datastore.disconnect()
    
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate DDL: {str(e)}"
        )
