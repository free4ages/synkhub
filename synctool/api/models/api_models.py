from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel


class JobSummary(BaseModel):
    """Summary information about a sync job"""
    name: str
    description: str
    strategies: List[str]
    enabled_strategies: List[str]
    last_run: Optional[datetime] = None
    last_status: Optional[str] = None
    total_runs: int = 0


class StrategyDetail(BaseModel):
    """Strategy configuration details"""
    name: str
    type: str
    enabled: bool
    column: str
    cron: Optional[str] = None
    sub_partition_step: int
    page_size: Optional[int] = None
    use_pagination: bool = False
    use_sub_partitions: bool = True


class JobDetail(BaseModel):
    """Detailed job configuration"""
    name: str
    description: str
    partition_key: str
    partition_step: int
    max_concurrent_partitions: int
    strategies: List[StrategyDetail]
    source_config: Dict[str, Any]
    destination_config: Dict[str, Any]
    column_mappings: List[Dict[str, Any]]


class RunSummary(BaseModel):
    """Summary of a job run"""
    run_id: str
    job_name: str
    strategy_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str
    duration_seconds: Optional[float] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    partition_count: int = 0
    successful_partitions: int = 0
    failed_partitions: int = 0


class RunDetail(BaseModel):
    """Detailed information about a job run"""
    run_id: str
    job_name: str
    strategy_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str
    duration_seconds: Optional[float] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    error_message: Optional[str] = None
    partition_count: int = 0
    successful_partitions: int = 0
    failed_partitions: int = 0


class LogEntry(BaseModel):
    """Log entry for a job run"""
    timestamp: datetime
    level: str
    message: str
    run_id: str


class ApiResponse(BaseModel):
    """Generic API response wrapper"""
    success: bool
    message: str
    data: Optional[Any] = None
