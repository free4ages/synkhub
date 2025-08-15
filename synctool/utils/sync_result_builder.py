"""
Centralized sync result builder to standardize result formatting across the application.
"""
from datetime import datetime
from typing import Dict, Any, List, Optional
from ..core.models import SyncProgress, SyncStrategy


class SyncResultBuilder:
    """Builder class for creating standardized sync results"""
    
    @staticmethod
    def build_success_result(
        job_name: str,
        strategy: SyncStrategy,
        progress: SyncProgress,
        partitions_count: int,
        partition_results: List[Dict[str, Any]],
        start_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Build a successful sync result dictionary"""
        effective_start_time = start_time or progress.start_time
        duration = f"{(datetime.now() - effective_start_time).total_seconds()} seconds" if effective_start_time else "0 seconds"
        
        return {
            'job_name': job_name,
            'strategy': strategy.value if hasattr(strategy, 'value') else str(strategy),
            'status': 'completed',
            'total_partitions': partitions_count,
            'successful_partitions': progress.completed_partitions,
            'failed_partitions': progress.failed_partitions,
            'total_rows_detected': progress.rows_detected,
            'total_rows_fetched': progress.rows_fetched,
            'total_rows_inserted': progress.rows_inserted,
            'total_rows_updated': progress.rows_updated,
            'total_rows_deleted': progress.rows_deleted,
            'total_hash_query_count': progress.hash_query_count,
            'total_data_query_count': progress.data_query_count,
            'duration': duration,
            'partition_results': partition_results
        }
    
    @staticmethod
    def build_failure_result(
        job_name: str,
        error: str,
        progress: Optional[SyncProgress] = None,
        start_time: Optional[datetime] = None,
        strategy: Optional[SyncStrategy] = None
    ) -> Dict[str, Any]:
        """Build a failed sync result dictionary"""
        duration = "0 seconds"
        if start_time:
            duration = f"{(datetime.now() - start_time).total_seconds()} seconds"
        elif progress and progress.start_time:
            duration = f"{(datetime.now() - progress.start_time).total_seconds()} seconds"
        
        return {
            'job_name': job_name,
            'strategy': strategy.value if strategy and hasattr(strategy, 'value') else (str(strategy) if strategy else 'unknown'),
            'status': 'failed',
            'error': error,
            'total_partitions': progress.total_partitions if progress else 0,
            'successful_partitions': progress.completed_partitions if progress else 0,
            'failed_partitions': (progress.failed_partitions + 1) if progress else 1,
            'total_rows_detected': progress.rows_detected if progress else 0,
            'total_rows_fetched': progress.rows_fetched if progress else 0,
            'total_rows_inserted': progress.rows_inserted if progress else 0,
            'total_rows_updated': progress.rows_updated if progress else 0,
            'total_rows_deleted': progress.rows_deleted if progress else 0,
            'total_hash_query_count': progress.hash_query_count if progress else 0,
            'total_data_query_count': progress.data_query_count if progress else 0,
            'duration': duration,
            'partition_results': []
        }
    
    @staticmethod
    def build_partition_success_result(
        partition_id: str,
        strategy: SyncStrategy,
        rows_detected: int = 0,
        rows_fetched: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
        hash_query_count: int = 0,
        data_query_count: int = 0
    ) -> Dict[str, Any]:
        """Build a successful partition result dictionary"""
        return {
            'partition_id': partition_id,
            'strategy': strategy.value if hasattr(strategy, 'value') else str(strategy),
            'status': 'success',
            'rows_detected': rows_detected,
            'rows_fetched': rows_fetched,
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'rows_deleted': rows_deleted,
            'hash_query_count': hash_query_count,
            'data_query_count': data_query_count,
        }
    
    @staticmethod
    def build_partition_failure_result(
        partition_id: str,
        error: str,
        strategy: Optional[SyncStrategy] = None
    ) -> Dict[str, Any]:
        """Build a failed partition result dictionary"""
        return {
            'partition_id': partition_id,
            'strategy': strategy.value if strategy and hasattr(strategy, 'value') else (str(strategy) if strategy else 'unknown'),
            'status': 'failed',
            'error': error,
            'rows_detected': 0,
            'rows_fetched': 0,
            'rows_inserted': 0,
            'rows_updated': 0,
            'rows_deleted': 0,
            'hash_query_count': 0,
            'data_query_count': 0,
        }
