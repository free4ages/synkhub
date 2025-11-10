"""
Centralized progress manager to handle progress updates and metrics collection.
"""
from typing import Optional, Callable, TYPE_CHECKING, List
import logging

from ..core.models import SyncProgress

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector


class ProgressManager:
    """Manages progress updates and coordinates with metrics collection"""
    
    def __init__(self, 
                 progress: SyncProgress,
                 metrics_collector: Optional['MetricsCollector'] = None,
                 progress_callback: Optional[Callable[[SyncProgress], None]] = None,
                 logger: Optional[logging.Logger] = None):
        self.progress = progress
        self.metrics_collector = metrics_collector
        self.progress_callback = progress_callback
        self.logger = logger or logging.getLogger(__name__)
        self.failed_partitions = set()
    
    def update_progress(self,
                       completed_partitions: int = 0,
                       failed_partitions: int = 0,
                       skipped_partitions: int = 0,
                       processed_partitions: int = 0,
                       total_partitions: int = 0,
                       rows_detected: int = 0,
                       rows_fetched: int = 0,
                       rows_inserted: int = 0,
                       rows_updated: int = 0,
                       rows_deleted: int = 0,
                       rows_failed: int = 0,
                       total_primary_partitions: int = 0,
                       detection_query_count: int = 0,
                       hash_query_count: int = 0,
                       data_query_count: int = 0
                      ) -> None:
        """Update progress and sync with metrics collector and callback"""
        
        # Update the progress object
        self.progress.update_progress(
            completed_partitions=completed_partitions,
            failed_partitions=failed_partitions,
            skipped_partitions=skipped_partitions,
            processed_partitions=processed_partitions,
            total_partitions=total_partitions,
            total_primary_partitions=total_primary_partitions,
            rows_detected=rows_detected,
            rows_fetched=rows_fetched,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            rows_failed=rows_failed,
            detection_query_count=detection_query_count,
            hash_query_count=hash_query_count,
            data_query_count=data_query_count
        )
        
        # Update metrics collector if available
        if self.metrics_collector:
            self.metrics_collector.update_progress(
                rows_detected=self.progress.rows_detected,
                rows_fetched=self.progress.rows_fetched,
                rows_inserted=self.progress.rows_inserted,
                rows_updated=self.progress.rows_updated,
                rows_deleted=self.progress.rows_deleted,
                rows_failed=self.progress.rows_failed,
                partition_count=self.progress.total_partitions,
                skipped_partitions=self.progress.skipped_partitions,
                failed_partitions=self.progress.failed_partitions,
                processed_partitions=self.progress.processed_partitions,
                total_primary_partitions=self.progress.total_primary_partitions,
                detection_query_count=self.progress.detection_query_count,
                hash_query_count=self.progress.hash_query_count,
                data_query_count=self.progress.data_query_count
            )
        
        # Call progress callback if provided
        if self.progress_callback:
            self.progress_callback(self.progress)
    
    def mark_failed_partitions(self, partition_ids: List[str]) -> None:
        for partition_id in partition_ids:
            if partition_id not in self.failed_partitions:
                self.failed_partitions.add(partition_id)
                self.update_progress(failed_partitions=1)
    
    # def update_from_partition_result(self, partition_result: dict, completed: bool = True, failed: bool = False) -> None:
    #     """Update progress from a partition result dictionary"""
    #     self.update_progress(
    #         completed=completed,
    #         failed=failed,
    #         rows_detected=partition_result.get('rows_detected', 0),
    #         rows_fetched=partition_result.get('rows_fetched', 0),
    #         rows_inserted=partition_result.get('rows_inserted', 0),
    #         rows_updated=partition_result.get('rows_updated', 0),
    #         rows_deleted=partition_result.get('rows_deleted', 0),
    #         hash_query_count=partition_result.get('hash_query_count', 0),
    #         data_query_count=partition_result.get('data_query_count', 0)
    #     )
    
    def finalize_metrics(self, partitions_count: int) -> None:
        """Final metrics update at the end of sync job"""
        if self.metrics_collector:
            self.metrics_collector.update_progress(
                rows_detected=self.progress.rows_detected,
                rows_fetched=self.progress.rows_fetched,
                rows_inserted=self.progress.rows_inserted,
                rows_updated=self.progress.rows_updated,
                rows_deleted=self.progress.rows_deleted,
                partition_count=partitions_count,
                successful_partitions=self.progress.completed_partitions,
                failed_partitions=self.progress.failed_partitions
            )
    
    def set_total_partitions(self, count: int) -> None:
        """Set total partitions count"""
        self.progress.total_partitions = count
        
    def log_completion(self, duration: str) -> None:
        """Log sync job completion"""
        if self.logger:
            self.logger.info(f"Sync job completed: {duration}")
