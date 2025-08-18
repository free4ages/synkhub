from typing import AsyncIterator, Dict, Any, TYPE_CHECKING
from ..base import BatchProcessor, DataBatch
from ...core.enums import SyncStrategy

if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine


class PopulateStage(BatchProcessor):
    """Stage that populates data into destination systems"""
    
    def __init__(self, sync_engine: Any, config: Dict[str, Any] = None, logger=None):
        super().__init__("populate", config, logger)
        self.sync_engine = sync_engine
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Populate data to destination"""
        metadata = batch.batch_metadata
        change_type = metadata.get("change_type")
        partition = metadata.get("partition")
        strategy = batch.context.strategy_config.type
        
        try:
            rows_processed = 0
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            
            if change_type == "deleted":
                # Handle deletion
                if partition:
                    rows_processed = await self.sync_engine.destination_provider.delete_partition_data(partition)
                    rows_deleted = rows_processed
                    batch.batch_metadata["rows_deleted"] = rows_processed
                
            elif batch.data:
                # Handle insertion/update based on strategy
                if strategy == SyncStrategy.FULL:
                    rows_processed = await self.sync_engine.destination_provider.insert_partition_data(
                        batch.data, partition, upsert=False
                    )
                    rows_inserted = len(batch.data)
                    batch.batch_metadata["rows_inserted"] = rows_inserted
                    
                elif strategy == SyncStrategy.DELTA:
                    rows_processed = await self.sync_engine.destination_provider.insert_delta_data(
                        batch.data, partition, upsert=True
                    )
                    rows_inserted = rows_processed
                    batch.batch_metadata["rows_inserted"] = rows_processed
                    
                elif strategy == SyncStrategy.HASH:
                    # Handle hash sync - check if we have row comparison results
                    if metadata.get("row_comparison_done", False):
                        # Process added, modified, and deleted rows separately
                        added_rows = metadata.get("added_rows", [])
                        modified_rows = metadata.get("modified_rows", [])
                        deleted_rows = metadata.get("deleted_rows", [])
                        
                        if added_rows:
                            await self.sync_engine.destination_provider.insert_partition_data(
                                added_rows, partition, upsert=True
                            )
                            rows_inserted = len(added_rows)
                        
                        if modified_rows:
                            await self.sync_engine.destination_provider.insert_partition_data(
                                modified_rows, partition, upsert=True
                            )
                            rows_updated = len(modified_rows)
                        
                        if deleted_rows:
                            # Note: This would require implementing row-level deletion in providers
                            # For now, we'll just count them
                            rows_deleted = len(deleted_rows)
                        
                        batch.batch_metadata["rows_inserted"] = rows_inserted
                        batch.batch_metadata["rows_updated"] = rows_updated
                        batch.batch_metadata["rows_deleted"] = rows_deleted
                    else:
                        # Simple hash sync without row comparison
                        rows_processed = await self.sync_engine.destination_provider.insert_partition_data(
                            batch.data, partition, upsert=True
                        )
                        
                        # Determine if this is insert or update based on change type
                        if change_type == "added":
                            rows_inserted = len(batch.data)
                            batch.batch_metadata["rows_inserted"] = rows_inserted
                        else:  # modified
                            rows_updated = len(batch.data)
                            batch.batch_metadata["rows_updated"] = rows_updated
            
            batch.batch_metadata["populated"] = True
            batch.batch_metadata["rows_processed"] = rows_processed
            
            # Update context statistics
            if "populate_stats" not in batch.context.stage_results:
                batch.context.stage_results["populate_stats"] = {
                    "total_rows_inserted": 0,
                    "total_rows_updated": 0,
                    "total_rows_deleted": 0
                }
            
            stats = batch.context.stage_results["populate_stats"]
            stats["total_rows_inserted"] += rows_inserted
            stats["total_rows_updated"] += rows_updated
            stats["total_rows_deleted"] += rows_deleted
            
            self.logger.debug(f"Populated batch {batch.batch_id}: "
                            f"{rows_inserted} inserted, {rows_updated} updated, {rows_deleted} deleted")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Population failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["population_error"] = str(e)
            raise
