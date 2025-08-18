from typing import AsyncIterator, Dict, Any, TYPE_CHECKING
from ..base import BatchProcessor, DataBatch

if TYPE_CHECKING:
    from ...sync.sync_engine import SyncEngine


class EnrichStage(BatchProcessor):
    """Stage that enriches data using the enrichment engine"""
    
    def __init__(self, sync_engine: Any, config: Dict[str, Any] = None, logger=None):
        super().__init__("enrich", config, logger)
        self.sync_engine = sync_engine
    
    def should_process(self, context) -> bool:
        """Only process if enrichment engine is available"""
        return (super().should_process(context) and 
                self.sync_engine.enrichment_engine is not None)
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Enrich the batch data"""
        if not batch.data:
            return batch
        
        try:
            enriched_data = await self.sync_engine.enrichment_engine.enrich(batch.data)
            batch.data = enriched_data
            batch.batch_metadata["enriched"] = True
            batch.batch_metadata["enriched_rows"] = len(enriched_data)
            
            self.logger.debug(f"Enriched {len(enriched_data)} rows in batch {batch.batch_id}")
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Enrichment failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["enrichment_error"] = str(e)
            raise
