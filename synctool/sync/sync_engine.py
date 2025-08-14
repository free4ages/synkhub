import asyncio
import concurrent.futures
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector

from ..core.models import SyncJobConfig, StrategyConfig, SyncProgress, Partition
from ..core.enums import HashAlgo, SyncStrategy, BackendType
from ..core.provider import Provider
from ..core.column_mapper import ColumnMapper
from ..utils.partition_generator import PartitionConfig, PartitionGenerator, add_exclusive_range
from ..utils.hash_calculator import HashCalculator
from ..enrichment.enrichment_engine import EnrichmentEngine
from .partition_processor import PartitionProcessor
from ..core.schema_models import UniversalDataType

class SyncEngine:
    """Main sync engine that orchestrates the sync process"""
    
    def __init__(self, config: SyncJobConfig, metrics_collector: Optional['MetricsCollector'] = None, logger= None):
        self.config = config
        self.source_provider: Provider = None
        self.destination_provider: Provider = None
        self.strategies: Dict[str, StrategyConfig] = {}
        self.enrichment_engine: Optional[EnrichmentEngine] = None
        self.hash_calculator = HashCalculator()
        self.progress = SyncProgress()
        logger_name = f"{logger.name}.engine" if logger else f"{__name__}.engine"
        self.logger = logging.getLogger(logger_name)
        self.hash_algo = config.hash_algo or HashAlgo.HASH_MD5_HASH
        self.column_mapper: ColumnMapper = None
        self.metrics_collector = metrics_collector
        
        # Thread pool for partition processing
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_concurrent_partitions,
            thread_name_prefix=f"sync-{config.name}"
        )
    
    
    async def initialize(self):
        """Initialize all providers and components"""
        # Create ColumnMapper if column_map is present in config
        # Parse strategies
        for strategy_config in self.config.strategies:
            if not strategy_config.get('enabled', True):
                continue
            if strategy_config.get('type') in (SyncStrategy.FULL, SyncStrategy.HASH) and strategy_config.get('column')!=self.config.partition_key:
                self.logger.warning(f"Column {strategy_config.get('column')} is not the same as partition key {self.config.partition_key}. It will be ignored")
                strategy_config['column'] = self.config.partition_key
            strategy = StrategyConfig(**strategy_config)
            self.strategies[strategy.name] = strategy
        
                # Initialize enrichment engine
        if self.config.enrichment:
            from ..core.models import EnrichmentConfig
            enrichment_config = EnrichmentConfig(**self.config.enrichment)
            self.enrichment_engine = EnrichmentEngine(enrichment_config)
            await self.enrichment_engine.connect()

        column_mapper: ColumnMapper = ColumnMapper(self.config.column_map)
        column_mapper.build_schemas(list(self.strategies.values()), self.enrichment_engine)
        column_mapper.validate()
        self.column_mapper = column_mapper

        #populate column_type for strategies
        for strategy in self.strategies.values():
            strategy.column_type = self.column_mapper.column(strategy.column).dtype
        
        # Create providers with appropriate ColumnSchema for both data and state
        self.source_provider = await self._create_provider(
            self.config.source_provider, 
            data_column_schema=column_mapper.schemas.get("source"),
            state_column_schema=column_mapper.schemas.get("source_state"),
            role="source"
        )
        self.destination_provider = await self._create_provider(
            self.config.destination_provider, 
            data_column_schema=column_mapper.schemas.get("destination"),
            state_column_schema=column_mapper.schemas.get("destination_state"),
            role="destination"
        )
        

        

        
        # Initialize data comparator
        # if self.source_provider.column_mapping.unique_key:
        #     self.data_comparator = DataComparator(self.source_provider.column_mapping.unique_key)
    
    async def sync(self, strategy_name: Optional[str] = None, start: Any = None, end: Any = None,
                   progress_callback: Optional[Callable[[SyncProgress], None]] = None) -> Dict[str, Any]:
        """Main sync method with progress tracking"""
        try:
            await self.initialize()
            self.progress.start_time = datetime.now()
            self.logger.info(f"Starting sync job: {self.config.name}")
            
            # Determine sync strategy
            sync_strategy, strategy_config = await self._determine_strategy(strategy_name, start, end)
            self.logger.info(f"Using sync strategy: {sync_strategy.value}")
            
            # Update metrics if collector is available
            if self.metrics_collector:
                self.metrics_collector.update_progress(
                    rows_processed=0,
                    partition_count=0,
                    successful_partitions=0,
                    failed_partitions=0
                )
            
            # Get sync bounds
            sync_start, sync_end = await self._get_sync_bounds(sync_strategy, strategy_config, start, end)
            if sync_start == sync_end:
                self.logger.info(f"Sync bounds are the same, no partitions to process")
                return {
                    'job_name': self.config.name,
                    'status': 'completed',
                    'total_partitions': 0,
                    'successful_partitions': 0,
                    'failed_partitions': 0,
                    'total_rows_detected': 0,
                    'total_rows_inserted': 0,
                    'total_rows_updated': 0,
                    'total_rows_deleted': 0,
                    'total_hash_query_count': 0,
                    'total_data_query_count': 0,
                    'duration': f"{(datetime.now() - self.progress.start_time).total_seconds()} seconds",
                    'partition_results': []
                }
            sync_end = add_exclusive_range(sync_end)
            self.logger.info(f"Sync bounds: {sync_start} to {sync_end}")
            
            # Generate partitions
            partitions = []
            if strategy_config and sync_start is not None and sync_end is not None:
                partition_generator: PartitionGenerator = PartitionGenerator(PartitionConfig(
                    name="main_partition_{pid}",
                    column=strategy_config.column,
                    column_type=self.column_mapper.column(strategy_config.column).dtype,
                    partition_step=self.config.partition_step
                ))
                partitions: list[Partition] = await partition_generator.generate_partitions(sync_start, sync_end)
            else:
                raise ValueError("Sync bounds are not set")

            
            self.progress.total_partitions = len(partitions)
            self.logger.info(f"Processing {len(partitions)} partitions")
            
            # Process partitions with controlled concurrency
            results: list[dict[str, Any]] = await self._process_partitions_concurrent(
                partitions, sync_strategy, strategy_config, progress_callback
            )
 
            
            sync_result = {
                'job_name': self.config.name,
                'strategy': sync_strategy.value,
                'total_partitions': len(partitions),
                'successful_partitions': self.progress.completed_partitions,
                'failed_partitions': self.progress.failed_partitions,
                'total_rows_detected': self.progress.rows_detected,
                'total_rows_inserted': self.progress.rows_inserted,
                'total_rows_updated': self.progress.rows_updated,
                'total_rows_deleted': self.progress.rows_deleted,
                'total_hash_query_count': self.progress.hash_query_count,
                'total_data_query_count': self.progress.data_query_count,
                'duration': f"{(datetime.now() - self.progress.start_time).total_seconds()} seconds",
                'partition_results': results
            }
            
            # Update final metrics
            if self.metrics_collector:
                self.metrics_collector.update_progress(
                    rows_processed=self.progress.rows_detected,
                    rows_inserted=self.progress.rows_inserted,
                    rows_updated=self.progress.rows_updated,
                    rows_deleted=self.progress.rows_deleted,
                    partition_count=len(partitions),
                    successful_partitions=self.progress.completed_partitions,
                    failed_partitions=self.progress.failed_partitions
                )
            
            self.logger.info(f"Sync job completed: {sync_result['duration']}")
            return sync_result
            
        except Exception as e:
            raise e
            self.logger.error(f"Sync job failed: {str(e)}")
            # Return error result instead of raising
            return {
                'job_name': self.config.name,
                'status': 'failed',
                'error': str(e),
                'total_partitions': 0,
                'successful_partitions': 0,
                'failed_partitions': 1,
                'total_rows_processed': 0,
                'total_rows_inserted': 0,
                'total_rows_updated': 0,
                'duration': f"{(datetime.now() - self.progress.start_time).total_seconds()} seconds" if hasattr(self.progress, 'start_time') else None,
                'partition_results': []
            }
        finally:
            await self.cleanup()
    
    async def _process_partitions_concurrent(self, partitions: List[Partition], 
                                           strategy: SyncStrategy,
                                           strategy_config: Optional[StrategyConfig],
                                           progress_callback: Optional[Callable[[SyncProgress], None]] = None) -> List[Dict[str, Any]]:
        """Process partitions with controlled concurrency"""
        results: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(self.config.max_concurrent_partitions)
        
        if self.config.max_concurrent_partitions == 1:
         
            for partition in partitions:
                try:
                    processor: PartitionProcessor = PartitionProcessor(self, partition, strategy, strategy_config)
                    result: dict[str, Any] = await processor.process()
                    results.append(result)
                    # Update progress to mirror concurrent path
                    self.progress.update_progress(
                        rows_detected=result.get('rows_detected', 0),
                        rows_inserted=result.get('rows_inserted', 0),
                        rows_updated=result.get('rows_updated', 0),
                        rows_deleted=result.get('rows_deleted', 0),
                        hash_query_count=result.get('hash_query_count', 0),
                        data_query_count=result.get('data_query_count', 0),
                        completed=True
                    )
                    if progress_callback:
                        progress_callback(self.progress)
                except Exception as e:
                    raise e
                    self.logger.error(f"Partition {partition.partition_id} failed: {str(e)}")
                    self.progress.update_progress(failed=True)
                    if progress_callback:
                        progress_callback(self.progress)
                    results.append({
                        'partition_id': partition.partition_id,
                        'status': 'failed',
                        'error': str(e),
                        'rows_processed': 0,
                        'rows_inserted': 0,
                        'rows_updated': 0
                    })
            return results
        
        async def process_partition_with_semaphore(partition: Partition) -> Dict[str, Any]:
            async with semaphore:
                try:
                    processor = PartitionProcessor(self, partition, strategy, strategy_config)
                    result = await processor.process()
                    
                    # Update progress
                    self.progress.update_progress(
                        rows_detected=result.get('rows_detected', 0),
                        rows_inserted=result.get('rows_inserted', 0),
                        rows_updated=result.get('rows_updated', 0),
                        rows_deleted=result.get('rows_deleted', 0),
                        hash_query_count=result.get('hash_query_count', 0),
                        data_query_count=result.get('data_query_count', 0),
                        completed=True
                    )
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(self.progress)
                    
                    return result
                    
                except Exception as e:
                    self.logger.error(f"Partition {partition.partition_id} failed: {str(e)}\nStacktrace:\n{traceback.format_exc()}")
                    self.progress.update_progress(failed=True)
                    
                    if progress_callback:
                        progress_callback(self.progress)
                    
                    return {
                        'partition_id': partition.partition_id,
                        'status': 'failed',
                        'error': str(e),
                        'rows_processed': 0,
                        'rows_inserted': 0,
                        'rows_updated': 0
                    }
        
        # Process partitions in batches
        batch_size = self.config.max_concurrent_partitions*2
        for i in range(0, len(partitions), batch_size):
            batch = partitions[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} partitions")
            
            tasks = [process_partition_with_semaphore(partition) for partition in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    self.logger.error(f"Batch processing error: {str(result)}")
                    results.append({
                        'status': 'failed',
                        'error': str(result),
                        'rows_detected': 0,
                        'rows_inserted': 0,
                        'rows_updated': 0,
                        'rows_deleted': 0,
                        'hash_query_count': 0,
                        'data_query_count': 0,
                    })
                else:
                    results.append(result)
        
        return results
    
    async def _determine_strategy(self, strategy_name: Optional[str], start: Any, end: Any) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
        """Determine which sync strategy to use"""
        if not await self.destination_provider.has_data():
            # Find full strategy
            full_strategy = next((s for s in self.strategies.values() if s.type == SyncStrategy.FULL), None)
            return SyncStrategy.FULL, full_strategy
        elif strategy_name == "full":
            hash_strategy = next((s for s in self.strategies.values() if s.type == SyncStrategy.HASH), None)
            return SyncStrategy.HASH, hash_strategy

        
        if strategy_name:
            if strategy_name in self.strategies:
                strategy_config = self.strategies[strategy_name]
                if strategy_config.type == "delta":
                    return SyncStrategy.DELTA, strategy_config
                elif strategy_config.type == "hash":
                    return SyncStrategy.HASH, strategy_config
                else:
                    return SyncStrategy.FULL, strategy_config
        
        # Default strategy selection logic
        if start and end:
            hash_strategy = next((s for s in self.strategies.values() if s.name == "hash"), None)
            return SyncStrategy.HASH, hash_strategy
        else:
            delta_strategy = next((s for s in self.strategies.values() if s.name == "delta"), None)
            return SyncStrategy.DELTA, delta_strategy
    
    async def _get_sync_bounds(self, strategy: SyncStrategy, strategy_config: Optional[StrategyConfig], 
                              start: Any, end: Any) -> Tuple[Any, Any]:
        """Get start and end bounds for sync"""
        if start and end:
            return start, end
        
        if strategy == SyncStrategy.DELTA and strategy_config:
            dest_start = await self.destination_provider.get_last_sync_point()
            source_end = await self.source_provider.get_max_sync_point()
            return dest_start, source_end
        if strategy_config.column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            start = "00000000000000000000000000000000"
            end = "ffffffffffffffffffffffffffffffff"
            if strategy_config.column_type == UniversalDataType.UUID_TEXT_DASH:
                return str(uuid.UUID(start)), str(uuid.UUID(end))
            elif strategy_config.column_type == UniversalDataType.UUID_TEXT:
                return start, end
            return uuid.UUID(start), uuid.UUID(end)

        return await self.source_provider.get_partition_bounds()
    
    async def _create_provider(self, config: Dict[str, Any], data_column_schema=None, state_column_schema=None, role=None) -> Provider:
        """Factory method to create providers with ColumnSchema for both data and state"""
        # Create provider with column schemas for both data and state backends
        provider = Provider(config, data_column_schema=data_column_schema, state_column_schema=state_column_schema, role=role)
        await provider.connect()
        return provider
    
    # async def _get_last_sync_point(self) -> Any:
    #     """Get the last sync point from destination state"""
    #     if not self.destination_state_provider:
    #         return None
        
    #     partition_key = self.destination_state_provider.column_mapping.delta_key
    #     if not partition_key:
    #         return None
        
    #     query = f"SELECT MAX({partition_key}) as max_point FROM {self.destination_state_provider.table}"
    #     result = await self.destination_state_provider.execute_query(query)
        
    #     if len(result) > 0 and result.iloc[0]['max_point'] is not None:
    #         return result.iloc[0]['max_point']
        
    #     return None
    
    # async def _get_source_max_point(self) -> Any:
    #     """Get the maximum point from source"""
    #     if not self.source_provider:
    #         return None
        
    #     partition_key = self.source_provider.column_mapping.partition_key
    #     if not partition_key:
    #         return None
        
    #     query = f"SELECT MAX({partition_key}) as max_point FROM {self.source_provider.table}"
    #     result = await self.source_provider.execute_query(query)
        
    #     if len(result) > 0 and result.iloc[0]['max_point'] is not None:
    #         return result.iloc[0]['max_point']
        
    #     return None
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            providers = [
                self.source_provider,
                self.destination_provider,
                self.enrichment_engine
            ]
            
            # Close all provider connections
            close_tasks = []
            for provider in providers:
                if provider:
                    close_tasks.append(provider.disconnect())
            
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
            
            # Shutdown executor
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
