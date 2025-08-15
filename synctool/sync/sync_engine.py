import asyncio
import concurrent.futures
import logging
import traceback
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..monitoring.metrics_collector import MetricsCollector

from ..core.models import SyncJobConfig, StrategyConfig, SyncProgress, Partition, DataStorage, ProviderConfig
from ..core.enums import HashAlgo, SyncStrategy, BackendType
from ..core.provider import Provider
from ..core.column_mapper import ColumnMapper
from ..utils.partition_generator import PartitionConfig, PartitionGenerator, add_exclusive_range
from ..utils.hash_calculator import HashCalculator
from ..utils.sync_result_builder import SyncResultBuilder
from ..utils.progress_manager import ProgressManager
from ..enrichment.enrichment_engine import EnrichmentEngine
from ..sync.partition_processor import PartitionProcessor
from ..core.schema_models import UniversalDataType

class SyncEngine:
    """Main sync engine that orchestrates the sync process"""
    
    def __init__(self, config: SyncJobConfig, metrics_collector: Optional['MetricsCollector'] = None, logger= None, data_storage: Optional[DataStorage] = None):
        self.config = config
        self.source_provider: Provider 
        self.destination_provider: Provider
        self.strategies: Dict[str, StrategyConfig] = {}
        self.enrichment_engine: Optional[EnrichmentEngine] = None
        self.hash_calculator = HashCalculator()
        self.progress = SyncProgress()
        logger_name = f"{logger.name}.engine" if logger else f"{__name__}.engine"
        self.logger = logging.getLogger(logger_name)
        self.hash_algo = config.hash_algo or HashAlgo.HASH_MD5_HASH
        self.column_mapper: ColumnMapper
        self.metrics_collector = metrics_collector
        self.data_storage = data_storage
        
        # Thread pool for partition processing
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_concurrent_partitions,
            thread_name_prefix=f"sync-{config.name}"
        )
    
    
    async def initialize(self):
        """Initialize all providers and components"""
        # Create ColumnMapper if column_map is present in config
        # Parse strategies
        for strategy_config_dict in self.config.strategies:
            if not strategy_config_dict.get('enabled', True):
                continue
            
            # Make a copy to avoid modifying the original config
            strategy_config_copy = strategy_config_dict.copy()
            
            if strategy_config_copy.get('type') in (SyncStrategy.FULL, SyncStrategy.HASH) and strategy_config_copy.get('column') != self.config.partition_key:
                self.logger.warning(f"Column {strategy_config_copy.get('column')} is not the same as partition key {self.config.partition_key}. It will be ignored")
                strategy_config_copy['column'] = self.config.partition_key
            
            strategy = StrategyConfig(**strategy_config_copy)
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
            
            # Create progress manager for centralized progress handling
            progress_manager = ProgressManager(
                progress=self.progress,
                metrics_collector=self.metrics_collector,
                progress_callback=progress_callback,
                logger=self.logger
            )
            
            # Determine sync strategy
            sync_strategy, strategy_config = await self._determine_strategy(strategy_name, start, end)
            self.logger.info(f"Using sync strategy: {sync_strategy.value}")
            
            # Initialize metrics
            progress_manager.update_progress()
            
            # Get sync bounds
            sync_start, sync_end = await self._get_sync_bounds(sync_strategy, strategy_config, start, end)
            if sync_start == sync_end:
                self.logger.info(f"Sync bounds are the same, no partitions to process")
                return SyncResultBuilder.build_success_result(
                    job_name=self.config.name,
                    strategy=sync_strategy,
                    progress=self.progress,
                    partitions_count=0,
                    partition_results=[],
                    start_time=self.progress.start_time
                )
            sync_end = add_exclusive_range(sync_end)
            self.logger.info(f"Sync bounds: {sync_start} to {sync_end}")
            
            # Generate partitions
            partitions = []
            if strategy_config and sync_start is not None and sync_end is not None:
                if not self.column_mapper:
                    raise ValueError("Column mapper not initialized")
                column_info = self.column_mapper.column(strategy_config.column)
                column_type_str = column_info.dtype if column_info.dtype else "string"
                partition_generator: PartitionGenerator = PartitionGenerator(PartitionConfig(
                    name="main_partition_{pid}",
                    column=strategy_config.column,
                    column_type=column_type_str,
                    partition_step=self.config.partition_step
                ))
                partitions: list[Partition] = await partition_generator.generate_partitions(sync_start, sync_end)
            else:
                raise ValueError("Sync bounds are not set")

            
            progress_manager.set_total_partitions(len(partitions))
            self.logger.info(f"Processing {len(partitions)} partitions")
            
            # Process partitions with controlled concurrency
            results: list[dict[str, Any]] = await self._process_partitions_concurrent(
                partitions, sync_strategy, strategy_config, progress_manager
            )
 
            
            # Build success result using centralized builder
            sync_result = SyncResultBuilder.build_success_result(
                job_name=self.config.name,
                strategy=sync_strategy,
                progress=self.progress,
                partitions_count=len(partitions),
                partition_results=results,
                start_time=self.progress.start_time
            )
            
            # Finalize metrics
            progress_manager.finalize_metrics(len(partitions))
            
            progress_manager.log_completion(sync_result['duration'])
            return sync_result
            
        except Exception as e:
            import traceback
            self.logger.error(f"Sync job failed: {traceback.format_exc()}")
            
            # Build failure result using centralized builder
            strategy_for_error = None
            try:
                strategy_for_error = sync_strategy
            except NameError:
                pass
            
            return SyncResultBuilder.build_failure_result(
                job_name=self.config.name,
                error=str(e),
                progress=self.progress,
                start_time=self.progress.start_time,
                strategy=strategy_for_error
            )
        finally:
            await self.cleanup()
    
    async def _process_partitions_concurrent(self, partitions: List[Partition], 
                                           strategy: SyncStrategy,
                                           strategy_config: Optional[StrategyConfig],
                                           progress_manager: ProgressManager) -> List[Dict[str, Any]]:
        """Process partitions with controlled concurrency"""
        results: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(self.config.max_concurrent_partitions)
        
        if self.config.max_concurrent_partitions == 1:
         
            for partition in partitions:
                try:
                    if strategy_config is None:
                        raise ValueError(f"Strategy config is required for partition processing")
                    processor: 'PartitionProcessor' = PartitionProcessor(self, partition, strategy, strategy_config, logger=self.logger)
                    result: dict[str, Any] = await processor.process()
                    results.append(result)
                    # Update progress using progress manager
                    progress_manager.update_from_partition_result(result, completed=True)
                except Exception as e:
                    self.logger.error(f"Partition {partition.partition_id} failed: {str(e)}")
                    progress_manager.update_progress(failed=True)
                    
                    # Build failure result using centralized builder
                    failure_result = SyncResultBuilder.build_partition_failure_result(
                        partition_id=partition.partition_id,
                        error=str(e),
                        strategy=strategy
                    )
                    results.append(failure_result)
                    raise e
            return results
        
        async def process_partition_with_semaphore(partition: Partition) -> Dict[str, Any]:
            async with semaphore:
                try:
                    if strategy_config is None:
                        raise ValueError(f"Strategy config is required for partition processing")

                    processor = PartitionProcessor(self, partition, strategy, strategy_config, logger=self.logger)
                    result = await processor.process()
                    
                    # Update progress using progress manager
                    progress_manager.update_from_partition_result(result, completed=True)
                    
                    return result
                    
                except Exception as e:
                    self.logger.error(f"Partition {partition.partition_id} failed: {str(e)}\nStacktrace:\n{traceback.format_exc()}")
                    progress_manager.update_progress(failed=True)
                    
                    # Build failure result using centralized builder
                    return SyncResultBuilder.build_partition_failure_result(
                        partition_id=partition.partition_id,
                        error=str(e),
                        strategy=strategy
                    )
        
        # Process partitions in batches
        batch_size = self.config.max_concurrent_partitions*2
        for i in range(0, len(partitions), batch_size):
            batch = partitions[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} partitions")
            
            tasks = [process_partition_with_semaphore(partition) for partition in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for batch_result in batch_results:
                if isinstance(batch_result, Exception):
                    self.logger.error(f"Batch processing error: {str(batch_result)}")
                    results.append({
                        'status': 'failed',
                        'error': str(batch_result),
                        'rows_detected': 0,
                        'rows_fetched': 0,
                        'rows_inserted': 0,
                        'rows_updated': 0,
                        'rows_deleted': 0,
                        'hash_query_count': 0,
                        'data_query_count': 0,
                    })
                else:
                    # batch_result should be a dict at this point
                    if isinstance(batch_result, dict):
                        results.append(batch_result)
        
        return results
    
    async def _determine_strategy(self, strategy_name: Optional[str], start: Any, end: Any) -> Tuple[SyncStrategy, Optional[StrategyConfig]]:
        """Determine which sync strategy to use"""
        if not self.destination_provider:
            raise ValueError("Destination provider not initialized")
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
            if not self.destination_provider or not self.source_provider:
                raise ValueError("Providers not initialized for delta sync")
            dest_start = await self.destination_provider.get_last_sync_point()
            source_end = await self.source_provider.get_max_sync_point()
            return dest_start, source_end
        if strategy_config and strategy_config.column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            start = "00000000000000000000000000000000"
            end = "ffffffffffffffffffffffffffffffff"
            if strategy_config.column_type == UniversalDataType.UUID_TEXT_DASH:
                return str(uuid.UUID(start)), str(uuid.UUID(end))
            elif strategy_config.column_type == UniversalDataType.UUID_TEXT:
                return start, end
            return uuid.UUID(start), uuid.UUID(end)

        return await self.source_provider.get_partition_bounds()
    
    async def _create_provider(self, provider_config: ProviderConfig, data_column_schema=None, state_column_schema=None, role=None) -> Provider:
        """Factory method to create providers with ColumnSchema for both data and state"""
        # Convert ProviderConfig to dictionary format that Provider expects
        config = {
            'data_backend': provider_config.data_backend.__dict__ if provider_config.data_backend else None,
            'state_backend': provider_config.state_backend.__dict__ if provider_config.state_backend else None
        }
        
        # Create provider with column schemas for both data and state backends
        provider = Provider(config, data_column_schema=data_column_schema, state_column_schema=state_column_schema, role=role, logger=self.logger, data_storage=self.data_storage)
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
