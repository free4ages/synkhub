from synctool.core.enums import DataStatus
from synctool.utils.data_comparator import calculate_row_status
from synctool.utils.partition_generator import merge_adjacent
from synctool.utils.partition_generator import build_intervals
from synctool.utils.partition_generator import calculate_partition_status
from synctool.utils.partition_generator import to_partitions
from typing import Tuple
from typing import List
import logging
import pandas as pd
from typing import Dict, Any, TYPE_CHECKING

from synctool.core.enums import HashAlgo
from synctool.utils.partition_generator import PartitionConfig, PartitionGenerator
from ..core.models import Partition, StrategyConfig, SyncStrategy
from ..utils.hash_calculator import HashCalculator

if TYPE_CHECKING:
    from .sync_engine import SyncEngine


class PartitionProcessor:
    """Process individual partitions with specific sync logic"""
    
    def __init__(self, sync_engine: 'SyncEngine', partition: Partition, strategy: SyncStrategy, strategy_config: StrategyConfig):
        self.sync_engine = sync_engine
        self.partition = partition
        self.strategy = strategy
        self.strategy_config = strategy_config
        self.logger = logging.getLogger(f"{__name__}")
    
    
    async def process(self) -> Dict[str, Any]:
        """Process a single partition"""
        try:
            self.logger.info(f"Processing partition {self.partition.partition_id} with strategy {self.strategy.value} from {self.partition.start} to {self.partition.end}")
            
            if self.strategy == SyncStrategy.FULL:
                result = await self._process_full_sync()
            elif self.strategy == SyncStrategy.DELTA:
                result = await self._process_delta_sync()
            elif self.strategy == SyncStrategy.HASH:
                result = await self._process_hash_sync()
            else:
                raise ValueError(f"Unknown strategy: {self.strategy}")
            
            self.logger.info(f"Completed partition {self.partition.partition_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed processing partition {self.partition.partition_id}: {str(e)}")
            raise

       
    async def _process_full_sync(self) -> Dict[str, Any]:
        """Process full sync for partition with pagination and subpartitioning"""
        rows_inserted = 0
        page_size = getattr(self.strategy_config, 'page_size', 1000) if self.strategy_config.use_pagination else None # Default to 1000 if not specified
        
        sub_partition_config: PartitionConfig = PartitionConfig(
            name="sub_partition_{pid}",
            column=self.partition.column,
            column_type=self.partition.column_type,
            partition_step=self.strategy_config.sub_partition_step
        )
        sub_partition_generator: PartitionGenerator = PartitionGenerator(sub_partition_config)
        sub_partitions: list[Partition] = await sub_partition_generator.generate_partitions(self.partition.start, self.partition.end, self.partition)

        # check if pagination is needed. it makes extra call without data
        # if parition column is unique, we can skip pagination
        use_pagination = self.strategy_config.use_pagination
        # if page_size >= self.strategy_config.sub_partition_step:
        #     unique_keys = [x.name for x in self.sync_engine.column_mapper.schemas["common"].unique_keys]
        #     if len(unique_keys) == 1 and unique_keys[0] == self.partition.column:
        #         skip_pagination = True
        
        for sub_partition in sub_partitions:
            # Process subpartition with pagination
            
            sub_partition_rows = 0
            offset = 0
            
            while True:
                self.logger.info(f"Processing subpartition {sub_partition.partition_id} from {sub_partition.start} to {sub_partition.end} with page_size {page_size} and offset {offset}")
                # Fetch paginated data from source
                data: list[dict[str, Any]] = await self.sync_engine.source_provider.fetch_partition_data(
                    sub_partition, 
                    hash_algo=self.sync_engine.hash_algo,
                    page_size=page_size if use_pagination else None,
                    offset=offset if use_pagination else None
                )
                
                # If no data returned, we've processed all pages for this subpartition
                if not data:
                    break
                
                # Enrich data if enrichment engine is configured
                if self.sync_engine.enrichment_engine:
                    data = await self.sync_engine.enrichment_engine.enrich(data)
                
                # Insert the current page of data
                r_inserted = await self.sync_engine.destination_provider.insert_partition_data(
                    data, sub_partition, upsert=False
                )
                
                sub_partition_rows += len(data)
                rows_inserted += len(data)
                if not use_pagination:
                    break


                offset += page_size


                
                # If we got fewer rows than page_size, we've reached the end
                if len(data) < page_size:
                    break


        
        return {
            'partition_id': self.partition.partition_id,
            'strategy': 'full',
            'rows_detected': rows_inserted,
            'rows_inserted': rows_inserted,
            'rows_updated': 0,
            'rows_deleted': 0,
            'status': 'success',
            'hash_query_count': len(sub_partitions),
            'data_query_count': len(sub_partitions),
        }

    
    async def _process_delta_sync(self) -> Dict[str, Any]:
        """Process delta sync for partition with pagination and subpartitioning"""
        rows_inserted = 0
        page_size = getattr(self.strategy_config, 'page_size', 1000)  # Default to 1000 if not specified
        
        sub_partition_config: PartitionConfig = PartitionConfig(
            name="sub_partition_{pid}",
            column=self.partition.column,
            column_type=self.partition.column_type,
            partition_step=self.strategy_config.sub_partition_step
        )
        sub_partition_generator: PartitionGenerator = PartitionGenerator(sub_partition_config)
        sub_partitions: list[Partition] = await sub_partition_generator.generate_partitions(self.partition.start, self.partition.end, self.partition)
        
        for sub_partition in sub_partitions:
            # Process subpartition with pagination
            sub_partition_rows = 0
            offset = 0
            
            while True:
                # Fetch paginated data from source
                data: list[dict[str, Any]] = await self.sync_engine.source_provider.fetch_delta_data(
                    sub_partition, 
                    hash_algo=self.sync_engine.hash_algo,
                    page_size=page_size,
                    offset=offset
                )
                
                # If no data returned, we've processed all pages for this subpartition
                if not data:
                    break
                
                # Enrich data if enrichment engine is configured
                if self.sync_engine.enrichment_engine:
                    data = await self.sync_engine.enrichment_engine.enrich(data)
                
                # Insert the current page of data
                r_inserted = await self.sync_engine.destination_provider.insert_delta_data(
                    data, sub_partition, upsert=True
                )
                
                sub_partition_rows += r_inserted
                rows_inserted += r_inserted
                offset += page_size
                
                # If we got fewer rows than page_size, we've reached the end
                if len(data) < page_size:
                    break
        
        return {
            'partition_id': self.partition.partition_id,
            'strategy': 'delta',
            'rows_detected': rows_inserted,
            'rows_inserted': rows_inserted,
            'rows_updated': 0,
            'rows_deleted': 0,
            'status': 'success',
            'hash_query_count': 0,
            'data_query_count': len(sub_partitions),
        }

    async def _process_hash_sync(self) -> Dict[str, Any]:
        """Process hash-based sync for partition"""
        rows_detected = 0
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        hash_query_count = 0
        data_query_count = 0
        self.partition.intervals = build_intervals(self.sync_engine.config.partition_step, self.strategy_config.sub_partition_step, self.strategy_config.interval_reduction_factor)
        sub_partition_step = self.strategy_config.sub_partition_step
        partitions, statuses, hash_query_count = await self.calculate_sub_partitions(self.partition, sub_partition_step,  max_level=len(self.partition.intervals)-1, page_size=self.strategy_config.page_size)
        
        partitions, statuses = merge_adjacent(partitions, statuses, sub_partition_step)
        sync_engine = self.sync_engine


        for partition,status in zip(partitions,statuses):
            added, modified, deleted = [], [], []
            rows_detected += partition.num_rows
            if status == DataStatus.ADDED:
                data = await self.sync_engine.source_provider.fetch_partition_data(partition, hash_algo=self.sync_engine.hash_algo)
                if self.sync_engine.enrichment_engine:
                    data = await self.sync_engine.enrichment_engine.enrich(data)
                r_inserted = await self.sync_engine.destination_provider.insert_partition_data(data, partition, upsert=True)
                rows_inserted += len(data)
                data_query_count += 1
            elif status == DataStatus.DELETED:
                r_deleted = await self.sync_engine.destination_provider.delete_partition_data(partition)
                rows_deleted += partition.num_rows
            elif status == DataStatus.MODIFIED:
                if self.strategy_config.prevent_update_unless_changed:
                    src_hashes, src_hash_has_data = await self.sync_engine.source_provider.fetch_partition_row_hashes(partition, hash_algo=self.sync_engine.hash_algo)
                    dest_hashes, dest_hash_has_data = await self.sync_engine.destination_provider.fetch_partition_row_hashes(partition, hash_algo=self.sync_engine.hash_algo)
                    hash_query_count += 1
                    # find changed rows by comparing src_hashes and dest_hashes using unique_keys
                    # if src_hash_has_data, it means the data format belons to data backend, otherwise it belongs to state backend

                    unique_keys = [x.name for x in self.sync_engine.column_mapper.schemas["common"].unique_keys]

                    # src_unique_keys = sync_engine.source_provider.data_unique_keys if src_hash_has_data else sync_engine.source_provider.state_unique_keys
                    # dest_unique_keys = sync_engine.destination_provider.data_unique_keys if dest_hash_has_data else sync_engine.destination_provider.state_unique_keys

                    # get each row status by comparing src_hashes and dest_hashes using unique_keys
                    
                    calculated_rows, statuses = calculate_row_status(src_hashes, dest_hashes, unique_keys)
                    for row, r_status in zip(calculated_rows, statuses):
                        if r_status == DataStatus.ADDED:
                            added.append(row)
                        elif r_status == DataStatus.MODIFIED:
                            modified.append(row)
                        elif r_status == DataStatus.DELETED:
                            deleted.append(row)
                    
                    if src_hash_has_data:
                        added = await self.sync_engine.enrichment_engine.enrich(added) if self.sync_engine.enrichment_engine else added
                        modified = await self.sync_engine.enrichment_engine.enrich(modified) if self.sync_engine.enrichment_engine else modified
                        if added:
                            r_inserted = await self.sync_engine.destination_provider.insert_partition_data(added, partition, upsert=True)
                            rows_inserted += len(added)
                        if modified:
                            r_inserted = await self.sync_engine.destination_provider.insert_partition_data(modified, partition, upsert=True)
                            rows_updated += len(modified)
                        if deleted:
                            r_deleted = await self.sync_engine.destination_provider.delete_rows(deleted, partition, self.strategy_config)
                            rows_deleted += len(deleted)
                    # else:
                    #     added = 
                else:
                    data = await self.sync_engine.source_provider.fetch_partition_data(partition, hash_algo=self.sync_engine.hash_algo)
                    if self.sync_engine.enrichment_engine:
                        data = await self.sync_engine.enrichment_engine.enrich(data)
                    data_query_count += 1
                    # if status == DataStatus.ADDED:
                    r_inserted = await self.sync_engine.destination_provider.insert_partition_data(data, partition,column_keys=self.get_destination_columns(), upsert=True)
                    rows_inserted += len(data)
                
        return {
            'partition_id': self.partition.partition_id,
            'strategy': self.strategy.value,
            'rows_detected': rows_detected,
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'rows_deleted': rows_deleted,
            'status': 'success',
            'hash_query_count': hash_query_count,
            'data_query_count': data_query_count,
        }
    
    async def calculate_sub_partitions(
        self,
        partition: Partition,
        sub_partition_step: int,
        max_level =100,
        page_size: int = 1000
    ) -> Tuple[List[Partition], List[str], int]:
        src_rows: list[dict[str, Any]] = await self.sync_engine.source_provider.fetch_child_partition_hashes(partition, hash_algo=self.sync_engine.hash_algo)
        destination_rows = await self.sync_engine.destination_provider.fetch_child_partition_hashes(partition, hash_algo=self.sync_engine.hash_algo)
        s_partitions: list[Partition] = to_partitions(src_rows, partition)
        d_partitions: list[Partition] = to_partitions(destination_rows, partition)
        partitions, status_map = calculate_partition_status(s_partitions, d_partitions)
        final_partitions, statuses = [], []
        hash_query_count = 0
        for p in partitions:
            key = (p.start, p.end, p.level)
            st = status_map[key]
            # if p.level == 1:
            if st in ('M', 'A') and (p.num_rows > page_size and p.level<max_level):
                # intervals[level] = math.floor(intervals[-1]/interval_reduction_factor)
                deeper_partitions, deeper_statuses, deeper_hash_query_count = await self.calculate_sub_partitions(
                    p,
                    sub_partition_step,
                    max_level
                )
                hash_query_count += deeper_hash_query_count
                final_partitions.extend(deeper_partitions)
                statuses.extend(deeper_statuses)
            else:
                final_partitions.append(p)
                statuses.append(st)
                hash_query_count += 1

        return final_partitions, statuses, hash_query_count