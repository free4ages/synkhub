import hashlib
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable
from collections import OrderedDict
from dataclasses import dataclass
import threading

from ..core.models import Partition
from ..core.enums import HashAlgo
from ..core.query_models import Query
from ..core.schema_models import UniversalDataType

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Cache entry containing row hashes for a partition"""
    partition_id: str
    row_hashes: List[Dict[str, Any]]
    unique_keys: List[str]
    order_keys: List[str]
    hash_algo: HashAlgo
    num_rows: int

class HashCache:
    """Cache backend for partition hash and individual row hash operations"""
    
    def __init__(self, max_rows: int = 50000, part_rows_threshold: int = 500):
        self.max_rows = max_rows
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.part_rows_threshold = part_rows_threshold
        self._lock = threading.RLock()  # Use RLock for potential recursive calls
        self._total_cached_rows = 0  # Track total rows across all partitions
        
        self._cache_hits = 0
        self._cache_misses = 0
        
    def _is_subpartition(self, partition_id: str, cached_partition_id: str) -> bool:
        """Check if partition_id is a subpartition of cached_partition_id"""
        # e.g., "1-2-3-4" is subpartition of "1-2-3"
        cached_parts = cached_partition_id.split('-')
        partition_parts = partition_id.split('-')
        
        if len(partition_parts) <= len(cached_parts):
            return False
            
        # Check if all cached parts match the beginning of partition parts
        return cached_parts == partition_parts[:len(cached_parts)]
    
    def _calculate_partition_id_from_row(self, row: Dict[str, Any], partition_column: str, 
                                       partition_column_type: str, intervals: List[int],
                                       parent_partition_id: Optional[str] = None, level: int = 0) -> str:
        """Calculate partition_id for a row based on partition column value"""
        if partition_column not in row:
            raise ValueError(f"Partition column {partition_column} not found in row")
        
        value = row[partition_column]
        parent_offset = 0
        
        # Calculate parent offset if we have parent partition
        if parent_partition_id:
            parent_parts = [int(x) for x in parent_partition_id.split('-')]
            parent_offset = sum(intervals[i] * parent_parts[i] for i in range(len(parent_parts)))
        
        # Calculate partition number based on column type
        if partition_column_type == UniversalDataType.INTEGER:
            partition_num = int((value - parent_offset) // intervals[level])
        elif partition_column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            import math
            timestamp = value.timestamp() if hasattr(value, 'timestamp') else value
            partition_num = int(math.floor((timestamp - parent_offset) / intervals[level]))
        elif partition_column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            from ..utils.partition_generator import hex_to_int
            # Extract first 8 characters and convert to int
            hex_str = str(value)[:8]
            hex_int = hex_to_int(hex_str)
            partition_num = int((hex_int - parent_offset) // intervals[level])
        else:
            raise ValueError(f"Unsupported partition column type: {partition_column_type}")
        
        # Build partition_id
        if parent_partition_id:
            return f"{parent_partition_id}-{partition_num}"
        else:
            return str(partition_num)
    
    def _get_partition_boundaries(self, partition: Partition, target_partition_id: str) -> Tuple[Any, Any]:
        """Calculate boundaries for a target partition using intervals and partition_id logic"""
        target_parts = [int(x) for x in target_partition_id.split('-')]
        intervals = partition.intervals
        
        if len(target_parts) > len(intervals):
            raise ValueError(f"Target partition level {len(target_parts)} exceeds available intervals {len(intervals)}")
        
        # Calculate start boundary using dot product logic
        start_offset = 0
        for index, part_num in enumerate(target_parts):
            start_offset += part_num * intervals[index]
        
        # End boundary is start + interval for the current level
        level = len(target_parts) - 1
        end_offset = start_offset + intervals[level]
        
        # Convert offsets to actual values based on column type
        column_type = partition.column_type
        
        if column_type in ("datetime", "timestamp"):
            from datetime import datetime
            start_val = datetime.fromtimestamp(start_offset)
            end_val = datetime.fromtimestamp(end_offset)
        elif column_type == "integer":
            start_val = start_offset
            end_val = end_offset
        elif column_type in ("uuid", "uuid_text", "uuid_text_dash"):
            from ..utils.partition_generator import int_to_hex, END_HEX_INT
            import uuid
            
            if end_offset >= END_HEX_INT:
                end_offset = END_HEX_INT
            
            start_hex = int_to_hex(start_offset, pad_len=8) + "0" * 24
            end_hex = int_to_hex(end_offset, pad_len=8) + "0" * 24
            
            if column_type == "uuid_text_dash":
                start_val = str(uuid.UUID(start_hex))
                end_val = str(uuid.UUID(end_hex))
            elif column_type == "uuid_text":
                start_val = start_hex
                end_val = end_hex
            elif column_type == "uuid":
                start_val = uuid.UUID(start_hex)
                end_val = uuid.UUID(end_hex)
        else:
            raise ValueError(f"Unsupported column type: {column_type}")
            
        return start_val, end_val
    
    def _filter_rows_for_subpartition(self, cached_entry: CacheEntry, partition: Partition, 
                                     target_partition_id: str) -> List[Dict[str, Any]]:
        """Filter cached rows to match the target subpartition boundaries"""
        start_val, end_val = self._get_partition_boundaries(partition, target_partition_id)
        
        # Filter rows based on partition column bounds
        partition_column = partition.column
        filtered_rows = []
        
        for row in cached_entry.row_hashes:
            if partition_column in row:
                row_val = row[partition_column]
                if start_val <= row_val < end_val:
                    filtered_rows.append(row)
        
        return filtered_rows
    
    def _calculate_hash_from_cache(self, filtered_rows: List[Dict[str, Any]], 
                                  unique_keys: List[str], order_keys: List[str], 
                                  hash_algo: HashAlgo) -> str:
        """Calculate partition hash from cached row data"""
        if not filtered_rows:
            return ""
        
        if hash_algo == HashAlgo.HASH_MD5_HASH:
            # Sort rows by order keys for consistent hash
            if order_keys:
                try:
                    filtered_rows.sort(key=lambda x: tuple(x.get(k, '') for k in order_keys))
                except (KeyError, TypeError):
                    # Fallback to unique keys if order keys fail
                    filtered_rows.sort(key=lambda x: tuple(x.get(k, '') for k in unique_keys))
            
            # Concatenate all hash values and compute MD5
            all_hashes = [row.get('hash__', '') for row in filtered_rows]
            combined = ''.join(str(h) for h in all_hashes)
            return hashlib.md5(combined.encode()).hexdigest()
            
        elif hash_algo == HashAlgo.MD5_SUM_HASH:
            # Sum all hash values (assuming they are numeric)
            total = 0
            for row in filtered_rows:
                hash_val = row.get('hash__', 0)
                if isinstance(hash_val, (int, float)):
                    total += int(hash_val)
                elif isinstance(hash_val, str):
                    try:
                        # Convert hex string to int if it's a hex hash
                        if hash_val.startswith('0x') or len(hash_val) == 8:
                            total += int(hash_val, 16) if hash_val.startswith('0x') else int(hash_val, 16)
                        else:
                            total += int(hash_val)
                    except ValueError:
                        pass
            return str(total)
        
        return ""
    
    # def _evict_if_needed(self):
    #     """Evict oldest entries if total cached rows exceeds max_rows"""
    #     while self._total_cached_rows >= self.max_rows:
    #         oldest_key = next(iter(self.cache))
    #         oldest_entry = self.cache[oldest_key]
    #         self._total_cached_rows -= oldest_entry.num_rows
    #         del self.cache[oldest_key]
    #         logger.debug(f"Evicted cache entry for partition {oldest_key} ({oldest_entry.num_rows} rows)")
    
    async def fetch_child_partition_hashes(self, partition: Partition,backend, fallback_fn: Callable, hash_algo: HashAlgo) -> List[Dict]:
        """
        Fetch child partition hashes, using cache when possible.
        If partition has partition_id and num_rows < 50, cache the row hashes.
        """
        with self._lock:
            # Check if we can serve from cache
            if partition.partition_id:
                for cached_partition_id, cached_entry in self.cache.items():
                    if self._is_subpartition(partition.partition_id, cached_partition_id):
                        logger.debug(f"Serving partition {partition.partition_id} from cache (parent: {cached_partition_id})")
                        
                        # Filter cached rows for this subpartition
                        filtered_rows = self._filter_rows_for_subpartition(
                            cached_entry, partition, partition.partition_id
                        )
                        backend_column_schema = backend.column_schema
                        unique_keys = [col.expr for col in backend_column_schema.unique_keys] if backend_column_schema.unique_keys else []
                        order_keys = [col.expr for col, _ in backend_column_schema.order_keys] if backend_column_schema.order_keys else []
                        
                        
                        # Calculate hash from filtered rows
                        partition_hash = self._calculate_hash_from_cache(
                            filtered_rows, unique_keys, order_keys, hash_algo
                        )
                        
                        # Move to end (most recently used)
                        self.cache.move_to_end(cached_partition_id)
                        
                        return [{
                            'partition_id': partition.partition_id,
                            'num_rows': len(filtered_rows),
                            'partition_hash': partition_hash
                        }]
        
        # Not in cache, fetch from backend directly
        result = await fallback_fn()
        
        # Cache if conditions are met or if num_rows is 0
        should_cache = False
        if partition.partition_id:
            for res in result:
                num_rows = res.get('num_rows', 0)
                if num_rows == 0 or (num_rows > 0 and num_rows < 50):
                    should_cache = True
                    break
        
        if should_cache:
            with self._lock:
                # Always try to cache, eviction will handle row limits
                try:
                    # Fetch row hashes for caching
                    row_hashes = await backend._fetch_partition_row_hashes_direct(partition, hash_algo=hash_algo)
                    
                    # Create cache entry first
                    cache_entry = CacheEntry(
                        partition_id=partition.partition_id,
                        row_hashes=row_hashes,
                        unique_keys=[col.expr for col in backend.column_schema.unique_keys] if backend.column_schema.unique_keys else [],
                        order_keys=[col.expr for col, _ in backend.column_schema.order_keys] if backend.column_schema.order_keys else [],
                        hash_algo=hash_algo,
                        num_rows=len(row_hashes)
                    )
                    
                    # # Evict if needed to make room for new entry
                    # temp_total = self._total_cached_rows + len(row_hashes)
                    # while temp_total >= self.max_rows and self.cache:
                    #     oldest_key = next(iter(self.cache))
                    #     oldest_entry = self.cache[oldest_key]
                    #     temp_total -= oldest_entry.num_rows
                    #     self._total_cached_rows -= oldest_entry.num_rows
                    #     del self.cache[oldest_key]
                    #     logger.debug(f"Evicted cache entry for partition {oldest_key} ({oldest_entry.num_rows} rows)")
                    
                    # Add new entry
                    self.cache[partition.partition_id] = cache_entry
                    self._total_cached_rows += len(row_hashes)
                    # Move to end (most recently used)
                    self.cache.move_to_end(partition.partition_id)
                    
                    logger.debug(f"Cached row hashes for partition {partition.partition_id} ({len(row_hashes)} rows)")
                    
                except Exception as e:
                    logger.warning(f"Failed to cache partition {partition.partition_id}: {e}")
        
        return result
    
    async def fetch_partition_row_hashes(self, partition: Partition, backend, fallback_fn: Callable, hash_algo: HashAlgo) -> List[Dict]:
        """
        Fetch partition row hashes, serving from cache if available.
        Checks if the asked partition is derived from existing cache partition.
        """
        with self._lock:
            # Check if partition is in cache
            if partition.partition_id and partition.partition_id in self.cache:
                cached_entry = self.cache[partition.partition_id]
                # Move to end (most recently used)
                self.cache.move_to_end(partition.partition_id)
                logger.debug(f"Serving row hashes for partition {partition.partition_id} from cache")
                return cached_entry.row_hashes
            
            # Check if partition is a subpartition of cached data using index
            if partition.partition_id:
                for cached_partition_id, cached_entry in self.cache.items():
                    if self._is_subpartition(partition.partition_id, cached_partition_id):
                        logger.debug(f"Serving row hashes for subpartition {partition.partition_id} from cache (parent: {cached_partition_id})")
                        
                        filtered_rows = self._filter_rows_for_subpartition(
                            cached_entry, partition, partition.partition_id
                        )
                        
                        # Move parent to end (most recently used)
                        self.cache.move_to_end(cached_partition_id)
                        self._cache_hits += 1
                        return filtered_rows
        
        # Not in cache, fetch from backend directly
        self._cache_misses += 1
        return await fallback_fn()
    
    async def fetch_row_hashes(self, unique_key_values: List[Dict[str, Any]], 
                             unique_keys: List[str], partition_column: str,
                             partition_column_type: str, intervals: List[int],
                             parent_partition_id: Optional[str] = None, level: int = 0) -> List[Dict[str, Any]]:
        """
        Fetch row hashes for specific unique key values from cache if possible.
        Calculates partition_id from partition_key and checks cache efficiently.
        """
        result = []
        
        with self._lock:
            for target_row in unique_key_values:
                try:
                    # Calculate partition_id for this row
                    calculated_partition_id = self._calculate_partition_id_from_row(
                        target_row, partition_column, partition_column_type, 
                        intervals, parent_partition_id, level
                    )
                    
                    # Check if we have this partition or a parent partition in cache
                    cached_entry = None
                    
                    # First check exact match
                    if calculated_partition_id in self.cache:
                        cached_entry = self.cache[calculated_partition_id]
                        self.cache.move_to_end(calculated_partition_id)
                    else:
                        # Check if any cached partition is a parent of calculated partition
                        for cached_id, entry in self.cache.items():
                            if self._is_subpartition(calculated_partition_id, cached_id):
                                cached_entry = entry
                                self.cache.move_to_end(cached_id)
                                break
                    
                    if cached_entry:
                        # Search for the specific row in cached data
                        target_key = tuple(target_row.get(k) for k in unique_keys)
                        for cached_row in cached_entry.row_hashes:
                            cached_key = tuple(cached_row.get(k) for k in unique_keys)
                            if cached_key == target_key:
                                result.append(cached_row)
                                break
                    
                except Exception as e:
                    logger.warning(f"Failed to calculate partition_id for row: {e}")
                    continue
        
        return result
    
    def mark_partition_complete(self, partition_id: str, partition: Optional[Partition] = None):
        """
        Mark partition as complete and evict it from cache.
        - If partition_id exists directly in cache (self), remove the entire entry
        - If partition_id is a subpartition of any cached partition, remove only the rows 
          belonging to this subpartition from the parent partition cache
        """
        with self._lock:
            # Case 1: Check if this partition is directly cached (self)
            if partition_id in self.cache:
                entry = self.cache[partition_id]
                self._total_cached_rows -= entry.num_rows
                del self.cache[partition_id]
                logger.debug(f"Evicted completed partition {partition_id} from cache ({entry.num_rows} rows)")
                return
            
            # Case 2: Check if this partition is a subpartition of any existing cached partition
            if partition:
                for cached_partition_id, cached_entry in self.cache.items():
                    # Check if partition_id is a subpartition of cached_partition_id
                    if self._is_subpartition(partition_id, cached_partition_id):
                        logger.debug(f"Found parent partition {cached_partition_id} for completed subpartition {partition_id}")
                        
                        try:
                            # Get boundaries for the completed subpartition
                            start_val, end_val = self._get_partition_boundaries(partition, partition_id)
                            
                            # Filter out rows that belong to the completed subpartition
                            partition_column = partition.column
                            remaining_rows = []
                            removed_count = 0
                            
                            for row in cached_entry.row_hashes:
                                if partition_column in row:
                                    row_val = row[partition_column]
                                    if start_val <= row_val < end_val:
                                        # This row belongs to the completed subpartition, remove it
                                        removed_count += 1
                                    else:
                                        # Keep this row
                                        remaining_rows.append(row)
                                else:
                                    # Keep rows without partition column
                                    remaining_rows.append(row)
                            
                            if removed_count > 0:
                                # Update the cached entry with remaining rows
                                old_row_count = cached_entry.num_rows
                                updated_entry = CacheEntry(
                                    partition_id=cached_entry.partition_id,
                                    row_hashes=remaining_rows,
                                    unique_keys=cached_entry.unique_keys,
                                    order_keys=cached_entry.order_keys,
                                    hash_algo=cached_entry.hash_algo,
                                    num_rows=len(remaining_rows)
                                )
                                
                                # Update cache and total row count
                                self.cache[cached_partition_id] = updated_entry
                                self._total_cached_rows = self._total_cached_rows - old_row_count + len(remaining_rows)
                                
                                logger.debug(f"Removed {removed_count} rows from cached parent partition {cached_partition_id} "
                                           f"for completed subpartition {partition_id}")
                                
                                # If no rows remain in parent partition, remove it entirely
                                if len(remaining_rows) == 0:
                                    del self.cache[cached_partition_id]
                                    self._total_cached_rows -= len(remaining_rows)  # Should be 0, but for safety
                                    logger.debug(f"Removed empty parent partition {cached_partition_id} after completing subpartition {partition_id}")
                                
                                return  # Found and processed the parent partition
                            
                        except Exception as e:
                            logger.warning(f"Failed to remove rows for completed subpartition {partition_id} from parent {cached_partition_id}: {e}")
                            continue
            
            # If we reach here, the partition was not found in cache (neither self nor subpartition)
            logger.debug(f"Completed partition {partition_id} was not found in cache")
    
    def clear_cache(self):
        """Clear all cached entries"""
        with self._lock:
            self.cache.clear()
            self._total_cached_rows = 0
            logger.debug("Cleared all cache entries")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total_requests = self._cache_hits + self._cache_misses
            hit_rate = self._cache_hits / total_requests if total_requests > 0 else 0
            
            return {
                'partitions_count': len(self.cache),
                'total_cached_rows': self._total_cached_rows,
                'max_rows': self.max_rows,
                'partitions': list(self.cache.keys()),
                'utilization': self._total_cached_rows / self.max_rows if self.max_rows > 0 else 0,
                'cache_hits': self._cache_hits,
                'cache_misses': self._cache_misses,
                'hit_rate': hit_rate
            }

