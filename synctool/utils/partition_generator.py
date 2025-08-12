from synctool.core.enums import DataStatus
import asyncio
from dataclasses import dataclass
import math
from typing import List, Any, Optional, Tuple       
from datetime import datetime, timedelta
from ..core.models import StrategyConfig, Partition

@dataclass
class PartitionConfig:
    """Partition configuration"""
    name: str
    column: str
    column_type: str
    partition_step: int


def add_exclusive_range(value):
    if value is None:
        return value
    if isinstance(value, int):
        return value+1
    if isinstance(value, datetime):
        return value + timedelta(seconds=1)
    return value


def to_partitions(
    partitions_data: List[dict],
    parent_partition: Partition,
) -> List[Partition]:
    partitions: List[Partition] = []
    start, end = parent_partition.start, parent_partition.end
    level: int = parent_partition.level+1
    intervals: list[int] = parent_partition.intervals
    column: str = parent_partition.column
    column_type: str = parent_partition.column_type
    for partition_data in partitions_data:
        partition_id, partition_hash, num_rows = partition_data["partition_id"], partition_data["partition_hash"], partition_data["num_rows"]
        parts: list[int] = [int(x) for x in partition_id.split('-')]
        partition_start = 0
        for index, part_num in enumerate(parts):
            partition_start += part_num*intervals[index]
        partition_end = partition_start+intervals[level]
        if column_type == "datetime":
            # Convert timestamps to datetime objects with the same timezone awareness as start/end
            if getattr(start, 'tzinfo', None) is not None:
                # If start is timezone-aware, create timezone-aware datetimes
                partition_start_dt: datetime = datetime.fromtimestamp(partition_start, start.tzinfo)
                partition_end_dt: datetime = datetime.fromtimestamp(partition_end, end.tzinfo)
                # step_size = partition_end-partition_start
            else:
                # If start is timezone-naive, create timezone-naive datetimes
                partition_start_dt: datetime = datetime.fromtimestamp(partition_start)
                partition_end_dt: datetime = datetime.fromtimestamp(partition_end)
                # step_size = partition_end-partition_start
            
            partition_start = max(partition_start_dt, start)
            partition_end = min(partition_end_dt, end)
        elif column_type == "int":
            partition_start = max(partition_start, start)
            partition_end = min(partition_end,end)
        partition: Partition = Partition(
            start=partition_start, 
            end=partition_end,
            # step_size=step_size,
            level=level,
            num_rows=num_rows, 
            hash=partition_hash,
            column=column,
            column_type=column_type,
            intervals=intervals,
            parent_partition=parent_partition,
            partition_id=partition_id
        )
        partitions.append(partition)
    return partitions


def calculate_partition_status(src_partitions: List[Partition], snk_partitions: List[Partition]) -> Tuple[List[Partition], dict[tuple[Any, Any, int], str]]:
    status: dict[tuple[Any, Any, int], str] = {}
    all_keys: set[tuple[Any, Any, int]] = {(c.start, c.end, c.level) for c in src_partitions} | {(c.start, c.end, c.level) for c in snk_partitions}
    src_map: dict[tuple[Any, Any, int], Partition] = {(c.start, c.end, c.level): c for c in src_partitions}
    snk_map: dict[tuple[Any, Any, int], Partition] = {(c.start, c.end, c.level): c for c in snk_partitions}
    result: list[Partition] = []

    for key in sorted(all_keys):
        sc: Partition | None = src_map.get(key)
        kc: Partition | None = snk_map.get(key)
        sel: Partition | None = sc or kc
        if sc and kc:
            sel = sc if sc.num_rows> kc.num_rows else kc
        if sc and kc and sc.num_rows == kc.num_rows and sc.hash == kc.hash:
            status[key] = DataStatus.UNCHANGED
        elif sc and kc:
            status[key] = DataStatus.MODIFIED
        elif sc and not kc:
            status[key] = DataStatus.ADDED
        else:
            status[key] = DataStatus.DELETED
        result.append(sel)

    return result, status       


def build_intervals(initial_partition_interval: int, max_partition_step: int, interval_reduction_factor: int) -> list[int]:
    intervals: list[int] = []
    interval: int = initial_partition_interval
    while interval>max_partition_step:
        intervals.append(interval)
        interval = interval//interval_reduction_factor
    intervals.append(interval)    
    return intervals

def merge_adjacent(partitions: List[Partition], statuses: List[str], max_partition_step: int) -> Tuple[List[Partition], List[str]]:
    merged_partitions, merged_statuses = [], []

    for c, st in zip(partitions, statuses):
        if st in ('M', 'A') and merged_partitions and merged_statuses[-1] == st and merged_partitions[-1].num_rows+c.num_rows <= max_partition_step:
            prev = merged_partitions[-1]
            prev.end = max(prev.end, c.end)
            prev.num_rows += c.num_rows
        else:
            merged_partitions.append(c)
            merged_statuses.append(st)

    return merged_partitions, merged_statuses

class PartitionGenerator:
    """Generate partitions based on strategy configuration"""
    
    def __init__(self, config: PartitionConfig):
        self.config = config
    
    async def generate_partitions(self, start: Any, end: Any, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate partition bounds between start and end"""
        partitions = []
        
        if self.config.column_type == "int":
            partitions: list[Partition] = await self._generate_int_partitions(start, end, parent_partition)
        elif self.config.column_type == "datetime":
            partitions = await self._generate_datetime_partitions(start, end, parent_partition)
        
        return partitions
    
    async def _generate_int_partitions(self, start: int, end: int, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate integer-based partitions"""
        partitions = []
        current = start
        partition_id = 0
        
        while current < end:
            partition_end = min(((current + self.config.partition_step)//self.config.partition_step)*self.config.partition_step, end)
            partitions.append(Partition(
                start=current,
                end=partition_end,
                column=self.config.column,
                column_type=self.config.column_type,
                partition_step=self.config.partition_step,
                partition_id=self.config.name.format(pid=partition_id),
                parent_partition=parent_partition
            ))
            current = partition_end
            partition_id += 1
        
        return partitions

    
    async def _generate_datetime_partitions(self, start: datetime, end: datetime, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate datetime-based partitions"""
        partitions = []
        current = start
        partition_id = 0
        start_timestamp = math.floor(start.timestamp())
        end_timestamp = math.ceil(end.timestamp())
        initial_partition_interval = self.config.partition_step
        cur = start_timestamp
        while cur<end_timestamp:
            cur1: int = ((cur+initial_partition_interval)//initial_partition_interval)*initial_partition_interval
            s1: datetime = datetime.fromtimestamp(cur, start.tzinfo)
            e1: datetime = datetime.fromtimestamp(min(cur1, end_timestamp), end.tzinfo)
            partitions.append(Partition(
                start=s1,
                end=e1,
                column=self.config.column,
                column_type=self.config.column_type,
                partition_step=self.config.partition_step,
                partition_id=self.config.name.format(pid=partition_id),
                parent_partition=parent_partition
            ))
            cur = cur1
        return partitions