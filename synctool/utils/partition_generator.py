from synctool.core.enums import DataStatus
import asyncio
from dataclasses import dataclass
import math
import itertools
import uuid
from typing import List, Any, Optional, Tuple       
from datetime import datetime, timedelta
from ..core.models import StrategyConfig, Partition
from ..core.schema_models import UniversalDataType
import logging

logger = logging.getLogger(__name__)

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

def generate_uuid_prefixes(start: str, end: str, length: int) -> List[str]:
    uuid_chars = [chr(i) for i in range(ord('0'), ord('9')+1)] + [chr(i) for i in range(ord('a'), ord('f')+1)]
    #generate all possible combinations from uuid_chars of length length
    prefixes = [''.join(p) for p in itertools.product(uuid_chars, repeat=length)]
    return prefixes


def hex_to_int(hexstr: str) -> int:
    """Convert a hex string (0-9a-f) to an integer."""
    if not all(c in "0123456789abcdef" for c in hexstr.lower()):
        raise ValueError(f"Invalid hex string: {hexstr}")
    return int(hexstr, 16)


def int_to_hex(num: int, pad_len: int = None) -> str:
    """Convert an integer to a hex string (lowercase), optionally zero-padded."""
    if num < 0:
        raise ValueError("Negative numbers not supported")
    hexstr = format(num, "x")  # lowercase hex
    if pad_len is not None:
        hexstr = hexstr.rjust(pad_len, "0")
    return hexstr

END_HEX_INT = hex_to_int(8*"f")


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
        assert len(parts) == level+1, f"Partition id {partition_id} has {len(parts)} parts number, expected {level+1}"
        partition_start = 0
        for index, part_num in enumerate(parts):
            partition_start += part_num*intervals[index]
        partition_end = partition_start+intervals[level]
        if column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
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

        elif column_type == UniversalDataType.INTEGER:
            partition_start = max(partition_start, start)
            partition_end = min(partition_end,end)

        elif column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            if partition_end >= END_HEX_INT:
                partition_end = END_HEX_INT
            partition_start_str = int_to_hex(partition_start, pad_len=8)
            partition_end_str = int_to_hex(partition_end, pad_len=8)
            partition_start = partition_start_str+24*"0"
            partition_end = partition_end_str+24*"0"
            # print(partition_start, partition_end)
            if column_type == UniversalDataType.UUID_TEXT_DASH:
                partition_start = str(uuid.UUID(partition_start))
                partition_end = str(uuid.UUID(partition_end))
            elif column_type == UniversalDataType.UUID_TEXT:
                partition_start = partition_start_str
                partition_end = partition_end_str
            elif column_type == UniversalDataType.UUID:
                partition_start = uuid.UUID(partition_start)
                partition_end = uuid.UUID(partition_end)
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


def calculate_partition_status(src_partitions: List[Partition], snk_partitions: List[Partition],skip_row_count=False) -> Tuple[List[Partition], dict[tuple[Any, Any, int], str]]:
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
        if sc and kc and (skip_row_count or sc.num_rows == kc.num_rows) and sc.hash == kc.hash:
            status[key] = DataStatus.UNCHANGED
        elif sc and kc:
            status[key] = DataStatus.MODIFIED
        elif sc and not kc:
            status[key] = DataStatus.ADDED
        else:
            status[key] = DataStatus.DELETED
        result.append(sel)

    return result, status       


def build_intervals(initial_partition_interval: int, min_partition_step: int, interval_reduction_factor: int) -> list[int]:
    intervals: list[int] = []
    interval: int = initial_partition_interval
    while interval>min_partition_step:
        intervals.append(interval)
        interval = interval//interval_reduction_factor
    intervals.append(interval)    
    return intervals

def merge_adjacent(partitions: List[Partition], statuses: List[str], page_size: int) -> Tuple[List[Partition], List[str]]:
    merged_partitions, merged_statuses = [], []
    #sort partitions,statuses by start, end, level
    zipped = sorted(zip(partitions, statuses), key=lambda x: (x[0].start, x[0].end, x[0].level))
    # might need to preserve the merged partition in future
    for c, st in zipped:
        if st in ('M', 'A') and merged_partitions and merged_statuses[-1] == st and merged_partitions[-1].num_rows+c.num_rows <= page_size:
            logger.info(f"Merging adjacent partitions {merged_partitions[-1].start}-{merged_partitions[-1].end} and {c.start}-{c.end}")
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
        
        if self.config.column_type == UniversalDataType.INTEGER:
            partitions: list[Partition] = await self._generate_int_partitions(start, end, parent_partition)
        elif self.config.column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            partitions = await self._generate_datetime_partitions(start, end, parent_partition)
        elif self.config.column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            partitions = await self._generate_uuid_partitions(start, end, column_type = self.config.column_type, parent_partition=parent_partition)
        
        return partitions
    
    async def _generate_int_partitions(self, start: int, end: int, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate integer-based partitions"""
        parent_partition_id = parent_partition.partition_id if parent_partition else None
        partitions = []
        current = start
        partition_id = 0
        level = parent_partition.level + 1 if parent_partition else 0
        
        while current < end:
            partition_end = min(((current + self.config.partition_step)//self.config.partition_step)*self.config.partition_step, end)
            partitions.append(Partition(
                start=current,
                end=partition_end,
                column=self.config.column,
                column_type=self.config.column_type,
                partition_step=self.config.partition_step,
                partition_id=f"{parent_partition_id}-{partition_id}" if parent_partition_id else f"{partition_id}",
                parent_partition=parent_partition,
                level=level
            ))
            current = partition_end
            partition_id += 1
        
        return partitions

    
    async def _generate_datetime_partitions(self, start: datetime, end: datetime, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate datetime-based partitions"""
        partitions = []
        current = start
        parent_partition_id = parent_partition.partition_id if parent_partition else None
        partition_id = 0
        level = parent_partition.level + 1 if parent_partition else 0
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
                partition_id=f"{parent_partition_id}-{partition_id}" if parent_partition_id else f"{partition_id}",
                parent_partition=parent_partition,
                level=level
            ))
            cur = cur1
            partition_id += 1
        return partitions
    
    async def _generate_uuid_partitions(self, start: uuid.UUID|str, end: uuid.UUID|str, column_type: UniversalDataType, parent_partition: Optional[Partition] = None) -> List[Partition]:
        """Generate uuid-based partitions"""
        
        prefix_length = 8
        partitions = []
        parent_partition_id = parent_partition.partition_id if parent_partition else None
        level = parent_partition.level + 1 if parent_partition else 0
        partition_id = 0
        start_int = hex_to_int(str(start)[:prefix_length])
        end_int = hex_to_int(str(end)[:prefix_length])
        initial_partition_interval = self.config.partition_step
        cur = start_int

        #handle case for last partition. We need to create extra partition for last partition
        if end_int >= END_HEX_INT:
            end_int = END_HEX_INT+1
        while cur<end_int:
            cur1: int = ((cur+initial_partition_interval)//initial_partition_interval)*initial_partition_interval
            k1: str = int_to_hex(cur, pad_len=prefix_length)+24*"0"
            k2: str = int_to_hex(min(cur1, end_int), pad_len=prefix_length)
            if len(k2) > 8:
                k2 = 32*"f"
            else:
                k2 = k2+24*"0"
            if column_type == UniversalDataType.UUID_TEXT_DASH:
                s1 = str(uuid.UUID(k1))
                e1 = str(uuid.UUID(k2))
            elif column_type == UniversalDataType.UUID_TEXT:
                s1 = k1
                e1 = k2
            elif column_type == UniversalDataType.UUID:
                s1 = uuid.UUID(k1)
                e1 = uuid.UUID(k2)
            partitions.append(Partition(
                start=s1,
                end=e1,
                column=self.config.column,
                column_type=self.config.column_type,
                partition_step=self.config.partition_step,
                partition_id=f"{parent_partition_id}-{partition_id}" if parent_partition_id else f"{partition_id}",
                parent_partition=parent_partition,
                level=level
            ))
            cur = cur1
            partition_id += 1
        return partitions
        # genrate all boundary points for uuid using first 2 characters and club them into partitions using self.config.partition_step
        # partitions = []
        # prefixes = generate_uuid_prefixes(str(start)[0:2], str(end)[0:2], length=prefix_length)
        # for i in range(0, len(prefixes), self.config.partition_step):
        #     start_uuid = prefixes[i]+"000000-0000-0000-0000-000000000000"
        #     end_uuid = prefixes[min(i+self.config.partition_step-1, len(prefixes)-1)]+"ffffff-ffff-ffff-ffff-ffffffffffff")
        #     partitions.append(Partition(
        #         start=start_uuid,
        #         end=end_uuid,
        #         column=self.config.column,
        #         column_type=self.config.column_type,
        #         partition_step=self.config.partition_step,
        #         partition_id=self.config.name.format(pid=prefixes[i]),
        #         parent_partition=parent_partition
        #     ))
        # return partitions