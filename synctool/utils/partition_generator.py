from synctool.core.enums import DataStatus
import asyncio
from dataclasses import dataclass
import math
import itertools
import uuid
from typing import List, Any, Optional, Tuple, Union, Generator, Dict       
from datetime import datetime, timedelta, timezone, date
from ..core.models import StrategyConfig, DimensionPartition, MultiDimensionPartition, DimensionPartitionConfig
from ..core.schema_models import UniversalDataType
from .datetime_partition_generator import (
    generate_datetime_partitions, 
    calculate_offset as datetime_calculate_offset, 
    add_units as datetime_add_units,
    get_boundary as datetime_get_boundary
)
from .date_partition_generator import (
    generate_date_partitions, 
    calculate_offset as date_calculate_offset, 
    add_units as date_add_units,
    get_boundary as date_get_boundary
)
import logging

logger = logging.getLogger(__name__)

# @dataclass
# class PartitionConfig:
#     """Partition configuration"""
#     name: str
#     column: str
#     data_type: str
#     step: int
#     step_unit: str
#     type: str
#     # lower_bound: Any
#     # upper_bound: Any



def add_exclusive_range(value):
    if value is None:
        return value
    if isinstance(value, int):
        return value+1
    if isinstance(value, datetime):
        return value + timedelta(seconds=1)
    if isinstance(value, date):
        return value + timedelta(days=1)
    return value

def generate_uuid_prefixes(start: str, end: str, length: int) -> Generator[str, None, None]:
    uuid_chars = [chr(i) for i in range(ord('0'), ord('9')+1)] + [chr(i) for i in range(ord('a'), ord('f')+1)]
    #generate all possible combinations from uuid_chars of length length
    for p in itertools.product(uuid_chars, repeat=length):
        yield ''.join(p)


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





# def to_partitions(
#     partitions_data: List[dict],
#     parent_partition: MultiDimensionPartition,
# ) -> List[MultiDimensionPartition]:
#     partitions: List[MultiDimensionPartition] = []
#     start, end = parent_partition.start, parent_partition.end
#     level: int = parent_partition.level+1
#     intervals: list[int] = parent_partition.intervals
#     column: str = parent_partition.column
#     data_type: str = parent_partition.data_type
#     for partition_data in partitions_data:
#         partition_id, partition_hash, num_rows = partition_data["partition_id"], partition_data["partition_hash"], partition_data["num_rows"]
#         parts: list[int] = [int(x) for x in partition_id.split('-')]
#         assert len(parts) == level+1, f"Partition id {partition_id} has {len(parts)} parts number, expected {level+1}"
#         partition_start = 0
#         for index, part_num in enumerate(parts):
#             partition_start += part_num*intervals[index]
#         partition_end = partition_start+intervals[level]
#         if data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
#             # Convert timestamps to datetime objects with the same timezone awareness as start/end
#             if getattr(start, 'tzinfo', None) is not None:
#                 # If start is timezone-aware, create timezone-aware datetimes
#                 partition_start_dt: datetime = datetime.fromtimestamp(partition_start, start.tzinfo)
#                 partition_end_dt: datetime = datetime.fromtimestamp(partition_end, end.tzinfo)
#                 # step_size = partition_end-partition_start
#             else:
#                 # If start is timezone-naive, create timezone-naive datetimes
#                 partition_start_dt: datetime = datetime.fromtimestamp(partition_start)
#                 partition_end_dt: datetime = datetime.fromtimestamp(partition_end)
#                 # step_size = partition_end-partition_start
            
#             partition_start = max(partition_start_dt, start)
#             partition_end = min(partition_end_dt, end)

#         elif data_type == UniversalDataType.INTEGER:
#             partition_start = max(partition_start, start)
#             partition_end = min(partition_end,end)

#         elif data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
#             if partition_end >= END_HEX_INT:
#                 partition_end = END_HEX_INT
#             partition_start_str = int_to_hex(partition_start, pad_len=8)
#             partition_end_str = int_to_hex(partition_end, pad_len=8)
#             partition_start = partition_start_str+24*"0"
#             partition_end = partition_end_str+24*"0"
#             # print(partition_start, partition_end)
#             if data_type == UniversalDataType.UUID_TEXT_DASH:
#                 partition_start = str(uuid.UUID(partition_start))
#                 partition_end = str(uuid.UUID(partition_end))
#             elif data_type == UniversalDataType.UUID_TEXT:
#                 partition_start = partition_start_str
#                 partition_end = partition_end_str
#             elif data_type == UniversalDataType.UUID:
#                 partition_start = uuid.UUID(partition_start)
#                 partition_end = uuid.UUID(partition_end)
#         partition: MultiDimensionPartition = MultiDimensionPartition(
#             start=partition_start, 
#             end=partition_end,
#             # step_size=step_size,
#             level=level,
#             num_rows=num_rows, 
#             hash=partition_hash,
#             column=column,
#             data_type=data_type,
#             intervals=intervals,
#             parent_partition=parent_partition,
#             partition_id=partition_id
#         )
#         partitions.append(partition)
#     return partitions


# def calculate_partition_status_legacy(src_partitions: List[MultiDimensionPartition], snk_partitions: List[MultiDimensionPartition],skip_row_count=False) -> Tuple[List[MultiDimensionPartition], dict[tuple[Any, Any, int], str]]:
#     status: dict[tuple[Any, Any, int], str] = {}
#     all_keys: set[tuple[Any, Any, int]] = {(c.start, c.end, c.level) for c in src_partitions} | {(c.start, c.end, c.level) for c in snk_partitions}
#     src_map: dict[tuple[Any, Any, int], MultiDimensionPartition] = {(c.start, c.end, c.level): c for c in src_partitions}
#     snk_map: dict[tuple[Any, Any, int], MultiDimensionPartition] = {(c.start, c.end, c.level): c for c in snk_partitions}
#     result: list[MultiDimensionPartition] = []

#     for key in sorted(all_keys):
#         sc: MultiDimensionPartition | None = src_map.get(key)
#         kc: MultiDimensionPartition | None = snk_map.get(key)
#         sel: MultiDimensionPartition | None = sc or kc
#         if sc and kc:
#             sel = sc if sc.num_rows> kc.num_rows else kc
#         if sc and kc and (skip_row_count or sc.num_rows == kc.num_rows) and sc.hash == kc.hash:
#             status[key] = DataStatus.UNCHANGED
#         elif sc and kc:
#             status[key] = DataStatus.MODIFIED
#         elif sc and not kc:
#             status[key] = DataStatus.ADDED
#         else:
#             status[key] = DataStatus.DELETED
#         result.append(sel)

#     return result, status   

def calculate_partition_status(src_partitions: List[Dict[str, Any]], snk_partitions: List[Dict[str, Any]],skip_row_count=False) -> Tuple[Dict[str, Any], dict[str, str]]:
    status: dict[str, str] = {}
    all_keys: set[str] = {c["partition_id"] for c in src_partitions} | {c["partition_id"] for c in snk_partitions}
    src_map: dict[str, Dict[str, Any]] = {c["partition_id"]: c for c in src_partitions}
    snk_map: dict[str, Dict[str, Any]] = {c["partition_id"]: c for c in snk_partitions}
    result: list[Dict[str, Any]] = []
    
    for key in sorted(all_keys):
        sc = src_map.get(key)
        kc = snk_map.get(key)
        sel = sc or kc
        if sc and kc:
            sel = sc if sc["num_rows"]> kc["num_rows"] else kc
        if sc and kc and (skip_row_count or sc["num_rows"] == kc["num_rows"]) and sc["partition_hash"] == kc["partition_hash"]:
            status[key] = DataStatus.UNCHANGED
        elif sc and kc:
            status[key] = DataStatus.MODIFIED
        elif sc and not kc:
            status[key] = DataStatus.ADDED
        else:
            status[key] = DataStatus.DELETED
        sel["count_diff"] = sc["num_rows"] - kc["num_rows"]
        result.append(sel)
    return result, status


# def build_intervals(initial_partition_interval: int, min_partition_step: int, interval_reduction_factor: int) -> list[int]:
#     intervals: list[int] = []
#     interval: int = initial_partition_interval
#     while interval>min_partition_step:
#         intervals.append(interval)
#         interval = interval//interval_reduction_factor
#     intervals.append(interval)    
#     return intervals

# def merge_adjacent(partitions: List[MultiDimensionPartition], statuses: List[str], page_size: int) -> Tuple[List[MultiDimensionPartition], List[str]]:
#     merged_partitions, merged_statuses = [], []
#     #sort partitions,statuses by start, end, level
#     zipped = sorted(zip(partitions, statuses), key=lambda x: (x[0].start, x[0].end, x[0].level))
#     # might need to preserve the merged partition in future
#     for c, st in zipped:
#         if st in ('M', 'A') and merged_partitions and merged_statuses[-1] == st and merged_partitions[-1].num_rows+c.num_rows <= page_size:
#             logger.info(f"Merging adjacent partitions {merged_partitions[-1].start}-{merged_partitions[-1].end} and {c.start}-{c.end}")
#             prev = merged_partitions[-1]
#             prev.end = max(prev.end, c.end)
#             prev.num_rows += c.num_rows
#         else:
#             merged_partitions.append(c)
#             merged_statuses.append(st)

#     return merged_partitions, merged_statuses

# def _parse_step(step: Union[int, str], data_type: str) -> Tuple[int, str]:
#     """Parse step value based on column type"""
#     # import pdb; pdb.set_trace()   
#     if data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
#         # Parse time-based steps
#         step = step.lower()
#         if step == 'daily':
#             return 24 * 60 * 60, 'timestamp'
#         elif step == 'weekly':
#             return 7 * 24 * 60 * 60, 'timestamp'
#         elif step == 'monthly':
#             return 1, 'month'
#         elif step == 'yearly':
#             return 1, 'year'
#         elif step == 'hourly':
#             return 60 * 60, 'timestamp'

    
#     return int(step), 'default'

def calculate_offset(start: Any, unit: str, data_type: str) -> Any:
    if data_type == UniversalDataType.INTEGER:
        return start, start
    elif data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
        return datetime_calculate_offset(start, unit)
    elif data_type in (UniversalDataType.DATE):
        return date_calculate_offset(start, unit)
    elif data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
        effective_int = hex_to_int(str(start)[:8])
        return effective_int, start
    else:
        raise ValueError(f"Unsupported column type: {data_type}")

def build_dimension_range_from_value(value: Any, dimension_config: DimensionPartitionConfig) -> Any:
    if dimension_config.type == "value":
        return value
    step, step_unit = dimension_config.step, dimension_config.step_unit
    if dimension_config.type == "range":
        if dimension_config.data_type == UniversalDataType.DATETIME:
            return datetime_get_boundary(value, step, step_unit)
        elif dimension_config.data_type == UniversalDataType.DATE:
            return date_get_boundary(value, step, step_unit)
        elif dimension_config.data_type == UniversalDataType.INTEGER:
            return (value//step)*step, (value//step+1)*step
        else:
            raise ValueError(f"Unsupported column type: {dimension_config.data_type}")



    
def generate_dimension_partitions(dim_config: DimensionPartitionConfig, start: Any, end: Any, bounded:bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate partition bounds between start and end"""
    if dim_config.type == "value":
        for partition in _generate_value_partitions(dim_config, start, end, bounded=bounded):
            yield partition
    elif dim_config.type == "range":
        if dim_config.data_type == UniversalDataType.INTEGER:
            for partition in _generate_int_partitions(dim_config, start, end, bounded=bounded):
                yield partition
        elif dim_config.data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
            for partition in _generate_datetime_partitions(dim_config, start, end, bounded=bounded):
                yield partition
        elif dim_config.data_type == UniversalDataType.DATE:
            for partition in _generate_date_partitions(dim_config, start, end, bounded=bounded):
                yield partition
        elif dim_config.data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
            for partition in _generate_uuid_partitions(dim_config, start, end, bounded=bounded):
                yield partition
        else:
            raise ValueError(f"Unsupported column type: {dim_config.data_type}")
# def _generate_value_partitions(self, start: Any, end: Any, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
#     """Generate value-based partitions"""
#     if not type(start) in (list, tuple):
#         start = [start]
#     step, step_unit = self.config.step, self.config.step_unit
#     for partition in generate_value_partitions(start, end, step, step_unit, bounded):
#         yield DimensionPartition(
#             start=partition[0],
#             end=partition[1],



def build_dimension_partition_from_id(dim_config: DimensionPartitionConfig, partition_id: str, parent_partition: Optional[DimensionPartition]) -> DimensionPartition:
    step, step_unit, column, data_type = dim_config.step, dim_config.step_unit, dim_config.column, dim_config.data_type
    offset, start = calculate_offset(parent_partition.start, step_unit, data_type)
    partition_num = int(partition_id)
    if data_type == UniversalDataType.INTEGER:
        return DimensionPartition(
            start=max(start+partition_num*step, parent_partition.start),
            end=min(start+partition_num*step+step, parent_partition.end),
            column=column,
            data_type=data_type,
            step=step,
            step_unit=step_unit,
            offset=offset+partition_num,
            partition_id=f"{partition_id}",
            level=parent_partition.level+1 if parent_partition else 0
        )
    elif data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
        return DimensionPartition(
            start=max(datetime_add_units(start, partition_num, step, step_unit), parent_partition.start),
            end=min(datetime_add_units(start, partition_num+1, step, step_unit), parent_partition.end),
            column=column,
            data_type=data_type,
            step=step,
            step_unit=step_unit,
            offset=offset+partition_num,
            partition_id=f"{partition_id}",
            level=parent_partition.level+1 if parent_partition else 0
        )
    elif data_type == UniversalDataType.DATE:
        return DimensionPartition(
            start=max(date_add_units(start, partition_num, step, step_unit), parent_partition.start),
            end=min(date_add_units(start, partition_num+1, step, step_unit), parent_partition.end),
            column=column,
            data_type=data_type,
            step=step,
            step_unit=step_unit,
            offset=offset+partition_num,
            partition_id=f"{partition_id}",
            level=parent_partition.level+1 if parent_partition else 0
        )
    elif data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
        start_int = hex_to_int(str(start)[:8])
        uuid_start = int_to_hex(start_int+partition_num*step, pad_len=8)+24*"0"
        uuid_end = int_to_hex(start_int+(partition_num+1)*step, pad_len=8)+24*"0"
        if data_type == UniversalDataType.UUID_TEXT_DASH:
            uuid_start = str(uuid.UUID(uuid_start))
            uuid_end = str(uuid.UUID(uuid_end))
        elif data_type == UniversalDataType.UUID_TEXT:
            uuid_start = uuid_start
            uuid_end = uuid_end
        elif data_type == UniversalDataType.UUID:
            uuid_start = uuid.UUID(uuid_start)
            uuid_end = uuid.UUID(uuid_end)
        return DimensionPartition(
            start=uuid_start,
            end=uuid_end,
            column=column,
            data_type=data_type,
            step=step,
            step_unit=step_unit,
            offset=start_int+partition_num,
            partition_id=f"{partition_id}",
            level=parent_partition.level+1 if parent_partition else 0
        )
    else:
        raise ValueError(f"Unsupported column type: {data_type}")

def _generate_value_partitions(dim_config: DimensionPartitionConfig, start: Any, end: Any, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate value-based partitions"""
    step, step_unit = dim_config.step, dim_config.step_unit
    values = list(set(start))
    generator = [values] if step==-1 else [values[i:i+step] for i in range(0, len(values), step)]
    partition_id = 0
    for partition in generator:
        yield DimensionPartition(
            column=dim_config.column,
            data_type=dim_config.data_type,
            step=step,
            step_unit=step_unit,
            start=None,
            end=None,
            partition_id=f"{partition_id}",
            type=dim_config.type,
            value=partition,
            level=0
        )
        partition_id += 1

def _generate_int_partitions(dim_config: DimensionPartitionConfig, start: int, end: int, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate integer-based partitions"""
    step, step_unit = dim_config.step, dim_config.step_unit
    if not bounded:
        # import pdb; pdb.set_trace()
        current_start = (start // step) * step
        floored = (end // step) * step
        if floored == end:
            effective_end = end
        else:
            effective_end = floored + step
        while current_start < effective_end:
            current_end = current_start + step
            partition_id = current_start // step
            yield DimensionPartition(
                start=current_start,
                end=current_end,
                column=dim_config.column,
                data_type=dim_config.data_type,
                step=step,
                step_unit=step_unit,
                offset=current_start,
                partition_id=f"{partition_id}",
                level=0
            )
            current_start = current_end
    else:
        current_start = start
        effective_end = end  # do not ceil
        partition_id = 0
        while current_start < effective_end:
            current_end = current_start + step
            yield DimensionPartition(
                start=max(current_start, start),
                end=min(current_end, effective_end),
                column=dim_config.column,
                data_type=dim_config.data_type,
                step=step,
                step_unit=step_unit,
                offset=current_start,
                partition_id=f"{partition_id}",
                level=0
            )
            partition_id += 1
            current_start = current_end
    
    # current = (start//step)*step if not bounded else start
    # partition_id = 0 if bounded else start//step
    
    # while True:
    #     if bounded and current >= end:
    #         break
    #     elif not bounded and current > end:
    #         break
    #     partition_end = min(current + step, end) if bounded else (current + step)
    #     if partition_end == current:
    #         break
    #     yield DimensionPartition(
    #         start=max(current, start) if bounded else current,
    #         end=partition_end,
    #         column=self.config.column,
    #         data_type=self.config.data_type,
    #         step=step,
    #         step_unit=step_unit,
    #         offset=current,
    #         partition_id=f"{partition_id}",
    #         level=0
    #     )
    #     current = partition_end
    #     partition_id += 1

def _generate_date_partitions(dim_config: DimensionPartitionConfig, start: date, end: date, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate date-based partitions"""
    step, step_unit = dim_config.step, dim_config.step_unit
    for partition in generate_date_partitions(start, end, step, step_unit, bounded):
        yield DimensionPartition(
            start=partition[0],
            end=partition[1],
            column=dim_config.column,
            data_type=dim_config.data_type,
            step=step,
            step_unit=step_unit,
            offset=partition[2],
            partition_id=f"{partition[2]}",
            level=0
        )


def _generate_datetime_partitions(dim_config: DimensionPartitionConfig, start: datetime, end: datetime, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """
    Generate datetime-based partitions.
    if start and end is not bounded, following logic will be applied:
        - if step_unit is day and step>1
            - start calculation: move start to the start of the day and get the partition start date where partition is created by adding step*1 day since 1970-01-01.
            e.g if step is 15 and start is 2025-01-01 10:12:11, then partition start will be to_datetime(((number of days since 1970-01-01)//15)*15) days since 1970-01-01
            - end_calculation: if the end is not start of the day, move it to the start of the next day else keep same. Then calculate partition end date by calculating start date of this parition
            if the start date of this partition is not equal to end, the move to the end of this partition
    if start and end is bounded, following logic will be applied:
        - if step_unit is day and step>1
            - move start to the start of the day and move end to the start of the next day if it is not start of day
            then increment the start date by step*1 day until it is greater than end
        - The final partition start and end should not cross the provided start and end for bounded case

    """

    step, step_unit = dim_config.step, dim_config.step_unit
    for partition in generate_datetime_partitions(start, end, step, step_unit, bounded):
        yield DimensionPartition(
            start=partition[0],
            end=partition[1],
            column=dim_config.column,
            data_type=dim_config.data_type,
            step=step,
            step_unit=step_unit,
            offset=partition[2],
            partition_id=f"{partition[2]}",
            level=0
        )
    # yield from generate_datetime_partitions(start, end, step, step_unit, bounded)
    # start = start.replace(tzinfo=timezone.utc)
    # end = end.replace(tzinfo=timezone.utc)
    # if step_unit == 'day':
    #     if not bounded:
    #         # keep only date part
    #         step = step* 24 * 60 * 60
    #         start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
    #         end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)
    #         calculation_start = datetime.fromtimestamp((start_date.timestamp()//step)*step, tz = timezone.utc)
    #         calcula

    #     step_unit = 'timestamp'
    #     step = step* 24 * 60 * 60
    
    # if step_unit == 'timestamp':
    #     start_int  = math.floor(start.timestamp())
    #     end_int = math.ceil(end.timestamp())
    # elif step_unit == 'month':
    #     # number of months since 1970-01-01
    #     start_int = (start.year - 1970)*12 + start.month-1
    #     end_int = (end.year - 1970)*12 + end.month - 1
    # elif step_unit == 'year':
    #     # number of years since 1970-01-01
    #     start_int = start.year - 1970
    #     end_int = end.year - 1970 + 1
    

    # current = (start_int//step)*step if not bounded else start_int
    # partition_id = 0 if bounded else start_int//step
    # # import pdb; pdb.set_trace()
    # while True:
    #     # in case of primary partition end partitions determined by end are inclusive
    #     # in case of secondary partition end is exclusive
    #     if bounded and current >= end_int:
    #         break
    #     elif not bounded and current > end_int:
    #         break
        
    #     if step_unit == 'timestamp':
    #         cur1 = current + step
    #         partition_start = datetime.fromtimestamp(current, tz = timezone.utc)
    #         partition_end = datetime.fromtimestamp(cur1, tz = timezone.utc)
    #         partition_start = max(partition_start, start) if bounded else partition_start
    #         partition_end = min(partition_end, end) if bounded else partition_end
    #     elif step_unit == 'month':
    #         cur1 = current + step
    #         # calculate month start date from current since 1970-01-01 using current as number of months since 1970-01-01
    #         partition_start = datetime(1970+current//12, current%12+1, 1, tzinfo = timezone.utc)
    #         partition_end = datetime(1970+(cur1//12), cur1%12+1, 1, tzinfo = timezone.utc)
    #         partition_start = max(partition_start, start) if bounded else partition_start
    #         partition_end = min(partition_end, end) if bounded else partition_end
    #     elif step_unit == 'year':
    #         # calculate year start date from current since 1970-01-01 using current as number of years since 1970-01-01
    #         cur1 = current + step
    #         partition_start = datetime(1970+current, 1, 1, tzinfo = timezone.utc)
    #         partition_end = datetime(1970+(cur1), 1, 1, tzinfo = timezone.utc)
    #         partition_start = max(partition_start, start) if bounded else partition_start
    #         partition_end = min(partition_end, end) if bounded else partition_end
    #     if partition_start == partition_end:
    #         break
    #     yield DimensionPartition(
    #         start=partition_start,
    #         end=partition_end,
    #         column=self.config.column,
    #         data_type=self.config.data_type,
    #         step=step,
    #         step_unit=step_unit,
    #         offset=current,
    #         partition_id=f"{partition_id}",
    #         level=0
    #     )
    #     current = cur1
    #     partition_id += 1


    #     s1: datetime = datetime.fromtimestamp(cur, start.tzinfo)
    #     e1: datetime = datetime.fromtimestamp(min(cur1, ending), end.tzinfo)
    #     beginning = start.replace(month=1, day=1)
    #     ending = end.replace(month=1, day=1)
    # current = (start_timestamp//step)*step if not parent_bound else start_timestamp
    # partition_id = 0 if not parent_bound else start_timestamp//step
    
    # cur = start_timestamp
    # while cur<end_timestamp:
    #     cur1: int = ((cur+initial_partition_interval)//initial_partition_interval)*initial_partition_interval
    #     s1: datetime = datetime.fromtimestamp(cur, start.tzinfo)
    #     e1: datetime = datetime.fromtimestamp(min(cur1, end_timestamp), end.tzinfo)
    #     partitions.append(Partition(
    #         start=s1,
    #         end=e1,
    #         column=self.config.column,
    #         data_type=self.config.data_type,
    #         partition_step=self.config.partition_step,
    #         partition_id=f"{parent_partition_id}-{partition_id}" if parent_partition_id else f"{partition_id}",
    #         parent_partition=parent_partition,
    #         level=level
    #     ))
    #     cur = cur1
    #     partition_id += 1
    # return partitions

def _generate_uuid_partitions(dim_config: DimensionPartitionConfig, start: uuid.UUID|str, end: uuid.UUID|str, bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate uuid-based partitions"""
    data_type = dim_config.data_type
    if start is None:
        start = "00000000000000000000000000000000"
    if end is None:
        end = "ffffffffffffffffffffffffffffffff"
    prefix_length = 8
    partition_id = 0
    start_int = hex_to_int(str(start)[:prefix_length])
    end_int = hex_to_int(str(end)[:prefix_length])
    step, step_unit = dim_config.step, dim_config.step_unit
    cur = start_int

    #handle case for last partition. We need to create extra partition for last partition
    if end_int >= END_HEX_INT:
        end_int = END_HEX_INT+1
    while cur<end_int:
        cur1: int = ((cur+step)//step)*step
        k1: str = int_to_hex(cur, pad_len=prefix_length)+24*"0"
        k2: str = int_to_hex(min(cur1, end_int), pad_len=prefix_length)
        if len(k2) > 8:
            k2 = 32*"f"
        else:
            k2 = k2+24*"0"
        if data_type == UniversalDataType.UUID_TEXT_DASH:
            s1 = str(uuid.UUID(k1))
            e1 = str(uuid.UUID(k2))
        elif data_type == UniversalDataType.UUID_TEXT:
            s1 = k1
            e1 = k2
        elif data_type == UniversalDataType.UUID:
            s1 = uuid.UUID(k1)
            e1 = uuid.UUID(k2)
        yield DimensionPartition(
            start=s1,
            end=e1,
            column=dim_config.column,
            data_type=dim_config.data_type,
            step=step,
            step_unit=step_unit,
            offset=cur,
            partition_id=f"{partition_id}",
            level=0
        )
        cur = cur1
        partition_id += 1
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
    #         data_type=self.config.data_type,
    #         partition_step=self.config.partition_step,
    #         partition_id=self.config.name.format(pid=prefixes[i]),
    #         parent_partition=parent_partition
    #     ))
    # return partitions