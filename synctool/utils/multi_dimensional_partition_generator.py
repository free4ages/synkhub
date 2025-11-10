import asyncio
import itertools
from typing import List, Dict, Any, Tuple, Optional, Union, Generator
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import OrderedDict
import math

from ..core.models import StrategyConfig, DimensionPartitionConfig, MultiDimensionPartition, DimensionPartition, PartitionBound
from ..core.schema_models import UniversalDataType
from ..core.column_mapper import ColumnSchema
from .partition_generator import  (
    build_dimension_partition_from_id, 
    build_dimension_range_from_value, 
    generate_dimension_partitions
)


# @dataclass
# class MultiDimensionalPartitionConfig:
#     """Configuration for multi-dimensional partitioning"""
#     dimensions: List[DimensionPartitionConfig]
#     column_schema: ColumnSchema = None

def build_multi_dimension_child_partitions(partitions: List[Dict[str, Any]], partition_dimensions: List[DimensionPartitionConfig], parent_partition: Optional[MultiDimensionPartition]) -> List[MultiDimensionPartition]:
    multi_dim_partitions = []
    parent_id_len = len(parent_partition.dimensions)
    parent_dimesion_map = {dimension.column: dimension for dimension in parent_partition.dimensions}

    # generators = {}
    # for dimension in partition_dimensions:
    #     generators[dimension.column] = PartitionGenerator(PartitionConfig(
    #         name="temp",
    #         column=dimension.column,
    #         data_type=dimension.data_type,
    #         step=dimension.step,
    #         step_unit=dimension.step_unit
    #     ))
    
    for partition in partitions:
        dims = []
        child_ids = partition["partition_id"].split("-")[parent_id_len:]
        for index, dimension in enumerate(partition_dimensions):
            child_id = child_ids[index]
            parent_dimension = parent_dimesion_map.get(dimension.column)
            # generator = generators.get(dimension.column)
            dim = build_dimension_partition_from_id(dimension, child_id, parent_dimension)
            dims.append(dim)
        multi_dim_partition = MultiDimensionPartition(
            dimensions=dims,
            parent_partition=parent_partition,
            level=parent_partition.level+1,
            num_rows=partition["num_rows"],
            count_diff=partition["count_diff"]
        )
        multi_dim_partitions.append(multi_dim_partition)
    return multi_dim_partitions

def build_multi_dimension_partitions_for_delta_data(data: List[Dict[str, Any]], dimension_configs: List[DimensionPartitionConfig]) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """
    DEPRECATED: This function creates unnecessary partitions when multiple value dimensions are used.
    Use build_multi_dimension_partitions_for_delta_data_composite instead.
    
    Now returns tuples of (partition, corresponding_data) instead of just partitions.
    """
    # if only single value dimensions are present, or two dimension with different type should use simple generator
    
    range_configs = [d for d in dimension_configs if d.type == "range"]
    value_configs = [d for d in dimension_configs if d.type == "value"]
    
    if len(value_configs) <= 1 and len(range_configs) <= 1:
        yield from build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs)
        return
    
    # Import the new composite generator
    from .composite_partition_generator import build_multi_dimension_partitions_for_delta_data_composite
    # Use the new composite logic instead
    yield from build_multi_dimension_partitions_for_delta_data_composite(data, dimension_configs)


def build_multi_dimension_partitions_for_delta_data_simple(data: List[Dict[str, Any]], dimension_configs: List[DimensionPartitionConfig]) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """
    Refactored implementation that returns both partition and corresponding data.
    Supports dimension_config types: value, range, or combinations of (range, value).
    Returns MultiDimensionPartition with dimensions attribute of type DimensionPartition.
    """
    if not data or not dimension_configs:
        return
    
    # Separate range and value dimensions
    range_configs = [d for d in dimension_configs if d.type == "range"]
    value_configs = [d for d in dimension_configs if d.type == "value"]
    
    if not value_configs:
        # Only range dimensions - create partitions with range bounds
        yield from _build_range_only_partitions_simple(data, range_configs)
        return
    
    if not range_configs:
        # Only value dimensions - create partitions with discrete values
        yield from _build_value_only_partitions_simple(data, value_configs)
        return
    
    # Mixed range and value dimensions - group by ranges, then handle values within each group
    yield from _build_mixed_partitions_simple(data, range_configs, value_configs)


def _build_range_only_partitions_simple(
    data: List[Dict[str, Any]], 
    range_configs: List[DimensionPartitionConfig]
) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """Handle case where all dimensions are range-based"""
    
    # Group data by range combinations
    range_groups = {}
    
    for row in data:
        range_key = []
        for range_config in range_configs:
            range_value = build_dimension_range_from_value(row[range_config.column], range_config)
            range_key.append(range_value)
        range_key = tuple(range_key)
        
        if range_key not in range_groups:
            range_groups[range_key] = []
        range_groups[range_key].append(row)
    
    # Create partitions for each range group
    for range_key, group_rows in range_groups.items():
        range_dimensions = []
        for i, range_config in enumerate(range_configs):
            start, end = range_key[i]
            range_dimensions.append(DimensionPartition(
                start=start,
                end=end,
                column=range_config.column,
                data_type=range_config.data_type,
                type="range",
                partition_id=f"r{i}"
            ))
        
        partition = MultiDimensionPartition(
            dimensions=range_dimensions,
            level=0,
            num_rows=len(group_rows)
        )
        yield (partition, group_rows)


def _build_value_only_partitions_simple(
    data: List[Dict[str, Any]], 
    value_configs: List[DimensionPartitionConfig]
) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """Handle case where all dimensions are value-based"""
    
    # Collect all unique value combinations and group data by them
    value_groups = {}
    
    for row in data:
        value_key = tuple(row[dim.column] for dim in value_configs)
        if value_key not in value_groups:
            value_groups[value_key] = []
        value_groups[value_key].append(row)
    
    # Determine maximum tuples per partition based on minimum step across dimensions
    max_tuples_per_partition = _get_max_tuples_per_partition(value_configs)
    
    # Group unique combinations into partitions based on step limit
    unique_combinations = list(value_groups.keys())
    
    if max_tuples_per_partition == -1:
        # Unlimited - single partition with all combinations
        partition_groups = [unique_combinations]
    else:
        # Split into chunks based on step limit
        partition_groups = []
        for i in range(0, len(unique_combinations), max_tuples_per_partition):
            partition_groups.append(unique_combinations[i:i + max_tuples_per_partition])
    
    # Create partitions for each group
    for partition_idx, combination_group in enumerate(partition_groups):
        # Create DimensionPartition objects for value dimensions
        value_dimensions = []
        for dim_idx, value_config in enumerate(value_configs):
            # Extract unique values for this dimension from the combination group
            dim_values = list(set(combo[dim_idx] for combo in combination_group))
            
            # Create a value-type DimensionPartition
            value_dimensions.append(DimensionPartition(
                start=None,
                end=None,
                column=value_config.column,
                data_type=value_config.data_type,
                type="value",
                value=dim_values,  # Store the discrete values
                partition_id=f"v{dim_idx}_{partition_idx}"
            ))
        
        # Find all data rows that match this partition's combinations
        partition_data = []
        combination_set = set(combination_group)
        for row in data:
            row_combo = tuple(row[dim.column] for dim in value_configs)
            if row_combo in combination_set:
                partition_data.append(row)
        
        partition = MultiDimensionPartition(
            dimensions=value_dimensions,
            level=0,
            num_rows=len(partition_data)
        )
        yield (partition, partition_data)


def _build_mixed_partitions_simple(
    data: List[Dict[str, Any]], 
    range_configs: List[DimensionPartitionConfig],
    value_configs: List[DimensionPartitionConfig]
) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """Handle mixed range and value dimensions"""
    
    # Group by range dimensions first
    range_groups = {}
    
    for row in data:
        # Build range key
        range_key = []
        for range_config in range_configs:
            range_value = build_dimension_range_from_value(row[range_config.column], range_config)
            range_key.append(range_value)
        range_key = tuple(range_key)
        
        if range_key not in range_groups:
            range_groups[range_key] = []
        range_groups[range_key].append(row)
    
    # Create partitions for each range group
    for range_key, group_rows in range_groups.items():
        # Create range dimensions
        range_dimensions = []
        for i, range_config in enumerate(range_configs):
            start, end = range_key[i]
            range_dimensions.append(DimensionPartition(
                start=start,
                end=end,
                column=range_config.column,
                data_type=range_config.data_type,
                type="range",
                partition_id=f"r{i}"
            ))
        
        # Collect unique value combinations within this range group
        value_groups = {}
        for row in group_rows:
            value_combo = tuple(row[dim.column] for dim in value_configs)
            if value_combo not in value_groups:
                value_groups[value_combo] = []
            value_groups[value_combo].append(row)
        
        # Create value partitions respecting step limits
        if value_groups:
            max_tuples_per_partition = _get_max_tuples_per_partition(value_configs)
            unique_combinations = list(value_groups.keys())
            
            if max_tuples_per_partition == -1:
                # Unlimited - single partition
                combination_groups = [unique_combinations]
            else:
                # Split into chunks based on step limit
                combination_groups = []
                for i in range(0, len(unique_combinations), max_tuples_per_partition):
                    combination_groups.append(unique_combinations[i:i + max_tuples_per_partition])
            
            # Create a partition for each combination group
            for group_idx, combination_group in enumerate(combination_groups):
                # Create value dimensions
                value_dimensions = []
                for dim_idx, value_config in enumerate(value_configs):
                    # Extract unique values for this dimension from the combination group
                    dim_values = list(set(combo[dim_idx] for combo in combination_group))
                    
                    value_dimensions.append(DimensionPartition(
                        start=None,
                        end=None,
                        column=value_config.column,
                        data_type=value_config.data_type,
                        type="value",
                        value=dim_values,
                        partition_id=f"v{dim_idx}_{group_idx}"
                    ))
                
                # Combine range and value dimensions
                all_dimensions = range_dimensions + value_dimensions
                
                # Find all rows in this range group that match this combination group
                combination_set = set(combination_group)
                partition_rows = []
                for row in group_rows:
                    row_value_combo = tuple(row[dim.column] for dim in value_configs)
                    if row_value_combo in combination_set:
                        partition_rows.append(row)
                
                partition = MultiDimensionPartition(
                    dimensions=all_dimensions,
                    level=0,
                    num_rows=len(partition_rows)
                )
                yield (partition, partition_rows)


def _get_max_tuples_per_partition(dimension_configs: List[DimensionPartitionConfig]) -> int:
    """
    Determine the maximum number of tuples per partition based on dimension step configurations.
    
    Rules:
    - step = -1 means unlimited tuples per partition
    - step > 0 means max 'step' tuples per partition
    - For multiple dimensions, use the minimum step to prevent long IN queries
    
    Returns:
    - -1 for unlimited
    - positive integer for the maximum tuples per partition
    """
    steps = [dim.step for dim in dimension_configs if dim.step != -1]
    
    if not steps:
        # All dimensions have step = -1, so unlimited
        return -1
    
    # Use minimum step to be most restrictive (prevent very long IN queries)
    return min(steps)


# class MultiDimensionalPartitionGenerator:
#     """Generate multi-dimensional partitions with recursive subdivision"""
    
#     def __init__(self, dimensions: List[DimensionPartitionConfig]):
#         self.dimensions = dimensions
#         # self.column_schema = config.column_schema
    
    # async def generate_all_partitions(self, partition_bounds: List[Dict[str, Any]], parent_partition: Optional[MultiDimensionPartition] = None) -> List[MultiDimensionPartition]:
    #     """Generate all partition combinations recursively"""
    #     # Step 1: Generate primary partitions (combinations of all primary dimensions)
    #     # import pdb; pdb.set_trace()
    #     # partition_bounds_map = {bound["column"]: (bound["start"], bound["end"]) for bound in partition_bounds}
    #     primary_partitions = await self._generate_primary_partitions(partition_bounds, bounded=bounded)
        
    #     # Step 2: If secondary partitions are configured, subdivide each primary partition
    #     # if self.config.secondary_dimensions:
    #     #     all_partitions = []
    #     #     for primary_partition in primary_partitions:
    #     #         secondary_partitions = await self._generate_secondary_partitions(primary_partition)
    #     #         all_partitions.extend(secondary_partitions)
    #     #    return all_partitions
        
    #     return primary_partitions
    
def generate_multi_dimension_partitions_from_partition_bounds(partition_bounds: List[PartitionBound], dimension_configs: List[DimensionPartitionConfig], parent_partition: Optional[MultiDimensionPartition] = None) -> Generator[MultiDimensionPartition, None, None]:
    """Generate primary partition combinations"""
    # Generate partitions for each dimension separately
    # import pdb; pdb.set_trace()
    dimension_partitions = {}
    partition_bounds_map = {bound.column: bound for bound in partition_bounds}
    for dimension_config in dimension_configs:
        column = dimension_config.column
        if column not in partition_bounds_map:
            partition_bound = PartitionBound(column=column, data_type=None, start=None, end=None, bounded=False)
        else:
            partition_bound = partition_bounds_map.get(column)
        # partition_bound = partition_bounds_map.get(column,PartitionBound(column=column, start=None, end=None, bounded=False))
        if partition_bound.type == "range":
            bounded = partition_bound.bounded
            start, end = partition_bound.start, partition_bound.end
        elif partition_bound.type == "value":
            bounded = partition_bound.bounded
            start, end = partition_bound.value, None
        
        # Generate partitions for this dimension
        partitions = list(_generate_dimension_partitions(
            dimension_config, start, end,  bounded=bounded
        ))
        dimension_partitions[column] = partitions
    # import pdb; pdb.set_trace()
    # Create all combinations of dimension partitions
    yield from _create_partition_combinations(dimension_partitions, dimension_configs, level=0, parent_partition=parent_partition)

# async def _generate_secondary_partitions(self, primary_partition: MultiDimensionPartition) -> List[MultiDimensionPartition]:
#     """Generate secondary partitions within a primary partition"""
#     # Generate partitions for each secondary dimension within the primary bounds
#     dimension_partitions = OrderedDict()
    
#     for dimension in self.config.secondary_dimensions:
#         column = dimension.column
        
#         # Get bounds from primary partition
#         if not primary_partition.has_column(column):
#             continue
            
#         start, end = primary_partition.get_bounds_for_column(column)
        
#         # Get column type from schema
#         column_info = self.column_schema.column(column) if self.column_schema else None
#         data_type = column_info.data_type if column_info else dimension.data_type
        
#         # Generate partitions for this dimension within the primary bounds
#         partitions = await self._generate_dimension_partitions(
#             dimension, start, end, data_type
#         )
#         dimension_partitions[column] = partitions
    
#     # Create all combinations of secondary dimension partitions
#     secondary_combinations = self._create_partition_combinations(
#         dimension_partitions, 
#         level=primary_partition.level + 1,
#         parent_partition=primary_partition
#     )
    
#     return secondary_combinations

def _generate_dimension_partitions(dimension: DimensionPartitionConfig, 
                                        start: Any, end: Any, 
                                        bounded: bool = False) -> Generator[DimensionPartition, None, None]:
    """Generate partitions for a single dimension"""
    # config = PartitionConfig(
    #     name="temp",
    #     column=dimension.column,
    #     data_type=dimension.data_type,
    #     step=dimension.step,
    #     step_unit=dimension.step_unit,
    #     type=dimension.type,
    #     # lower_bound=dimension.lower_bound,
    #     # upper_bound=dimension.upper_bound
    # )
    # generator = PartitionGenerator(config)
    # step = self._parse_step(dimension.step, data_type)
    yield from generate_dimension_partitions(dimension, start, end, bounded=bounded)
    
    # if data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
    #     return await self._generate_datetime_dimension_partitions(start, end, step)
    # elif data_type == UniversalDataType.INTEGER:
    #     return await self._generate_integer_dimension_partitions(start, end, step)
    # elif data_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
    #     return await self._generate_uuid_dimension_partitions(start, end, step, data_type)
    # else:
    #     raise ValueError(f"Unsupported column type for partitioning: {data_type}")

# def _parse_step(self, step: Union[int, str], data_type: str) -> int:
#     """Parse step value based on column type"""
#     # import pdb; pdb.set_trace()
#     if isinstance(step, int):
#         return step
        
#     if data_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
#         # Parse time-based steps
#         time_steps = {
#             'daily': 24 * 60 * 60,
#             'weekly': 7 * 24 * 60 * 60,
#             'monthly': 30 * 24 * 60 * 60,  # Approximate
#             'yearly': 365 * 24 * 60 * 60   # Approximate
#         }
#         return time_steps[step.lower()]
    
#     return int(step)

# async def _generate_datetime_dimension_partitions(self, start: datetime, end: datetime, 
#                                                 step_seconds: int) -> List[Tuple[datetime, datetime]]:
#     """Generate datetime-based partitions for a dimension"""
#     partitions = []
#     current = start
    
#     while current < end:
#         next_boundary = current + timedelta(seconds=step_seconds)
#         partition_end = min(next_boundary, end)
#         partitions.append((current, partition_end))
#         current = next_boundary
        
#     return partitions

# async def _generate_integer_dimension_partitions(self, start: int, end: int, 
#                                                step: int) -> List[Tuple[int, int]]:
#     """Generate integer-based partitions for a dimension"""
#     partitions = []
#     current = start
    
#     while current < end:
#         partition_end = min(current + step, end)
#         partitions.append((current, partition_end))
#         current += step
        
#     return partitions

# async def _generate_uuid_dimension_partitions(self, start: Any, end: Any, step: int,
#                                             data_type: str) -> List[Tuple[Any, Any]]:
#     """Generate UUID-based partitions for a dimension"""
#     # Use the existing UUID partition logic from PartitionGenerator
#     config = PartitionConfig(
#         name="temp",
#         column="temp",
#         data_type=data_type,
#         partition_step=step
#     )
#     generator = PartitionGenerator(config)
    
#     # Generate single-dimension partitions and extract bounds
#     single_partitions = await generator._generate_uuid_partitions(start, end, data_type)
#     return [(p.start, p.end) for p in single_partitions]

def _create_partition_combinations(dimension_partitions: Dict[str, List[DimensionPartition]], dimension_configs: List[DimensionPartitionConfig], 
                                    level: int = 0,
                                    parent_partition: Optional[MultiDimensionPartition] = None) -> Generator[MultiDimensionPartition, None, None]:
    """Create all combinations of dimension partitions"""
    if not dimension_partitions:
        return
    
    # Get dimension names in the order specified by primary_partition_config
    # Use the order from self.config.dimensions to maintain consistency
    columns = [dim.column for dim in dimension_configs if dim.column in dimension_partitions]

    partition_lists = [dimension_partitions[col] for col in columns]
    
    # Generate all combinations using itertools.product
    for combination in itertools.product(*partition_lists):
        # Create list of DimensionPartition objects for this combination
        dimensions = []
        # partition_id_parts = []
        
        for col, dim_partition in zip(columns, combination):
            dimensions.append(dim_partition)
            # partition_id_parts.append(dim_partition.partition_id or "0")
        
        # # Create partition ID by combining all dimension partition IDs with hyphens
        # if parent_partition:
        #     partition_id = f"{parent_partition.partition_id}-{'-'.join(partition_id_parts)}"
        # else:
        #     partition_id = '-'.join(partition_id_parts)
        
        # Create multi-dimensional partition
        partition = MultiDimensionPartition(
            dimensions=dimensions,
            # partition_id=partition_id,
            parent_partition=parent_partition,
            level=level
        )
        yield partition


# def convert_to_legacy_partitions(multi_dim_partitions: List[MultiDimensionPartition], 
#                                primary_column: str) -> List['Partition']:
#     """Convert multi-dimensional partitions to legacy Partition objects for backward compatibility"""
#     from ..core.models import Partition

#     legacy_partitions = []
#     for md_partition in multi_dim_partitions:
#         if md_partition.has_column(primary_column):
#             start, end = md_partition.get_bounds_for_column(primary_column)
        
#             # Create legacy partition focusing on the primary column
#             legacy_partition = Partition(
#                 start=start,
#                 end=end,
#                 column=primary_column,
#                 data_type="",  # Will be filled by caller
#                 partition_id=md_partition.partition_id,
#                 level=md_partition.level,
#                 num_rows=md_partition.num_rows,
#                 hash=md_partition.hash
#             )
        
#             # Store multi-dimensional info in a custom attribute
#             legacy_partition._multi_dimensional_partition = md_partition
#             legacy_partitions.append(legacy_partition)

#     return legacy_partitions
