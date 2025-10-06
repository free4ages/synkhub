"""
Composite Partition Generator for efficient multi-dimensional partitioning
that avoids unnecessary partition combinations.
"""
from typing import List, Dict, Any, Generator, Optional, Tuple
from ..core.models import (
    DimensionPartitionConfig, 
    CompositeDimensionConfig, 
    CompositeDimensionPartition, 
    MultiDimensionPartition,
    DimensionPartition
)
from .partition_generator import build_dimension_range_from_value


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


def build_multi_dimension_partitions_for_delta_data_composite(
    data: List[Dict[str, Any]], 
    dimension_configs: List[DimensionPartitionConfig]
) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """
    Build partitions using composite approach to avoid unnecessary combinations.
    This is the main replacement for the flawed build_multi_dimension_partitions_for_delta_data.
    """
    
    if not data or not dimension_configs:
        return
    
    # Separate range and value dimensions
    range_configs = [d for d in dimension_configs if d.type == "range"]
    value_configs = [d for d in dimension_configs if d.type == "value"]
    
    if not value_configs:
        # Only range dimensions - create single partition with all ranges
        yield from _build_range_only_partitions(data, range_configs)
        return
    
    if not range_configs:
        # Only value dimensions - create single composite partition with all unique combinations
        yield from _build_value_only_partitions(data, value_configs)
        return
    
    # Mixed range and value dimensions - group by ranges, then create composite values within each group
    yield from _build_mixed_partitions(data, range_configs, value_configs)


def _build_range_only_partitions(
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
            composite_dimensions=[],
            level=0,
            num_rows=len(group_rows)
        )
        yield (partition, group_rows)


def _build_value_only_partitions(
    data: List[Dict[str, Any]], 
    value_configs: List[DimensionPartitionConfig]
) -> Generator[Tuple[MultiDimensionPartition, List[Dict[str, Any]]], None, None]:
    """Handle case where all dimensions are value-based with step-based partitioning"""
    
    # Collect all unique value combinations
    unique_combinations = set()
    for row in data:
        combo = tuple(row[dim.column] for dim in value_configs)
        unique_combinations.add(combo)
    
    # Determine maximum tuples per partition based on minimum step across dimensions
    # step = -1 means unlimited, step > 0 means max step tuples per partition
    max_tuples_per_partition = _get_max_tuples_per_partition(value_configs)
    
    # Group unique combinations into partitions based on step limit
    unique_combinations_list = list(unique_combinations)
    
    if max_tuples_per_partition == -1:
        # Unlimited - single partition with all combinations
        partition_groups = [unique_combinations_list]
    else:
        # Split into chunks based on step limit
        partition_groups = []
        for i in range(0, len(unique_combinations_list), max_tuples_per_partition):
            partition_groups.append(unique_combinations_list[i:i + max_tuples_per_partition])
    
    # Create partitions for each group
    for partition_idx, combination_group in enumerate(partition_groups):
        # Convert to CompositeDimensionPartition
        composite_configs = [
            CompositeDimensionConfig(
                column=dim.column,
                data_type=dim.data_type,
                type="value",
                step=dim.step
            ) for dim in value_configs
        ]
        
        composite_partition = CompositeDimensionPartition(
            dimensions=composite_configs,
            value_tuples=combination_group,
            partition_id=f"composite_values_{partition_idx}"
        )
        
        # Find all data rows that match this partition's combinations
        partition_data = []
        combination_set = set(combination_group)
        for row in data:
            row_combo = tuple(row[dim.column] for dim in value_configs)
            if row_combo in combination_set:
                partition_data.append(row)
        
        partition = MultiDimensionPartition(
            dimensions=[],
            composite_dimensions=[composite_partition],
            level=0,
            num_rows=len(partition_data)
        )
        yield (partition, partition_data)


def _build_mixed_partitions(
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
        unique_value_combinations = set()
        for row in group_rows:
            value_combo = tuple(row[dim.column] for dim in value_configs)
            unique_value_combinations.add(value_combo)
        
        # Create composite value partitions respecting step limits
        if unique_value_combinations:
            max_tuples_per_partition = _get_max_tuples_per_partition(value_configs)
            unique_combinations_list = list(unique_value_combinations)
            
            if max_tuples_per_partition == -1:
                # Unlimited - single composite partition
                combination_groups = [unique_combinations_list]
            else:
                # Split into chunks based on step limit
                combination_groups = []
                for i in range(0, len(unique_combinations_list), max_tuples_per_partition):
                    combination_groups.append(unique_combinations_list[i:i + max_tuples_per_partition])
            
            # Create a partition for each combination group
            for group_idx, combination_group in enumerate(combination_groups):
                composite_configs = [
                    CompositeDimensionConfig(
                        column=dim.column,
                        data_type=dim.data_type,
                        type="value",
                        step=dim.step
                    ) for dim in value_configs
                ]
                
                composite_partition = CompositeDimensionPartition(
                    dimensions=composite_configs,
                    value_tuples=combination_group,
                    partition_id=f"composite_{hash(range_key)}_{group_idx}"
                )
                
                # Find all rows in this range group that match this combination group
                combination_set = set(combination_group)
                partition_rows = []
                for row in group_rows:
                    row_value_combo = tuple(row[dim.column] for dim in value_configs)
                    if row_value_combo in combination_set:
                        partition_rows.append(row)
                
                partition = MultiDimensionPartition(
                    dimensions=range_dimensions,
                    composite_dimensions=[composite_partition],
                    level=0,
                    num_rows=len(partition_rows)
                )
                yield (partition, partition_rows)


# This function was removed as it's not used anywhere in the codebase.
# The mixed type logic is handled directly in _build_mixed_partitions() above.


# Note: Utility functions were removed as they were not being used anywhere in the codebase.
# The core functionality is provided by build_multi_dimension_partitions_for_delta_data_composite() above.
