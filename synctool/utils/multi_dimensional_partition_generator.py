import asyncio
import itertools
from typing import List, Dict, Any, Tuple, Optional, Union, Generator
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import OrderedDict
import math

from ..core.models import StrategyConfig, PartitionDimensionConfig, MultiDimensionalPartition, DimensionPartition
from ..core.schema_models import UniversalDataType
from ..core.column_mapper import ColumnSchema
from .partition_generator import PartitionGenerator, PartitionConfig, calculate_offset


# @dataclass
# class MultiDimensionalPartitionConfig:
#     """Configuration for multi-dimensional partitioning"""
#     dimensions: List[PartitionDimensionConfig]
#     column_schema: ColumnSchema = None

def build_partitions(partitions: List[Dict[str, Any]], partition_dimensions: List[PartitionDimensionConfig], parent_partition: Optional[MultiDimensionalPartition]) -> List[MultiDimensionalPartition]:
    multi_dim_partitions = []
    parent_id_len = len(parent_partition.dimensions)
    parent_dimesion_map = {dimension.column: dimension for dimension in parent_partition.dimensions}

    generators = {}
    for dimension in partition_dimensions:
        generators[dimension.column] = PartitionGenerator(PartitionConfig(
            name="temp",
            column=dimension.column,
            column_type=dimension.dtype,
            step=dimension.step,
            step_unit=dimension.step_unit
        ))
    
    for partition in partitions:
        dims = []
        child_ids = partition["partition_id"].split("-")[parent_id_len:]
        for index, dimension in enumerate(partition_dimensions):
            child_id = child_ids[index]
            parent_dimension = parent_dimesion_map.get(dimension.column)
            generator = generators.get(dimension.column)
            dim = generator.build_dimension_partition(child_id, parent_dimension)
            dims.append(dim)
        multi_dim_partition = MultiDimensionalPartition(
            dimensions=dims,
            parent_partition=parent_partition,
            level=parent_partition.level+1
        )
        multi_dim_partitions.append(multi_dim_partition)
    return multi_dim_partitions




class MultiDimensionalPartitionGenerator:
    """Generate multi-dimensional partitions with recursive subdivision"""
    
    def __init__(self, dimensions: List[PartitionDimensionConfig]):
        self.dimensions = dimensions
        # self.column_schema = config.column_schema
    
    # async def generate_all_partitions(self, sync_bounds: List[Dict[str, Any]], parent_partition: Optional[MultiDimensionalPartition] = None) -> List[MultiDimensionalPartition]:
    #     """Generate all partition combinations recursively"""
    #     # Step 1: Generate primary partitions (combinations of all primary dimensions)
    #     # import pdb; pdb.set_trace()
    #     # sync_bounds_map = {bound["column"]: (bound["start"], bound["end"]) for bound in sync_bounds}
    #     primary_partitions = await self._generate_primary_partitions(sync_bounds, bounded=bounded)
        
    #     # Step 2: If secondary partitions are configured, subdivide each primary partition
    #     # if self.config.secondary_dimensions:
    #     #     all_partitions = []
    #     #     for primary_partition in primary_partitions:
    #     #         secondary_partitions = await self._generate_secondary_partitions(primary_partition)
    #     #         all_partitions.extend(secondary_partitions)
    #     #    return all_partitions
        
    #     return primary_partitions
    
    def generate_all_partitions(self, sync_bounds: List[Dict[str, Any]], parent_partition: Optional[MultiDimensionalPartition] = None) -> Generator[MultiDimensionalPartition, None, None]:
        """Generate primary partition combinations"""
        # Generate partitions for each dimension separately
        # import pdb; pdb.set_trace()
        dimension_partitions = {}
        sync_bounds_map = {bound["column"]: bound for bound in sync_bounds}
        for dimension in self.dimensions:
            column = dimension.column
            sync_bound = sync_bounds_map.get(column,{})
            bounded = sync_bound.get("bounded", False)
            start, end = sync_bound.get("start"), sync_bound.get("end")
            
            # Generate partitions for this dimension
            partitions = list(self._generate_dimension_partitions(
                dimension, start, end,  bounded=bounded
            ))
            dimension_partitions[column] = partitions
        
        # Create all combinations of dimension partitions
        yield from self._create_partition_combinations(dimension_partitions, level=0, parent_partition=parent_partition)
    
    # async def _generate_secondary_partitions(self, primary_partition: MultiDimensionalPartition) -> List[MultiDimensionalPartition]:
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
    #         column_type = column_info.dtype if column_info else dimension.column_type
            
    #         # Generate partitions for this dimension within the primary bounds
    #         partitions = await self._generate_dimension_partitions(
    #             dimension, start, end, column_type
    #         )
    #         dimension_partitions[column] = partitions
        
    #     # Create all combinations of secondary dimension partitions
    #     secondary_combinations = self._create_partition_combinations(
    #         dimension_partitions, 
    #         level=primary_partition.level + 1,
    #         parent_partition=primary_partition
    #     )
        
    #     return secondary_combinations
    
    def _generate_dimension_partitions(self, dimension: PartitionDimensionConfig, 
                                           start: Any, end: Any, 
                                            bounded: bool = False) -> Generator[DimensionPartition, None, None]:
        """Generate partitions for a single dimension"""
        config = PartitionConfig(
            name="temp",
            column=dimension.column,
            column_type=dimension.dtype,
            step=dimension.step,
            step_unit=dimension.step_unit
        )
        generator = PartitionGenerator(config)
        # step = self._parse_step(dimension.step, column_type)
        yield from generator.generate_partitions(start, end, bounded=bounded)
        
        # if column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
        #     return await self._generate_datetime_dimension_partitions(start, end, step)
        # elif column_type == UniversalDataType.INTEGER:
        #     return await self._generate_integer_dimension_partitions(start, end, step)
        # elif column_type in (UniversalDataType.UUID, UniversalDataType.UUID_TEXT, UniversalDataType.UUID_TEXT_DASH):
        #     return await self._generate_uuid_dimension_partitions(start, end, step, column_type)
        # else:
        #     raise ValueError(f"Unsupported column type for partitioning: {column_type}")
    
    # def _parse_step(self, step: Union[int, str], column_type: str) -> int:
    #     """Parse step value based on column type"""
    #     # import pdb; pdb.set_trace()
    #     if isinstance(step, int):
    #         return step
            
    #     if column_type in (UniversalDataType.DATETIME, UniversalDataType.TIMESTAMP):
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
    #                                             column_type: str) -> List[Tuple[Any, Any]]:
    #     """Generate UUID-based partitions for a dimension"""
    #     # Use the existing UUID partition logic from PartitionGenerator
    #     config = PartitionConfig(
    #         name="temp",
    #         column="temp",
    #         column_type=column_type,
    #         partition_step=step
    #     )
    #     generator = PartitionGenerator(config)
        
    #     # Generate single-dimension partitions and extract bounds
    #     single_partitions = await generator._generate_uuid_partitions(start, end, column_type)
    #     return [(p.start, p.end) for p in single_partitions]
    
    def _create_partition_combinations(self, dimension_partitions: Dict[str, List[DimensionPartition]], 
                                     level: int = 0,
                                     parent_partition: Optional[MultiDimensionalPartition] = None) -> Generator[MultiDimensionalPartition, None, None]:
        """Create all combinations of dimension partitions"""
        if not dimension_partitions:
            return
        
        # Get dimension names in the order specified by primary_partition_config
        # Use the order from self.config.dimensions to maintain consistency
        columns = [dim.column for dim in self.dimensions if dim.column in dimension_partitions]

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
            partition = MultiDimensionalPartition(
                dimensions=dimensions,
                # partition_id=partition_id,
                parent_partition=parent_partition,
                level=level
            )
            yield partition


# def convert_to_legacy_partitions(multi_dim_partitions: List[MultiDimensionalPartition], 
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
#                 column_type="",  # Will be filled by caller
#                 partition_id=md_partition.partition_id,
#                 level=md_partition.level,
#                 num_rows=md_partition.num_rows,
#                 hash=md_partition.hash
#             )
            
#             # Store multi-dimensional info in a custom attribute
#             legacy_partition._multi_dimensional_partition = md_partition
#             legacy_partitions.append(legacy_partition)
    
#     return legacy_partitions
