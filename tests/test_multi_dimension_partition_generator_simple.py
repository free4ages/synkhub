"""
Test cases for the refactored build_multi_dimension_partitions_for_delta_data_simple function.

This module tests the simple multi-dimensional partition generator that:
1. Returns both partition and corresponding data as tuples
2. Uses DimensionPartition objects (not CompositeDimensionPartition)
3. Supports value, range, and mixed (value+range) dimension configurations
4. Handles step-based partitioning for value dimensions

The simple version is designed for straightforward cases where we need individual
DimensionPartition objects rather than composite partitions.
"""

import pytest
from datetime import datetime, date, timedelta
from typing import List, Dict, Any

from synctool.core.models import (
    DimensionPartitionConfig, 
    MultiDimensionPartition, 
    DimensionPartition
)
from synctool.core.schema_models import UniversalDataType
from synctool.utils.multi_dimensional_partition_generator import build_multi_dimension_partitions_for_delta_data_simple


class TestBuildMultiDimensionPartitionsForDeltaDataSimple:
    """Test cases for build_multi_dimension_partitions_for_delta_data_simple function."""

    def test_empty_data(self):
        """Test behavior with empty data."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            )
        ]
        data = []
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 0

    def test_empty_dimension_configs(self):
        """Test behavior with empty dimension configs."""
        # Arrange
        dimension_configs = []
        data = [{"user_id": 50, "name": "Alice"}]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 0

    # VALUE-ONLY DIMENSION TESTS
    
    def test_single_value_dimension_unlimited_step(self):
        """Test single value dimension with unlimited step (-1)."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=-1,  # Unlimited
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"status": "active", "name": "Alice"},
            {"status": "inactive", "name": "Bob"},
            {"status": "active", "name": "Charlie"},
            {"status": "pending", "name": "David"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 1  # Single partition with all values
        
        partition, partition_data = partition_data_pairs[0]
        assert isinstance(partition, MultiDimensionPartition)
        assert len(partition.dimensions) == 1
        assert len(partition.composite_dimensions) == 0  # Simple version uses DimensionPartition, not composite
        
        dimension = partition.dimensions[0]
        assert isinstance(dimension, DimensionPartition)
        assert dimension.column == "status"
        assert dimension.type == "value"
        assert dimension.start is None
        assert dimension.end is None
        assert set(dimension.value) == {"active", "inactive", "pending"}  # All unique values
        
        # Check that all data is included
        assert len(partition_data) == 4
        assert partition.num_rows == 4

    def test_single_value_dimension_limited_step(self):
        """Test single value dimension with step limit."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=1,  # Max 1 unique value per partition
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"status": "active", "name": "Alice"},
            {"status": "inactive", "name": "Bob"},
            {"status": "active", "name": "Charlie"},
            {"status": "pending", "name": "David"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 3  # One partition per unique status value
        
        # Collect all statuses and verify partitions
        all_statuses = set()
        total_rows = 0
        
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert len(partition.composite_dimensions) == 0
            
            dimension = partition.dimensions[0]
            assert isinstance(dimension, DimensionPartition)
            assert dimension.column == "status"
            assert dimension.type == "value"
            assert len(dimension.value) == 1  # Max 1 value per partition due to step=1
            
            status_value = dimension.value[0]
            all_statuses.add(status_value)
            
            # Check that partition data matches the status
            for row in partition_data:
                assert row["status"] == status_value
            
            total_rows += len(partition_data)
        
        # Verify all unique statuses are covered
        assert all_statuses == {"active", "inactive", "pending"}
        assert total_rows == 4  # All original data rows accounted for

    def test_multiple_value_dimensions(self):
        """Test multiple value dimensions."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=-1,  # Unlimited
                data_type=UniversalDataType.TEXT,
                type="value"
            ),
            DimensionPartitionConfig(
                column="category",
                step=-1,  # Unlimited
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"status": "active", "category": "A", "name": "Alice"},
            {"status": "inactive", "category": "B", "name": "Bob"},
            {"status": "active", "category": "A", "name": "Charlie"},
            {"status": "active", "category": "B", "name": "David"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 1  # Single partition with all combinations
        
        partition, partition_data = partition_data_pairs[0]
        assert isinstance(partition, MultiDimensionPartition)
        assert len(partition.dimensions) == 2  # Two value dimensions
        assert len(partition.composite_dimensions) == 0
        
        # Check status dimension
        status_dim = next(d for d in partition.dimensions if d.column == "status")
        assert status_dim.type == "value"
        assert set(status_dim.value) == {"active", "inactive"}
        
        # Check category dimension
        category_dim = next(d for d in partition.dimensions if d.column == "category")
        assert category_dim.type == "value"
        assert set(category_dim.value) == {"A", "B"}
        
        # Check that all data is included
        assert len(partition_data) == 4
        assert partition.num_rows == 4

    # RANGE-ONLY DIMENSION TESTS
    
    def test_single_range_dimension_integer(self):
        """Test single integer range dimension."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            )
        ]
        
        data = [
            {"user_id": 50, "name": "Alice"},
            {"user_id": 150, "name": "Bob"},
            {"user_id": 75, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 2  # Two different ranges: [0-100) and [100-200)
        
        # Check partitions
        range_groups = {}
        total_rows = 0
        
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert len(partition.composite_dimensions) == 0
            
            dimension = partition.dimensions[0]
            assert isinstance(dimension, DimensionPartition)
            assert dimension.column == "user_id"
            assert dimension.type == "range"
            assert dimension.start is not None
            assert dimension.end is not None
            
            range_key = (dimension.start, dimension.end)
            range_groups[range_key] = partition_data
            total_rows += len(partition_data)
        
        # Verify ranges and data distribution
        assert len(range_groups) == 2
        assert total_rows == 3  # All original data rows accounted for
        
        # Check specific ranges (exact values depend on partition logic)
        for (start, end), group_data in range_groups.items():
            for row in group_data:
                user_id = row["user_id"]
                assert start <= user_id < end

    def test_single_range_dimension_datetime(self):
        """Test single datetime range dimension."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="created_at",
                step=1,
                step_unit="day",
                data_type=UniversalDataType.DATETIME,
                type="range"
            )
        ]
        
        base_date = datetime(2023, 1, 1, 10, 0, 0)
        data = [
            {"created_at": base_date, "name": "Alice"},
            {"created_at": base_date + timedelta(hours=5), "name": "Bob"},
            {"created_at": base_date + timedelta(days=1, hours=2), "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 2  # Two different days
        
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert len(partition.composite_dimensions) == 0
            
            dimension = partition.dimensions[0]
            assert isinstance(dimension, DimensionPartition)
            assert dimension.column == "created_at"
            assert dimension.type == "range"
            assert isinstance(dimension.start, datetime)
            assert isinstance(dimension.end, datetime)

    def test_multiple_range_dimensions(self):
        """Test multiple range dimensions."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            ),
            DimensionPartitionConfig(
                column="score",
                step=50,
                data_type=UniversalDataType.INTEGER,
                type="range"
            )
        ]
        
        data = [
            {"user_id": 50, "score": 75, "name": "Alice"},
            {"user_id": 150, "score": 125, "name": "Bob"},
            {"user_id": 75, "score": 25, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) >= 1  # At least one partition
        
        total_rows = 0
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2  # Two range dimensions
            assert len(partition.composite_dimensions) == 0
            
            # Check user_id dimension
            user_id_dim = next(d for d in partition.dimensions if d.column == "user_id")
            assert user_id_dim.type == "range"
            assert user_id_dim.start is not None
            assert user_id_dim.end is not None
            
            # Check score dimension
            score_dim = next(d for d in partition.dimensions if d.column == "score")
            assert score_dim.type == "range"
            assert score_dim.start is not None
            assert score_dim.end is not None
            
            # Verify data falls within ranges
            for row in partition_data:
                assert user_id_dim.start <= row["user_id"] < user_id_dim.end
                assert score_dim.start <= row["score"] < score_dim.end
            
            total_rows += len(partition_data)
        
        assert total_rows == 3  # All original data rows accounted for

    # MIXED DIMENSION TESTS (VALUE + RANGE)
    
    def test_mixed_single_range_single_value(self):
        """Test mixed dimensions: one range + one value."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            ),
            DimensionPartitionConfig(
                column="status",
                step=-1,  # Unlimited
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"user_id": 50, "status": "active", "name": "Alice"},
            {"user_id": 150, "status": "active", "name": "Bob"},
            {"user_id": 75, "status": "inactive", "name": "Charlie"},
            {"user_id": 175, "status": "inactive", "name": "David"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        # Should group by range first, then create value partitions within each range
        assert len(partition_data_pairs) == 2  # Two range groups: [0-100) and [100-200)
        
        range_groups = {}
        total_rows = 0
        
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2  # One range + one value dimension
            assert len(partition.composite_dimensions) == 0
            
            # Check range dimension
            range_dim = next(d for d in partition.dimensions if d.column == "user_id")
            assert range_dim.type == "range"
            assert range_dim.start is not None
            assert range_dim.end is not None
            
            # Check value dimension
            value_dim = next(d for d in partition.dimensions if d.column == "status")
            assert value_dim.type == "value"
            assert value_dim.start is None
            assert value_dim.end is None
            assert isinstance(value_dim.value, list)
            
            range_key = (range_dim.start, range_dim.end)
            range_groups[range_key] = {
                'statuses': set(value_dim.value),
                'data': partition_data
            }
            
            total_rows += len(partition_data)
        
        # Verify we have both range groups
        assert len(range_groups) == 2
        assert total_rows == 4  # All original data rows accounted for
        
        # Verify each range group contains the correct statuses
        for (start, end), group_info in range_groups.items():
            for row in group_info['data']:
                user_id = row["user_id"]
                assert start <= user_id < end
                assert row["status"] in group_info['statuses']

    def test_mixed_range_value_with_step_limit(self):
        """Test mixed dimensions with step limits on value dimension."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            ),
            DimensionPartitionConfig(
                column="status",
                step=1,  # Max 1 value per partition
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"user_id": 50, "status": "active", "name": "Alice"},
            {"user_id": 150, "status": "active", "name": "Bob"},
            {"user_id": 75, "status": "inactive", "name": "Charlie"},
            {"user_id": 175, "status": "inactive", "name": "David"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        # With step=1 for status, each range group will have separate partitions for each status value
        # Range [0-100): active, inactive -> 2 partitions  
        # Range [100-200): active, inactive -> 2 partitions
        # Total: 4 partitions
        assert len(partition_data_pairs) == 4
        
        # Group partitions by range
        range_groups = {}
        total_rows = 0
        
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2  # One range + one value dimension
            assert len(partition.composite_dimensions) == 0
            
            # Check range dimension
            range_dim = next(d for d in partition.dimensions if d.column == "user_id")
            assert range_dim.type == "range"
            
            # Check value dimension
            value_dim = next(d for d in partition.dimensions if d.column == "status")
            assert value_dim.type == "value"
            assert len(value_dim.value) == 1  # Max 1 value per partition due to step=1
            
            range_start = range_dim.start
            status_value = value_dim.value[0]
            
            if range_start not in range_groups:
                range_groups[range_start] = []
            range_groups[range_start].append(status_value)
            
            total_rows += len(partition_data)
        
        # Verify we have both range groups
        assert set(range_groups.keys()) == {0, 100}
        assert total_rows == 4  # All original data rows accounted for
        
        # Verify each range group has both status values
        for range_start, status_values in range_groups.items():
            assert set(status_values) == {"active", "inactive"}

    def test_mixed_multiple_range_multiple_value(self):
        """Test complex mixed dimensions: multiple ranges + multiple values."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            ),
            DimensionPartitionConfig(
                column="created_at",
                step=1,
                step_unit="day",
                data_type=UniversalDataType.DATETIME,
                type="range"
            ),
            DimensionPartitionConfig(
                column="status",
                step=-1,  # Unlimited
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        base_date = datetime(2023, 1, 1, 10, 0, 0)
        data = [
            {"user_id": 50, "status": "active", "created_at": base_date, "name": "Alice"},
            {"user_id": 150, "status": "active", "created_at": base_date + timedelta(days=1), "name": "Bob"},
            {"user_id": 75, "status": "inactive", "created_at": base_date, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) >= 1
        
        total_rows = 0
        for partition, partition_data in partition_data_pairs:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 3  # Two range + one value dimension
            assert len(partition.composite_dimensions) == 0
            
            # Check range dimensions
            range_columns = {d.column for d in partition.dimensions if d.type == "range"}
            assert "user_id" in range_columns
            assert "created_at" in range_columns
            
            # Check value dimension
            value_dims = [d for d in partition.dimensions if d.type == "value"]
            assert len(value_dims) == 1
            assert value_dims[0].column == "status"
            
            total_rows += len(partition_data)
        
        assert total_rows == 3  # All original data rows accounted for

    def test_return_type_structure(self):
        """Test that the return type structure is correct."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            )
        ]
        
        data = [{"user_id": 50, "name": "Alice"}]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs))
        
        # Assert
        assert len(partition_data_pairs) == 1
        
        partition, partition_data = partition_data_pairs[0]
        
        # Check partition structure
        assert isinstance(partition, MultiDimensionPartition)
        assert partition.level == 0
        assert partition.parent_partition is None
        assert partition.num_rows == 1
        assert partition.hash is None  # Default value
        
        # Check that we have DimensionPartition objects, not composite
        assert len(partition.dimensions) == 1
        assert len(partition.composite_dimensions) == 0
        assert isinstance(partition.dimensions[0], DimensionPartition)
        
        # Check data structure
        assert isinstance(partition_data, list)
        assert len(partition_data) == 1
        assert partition_data[0] == {"user_id": 50, "name": "Alice"}

    def test_step_limit_behavior_value_dimensions(self):
        """Test that step limits properly control partition sizes for value dimensions."""
        # Test 1: step = -1 (unlimited)
        dimension_configs_unlimited = [
            DimensionPartitionConfig(
                column="category",
                step=-1,
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        data = [
            {"category": "A", "name": "Alice"},
            {"category": "B", "name": "Bob"},
            {"category": "C", "name": "Charlie"},
            {"category": "D", "name": "David"},
            {"category": "E", "name": "Eve"},
        ]
        
        # Act - unlimited
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs_unlimited))
        
        # Assert - should have 1 partition with all 5 values
        assert len(partition_data_pairs) == 1
        partition, partition_data = partition_data_pairs[0]
        assert len(partition.dimensions[0].value) == 5
        assert len(partition_data) == 5
        
        # Test 2: step = 2 (max 2 values per partition)
        dimension_configs_limited = [
            DimensionPartitionConfig(
                column="category",
                step=2,
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        # Act - limited
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data_simple(data, dimension_configs_limited))
        
        # Assert - should have 3 partitions: 2+2+1 values
        assert len(partition_data_pairs) == 3
        value_counts = [len(p[0].dimensions[0].value) for p in partition_data_pairs]
        value_counts.sort()
        assert value_counts == [1, 2, 2]  # One partition with 1 value, two with 2 values
        
        # Verify all data is accounted for
        total_data_rows = sum(len(p[1]) for p in partition_data_pairs)
        assert total_data_rows == 5


if __name__ == "__main__":
    pytest.main([__file__])
