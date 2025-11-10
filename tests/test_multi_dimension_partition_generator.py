"""
Test cases for composite multi-dimensional partition generator functions.

This module tests the build_multi_dimension_partitions_for_delta_data function
which creates optimized composite partitions from delta data, eliminating
unnecessary partition combinations.

KEY IMPROVEMENTS TESTED:
1. Multiple value dimensions create composite partitions instead of Cartesian products
2. Mixed range + value dimensions are optimized with composite value grouping
3. SQL generation benefits from precise tuple filtering: (col1, col2) IN (...)
4. Dramatic reduction in partition count for sparse data combinations

EXAMPLES:
- Old: 3 users × 4 products × 2 statuses = 24 partitions (most empty)
- New: 1 composite partition with 4 actual combinations
- Reduction: 6x improvement in partition efficiency
"""

import pytest
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass

from synctool.core.models import (
    DimensionPartitionConfig, 
    MultiDimensionPartition, 
    DimensionPartition,
    CompositeDimensionPartition
)
from synctool.core.schema_models import UniversalDataType
from synctool.utils.multi_dimensional_partition_generator import build_multi_dimension_partitions_for_delta_data


class TestBuildMultiDimensionPartitionsForDeltaData:
    """Test cases for build_multi_dimension_partitions_for_delta_data function."""

    def test_basic_integer_range_partitions(self):
        """Test basic integer range partitioning with simple data."""
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 2  # Two different ranges: [0-100) and [100-200)
        
        # Check that partitions have correct structure
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "user_id"
            assert partition.level == 0
            assert partition.parent_partition is None
    
    def test_basic_integer_value_partitions(self):
        """Test basic integer value partitioning creates composite partitions."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=-1,
                data_type=UniversalDataType.INTEGER,
                type="value"
            )
        ]
        
        data = [
            {"user_id": 50, "name": "Alice"},
            {"user_id": 150, "name": "Bob"},
            {"user_id": 75, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1  # Single composite partition with all unique values
        
        # Check that partition has correct structure
        partition = partitions[0]
        assert isinstance(partition, MultiDimensionPartition)
        
        # Should have composite dimensions instead of regular dimensions for value-only case
        assert len(partition.composite_dimensions) == 1
        assert len(partition.dimensions) == 0  # No regular dimensions
        
        composite_dim = partition.composite_dimensions[0]
        assert isinstance(composite_dim, CompositeDimensionPartition)
        assert composite_dim.columns == ["user_id"]
        assert composite_dim.is_value_based
        assert len(composite_dim.value_tuples) == 3  # Three unique user_id values
        assert set(composite_dim.value_tuples) == {(50,), (150,), (75,)}

    def test_datetime_range_partitions(self):
        """Test datetime range partitioning with daily steps."""
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 2  # Two different days
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "created_at"
            assert isinstance(partition.dimensions[0].start, datetime)
            assert isinstance(partition.dimensions[0].end, datetime)

    def test_value_type_partitions(self):
        """Test value-type partitioning creates composite partitions with step limits."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=1,  # Max 1 tuple per partition
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # With step=1, should create 3 partitions (one for each unique status value)
        assert len(partitions) == 3
        
        # Collect all status values across partitions
        all_status_tuples = set()
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.composite_dimensions) == 1
            assert len(partition.dimensions) == 0
            
            composite_dim = partition.composite_dimensions[0]
            assert composite_dim.columns == ["status"]
            assert composite_dim.is_value_based
            assert len(composite_dim.value_tuples) == 1  # Max 1 tuple per partition due to step=1
            
            all_status_tuples.update(composite_dim.value_tuples)
        
        # Verify all unique status values are covered
        assert all_status_tuples == {("active",), ("inactive",), ("pending",)}

    def test_mixed_dimension_types(self):
        """Test mixed dimension types (range + value) with step limits."""
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
                step=1,  # Max 1 tuple per partition
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # With step=1 for status, each range group will have separate partitions for each status value
        # Range [0-100): active, inactive -> 2 partitions  
        # Range [100-200): active, inactive -> 2 partitions
        # Total: 4 partitions
        assert len(partitions) == 4
        
        # Group partitions by range
        range_groups = {}
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1  # Range dimension
            assert len(partition.composite_dimensions) == 1  # Composite value dimension
            
            # Check range dimension
            range_start = partition.dimensions[0].start
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[0].type == "range"
            
            # Check composite dimension
            composite_dim = partition.composite_dimensions[0]
            assert composite_dim.columns == ["status"]
            assert composite_dim.is_value_based
            assert len(composite_dim.value_tuples) == 1  # Max 1 tuple per partition due to step=1
            
            if range_start not in range_groups:
                range_groups[range_start] = []
            range_groups[range_start].extend(composite_dim.value_tuples)
        
        # Verify we have both range groups
        assert set(range_groups.keys()) == {0, 100}
        
        # Verify each range group has both status values
        for range_start, status_tuples in range_groups.items():
            assert set(status_tuples) == {("active",), ("inactive",)}

    def test_multiple_value_dimensions_composite_optimization(self):
        """Test the key improvement: multiple value dimensions create composite partitions."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=-1,
                data_type=UniversalDataType.INTEGER,
                type="value"
            ),
            DimensionPartitionConfig(
                column="product_id",
                step=-1,
                data_type=UniversalDataType.INTEGER,
                type="value"
            ),
            DimensionPartitionConfig(
                column="status",
                step=-1,
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        # Data with specific combinations (not all possible combinations exist)
        data = [
            {"user_id": 1, "product_id": 2, "status": "active", "name": "Alice"},
            {"user_id": 3, "product_id": 1, "status": "pending", "name": "Bob"},
            {"user_id": 5, "product_id": 4, "status": "active", "name": "Charlie"},
            {"user_id": 1, "product_id": 3, "status": "active", "name": "David"},  # user_id=1 with different product
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # OLD BEHAVIOR would create: 3 users × 4 products × 2 statuses = 24 partitions (most empty!)
        # NEW BEHAVIOR: Single composite partition with only actual combinations
        assert len(partitions) == 1
        
        partition = partitions[0]
        assert isinstance(partition, MultiDimensionPartition)
        assert len(partition.dimensions) == 0  # No range dimensions
        assert len(partition.composite_dimensions) == 1  # Single composite dimension
        
        composite_dim = partition.composite_dimensions[0]
        assert composite_dim.columns == ["user_id", "product_id", "status"]
        assert composite_dim.is_value_based
        assert len(composite_dim.value_tuples) == 4  # Only the 4 actual combinations
        
        expected_tuples = {
            (1, 2, "active"),
            (3, 1, "pending"), 
            (5, 4, "active"),
            (1, 3, "active")
        }
        assert set(composite_dim.value_tuples) == expected_tuples

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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 0

    def test_single_row_data(self):
        """Test behavior with single row of data."""
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1
        partition = partitions[0]
        assert isinstance(partition, MultiDimensionPartition)
        assert len(partition.dimensions) == 1
        assert partition.dimensions[0].column == "user_id"

    def test_duplicate_values_same_partition(self):
        """Test that duplicate values in the same partition are handled correctly."""
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
            {"user_id": 75, "name": "Bob"},
            {"user_id": 60, "name": "Charlie"},  # All in same range [0-100)
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1  # All values fall in the same range
        partition = partitions[0]
        assert isinstance(partition, MultiDimensionPartition)
        assert len(partition.dimensions) == 1
        assert partition.dimensions[0].column == "user_id"

    def test_date_range_partitions(self):
        """Test date (not datetime) range partitioning."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="event_date",
                step=7,  # Weekly partitions
                step_unit="day",
                data_type=UniversalDataType.DATE,
                type="range"
            )
        ]
        
        base_date = date(2023, 1, 1)  # Sunday
        data = [
            {"event_date": base_date, "event": "A"},
            {"event_date": base_date + timedelta(days=3), "event": "B"},  # Same week
            {"event_date": base_date + timedelta(days=8), "event": "C"},  # Next week
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 2  # Two different weeks
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "event_date"
            assert isinstance(partition.dimensions[0].start, date)
            assert isinstance(partition.dimensions[0].end, date)

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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # Should create partitions for each combination of ranges
        assert len(partitions) >= 1
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[1].column == "score"

    def test_three_dimensions(self):
        """Test with three dimensions (2 ranges + 1 value) for complex scenarios."""
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
                step=1,
                data_type=UniversalDataType.TEXT,
                type="value"
            ),
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
            {"user_id": 50, "status": "active", "created_at": base_date, "name": "Alice"},
            {"user_id": 150, "status": "active", "created_at": base_date + timedelta(days=1), "name": "Bob"},
            {"user_id": 75, "status": "inactive", "created_at": base_date, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # NEW BEHAVIOR: Mixed range + value creates optimized partitions
        # Expected: partitions grouped by range combinations with composite value dimensions
        assert len(partitions) >= 1
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            # Should have 2 range dimensions + 1 composite dimension
            assert len(partition.dimensions) == 2  # Two range dimensions
            assert len(partition.composite_dimensions) == 1  # One composite value dimension
            
            # Check range dimensions
            range_columns = {dim.column for dim in partition.dimensions}
            assert "user_id" in range_columns
            assert "created_at" in range_columns
            
            # Check composite dimension
            composite_dim = partition.composite_dimensions[0]
            assert composite_dim.columns == ["status"]
            assert composite_dim.is_value_based

    def test_partition_properties(self):
        """Test that partition properties are set correctly."""
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
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1
        partition = partitions[0]
        
        # Check partition properties
        assert partition.level == 0
        assert partition.parent_partition is None
        assert partition.num_rows == 1  # NEW: Now correctly tracks the number of rows in the partition
        assert partition.hash is None  # Default value
        
        # Check dimension properties
        dimension = partition.dimensions[0]
        assert isinstance(dimension, DimensionPartition)
        assert dimension.column == "user_id"
        assert dimension.data_type == UniversalDataType.INTEGER

    def test_large_dataset_performance(self):
        """Test with larger dataset to check composite optimization performance with step limits."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=1000,
                data_type=UniversalDataType.INTEGER,
                type="range"
            ),
            DimensionPartitionConfig(
                column="category",
                step=1,  # Max 1 tuple per partition
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        # Create larger dataset
        data = []
        categories = ["A", "B", "C", "D", "E"]
        for i in range(1000):
            data.append({
                "user_id": i * 10,  # Spread across multiple ranges (0-9999, so 10 ranges)
                "category": categories[i % len(categories)],
                "name": f"User_{i}"
            })
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        # With step=1 for category, each range will have 5 separate partitions (one per category)
        # Expected: 10 user_id ranges × 5 categories = 50 partitions
        assert len(partitions) == 50
        
        # Group partitions by range to verify structure
        range_groups = {}
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1  # Range dimension
            assert len(partition.composite_dimensions) == 1  # Composite value dimension
            
            # Check range dimension
            range_start = partition.dimensions[0].start
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[0].type == "range"
            
            # Check composite dimension
            composite_dim = partition.composite_dimensions[0]
            assert composite_dim.columns == ["category"]
            assert composite_dim.is_value_based
            assert len(composite_dim.value_tuples) == 1  # Max 1 tuple per partition due to step=1
            
            if range_start not in range_groups:
                range_groups[range_start] = []
            range_groups[range_start].extend(composite_dim.value_tuples)
        
        # Verify we have 10 range groups
        assert len(range_groups) == 10
        
        # Verify each range group has all 5 categories
        for range_start, category_tuples in range_groups.items():
            assert len(category_tuples) == 5
            assert set(category_tuples) == {("A",), ("B",), ("C",), ("D",), ("E",)}

    def test_composite_partition_bounds_generation(self):
        """Test that composite partitions generate correct partition bounds for SQL."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="user_id",
                step=-1,
                data_type=UniversalDataType.INTEGER,
                type="value"
            ),
            DimensionPartitionConfig(
                column="product_id", 
                step=-1,
                data_type=UniversalDataType.INTEGER,
                type="value"
            )
        ]
        
        data = [
            {"user_id": 1, "product_id": 2},
            {"user_id": 3, "product_id": 1},
            {"user_id": 5, "product_id": 4},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1
        partition = partitions[0]
        
        # Get partition bounds (this would be used by the SQL generator)
        bounds = partition.get_partition_bounds()
        assert len(bounds) == 1
        
        composite_bound = bounds[0]
        assert hasattr(composite_bound, 'columns')
        assert composite_bound.columns == ["user_id", "product_id"]
        assert composite_bound.is_value_based
        assert len(composite_bound.value_tuples) == 3
        assert set(composite_bound.value_tuples) == {(1, 2), (3, 1), (5, 4)}
        
        # This would generate SQL: WHERE (user_id, product_id) IN ((1, 2), (3, 1), (5, 4))
        # Instead of: WHERE user_id IN (1, 3, 5) AND product_id IN (2, 1, 4)
        # The composite version is more precise!

    def test_step_limit_behavior(self):
        """Test that step limits properly control partition sizes."""
        # Test 1: step = -1 (unlimited)
        dimension_configs_unlimited = [
            DimensionPartitionConfig(column="user_id", step=-1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="product_id", step=-1, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        data = [
            {"user_id": 1, "product_id": 10},
            {"user_id": 2, "product_id": 20}, 
            {"user_id": 3, "product_id": 30},
            {"user_id": 4, "product_id": 40},
            {"user_id": 5, "product_id": 50},
        ]
        
        # Act - unlimited
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs_unlimited))
        partitions_unlimited = [pair[0] for pair in partition_data_pairs]
        
        # Assert - should have 1 partition with all 5 tuples
        assert len(partitions_unlimited) == 1
        composite_dim = partitions_unlimited[0].composite_dimensions[0]
        assert len(composite_dim.value_tuples) == 5
        
        # Test 2: step = 2 (max 2 tuples per partition)
        dimension_configs_limited = [
            DimensionPartitionConfig(column="user_id", step=2, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="product_id", step=2, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        # Act - limited
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs_limited))
        partitions_limited = [pair[0] for pair in partition_data_pairs]
        
        # Assert - should have 3 partitions: 2+2+1 tuples
        assert len(partitions_limited) == 3
        tuple_counts = [len(p.composite_dimensions[0].value_tuples) for p in partitions_limited]
        tuple_counts.sort()
        assert tuple_counts == [1, 2, 2]  # One partition with 1 tuple, two with 2 tuples
        
        # Test 3: Mixed step limits (min should be used)
        dimension_configs_mixed = [
            DimensionPartitionConfig(column="user_id", step=1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="product_id", step=3, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        # Act - mixed (should use min = 1)
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs_mixed))
        partitions_mixed = [pair[0] for pair in partition_data_pairs]
        
        # Assert - should have 5 partitions with 1 tuple each
        assert len(partitions_mixed) == 5
        for partition in partitions_mixed:
            assert len(partition.composite_dimensions[0].value_tuples) == 1

    def test_boundary_values(self):
        """Test behavior with boundary values."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="value",
                step=100,
                data_type=UniversalDataType.INTEGER,
                type="range"
            )
        ]
        
        data = [
            {"value": 0, "name": "Zero"},      # Start of range
            {"value": 99, "name": "End"},     # End of range
            {"value": 100, "name": "Next"},   # Start of next range
            {"value": 199, "name": "NextEnd"} # End of next range
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 2  # Two ranges: [0-100) and [100-200)
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "value"


if __name__ == "__main__":
    pytest.main([__file__])
