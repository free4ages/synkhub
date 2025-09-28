"""
Test cases for multi-dimensional partition generator functions.

This module tests the build_multi_dimension_partitions_for_delta_data function
which creates multi-dimensional partitions from delta data.
"""

import pytest
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
from dataclasses import dataclass

from synctool.core.models import DimensionPartitionConfig, MultiDimensionPartition, DimensionPartition
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
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
        """Test basic integer range partitioning with simple data."""
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) == 1  # Two different ranges: [0-100) and [100-200)
        
        # Check that partitions have correct structure
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "user_id"
            assert partition.level == 0
            assert partition.parent_partition is None
            assert sorted(partition.dimensions[0].value) == sorted([50, 150, 75])

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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) == 2  # Two different days
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "created_at"
            assert isinstance(partition.dimensions[0].start, datetime)
            assert isinstance(partition.dimensions[0].end, datetime)

    def test_value_type_partitions(self):
        """Test value-type partitioning (non-range)."""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=1,
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        # Should group by status values, so expect 3 partitions (active, inactive, pending)
        assert len(partitions) == 3
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "status"

    def test_mixed_dimension_types(self):
        """Test mixed dimension types (range + value)."""
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
            )
        ]
        
        data = [
            {"user_id": 50, "status": "active", "name": "Alice"},
            {"user_id": 150, "status": "active", "name": "Bob"},
            {"user_id": 75, "status": "inactive", "name": "Charlie"},
            {"user_id": 175, "status": "inactive", "name": "David"},
        ]
        
        # Act
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        # Should create partitions for each combination of range and value
        # Range: [0-100), [100-200), [100-200)
        # Status: active, active, inactive, inactive
        # Combinations: (0-100,active), (100-200,active), (0-100,inactive), (100-200,inactive)
        assert len(partitions) == 4
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[1].column == "status"

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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        # Should create partitions for each combination of ranges
        assert len(partitions) >= 1
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[1].column == "score"

    def test_three_dimensions(self):
        """Test with three dimensions for more complex scenarios."""
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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) >= 1
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 3
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[1].column == "status"
            assert partition.dimensions[2].column == "created_at"

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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) == 1
        partition = partitions[0]
        
        # Check partition properties
        assert partition.level == 0
        assert partition.parent_partition is None
        assert partition.num_rows == 0  # Default value
        assert partition.hash is None  # Default value
        
        # Check dimension properties
        dimension = partition.dimensions[0]
        assert isinstance(dimension, DimensionPartition)
        assert dimension.column == "user_id"
        assert dimension.data_type == UniversalDataType.INTEGER

    def test_large_dataset_performance(self):
        """Test with larger dataset to check performance characteristics."""
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
                step=1,
                data_type=UniversalDataType.TEXT,
                type="value"
            )
        ]
        
        # Create larger dataset
        data = []
        categories = ["A", "B", "C", "D", "E"]
        for i in range(1000):
            data.append({
                "user_id": i * 10,  # Spread across multiple ranges
                "category": categories[i % len(categories)],
                "name": f"User_{i}"
            })
        
        # Act
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) > 0
        
        # Verify all partitions have correct structure
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 2
            assert partition.dimensions[0].column == "user_id"
            assert partition.dimensions[1].column == "category"

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
        partitions = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        
        # Assert
        assert len(partitions) == 2  # Two ranges: [0-100) and [100-200)
        
        for partition in partitions:
            assert isinstance(partition, MultiDimensionPartition)
            assert len(partition.dimensions) == 1
            assert partition.dimensions[0].column == "value"


if __name__ == "__main__":
    pytest.main([__file__])
