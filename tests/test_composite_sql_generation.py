"""
Test cases for SQL query generation with composite partitions.

This module tests that composite partitions generate optimized SQL queries
with precise tuple filtering instead of imprecise Cartesian product filtering.

KEY SQL IMPROVEMENTS TESTED:
1. Composite value partitions: (col1, col2) IN ((val1, val2), (val3, val4))
2. Mixed range + composite: Range conditions + tuple IN conditions
3. Composite range partitions: (col1, col2) >= (start1, start2) AND (col1, col2) < (end1, end2)
4. Proper parameter binding and SQL dialect handling

EXAMPLES:
- Old SQL: user_id IN (1, 3, 5) AND product_id IN (2, 1, 4) -- matches non-existent combinations!
- New SQL: (user_id, product_id) IN ((1, 2), (3, 1), (5, 4)) -- only actual combinations
"""

import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

from synctool.core.models import (
    DimensionPartitionConfig,
    MultiDimensionPartition,
    CompositeDimensionPartition,
    CompositeDimensionConfig,
    Column
)
from synctool.core.schema_models import UniversalDataType
from synctool.core.query_models import Filter, Query, Table, Field
from synctool.utils.multi_dimensional_partition_generator import build_multi_dimension_partitions_for_delta_data
from synctool.utils.sql_builder import SqlBuilder
def create_test_query_from_partition(partition: MultiDimensionPartition) -> Query:
    """Create a test query from a partition for SQL generation testing"""
    # Create base query
    table = Table(table="test_table", alias="t")
    query = Query(
        table=table,
        select=[Field(expr="*")],
        filters=[]
    )
    
    # Add filters based on partition bounds
    partition_bounds = partition.get_partition_bounds()
    for bound in partition_bounds:
        if hasattr(bound, 'columns'):  # CompositePartitionBound (can be single or multi-column)
            if bound.type == "composite_values":
                if len(bound.columns) == 1:
                    # Single column composite - treat as regular IN filter
                    values = [t[0] for t in bound.value_tuples]  # Extract single values from tuples
                    filter_obj = Filter(
                        column=bound.columns[0],
                        operator="IN",
                        value=values
                    )
                else:
                    # Multi-column composite - use composite handling
                    filter_obj = Filter(
                        column="",  # Not used for composite
                        operator="IN",
                        value=bound.value_tuples,
                        columns=bound.columns,
                        composite_bound=bound
                    )
                query.filters.append(filter_obj)
            elif bound.type == "composite_range":
                # Create composite range filter
                filter_obj = Filter(
                    column="",  # Not used for composite
                    operator="COMPOSITE_RANGE",
                    value=None,
                    columns=bound.columns,
                    start_values=bound.start_values,
                    end_values=bound.end_values,
                    composite_bound=bound
                )
                query.filters.append(filter_obj)
        else:  # Regular PartitionBound
            if bound.type == "range":
                # Add range filters
                query.filters.extend([
                    Filter(column=bound.column, operator=">=", value=bound.start),
                    Filter(column=bound.column, operator="<", value=bound.end)
                ])
            elif bound.type == "value":
                query.filters.append(
                    Filter(column=bound.column, operator="IN", value=bound.value)
                )
    
    return query


class TestCompositeSQLGeneration:
    """Test cases for composite partition SQL generation."""

    def test_composite_value_partition_sql_generation(self):
        """Test SQL generation for composite value partitions: (col1, col2) IN (...)"""
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
            {"user_id": 1, "product_id": 2, "name": "Alice"},
            {"user_id": 3, "product_id": 1, "name": "Bob"},
            {"user_id": 5, "product_id": 4, "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert partition structure
        assert len(partitions) == 1
        partition = partitions[0]
        assert len(partition.composite_dimensions) == 1
        
        # Generate SQL using direct query creation
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Verify SQL structure
        assert "(user_id, product_id) IN" in sql
        assert "($1, $2), ($3, $4), ($5, $6)" in sql or "($1, $2), ($5, $6), ($3, $4)" in sql
        
        # Verify parameters (order may vary)
        assert len(params) == 6
        param_pairs = [(params[i], params[i+1]) for i in range(0, len(params), 2)]
        expected_pairs = {(1, 2), (3, 1), (5, 4)}
        assert set(param_pairs) == expected_pairs

    def test_mixed_range_composite_sql_generation(self):
        """Test SQL generation for mixed range + composite value partitions with unlimited step"""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="created_at",
                step=1,
                step_unit="hour",
                data_type=UniversalDataType.DATETIME,
                type="range"
            ),
            DimensionPartitionConfig(
                column="user_id",
                step=-1,  # Unlimited
                data_type=UniversalDataType.INTEGER,
                type="value"
            ),
            DimensionPartitionConfig(
                column="status",
                step=-1,  # Unlimited
                data_type=UniversalDataType.VARCHAR,
                type="value"
            )
        ]
        
        base_time = datetime(2023, 1, 1, 10, 0, 0)
        data = [
            {"user_id": 1, "status": "active", "created_at": base_time + timedelta(minutes=15)},
            {"user_id": 2, "status": "pending", "created_at": base_time + timedelta(minutes=30)},
            {"user_id": 3, "status": "active", "created_at": base_time + timedelta(minutes=45)},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert partition structure
        assert len(partitions) == 1  # All in same hour, unlimited step allows single partition
        partition = partitions[0]
        assert len(partition.dimensions) == 1  # Range dimension
        assert len(partition.composite_dimensions) == 1  # Composite value dimension
        
        # Generate SQL
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Verify mixed SQL structure
        assert "created_at >=" in sql  # Range condition
        assert "created_at <" in sql   # Range condition
        assert "(user_id, status) IN" in sql  # Composite condition
        
        # Should have 2 range params + 6 composite params (3 tuples × 2 values each)
        assert len(params) == 8

    def test_single_column_composite_sql_generation(self):
        """Test that single value column creates composite partition with single-element tuples"""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(
                column="status",
                step=-1,
                data_type=UniversalDataType.VARCHAR,
                type="value"
            )
        ]
        
        data = [
            {"status": "active", "name": "Alice"},
            {"status": "pending", "name": "Bob"},
            {"status": "active", "name": "Charlie"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert
        assert len(partitions) == 1
        partition = partitions[0]
        assert len(partition.composite_dimensions) == 1
        
        composite_dim = partition.composite_dimensions[0]
        assert len(composite_dim.value_tuples) == 2  # 'active' and 'pending'
        
        # Generate SQL
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Should generate status IN ($1, $2) for single-column composite
        assert "status IN" in sql
        assert len(params) == 2
        assert set(params) == {"active", "pending"}

    def test_sql_injection_prevention(self):
        """Test that composite SQL generation properly prevents SQL injection"""
        # Arrange with potentially malicious data
        dimension_configs = [
            DimensionPartitionConfig(column="user_id", step=-1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="status", step=-1, data_type=UniversalDataType.VARCHAR, type="value")
        ]
        
        # Data with SQL injection attempts
        data = [
            {"user_id": 1, "status": "'; DROP TABLE users; --"},
            {"user_id": 2, "status": "active' OR '1'='1"},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        partition = partitions[0]
        
        # Generate SQL
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Assert
        # SQL should use parameterized queries, not string interpolation
        assert "DROP TABLE" not in sql
        assert "OR '1'='1" not in sql
        assert "(user_id, status) IN" in sql
        assert "$1, $2" in sql and "$3, $4" in sql
        
        # Malicious content should be safely in parameters
        assert "'; DROP TABLE users; --" in params
        assert "active' OR '1'='1" in params

    def test_sql_precision_improvement_demonstration(self):
        """Demonstrate the SQL precision improvement: composite vs separate IN clauses"""
        # Arrange - create data where not all combinations exist
        dimension_configs = [
            DimensionPartitionConfig(column="user_id", step=-1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="product_id", step=-1, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        # Only 3 actual combinations out of 6 possible (2 users × 3 products)
        data = [
            {"user_id": 1, "product_id": 10},  # User 1 has products 10, 30
            {"user_id": 1, "product_id": 30},  
            {"user_id": 2, "product_id": 20},  # User 2 has only product 20
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        partition = partitions[0]
        
        # Generate composite SQL
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Assert - Demonstrate precision improvement
        print(f"\n=== SQL PRECISION IMPROVEMENT DEMO ===")
        print(f"OLD APPROACH (imprecise): WHERE user_id IN (1, 2) AND product_id IN (10, 20, 30)")
        print(f"  - Would match 6 combinations: (1,10), (1,20), (1,30), (2,10), (2,20), (2,30)")
        print(f"  - But only 3 combinations actually exist in data!")
        print(f"")
        print(f"NEW APPROACH (precise): {sql.replace(chr(10), ' ')}")
        print(f"  - Matches exactly 3 combinations: {set([(params[i], params[i+1]) for i in range(0, len(params), 2)])}")
        print(f"  - 50% reduction in false matches!")
        
        # Verify the precise SQL
        assert "(user_id, product_id) IN" in sql
        assert len(params) == 6  # 3 tuples × 2 values each
        actual_combinations = set([(params[i], params[i+1]) for i in range(0, len(params), 2)])
        expected_combinations = {(1, 10), (1, 30), (2, 20)}
        assert actual_combinations == expected_combinations

    def test_performance_comparison_demonstration(self):
        """Demonstrate performance improvement with many sparse combinations"""
        # Arrange - simulate a scenario with many potential but few actual combinations
        dimension_configs = [
            DimensionPartitionConfig(column="user_id", step=-1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="category_id", step=-1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="region_id", step=-1, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        # Only 5 actual combinations out of 60 possible (3 users × 4 categories × 5 regions)
        data = [
            {"user_id": 1, "category_id": 10, "region_id": 100},
            {"user_id": 1, "category_id": 20, "region_id": 100},  # Same user, region; different category
            {"user_id": 2, "category_id": 10, "region_id": 200},  # Different user, region; same category  
            {"user_id": 3, "category_id": 30, "region_id": 300},
            {"user_id": 3, "category_id": 40, "region_id": 300},  # Same user, region; different category
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        partition = partitions[0]
        
        query = create_test_query_from_partition(partition)
        sql, params = SqlBuilder.build(query, dialect='asyncpg')
        
        # Assert - Demonstrate performance improvement
        print(f"\n=== PERFORMANCE IMPROVEMENT DEMO ===")
        print(f"POTENTIAL COMBINATIONS: 3 users × 4 categories × 5 regions = 60 combinations")
        print(f"ACTUAL COMBINATIONS: {len(data)} combinations")
        print(f"EFFICIENCY GAIN: {((60 - len(data)) / 60) * 100:.1f}% reduction in false matches")
        print(f"")
        print(f"COMPOSITE SQL: (user_id, category_id, region_id) IN (...)")
        print(f"PARAMETERS: {len(params)} values (exactly {len(data)} tuples)")
        
        # Verify the efficient SQL
        assert "(user_id, category_id, region_id) IN" in sql
        assert len(params) == 15  # 5 tuples × 3 values each
        
        # Verify all actual combinations are present
        actual_tuples = []
        for i in range(0, len(params), 3):
            actual_tuples.append((params[i], params[i+1], params[i+2]))
        
        expected_tuples = [
            (1, 10, 100), (1, 20, 100), (2, 10, 200), (3, 30, 300), (3, 40, 300)
        ]
        assert set(actual_tuples) == set(expected_tuples)

    def test_step_limit_sql_generation(self):
        """Test SQL generation with step limits creating multiple partitions"""
        # Arrange - step=1 will create multiple partitions
        dimension_configs = [
            DimensionPartitionConfig(column="user_id", step=1, data_type=UniversalDataType.INTEGER, type="value"),
            DimensionPartitionConfig(column="product_id", step=1, data_type=UniversalDataType.INTEGER, type="value")
        ]
        
        data = [
            {"user_id": 1, "product_id": 10},
            {"user_id": 2, "product_id": 20},
            {"user_id": 3, "product_id": 30},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert - should create 3 partitions (one per tuple due to step=1)
        assert len(partitions) == 3
        
        # Generate SQL for each partition
        all_tuples = set()
        for partition in partitions:
            assert len(partition.composite_dimensions) == 1
            composite_dim = partition.composite_dimensions[0]
            assert len(composite_dim.value_tuples) == 1  # Max 1 tuple per partition
            
            query = create_test_query_from_partition(partition)
            sql, params = SqlBuilder.build(query, dialect='asyncpg')
            
            # Each partition should generate simple SQL with single tuple
            assert "(user_id, product_id) IN" in sql
            assert len(params) == 2  # Single tuple = 2 parameters
            
            # Collect the tuple
            tuple_value = (params[0], params[1])
            all_tuples.add(tuple_value)
        
        # Verify all original tuples are covered across partitions
        assert all_tuples == {(1, 10), (2, 20), (3, 30)}
        
        print(f"\n=== STEP LIMIT SQL DEMO ===")
        print(f"With step=1: {len(partitions)} partitions, each with 1 tuple")
        print(f"Each generates: (user_id, product_id) IN (($1, $2))")
        print(f"Prevents overly long IN clauses while maintaining precision!")

    def test_mixed_step_limit_sql_generation(self):
        """Test SQL generation with mixed step limits (range + limited value)"""
        # Arrange
        dimension_configs = [
            DimensionPartitionConfig(column="created_at", step=1, step_unit="hour", data_type=UniversalDataType.DATETIME, type="range"),
            DimensionPartitionConfig(column="user_id", step=2, data_type=UniversalDataType.INTEGER, type="value"),  # Max 2 tuples
            DimensionPartitionConfig(column="status", step=2, data_type=UniversalDataType.VARCHAR, type="value")   # Max 2 tuples
        ]
        
        base_time = datetime(2023, 1, 1, 10, 0, 0)
        data = [
            {"user_id": 1, "status": "active", "created_at": base_time + timedelta(minutes=15)},
            {"user_id": 2, "status": "pending", "created_at": base_time + timedelta(minutes=30)},
            {"user_id": 3, "status": "active", "created_at": base_time + timedelta(minutes=45)},
            {"user_id": 4, "status": "completed", "created_at": base_time + timedelta(minutes=50)},
            {"user_id": 5, "status": "cancelled", "created_at": base_time + timedelta(minutes=55)},
        ]
        
        # Act
        partition_data_pairs = list(build_multi_dimension_partitions_for_delta_data(data, dimension_configs))
        partitions = [pair[0] for pair in partition_data_pairs]
        
        # Assert - should create multiple partitions due to step=2 limit
        assert len(partitions) >= 2  # At least 2 partitions due to step limits
        
        total_tuples = 0
        for partition in partitions:
            assert len(partition.dimensions) == 1  # Range dimension
            assert len(partition.composite_dimensions) == 1  # Composite value dimension
            
            composite_dim = partition.composite_dimensions[0]
            assert len(composite_dim.value_tuples) <= 2  # Max 2 tuples per partition due to step=2
            total_tuples += len(composite_dim.value_tuples)
            
            # Generate SQL
            query = create_test_query_from_partition(partition)
            sql, params = SqlBuilder.build(query, dialect='asyncpg')
            
            # Should have range conditions + composite IN
            assert "created_at >=" in sql
            assert "created_at <" in sql
            assert "(user_id, status) IN" in sql
            
            # Parameters: 2 for range + (2 × tuple_count) for composite
            expected_param_count = 2 + (len(composite_dim.value_tuples) * 2)
            assert len(params) == expected_param_count
        
        # Verify all 5 unique combinations are covered across partitions
        assert total_tuples == 5


if __name__ == "__main__":
    pytest.main([__file__])
