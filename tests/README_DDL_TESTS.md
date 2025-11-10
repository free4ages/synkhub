# DDL Generation Tests

Comprehensive test suite for DDL generation functionality across PostgreSQL, MySQL, and StarRocks.

## Test Coverage

### 1. PostgreSQL Tests (`TestPostgresDatastoreDDL`)
- ✅ Type mapping: Universal → PostgreSQL
- ✅ Type mapping: PostgreSQL → Universal
- ✅ CREATE TABLE DDL generation
- ✅ CREATE TABLE IF NOT EXISTS
- ✅ ALTER TABLE DDL generation (ADD, MODIFY, DROP columns)

### 2. MySQL Tests (`TestMySQLDatastoreDDL`)
- ✅ Type mapping: Universal → MySQL
- ✅ Type mapping: MySQL → Universal
- ✅ CREATE TABLE DDL generation with AUTO_INCREMENT
- ✅ ALTER TABLE DDL generation

### 3. StarRocks Tests (`TestStarRocksDatastoreDDL`)
- ✅ Type mapping: Universal → StarRocks
- ✅ Type mapping: StarRocks → Universal
- ✅ CREATE TABLE DDL with ENGINE=OLAP and distribution
- ✅ Special handling for STRING type

### 4. SchemaManager Tests (`TestSchemaManager`)
- ✅ Column to schema conversion
- ✅ Schema comparison: no existing table
- ✅ Schema comparison: no changes needed
- ✅ Schema comparison: add columns
- ✅ Schema comparison with datastore type mapping
- ✅ Real type change detection (BIGINT vs INTEGER)
- ✅ Equivalent type handling (TIMESTAMP vs DATETIME → TIMESTAMP)
- ✅ End-to-end table creation
- ✅ End-to-end no changes scenario

## Running the Tests

### Run all DDL tests
```bash
pytest tests/test_ddl_generation.py -v
```

### Run specific test class
```bash
# PostgreSQL tests only
pytest tests/test_ddl_generation.py::TestPostgresDatastoreDDL -v

# MySQL tests only
pytest tests/test_ddl_generation.py::TestMySQLDatastoreDDL -v

# StarRocks tests only
pytest tests/test_ddl_generation.py::TestStarRocksDatastoreDDL -v

# SchemaManager tests only
pytest tests/test_ddl_generation.py::TestSchemaManager -v
```

### Run specific test
```bash
pytest tests/test_ddl_generation.py::TestPostgresDatastoreDDL::test_generate_create_table_ddl_postgres -v
```

### Run with coverage
```bash
pytest tests/test_ddl_generation.py --cov=synctool.utils.schema_manager --cov=synctool.datastore --cov-report=html
```

### Run with output
```bash
pytest tests/test_ddl_generation.py -v -s
```

## Key Test Scenarios

### 1. Type Equivalence Testing
Tests that TIMESTAMP and DATETIME both map to PostgreSQL's TIMESTAMP:
```python
test_compare_schemas_with_datastore_impl()
```

This ensures that changing from TIMESTAMP to DATETIME in config doesn't generate unnecessary ALTER statements.

### 2. Real Type Change Detection
Tests that genuine type changes (e.g., BIGINT → INTEGER) are properly detected:
```python
test_compare_schemas_real_type_change()
```

### 3. Database-Specific DDL
Each database has unique DDL syntax:
- **PostgreSQL**: Separate ALTER COLUMN statements for type and nullable
- **MySQL**: MODIFY COLUMN with AUTO_INCREMENT support
- **StarRocks**: ENGINE=OLAP with DISTRIBUTED BY HASH

## Expected Test Results

All tests should pass. If any fail, check:
1. Database type mappings in datastore implementations
2. DDL generation syntax
3. Schema comparison logic

## Integration with CI/CD

Add to your CI pipeline:
```yaml
- name: Run DDL Tests
  run: |
    pytest tests/test_ddl_generation.py -v --junit-xml=test-results/ddl-tests.xml
```

## Test Data

Tests use realistic column definitions:
- Integer IDs (primary keys)
- VARCHAR user names
- TIMESTAMP/DATETIME created_at fields
- DECIMAL/FLOAT amounts

## Mocking Strategy

Tests use:
- ✅ Real datastore implementations (no mocking) for type mapping
- ✅ Mocked database connections (no actual DB required)
- ✅ AsyncMock for async operations

This ensures tests are fast and don't require real database instances.

