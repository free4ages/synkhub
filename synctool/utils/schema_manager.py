"""
Schema management utility for automatic table creation and updates.
"""
import logging
from typing import List, Optional, Dict, Any

from ..core.models import Column, DataStore
from ..core.schema_models import UniversalSchema, UniversalColumn, UniversalDataType


class SchemaManager:
    """Manage schema operations using datastores directly"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
    
    def columns_to_universal_schema(
        self, 
        columns: List[Column], 
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None
    ) -> UniversalSchema:
        """
        Convert config Column objects to UniversalSchema.
        
        Args:
            columns: List of Column objects from config
            table_name: Target table name
            schema_name: Optional schema name
            database_name: Optional database name
            
        Returns:
            UniversalSchema object
        """
        universal_columns = []
        primary_keys = []
        
        for col in columns:
            # Skip virtual columns - they don't exist in the database
            if col.virtual:
                self.logger.debug(f"Skipping virtual column: {col.name}")
                continue
            
            # Use expr as the actual column name (expr is the real DB column, name might be an alias)
            # If expr is not provided, fallback to name
            actual_column_name = col.expr if col.expr else col.name
            
            # Determine if this should be a primary key (unique_column in sync context)
            is_pk = col.unique_column
            if is_pk:
                primary_keys.append(actual_column_name)
            
            universal_col = UniversalColumn(
                name=actual_column_name,  # Use actual DB column name
                data_type=col.data_type or UniversalDataType.TEXT,
                nullable=not is_pk,  # Primary keys shouldn't be nullable
                primary_key=is_pk,
                unique=col.unique_column,
                auto_increment=False,
                default_value=None,
                max_length=col.max_length,
                precision=col.precision,
                scale=col.scale
            )
            universal_columns.append(universal_col)
        
        return UniversalSchema(
            table_name=table_name,
            columns=universal_columns,
            schema_name=schema_name,
            database_name=database_name,
            primary_keys=primary_keys
        )
    
    async def ensure_table_schema(
        self,
        datastore: DataStore,
        columns: List[Column],
        table_name: str,
        schema_name: Optional[str] = None,
        database_name: Optional[str] = None,
        apply: bool = False,
        if_not_exists: bool = False
    ) -> Dict[str, Any]:
        """
        Ensure table schema matches config columns using datastore.
        
        Args:
            datastore: DataStore instance
            columns: List of Column objects from config
            table_name: Target table name
            schema_name: Optional schema name
            database_name: Optional database name
            apply: If True, execute DDL statements
            if_not_exists: Add IF NOT EXISTS clause for CREATE TABLE
        
        Returns:
            Dict with DDL statements and comparison results:
            {
                'table_name': str,
                'action': 'create'|'alter'|'no_change',
                'table_exists': bool,
                'changes': List[Dict],
                'ddl_statements': List[str],
                'applied': bool
            }
        """
        # Ensure datastore is connected
        if not datastore._datastore_impl:
            await datastore.connect(self.logger)
        
        datastore_impl = datastore._datastore_impl
        
        # Convert columns to universal schema
        desired_schema = self.columns_to_universal_schema(
            columns, table_name, schema_name, database_name
        )
        
        # Check if table exists
        table_exists = await datastore_impl.table_exists(table_name, schema_name)
        
        # Get existing schema if table exists
        existing_schema = None
        if table_exists:
            try:
                existing_schema = await datastore_impl.extract_table_schema(table_name, schema_name)
            except Exception as e:
                self.logger.warning(f"Could not extract existing schema: {e}")
        
        # Compare schemas (pass datastore_impl for accurate type comparison)
        comparison = self._compare_schemas(existing_schema, desired_schema, datastore_impl)
        
        # Generate DDL
        ddl_statements = []
        if comparison['action'] == 'create':
            ddl = datastore_impl.generate_create_table_ddl(
                desired_schema, 
                if_not_exists=if_not_exists
            )
            ddl_statements.append(ddl)
        elif comparison['action'] == 'alter':
            ddl_statements = datastore_impl.generate_alter_table_ddl(
                table_name,
                comparison['changes'],
                schema_name
            )
        
        # Apply if requested
        if apply and ddl_statements:
            self.logger.info(f"Applying {len(ddl_statements)} DDL statement(s) to {table_name}")
            for ddl in ddl_statements:
                self.logger.info(f"Executing: {ddl}")
                try:
                    await datastore.execute_query(ddl, action='ddl')
                    self.logger.info(f"Successfully executed DDL")
                except Exception as e:
                    self.logger.error(f"Failed to execute DDL: {e}")
                    raise
        
        return {
            'table_name': table_name,
            'action': comparison['action'],
            'table_exists': table_exists,
            'changes': comparison.get('changes', []),
            'ddl_statements': ddl_statements,
            'applied': apply
        }
    
    def _compare_schemas(
        self,
        existing: Optional[UniversalSchema],
        desired: UniversalSchema,
        datastore_impl=None
    ) -> Dict[str, Any]:
        """
        Compare existing and desired schemas.
        
        Args:
            existing: Existing schema from database (None if table doesn't exist)
            desired: Desired schema from config
            datastore_impl: Datastore implementation for type mapping
            
        Returns:
            Dict with action and changes:
            {
                'action': 'create'|'alter'|'no_change',
                'changes': List[Dict] - list of changes needed
            }
        """
        if not existing:
            return {
                'action': 'create',
                'changes': []
            }
        
        changes = []
        existing_cols = {col.name: col for col in existing.columns}
        desired_cols = {col.name: col for col in desired.columns}
        
        # Check for new columns
        for col_name, col in desired_cols.items():
            if col_name not in existing_cols:
                changes.append({
                    'type': 'add_column',
                    'column': col,
                    'description': f"Add column '{col_name}' ({col.data_type.value})"
                })
            else:
                # Check for type changes - compare base data type only, ignore precision/scale/length
                existing_col = existing_cols[col_name]
                
                # If we have a datastore implementation, compare the base types only
                if datastore_impl:
                    # Get base type without parameters (e.g., VARCHAR, DECIMAL, CHAR)
                    existing_db_type = datastore_impl.map_universal_type_to_target(existing_col.data_type)
                    desired_db_type = datastore_impl.map_universal_type_to_target(col.data_type)
                    
                    # Extract base type without parameters for comparison
                    # e.g., "VARCHAR(100)" -> "VARCHAR", "DECIMAL(10,2)" -> "DECIMAL"
                    import re
                    existing_base = re.sub(r'\(.*?\)', '', existing_db_type).strip()
                    desired_base = re.sub(r'\(.*?\)', '', desired_db_type).strip()
                    
                    # Only flag as a change if the base database types differ
                    # This ignores differences in length, precision, and scale
                    if existing_base != desired_base:
                        changes.append({
                            'type': 'modify_column',
                            'column': col,
                            'old_type': existing_col.data_type,
                            'new_type': col.data_type,
                            'description': f"Modify column '{col_name}' type from {existing_col.data_type.value} ({existing_base}) to {col.data_type.value} ({desired_base})"
                        })
                else:
                    # Fallback: compare universal types directly (less accurate)
                    if existing_col.data_type != col.data_type:
                        changes.append({
                            'type': 'modify_column',
                            'column': col,
                            'old_type': existing_col.data_type,
                            'new_type': col.data_type,
                            'description': f"Modify column '{col_name}' type from {existing_col.data_type.value} to {col.data_type.value}"
                        })
        
        # Note: We intentionally don't check for removed columns for safety
        # Users should manually drop columns they no longer need
        
        return {
            'action': 'alter' if changes else 'no_change',
            'changes': changes
        }

