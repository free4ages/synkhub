from functools import cached_property
from typing import Optional, List, Tuple, Dict, Any
from synctool.core.models import Column, StrategyConfig
from synctool.enrichment.enrichment_engine import EnrichmentEngine
from synctool.core.models import SyncStrategy


class ColumnSchema:
    def __init__(self, columns: List[Column]):
        self.columns = sorted(columns, key=lambda x: x.name)
        self.column_map = {c.name: c for c in self.columns}

    @cached_property
    def partition_column(self) -> Column:
        for c in self.columns:
            if c.partition_column:
                return c
        raise ValueError("No partition key found in column schema")

    @cached_property
    def order_columns(self) -> List[Column] | None:
        """
        Returns the first column with an order_column role and its direction ('asc' or 'desc').
        """
        return [c for c in self.columns if c.order_column] or None

    @cached_property
    def delta_column(self) -> Optional[Column]:
        for c in self.columns:
            if c.delta_column:
                return c
        return None

    @cached_property
    def unique_columns(self) -> List[Column]:
        return [c for c in self.columns if c.unique_column]

    @cached_property
    def hash_key(self) -> Column|None:
        for c in self.columns:
            if c.hash_key:
                return c
        return None
    
    def column(self, name: str) -> Column:
        return self.column_map[name]

    def columns_to_fetch(self) -> List[Column]:
        """Returns list of source expressions (expr) or names if expr missing."""
        return [c for c in self.columns if not c.virtual]
    
    def columns_to_hash(self) -> List[Column]:
        return [c for c in self.columns if c.hash_column and not c.virtual]

    def columns_to_insert(self) -> List[Column]:
        """Returns list of column names to be inserted (insert==True)."""
        return [c for c in self.columns if not c.virtual]
    
    def transform_data(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        new_data = {}
        for col in self.columns_to_insert():
            # if not col.hash_key:
            new_data[col.expr] = data[col.name]
        # if self.hash_key:
        #     new_data[self.hash_key.expr] = data['hash__']
        return new_data
    



class ColumnMapper:
    def __init__(self, fields: List[Dict[str, Any]]):
        self.field_map = {f['name']: f for f in fields}
        self.schema = self.build_schema(fields)

    def build_schema(self, field_map: List[Dict[str, Any]]) -> ColumnSchema:
        columns = []
        for f in field_map:
            if f['name'] in self.field_map:
                obj = f.copy()
                obj.update(self.field_map[f['name']])
                columns.append(Column(**obj))
        return ColumnSchema(columns)

    def build_schemas(self, strategies: List[StrategyConfig], enrichment_engine: Optional[EnrichmentEngine]=None) -> Dict[str, ColumnSchema]:
        """
        Builds the column schemas for the source, destination, source state, destination state, and common columns.
        Adds columns from column_map to the schemas. Also adds enriched columns from enrichment_engine.
        Also sets roles from strategies.
        """
        source_cols, dest_cols, source_state_cols, dest_state_cols, common_cols = [], [], [], [], []
        delta_column, partition_column, hash_partition_column = None, None, None
        for strategy in strategies:
            if strategy.type == SyncStrategy.DELTA:
                delta_column = strategy.column
            elif strategy.type == SyncStrategy.FULL:
                partition_column = strategy.column
            elif strategy.type == SyncStrategy.HASH:
                hash_partition_column = strategy.column
        assert hash_partition_column == partition_column, "Hash partition key and partition key must be the same"
        # @TODO: Add enrichment keys to the schema
        enriched_column_details: dict[str, Dict[str, Any]] = {c["dest"]: c for c in (enrichment_engine.get_enriched_column_details() if enrichment_engine else [])}

        for f in self.field_map:
            roles = []
            name: str = f["name"]
            src: Any | None = f.get("src")
            dest: Any | None = f.get("dest")
            src_state: Any | None = f.get("src_state")
            dest_state: Any | None = f.get("dest_state")
            data_type: Any | None = f.get("data_type")
            dest_dtype: Any | None = f.get("dest_dtype") or f.get("data_type")
            insert: bool = f.get("insert", True)
            expr_map: dict[str, str | None] = dict(source=src, destination=dest, source_state=src_state, destination_state=dest_state)
            if dest in enriched_column_details:
                # if data_type is not provided, use the data_type from the enriched column details
                if not data_type:
                    data_type = enriched_column_details[dest]["data_type"]
                # if dest_dtype is not provided, use the data_type from the enriched column details
                if not dest_dtype:
                    dest_dtype = enriched_column_details[dest]["data_type"]
                # if insert is not provided, use the insert from the enriched column details
                insert = True
                enriched_column_details.pop(dest)
            if f.get("enriched_key"):
                roles.append("enriched_key")
            if f.get("unique_column"):
                roles.append("unique_column")
            if f.get("order_column"):
                roles.append(f"order_column[{f.get('direction', 'asc')}]")
            if f.get("hash_key"):
                roles.append("hash_key")
            if name == partition_column:
                roles.append("partition_column")
                assert data_type is not None, "Partition key must have a data_type"
            if name == delta_column:
                roles.append("delta_column")
                assert data_type is not None, "Delta key must have a data_type"

            common_cols.append(Column(name=name, expr=name, data_type=data_type, roles=roles, insert=True, expr_map=expr_map))
            # Source
            if src is not None:
                source_cols.append(Column(name=name, expr=src, data_type=data_type, roles=roles, insert=True, expr_map=expr_map))
            # Destination
            if insert:
                dest_cols.append(Column(name=name, expr=dest, data_type=dest_dtype, roles=roles, insert=True, expr_map=expr_map))

            # Source state
            if src_state:
                source_state_cols.append(Column(name=name, expr=src_state, data_type=data_type, roles=roles, insert=True, expr_map=expr_map))
            # Destination state
            if dest_state and insert:
                dest_state_cols.append(Column(name=name, expr=dest_state, data_type=dest_dtype, roles=roles, insert=True, expr_map=expr_map))
        for dest, details in enriched_column_details.items():
            expr_map: dict[str, str | None] = dict(source=None, destination=dest, source_state=None, destination_state=None)
            dest_cols.append(Column(name=dest, expr=dest, data_type=details.get("data_type"), roles=["enriched_key"], insert=True, expr_map=expr_map))
            common_cols.append(Column(name=dest, expr=dest, data_type=details.get("data_type"), roles=["enriched_key"], insert=True, expr_map=expr_map))

        self.schemas = {
            "common": ColumnSchema(common_cols),
            "source": ColumnSchema(source_cols),
            "destination": ColumnSchema(dest_cols),
            "source_state": ColumnSchema(source_state_cols),
            "destination_state": ColumnSchema(dest_state_cols),
        }
        return self.schemas
    
    def column(self, name: str) -> Column:
        return self.schemas["common"].column(name)

    def validate(self) -> None:
        """
        Validate the field map against rules:
        1. Each of partition_column, delta_column, hash_key must have exactly one field.
        2. These fields must be present (property).
        """

        # We will check destination schema columns for these roles
        dest_cols = self.schemas.get("destination")
        if dest_cols is None:
            raise ValueError("Schemas not built yet, call build_schemas() before validate()")

        def get_fields_by_role(role_name: str) -> List[Column]:
            return [c for c in dest_cols.columns if any(r.startswith(role_name) for r in c.roles)]

        # Roles to check
        single_roles = ["partition_column", "delta_column", "hash_key"]

        for role in single_roles:
            fields = get_fields_by_role(role)
            # Skip validation if no fields with this role (optional roles)
            if len(fields) == 0:
                continue
            
            if len(fields) > 1:
                field_names = [f.expr for f in fields]
                raise ValueError(f"Validation Error: Multiple fields marked with role '{role}': {field_names}. Only one allowed.")

            # Check that field is a property: here, just check that name is not None or empty
            field = fields[0]
            if not field.expr:
                raise ValueError(f"Validation Error: Field with role '{role}' must have a valid name.")

