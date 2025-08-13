from functools import cached_property
from typing import Optional, List, Tuple, Dict, Any
from synctool.core.models import Column, StrategyConfig
from synctool.enrichment.enrichment_engine import EnrichmentEngine
from synctool.core.models import SyncStrategy


class ColumnSchema:
    def __init__(self, columns: List[Column]):
        self.columns = columns
        self.column_map = {c.name: c for c in columns}

    @cached_property
    def partition_key(self) -> Column:
        for c in self.columns:
            if c.has_role("partition_key"):
                return c
        raise ValueError("No partition key found in column schema")

    @cached_property
    def order_keys(self) -> List[Tuple[Column, str]] | None:
        """
        Returns the first column with an order_key role and its direction ('asc' or 'desc').
        """
        return [(c, c.get_order_direction()) for c in self.columns if c.has_role("order_key")] or None

    @cached_property
    def delta_key(self) -> Optional[Column]:
        for c in self.columns:
            if c.has_role("delta_key"):
                return c
        return None

    @cached_property
    def unique_keys(self) -> List[Column]:
        return [c for c in self.columns if c.has_role("unique_key")]

    @cached_property
    def hash_key(self) -> Column:
        for c in self.columns:
            if c.has_role("hash_key"):
                return c
        return None
    
    def column(self, name: str) -> Column:
        return self.column_map[name]

    def columns_to_fetch(self) -> List[Column]:
        """Returns list of source expressions (expr) or names if expr missing."""
        return self.columns

    def columns_to_insert(self) -> List[Column]:
        """Returns list of column names to be inserted (insert==True)."""
        return [c for c in self.columns if c.insert]
    
    def transform_data(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        new_data = {}
        for col in self.columns_to_insert():
            if not col.has_role("hash_key"):
                new_data[col.expr] = data[col.name]
        if self.hash_key:
            new_data[self.hash_key.expr] = data['hash__']
        return new_data
    



class ColumnMapper:
    def __init__(self, field_map: List[Dict[str, Any]]):
        self.field_map = field_map
        self.schemas = {}

    def build_schemas(self, strategies: List[StrategyConfig], enrichment_engine: Optional[EnrichmentEngine]=None) -> Dict[str, ColumnSchema]:
        """
        Builds the column schemas for the source, destination, source state, destination state, and common columns.
        Adds columns from column_map to the schemas. Also adds enriched columns from enrichment_engine.
        Also sets roles from strategies.
        """
        source_cols, dest_cols, source_state_cols, dest_state_cols, common_cols = [], [], [], [], []
        delta_key, partition_key, hash_partition_key = None, None, None
        for strategy in strategies:
            if strategy.type == SyncStrategy.DELTA:
                delta_key = strategy.column
            elif strategy.type == SyncStrategy.FULL:
                partition_key = strategy.column
            elif strategy.type == SyncStrategy.HASH:
                hash_partition_key = strategy.column
        assert hash_partition_key == partition_key, "Hash partition key and partition key must be the same"
        # @TODO: Add enrichment keys to the schema
        enriched_column_details: dict[str, Dict[str, Any]] = {c["dest"]: c for c in (enrichment_engine.get_enriched_column_details() if enrichment_engine else [])}

        for f in self.field_map:
            roles = []
            name: str = f["name"]
            src: Any | None = f.get("src")
            dest: Any | None = f.get("dest")
            src_state: Any | None = f.get("src_state")
            dest_state: Any | None = f.get("dest_state")
            dtype: Any | None = f.get("dtype")
            dest_dtype: Any | None = f.get("dest_dtype") or f.get("dtype")
            insert: bool = f.get("insert", True)
            expr_map: dict[str, str | None] = dict(source=src, destination=dest, source_state=src_state, destination_state=dest_state)
            if dest in enriched_column_details:
                # if dtype is not provided, use the dtype from the enriched column details
                if not dtype:
                    dtype = enriched_column_details[dest]["dtype"]
                # if dest_dtype is not provided, use the dtype from the enriched column details
                if not dest_dtype:
                    dest_dtype = enriched_column_details[dest]["dtype"]
                # if insert is not provided, use the insert from the enriched column details
                insert = True
                enriched_column_details.pop(dest)
            if f.get("enriched_key"):
                roles.append("enriched_key")
            if f.get("unique_key"):
                roles.append("unique_key")
            if f.get("order_key"):
                roles.append(f"order_key[{f.get('direction', 'asc')}]")
            if f.get("hash_key"):
                roles.append("hash_key")
            if name == partition_key:
                roles.append("partition_key")
                assert dtype is not None, "Partition key must have a dtype"
            if name == delta_key:
                roles.append("delta_key")
                assert dtype is not None, "Delta key must have a dtype"

            common_cols.append(Column(name=name, expr=name, dtype=dtype, roles=roles, insert=True, expr_map=expr_map))
            # Source
            if src is not None:
                source_cols.append(Column(name=name, expr=src, dtype=dtype, roles=roles, insert=True, expr_map=expr_map))
            # Destination
            if insert:
                dest_cols.append(Column(name=name, expr=dest, dtype=dest_dtype, roles=roles, insert=True, expr_map=expr_map))

            # Source state
            if src_state:
                source_state_cols.append(Column(name=name, expr=src_state, dtype=dtype, roles=roles, insert=True, expr_map=expr_map))
            # Destination state
            if dest_state and insert:
                dest_state_cols.append(Column(name=name, expr=dest_state, dtype=dest_dtype, roles=roles, insert=True, expr_map=expr_map))
        for dest, details in enriched_column_details.items():
            expr_map: dict[str, str | None] = dict(source=None, destination=dest, source_state=None, destination_state=None)
            dest_cols.append(Column(name=dest, expr=dest, dtype=details.get("dtype"), roles=["enriched_key"], insert=True, expr_map=expr_map))
            common_cols.append(Column(name=dest, expr=dest, dtype=details.get("dtype"), roles=["enriched_key"], insert=True, expr_map=expr_map))

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
        1. Each of partition_key, delta_key, hash_key must have exactly one field.
        2. These fields must be present (property).
        """

        # We will check destination schema columns for these roles
        dest_cols = self.schemas.get("destination")
        if dest_cols is None:
            raise ValueError("Schemas not built yet, call build_schemas() before validate()")

        def get_fields_by_role(role_name: str) -> List[Column]:
            return [c for c in dest_cols.columns if any(r.startswith(role_name) for r in c.roles)]

        # Roles to check
        single_roles = ["partition_key", "delta_key", "hash_key"]

        for role in single_roles:
            fields = get_fields_by_role(role)
            # if len(fields) == 0:
            #     raise ValueError(f"Validation Error: No field marked with role '{role}' found.")
            if len(fields) > 1:
                field_names = [f.expr for f in fields]
                raise ValueError(f"Validation Error: Multiple fields marked with role '{role}': {field_names}. Only one allowed.")

            # Check that field is a property: here, just check that name is not None or empty
            field = fields[0]
            if not field.expr:
                raise ValueError(f"Validation Error: Field with role '{role}' must have a valid name.")

