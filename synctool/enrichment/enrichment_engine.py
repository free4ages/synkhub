import asyncio
import datetime

from typing import Dict, List

from synctool.enrichment.sources import get_source_class
from synctool.utils import safe_eval
from ..core.models import DimensionFieldConfig, EnrichmentConfig, DimensionConfig, TransformationConfig


from typing import List, Dict, Any
from dataclasses import dataclass

from .cache import get_cache_backend
from threading import Lock

TRANSFORM_FUNCTIONS = {
    "to_str": lambda x: str(x) if x is not None else "",
    "to_int": lambda x: int(x) if x is not None else None,
    "to_float": lambda x: float(x) if x is not None else None,
    "datetime_to_date": lambda x: x.date() if hasattr(x, 'date') else None,
    "lowercase": lambda x: x.lower() if isinstance(x, str) else x,
    "uppercase": lambda x: x.upper() if isinstance(x, str) else x,
    "strip": lambda x: x.strip() if isinstance(x, str) else x,
    "length": lambda x: len(x) if hasattr(x, '__len__') else None,
}

class EnrichmentEngine:
    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.dimensions = [DimensionConfig(**dim) for dim in config.dimensions or []]
        for dim in self.dimensions:
            dim.fields = [DimensionFieldConfig(**field) for field in dim.fields]
        self.cache = None
        self.source_cache: Dict[str, Any] = {}  # dim_name -> source instance
        self.initialized = False
        self.transformations  = [TransformationConfig(**f) for f in config.transformations]

        # Locks
        self.init_lock = Lock()
        self.source_cache_lock = Lock()
    
    def get_enriched_column_details(self):
        enriched_columns = []
        for transformation in self.transformations:
            column_details = {
                "dest": transformation.dest
            }
            if transformation.data_type:
                column_details["data_type"] = transformation.data_type
            enriched_columns.append(column_details)
        for dim in self.dimensions:
            for field in dim.fields:
                column_details = {
                    "dest": field.dest
                }
                if field.data_type:
                    column_details["data_type"] = field.data_type
                enriched_columns.append(column_details)
        return enriched_columns
    
    def get_enriched_keys(self):
        keys = set()
        for transformation in self.transformations:
            if transformation.dest:
                keys.add(transformation.dest)
        for dim in self.dimensions:
            for field in dim.fields:
                if field.dest:
                    keys.add(field.dest)
        return list(keys)

    def initialize(self):
        """Thread-safe one-time initialization"""
        with self.init_lock:
            if self.initialized:
                return
            self.cache = get_cache_backend(self.config.cache_backend, self.config.cache_config)
            self.initialized = True

    async def connect(self):
        """Connect cache + all source instances"""
        self.initialize()

        if hasattr(self.cache, "connect"):
            await self.cache.connect(self.config.cache_config)

        for dim in self.dimensions:
            with self.source_cache_lock:
                if dim.name not in self.source_cache:
                    source_cls = get_source_class(dim.source.type)
                    source = source_cls(dim.source.config)
                    self.source_cache[dim.name] = source
                else:
                    source = self.source_cache[dim.name]

            if hasattr(source, "connect"):
                await source.connect(dim.source.config)

    async def disconnect(self):
        """Clean up all connections"""

        await self.cache.disconnect()

        with self.source_cache_lock:
            for source in self.source_cache.values():
                await source.disconnect()

    async def enrich(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        
        for dim in self.dimensions:
            await self._enrich_with_dimension(data, dim)
        await self.transform_data(data)

        return data
    
    async def transform_data(self, data: List[Dict]) -> List[Dict]:
        results = []
        for row in data:
            for config in self.transformations:
                if not config.dest or not config.transform:
                    continue

                transform = config.transform.strip()
                result = None

                try:
                    if transform.startswith("lambda"):
                        # Unsafe lambda string (if trusted config)
                        func = safe_eval(transform)
                        result = func(row)
                    elif transform in TRANSFORM_FUNCTIONS:
                        func = TRANSFORM_FUNCTIONS[transform]
                        if config.columns:
                            value = row.get(config.columns[0])
                            result = func(value)
                    else:
                        raise ValueError(f"Unknown transformation: {transform}")
                except Exception as e:
                    result = None  # Optional: log this
                    raise e

                row[config.dest] = result
        return data

    async def _enrich_with_dimension(self, data: List[Dict], dim: DimensionConfig):
        join_key = dim.join_key
        keys = list({row[join_key] for row in data if join_key in row})

        # --- Source loading (thread-safe) ---
        with self.source_cache_lock:
            source = self.source_cache.get(dim.name)
            if not source:
                source_cls = get_source_class(dim.source.type)
                source = source_cls(dim.source.config)
                await source.connect(dim.source.config)
                self.source_cache[dim.name] = source

        # --- Cache check and batch fetch ---
        lookup = {}
        missing_keys = []

        for key in keys:
            cached = self.cache.get(key)
            if cached:
                lookup[key] = cached
            else:
                missing_keys.append(key)

        if missing_keys:
            fetched = await source.batch_fetch(missing_keys)
            for k, v in fetched.items():
                self.cache.set(k, v)
                lookup[k] = v

        # --- Transform and assign ---
        for row in data:
            key = row.get(join_key)
            record = lookup.get(key, {})

            for field in dim.fields:
                val = record.get(field.source)

                if field.transform and field.transform.strip().startswith("lambda"):
                    try:
                        func = safe_eval(field.transform)
                        val = func(val)
                    except Exception:
                        val = None

                row[field.dest] = val
    