import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple
from collections import defaultdict

from ...pipeline.base import PipelineStage, DataBatch
from ...core.models import PipelineJobConfig, GlobalStageConfig, JoinCondition, JoinStageConfig, AsofConfig, DimensionPartitionConfig
from ...core.query_models import Query, Filter, Field, Table
from ...cache_backend import get_cache_backend, BaseCacheBackend
from ...utils.multi_dimensional_partition_generator import build_multi_dimension_partitions_for_delta_data


@dataclass
class JoinStageConfigInternal(GlobalStageConfig):
    """Internal configuration for join stage"""
    config: JoinStageConfig = None
    
    @classmethod
    def from_global_stage_config(cls, global_config: GlobalStageConfig):
        join_config_dict = global_config.config or {}
        
        # Parse join conditions (for equality conditions)
        join_on = []
        for condition in join_config_dict.get('join_on', []):
            join_on.append(JoinCondition(
                data_key=condition.get('data_key'),
                backend_key=condition.get('backend_key'),
                operator=condition.get('operator', '=')
            ))
        
        # Parse ASOF configuration (if provided)
        asof_dict = join_config_dict.get('asof')
        asof = None
        if asof_dict:
            asof = AsofConfig(
                data_key=asof_dict.get('data_key'),
                valid_from_column=asof_dict.get('valid_from_column'),
                valid_to_column=asof_dict.get('valid_to_column'),
                operator=asof_dict.get('operator', '<='),
                include_open_ended=asof_dict.get('include_open_ended', True)
            )
        
        join_config = JoinStageConfig(
            join_type=join_config_dict.get('join_type', 'left'),
            join_on=join_on,
            asof=asof,
            fetch_columns=join_config_dict.get('fetch_columns'),
            cache=join_config_dict.get('cache'),
            cardinality=join_config_dict.get('cardinality', 'one_to_one'),
            merge=join_config_dict.get('merge', True),
            output_key=join_config_dict.get('output_key', 'joined_data')
        )
        
        return cls(
            name=global_config.name,
            type=global_config.type,
            enabled=global_config.enabled,
            source=global_config.source,
            destination=global_config.destination,
            columns=global_config.columns,
            config=join_config
        )


class JoinStage(PipelineStage):
    """
    Stage that performs joins with external data sources.
    
    Supports:
    - left join: Standard left outer join
    - inner join: Only matching rows
    - asof_left join: ClickHouse-style ASOF join for SCD (Slowly Changing Dimension) tables
      
    ASOF Join Behavior (for SCD tables):
    
    Type 2a SCD (only valid_from):
      - Finds the most recent record where valid_from <= data_timestamp
      - Config: asof: {data_key: 'event_time', valid_from_column: 'valid_from'}
    
    Type 2b SCD (valid_from + valid_to):
      - Finds record where data_timestamp BETWEEN valid_from AND valid_to
      - Much more efficient: uses index on both columns
      - Config: asof: {data_key: 'event_time', valid_from_column: 'valid_from', valid_to_column: 'valid_to'}
      - Automatically handles NULL valid_to (current/open-ended records)
    
    OPTIMIZATION with partition bounds:
      - Type 2a: Limits fetched records to valid_from <= partition_end
      - Type 2b: Adds BETWEEN filter: valid_from <= partition_end AND (valid_to >= partition_start OR valid_to IS NULL)
    
    Example configs at bottom of file.
    """
    
    def __init__(self, sync_engine: Any, config: GlobalStageConfig, 
                 pipeline_config: PipelineJobConfig, logger=None, 
                 data_storage=None, progress_manager=None):
        
        config = JoinStageConfigInternal.from_global_stage_config(config)
        super().__init__(config.name, config, pipeline_config, logger)
        
        self.sync_engine = sync_engine
        self.join_backend = sync_engine.create_backend(self.config.source)
        self.join_config: JoinStageConfig = config.config
        
        # Validate cardinality
        self.cardinality = getattr(self.join_config, 'cardinality', 'one_to_one')
        if self.cardinality not in ['one_to_one', 'one_to_many']:
            raise ValueError(f"Invalid cardinality: {self.cardinality}")
        
        # Merge configuration
        self.merge = getattr(self.join_config, 'merge', True)
        self.output_key = getattr(self.join_config, 'output_key', 'joined_data')
        
        # Parse join structure for ASOF
        self._parse_join_conditions()
        # import pdb; pdb.set_trace()
        
        # Initialize cache backend if enabled
        self.cache_enabled = self.join_config.cache and self.join_config.cache.get('enabled', False)
        self.cache: Optional[BaseCacheBackend] = None
        
        if self.cache_enabled:
            try:
                cache_config = self.join_config.cache
                self.cache = get_cache_backend(cache_config.get('backend'), cache_config)
                self.logger.info(
                    f"Initialized {self.join_config.cache.get('type', 'memory')} cache "
                    f"with {self.join_config.cache.get('policy', 'lru')} policy"
                )
            except Exception as e:
                self.logger.error(f"Failed to initialize cache: {e}. Disabling cache.")
                self.cache_enabled = False
                self.cache = None
    
    def _parse_join_conditions(self):
        """Parse join conditions into equality and ASOF configurations"""
        self.equality_conditions = []
        self.asof_config = None
        
        # Check if using new ASOF config format
        if self.join_config.asof:
            self.asof_config = self.join_config.asof
            # All join_on conditions are equality
            self.equality_conditions = self.join_config.join_on
            
            # Validate ASOF configuration
            if self.join_config.join_type == 'asof_left':
                if not self.asof_config.data_key or not self.asof_config.valid_from_column:
                    raise ValueError("ASOF join requires data_key and valid_from_column")
        else:
            # Legacy format: parse from join_on conditions
            asof_condition = None
            range_conditions = []  # Track range conditions for validation
            
            for condition in self.join_config.join_on:
                if condition.operator == '=':
                    self.equality_conditions.append(condition)
                elif condition.operator in ['>=', '<=', '>', '<', '!=']:
                    range_conditions.append(condition)
                    
                    if self.join_config.join_type == 'asof_left':
                        if asof_condition is not None:
                            raise ValueError(
                                "ASOF join supports only one time-based condition in legacy format. "
                                f"Found multiple: {asof_condition.data_key} and {condition.data_key}. "
                                "Use the 'asof' config block for complex ASOF joins."
                            )
                        asof_condition = condition
                else:
                    raise ValueError(
                        f"Unsupported operator '{condition.operator}' in join condition for column '{condition.data_key}'. "
                        f"Supported operators: '=', '<', '<=', '>', '>=', '!='"
                    )
            
            # Validate: range operators require asof_left join type
            if range_conditions and self.join_config.join_type != 'asof_left':
                range_cols = [f"{c.data_key} {c.operator} {c.backend_key}" for c in range_conditions]
                raise ValueError(
                    f"Range operators (<, <=, >, >=) are only supported with join_type='asof_left'. "
                    f"Found join_type='{self.join_config.join_type}' with range conditions: {', '.join(range_cols)}. "
                    f"Either change join_type to 'asof_left' or remove range conditions."
                )
            
            # Convert legacy format to AsofConfig
            if asof_condition and self.join_config.join_type == 'asof_left':
                self.asof_config = AsofConfig(
                    data_key=asof_condition.data_key,
                    valid_from_column=asof_condition.backend_key,
                    valid_to_column=None,
                    operator=asof_condition.operator,
                    include_open_ended=True
                )
            elif range_conditions and self.join_config.join_type == 'asof_left':
                # This shouldn't happen due to the check above, but just in case
                raise ValueError(
                    "ASOF join detected but no valid time-based condition found. "
                    "Check your join_on configuration."
                )
    
    async def setup(self, context: Any):
        """Connect to join backend and cache"""
        await self.join_backend.connect()
        if self.cache:
            await self.cache.connect()
    
    async def teardown(self, context: Any):
        """Disconnect from join backend and cache"""
        await self.join_backend.disconnect()
        if self.cache:
            # Log cache stats before disconnecting
            stats = self.cache.get_stats()
            self.logger.info(f"Join cache stats: {stats}")
            await self.cache.disconnect()
    
    async def process(self, input_stream: AsyncIterator[DataBatch]) -> AsyncIterator[DataBatch]:
        """Process batches and enrich with joined data"""
        async for batch in input_stream:
            yield await self.process_batch(batch)
    
    async def process_batch(self, batch: DataBatch) -> DataBatch:
        """Join data with backend for a single batch"""
        if not batch.data:
            return batch
        
        try:
            # Extract time bounds from batch metadata (for ASOF optimization)
            # @TODO: implement partition bounds logic
            time_bounds = None
            if False:
                time_bounds = self._extract_time_bounds(batch)
            
            # Extract unique keys from batch data
            lookup_keys = self._extract_lookup_keys(batch.data)
            
            if not lookup_keys:
                self.logger.warning("No valid lookup keys found in batch")
                return batch
            
            # Fetch joined data (with caching and time bounds optimization)
            joined_data = await self._fetch_joined_data(lookup_keys, time_bounds)
            # import pdb; pdb.set_trace()
            # Merge or store joined data based on configuration
            if self.merge:
                # Traditional merge: add columns to rows
                if self.cardinality == 'one_to_one':
                    self._merge_joined_data_one_to_one(batch.data, joined_data)
                else:  # one_to_many
                    batch.data = self._merge_joined_data_one_to_many(batch.data, joined_data)
            else:
                # Fetch-only mode: store in metadata without modifying rows
                batch.batch_metadata[self.output_key] = joined_data
            
            # batch.batch_metadata["joined"] = True
            # batch.batch_metadata["join_backend"] = self.join_backend.name
            # batch.batch_metadata["join_type"] = self.join_config.join_type
            # batch.batch_metadata["join_cardinality"] = self.cardinality
            # batch.batch_metadata["join_merged"] = self.merge
            # batch.batch_metadata["join_matches"] = len(joined_data)
            # if self.asof_config and self.asof_config.valid_to_column:
            #     batch.batch_metadata["join_scd_type"] = "type2b_with_valid_to"
            # elif self.asof_config:
            #     batch.batch_metadata["join_scd_type"] = "type2a_valid_from_only"
            
            self.logger.debug(
                f"Joined {len(joined_data)} records to batch (type: {self.join_config.join_type}, "
                f"cardinality: {self.cardinality}, merged: {self.merge})"
            )
            
            return batch
            
        except Exception as e:
            self.logger.error(f"Join failed for batch {batch.batch_id}: {e}")
            batch.batch_metadata["join_error"] = str(e)
            raise
    
    def _extract_time_bounds(self, batch: DataBatch) -> Optional[Tuple[Any, Any]]:
        """
        Extract time bounds from batch metadata for ASOF optimization.
        
        Looks for partition bounds in metadata that match the ASOF time column.
        Returns (min_time, max_time) or None if not available.
        """
        if self.join_config.join_type != 'asof_left' or not self.asof_config:
            return None
        
        # Check if batch has partition metadata with time bounds
        partition = batch.batch_metadata.get('partition')
        if not partition:
            return None
        
        # Look for partition bounds matching the ASOF data key
        partition_bounds = getattr(partition, 'get_partition_bounds', lambda: [])()
        
        for bound in partition_bounds:
            if bound.column == self.asof_config.data_key:
                self.logger.debug(
                    f"Using partition time bounds for ASOF optimization: "
                    f"[{bound.start}, {bound.end}]"
                )
                return (bound.start, bound.end)
        
        return None
    
    def _extract_lookup_keys(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """
        Extract unique lookup keys from data based on join conditions.
        """
        unique_keys = set()
        
        # For ASOF joins, include equality keys + time key
        if self.asof_config:
            data_keys = [cond.data_key for cond in self.equality_conditions] + [self.asof_config.data_key]
        else:
            data_keys = [condition.data_key for condition in self.join_config.join_on]
        
        for row in data:
            # Build key tuple from all join data_keys
            key_values = []
            valid = True
            for data_key in data_keys:
                if data_key not in row:
                    valid = False
                    break
                key_values.append(row[data_key])
            
            if valid:
                unique_keys.add(tuple(key_values))
        
        # Convert back to list of dicts
        return [
            {data_keys[i]: key[i] for i in range(len(data_keys))}
            for key in unique_keys
        ]
    
    async def _fetch_joined_data(self, lookup_keys: List[Dict[str, Any]], 
                                 time_bounds: Optional[Tuple[Any, Any]] = None) -> Dict[tuple, Any]:
        """
        Fetch data from join backend for given lookup keys.
        """
        if not lookup_keys:
            return {}
        
        # For ASOF joins, caching is complex (time-dependent)
        if self.cache_enabled and self.join_config.join_type != 'asof_left':
            cached_results, missing_keys = await self._check_cache(lookup_keys)
        else:
            cached_results = {}
            missing_keys = lookup_keys
        
        # Fetch missing keys from backend
        if missing_keys:
            if self.join_config.join_type == 'asof_left':
                fetched = await self._batch_fetch_asof(missing_keys, time_bounds)
            else:
                fetched = await self._batch_fetch_standard(missing_keys)
            
            # Update cache (only for non-ASOF)
            if self.cache_enabled and self.join_config.join_type != 'asof_left':
                await self._update_cache(fetched)
            
            # Merge with cached results
            cached_results.update(fetched)
        
        return cached_results
    
    async def _check_cache(self, lookup_keys: List[Dict[str, Any]]) -> tuple[Dict[tuple, Any], List[Dict]]:
        """
        Check cache for existing data, return (cached, missing).
        Uses batch get_many operation for efficiency.
        """
        if not self.cache:
            return {}, lookup_keys
        # import pdb; pdb.set_trace()
        
        # Convert to key tuples
        key_tuples = [self._dict_to_key_tuple(key_dict) for key_dict in lookup_keys]
        
        # Batch get from cache
        cached_data = await self.cache.get_many(key_tuples)
        
        # Determine missing keys
        missing = []
        for key_dict in lookup_keys:
            key_tuple = self._dict_to_key_tuple(key_dict)
            if key_tuple not in cached_data:
                missing.append(key_dict)
        
        return cached_data, missing
    
    async def _update_cache(self, fetched_data: Dict[tuple, Any]):
        """
        Update cache with fetched data.
        Uses batch set_many operation for efficiency and atomicity.
        """
        if not self.cache or not fetched_data:
            return
        
        try:
            # Batch set to cache
            await self.cache.set_many(fetched_data)
        except Exception as e:
            self.logger.warning(f"Failed to update cache: {e}")
    
    async def _batch_fetch_asof(self, lookup_keys: List[Dict[str, Any]], 
                                time_bounds: Optional[Tuple[Any, Any]] = None) -> Dict[tuple, Any]:
        """
        Fetch data using ASOF join logic for SCD tables.
        
        Optimizations based on SCD type:
        - Type 2a (valid_from only): Fetch records where valid_from <= max(time)
        - Type 2b (valid_from + valid_to): Use BETWEEN for much better performance
        """
        # Build query to fetch candidate records
        filters = self._build_asof_filters(lookup_keys, time_bounds)
        select_fields = self._build_select_fields()
        
        # Build query
        query = Query(
            action='select',
            table=Table(
                table=self.join_backend.table,
                schema=self.join_backend.schema,
                alias=self.join_backend.alias
            ),
            select=select_fields,
            filters=filters
        )
        
        # Execute query
        sql, params = self.join_backend._build_sql(query)
        
        if time_bounds:
            scd_type = "Type2b (valid_from+valid_to)" if self.asof_config.valid_to_column else "Type2a (valid_from only)"
            self.logger.debug(
                f"ASOF join optimized for {scd_type} with time bounds: [{time_bounds[0]}, {time_bounds[1]}]"
            )
        
        results = await self.join_backend.execute_query(sql, params)
        
        self.logger.debug(f"Fetched {len(results)} candidate records for ASOF matching")
        
        # Apply ASOF matching logic
        if self.asof_config.valid_to_column:
            # Type 2b: Records already filtered by BETWEEN, just need to pick best match if multiple
            return self._apply_asof_matching_with_valid_to(results, lookup_keys)
        else:
            # Type 2a: Need to find most recent record <= timestamp
            return self._apply_asof_matching_valid_from_only(results, lookup_keys)
    
    def _build_asof_filters(self, lookup_keys: List[Dict[str, Any]], 
                           time_bounds: Optional[Tuple[Any, Any]] = None) -> List[Filter]:
        """
        Build filters for ASOF join with optimization based on SCD type.
        
        Type 2a (valid_from only):
          - equality conditions via IN
          - valid_from <= max(event_times) [or partition_end if bounds available]
        
        Type 2b (valid_from + valid_to):
          - equality conditions via IN
          - Much better optimization using BETWEEN:
            * valid_from <= partition_end (or max event time)
            * (valid_to >= partition_start OR valid_to IS NULL)
          - This uses composite index (valid_from, valid_to) efficiently!
        """
        filters = []
        
        # Add filters for equality conditions
        for condition in self.equality_conditions:
            values = [key[condition.data_key] for key in lookup_keys]
            unique_values = list(set(values))
            
            filters.append(Filter(
                column=condition.backend_key,
                operator='in',
                value=unique_values
            ))
        
        # Add time filters based on SCD type
        if not self.asof_config:
            return filters
        
        time_values = [key[self.asof_config.data_key] for key in lookup_keys]
        
        if self.asof_config.valid_to_column:
            # TYPE 2B: SCD with valid_from AND valid_to
            # This is the OPTIMAL case - can use BETWEEN-like logic
            
            if time_bounds:
                bound_start, bound_end = time_bounds
                
                # Filter 1: valid_from <= partition_end
                # (Records that started before or during the partition)
                filters.append(Filter(
                    column=self.asof_config.valid_from_column,
                    operator='<=',
                    value=bound_end
                ))
                
                # Filter 2: (valid_to >= partition_start OR valid_to IS NULL)
                # (Records that are still valid during the partition)
                # Note: This is expressed as NOT(valid_to < partition_start AND valid_to IS NOT NULL)
                # Or: valid_to IS NULL OR valid_to >= partition_start
                
                # For SQL builder, we need to handle this carefully
                # Option 1: Fetch slightly more and filter in memory
                # Option 2: Use OR condition if supported by query builder
                
                # For now, using conservative approach:
                # Don't filter on valid_to in query, let in-memory matching handle it
                # OR add a lenient filter:
                if not self.asof_config.include_open_ended:
                    # Only if we don't want open-ended records
                    filters.append(Filter(
                        column=self.asof_config.valid_to_column,
                        operator='>=',
                        value=bound_start
                    ))
                # If include_open_ended=True, we need to handle NULL in the query
                # This is database-specific, so we'll filter in memory
                
            else:
                # No bounds: use event times
                min_time = min(time_values)
                max_time = max(time_values)
                
                filters.append(Filter(
                    column=self.asof_config.valid_from_column,
                    operator='<=',
                    value=max_time
                ))
                
                # valid_to >= min_time OR NULL (filter more in memory)
                if not self.asof_config.include_open_ended:
                    filters.append(Filter(
                        column=self.asof_config.valid_to_column,
                        operator='>=',
                        value=min_time
                    ))
        
        else:
            # TYPE 2A: SCD with valid_from only (original logic)
            if time_bounds:
                bound_start, bound_end = time_bounds
                
                if self.asof_config.operator in ['<=', '<']:
                    # Standard ASOF: valid_from <= partition_end
                    filters.append(Filter(
                        column=self.asof_config.valid_from_column,
                        operator='<=',
                        value=bound_end
                    ))
                elif self.asof_config.operator in ['>=', '>']:
                    # Reverse ASOF
                    filters.append(Filter(
                        column=self.asof_config.valid_from_column,
                        operator='>=',
                        value=bound_start
                    ))
            else:
                # No bounds: use event times
                if self.asof_config.operator in ['<=', '<']:
                    max_time = max(time_values)
                    filters.append(Filter(
                        column=self.asof_config.valid_from_column,
                        operator='<=',
                        value=max_time
                    ))
                elif self.asof_config.operator in ['>=', '>']:
                    min_time = min(time_values)
                    filters.append(Filter(
                        column=self.asof_config.valid_from_column,
                        operator='>=',
                        value=min_time
                    ))
        
        return filters
    
    def _apply_asof_matching_with_valid_to(self, results: List[Dict], 
                                           lookup_keys: List[Dict[str, Any]]) -> Dict[tuple, Any]:
        """
        Apply ASOF matching for Type 2b SCD (with valid_from and valid_to).
        
        For each lookup key:
        1. Filter records matching equality conditions
        2. Filter records where: valid_from <= event_time AND (valid_to > event_time OR valid_to IS NULL)
        3. If multiple matches (shouldn't happen in well-formed SCD), pick most recent valid_from
        """
        # Group results by equality keys
        grouped_results = defaultdict(list)
        
        for record in results:
            eq_key = tuple(record.get(cond.backend_key) for cond in self.equality_conditions)
            grouped_results[eq_key].append(record)
        
        matched = {}
        
        for key_dict in lookup_keys:
            # Build equality key
            eq_key = tuple(key_dict.get(cond.data_key) for cond in self.equality_conditions)
            
            # Get event time
            event_time = key_dict.get(self.asof_config.data_key)
            
            # Get candidate records
            candidates = grouped_results.get(eq_key, [])
            
            # Filter candidates based on valid_from and valid_to
            valid_candidates = []
            for candidate in candidates:
                valid_from = candidate.get(self.asof_config.valid_from_column)
                valid_to = candidate.get(self.asof_config.valid_to_column)
                
                if valid_from is None:
                    continue
                
                # Check if event_time falls within [valid_from, valid_to)
                if self.asof_config.operator in ['<=', '<']:
                    # Standard ASOF
                    if valid_from <= event_time:
                        # Check valid_to
                        if valid_to is None:
                            # Open-ended record (current)
                            if self.asof_config.include_open_ended:
                                valid_candidates.append(candidate)
                        elif event_time < valid_to:  # or <= depending on your SCD logic
                            valid_candidates.append(candidate)
                
                elif self.asof_config.operator in ['>=', '>']:
                    # Reverse ASOF (unusual for SCD)
                    if valid_from >= event_time:
                        if valid_to is None or event_time < valid_to:
                            valid_candidates.append(candidate)
            
            # Select best match (should be only one for well-formed SCD)
            if valid_candidates:
                if len(valid_candidates) > 1:
                    # Multiple matches - pick most recent valid_from
                    self.logger.warning(
                        f"Multiple SCD records match for key {eq_key} at time {event_time}. "
                        f"This indicates overlapping validity periods. Using most recent."
                    )
                    best_match = max(valid_candidates,
                                   key=lambda r: r.get(self.asof_config.valid_from_column))
                else:
                    best_match = valid_candidates[0]
                
                full_key = self._dict_to_key_tuple(key_dict)
                
                if self.cardinality == 'one_to_one':
                    matched[full_key] = best_match
                else:
                    matched[full_key] = [best_match]
        
        return matched
    
    def _apply_asof_matching_valid_from_only(self, results: List[Dict], 
                                            lookup_keys: List[Dict[str, Any]]) -> Dict[tuple, Any]:
        """
        Apply ASOF matching for Type 2a SCD (valid_from only).
        
        Finds the most recent record where valid_from <= event_time.
        """
        # Group results by equality keys
        grouped_results = defaultdict(list)
        
        for record in results:
            eq_key = tuple(record.get(cond.backend_key) for cond in self.equality_conditions)
            grouped_results[eq_key].append(record)
        
        matched = {}
        
        for key_dict in lookup_keys:
            eq_key = tuple(key_dict.get(cond.data_key) for cond in self.equality_conditions)
            event_time = key_dict.get(self.asof_config.data_key)
            
            candidates = grouped_results.get(eq_key, [])
            
            # Filter candidates based on ASOF condition
            valid_candidates = []
            for candidate in candidates:
                backend_time = candidate.get(self.asof_config.valid_from_column)
                
                if backend_time is None:
                    continue
                
                if self.asof_config.operator == '<=':
                    if backend_time <= event_time:
                        valid_candidates.append(candidate)
                elif self.asof_config.operator == '<':
                    if backend_time < event_time:
                        valid_candidates.append(candidate)
                elif self.asof_config.operator == '>=':
                    if backend_time >= event_time:
                        valid_candidates.append(candidate)
                elif self.asof_config.operator == '>':
                    if backend_time > event_time:
                        valid_candidates.append(candidate)
            
            # Select best match
            if valid_candidates:
                if self.asof_config.operator in ['<=', '<']:
                    best_match = max(valid_candidates,
                                   key=lambda r: r.get(self.asof_config.valid_from_column))
                else:
                    best_match = min(valid_candidates,
                                   key=lambda r: r.get(self.asof_config.valid_from_column))
                
                full_key = self._dict_to_key_tuple(key_dict)
                
                if self.cardinality == 'one_to_one':
                    matched[full_key] = best_match
                else:
                    matched[full_key] = [best_match]
        
        return matched
    
    async def _batch_fetch_standard(self, lookup_keys: List[Dict[str, Any]]) -> Dict[tuple, Any]:
        """Standard batch fetch (non-ASOF joins)"""
        
        data_keys = [condition.data_key for condition in self.join_config.join_on]
        # print(data_keys)
        # import pdb; pdb.set_trace()
        partition_dimensions = []
        for key in data_keys:
            partition_dimensions.append(DimensionPartitionConfig(column=key, step=500, data_type=self.join_backend.db_column_schema.column(key).data_type, type="value"))
        results = []
        for new_partition, data in build_multi_dimension_partitions_for_delta_data(lookup_keys, partition_dimensions):
            results.extend(await self.join_backend.fetch_partition_data(new_partition, with_hash=True, hash_algo=self.pipeline_config.hash_algo))
        if self.cardinality == 'one_to_one':
            return self._results_to_keyed_dict_one_to_one(results)
        else:
            return self._results_to_keyed_dict_one_to_many(results)
    
    def _build_standard_filters(self, lookup_keys: List[Dict[str, Any]]) -> List[Filter]:
        """Build filters for standard (non-ASOF) joins"""
        filters = []
        
        for condition in self.join_config.join_on:
            if condition.operator == '=':
                values = [key[condition.data_key] for key in lookup_keys]
                unique_values = list(set(values))
                
                filters.append(Filter(
                    column=condition.backend_key,
                    operator='in',
                    value=unique_values
                ))
        
        return filters
    
    def _build_select_fields(self) -> List[Field]:
        """Build list of fields to select from join backend"""
        fields = []
        
        # For ASOF joins, include time columns
        if self.asof_config:
            for condition in self.equality_conditions:
                fields.append(Field(expr=condition.backend_key))
            fields.append(Field(expr=self.asof_config.valid_from_column))
            if self.asof_config.valid_to_column:
                fields.append(Field(expr=self.asof_config.valid_to_column))
        else:
            # Include all join keys
            for condition in self.join_config.join_on:
                fields.append(Field(expr=condition.backend_key))
        
        # Add requested columns
        if self.join_config.fetch_columns:
            for col in self.join_config.fetch_columns:
                col_name = col.get('name')
                col_alias = col.get('alias', col_name)
                fields.append(Field(expr=col_name, alias=col_alias))
        else:
            # Fetch all from backend
            if self.join_backend.column_schema:
                for col in self.join_backend.column_schema.data_columns:
                    fields.append(Field(expr=col.expr, alias=col.name))
        
        return fields
    
    def _results_to_keyed_dict_one_to_one(self, results: List[Dict]) -> Dict[tuple, Dict]:
        """Convert query results to keyed dictionary for one-to-one joins"""
        keyed = {}
        
        for row in results:
            if self.asof_config:
                key_values = [row.get(cond.backend_key) for cond in self.equality_conditions]
                key_values.append(row.get(self.asof_config.valid_from_column))
            else:
                key_values = [row.get(cond.backend_key) for cond in self.join_config.join_on]
            
            key_tuple = tuple(key_values)
            
            if key_tuple not in keyed:
                keyed[key_tuple] = row
        
        return keyed
    
    def _results_to_keyed_dict_one_to_many(self, results: List[Dict]) -> Dict[tuple, List[Dict]]:
        """Convert query results to keyed dictionary for one-to-many joins"""
        keyed = defaultdict(list)
        
        for row in results:
            if self.asof_config:
                key_values = [row.get(cond.backend_key) for cond in self.equality_conditions]
                key_values.append(row.get(self.asof_config.valid_from_column))
            else:
                key_values = [row.get(cond.backend_key) for cond in self.join_config.join_on]
            
            key_tuple = tuple(key_values)
            keyed[key_tuple].append(row)
        
        return dict(keyed)
    
    def _merge_joined_data_one_to_one(self, data: List[Dict], joined_data: Dict[tuple, Dict]):
        """Merge joined data into original data rows for one-to-one joins"""
        for row in data:
            # Build lookup key
            if self.asof_config:
                key_values = [row.get(cond.data_key) for cond in self.equality_conditions]
                key_values.append(row.get(self.asof_config.data_key))
            else:
                key_values = [row.get(cond.data_key) for cond in self.join_config.join_on]
            
            key_tuple = tuple(key_values)
            joined_record = joined_data.get(key_tuple, {})
            
            # Merge based on join type
            if self.join_config.join_type in ['left', 'asof_left']:
                if self.join_config.fetch_columns:
                    for col in self.join_config.fetch_columns:
                        col_name = col.get('name')
                        col_alias = col.get('alias', col_name)
                        row[col_alias] = joined_record.get(col_alias)
                else:
                    # Merge all columns (excluding join keys)
                    if self.asof_config:
                        backend_keys = {cond.backend_key for cond in self.equality_conditions}
                        backend_keys.add(self.asof_config.valid_from_column)
                        if self.asof_config.valid_to_column:
                            backend_keys.add(self.asof_config.valid_to_column)
                    else:
                        backend_keys = {c.backend_key for c in self.join_config.join_on}
                    
                    for k, v in joined_record.items():
                        if k not in row and k not in backend_keys:
                            row[k] = v
            
            elif self.join_config.join_type == 'inner':
                if joined_record:
                    if self.asof_config:
                        backend_keys = {cond.backend_key for cond in self.equality_conditions}
                        backend_keys.add(self.asof_config.valid_from_column)
                        if self.asof_config.valid_to_column:
                            backend_keys.add(self.asof_config.valid_to_column)
                    else:
                        backend_keys = {c.backend_key for c in self.join_config.join_on}
                    
                    for k, v in joined_record.items():
                        if k not in row and k not in backend_keys:
                            row[k] = v
                else:
                    row['_no_match_'] = True
        
        if self.join_config.join_type == 'inner':
            data[:] = [row for row in data if not row.get('_no_match_')]
    
    def _merge_joined_data_one_to_many(self, data: List[Dict], 
                                       joined_data: Dict[tuple, List[Dict]]) -> List[Dict]:
        """Merge joined data for one-to-many joins (flatten)"""
        result = []
        
        for row in data:
            if self.asof_config:
                key_values = [row.get(cond.data_key) for cond in self.equality_conditions]
                key_values.append(row.get(self.asof_config.data_key))
            else:
                key_values = [row.get(cond.data_key) for cond in self.join_config.join_on]
            
            key_tuple = tuple(key_values)
            joined_records = joined_data.get(key_tuple, [])
            
            if self.join_config.join_type in ['left', 'asof_left']:
                if not joined_records:
                    new_row = row.copy()
                    if self.join_config.fetch_columns:
                        for col in self.join_config.fetch_columns:
                            col_alias = col.get('alias', col.get('name'))
                            new_row[col_alias] = None
                    result.append(new_row)
                else:
                    for joined_record in joined_records:
                        new_row = row.copy()
                        
                        if self.asof_config:
                            backend_keys = {cond.backend_key for cond in self.equality_conditions}
                            backend_keys.add(self.asof_config.valid_from_column)
                            if self.asof_config.valid_to_column:
                                backend_keys.add(self.asof_config.valid_to_column)
                        else:
                            backend_keys = {c.backend_key for c in self.join_config.join_on}
                        
                        if self.join_config.fetch_columns:
                            for col in self.join_config.fetch_columns:
                                col_name = col.get('name')
                                col_alias = col.get('alias', col_name)
                                new_row[col_alias] = joined_record.get(col_alias)
                        else:
                            for k, v in joined_record.items():
                                if k not in backend_keys:
                                    new_row[k] = v
                        
                        result.append(new_row)
            
            elif self.join_config.join_type == 'inner':
                # cant support inner join here because we need to return all rows from the left table for syncing to work
                for joined_record in joined_records:
                    new_row = row.copy()
                    
                    if self.asof_config:
                        backend_keys = {cond.backend_key for cond in self.equality_conditions}
                        backend_keys.add(self.asof_config.valid_from_column)
                        if self.asof_config.valid_to_column:
                            backend_keys.add(self.asof_config.valid_to_column)
                    else:
                        backend_keys = {c.backend_key for c in self.join_config.join_on}
                    
                    if self.join_config.fetch_columns:
                        for col in self.join_config.fetch_columns:
                            col_name = col.get('name')
                            col_alias = col.get('alias', col_name)
                            new_row[col_alias] = joined_record.get(col_alias)
                    else:
                        for k, v in joined_record.items():
                            if k not in backend_keys:
                                new_row[k] = v
                    
                    result.append(new_row)
        
        return result
    
    def _dict_to_key_tuple(self, key_dict: Dict[str, Any]) -> tuple:
        """Convert key dict to consistent tuple for caching"""
        if self.asof_config:
            data_keys = [cond.data_key for cond in self.equality_conditions] + [self.asof_config.data_key]
        else:
            data_keys = [condition.data_key for condition in self.join_config.join_on]
        return tuple(key_dict.get(k) for k in data_keys)


"""
=========================
CONFIGURATION EXAMPLES
=========================

# Example 1: Type 2a SCD (valid_from only) - Legacy format
- name: 'join_user_status_scd_type2a'
  type: 'join'
  enabled: true
  source:
    name: user_status_scd_backend
  config:
    join_type: 'asof_left'
    cardinality: 'one_to_one'
    merge: true
    join_on:
      - data_key: 'user_id'
        backend_key: 'user_id'
        operator: '='
      - data_key: 'event_time'
        backend_key: 'valid_from'
        operator: '<='
    fetch_columns:
      - name: 'status'
      - name: 'tier'

# Example 2: Type 2b SCD (valid_from + valid_to) - NEW OPTIMIZED FORMAT
- name: 'join_user_status_scd_type2b'
  type: 'join'
  enabled: true
  source:
    name: user_status_scd_backend
  config:
    join_type: 'asof_left'
    cardinality: 'one_to_one'
    merge: true
    # Equality conditions (for composite keys)
    join_on:
      - data_key: 'user_id'
        backend_key: 'user_id'
        operator: '='
    # ASOF-specific config for SCD with valid_from and valid_to
    asof:
      data_key: 'event_time'              # Time column in data
      valid_from_column: 'valid_from'      # Start of validity
      valid_to_column: 'valid_to'          # End of validity (NULL = current)
      operator: '<='                       # event_time comparison
      include_open_ended: true             # Include records where valid_to IS NULL
    fetch_columns:
      - name: 'status'
        alias: 'user_status_at_event'
      - name: 'tier'
      - name: 'valid_from'                 # Optional: include for debugging
      - name: 'valid_to'

# Example 3: Multi-key Type 2b SCD with partition optimization
- name: 'join_account_balance_scd'
  type: 'join'
  enabled: true
  source:
    name: account_balance_scd_backend
  config:
    join_type: 'asof_left'
    cardinality: 'one_to_one'
    merge: true
    # Multiple equality conditions
    join_on:
      - data_key: 'user_id'
        backend_key: 'user_id'
        operator: '='
      - data_key: 'account_id'
        backend_key: 'account_id'
        operator: '='
    # ASOF for SCD Type 2b
    asof:
      data_key: 'transaction_time'
      valid_from_column: 'snapshot_from'
      valid_to_column: 'snapshot_to'
      operator: '<='
      include_open_ended: true
    fetch_columns:
      - name: 'balance'
      - name: 'currency'
    # Partition bounds automatically used for optimization:
    # Query becomes:
    #   WHERE user_id IN (...) AND account_id IN (...)
    #   AND snapshot_from <= partition_end
    #   AND (snapshot_to >= partition_start OR snapshot_to IS NULL)
    # This uses index on (user_id, account_id, snapshot_from, snapshot_to) efficiently!

# Example 4: Fetch-only mode for validation
- name: 'validate_user_active'
  type: 'join'
  enabled: true
  source:
    name: user_status_scd_backend
  config:
    join_type: 'asof_left'
    cardinality: 'one_to_one'
    merge: false                           # Don't merge into rows
    output_key: 'user_status_at_event'    # Store in metadata
    join_on:
      - data_key: 'user_id'
        backend_key: 'user_id'
        operator: '='
      asof:
        data_key: 'event_time'
        valid_from_column: 'valid_from'
        valid_to_column: 'valid_to'
        operator: '<='
        include_open_ended: true
      fetch_columns:
        - name: 'status'
      cache:
        enabled: false  # ASOF joins are time-dependent, caching can be tricky

# Example 5: FIFO cache (predictable eviction for testing)
- name: 'join_with_fifo'
  type: 'join'
  enabled: true
  source:
    name: metadata_backend
  config:
    join_type: 'left'
    join_on:
      - data_key: 'item_id'
        backend_key: 'item_id'
        operator: '='
      cache:
        enabled: true
        type: 'memory'
        policy: 'fifo'   # First In First Out eviction
        max_size: 1000
"""
