from typing import Tuple, List, Literal
from ..core.query_models import Query

class SqlBuilder:
    @staticmethod
    def build(
        query: Query,
        dialect: Literal['psycopg', 'asyncpg', 'mysql', 'sqlalchemy'] = 'psycopg'
    ) -> Tuple[str, List]:
        parts = []
        params = []

        def get_placeholder(index: int) -> str:
            if dialect in ('psycopg', 'mysql'):
                return "%s"
            elif dialect == 'asyncpg':
                return f"${index + 1}"  # asyncpg starts from $1
            elif dialect == 'sqlalchemy':
                return f":param{index}"
            else:
                raise ValueError(f"Unsupported dialect: {dialect}")

        def handle_filter(flt, filter_exprs, params):
            """Handle individual filter, including composite tuple filtering"""
            
            # Handle composite filtering
            if getattr(flt, 'is_composite', False) and flt.is_composite:
                if flt.operator.upper() == 'COMPOSITE_RANGE':
                    # Handle composite range filtering: (col1, col2) >= (start1, start2) AND (col1, col2) < (end1, end2)
                    if not (flt.start_values and flt.end_values):
                        raise ValueError("Composite range filter requires start_values and end_values")
                        
                    column_list = f"({', '.join(flt.columns)})"
                    
                    # Build >= condition
                    start_placeholders = []
                    for val in flt.start_values:
                        placeholder = get_placeholder(len(params))
                        start_placeholders.append(placeholder)
                        params.append(val)
                    filter_exprs.append(f"{column_list} >= ({', '.join(start_placeholders)})")
                    
                    # Build < condition  
                    end_placeholders = []
                    for val in flt.end_values:
                        placeholder = get_placeholder(len(params))
                        end_placeholders.append(placeholder)
                        params.append(val)
                    filter_exprs.append(f"{column_list} < ({', '.join(end_placeholders)})")
                    
                elif flt.operator.upper() == 'COMPOSITE_MIXED':
                    # Handle mixed range and value filtering
                    if not flt.composite_bound:
                        raise ValueError("Composite mixed filter requires composite_bound")
                    
                    bound = flt.composite_bound
                    range_dims = bound.get_range_dimensions()
                    value_dims = bound.get_value_dimensions()
                    
                    conditions = []
                    
                    # Handle range dimensions
                    for dim_idx in range_dims:
                        column = bound.columns[dim_idx]
                        start_val, end_val = bound.dimension_values[dim_idx]  # tuple (start, end)
                        
                        start_placeholder = get_placeholder(len(params))
                        params.append(start_val)
                        conditions.append(f"{column} >= {start_placeholder}")
                        
                        end_placeholder = get_placeholder(len(params))
                        params.append(end_val)
                        conditions.append(f"{column} < {end_placeholder}")
                    
                    # Handle value dimensions
                    if len(value_dims) == 1:
                        # Single value dimension - use regular IN
                        column = bound.columns[value_dims[0]]
                        values = bound.dimension_values[value_dims[0]]
                        if not isinstance(values, (list, tuple)):
                            values = [values]
                        
                        placeholders = []
                        for val in values:
                            placeholder = get_placeholder(len(params))
                            placeholders.append(placeholder)
                            params.append(val)
                        conditions.append(f"{column} IN ({', '.join(placeholders)})")
                    elif len(value_dims) > 1:
                        # Multiple value dimensions - use tuple IN
                        value_columns = [bound.columns[i] for i in value_dims]
                        column_list = f"({', '.join(value_columns)})"
                        
                        # Get all combinations of values for value dimensions
                        value_lists = [bound.dimension_values[i] for i in value_dims]
                        
                        # Ensure all value dimensions have lists
                        for i, val_list in enumerate(value_lists):
                            if not isinstance(val_list, (list, tuple)):
                                value_lists[i] = [val_list]
                        
                        # Generate cartesian product of value combinations
                        import itertools
                        value_combinations = list(itertools.product(*value_lists))
                        
                        tuple_placeholders = []
                        for combo in value_combinations:
                            tuple_params = []
                            for val in combo:
                                placeholder = get_placeholder(len(params))
                                tuple_params.append(placeholder)
                                params.append(val)
                            tuple_placeholders.append(f"({', '.join(tuple_params)})")
                        
                        conditions.append(f"{column_list} IN ({', '.join(tuple_placeholders)})")
                    
                    # Combine all conditions with AND
                    if conditions:
                        filter_exprs.append(f"({' AND '.join(conditions)})")
                        
                elif flt.operator.upper() == 'IN':
                    # Handle composite tuple IN filtering: (col1, col2) IN ((val1, val2), (val3, val4))
                    if isinstance(flt.value, (list, tuple)) and all(isinstance(v, (list, tuple)) for v in flt.value):
                        if not flt.value:  # Empty list
                            filter_exprs.append("1=0")  # Always false condition
                            return
                            
                        column_list = f"({', '.join(flt.columns)})"
                        tuple_placeholders = []
                        
                        for value_tuple in flt.value:
                            if len(value_tuple) != len(flt.columns):
                                raise ValueError(f"Tuple length {len(value_tuple)} doesn't match columns length {len(flt.columns)}")
                                
                            tuple_params = []
                            for val in value_tuple:
                                placeholder = get_placeholder(len(params))
                                tuple_params.append(placeholder)
                                params.append(val)
                            tuple_placeholders.append(f"({', '.join(tuple_params)})")
                        
                        filter_exprs.append(f"{column_list} IN ({', '.join(tuple_placeholders)})")
                    else:
                        raise ValueError("Composite tuple filter requires list of tuples as value")
                return
                
            # Handle regular single-column filtering
            if flt.operator.upper() == 'IN':
                # Handle IN operator with list of values
                if isinstance(flt.value, (list, tuple)):
                    if not flt.value:  # Empty list
                        filter_exprs.append("1=0")  # Always false condition
                        return
                    
                    placeholders = []
                    for value in flt.value:
                        placeholder = get_placeholder(len(params))
                        placeholders.append(placeholder)
                        params.append(value)
                    
                    filter_exprs.append(f"{flt.column} IN ({', '.join(placeholders)})")
                else:
                    # Single value IN query (treat as equals)
                    placeholder = get_placeholder(len(params))
                    filter_exprs.append(f"{flt.column} IN ({placeholder})")
                    params.append(flt.value)
            elif flt.operator.upper() == 'NOT IN':
                # Handle NOT IN operator with list of values
                if isinstance(flt.value, (list, tuple)):
                    if not flt.value:  # Empty list
                        filter_exprs.append("1=1")  # Always true condition
                        return
                    
                    placeholders = []
                    for value in flt.value:
                        placeholder = get_placeholder(len(params))
                        placeholders.append(placeholder)
                        params.append(value)
                    
                    filter_exprs.append(f"{flt.column} NOT IN ({', '.join(placeholders)})")
                else:
                    # Single value NOT IN query (treat as not equals)
                    placeholder = get_placeholder(len(params))
                    filter_exprs.append(f"{flt.column} NOT IN ({placeholder})")
                    params.append(flt.value)
            else:
                # Handle regular operators (=, >, <, >=, <=, !=, LIKE, etc.)
                placeholder = get_placeholder(len(params))
                filter_exprs.append(f"{flt.column} {flt.operator} {placeholder}")
                params.append(flt.value)

        # ---------------------------
        # DELETE QUERY
        # ---------------------------
        if getattr(query, "action", "select") == "delete":
            # FROM clause
            table = query.table
            delete_clause = "DELETE FROM "
            if table.schema:
                delete_clause += f"{table.schema}."
            delete_clause += table.table
            if table.alias:
                delete_clause += f" AS {table.alias}"
            parts.append(delete_clause)

            # JOINs (allowed in some backends)
            for j in query.joins or []:
                join_clause = f"{j.type.upper()} JOIN {j.table}"
                if j.alias:
                    join_clause += f" AS {j.alias}"
                join_clause += f" ON {j.on}"
                parts.append(join_clause)

            # WHERE clause
            if query.filters:
                filter_exprs = []
                for flt in query.filters:
                    handle_filter(flt, filter_exprs, params)
                parts.append("WHERE " + " AND ".join(filter_exprs))

            # LIMIT (only supported in some DBs)
            if query.limit is not None:
                placeholder = get_placeholder(len(params))
                parts.append(f"LIMIT {placeholder}")
                params.append(query.limit)

            # OFFSET
            if query.offset is not None:
                placeholder = get_placeholder(len(params))
                parts.append(f"OFFSET {placeholder}")
                params.append(query.offset)

            return "\n".join(parts), params

        # ---------------------------
        # SELECT QUERY (default)
        # ---------------------------
        # SELECT clause
        select_clause = []
        for f in query.select or []:
            if f.alias:
                select_clause.append(f"{f.expr} AS {f.alias}")
            else:
                select_clause.append(f.expr)
        parts.append("SELECT " + ", ".join(select_clause))

        # FROM clause
        table = query.table
        from_clause = "FROM "
        if table.schema:
            from_clause += f"{table.schema}."
        from_clause += table.table
        if table.alias:
            from_clause += f" AS {table.alias}"
        parts.append(from_clause)

        # JOINs
        for j in query.joins or []:
            join_clause = f"{j.type.upper()} JOIN {j.table}"
            if j.alias:
                join_clause += f" AS {j.alias}"
            join_on_parts = []
            for on_part in j.on.split(" AND "):
                if '=' in on_part and not (on_part.strip().startswith('(') or on_part.strip().endswith(')')):
                    left, right = on_part.split('=')
                    join_on_parts.append(f"{left.strip()} = {right.strip()}")
                else:
                    join_on_parts.append(on_part)
            join_clause += f" ON {' AND '.join(join_on_parts)}"
            # join_clause += f" ON {j.on}"
            parts.append(join_clause)

        # WHERE clause
        if query.filters:
            filter_exprs = []
            for flt in query.filters:
                handle_filter(flt, filter_exprs, params)
            parts.append("WHERE " + " AND ".join(filter_exprs))

        # GROUP BY
        if query.group_by:
            parts.append("GROUP BY " + ", ".join(f.expr for f in query.group_by))

        # ORDER BY
        if query.order_by:
            parts.append("ORDER BY " + ", ".join(query.order_by))

        # LIMIT
        if query.limit is not None:
            placeholder = get_placeholder(len(params))
            parts.append(f"LIMIT {placeholder}")
            params.append(query.limit)

        # OFFSET
        if query.offset is not None:
            placeholder = get_placeholder(len(params))
            parts.append(f"OFFSET {placeholder}")
            params.append(query.offset)

        return "\n".join(parts), params
