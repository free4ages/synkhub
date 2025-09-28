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
            """Handle individual filter, including IN queries"""
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
