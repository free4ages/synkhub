def get_source_class(source_type: str):
    if source_type == "postgres":
        from .postgres import PostgresSource
        return PostgresSource
    raise ValueError(f"Unsupported source type: {source_type}")