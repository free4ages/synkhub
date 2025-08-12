import asyncpg

class PostgresSource:
    def __init__(self, config):
        self.config = config
        self.pool = None

    async def connect(self, config: dict):
        self.pool = await asyncpg.create_pool(**config["connection"])

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def batch_fetch(self, keys: list) -> dict:
        table = self.config["table"]
        key_col = self.config["key"] or "id"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {table} WHERE {key_col} = ANY($1)", keys
            )
        return {row[key_col]: dict(row) for row in rows}
