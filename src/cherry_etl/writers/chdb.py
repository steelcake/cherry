import asyncio
from typing import Dict
import pyarrow as pa
import chdb
from contextlib import closing

from .base import DataWriter
from ..config import ChdbWriterConfig
from .clickhouse import pyarrow_type_to_clickhouse


class Writer(DataWriter):
    def __init__(self, config: ChdbWriterConfig):
        self.db_path = config.db_path
        self.first_insert = True
        self.engine = config.engine if hasattr(config, "engine") else "MergeTree"

    async def _create_table_if_not_exists(
        self, db_name: str, table_name: str, schema: pa.Schema
    ) -> None:
        with closing(chdb.connect(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} ENGINE = Atomic")

            columns = [
                f"`{field.name}` {pyarrow_type_to_clickhouse(field.type)}"
                for field in schema
            ]
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
                {", ".join(columns)}
            ) ENGINE = {self.engine} 
            ORDER BY tuple()
            """
            cursor.execute(create_table_sql)

    async def _insert_data(self, full_table_name: str, table_data: pa.Table) -> None:
        with closing(chdb.connect(self.db_path)) as conn:
            conn.query(
                f"INSERT INTO {full_table_name} SELECT * FROM Python(table_data)"
            )

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        if self.first_insert:
            create_tasks = []
            for full_table_name, table_data in data.items():
                db_name, table_name = full_table_name.split(".", 1)
                task = asyncio.create_task(
                    self._create_table_if_not_exists(
                        db_name, table_name, table_data.schema
                    ),
                    name=f"create table {full_table_name}",
                )
                create_tasks.append(task)
            await asyncio.gather(*create_tasks)
            self.first_insert = False

        insert_tasks = []
        for full_table_name, table_data in data.items():
            task = asyncio.create_task(
                self._insert_data(full_table_name, table_data),
                name=f"insert into {full_table_name}",
            )
            insert_tasks.append(task)
        await asyncio.gather(*insert_tasks)
