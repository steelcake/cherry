import logging
from typing import Dict
from copy import deepcopy
import asyncio

import pyarrow as pa
from deltalake import write_deltalake

from ..config import DeltaLakeWriterConfig
from ..writers.base import DataWriter

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: DeltaLakeWriterConfig):
        self.config = deepcopy(config)
        self.config.data_uri = self.config.data_uri.rstrip("/")

    async def write_table(self, table_name: str, table_data: pa.Table) -> None:
        if table_data.num_rows == 0:
            return

        await asyncio.to_thread(
            write_deltalake,
            table_or_uri=f"{self.config.data_uri}/{table_name}",
            data=table_data,
            schema=table_data.schema,
            partition_by=self.config.partition_by.get(table_name, None),
            mode="append",
            schema_mode="merge",
            storage_options=self.config.storage_options,
        )

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        # insert into all tables except the anchor table in parallel
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.config.anchor_table:
                continue

            task = asyncio.create_task(
                self.write_table(table_name, table_data),
                name=f"write to {table_name}",
            )

            tasks.append(task)

        for task in tasks:
            await task

        # insert into anchor table after all other inserts are done
        if self.config.anchor_table is not None:
            table_data = data[self.config.anchor_table]
            await self.write_table(self.config.anchor_table, table_data)
