import logging
from typing import Dict
import pyarrow as pa
from .base import DataWriter
from ..config import DuckdbWriterConfig
import asyncio
import duckdb

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: DuckdbWriterConfig):
        self.connection = config.connection
        self.first_push = True

    def push_data_impl(self, data: Dict[str, pa.Table]) -> None:
        self.connection.begin()

        if self.first_push:
            for table_name, table_data in data.items():
                try:
                    self.connection.table(table_name)
                    self.connection.sql(
                        f"INSERT INTO {table_name} SELECT * FROM table_data"
                    )
                except duckdb.CatalogException:
                    logger.debug(
                        f"creating table {table_name} as it doesn't exist in the database yet"
                    )
                    self.connection.sql(
                        f"CREATE TABLE {table_name} AS SELECT * FROM table_data"
                    )

                # ignore lint warning relating to unused variable
                # this variable is used in the sql query string
                _ = table_data

            self.first_push = False
        else:
            for table_name, table_data in data.items():
                self.connection.sql(
                    f"INSERT INTO {table_name} SELECT * FROM table_data"
                )
                # ignore lint warning relating to unused variable
                # this variable is used in the sql query string
                _ = table_data

        self.connection.commit()

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        await asyncio.to_thread(self.push_data_impl, data)
