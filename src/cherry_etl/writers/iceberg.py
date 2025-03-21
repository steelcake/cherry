import logging
from typing import Dict
from ..writers.base import DataWriter
from ..config import IcebergWriterConfig
import pyarrow as pa

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: IcebergWriterConfig):
        logger.debug("Initializing Iceberg writer...")

        try:
            config.catalog.create_namespace(
                config.namespace,
            )
        except Exception as e:
            logger.warning(f"Error creating namespace: {e}")

        logger.debug(f"Created namespace: {config.namespace}")

        self.namespace = config.namespace
        self.first_write = True
        self.write_location = config.write_location
        self.catalog = config.catalog

    async def write_table(self, table_name: str, arrow_table: pa.Table) -> None:
        logger.debug(f"Writing table: {table_name}")

        table_identifier = f"{self.namespace}.{table_name}"

        iceberg_table = self.catalog.load_table(table_identifier)
        iceberg_table.append(arrow_table)

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        if self.first_write:
            for table_name, table_data in data.items():
                table_identifier = f"{self.namespace}.{table_name}"
                self.catalog.create_table_if_not_exists(
                    identifier=table_identifier,
                    schema=table_data.schema,
                    location=self.write_location,
                )

            self.first_write = False

        for table_name, arrow_table in data.items():
            await self.write_table(table_name, arrow_table)
