import logging
from typing import Dict
from ..writers.base import DataWriter
from ..config import IcebergWriterConfig
import pyarrow as pa

logger = logging.getLogger(__name__)

class Writer(DataWriter):
    def __init__(self, config: IcebergWriterConfig):
        logger.info("Initializing Iceberg writer...")

        try:
            config.catalog.create_namespace(
                config.namespace,
                properties={"location": config.write_location}
            )
        except Exception as e:
            logger.warning(f"Error creating namespace: {e}")
        
        logger.info(f"Created namespace: {config.namespace}")

        self.namespace = config.namespace
        self.first_write = True
        self.write_location = config.write_location
        self.catalog = config.catalog

    async def write_table(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        logger.info(f"Writing table: {table_name}")
        
        table_identifier = f"s3://blockchain-data/{self.namespace}.{table_name}"
        
        arrow_table = pa.Table.from_batches([record_batch])

        iceberg_table = self.catalog.load_table(table_identifier)
        iceberg_table.append(arrow_table)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        if self.first_write:
            for table_name, table_data in data.items():
                table_identifier = f"s3://blockchain-data/{self.namespace}.{table_name}"
                
                try:
                    self.catalog.create_table(
                        identifier=table_identifier,
                        schema=table_data.schema,
                        location=self.write_location
                    )
                except Exception as e:
                    logger.warning(f"Error creating table: {e}")
                
            self.first_write = False

        for table_name, record_batch in data.items():
            await self.write_table(table_name, record_batch)
