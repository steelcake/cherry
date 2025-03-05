import logging
from typing import Dict
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
from datetime import datetime
from .base import DataWriter
from ..config import PyArrowDatasetWriterConfig, ExistingDataBehavior
import asyncio

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: PyArrowDatasetWriterConfig):
        self.output_dir = Path(config.output_dir)
        self.partition_cols = config.partition_cols
        self.anchor_table = config.anchor_table
        self.max_partitions = config.max_partitions
        self.filesystem = config.filesystem
        self.use_threads = config.use_threads
        self.existing_data_behavior: ExistingDataBehavior = (
            config.existing_data_behavior
        )
        os.makedirs(self.output_dir, exist_ok=True)

    async def _write_table(self, table_name: str, table: pa.Table) -> None:
        output_path = self.output_dir / table_name
        os.makedirs(output_path, exist_ok=True)

        partition_cols = (
            self.partition_cols.get(table_name, []) if self.partition_cols else []
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        base_filename = f"{timestamp}_{{i}}.parquet"

        try:
            pq.write_to_dataset(
                table,
                root_path=str(output_path),
                partition_cols=partition_cols,
                filesystem=self.filesystem,
                basename_template=base_filename,
                use_threads=self.use_threads,
                existing_data_behavior=self.existing_data_behavior,
                max_partitions=self.max_partitions,
            )
        except Exception as e:
            logger.error(f"Error writing table {table_name}: {str(e)}")
            raise

        logger.debug(
            f"Written table {table_name} to {output_path} with partitioning {partition_cols}"
        )

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.anchor_table:
                continue

            task = asyncio.create_task(
                self._write_table(table_name, table_data), name=f"write to {table_name}"
            )
            tasks.append(task)

        for task in tasks:
            await task

        if self.anchor_table and self.anchor_table in data:
            await self._write_table(self.anchor_table, data[self.anchor_table])
