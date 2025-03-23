import logging
from typing import Dict
import pyarrow as pa
import pyarrow.dataset as pa_dataset
from .base import DataWriter
from ..config import PyArrowDatasetWriterConfig
import asyncio
from copy import deepcopy

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: PyArrowDatasetWriterConfig):
        self.config = deepcopy(config)
        self.config.base_dir = self.config.base_dir.rstrip("/")

    async def _write_table(self, table_name: str, table_data: pa.Table) -> None:
        await asyncio.to_thread(
            pa_dataset.write_dataset,
            data=table_data,
            base_dir=f"{self.config.base_dir}/{table_name}",
            basename_template=self.config.basename_template,
            format="parquet",
            partitioning=self.config.partitioning.get(table_name, None),
            partitioning_flavor=self.config.partitioning_flavor.get(table_name, None),
            filesystem=self.config.filesystem,
            file_options=self.config.file_options,
            use_threads=self.config.use_threads,
            max_partitions=self.config.max_partitions,
            max_open_files=self.config.max_open_files,
            max_rows_per_file=self.config.max_rows_per_file,
            min_rows_per_group=self.config.min_rows_per_group,
            max_rows_per_group=self.config.max_rows_per_group,
            existing_data_behavior="overwrite_or_ignore",
            create_dir=self.config.create_dir,
        )

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.config.anchor_table:
                continue

            task = asyncio.create_task(
                self._write_table(table_name, table_data), name=f"write to {table_name}"
            )
            tasks.append(task)

        for task in tasks:
            await task

        if self.config.anchor_table:
            await self._write_table(
                self.config.anchor_table, data[self.config.anchor_table]
            )
