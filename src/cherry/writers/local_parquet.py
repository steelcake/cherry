import logging
from typing import Dict
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
from datetime import datetime
from .base import DataWriter
from ..config import LocalParquetWriterConfig
import asyncio

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: LocalParquetWriterConfig):
        self.output_dir = Path(config.output_dir)

        os.makedirs(self.output_dir, exist_ok=True)

    async def _write_table(self, table_name: str, table: pa.Table) -> None:
        output_path = self.output_dir / table_name

        os.makedirs(output_path, exist_ok=True)

        # Generate timestamp-based filename using datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output_file = output_path / f"data_{timestamp}.parquet"
        pq.write_table(table, output_file)

        logger.debug(f"Written table {table_name} to {output_file}")

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        # Write all tables in parallel
        tasks = []
        for table_name, table_data in data.items():
            task = asyncio.create_task(
                self._write_table(table_name, table_data), name=f"write to {table_name}"
            )
            tasks.append(task)

        # Wait for all writes to complete
        for task in tasks:
            await task
