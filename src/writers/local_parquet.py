import asyncio, logging
from typing import Dict
from src.writers.base import DataWriter
from src.config.parser import WriterConfig
import pyarrow as pa, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.utils.writer import get_output_path
import os
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path

logger = logging.getLogger(__name__)

class ParquetWriter(DataWriter):
    def __init__(self, writer_config: WriterConfig):
        self._init_config(writer_config)
        logger.info(f"Initialized ParquetWriter with output path {self.path}")

    def _init_config(self, writer_config: WriterConfig) -> None:
        self.path = Path(writer_config.path)
        self.anchor_table = writer_config.anchor_table
        
        if not self.path:
            raise ValueError("output_path is required")          
        # Create output directory if it doesn't exist
        self.path.mkdir(parents=True, exist_ok=True)

    async def write_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Write data to parquet file"""
        def write():
            logger.info(f"Writing {len(df)} rows to {path}")
            df.to_parquet(
                path,
                index=False,
                compression=None
            )
            logger.info(f"Successfully wrote {len(df)} rows to {path}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, write)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")

            if self.anchor_table is None:
                await self._write_tables(data)
            else:
                non_anchor_tables = {k: v for k, v in data.items() if k != self.anchor_table}
                anchor_tables = {k: v for k, v in data.items() if k == self.anchor_table}
                await self._write_tables(non_anchor_tables)
                await self._write_tables(anchor_tables)
            
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            raise

    async def _write_tables(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write event data to S3"""
        event_tasks = {
            table_name: asyncio.create_task(self.write_parquet(df=df.to_pandas(), path=self.path / table_name), name=f"write_{table_name}")
            for table_name, df in data.items()
        }
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing parquet table into {name}: {e}")
            

    """async def push_data(combined_data: Dict[str, pa.RecordBatch], writer: Writer) -> bool:
    
    logger.info(f"Writer: {writer}")

    logger.info(f"Writing data to {base_path}")
    
    # Create base data directory if it doesn't exist
    os.makedirs(base_path, exist_ok=True)
    
    # Get current timestamp for filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Write each table to a parquet file in its own subdirectory
    for table_name, data in combined_data.items():
        # Create table-specific subdirectory
        table_dir = os.path.join(base_path, table_name)
        os.makedirs(table_dir, exist_ok=True)
        
        # Create filename with timestamp
        output_path = os.path.join(table_dir, f"{table_name}_{timestamp}.parquet")
        
        # Convert RecordBatch to Table before writing
        table = pa.Table.from_batches([data])
        pq.write_table(table, output_path)

        logger.info(f"Wrote table {table_name} to {output_path}")

    return True"""