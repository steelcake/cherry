import asyncio, logging
from typing import Dict
from ..writers.base import DataWriter
from ..config.parser import WriterConfig
import pyarrow as pa, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        event_tasks = {}
        for table_name, df in data.items():
            # Create table directory if it doesn't exist
            table_dir = self.path / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            
            event_tasks[table_name] = asyncio.create_task(
                self.write_parquet(
                    df=df.to_pandas(), 
                    path=table_dir / f"{table_name}_{timestamp}.parquet"
                ),
                name=f"write_{table_name}"
            )
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing parquet table into {name}: {e}")
            