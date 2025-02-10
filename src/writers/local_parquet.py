import asyncio, logging
from typing import Dict
from src.writers.base import DataWriter
from src.config.parser import Output
import pyarrow as pa, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.utils.writer import get_output_path
import os
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path

logger = logging.getLogger(__name__)

class ParquetWriter(DataWriter):
    def __init__(self, config: Output):
        logger.info("Initializing Parquet writer...")
        self._init_config(config)
        logger.info(f"Initialized ParquetWriter with output path {self.path}")

    def _init_config(self, config: Output) -> None:
        """Initialize Parquet configuration"""
        self.path = Path(config.output_path)
        if not self.path:
            raise ValueError("output_path is required")
            
        # Create output directory if it doesn't exist
        self.path.mkdir(parents=True, exist_ok=True)

    def _convert_to_table(self, df: pa.RecordBatch | pa.Table) -> pa.Table:
        """Convert input to PyArrow Table"""
        if isinstance(df, pa.RecordBatch):
            return pa.Table.from_batches([df])
        elif isinstance(df, pa.Table):
            return df
        raise TypeError(f"Expected RecordBatch or Table, got {type(df)}")

    async def write_parquet(self, table: str, data: pa.Table) -> None:
        """Write data to parquet file"""
        try:
            df = data.to_pandas()
            
            # Sort by block number to ensure sequential order
            df = df.sort_values('block_number')
            min_block = df['block_number'].min()
            max_block = df['block_number'].max()

            logger.info(f"Writing {len(df)} rows to {table} with Min block: {min_block}, Max block: {max_block}")

            
            # Create output directory if needed
            if table.endswith('_events'):
                event_type = table.replace('_events', '')
                output_dir = self.path / 'events' / event_type
            else:
                output_dir = self.path / table
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with timestamp and block range
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{min_block}_{max_block}.parquet"
            output_path = output_dir / filename
            
            logger.info(f"Writing batch chunk with {len(df)} rows to {output_path} (blocks {min_block} to {max_block})")
            
            # Ensure data is sorted before writing
            df = df.sort_values('block_number')
            pq.write_table(
                pa.Table.from_pandas(df),
                str(output_path),  # Convert Path to string for pyarrow
                compression='snappy'
            )
            logger.info(f"Successfully wrote batch chunk with {len(df)} rows (blocks {min_block} to {max_block})")
            
        except Exception as e:
            logger.error(f"Error writing parquet file: {e}")
            raise

    async def _write_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Write DataFrame to Parquet using thread pool"""
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
        """Push data to Parquet files"""
        logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")
        
        await self._write_events(data)
        await self._write_blocks(data)

    async def _write_events(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write event data to Parquet files"""
        event_tasks = {
            name: asyncio.create_task(self.write_parquet(name, df), name=f"write_{name}")
            for name, df in data.items() 
            if name.endswith('_events')
        }
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing parquet table into {name}: {e}")

    async def _write_blocks(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write combined blocks data to Parquet"""
        blocks_tables = [name for name in data if name.startswith('blocks_')]
        if blocks_tables:
            try:
                blocks_data = self.combine_blocks(data)
                await self.write_parquet('blocks', blocks_data)
            except Exception as e:
                logger.error(f"Error writing blocks anchor table: {e}")
                raise 