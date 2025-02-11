import asyncio
import logging
from typing import Dict, Optional
from src.types.data import Data
from src.writers.base import DataWriter
from src.writers.local_parquet import ParquetWriter
from src.writers.postgres import PostgresWriter
from src.writers.s3 import S3Writer
from src.writers.clickhouse import ClickHouseWriter
from src.writers.aws_wrangler_s3 import AWSWranglerWriter
from src.config.parser import Config
import pyarrow as pa
import pandas as pd

logger = logging.getLogger(__name__)

class Writer:
    WRITER_CLASSES = {
        'aws_wrangler_s3': AWSWranglerWriter,
        'local_parquet': ParquetWriter,
        'postgres': PostgresWriter,
        's3': S3Writer,
        'clickhouse': ClickHouseWriter
    }

    def __init__(self, writers: Dict[str, DataWriter]):
        self._writers = writers
        # Set combine_blocks method for each writer
        for writer in self._writers.values():
            writer.set_combine_blocks(self.combine_blocks)
        logger.info(f"Initialized {len(writers)} writers: {', '.join(writers.keys())}")

    @classmethod
    def initialize_writers(cls, config: Config) -> Dict[str, DataWriter]:
        """Initialize configured writers"""
        writers = {}
        for output in config.output:
            if output.kind in cls.WRITER_CLASSES:
                logger.info(f"Initializing {output.kind} writer")
                writers[output.kind] = cls.WRITER_CLASSES[output.kind](output)
        return writers

    @staticmethod
    def combine_blocks(data: Dict[str, pa.RecordBatch]) -> Optional[pa.Table]:
        """Combine and deduplicate block tables"""
        blocks_tables = [name for name in data if name.startswith('blocks_')]
        if not blocks_tables:
            return None
        
        # Combine all blocks into one table
        blocks_data = pa.concat_tables([
            pa.Table.from_batches([data[name]]) 
            for name in blocks_tables
        ])
        
        # Convert to pandas for easier manipulation
        blocks_df = blocks_data.to_pandas()
        
        # Get block range from events
        event_tables = [name for name in data if name.endswith('_events')]
        if event_tables:
            # Find min/max blocks across all event tables
            event_blocks = pd.concat([
                data[table].to_pandas()['block_number'] 
                for table in event_tables
            ])
            min_block = event_blocks.min()
            max_block = event_blocks.max()
            
            # Filter blocks to event range
            blocks_df = blocks_df[
                (blocks_df['block_number'] >= min_block) & 
                (blocks_df['block_number'] <= max_block)
            ]
            logger.info(f"Filtered blocks to event range {min_block} to {max_block}")
        
        # Sort and deduplicate
        blocks_df = (blocks_df
                    .sort_values('block_number')
                    .drop_duplicates(subset=['block_number'], keep='last'))
        
        logger.info(f"Combined {len(blocks_df)} unique blocks from "
                   f"{blocks_df['block_number'].min()} to {blocks_df['block_number'].max()}")
        
        return pa.Table.from_pandas(blocks_df)

    async def write(self, data: Data) -> None:
        """Write data to all configured targets"""
        if not self._writers or not data or not data.events:
            return

        try:
            # Convert data to RecordBatch format
            record_batches = {}
            
            # Process events
            for event_name, events_df in data.events.items():
                table = events_df.to_arrow()
                df = table.to_pandas().sort_values('block_number')
                min_block = df['block_number'].min()
                max_block = df['block_number'].max()
                table_name = f"{event_name.lower()}_events"
                logger.info(f"Processing {table_name}: {len(df)} events, blocks {min_block} to {max_block}")
                record_batches[table_name] = pa.Table.from_pandas(df).to_batches()[0]
            
            # Process blocks
            for event_name, blocks_df in data.blocks.items():
                table = blocks_df.to_arrow()
                df = table.to_pandas().sort_values('block_number')
                table_name = f"blocks_{event_name.lower()}"
                logger.info(f"Processing {table_name}: blocks {df['block_number'].min()} to {df['block_number'].max()}")
                record_batches[table_name] = pa.Table.from_pandas(df).to_batches()[0]
            
            # Write in parallel to all targets
            tasks = [
                writer.push_data(record_batches)
                for writer in self._writers.values()
            ]
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Error during write: {str(e)}")
            raise