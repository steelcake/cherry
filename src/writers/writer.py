import asyncio
import logging
from typing import Dict
from src.types.data import Data
from src.writers.base import DataWriter
from src.writers.local_parquet import ParquetWriter
from src.writers.postgres import PostgresWriter
from src.writers.s3 import S3Writer
from src.writers.clickhouse import ClickHouseWriter
from src.writers.aws_wrangler_s3 import AWSWranglerWriter
from src.config.parser import Config
import pyarrow as pa

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