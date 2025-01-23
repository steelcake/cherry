from pathlib import Path
import logging
import polars as pl
from datetime import datetime
from src.ingesters.base import Data
from src.loaders.base import DataLoader
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter
import asyncio

logger = logging.getLogger(__name__)

class ParquetLoader(DataLoader):
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
    
    async def load(self, data: Data) -> None:
        """Load data to parquet files"""
        try:
            if not data or not data.events:
                return

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Get block range for filenames
            min_block = float('inf')
            max_block = 0
            if data.events:
                for event_df in data.events.values():
                    if event_df.height > 0:
                        min_block = min(min_block, event_df['block_number'].min())
                        max_block = max(max_block, event_df['block_number'].max())
            
            write_tasks = []
            
            # Write events data
            if data.events:
                total_events = 0
                for event_name, event_df in data.events.items():
                    if event_df.height > 0:
                        try:
                            parquet_path = self.output_dir / f"events_{event_name}_{timestamp}_block_{min_block}_to_{max_block}.parquet"
                            write_tasks.append(
                                asyncio.create_task(
                                    asyncio.to_thread(
                                        event_df.write_parquet,
                                        parquet_path
                                    ),
                                    name=f"write_events_{event_name}"
                                )
                            )
                            total_events += event_df.height
                            logger.info(f"Queued {event_df.height} events for {event_name} to {parquet_path}")
                        except Exception as e:
                            logger.error(f"Error writing {event_name} events to Parquet: {e}")
                            raise
                logger.info(f"Queued {total_events} total events for writing to Parquet files")
            
            # Write blocks data
            if data.blocks:
                total_blocks = 0
                for event_name, block_df in data.blocks.items():
                    if block_df.height > 0:
                        try:
                            unique_blocks = block_df.unique(subset=["block_number"]).sort("block_number")
                            parquet_path = self.output_dir / f"blocks_{event_name}_{timestamp}_block_{min_block}_to_{max_block}.parquet"
                            write_tasks.append(
                                asyncio.create_task(
                                    asyncio.to_thread(
                                        unique_blocks.write_parquet,
                                        parquet_path
                                    ),
                                    name=f"write_blocks_{event_name}"
                                )
                            )
                            total_blocks += unique_blocks.height
                            logger.info(f"Queued {unique_blocks.height} unique blocks for {event_name} to {parquet_path}")
                        except Exception as e:
                            logger.error(f"Error writing blocks to Parquet: {e}")
                            raise
                logger.info(f"Queued {total_blocks} total unique blocks for writing to Parquet files")

            # Wait for all writes to complete
            if write_tasks:
                await asyncio.gather(*write_tasks)
                logger.info("All Parquet writes completed successfully")
                    
        except Exception as e:
            logger.error(f"Error writing data to Parquet files: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 