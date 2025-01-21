from pathlib import Path
import logging
import polars as pl
from datetime import datetime
from src.ingesters.base import Data
from src.loaders.base import DataLoader
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter

logger = logging.getLogger(__name__)

class ParquetLoader(DataLoader):
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
    
    async def write_data(self, data: Data, **kwargs) -> None:
        """Write data to Parquet files with schema validation"""
        try:
            from_block = kwargs.get('from_block')
            to_block = kwargs.get('to_block')
            
            logger.info("Writing blockchain data to Parquet files")
            self.output_dir.mkdir(exist_ok=True)
            
            # Write events data
            if data.events:
                for event_name, event_df in data.events.items():
                    if event_df.height > 0:
                        try:
                            # Apply schema
                            event_df = event_df.cast(self.events_schema)
                            parquet_path = self.output_dir / f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{from_block}_{to_block}.parquet"
                            event_df.write_parquet(parquet_path)
                            logger.info(f"Successfully wrote {event_df.height} {event_name} events to Parquet")
                        except Exception as e:
                            logger.error(f"Error writing {event_name} events to Parquet: {e}")
                            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                            raise
            
            # Write blocks data
            if data.blocks:
                try:
                    blocks_df = pl.concat([df for df in data.blocks.values() if df.height > 0])
                    if blocks_df.height > 0:
                        # Apply schema
                        blocks_df = blocks_df.cast(self.blocks_schema)
                        blocks_df = blocks_df.unique(subset=["block_number"])
                        parquet_path = self.output_dir / f"blocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{from_block}_{to_block}.parquet"
                        logger.debug(f"Writing {blocks_df.height} unique blocks to {parquet_path}")
                        blocks_df.write_parquet(parquet_path)
                        logger.info(f"Successfully wrote {blocks_df.height} blocks to Parquet")
                except Exception as e:
                    logger.error(f"Error writing blocks to Parquet: {e}")
                    logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                    raise
                    
            logger.info("Successfully completed writing all data to Parquet files")
            
        except Exception as e:
            logger.error(f"Error writing data to Parquet files: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 