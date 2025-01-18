from pathlib import Path
import logging
import polars as pl
from src.ingesters.base import Data
from datetime import datetime

logger = logging.getLogger(__name__)

def write_parquet_data(data: Data, output_dir: Path, from_block: int, to_block: int):
    """Write data to Parquet files"""
    try:
        logger.info("Writing blockchain data to Parquet files")
        output_dir.mkdir(exist_ok=True)
        
        # Write events data
        if data.events:
            for event_name, event_df in data.events.items():
                if event_df.height > 0:
                    try:
                        parquet_path = output_dir / f"{event_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{from_block}_{to_block}.parquet"
                        logger.debug(f"Writing {event_df.height} {event_name} events to {parquet_path}")
                        event_df.write_parquet(parquet_path)
                        logger.info(f"Successfully wrote {event_df.height} {event_name} events to Parquet")
                    except Exception as e:
                        logger.error(f"Error writing {event_name} events to Parquet: {e}")
                        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                        raise
        
        # Write blocks data if present
        if data.blocks:
            try:
                blocks_df = pl.concat([df for df in data.blocks.values() if df.height > 0])
                if blocks_df.height > 0:
                    blocks_df = blocks_df.unique(subset=["block_number"])
                    parquet_path = output_dir / f"blocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{from_block}_{to_block}.parquet"
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