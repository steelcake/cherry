import asyncio
import logging
from pathlib import Path
from typing import Dict
from sqlalchemy import create_engine
from src.config.parser import parse_config, OutputKind
from src.utils.logging_setup import setup_logging
from src.loaders.base import DataLoader
from src.loaders.postgres import PostgresLoader
from src.loaders.parquet import ParquetLoader
from src.ingesters.factory import Ingester
from src.ingesters.base import Data
from typing import Dict

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def initialize_loaders(config) -> Dict[str, DataLoader]:
    loaders = {}
    for output in config.output:
        if not output.enabled:
            continue
            
        if output.kind == OutputKind.POSTGRES:
            postgres_loader = PostgresLoader(create_engine(output.url))
            postgres_loader.create_tables()  # Create tables during initialization
            loaders['postgres'] = postgres_loader
        elif output.kind == OutputKind.PARQUET:
            loaders['parquet'] = ParquetLoader(Path(output.output_dir))
            
    return loaders

async def write_to_targets(data: Data, loaders: Dict[str, DataLoader]) -> None:
    if not any(df.height > 0 for df in data.events.values()):
        logger.info("No data to write")
        return

    write_tasks = [
        asyncio.create_task(loader.write_data(data))
        for loader in loaders.values()
    ]
    
    if write_tasks:
        logger.info(f"Writing to {len(write_tasks)} ({', '.join(loaders.keys())}) targets")
        await asyncio.gather(*write_tasks)
        logger.info("Write complete")

async def process_batch(ingester: Ingester, loaders: Dict[str, DataLoader]) -> bool:
    """Process a single batch of data"""
    current_block = ingester.current_block
    logger.info(f"=== Processing Batch: Blocks {current_block} to ? ===")
    
    try:
        data = await ingester.get_data_stream()
        
        # Log batch statistics
        logger.info("Batch Statistics:")
        for event_name, event_df in data.events.items():
            logger.info(f"Event: {event_name}")
            logger.info(f"- Event row count: {event_df.height}")
            if event_df.height > 0:
                logger.info(f"- Block Range: {event_df['block_number'].min()} to {event_df['block_number'].max()}")
        
        # Write to all enabled targets in parallel
        await write_to_targets(data, loaders)
        
        logger.info("=== Batch Processing Completed ===")
        return True
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

async def main():
    logger.info("Starting blockchain data ingestion")
    try:
        config = parse_config(Path("config.yaml"))
        loaders = initialize_loaders(config)
        ingester = Ingester(config)
        
        while True:
            logger.info(f"Processing batch {ingester.current_block} to ?")
            try:
                has_more_data = await process_batch(ingester, loaders)
                if not has_more_data:
                    logger.info("Completed processing all blocks")
                    break
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                break
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise
    finally:
        logger.info("Ingestion completed")

if __name__ == "__main__":
    asyncio.run(main())