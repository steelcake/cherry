import asyncio
import logging
from pathlib import Path
from sqlalchemy import create_engine
from src.config.parser import parse_config, OutputKind
from src.utils.logging_setup import setup_logging
from src.loaders.base import DataLoader
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.parquet_loader import ParquetLoader
from src.ingesters.factory import Ingester
from src.ingesters.base import Data
from typing import Dict

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

async def write_to_targets(data: Data, loaders: Dict[str, DataLoader], current_block: int, next_block: int) -> None:
    """Write data to all enabled targets in parallel"""
    if not any(df.height > 0 for df in data.events.values()):
        logger.info("No data to write to targets")
        return

    write_tasks = []
    for name, loader in loaders.items():
        logger.info(f"Queuing write task for {name}")
        task = asyncio.create_task(
            loader.write_data(
                data,
                from_block=current_block,
                to_block=next_block
            )
        )
        write_tasks.append(task)
    
    if write_tasks:
        logger.info(f"Writing to {len(write_tasks)} targets in parallel")
        await asyncio.gather(*write_tasks)
        logger.info("Completed writing to all targets")

async def process_batch(ingester: Ingester, loaders: Dict[str, DataLoader]) -> bool:
    """Process a single batch of data"""
    current_block = ingester.current_block
    next_block = current_block + ingester.batch_size
    logger.info(f"=== Processing Batch: Blocks {current_block} to {next_block} ===")
    
    try:
        # Get next batch of data
        logger.info("Fetching next batch of data...")
        data = await ingester.get_data_stream()
        
        # Log batch statistics
        logger.info("Batch Statistics:")
        for event_name, event_df in data.events.items():
            logger.info(f"Event: {event_name}")
            logger.info(f"- Event row count: {event_df.height}")
            if event_df.height > 0:
                logger.info(f"- Block Range: {event_df['block_number'].min()} to {event_df['block_number'].max()}")
        
        # Write to all enabled targets in parallel
        await write_to_targets(data, loaders, current_block, next_block)
        
        logger.info("=== Batch Processing Completed ===")
        return True
        
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

async def main():
    logger.info("Starting blockchain data ingestion")
    try:
        # Load config
        logger.info("Loading configuration")
        config = parse_config(Path("config.yaml"))
        
        # Initialize loaders based on configuration
        loaders: Dict[str, DataLoader] = {}
        
        for output_config in config.output:
            if output_config.enabled:
                if output_config.kind == OutputKind.POSTGRES:
                    logger.info("Initializing PostgreSQL loader")
                    engine = create_engine(output_config.url)
                    postgres_loader = PostgresLoader(engine)
                    postgres_loader.create_tables()  # Create tables using loader method
                    loaders['postgres'] = postgres_loader
                elif output_config.kind == OutputKind.PARQUET:
                    logger.info("Initializing Parquet loader")
                    output_dir = Path(output_config.output_dir)
                    loaders['parquet'] = ParquetLoader(output_dir)
            else:
                logger.info(f"Output {output_config.kind} is disabled")
        
        # Initialize ingester
        logger.info("Initializing data ingester")
        ingester = Ingester(config)
        
        # Process data in batches
        logger.info("Starting batch processing")
        while True:
            logger.info(f"Processing batch {ingester.current_block} to {ingester.current_block + ingester.batch_size}")
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
        logger.info("Blockchain data ingestion completed")

if __name__ == "__main__":
    asyncio.run(main())