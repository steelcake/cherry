import asyncio
import logging
from pathlib import Path
from sqlalchemy import Engine, create_engine
from src.config.parser import parse_config
from src.utils.logging_setup import setup_logging
from src.db.postgres import create_tables, create_engine_from_config, ingest_data
from src.ingesters.base import Data
from src.ingesters.factory import Ingester

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

async def process_batch(ingester: Ingester, engine: Engine) -> bool:
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
            logger.info(f"- Rows: {event_df.height}")
            if event_df.height > 0:
                logger.info(f"- Columns: {event_df.columns}")
                logger.info(f"- Block Range: {event_df['block_number'].min()} to {event_df['block_number'].max()}")
            
        # Ingest to PostgreSQL
        if any(df.height > 0 for df in data.events.values()):
            logger.info("Starting PostgreSQL ingestion...")
            ingest_data(engine, data)
            logger.info("PostgreSQL ingestion completed")
        else:
            logger.info("No data to ingest in this batch")
        
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
        
        # Create database engine
        logger.info("Creating database engine")
        engine = create_engine_from_config(config.output[0].url)
        
        # Create tables
        create_tables(engine)
        
        # Initialize ingester
        logger.info("Initializing data ingester")
        ingester = Ingester(config)
        
        # Process data in batches
        logger.info("Starting batch processing")
        while True:
            logger.info(f"Processing batch {ingester.current_block} to {ingester.current_block + ingester.batch_size}")
            try:
                has_more_data = await process_batch(ingester, engine)
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