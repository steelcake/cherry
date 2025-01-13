import asyncio
import logging
from pathlib import Path
from sqlalchemy import create_engine
from src.config.parser import parse_config
from src.utils.logging_setup import setup_logging
from src.db.postgres import create_tables, create_engine_from_config, ingest_data
from src.ingesters.base import Data
from src.ingesters.ingester import Ingester

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

async def process_batch(ingester: Ingester, engine) -> bool:
    """Process a single batch of data"""
    current_block = ingester.current_block
    next_block = current_block + ingester.batch_size
    logger.info(f"=== Processing Batch: Blocks {current_block} to {next_block} ===")
    
    try:
        # Get next batch of data
        logger.info("Fetching next batch of data...")
        data = await ingester.get_next_data_batch()
        logger.info(f"Received data batch with {len(data.blocks)} blocks")
        
        # If no more data, return False
        if len(data.blocks) == 0:
            logger.info("No more blocks to process in this batch")
            return False
        
        # Log batch statistics
        logger.info("Batch Statistics:")
        logger.info(f"- Blocks: {len(data.blocks)}")
        logger.info(f"- Transactions: {len(data.transactions) if data.transactions is not None else 0}")
        logger.info(f"- Events: {len(data.events)}")
        # Log only the first 5 values of each column to reduce noise
        logger.info(", ".join(f"- {col_name}: {data.events[col_name].to_pylist()[:5]}" for col_name in data.events.column_names))
            
        # Ingest to PostgreSQL
        logger.info("Starting PostgreSQL ingestion...")
        ingest_data(engine, data)
        
        logger.info(f"Successfully processed batch of {len(data.blocks)} blocks")
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