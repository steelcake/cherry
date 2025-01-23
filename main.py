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
from src.loaders.loader import Loader

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def initialize_loaders(config) -> Dict[str, DataLoader]:
    """Initialize data loaders based on config"""
    loaders = {}
    for output in config.output:
        if not output.enabled:
            continue
            
        if output.kind == OutputKind.POSTGRES:
            postgres_loader = PostgresLoader(create_engine(output.url))
            postgres_loader.create_tables()
            loaders['postgres'] = postgres_loader
        elif output.kind == OutputKind.PARQUET:
            loaders['parquet'] = ParquetLoader(Path(output.output_dir))
            
    return loaders

async def process_data(ingester: Ingester, loader: Loader) -> None:
    """Process blockchain data"""
    try:
        async for data in ingester:
            logger.info(f"Processing data from block {ingester.current_block}")
            await loader.load(data)
            
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

async def main():
    """Main entry point"""
    try:
        logger.info("Starting blockchain data ingestion")
        
        # Parse configuration
        config = parse_config("config.yaml")
        logger.info("Parsed configuration from config.yaml")
        
        # Initialize components
        ingester = Ingester(config)
        loaders = initialize_loaders(config)
        loader = Loader(loaders)
        
        # Process data
        await process_data(ingester, loader)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise
    finally:
        logger.info("Blockchain data ingestion completed")

if __name__ == "__main__":
    asyncio.run(main())