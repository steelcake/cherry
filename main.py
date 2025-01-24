import asyncio
import logging
from pathlib import Path
from sqlalchemy import create_engine
from src.config.parser import parse_config
from src.utils.logging_setup import setup_logging
from src.ingesters.factory import Ingester
from src.loaders.loader import Loader
from src.loaders.postgres import PostgresLoader
from src.loaders.parquet import ParquetLoader
from src.loaders.s3 import S3Loader

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def initialize_loaders(config) -> dict:
    """Initialize data loaders based on config"""
    loaders = {}
    
    # Initialize PostgreSQL loader
    if config.output.postgres and config.output.postgres.get('enabled'):
        postgres_loader = PostgresLoader(create_engine(config.output.postgres['url']))
        postgres_loader.create_tables()
        loaders['postgres'] = postgres_loader
        logger.info("Initialized PostgreSQL loader")

    # Initialize Local Parquet loader
    if config.output.parquet and config.output.parquet.get('enabled'):
        loaders['local_parquet'] = ParquetLoader(Path(config.output.parquet['output_dir']))
        logger.info("Initialized Local Parquet loader")

    # Initialize S3 loader
    if config.output.s3 and config.output.s3.get('enabled'):
        loaders['s3'] = S3Loader(
            endpoint=config.output.s3['endpoint'],
            access_key=config.output.s3['access_key'],
            secret_key=config.output.s3['secret_key'],
            bucket=config.output.s3['bucket'],
            secure=config.output.s3.get('secure', False)
        )
        logger.info("Initialized S3 loader")

    if not loaders:
        logger.warning("No loaders were initialized!")
    else:
        logger.info(f"Initialized {len(loaders)} loaders: {', '.join(loaders.keys())}")

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