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
    postgres_config = config.output.get('postgres')
    if postgres_config and postgres_config.enabled:
        postgres_loader = PostgresLoader(create_engine(postgres_config.url))
        postgres_loader.create_tables()
        loaders['postgres'] = postgres_loader
        logger.info("Initialized PostgreSQL loader")

    # Initialize Local Parquet loader
    parquet_config = config.output.get('parquet')
    if parquet_config and parquet_config.enabled:
        loaders['local_parquet'] = ParquetLoader(Path(parquet_config.output_dir))
        logger.info("Initialized Local Parquet loader")

    # Initialize S3 loader
    s3_config = config.output.get('s3')
    if s3_config and s3_config.enabled:
        loaders['s3'] = S3Loader(
            endpoint=s3_config.endpoint,
            access_key=s3_config.access_key,
            secret_key=s3_config.secret_key,
            bucket=s3_config.bucket,
            secure=s3_config.secure
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