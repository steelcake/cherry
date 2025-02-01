import asyncio
import logging
from pathlib import Path
from sqlalchemy import create_engine
from src.config.parser import parse_config, Config
from src.utils.logging_setup import setup_logging
from src.ingesters.factory import Ingester
from src.loaders.loader import Loader
from src.loaders.postgres import PostgresLoader
from src.loaders.parquet import ParquetLoader
from src.loaders.s3 import S3Loader
from src.loaders.base import DataLoader
from typing import Dict
import sys

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def initialize_loaders(config: Config) -> Dict[str, DataLoader]:
    """Initialize data loaders based on config"""
    loaders = {}
    
    for output in config.output:
        if output.kind.lower() == 's3':
            logger.info("Initializing S3 loader")
            loaders['s3'] = S3Loader(
                endpoint=output.endpoint,
                bucket=output.bucket,
                access_key=output.access_key,
                secret_key=output.secret_key,
                region=output.region,
                secure=output.secure
            )
            logger.info(f"Initialized S3Loader with endpoint {output.endpoint}, bucket {output.bucket}")

        elif output.kind.lower() == 'local_parquet':
            logger.info("Initializing Local Parquet loader")
            loaders['local_parquet'] = ParquetLoader(
                output_dir=output.path
            )
            logger.info(f"Initialized ParquetLoader with output directory {output.path}")

    if not loaders:
        raise ValueError("No loaders configured")

    logger.info(f"Initialized {len(loaders)} loaders: {', '.join(loaders.keys())}")
    return loaders

async def process_data(ingester: Ingester, loader: Loader):
    """Process blockchain data from ingester and write to loader"""
    try:
        async for data in ingester:
            if data is None:
                continue  # Skip if no data returned
                
            # Get latest block number from data
            latest_block = 0
            if data.events:
                for events_df in data.events.values():
                    latest_block = max(latest_block, events_df['block_number'].max())
            elif data.blocks:
                for blocks_df in data.blocks.values():
                    latest_block = max(latest_block, blocks_df['number'].max())

            logger.info(f"Processing data from block {latest_block}")

            # Write data to all targets
            await loader.load(data)

    except StopAsyncIteration:
        logger.info("All data streams completed")
        return
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        raise

async def main():
    """Main entry point"""
    try:
        logger.info("Starting blockchain data ingestion")

        # Load configuration
        config = parse_config("config.yaml")
        logger.info("Parsed configuration from config.yaml")

        # Initialize components
        ingester = Ingester(config)
        loader = Loader(initialize_loaders(config))

        # Process data
        await process_data(ingester, loader)

        logger.info("Blockchain data ingestion completed successfully")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        raise
    finally:
        # Cleanup if needed
        pass

if __name__ == "__main__":
    asyncio.run(main())