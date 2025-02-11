import asyncio
import logging
from src.config.parser import parse_config, Config
from src.utils.logging_setup import setup_logging
from src.ingesters.factory import Ingester
from src.writers.writer import Writer
from src.writers.parquet import ParquetWriter
from src.writers.s3 import S3Writer
from src.writers.base import DataWriter
from typing import Dict
import sys

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def initialize_writers(config: Config) -> Dict[str, DataWriter]:
    """Initialize data writers based on config"""
    writers = {}
    
    for output in config.output:
        if output.kind.lower() == 's3':
            logger.info("Initializing S3 writer")
            writers['s3'] = S3Writer(
                endpoint=output.endpoint,
                bucket=output.bucket,
                access_key=output.access_key,
                secret_key=output.secret_key,
                region=output.region,
                secure=output.secure
            )
            logger.info(f"Initialized S3Writer with endpoint {output.endpoint}, bucket {output.bucket}")

        elif output.kind.lower() == 'local_parquet':
            logger.info("Initializing Local Parquet writer")
            writers['local_parquet'] = ParquetWriter(
                output_dir=output.path
            )
            logger.info(f"Initialized ParquetWriter with output directory {output.path}")

    if not writers:
        raise ValueError("No writers configured")

    logger.info(f"Initialized {len(writers)} writers: {', '.join(writers.keys())}")
    return writers

async def process_data(ingester: Ingester, writer: Writer):
    """Process blockchain data from ingester and write to writer"""
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
            await writer.write(data)

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
        writer = Writer(initialize_writers(config))

        # Process data
        await process_data(ingester, writer)

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