import asyncio
import logging
from src.config.parser import parse_config
from src.utils.logging_setup import setup_logging
from src.writers.writer import Writer
from src.ingesters.factory import Ingester
from dotenv import load_dotenv
import sys

# Load environment variables and setup logging
load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

async def process_data(ingester: Ingester, writer: Writer):
    """Process blockchain data from ingester and write to writer"""
    try:
        async for data in ingester.process_all():
            if not data.is_empty():
                logger.info(f"Processing blocks")
                await writer.write(data)
    except Exception as e:
        logger.error(f"Error processing data: {e}", exc_info=True)
        raise

async def main():
    """Main entry point"""
    try:
        logger.info("Starting blockchain data ingestion")
        
        config = parse_config("config.yaml")
        logger.info("Parsed configuration from config.yaml")

        ingester = Ingester(config)
        writer = Writer(Writer.initialize_writers(config))
        
        await process_data(ingester, writer)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)