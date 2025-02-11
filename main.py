import asyncio
import logging
from pathlib import Path
from src.config.parser import parse_config, Config
from src.utils.logging_setup import setup_logging
from typing import Dict
import sys
from dotenv import load_dotenv

load_dotenv()

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

async def main():
    """Main entry point"""
    try:
        logger.info("Starting blockchain data ingestion")

        # Load configuration
        config = parse_config("config.yaml")
        logger.info(f"Parsed configuration from config.yaml, config: {config}")
        logger.info(f"Query: {config.pipelines['my_pipeline'].provider.config.query}")

        sys.exit()

        # Initialize components
        ingester = Ingester(config)
        writer = Writer(Writer.initialize_writers(config))

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