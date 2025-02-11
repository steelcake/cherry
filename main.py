import asyncio
import logging
from pathlib import Path
from src.config.parser import parse_config
from src.utils.logging_setup import setup_logging
from src.writers.writer import Writer
from src.processors.hypersync import EventProcessor
from src.ingesters.streams import StreamManager
from dotenv import load_dotenv

# Load environment variables
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
        logger.info("Parsed configuration from config.yaml")
        
        # Initialize components
        processor = EventProcessor(config)
        stream_manager = StreamManager(config.streams, processor)
        writer = Writer(Writer.initialize_writers(config))
        
        # Process streams
        async for data in stream_manager.process_all():
            try:
                await writer.write(data)
            except Exception as e:
                logger.error(f"Error writing data: {e}")
                raise
                
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        await processor.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        import sys
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        sys.exit(1)