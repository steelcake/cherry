import logging
from src.utils.logging_setup import setup_logging
from src.ingesters.providers.hypersync import HypersyncIngester
from src.config.parser import Config, DataSourceKind
from src.ingesters.base import Data

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

class Ingester:
    """Factory class for creating appropriate ingester based on config"""
    
    def __init__(self, config: Config):
        self.config = config
        self.current_block = self.config.from_block
        self.batch_size = 2  # Smaller batch size to ensure we get results
        logger.info(f"Initializing Ingester starting from block {self.current_block}")
        
        # Use HypersyncIngester
        if config.data_source[0].kind == DataSourceKind.HYPERSYNC:
            logger.info("Using HypersyncIngester for data ingestion")
            self.ingester = HypersyncIngester(config)
        else:
            logger.warning("Defaulting to HypersyncIngester despite config specifying different source")
            self.ingester = HypersyncIngester(config)

    async def get_data_stream(self) -> Data:
        """Stream data for the next batch"""
        next_block = self.current_block + self.batch_size
        logger.debug(f"Streaming data from {self.current_block} to {next_block}")
        data = await self.ingester.get_data(self.current_block, next_block)
        self.current_block = next_block
        return data