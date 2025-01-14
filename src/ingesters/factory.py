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
        # Use the known working block range from our tests
        self.current_block = self.config.from_block  # Known working block number
        self.batch_size = 100  # Smaller batch size to ensure we get results
        logger.info(f"Initializing Ingester starting from block {self.current_block}")
        
        # Use HypersyncIngester instead of EthRpcIngester
        if config.data_source[0].kind == DataSourceKind.HYPERSYNC:
            logger.info("Using HypersyncIngester for data ingestion")
            self.ingester = HypersyncIngester(config)
        else:
            logger.warning("Defaulting to HypersyncIngester despite config specifying different source")
            self.ingester = HypersyncIngester(config)

    async def get_next_data_batch(self) -> Data:
        """Get the next batch of data"""
        next_block = self.current_block + self.batch_size
        logger.debug(f"Fetching next batch from {self.current_block} to {next_block}")
        data = await self.ingester.get_data(self.current_block, next_block)
        self.current_block = next_block
        return data