import logging
from src.utils.logging_setup import setup_logging
from src.ingesters.providers.hypersync import HypersyncIngester
from src.config.parser import Config, DataSourceKind
from src.ingesters.base import Data

logger = logging.getLogger(__name__)

class Ingester:
    """Factory class for creating appropriate ingester based on config"""
    
    def __init__(self, config: Config):
        self.config = config
        self.current_block = config.from_block
        self.to_block = config.to_block
        self.ingester = self._create_ingester()
        logger.info(f"Initialized ingester starting from block {self.current_block}")

    def _create_ingester(self) -> HypersyncIngester:
        if self.config.data_source[0].kind != DataSourceKind.HYPERSYNC:
            logger.warning("Defaulting to HypersyncIngester")
        return HypersyncIngester(self.config)

    async def get_data_stream(self) -> Data:
        """Stream data for the next batch"""
        logger.debug(f"Streaming data from {self.current_block} to {self.to_block}")
        return await self.ingester.get_data(self.current_block)