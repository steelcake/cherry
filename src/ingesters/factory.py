import logging
from typing import AsyncGenerator, Dict
from src.config.parser import Config
from src.ingesters.base import DataIngester
from src.ingesters.providers.hypersync import HypersyncIngester
from src.types.data import Data
from src.loaders.base import DataLoader

logger = logging.getLogger(__name__)

class Ingester(DataIngester):
    """Factory class for creating and managing data ingesters"""
    def __init__(self, config: Config):
        self._ingester = HypersyncIngester(config)
        self._current_block = config.blocks.range.from_block
        logger.info(f"Initialized ingester starting from block {self._current_block}")

    @property
    def current_block(self) -> int:
        """Get current block number"""
        return self._current_block

    @current_block.setter
    def current_block(self, value: int):
        """Set current block number"""
        self._current_block = value
        self._ingester.current_block = value

    async def initialize_loaders(self, loaders: Dict[str, DataLoader]):
        """Initialize data loaders"""
        await self._ingester.initialize_loaders(loaders)

    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Get data from the underlying ingester"""
        async for data in self._ingester.get_data(from_block):
            if data is not None:
                yield data

    def __aiter__(self):
        return self

    async def __anext__(self) -> Data:
        """Get next batch of data"""
        try:
            async for data in self._ingester.get_data(self.current_block):
                if data:
                    self.current_block = self._ingester.current_block
                    return data
            raise StopAsyncIteration
        except Exception as e:
            logger.error(f"Error getting data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    @property
    def config(self) -> Config:
        return self._ingester.config