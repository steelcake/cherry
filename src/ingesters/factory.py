import logging
from typing import AsyncGenerator
from src.config.parser import Config
from src.ingesters.base import Data, DataIngester
from src.ingesters.providers.hypersync import HypersyncIngester

logger = logging.getLogger(__name__)

class Ingester(DataIngester):
    """Factory class for creating appropriate ingester based on config"""
    
    def __init__(self, config: Config):
        self._config = config
        self._ingester = HypersyncIngester(config)
        self._current_block = config.blocks.range.from_block
        logger.info(f"Initialized ingester starting from block {self.current_block}")

    def __aiter__(self):
        return self

    async def __anext__(self) -> Data:
        if self._config.blocks.range.to_block and self.current_block > self._config.blocks.range.to_block:
            logger.info(f"Reached target block {self._config.blocks.range.to_block}")
            raise StopAsyncIteration
        
        try:
            async for data in self._ingester.get_data(self.current_block):
                if data is not None:
                    if data.events or data.blocks:
                        self._current_block = max(
                            max(df['block_number'].max() for df in data.events.values()) if data.events else 0,
                            max(df['block_number'].max() for df in data.blocks.values()) if data.blocks else 0
                        ) + 1
                        return data
        except Exception as e:
            logger.error(f"Error getting data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Get data from the underlying ingester"""
        async for data in self._ingester.get_data(from_block):
            if data is not None:
                yield data

    async def initialize_loaders(self, loaders):
        """Initialize data loaders in the underlying ingester"""
        await self._ingester.initialize_loaders(loaders)

    @property
    def current_block(self) -> int:
        return self._current_block

    @current_block.setter
    def current_block(self, value: int):
        self._current_block = value
        self._ingester.current_block = value

    @property
    def config(self) -> Config:
        return self._config