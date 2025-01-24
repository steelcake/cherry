import logging
from src.utils.logging_setup import setup_logging
from src.ingesters.providers.hypersync import HypersyncIngester
from src.config.parser import Config, DataSourceKind
from src.ingesters.base import Data, DataIngester
from typing import AsyncGenerator

logger = logging.getLogger(__name__)

class AsyncIngesterWrapper:
    """Wrapper to handle async iteration of ingester data"""
    def __init__(self, ingester: DataIngester, config: Config):
        self._ingester = ingester
        self._config = config
        self.current_block = config.from_block
        logger.info(f"Initialized ingester starting from block {self.current_block}")

    async def __aiter__(self):
        return self

    async def __anext__(self) -> Data:
        if self._config.to_block and self.current_block > self._config.to_block:
            raise StopAsyncIteration
        
        try:
            async for data in await self._ingester.get_data(self.current_block):
                if data.events or data.blocks:
                    self.current_block = max(
                        max(df['block_number'].max() for df in data.events.values()) if data.events else 0,
                        max(df['block_number'].max() for df in data.blocks.values()) if data.blocks else 0
                    ) + 1
                    return data
            
            raise StopAsyncIteration
            
        except Exception as e:
            logger.error(f"Error getting data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

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
            logger.debug(f"Fetching data from block {self.current_block}")
            async for data in self._ingester.get_data(self.current_block):
                if data is not None:
                    if data.events or data.blocks:
                        self._current_block = max(
                            max(df['block_number'].max() for df in data.events.values()) if data.events else 0,
                            max(df['block_number'].max() for df in data.blocks.values()) if data.blocks else 0
                        ) + 1
                        return data
            
            raise StopAsyncIteration
            
        except Exception as e:
            logger.error(f"Error getting data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    @property
    def current_block(self) -> int:
        """Get current block number"""
        return self._current_block

    @current_block.setter
    def current_block(self, value: int):
        """Set current block number and propagate to underlying ingester"""
        self._current_block = value
        self._ingester.current_block = value  # Ensure underlying ingester is updated

    @property
    def config(self) -> Config:
        """Get configuration"""
        return self._config

    @config.setter
    def config(self, value: Config):
        """Set configuration"""
        self._config = value

    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Get data from the underlying ingester"""
        async for data in self._ingester.get_data(from_block):
            if data is not None:
                yield data

    async def initialize_loaders(self, loaders):
        """Initialize data loaders in the underlying ingester"""
        await self._ingester.initialize_loaders(loaders)

    async def get_data_stream(self) -> Data:
        """Stream data for the next batch"""
        logger.debug(f"Streaming data from {self.current_block} to {self._config.to_block}")
        return await self._ingester.get_data(self.current_block)