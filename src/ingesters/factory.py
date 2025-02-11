import logging
from typing import AsyncGenerator, Dict
from src.config.parser import Config
from src.processors.hypersync import EventProcessor
from src.ingesters.streams import StreamManager
from src.types.data import Data

logger = logging.getLogger(__name__)

class Ingester:
    """Manages data ingestion from configured streams"""
    
    def __init__(self, config: Config):
        self.processor = EventProcessor(config)
        self.stream_manager = StreamManager(config.streams, self.processor)
        logger.info(f"Initialized ingester with {len(config.streams)} event streams")

    async def __aiter__(self) -> AsyncGenerator[Data, None]:
        """Process all configured streams"""
        try:
            async for data in self.stream_manager.process_all():
                if not data.is_empty():
                    block_range = data.get_block_range()
                    logger.info(f"Processing blocks {block_range[0]} to {block_range[1]}")
                    yield data
                    
        except Exception as e:
            logger.error(f"Error in data ingestion: {e}")
            raise
        finally:
            await self.processor.close()

    @property
    def current_block(self) -> int:
        """Get current block number being processed"""
        return self.processor.current_block if self.processor else 0