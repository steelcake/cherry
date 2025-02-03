import logging
from typing import AsyncGenerator, Dict
from src.config.parser import Config, Stream
from src.ingesters.base import DataIngester
from src.ingesters.providers.hypersync import HypersyncIngester
from src.types.data import Data
from src.writers.base import DataWriter
import asyncio
from src.processors.hypersync import ParallelEventProcessor

logger = logging.getLogger(__name__)

class Ingester(DataIngester):
    """Manages ingestion of multiple event streams"""
    def __init__(self, config: Config):
        self._config = config
        self._event_streams = [s for s in config.streams if s.kind == "event"]
        self._ingester = HypersyncIngester(config)
        self._completed_streams = set()
        self._active_tasks = {}  # Track active tasks per stream
        
        # Initialize stream states with stream-specific from_block
        self._stream_states = {
            s.name: s.from_block for s in self._event_streams
        }
        
        logger.info(f"Initialized ingester with {len(self._event_streams)} event streams")

    @property
    def current_block(self) -> int:
        """Get current block number"""
        if not self._stream_states:
            return self.config.blocks.from_block
        return min(self._stream_states.values())

    @current_block.setter
    def current_block(self, value: int):
        """Set current block number"""
        for stream in self._event_streams:
            self._stream_states[stream.name] = value
            self._ingester.current_block = value

    async def initialize_writers(self, writers: Dict[str, DataWriter]):
        """Initialize data writers"""
        await self._ingester.initialize_writers(writers)

    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Get data from the underlying ingester"""
        try:
            async for data in self._ingester.get_data(from_block):
                if data is not None:
                    yield data
        except StopAsyncIteration:
            logger.info("Reached end of data stream")
            raise

    def __aiter__(self):
        """Required for async iteration - should not be async"""
        return self

    async def __anext__(self) -> Data:
        """Get next batch of data"""
        try:
            return await self.get_next_batch()
        except StopAsyncIteration:
            raise

    async def get_next_batch(self) -> Data:
        """Get next batch of data from any available stream"""
        try:
            # Create or get tasks for all active streams
            for stream in self._event_streams:
                if stream.name not in self._completed_streams and stream.name not in self._active_tasks:
                    logger.info(f"Creating task for stream {stream.name}")
                    task = asyncio.create_task(
                        self._process_stream(stream),
                        name=f"process_{stream.name}"
                    )
                    self._active_tasks[stream.name] = task

            if not self._active_tasks:
                logger.info("No active streams remaining")
                raise StopAsyncIteration

            # Wait for any stream to complete its current batch
            done, pending = await asyncio.wait(
                list(self._active_tasks.values()),
                return_when=asyncio.FIRST_COMPLETED
            )

            # Process completed task and return its data immediately
            for task in done:
                stream_name = task.get_name().replace('process_', '')
                try:
                    result = await task
                    # Remove completed task
                    self._active_tasks.pop(stream_name, None)
                    
                    if result:
                        # Create new task for this stream
                        if stream_name not in self._completed_streams:
                            new_task = asyncio.create_task(
                                self._process_stream(next(s for s in self._event_streams if s.name == stream_name)),
                                name=f"process_{stream_name}"
                            )
                            self._active_tasks[stream_name] = new_task
                        return result
                except StopAsyncIteration:
                    # Stream completed
                    self._active_tasks.pop(stream_name, None)
                    if stream_name not in self._completed_streams:
                        self._completed_streams.add(stream_name)
                        logger.info(f"Stream {stream_name} completed")
                except Exception as e:
                    logger.error(f"Error in stream {stream_name}: {e}")
                    # Cancel all tasks on error
                    for t in self._active_tasks.values():
                        t.cancel()
                    self._active_tasks.clear()
                    raise

            # Check if all streams are done
            if len(self._completed_streams) == len(self._event_streams):
                raise StopAsyncIteration

            # Continue with remaining tasks
            return None

        except Exception as e:
            if not isinstance(e, StopAsyncIteration):
                logger.error(f"Error getting next batch: {e}")
            raise

    async def _process_stream(self, stream: Stream) -> Data:
        """Process a single event stream"""
        try:
            current_block = self._stream_states[stream.name]
            logger.info(f"Starting to process stream {stream.name} from block {current_block}")

            async for batch in self._ingester.get_data(current_block, stream):
                if batch and (batch.events or batch.blocks):
                    # Update stream's block state
                    if batch.events and stream.name in batch.events:
                        max_block = batch.events[stream.name]['block_number'].max()
                        self._stream_states[stream.name] = max_block + 1
                        logger.info(f"Stream {stream.name} processed batch with {len(batch.events[stream.name])} events")
                    
                    # Check if we've reached the stream's to_block
                    if stream.to_block and current_block >= stream.to_block:
                        self._completed_streams.add(stream.name)
                        logger.info(f"Stream {stream.name} reached target block {stream.to_block}")
                        raise StopAsyncIteration
                        
                    return batch

            # Stream completed normally
            logger.info(f"Stream {stream.name} completed normally")
            self._completed_streams.add(stream.name)
            raise StopAsyncIteration

        except StopAsyncIteration:
            raise
        except Exception as e:
            logger.error(f"Error processing stream {stream.name}: {e}")
            raise

    @property
    def config(self) -> Config:
        return self._ingester.config