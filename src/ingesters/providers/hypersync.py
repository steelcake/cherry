import logging, os
from typing import List, Optional, Dict, Tuple, AsyncGenerator, AsyncIterator
import polars as pl
from hypersync import (
    HypersyncClient, 
    ClientConfig, 
    Query, 
    StreamConfig, 
    LogSelection, 
    BlockField, 
    LogField,
    HexOutput, 
    signature_to_topic0,
    FieldSelection
)
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config, Stream, StreamState
from src.processors.hypersync import EventData
from src.types.hypersync import StreamParams
from src.utils.generate_hypersync_query import generate_event_query
from src.writers.base import DataWriter
import asyncio
from src.processors.hypersync import ParallelEventProcessor

logger = logging.getLogger(__name__)

class AsyncEventProcessor:
    """Helper class to handle async iteration of event data"""
    def __init__(self, event_processor):
        self.event_processor = event_processor
        self.events_data = {}
        self.blocks_data = {}
        self.is_completed = False  # New flag to track completion
        logger.info(f"Initialized AsyncEventProcessor for {self.event_processor.event_name}")

    def __aiter__(self):
        """Required for async iteration"""
        return self

    async def __anext__(self):
        try:
            # If this event stream is already completed, stop processing
            if self.is_completed:
                logger.debug(f"Event {self.event_processor.event_name} already completed, skipping")
                raise StopAsyncIteration

            # Check if we've reached the end block before receiving
            if (self.event_processor.to_block and 
                self.event_processor.current_block >= self.event_processor.to_block):
                logger.info(f"Reached end block {self.event_processor.to_block} for {self.event_processor.event_name}")
                # Handle any remaining data
                if self.event_processor.total_events > 0:
                    combined_events, combined_blocks = self.event_processor.get_combined_data()
                    if (combined_events is not None and not combined_events.is_empty()) or \
                       (combined_blocks is not None and not combined_blocks.is_empty()):
                        data = Data(
                            events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                            blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                            transactions=None
                        )
                        logger.info(f"Final batch for {self.event_processor.event_name}: "
                                  f"{combined_events.height if combined_events is not None else 0} events, "
                                  f"{combined_blocks.height if combined_blocks is not None else 0} blocks")
                        self.is_completed = True  # Mark as completed
                        return data
                self.is_completed = True  # Mark as completed even if no data
                raise StopAsyncIteration

            res = await self.event_processor.receiver.recv()
            if res is None:
                # Handle any remaining data before stopping
                if self.event_processor.total_events > 0:
                    combined_events, combined_blocks = self.event_processor.get_combined_data()
                    if (combined_events is not None and not combined_events.is_empty()) or \
                       (combined_blocks is not None and not combined_blocks.is_empty()):
                        data = Data(
                            events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                            blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                            transactions=None
                        )
                        logger.info(f"Final batch for {self.event_processor.event_name}: "
                                  f"{combined_events.height if combined_events is not None else 0} events, "
                                  f"{combined_blocks.height if combined_blocks is not None else 0} blocks")
                        self.is_completed = True  # Mark as completed
                        return data
                logger.info(f"Reached end of data stream for {self.event_processor.event_name}")
                self.is_completed = True  # Mark as completed
                raise StopAsyncIteration

            should_write = self.event_processor.append_data(res)
            
            # Check if we've reached the end block after processing
            if (self.event_processor.to_block and 
                self.event_processor.current_block >= self.event_processor.to_block):
                logger.info(f"Reached end block {self.event_processor.to_block} for {self.event_processor.event_name}")
                combined_events, combined_blocks = self.event_processor.get_combined_data()
                if (combined_events is not None and not combined_events.is_empty()) or \
                   (combined_blocks is not None and not combined_blocks.is_empty()):
                    data = Data(
                        events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                        blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                        transactions=None
                    )
                    self.is_completed = True  # Mark as completed
                    return data
                self.is_completed = True  # Mark as completed
                raise StopAsyncIteration
            
            if should_write:
                combined_events, combined_blocks = self.event_processor.get_combined_data()
                if (combined_events is not None and not combined_events.is_empty()) or \
                   (combined_blocks is not None and not combined_blocks.is_empty()):
                    data = Data(
                        events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                        blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                        transactions=None
                    )
                    # Update current block to next block from response
                    self.event_processor.current_block = res.next_block
                    return data
            
            return None

        except Exception as e:
            if isinstance(e, StopAsyncIteration):
                logger.info(f"Completed processing for {self.event_processor.event_name}")
                self.is_completed = True  # Ensure completion is marked
                raise
            logger.error(f"Error processing event data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

class HypersyncIngester(DataIngester):
    """Ingests data from Hypersync"""
    def __init__(self, config: Config):
        self.config = config
        self.hypersyncapi_token = config.data_source[0].token
        self.client = HypersyncClient(ClientConfig(
            url=self.config.data_source[0].url,
            bearer_token=self.hypersyncapi_token,
        ))
        self._event_processors = {}
        self._completed_events = set()
        self._writers = None
        self._stream_states = {}  # Track block state per stream
        logger.info("Initialized HypersyncIngester")

    @property
    def current_block(self) -> int:
        """Get current block number"""
        # This is only used for compatibility, not for actual block tracking
        return self.config.blocks.from_block

    @current_block.setter
    def current_block(self, value: int):
        """Set current block number"""
        # This is now a no-op since we manage blocks per stream
        pass

    def get_stream_block(self, stream_name: str) -> int:
        """Get current block for stream"""
        stream = next(s for s in self.config.streams if s.name == stream_name)
        
        # Convert dict to StreamState if needed
        if stream.state and isinstance(stream.state, dict):
            stream_state = StreamState(**stream.state)
        else:
            stream_state = None
            
        return (
            stream_state.last_block if stream_state and stream_state.resume 
            else stream.from_block
        )

    def set_stream_block(self, stream_name: str, block: int):
        """Set current block for a stream"""
        self._stream_states[stream_name] = block

    async def initialize_writers(self, writers: Dict[str, DataWriter]):
        """Initialize data writers"""
        self._writers = writers
        logger.info(f"Initialized {len(writers)} data writers: {', '.join(writers.keys())}")

    async def _write_to_targets(self, data: Data) -> None:
        """Write data to configured targets in parallel"""
        if not self._writers:
            logger.error("No writers initialized")
            return

        if not data.events or not any(df.height > 0 for df in data.events.values()):
            logger.info("No data to write")
            return

        try:
            # Create separate copies for each writer
            writer_data = {}
            for writer_type in self._writers.keys():
                writer_data[writer_type] = Data(
                    events={name: df.clone() for name, df in data.events.items()} if data.events else None,
                    blocks={name: df.clone() for name, df in data.blocks.items()} if data.blocks else None,
                    transactions=data.transactions
                )

            # Create tasks for all writers to write in parallel
            write_tasks = {
                writer_type: asyncio.create_task(
                    writer.write_data(writer_data[writer_type]),
                    name=f"write_{writer_type}"
                )
                for writer_type, writer in self._writers.items()
            }
            
            if write_tasks:
                logger.info(f"Writing in parallel to {len(write_tasks)} targets ({', '.join(self._writers.keys())})")
                
                # Wait for all writes to complete concurrently
                results = await asyncio.gather(
                    *write_tasks.values(), 
                    return_exceptions=True
                )
                
                # Check for any errors
                for writer_type, result in zip(write_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in {writer_type} writer: {result}")
                        raise result

        except Exception as e:
            logger.error(f"Error during parallel write: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    async def _get_event_query(self, stream: Stream) -> Tuple[Query, StreamConfig, StreamParams]:
        """Get cached query or create new one"""
        logger.info(f"Generating query for event {stream.name}")
        
        # Use stream-specific block
        current_block = self.get_stream_block(stream.name)
        
        # Get stream-specific batch size
        batch_size = stream.batch_size or self.config.processing.default_batch_size
        
        # Create stream params
        stream_params = StreamParams(
            event_name=stream.name,
            signature=stream.signature,
            from_block=current_block,
            to_block=stream.to_block,  # Use stream-specific to_block
            items_per_section=batch_size,  # Use stream-specific batch size
            column_mapping=stream.column_cast if hasattr(stream, 'column_cast') else None,
            client=self.client
        )
        
        # Generate query
        query, stream_config = generate_event_query(
            stream=stream,
            client=self.client,
            from_block=current_block,
            items_per_section=batch_size
        )
        
        return query, stream_config, stream_params

    async def _create_event_processor(self, stream: Stream, from_block: int) -> AsyncEventProcessor:
        """Create and initialize a new event processor for a stream"""
        # Update stream state
        self.set_stream_block(stream.name, from_block)
        
        logger.info(f"Processing event: {stream.name} from block {from_block}")
        
        # Create new processor for the stream
        query, stream_config, stream_params = await self._get_event_query(stream)
        receiver = await self.client.stream_arrow(query, stream_config)
        
        event_data = EventData(stream_params)
        event_data.receiver = receiver
        event_data.from_block = from_block
        
        return AsyncEventProcessor(event_data)

    async def get_data(self, from_block: int, stream: Stream) -> AsyncIterator[Data]:
        """Get data for a stream starting from block number"""
        try:
            # Skip if event is already completed
            if stream.name in self._completed_events:
                return
            
            # Get or create event processor
            if stream.name not in self._event_processors:
                self._event_processors[stream.name] = await self._create_event_processor(stream, from_block)

            processor = self._event_processors[stream.name]
            async for data in processor:
                if data:
                    yield data
                    # Check if this was the final batch
                    if processor.is_completed:
                        self._completed_events.add(stream.name)
                        logger.info(f"Event {stream.name} completed after final batch")
                        return

            # Mark event as completed if we exit the loop
            if stream.name not in self._completed_events:
                self._completed_events.add(stream.name)
                logger.info(f"Event {stream.name} completed")

        except Exception as e:
            if not isinstance(e, StopAsyncIteration):
                logger.error(f"Error getting data for stream {stream.name}: {e}")
            raise

    async def process_events(self) -> AsyncGenerator[Data, None]:
        """Process events in parallel streams"""
        try:
            # Create stream parameters for each event stream
            stream_params = []
            for stream in self.config.streams:
                if stream.kind == "event":
                    # Initialize stream state if needed
                    if stream.name not in self._stream_states:
                        self._stream_states[stream.name] = (
                            stream.state.last_block if stream.state and stream.state.resume 
                            else stream.from_block
                        )
                    
                    # Create stream parameters
                    params = StreamParams(
                        client=self.client,
                        event_name=stream.name,
                        signature=stream.signature,
                        from_block=self._stream_states[stream.name],
                        to_block=stream.to_block,
                        items_per_section=stream.batch_size,
                        column_mapping=stream.column_cast
                    )
                    stream_params.append(params)
                    logger.info(f"Added stream {stream.name} for parallel processing from block {params.from_block}")

            if not stream_params:
                logger.warning("No event streams to process")
                return

            # Initialize parallel processor with all streams
            processor = ParallelEventProcessor(stream_params)
            logger.info(f"Initialized parallel processing for {len(stream_params)} streams")

            # Process all streams in parallel
            while True:
                data = await processor.process_events()
                if data:
                    # Update stream states from processed data
                    if data.events:
                        for stream_name, events_df in data.events.items():
                            if not events_df.is_empty():
                                max_block = events_df['block_number'].max()
                                self._stream_states[stream_name] = max_block + 1
                                logger.debug(f"Updated {stream_name} next block to {max_block + 1}")
                    yield data
                else:
                    logger.info("All streams completed")
                    break

        except Exception as e:
            logger.error(f"Error in parallel event processing: {e}")
            raise 