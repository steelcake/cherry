import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, List
import polars as pl
from hypersync import ArrowResponse, DataType
from src.types.hypersync import StreamParams
from src.schemas.blockchain_schemas import BLOCKS, EVENTS, EVENT_SCHEMAS
from src.schemas.base import SchemaConverter
import time
import asyncio
from src.types.data import Data

logger = logging.getLogger(__name__)

class EventData:
    """Handles event data processing and storage"""
    def __init__(self, params: StreamParams):
        self.event_name = params.event_name
        self.signature = params.signature
        self.from_block = params.from_block
        self.to_block = params.to_block
        self.current_block = params.from_block
        self.logs_df_list = []
        self.blocks_df_list = []
        self.items_per_section = params.items_per_section
        self.column_mapping = params.column_mapping if params.column_mapping else {}
        self.last_process_time = time.time()
        self.last_event_count = 0
        self.total_events = 0
        logger.info(f"Initialized EventData processor for {self.event_name} from block {self.from_block} to {self.to_block or 'latest'}")

    def append_data(self, res: ArrowResponse) -> bool:
        """Process and append new data"""
        try:
            if res is None:
                return False

            # Update current block from response
            self.current_block = res.next_block
            
            # Convert Arrow data to Polars with column mapping
            logs_df = pl.from_arrow(res.data.logs)
            if logs_df.height == 0:
                return False

            decoded_logs_df = pl.from_arrow(res.data.decoded_logs).rename(lambda n: f"decoded_{n}")
            blocks_df = pl.from_arrow(res.data.blocks).rename(lambda n: f"block_{n}")

            # Join dataframes
            combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")

            # Apply column mappings if they exist
            if isinstance(self.column_mapping, dict) and self.column_mapping:
                for col, dtype_str in self.column_mapping.items():
                    decoded_col = f"decoded_{col}"
                    if decoded_col in combined_df.columns:
                        try:
                            polars_type = self._convert_hypersync_type(dtype_str)
                            combined_df = combined_df.with_columns(pl.col(decoded_col).cast(polars_type))
                        except Exception as e:
                            logger.warning(f"Failed to cast column {decoded_col} to {dtype_str}: {e}")

            if combined_df.height > 0:
                self.total_events += combined_df.height
                self.logs_df_list.append(combined_df)
                
                # Calculate and log processing speed every 2 seconds
                current_time = time.time()
                time_diff = current_time - self.last_process_time
                if time_diff >= 2:
                    events_diff = self.total_events - self.last_event_count
                    speed = events_diff / time_diff
                    blocks_processed = self.current_block - self.from_block
                    blocks_per_second = blocks_processed / time_diff
                    logger.info(f"Event: {self.event_name} - Processed {combined_df.height} events "
                              f"(Total: {self.total_events}/{self.items_per_section}), "
                              f"Speed: {speed:.0f} events/s, {blocks_per_second:.0f} blocks/s")
                    self.last_process_time = current_time
                    self.last_event_count = self.total_events

            # Check if we should write based on block progress
            should_write = (
                self.total_events >= self.items_per_section or 
                (self.to_block and self.current_block >= self.to_block)
            )

            return should_write

        except Exception as e:
            logger.error(f"Error processing data chunk: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    def _convert_hypersync_type(self, dtype: str) -> pl.DataType:
        """Convert Hypersync DataType string to Polars DataType"""
        type_mapping = {
            "float64": pl.Float64,
            "int64": pl.Int64,
            "string": pl.Utf8,
            "bool": pl.Boolean,
            "bytes": pl.Binary
        }
        return type_mapping.get(str(dtype).lower(), pl.Utf8)

    def get_combined_data(self) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        """Combine and return all collected data"""
        try:
            events_df = pl.concat(self.logs_df_list) if self.logs_df_list else None
            
            # Extract unique blocks data from events
            if events_df is not None:
                blocks_df = (events_df
                    .select([
                        "block_number",
                        "block_timestamp"
                    ])
                    .unique(subset=["block_number"])
                    .sort("block_number")
                )
            else:
                blocks_df = None
            
            if events_df is not None:
                logger.info(f"Combined {len(self.logs_df_list)} event dataframes, total rows: {events_df.height}")
            if blocks_df is not None:
                logger.info(f"Combined block data, total unique blocks: {blocks_df.height}")
                
            self.logs_df_list = []
            self.blocks_df_list = []
            self.total_events = 0
            
            return events_df, blocks_df
            
        except Exception as e:
            logger.error(f"Error combining data: {e}")
            raise

class ParallelEventProcessor:
    """Handles parallel processing of multiple event streams"""
    def __init__(self, stream_params: List[StreamParams]):
        self.stream_params = stream_params
        self.processors: Dict[str, EventData] = {}
        self.current_block = min(p.from_block for p in stream_params)
        self._pending_tasks = {}
        
    async def process_events(self) -> Optional[Data]:
        """Process all event streams in parallel"""
        try:
            logger.info(f"Processing {len(self.processors)} streams in parallel")
            
            # Initialize processors for each event stream
            for params in self.stream_params:
                if params.event_name not in self.processors:
                    self.processors[params.event_name] = EventData(params)

            # Create or reuse tasks for active streams
            for event_name, processor in self.processors.items():
                if event_name not in self._pending_tasks:
                    logger.info(f"Creating new task for stream {event_name}")
                    self._pending_tasks[event_name] = asyncio.create_task(
                        self._process_stream(processor),
                        name=f"process_{event_name}"
                    )

            if not self._pending_tasks:
                logger.info("No active tasks remaining")
                return None

            # Wait for any stream to complete with timeout
            done, pending = await asyncio.wait(
                list(self._pending_tasks.values()),
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED
            )

            # Process completed tasks
            combined_data = Data()
            completed_events = []

            for task in done:
                event_name = next(name for name, t in self._pending_tasks.items() if t == task)
                logger.info(f"Processing completed task for {event_name}")
                try:
                    result = await task
                    if result:
                        if result.events:
                            if not combined_data.events:
                                combined_data.events = {}
                            combined_data.events.update(result.events)
                            logger.info(f"Added {len(result.events[event_name])} events for {event_name}")
                        if result.blocks:
                            if not combined_data.blocks:
                                combined_data.blocks = {}
                            combined_data.blocks.update(result.blocks)
                        
                        # Create new task for this stream
                        self._pending_tasks[event_name] = asyncio.create_task(
                            self._process_stream(self.processors[event_name]),
                            name=f"process_{event_name}"
                        )
                        logger.info(f"Created new task for {event_name}")
                    else:
                        # Stream completed
                        completed_events.append(event_name)
                        logger.info(f"Stream {event_name} completed with no data")
                except Exception as e:
                    logger.error(f"Error processing stream {event_name}: {e}")
                    raise

            # Update pending tasks
            self._pending_tasks = {
                name: task for name, task in self._pending_tasks.items()
                if not task.done() and name not in completed_events
            }

            return combined_data if combined_data.events or combined_data.blocks else None

        except Exception as e:
            logger.error(f"Error in parallel event processing: {e}")
            raise

    async def _process_stream(self, processor: EventData) -> Optional[Data]:
        """Process individual event stream"""
        try:
            should_write = await processor.process_batch()
            if should_write:
                events_df, blocks_df = processor.get_combined_data()
                return Data(
                    events={processor.event_name: events_df} if events_df is not None else None,
                    blocks={processor.event_name: blocks_df} if blocks_df is not None else None
                )
            return None
        except Exception as e:
            logger.error(f"Error processing stream {processor.event_name}: {e}")
            raise