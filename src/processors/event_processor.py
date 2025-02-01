from typing import Dict, List, Optional
import asyncio
import logging
from src.types.data import Data
from src.processors.hypersync import EventData
from src.types.hypersync import StreamParams

logger = logging.getLogger(__name__)

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
                self._pending_tasks.values(),
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED
            )

            logger.info(f"Completed tasks: {len(done)}, Pending tasks: {len(pending)}")

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
            self._pending_tasks.update({
                name: task for name, task in self._pending_tasks.items()
                if not task.done() and name not in completed_events
            })

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