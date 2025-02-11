import logging
from typing import Optional, Dict, List, AsyncGenerator
from dataclasses import dataclass
import pandas as pd
from src.config.parser import Stream
from src.processors.hypersync import EventProcessor
from src.types.data import Data

logger = logging.getLogger(__name__)

@dataclass
class StreamConfig:
    """Stream configuration"""
    name: str
    kind: str
    signature: Optional[str] = None
    addresses: Optional[List[str]] = None
    from_block: Optional[int] = None
    resume_path: Optional[str] = None
    resume: bool = False

class StreamManager:
    """Manages data streams for blocks and events"""
    
    def __init__(self, streams: List[Stream], processor: EventProcessor):
        self.processor = processor
        self.streams = self._init_streams(streams)
        logger.info(f"Initialized {len(self.streams)} streams: {', '.join(self.streams.keys())}")

    def _init_streams(self, configs: List[Stream]) -> Dict[str, StreamConfig]:
        """Initialize stream configurations"""
        streams = {}
        for config in configs:
            name = config.kind if config.kind == 'block' else config.event_name
            streams[name] = StreamConfig(
                name=name,
                kind=config.kind,
                signature=config.signature if hasattr(config, 'signature') else None,
                addresses=config.addresses if hasattr(config, 'addresses') else None,
                from_block=config.from_block,
                resume_path=config.state.path if hasattr(config, 'state') else None,
                resume=config.state.resume if hasattr(config, 'state') else False
            )
        return streams

    async def process_stream(self, stream: StreamConfig) -> AsyncGenerator[Data, None]:
        """Process a single stream"""
        try:
            if stream.kind == 'block':
                async for data in self.processor.process_blocks(stream.from_block):
                    yield Data(blocks={stream.name: data})
            else:
                async for events, blocks in self.processor.process_events(
                    stream.name,
                    stream.signature,
                    stream.from_block,
                    stream.addresses
                ):
                    yield Data(
                        events={stream.name: events},
                        blocks={stream.name: blocks}
                    )
        except Exception as e:
            logger.error(f"Error processing stream {stream.name}: {e}")
            raise

    async def process_all(self) -> AsyncGenerator[Data, None]:
        """Process all configured streams"""
        for stream in self.streams.values():
            async for data in self.process_stream(stream):
                if data.events or data.blocks:
                    yield data 