import logging
import sys
from typing import Dict, Optional, AsyncGenerator
import pandas as pd
import pyarrow as pa
from datetime import datetime
from src.types.data import Data
from cherry_core import evm_decode_events, evm_signature_to_topic0
from cherry_core import ingest
import asyncio

logger = logging.getLogger(__name__)

class SQDProcessor:
    def __init__(self, url: str, query: Dict, event_signature: Optional[str] = None, allow_decode_fail: bool = False):
        self.url = url
        self.query = query
        self.event_signature = event_signature
        self.allow_decode_fail = allow_decode_fail
        self._current_block = 0
        self.stream = self._init_stream()

    def _init_stream(self):
        """Initialize SQD stream"""
        log_request = ingest.LogRequest(
            address=self.query['logs'][0].get('address', []),
            topic0=[evm_signature_to_topic0(sig) for sig in self.query['logs'][0].get('event_signatures', [])]
        )
        
        return ingest.start_stream(ingest.StreamConfig(
            format=ingest.Format.EVM,
            provider=ingest.Provider(
                kind=ingest.ProviderKind.SQD,
                config=ingest.ProviderConfig(url=self.url)
            ),
            query=ingest.EvmQuery(
                from_block=self.query.get('from_block', 0),
                logs=[log_request],
                fields=ingest.Fields(
                    block=ingest.BlockFields(number=True),
                    log=ingest.LogFields(
                        data=True, topic0=True, 
                        topic1=True, topic2=True, topic3=True
                    )
                )
            )
        ))

    async def process_data(self) -> AsyncGenerator[Data, None]:
        """Process data from SQD stream"""
        try:
            start_time = datetime.now()
            
            while True:
                batch = await self.stream.next()
                if not batch:
                    break

                logger.debug(f"Received batch with {len(batch)} record types")
                
                # Process blocks and events in parallel
                tasks = [
                    self._process_record_type('blocks', batch.get('blocks')),
                    self._process_record_type('events', batch.get('logs')),
                ]
                
                # Run tasks concurrently
                results = await asyncio.gather(*tasks)
                
                # Yield non-empty data
                for data in results:
                    if data and not data.is_empty():
                        yield data

            logger.info(f"Processing complete in {(datetime.now() - start_time).total_seconds():.1f}s")
        
        except Exception as e:
            logger.error(f"Error processing SQD data: {e}", exc_info=True)
            raise

    async def _process_record_type(self, record_type: str, batch) -> Optional[Data]:
        """Process a single record type"""
        if batch is None or batch.num_rows == 0:
            logger.debug(f"Skipping empty {record_type} batch")
            return None

        logger.debug(f"Processing {record_type} batch with {batch.num_rows} rows")
        
        if record_type == 'events':
            # Decode events if needed
            if self.event_signature:
                batch = evm_decode_events(self.event_signature, batch)
        
        return self._create_single_stream_data(record_type, batch)

    def _create_single_stream_data(self, stream_type: str, batch):
        """Create Data object for a single stream"""
        if stream_type == 'events':
            logger.debug(f"Creating events data with {batch.num_rows} rows")
            return Data(events={'events': batch})
        elif stream_type == 'blocks':
            logger.debug(f"Creating blocks data with {batch.num_rows} rows")
            return Data(blocks={'blocks': batch})
        logger.warning(f"Unknown stream type: {stream_type}")
        return None

    @property
    def current_block(self) -> int:
        """Get the current block number"""
        return self._current_block 