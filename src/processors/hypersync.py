import logging
import time
from typing import AsyncGenerator, Tuple, Optional, List, Dict
import pandas as pd
import polars as pl
from hypersync import HypersyncClient, ClientConfig, ArrowResponse
from src.utils.generate_hypersync_query import generate_event_query, generate_block_query
from src.types.data import Data
from src.config.parser import Config, Stream
import aiohttp

logger = logging.getLogger(__name__)

class EventProcessor:
    """Processes blockchain events and blocks from Hypersync"""
    
    def __init__(self, config: Config):
        self.client = HypersyncClient(
            ClientConfig(
                url=config.data_source[0].url,
                auth_header=f"Bearer {config.data_source[0].token}"
            )
        )
        self.batch_size = config.streams[0].batch_size
        self._current_block = 0
        self.config = config

    async def process_blocks(self, from_block: int) -> AsyncGenerator[pd.DataFrame, None]:
        """Process block data"""
        current_block = from_block
        last_log_time = time.time()
        total_blocks = 0
        
        while True:
            try:
                # Get block stream configuration from config
                block_stream = next((s for s in self.config.streams if s.kind == 'block'), None)
                column_mapping = block_stream.column_cast if block_stream else None
                
                query, stream_config = generate_block_query(
                    from_block=current_block,
                    to_block=current_block + self.batch_size,
                    include_transactions=block_stream.include_transactions if block_stream else True,
                    include_logs=block_stream.include_logs if block_stream else False,
                    column_mapping=column_mapping
                )
                
                receiver = await self.client.stream_arrow(query, stream_config)
                
                while True:
                    res = await receiver.recv()
                    if res is None:
                        break
                        
                    # Process blocks data
                    blocks_df = self._process_block_response(res)
                    if blocks_df.empty:
                        break
                        
                    # Update progress
                    current_block = blocks_df['number'].max() + 1
                    self._current_block = current_block
                    total_blocks += len(blocks_df)
                    
                    # Log progress every 2 seconds
                    current_time = time.time()
                    if current_time - last_log_time >= 2:
                        blocks_processed = current_block - from_block
                        blocks_per_second = blocks_processed / (current_time - last_log_time)
                        logger.info(
                            f"Processed {total_blocks} blocks "
                            f"({blocks_per_second:.0f} blocks/s)"
                        )
                        last_log_time = current_time
                    
                    yield blocks_df
                    
            except Exception as e:
                logger.error(f"Error processing blocks: {e}")
                raise

    def _process_block_response(self, res: ArrowResponse) -> pd.DataFrame:
        """Process block response data"""
        blocks_df = pl.from_arrow(res.data.blocks)
        transactions_df = pl.from_arrow(res.data.transactions)
        
        if blocks_df.is_empty():
            return pd.DataFrame()
            
        # Add transaction data if available
        if not transactions_df.is_empty():
            # Prefix transaction columns
            transactions_df = transactions_df.rename(lambda n: f"transaction_{n}")
            blocks_df = blocks_df.join(
                transactions_df,
                left_on="number",
                right_on="transaction_block_number"
            )
            
        return blocks_df.to_pandas()

    async def process_events(
        self, 
        event_name: str,
        signature: str,
        from_block: int,
        addresses: Optional[List[str]] = None
    ) -> AsyncGenerator[Tuple[pd.DataFrame, pd.DataFrame], None]:
        """Process event data and associated blocks"""
        current_block = from_block
        last_log_time = time.time()
        total_events = 0
        
        # Get event stream configuration
        event_stream = next(
            (s for s in self.config.streams 
             if s.kind == 'event' and s.name == event_name),
            None
        )
        
        while True:
            try:
                query, stream_config = generate_event_query(
                    signature=signature,
                    from_block=current_block,
                    to_block=current_block + self.batch_size,
                    addresses=addresses,
                    include_transactions=event_stream.include_transactions if event_stream else False,
                    column_mapping=event_stream.column_cast if event_stream else None
                )
                
                receiver = await self.client.stream_arrow(query, stream_config)
                
                while True:
                    res = await receiver.recv()
                    if res is None:
                        break
                        
                    # Process events data
                    events_df = self._process_event_response(res, event_name)
                    if events_df.empty:
                        break
                        
                    # Get associated blocks if needed
                    blocks_df = None
                    if event_stream and event_stream.include_blocks:
                        blocks_query, blocks_config = generate_block_query(
                            from_block=events_df['block_number'].min(),
                            to_block=events_df['block_number'].max()
                        )
                        blocks_receiver = await self.client.stream_arrow(blocks_query, blocks_config)
                        blocks_df = await self._get_blocks_data(blocks_receiver)
                    
                    # Update progress
                    current_block = events_df['block_number'].max() + 1
                    self._current_block = current_block
                    total_events += len(events_df)
                    
                    # Log progress every 2 seconds
                    current_time = time.time()
                    if current_time - last_log_time >= 2:
                        blocks_processed = current_block - from_block
                        speed = total_events / (current_time - last_log_time)
                        blocks_per_second = blocks_processed / (current_time - last_log_time)
                        logger.info(
                            f"Event: {event_name} - Processed {len(events_df)} events "
                            f"(Total: {total_events}), Speed: {speed:.0f} events/s, "
                            f"{blocks_per_second:.0f} blocks/s"
                        )
                        last_log_time = current_time
                    
                    yield events_df, blocks_df
                    
            except Exception as e:
                logger.error(f"Error processing {event_name} events: {e}")
                raise

    def _process_event_response(self, res: ArrowResponse, event_name: str) -> pd.DataFrame:
        """Process event response data"""
        logs_df = pl.from_arrow(res.data.logs)
        decoded_logs_df = pl.from_arrow(res.data.decoded_logs)
        blocks_df = pl.from_arrow(res.data.blocks)
        transactions_df = pl.from_arrow(res.data.transactions)
        
        if decoded_logs_df.is_empty():
            return pd.DataFrame()
            
        # Combine data
        prefixed_decoded_logs = decoded_logs_df.rename(lambda n: f"decoded_{n}")
        prefixed_blocks = blocks_df.rename(lambda n: f"block_{n}")
        
        combined_df = (
            logs_df
            .join(prefixed_decoded_logs)
            .join(prefixed_blocks, on="block_number")
        )
        
        # Add transaction data if available
        if not transactions_df.is_empty():
            prefixed_transactions = transactions_df.rename(lambda n: f"transaction_{n}")
            combined_df = combined_df.join(
                prefixed_transactions,
                left_on="transaction_hash",
                right_on="transaction_hash"
            )
        
        return combined_df.to_pandas()

    async def _get_blocks_data(self, receiver) -> pd.DataFrame:
        """Get blocks data from receiver"""
        blocks_dfs = []
        while True:
            res = await receiver.recv()
            if res is None:
                break
            blocks_df = pl.from_arrow(res.data.blocks)
            if not blocks_df.is_empty():
                blocks_dfs.append(blocks_df)
                
        if not blocks_dfs:
            return pd.DataFrame()
            
        return pl.concat(blocks_dfs).to_pandas()

    @property
    def current_block(self) -> int:
        """Get current block number"""
        return self._current_block

    async def close(self):
        """Clean up resources"""
        pass