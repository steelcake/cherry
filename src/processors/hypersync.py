import logging
import time
from typing import AsyncGenerator, Dict, Optional, List
import polars as pl
from hypersync import (
    HypersyncClient, ClientConfig, ArrowResponse,
    BlockField, TransactionField, LogField, TraceField,
    Query, StreamConfig, BlockSelection, LogSelection,
    FieldSelection, ColumnMapping, DataType, HexOutput,
    JoinMode, signature_to_topic0
)
from src.types.data import Data

logger = logging.getLogger(__name__)

class HypersyncProcessor:
    """Processes blockchain data from Hypersync"""
    
    def __init__(self, url: str, token: str):
        self.client = HypersyncClient(
            ClientConfig(url=url, auth_header=f"Bearer {token}")
        )
        self._current_block = 0

    async def process_blocks(
        self,
        from_block: int,
        to_block: Optional[int] = None,
        batch_size: int = 10000,
        include_logs: bool = True,
        include_traces: bool = True
    ) -> AsyncGenerator[Data, None]:
        """Process block data with transactions"""
        try:
            query = Query(
                from_block=from_block,
                to_block=None if to_block is None else to_block + 1,
                join_mode=JoinMode.JOIN_ALL,
                blocks=[BlockSelection()],
                field_selection=FieldSelection(
                    block=[field.value for field in BlockField],
                    transaction=[field.value for field in TransactionField],
                    log=[field.value for field in LogField] if include_logs else None,
                    trace=[field.value for field in TraceField] if include_traces else None
                )
            )

            stream_config = StreamConfig(
                hex_output=HexOutput.PREFIXED,
                column_mapping=ColumnMapping(
                    block=self._get_block_mapping(),
                    transaction=self._get_transaction_mapping()
                )
            )

            receiver = await self.client.stream_arrow(query, stream_config)
            
            while True:
                start_time = time.time()
                res = await receiver.recv()
                if res is None:
                    break

                data = self._process_block_response(res)
                if not data.is_empty():
                    self._current_block = data.get_latest_block()
                    logger.info(
                        f"Processed blocks {data.get_block_range()} in "
                        f"{time.time() - start_time:.2f}s"
                    )
                    yield data

        except Exception as e:
            logger.error(f"Error processing blocks: {e}")
            raise

    async def process_events(
        self,
        signature: str,
        from_block: int,
        to_block: Optional[int] = None,
        addresses: Optional[List[str]] = None,
        include_transactions: bool = False,
        column_mapping: Optional[Dict[str, str]] = None,
        batch_size: int = 10000
    ) -> AsyncGenerator[Data, None]:
        """Process event data"""
        try:
            topic0 = signature_to_topic0(signature)
            
            query = Query(
                from_block=from_block,
                to_block=None if to_block is None else to_block + 1,
                logs=[LogSelection(
                    address=addresses,
                    topics=[[topic0]]
                )],
                field_selection=FieldSelection(
                    log=[field.value for field in LogField],
                    block=[BlockField.NUMBER.value, BlockField.TIMESTAMP.value],
                    transaction=[TransactionField.HASH.value, TransactionField.FROM.value] 
                    if include_transactions else None
                )
            )

            stream_config = StreamConfig(
                hex_output=HexOutput.PREFIXED,
                event_signature=signature,
                column_mapping=ColumnMapping(
                    decoded_log=self._get_event_mapping(column_mapping),
                    block={BlockField.TIMESTAMP: DataType.INT64}
                )
            )

            receiver = await self.client.stream_arrow(query, stream_config)
            
            while True:
                start_time = time.time()
                res = await receiver.recv()
                if res is None:
                    break

                data = self._process_event_response(res)
                if not data.is_empty():
                    self._current_block = data.get_latest_block()
                    logger.info(
                        f"Processed events {data.get_block_range()} in "
                        f"{time.time() - start_time:.2f}s"
                    )
                    yield data

        except Exception as e:
            logger.error(f"Error processing events: {e}")
            raise

    def _process_block_response(self, res: ArrowResponse) -> Data:
        """Process block response data"""
        blocks_df = pl.from_arrow(res.data.blocks)
        transactions_df = pl.from_arrow(res.data.transactions)
        logs_df = pl.from_arrow(res.data.logs)
        traces_df = pl.from_arrow(res.data.traces)

        if blocks_df.is_empty():
            return Data()

        # Add block timestamp to all dataframes
        block_timestamp_df = blocks_df.select([
            pl.col("number").alias("block_number"),
            pl.col("timestamp").alias("block_timestamp"),
        ])

        result = {}
        
        if not blocks_df.is_empty():
            blocks_df = blocks_df.with_columns([
                blocks_df.get_column("number").alias("block_number"),
                blocks_df.get_column("timestamp").alias("block_timestamp"),
            ])
            result['blocks'] = blocks_df.to_pandas()

        if not transactions_df.is_empty():
            transactions_df = transactions_df.join(
                block_timestamp_df, on="block_number"
            )
            result['transactions'] = transactions_df.to_pandas()

        if not logs_df.is_empty():
            logs_df = logs_df.join(block_timestamp_df, on="block_number")
            result['logs'] = logs_df.to_pandas()

        if not traces_df.is_empty():
            traces_df = traces_df.join(block_timestamp_df, on="block_number")
            result['traces'] = traces_df.to_pandas()

        return Data(**result)

    def _process_event_response(self, res: ArrowResponse) -> Data:
        """Process event response data"""
        logs_df = pl.from_arrow(res.data.logs)
        decoded_logs_df = pl.from_arrow(res.data.decoded_logs)
        blocks_df = pl.from_arrow(res.data.blocks)
        transactions_df = pl.from_arrow(res.data.transactions)

        if decoded_logs_df.is_empty():
            return Data()

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

        return Data(events={'events': combined_df.to_pandas()})

    @staticmethod
    def _get_block_mapping() -> Dict[BlockField, DataType]:
        """Get block column mapping"""
        return {
            BlockField.DIFFICULTY: DataType.INTSTR,
            BlockField.TOTAL_DIFFICULTY: DataType.INTSTR,
            BlockField.SIZE: DataType.INTSTR,
            BlockField.GAS_LIMIT: DataType.INTSTR,
            BlockField.GAS_USED: DataType.INTSTR,
            BlockField.TIMESTAMP: DataType.INT64,
            BlockField.BASE_FEE_PER_GAS: DataType.INTSTR,
            BlockField.BLOB_GAS_USED: DataType.INTSTR,
            BlockField.EXCESS_BLOB_GAS: DataType.INTSTR,
            BlockField.SEND_COUNT: DataType.INTSTR,
        }

    @staticmethod
    def _get_transaction_mapping() -> Dict[TransactionField, DataType]:
        """Get transaction column mapping"""
        return {
            TransactionField.GAS: DataType.INTSTR,
            TransactionField.GAS_PRICE: DataType.INTSTR,
            TransactionField.NONCE: DataType.INTSTR,
            TransactionField.VALUE: DataType.INTSTR,
            TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.INTSTR,
            TransactionField.MAX_FEE_PER_GAS: DataType.INTSTR,
            TransactionField.CHAIN_ID: DataType.INT64,
            TransactionField.EFFECTIVE_GAS_PRICE: DataType.INTSTR,
            TransactionField.GAS_USED: DataType.INTSTR,
            TransactionField.CUMULATIVE_GAS_USED: DataType.INTSTR,
            TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.INTSTR,
        }

    @staticmethod
    def _get_event_mapping(mapping: Optional[Dict[str, str]] = None) -> Dict[str, DataType]:
        """Convert string type mapping to DataType mapping"""
        if not mapping:
            return {}
            
        type_map = {
            'int64': DataType.INT64,
            'int32': DataType.INT32,
            'uint64': DataType.UINT64,
            'uint32': DataType.UINT32,
            'float64': DataType.FLOAT64,
            'float32': DataType.FLOAT32,
            'string': DataType.STRING,
            'intstr': DataType.INTSTR
        }
        
        return {
            field: type_map.get(type_str.lower(), DataType.INTSTR)
            for field, type_str in mapping.items()
        }

    @property
    def current_block(self) -> int:
        """Get current block number"""
        return self._current_block