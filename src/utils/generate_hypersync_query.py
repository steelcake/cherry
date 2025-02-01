from hypersync import (
    HypersyncClient,
    Query,
    LogSelection,
    TransactionSelection,
    FieldSelection,
    LogField,
    BlockField,
    signature_to_topic0,
    StreamConfig,
    HexOutput,
    ColumnMapping,
    DataType
)
import polars as pl
from src.config.parser import Config, Stream
from src.types.hypersync import StreamParams
from typing import List, Union, Optional, Tuple, Dict
import logging

logger = logging.getLogger(__name__)

def convert_column_cast_to_dict(column_cast) -> Dict[str, str]:
    """Convert ColumnCast model to dictionary"""
    if not column_cast:
        return {}
    
    result = {}
    if column_cast.transaction:
        result.update(column_cast.transaction)
    if column_cast.value:
        result['value'] = column_cast.value
    if column_cast.amount:
        result['amount'] = column_cast.amount
    return result

def convert_event_to_hypersync(stream: Stream) -> LogSelection:
    """Convert event config to Hypersync LogSelection"""
    if not stream or not stream.signature:
        raise ValueError("Stream or signature is missing")

    # Convert signature to topic0
    topic0 = signature_to_topic0(stream.signature)
    
    return LogSelection(
        topics=[[topic0]]  # First topic is the event signature
    )

def generate_contract_stream_config() -> StreamConfig:
    """Generate StreamConfig for contract address queries"""
    return StreamConfig(hex_output=HexOutput.PREFIXED)

def generate_event_stream_config(stream: Stream) -> StreamConfig:
    """Generate StreamConfig for event data queries"""
    return StreamConfig(
        hex_output=HexOutput.PREFIXED,
        event_signature=stream.signature,
        column_mapping=ColumnMapping(
            decoded_log=stream.column_cast,
            block={BlockField.TIMESTAMP: DataType.INT64}
        )
    )

def generate_event_stream_params(client: HypersyncClient, stream: Stream,
                               contract_addr_list: Optional[List[pl.Series]], 
                               from_block: int, items_per_section: int) -> StreamParams:
    """Generate StreamParams for event data queries"""
    return StreamParams(
        client=client,
        event_name=stream.name,
        signature=stream.signature,
        contract_addr_list=contract_addr_list,
        from_block=from_block,
        items_per_section=items_per_section,
        column_mapping=stream.column_cast
    )

def generate_contract_query(config: Config, signature: str, client: HypersyncClient) -> Query:
    """Generate query for contract address discovery"""
    return Query(
        from_block=config.blocks.from_block,
        to_block=config.blocks.to_block,
        logs=[LogSelection(signature=signature)],
        blocks=[BlockField.NUMBER, BlockField.TIMESTAMP]
    )

def generate_event_query(
    stream: Stream,
    client: HypersyncClient,
    from_block: int,
    items_per_section: int
) -> Tuple[Query, StreamConfig]:
    """Generate query for event data"""
    logger.info(f"Generating event data query for blocks {from_block} onwards")

    # Convert signature to topic0
    topic0 = signature_to_topic0(stream.signature)
    
    query = Query(
        from_block=from_block,
        to_block=stream.to_block,  # Use stream-specific to_block
        logs=[LogSelection(
            topics=[[topic0]]
                )],
        field_selection=FieldSelection(
            log=[
                LogField.ADDRESS,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_INDEX,
                LogField.LOG_INDEX
            ],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP]
        )
    )
    
    stream_config = StreamConfig(hex_output=HexOutput.PREFIXED)
    
    return query, stream_config