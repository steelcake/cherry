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

def pad_address(address: str) -> str:
    """Pad address to 32 bytes (64 characters)"""
    if address.startswith('0x'):
        address = address[2:]  # Remove 0x prefix
    return '0x' + '0' * 24 + address  # Pad with 24 zeros (12 bytes) to make it 32 bytes total

def generate_event_query(
    stream: Stream,
    client: HypersyncClient,
    from_block: int,
    items_per_section: int
) -> Tuple[Query, StreamConfig]:
    """Generate query for event data"""
    logger.info(f"Generating event data query for blocks {from_block} onwards")

    # Convert signature to topic0 if not provided in config
    topic0 = stream.topics[0] if stream.topics else signature_to_topic0(stream.signature)
    
    # Build topics list for query
    topics = []
    if stream.topics:
        # First topic is always the event signature
        topics.append([topic0])
        
        # Handle indexed parameters (topics[1:])
        for topic in stream.topics[1:]:
            if topic is None:
                topics.append([])  # Empty list for wildcard match
            elif isinstance(topic, list):
                # Pad each address in the list
                padded_topics = [pad_address(t) for t in topic]
                topics.append(padded_topics)
            else:
                # Pad single address
                topics.append([pad_address(topic)])
    else:
        topics = [[topic0]]  # Default to just the event signature
    
    # Create query
    query = Query(
        from_block=from_block,
        to_block=stream.to_block,
        logs=[LogSelection(
            topics=topics,
            address=stream.address
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
    
    # Log the generated query for debugging
    logger.info(f"Generated Hypersync query for {stream.name}:")
    logger.info(f"  From block: {from_block}")
    logger.info(f"  To block: {stream.to_block}")
    logger.info(f"  Topics: {topics}")
    logger.info(f"  Addresses: {stream.address}")
    
    stream_config = StreamConfig(hex_output=HexOutput.PREFIXED)
    
    return query, stream_config