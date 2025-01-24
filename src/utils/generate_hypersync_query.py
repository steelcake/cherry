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
from src.config.parser import Config, Event
from src.types.hypersync import StreamParams
from typing import List, Union, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def generate_contract_stream_config() -> StreamConfig:
    """Generate StreamConfig for contract address queries"""
    return StreamConfig(hex_output=HexOutput.PREFIXED)

def generate_event_stream_config(event: Event) -> StreamConfig:
    """Generate StreamConfig for event data queries"""
    return StreamConfig(
        hex_output=HexOutput.PREFIXED,
        event_signature=event.signature,
        column_mapping=ColumnMapping(
            decoded_log=event.column_mapping,
            block={BlockField.TIMESTAMP: DataType.INT64}
        )
    )

def generate_event_stream_params(client: HypersyncClient, event: Event,
                               contract_addr_list: Optional[List[pl.Series]], 
                               from_block: int, items_per_section: int) -> StreamParams:
    """Generate StreamParams for event data queries"""
    return StreamParams(
        client=client,
        event_name=event.name,
        signature=event.signature,
        contract_addr_list=contract_addr_list,
        from_block=from_block,
        items_per_section=items_per_section,
        column_mapping=event.column_mapping
    )

def convert_event_to_hypersync(event: Event) -> LogSelection:
    """Convert an Event model to Hypersync LogSelection"""
    topic0 = signature_to_topic0(event.signature)
    addresses = event.filters.get('addresses', []) if event.filters else []
    topics = [[topic0]]
    
    if event.filters and event.filters.get('topics'):
        topics.extend(event.filters['topics'])
    
    return LogSelection(
        address=addresses if addresses else None,
        topics=topics
    )

def generate_contract_query(signature: str, from_block: int) -> Tuple[Query, StreamConfig]:
    """Generate a query and stream config to fetch contract addresses"""
    topic0 = signature_to_topic0(signature)
    query = Query(
        from_block=from_block,
        logs=[LogSelection(topics=[[topic0]])],
        field_selection=FieldSelection(log=[LogField.ADDRESS])
    )
    return query, generate_contract_stream_config()

def generate_event_query(config: Config, event: Event, client: HypersyncClient,
                        contract_addr_list: Optional[List[pl.Series]], 
                        from_block: int, items_per_section: int) -> Tuple[Query, StreamConfig, StreamParams]:
    """Generate query, stream config and params for event data"""
    logger.info(f"Generating event data query for blocks {from_block}")
    
    query = Query(
        from_block=from_block,
        to_block=None if config.blocks.range.to_block is None else config.blocks.range.to_block + 1,
        field_selection=FieldSelection(
            log=[e.value for e in LogField],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
        logs=[convert_event_to_hypersync(event)]
    )
    
    return (
        query,
        generate_event_stream_config(event),
        generate_event_stream_params(client, event, contract_addr_list, from_block, items_per_section)
    )