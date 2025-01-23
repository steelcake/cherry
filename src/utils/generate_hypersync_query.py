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
        client=client, column_mapping=event.column_mapping,
        event_name=event.name, signature=event.signature,
        contract_addr_list=contract_addr_list, from_block=from_block,
        items_per_section=items_per_section
    )

def convert_event_to_hypersync(event: Event) -> LogSelection:
    """Convert an Event model to Hypersync LogSelection"""
    logger.debug(f"Converting event {event.name} to Hypersync LogSelection")
    topic0 = signature_to_topic0(event.signature) if event.signature else None
    addresses = []
    if isinstance(event.address, list): addresses.extend(event.address)
    elif event.address: addresses.append(event.address)
    
    selection = LogSelection(
        address=addresses,
        topics=[[topic0]] if topic0 else None
    )
    logger.debug(f"Created LogSelection: {selection}")
    return selection

def generate_contract_query(signature: str, from_block: int) -> Tuple[Query, StreamConfig]:
    """Generate a query and stream config to fetch contract addresses"""
    logger.info(f"Generating contract address query for signature {signature}")
    topic0 = signature_to_topic0(signature)
    query = Query(
        from_block=from_block,
        logs=[LogSelection(topics=[[topic0]])],
        field_selection=FieldSelection(log=[LogField.ADDRESS])
    )
    logger.debug(f"Contract query: {query}")
    return query, generate_contract_stream_config()

def generate_event_query(config: Config, event: Event, client: HypersyncClient,
                        contract_addr_list: Optional[List[pl.Series]], 
                        from_block: int, items_per_section: int) -> Tuple[Query, StreamConfig, StreamParams]:
    """Generate query, stream config and params for event data"""
    logger.info(f"Generating event data query for blocks {from_block}")
    
    # Convert events to LogSelections
    event_filters = [convert_event_to_hypersync(event)]
    logger.debug(f"Created event filter for {event.name}")
    
    # Create query
    query = Query(
        from_block=from_block,
        field_selection=FieldSelection(
            log=[e.value for e in LogField],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
        logs=event_filters
    )
    logger.info("Successfully generated event data query")
    logger.debug(f"Event query: {query}")
    
    return (
        query,
        generate_event_stream_config(event),
        generate_event_stream_params(client, event, contract_addr_list, from_block, items_per_section)
    )