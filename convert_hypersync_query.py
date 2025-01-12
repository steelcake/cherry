from hypersync import (
    Query,
    LogSelection,
    TransactionSelection,
    FieldSelection,
    LogField,
    BlockField
)
from src.config.parser import Config, Event, parse_config
from pathlib import Path
from typing import List
import logging, json
from src.utils.logging_setup import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def convert_event_to_hypersync(event: Event) -> LogSelection:
    """Convert an Event model to Hypersync LogSelection"""
    logger.debug(f"Converting event {event.name} to Hypersync LogSelection")
    
    # For address: only use if it's a single address
    address = None
    if event.address and isinstance(event.address, list) and len(event.address) == 1:
        address = event.address[0]
    
    # For topics: only use the event signature as the first topic
    topics = [event.signature] if event.signature else None
    
    selection = LogSelection(
        address=address,
        topics=topics
    )
    logger.debug(f"Created LogSelection: {selection}")
    return selection

def convert_transaction_filters(config: Config) -> TransactionSelection:
    """Convert transaction filters from config to Hypersync TransactionSelection"""
    logger.debug("Converting transaction filters to Hypersync TransactionSelection")
    if config.transactions:
        logger.debug(f"Transaction filters: {json.dumps(config.transactions.model_dump(), indent=2)}")
    
    tx_selection = TransactionSelection(
        from_=config.transactions.from_address if config.transactions else None,
        to=config.transactions.to_address if config.transactions else None
    )
    logger.debug(f"Created TransactionSelection: {tx_selection}")
    return tx_selection

def generate_hypersync_query(config: Config) -> Query:
    """Generate a Hypersync Query from the config"""
    logger.info("Generating Hypersync query from configuration")
    
    # Convert events to LogSelections
    event_filters = [convert_event_to_hypersync(event) for event in config.events]
    logger.debug(f"Created {len(event_filters)} event filters")
    
    # Create transaction filter
    tx_filter = convert_transaction_filters(config)
    
    # Create and return the Query
    query = Query(
        from_block=0,
        field_selection=FieldSelection(
            log=[e.value for e in LogField],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
        logs=event_filters,
        transactions=tx_filter
    )
    logger.info("Successfully generated Hypersync query")
    return query

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        # Parse the config file
        logger.info("Parsing configuration file")
        config = parse_config(Path("config.yaml"))
        
        # Generate Hypersync query
        query = generate_hypersync_query(config)
        
        # Print the query
        print(query)
        logger.info("Successfully generated and printed query")
    except Exception as e:
        logger.error(f"Error generating Hypersync query: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")