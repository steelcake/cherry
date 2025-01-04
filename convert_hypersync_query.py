from hypersync import (
    Query,
    LogSelection,
    TransactionSelection,
    FieldSelection,
    LogField,
    BlockField
)
from parse import Config, Event, parse_config
from pathlib import Path
from typing import List

def convert_event_to_hypersync(event: Event) -> LogSelection:
    """Convert an Event model to Hypersync LogSelection"""
    return LogSelection(
        address=event.address,
        topics=event.topics
    )

def convert_transaction_filters(config: Config) -> TransactionSelection:
    """Convert transaction filters from config to Hypersync TransactionSelection"""
    if not config.transactions:
        return None
    
    return TransactionSelection(
        from_=config.transactions.from_address,
        to=config.transactions.to_address
    )


def generate_hypersync_query(config: Config) -> Query:
    """Generate a Hypersync Query from the config"""
    # Convert events to LogSelections
    event_filters = [convert_event_to_hypersync(event) for event in config.events]
    
    # Create transaction filter
    tx_filter = convert_transaction_filters(config)
    
    # Create and return the Query
    return Query(
        from_block=0,
        field_selection=FieldSelection(
            log=[e.value for e in LogField],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
        logs=event_filters,
        transactions=tx_filter
    )

if __name__ == "__main__":
    # Parse the config file
    config = parse_config(Path("config.yaml"))
    
    # Generate Hypersync query
    query = generate_hypersync_query(config)
    
    # Print the query
    print(query)