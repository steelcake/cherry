import pyarrow as pa
from dataclasses import dataclass
from typing import List, Dict

# Block schema definition
BLOCKS = pa.schema([
    pa.field("block_number", pa.int64()),
    pa.field("block_timestamp", pa.uint64()),
])

# Base event schema (common fields)
BASE_EVENT_FIELDS = [
    pa.field("removed", pa.bool_()),
    pa.field("log_index", pa.int64()),
    pa.field("transaction_index", pa.int64()),
    pa.field("transaction_hash", pa.string()),
    pa.field("block_hash", pa.string()),
    pa.field("block_number", pa.int64()),
    pa.field("address", pa.string()),
    pa.field("data", pa.string()),
    pa.field("topic0", pa.string()),
    pa.field("topic1", pa.string()),
    pa.field("topic2", pa.string()),
    pa.field("topic3", pa.string()),
    pa.field("block_timestamp", pa.uint64()),
]

# Event-specific fields
TRANSFER_FIELDS = BASE_EVENT_FIELDS + [
    pa.field("decoded_from", pa.string()),
    pa.field("decoded_to", pa.string()),
    pa.field("decoded_amount", pa.float64())
]

APPROVAL_FIELDS = BASE_EVENT_FIELDS + [
    pa.field("decoded_owner", pa.string()),
    pa.field("decoded_spender", pa.string()),
    pa.field("decoded_value", pa.string())
]

# Create schemas
EVENTS = pa.schema(BASE_EVENT_FIELDS)
TRANSFER_EVENTS = pa.schema(TRANSFER_FIELDS)
APPROVAL_EVENTS = pa.schema(APPROVAL_FIELDS)

# Schema mapping
EVENT_SCHEMAS = {
    "Transfer": TRANSFER_EVENTS,
    "Approval": APPROVAL_EVENTS
}

# Transaction schema definition
TRANSACTIONS = pa.schema([
    pa.field("transaction_hash", pa.string()),
    pa.field("block_number", pa.int64()),
    pa.field("from_address", pa.string()),
    pa.field("to_address", pa.string()),
    pa.field("value", pa.int64()),
    pa.field("event_name", pa.string()),
    pa.field("contract_address", pa.string()),
    pa.field("event_signature", pa.string()),
    pa.field("raw_data", pa.string())
])

@dataclass
class BlockchainSchema:
    """Schema definitions for blockchain data"""
    
    BLOCKS_TABLE_NAME = "blocks"
    
    # Column groups
    HEX_COLUMNS = ['block_number', 'log_index', 'transaction_index', 'block_timestamp']
    STRING_COLUMNS = ['transaction_hash', 'block_hash', 'address', 'data', 'topic0', 'topic1', 'topic2', 'topic3']
    TIMESTAMP_COLUMNS = ['block_timestamp']
    
    @staticmethod
    def get_clickhouse_blocks_ddl() -> str:
        """Get ClickHouse blocks table DDL"""
        return """
            CREATE TABLE IF NOT EXISTS blocks (
                block_number Int64,
                block_hash String,
                block_parent_hash String,
                block_timestamp DateTime64(3),
                block_transaction_count Int32,
                block_size Int32,
                PRIMARY KEY (block_number)
            ) ENGINE = ReplacingMergeTree
            ORDER BY block_number
        """
    
    @staticmethod
    def get_clickhouse_events_ddl(table_name: str) -> str:
        """Get ClickHouse events table DDL"""
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                log_index UInt64,
                transaction_index UInt64,
                block_number UInt64,
                address String,
                data String,
                topic0 String,
                topic1 String,
                topic2 String,
                topic3 String,
                block_timestamp DateTime64(3),
                INDEX address_idx address TYPE bloom_filter GRANULARITY 1
            ) ENGINE = MergeTree
            PRIMARY KEY (log_index, block_number, address)
            ORDER BY (log_index, block_number, address)
        """

    @staticmethod
    def get_postgres_blocks_ddl() -> str:
        """Get PostgreSQL blocks table DDL"""
        return """
            CREATE TABLE IF NOT EXISTS blocks (
                block_number BIGINT PRIMARY KEY,
                block_hash TEXT,
                block_parent_hash TEXT,
                block_timestamp TIMESTAMP,
                block_transaction_count INTEGER,
                block_size INTEGER
            );
        """

    @staticmethod
    def get_postgres_events_ddl(table_name: str) -> str:
        """Get PostgreSQL events table DDL"""
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                log_index BIGINT,
                transaction_index BIGINT,
                block_number BIGINT,
                address TEXT,
                data TEXT,
                topic0 TEXT,
                topic1 TEXT,
                topic2 TEXT,
                topic3 TEXT,
                block_timestamp TIMESTAMP,
                PRIMARY KEY (log_index, block_number, address)
            );
            CREATE INDEX IF NOT EXISTS idx_{table_name}_address ON {table_name} (address);
        """