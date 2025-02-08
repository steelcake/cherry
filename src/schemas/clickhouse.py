"""ClickHouse schema definitions and type mappings"""

# ClickHouse type mapping
ARROW_TO_CLICKHOUSE_TYPES = {
    'bool': 'Bool',
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'uint8': 'UInt8',
    'uint16': 'UInt16',
    'uint32': 'UInt32',
    'uint64': 'UInt64',
    'float32': 'Float32',
    'float64': 'Float64',
    'string': 'String',
    'large_string': 'String',
    'timestamp[ns]': 'DateTime64(3)',
    'timestamp[us]': 'DateTime64(3)',
    'timestamp[ms]': 'DateTime64(3)',
    'timestamp[s]': 'DateTime64(3)',
}

# Blocks table definition
BLOCKS_TABLE_TEMPLATE = """
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

# Event table template
EVENT_TABLE_TEMPLATE = """
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

# Column mappings
HEX_TO_INT_COLUMNS = ['block_number', 'log_index', 'transaction_index', 'block_timestamp']
STRING_COLUMNS = ['transaction_hash', 'block_hash', 'address', 'data', 'topic0', 'topic1', 'topic2', 'topic3']