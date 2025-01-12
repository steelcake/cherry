import pyarrow as pa

# Transaction schema for Arrow streaming
TRANSACTION_SCHEMA = pa.schema([
    ("transaction_hash", pa.string()),
    ("block_number", pa.int64()),
    ("from_address", pa.string()),
    ("to_address", pa.string()),
    ("value", pa.int64()),
    ("event_name", pa.string()),
    ("contract_address", pa.string()),
    ("topic0", pa.string()),
    ("raw_data", pa.string())
])

# Block schema for Arrow streaming
BLOCK_SCHEMA = pa.schema([
    ("number", pa.int64()),
    ("timestamp", pa.int64())
])

# Event schema for Arrow streaming
EVENT_SCHEMA = pa.schema([
    ("transaction_hash", pa.string()),
    ("block_number", pa.int64()),
    ("from_address", pa.string()),
    ("to_address", pa.string()),
    ("value", pa.int64()),
    ("event_name", pa.string()),
    ("contract_address", pa.string()),
    ("topic0", pa.string()),
    ("raw_data", pa.string())
]) 