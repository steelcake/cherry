import pyarrow as pa

# Block schema for Arrow streaming
BLOCK_SCHEMA = pa.schema([
    ("block_hash", pa.string()),
    ("author", pa.string()),
    ("block_number", pa.int64()),
    ("gas_used", pa.int64()),
    ("extra_data", pa.string()),
    ("timestamp", pa.int64()),
    ("base_fee_per_gas", pa.int64()),
    ("chain_id", pa.int64())
])

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