import pyarrow as pa

# Block schema definition
BLOCKS = pa.schema([
    pa.field("block_number", pa.int64()),
    pa.field("block_timestamp", pa.int64()),
])

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

# Event schema definition
EVENTS = pa.schema([
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
    pa.field("decoded_from", pa.string()),
    pa.field("decoded_to", pa.string()),
    pa.field("decoded_amount", pa.float64()),
    pa.field("block_timestamp", pa.int64())
])