import pyarrow as pa

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