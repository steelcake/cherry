from .base import BlockchainSchema

# Block schema definition
BLOCKS = BlockchainSchema("blocks", {
    "block_hash": "string",
    "author": "string", 
    "block_number": "int64",
    "gas_used": "int64",
    "extra_data": "string",
    "timestamp": "int64",
    "base_fee_per_gas": "int64",
    "chain_id": "int64"
})

# Transaction schema definition
TRANSACTIONS = BlockchainSchema("transactions", {
    "transaction_hash": "string",
    "block_number": "int64",
    "from_address": "string",
    "to_address": "string",
    "value": "int64",
    "event_name": "string",
    "contract_address": "string",
    "event_signature": "string",
    "raw_data": "string"
})

# Event schema definition
EVENTS = BlockchainSchema("events", {
    "removed": "bool",
    "log_index": "int64",
    "transaction_index": "int64",
    "transaction_hash": "string",
    "block_hash": "string",
    "block_number": "int64",
    "address": "string",
    "data": "string",
    "topic0": "string",
    "topic1": "string",
    "topic2": "string",
    "topic3": "string",
    "decoded_from": "string",
    "decoded_to": "string",
    "decoded_amount": "float64",
    "block_timestamp": "int64"
})