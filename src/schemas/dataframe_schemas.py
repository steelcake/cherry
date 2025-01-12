import polars as pl

# Schema for blocks DataFrame
BLOCKS_SCHEMA = {
    "block_hash": pl.Utf8,
    "author": pl.Utf8,
    "block_number": pl.Int64,
    "gas_used": pl.Int64,
    "extra_data": pl.Utf8,
    "timestamp": pl.Int64,
    "base_fee_per_gas": pl.Int64,
    "chain_id": pl.Int64
}

# Schema for transactions DataFrame
TRANSACTIONS_SCHEMA = {
    "transaction_hash": pl.Utf8,
    "block_number": pl.Int64,
    "from_address": pl.Utf8,
    "to_address": pl.Utf8,
    "value": pl.Int64,
    "event_name": pl.Utf8,
    "contract_address": pl.Utf8,
    "topic0": pl.Utf8,
    "raw_data": pl.Utf8
}

# Schema for events DataFrame
EVENTS_SCHEMA = {
    "transaction_hash": pl.Utf8,
    "block_number": pl.Int64,
    "from_address": pl.Utf8,
    "to_address": pl.Utf8,
    "value": pl.Int64,
    "event_name": pl.Utf8,
    "contract_address": pl.Utf8,
    "topic0": pl.Utf8,
    "raw_data": pl.Utf8
} 