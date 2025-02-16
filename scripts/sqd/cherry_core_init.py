from .. import cherry_core as cc
from typing import Tuple
import pyarrow
from .. import ingest

def cast(map: list[Tuple[str, str]], data: pyarrow.RecordBatch, allow_cast_fail: bool = False) -> pyarrow.RecordBatch:
    return cc.cast(map, data, allow_cast_fail)

def cast_schema(map: list[Tuple[str, str]], schema: pyarrow.Schema) -> pyarrow.Schema:
    return cc.cast_schema(map, schema)

def hex_encode(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.hex_encode(data)

def prefix_hex_encode(data: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
    return cc.prefix_hex_encode(data)

def hex_encode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.hex_encode_column(col)

def prefix_hex_encode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.prefix_hex_encode_column(col)

def hex_decode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.hex_decode_column(col)

def prefix_hex_decode_column(col: pyarrow.Array) -> pyarrow.Array:
    return cc.prefix_hex_decode_column(col)

def u256_column_from_binary(col: pyarrow.Array) -> pyarrow.Array:
    return cc.u256_column_from_binary(col)

def u256_column_to_binary(col: pyarrow.Array) -> pyarrow.Array:
    return cc.u256_column_to_binary(col)

def evm_decode_call_inputs(signature: str, data: pyarrow.Array, allow_decode_fail: bool = False) -> pyarrow.RecordBatch:
    return cc.evm_decode_call_inputs(signature, data, allow_decode_fail)

def evm_decode_call_outputs(signature: str, data: pyarrow.Array, allow_decode_fail: bool = False) -> pyarrow.RecordBatch:
    return cc.evm_decode_call_outputs(signature, data, allow_decode_fail)

def evm_decode_events(signature: str, data: pyarrow.RecordBatch, allow_decode_fail: bool = False) -> pyarrow.RecordBatch:
    return cc.evm_decode_events(signature, data, allow_decode_fail)

def evm_event_signature_to_arrow_schema(signature: str) -> pyarrow.Schema:
    return cc.evm_event_signature_to_arrow_schema(signature)

def evm_transaction_signature_to_arrow_schemas(signature: str) -> Tuple[pyarrow.Schema, pyarrow.Schema]:
    return cc.evm_transaction_signature_to_arrow_schemas(signature)

def evm_validate_block_data(blocks: pyarrow.RecordBatch, transactions: pyarrow.RecordBatch, logs: pyarrow.RecordBatch, traces: pyarrow.RecordBatch):
    return cc.evm_validate_block_data(blocks, transactions, logs, traces)

def evm_signature_to_topic0(signature: str) -> str:
    return cc.evm_signature_to_topic0(signature)
