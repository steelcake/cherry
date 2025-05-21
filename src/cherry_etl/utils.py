import pyarrow as pa
from hashlib import sha256
import polars as pl
from typing import Dict
from .config import CastByTypeConfig
from cherry_core import cast_by_type, cast_schema_by_type


def pyarrow_data_to_pl(data: Dict[str, pa.Table]) -> Dict[str, pl.DataFrame]:
    new_data = {}

    for table_name, table_data in data.items():
        new_data[table_name] = pl.from_arrow(table_data)

    return new_data


def pl_data_to_pyarrow(data: Dict[str, pl.DataFrame]) -> Dict[str, pa.Table]:
    new_data = {}

    for table_name, table_data in data.items():
        new_data[table_name] = table_data.to_arrow()

    return new_data


def cast_table_by_type(table: pa.Table, config: CastByTypeConfig) -> pa.Table:
    batches = table.to_batches()
    out_batches = []

    for batch in batches:
        out_batches.append(cast_by_type(batch, config.from_type, config.to_type, config.allow_cast_fail))
    
    new_schema = cast_schema_by_type(
            table.schema, config.from_type, config.to_type
        )
    return pa.Table.from_batches(out_batches, schema=new_schema)


# https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/program/common.rs
def svm_anchor_discriminator(name: str, namespace: str = "global") -> bytes:
    preimage = f"{namespace}:{name}".encode("utf-8")
    hash_bytes = sha256(preimage).digest()[:8]

    return hash_bytes
