import pyarrow as pa
from hashlib import sha256
import polars as pl
from typing import Dict


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


# https://github.com/solana-foundation/anchor/blob/master/lang/syn/src/codegen/program/common.rs
def svm_anchor_discriminator(name: str, namespace: str = "global") -> bytes:
    preimage = f"{namespace}:{name}".encode("utf-8")
    hash_bytes = sha256(preimage).digest()[:8]

    return hash_bytes
