import pyarrow as pa
import polars as pl
from typing import Dict


def pyarrow_data_to_pl(data: Dict[str, pa.Table]) -> Dict[str, pl.DataFrame]:
    """Convert a dictionary of pyarrow Tables to polars DataFrames."""
    return {k: pl.from_arrow(v) for k, v in data.items()}


def pl_data_to_pyarrow(data: Dict[str, pl.DataFrame]) -> Dict[str, pa.Table]:
    """Convert a dictionary of polars DataFrames to pyarrow Tables."""
    return {k: v.to_arrow() for k, v in data.items()}


def pyarrow_large_binary_to_binary(table: pa.Table) -> pa.Table:
    """Convert large binary columns to binary columns in a pyarrow Table."""
    schema = table.schema
    new_fields = []
    for field in schema:
        if field.type == pa.large_binary():
            new_fields.append(pa.field(field.name, pa.binary()))
        else:
            new_fields.append(field)
    new_schema = pa.schema(new_fields)
    return table.cast(new_schema) 