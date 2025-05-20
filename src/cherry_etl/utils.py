import pyarrow as pa
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
