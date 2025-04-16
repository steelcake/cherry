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
        new_data[table_name] = pyarrow_large_binary_to_binary(table_data.to_arrow())

    return new_data


def pyarrow_large_binary_to_binary(table: pa.Table) -> pa.Table:
    columns = []
    for column in table.columns:
        if column.type == pa.large_binary():
            columns.append(column.cast(pa.binary(), safe=True))
        elif column.type == pa.large_string():
            columns.append(column.cast(pa.string(), safe=True))
        else:
            columns.append(column)

    return pa.Table.from_arrays(columns, names=table.column_names)
