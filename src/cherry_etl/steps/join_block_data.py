from typing import Dict
from copy import deepcopy

from .. import utils
from ..config import JoinBlockDataConfig
import pyarrow as pa
import polars as pl
from polars import DataFrame


def execute(
    data: Dict[str, pa.Table], config: JoinBlockDataConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)
    table_names = data.keys() if config.tables is None else config.tables

    blocks_df: DataFrame = pl.DataFrame(pl.from_arrow(data["blocks"]))

    missing_columns = [
        col for col in config.join_blocks_on if col not in blocks_df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Join columns {missing_columns} not found in blocks table. Available columns: {blocks_df.columns}"
        )

    joined_data = {}
    for table_name in table_names:
        if table_name == "blocks":
            continue
        table = data[table_name]
        table_df: DataFrame = pl.DataFrame(pl.from_arrow(table))

        missing_columns = [
            col for col in config.join_left_on if col not in table_df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Join columns {missing_columns} not found in table '{table_name}'. Available columns: {table_df.columns}"
            )

        joined_df: DataFrame = table_df.join(
            blocks_df,
            left_on=config.join_left_on,
            right_on=config.join_blocks_on,
            how="left",
        )
        joined_data[table_name] = joined_df

    joined_data = utils.pl_data_to_pyarrow(joined_data)

    data.update(joined_data)

    return data
