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
    print(data.keys())
    table_names = data.keys() if config.tables is None else config.tables

    blocks_df: DataFrame = pl.DataFrame(pl.from_arrow(data["blocks"]))

    # Validate join columns exist
    if config.join_blocks_on not in blocks_df.columns:
        raise ValueError(
            f"Join column '{config.join_blocks_on}' not found in blocks table"
        )

    joined_data = {}
    for table_name in table_names:
        if table_name == "blocks":
            continue
        table = data[table_name]
        table_df: DataFrame = pl.DataFrame(pl.from_arrow(table))

        # Validate join column exists in current table
        if config.join_left_on not in table_df.columns:
            raise ValueError(
                f"Join column '{config.join_left_on}' not found in table '{table_name}'"
            )

        joined_df: DataFrame = table_df.join(
            blocks_df,
            left_on=config.join_left_on,
            right_on=config.join_blocks_on,
            how="left",
        )
        joined_data[table_name] = joined_df

    data = utils.pl_data_to_pyarrow(joined_data)

    return data
