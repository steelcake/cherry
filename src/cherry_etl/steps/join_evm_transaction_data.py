from typing import Dict

from .. import utils
from ..config import JoinEvmTransactionDataConfig, CastByTypeConfig
import pyarrow as pa
import polars as pl
from polars import DataFrame


def execute(
    data: Dict[str, pa.Table], config: JoinEvmTransactionDataConfig
) -> Dict[str, pa.Table]:
    table_names = data.keys() if config.tables is None else config.tables
    cast_by_type_config = CastByTypeConfig(
        from_type=pa.decimal256(76, 0),
        to_type=pa.float64(),
    )
    data["transactions"] = utils.cast_table_by_type(
        data["transactions"], cast_by_type_config
    )
    transactions_df: DataFrame = pl.DataFrame(pl.from_arrow(data["transactions"]))

    missing_columns = [
        col for col in config.join_transactions_on if col not in transactions_df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Join columns {missing_columns} not found in transactions table. Available columns: {transactions_df.columns}"
        )

    joined_data = {}
    for table_name in table_names:
        # Skip tables that don't have connections to transactions
        if table_name in ["blocks", "transactions"]:
            continue
        table = data[table_name]
        table = utils.cast_table_by_type(table, cast_by_type_config)
        table_df: DataFrame = pl.DataFrame(pl.from_arrow(table))

        missing_columns = [
            col for col in config.join_left_on if col not in table_df.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Join columns {missing_columns} not found in table '{table_name}'. Available columns: {table_df.columns}"
            )

        joined_df: DataFrame = table_df.join(
            transactions_df,
            left_on=config.join_left_on,
            right_on=config.join_transactions_on,
            how="left",
        )
        joined_data[table_name] = joined_df

    joined_data = utils.pl_data_to_pyarrow(joined_data)

    data.update(joined_data)

    return data
