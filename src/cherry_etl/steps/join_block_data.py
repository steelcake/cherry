from typing import Dict
from copy import deepcopy

from cherry_core import svm_decode_instructions, instruction_signature_to_arrow_schema
from ..config import SvmDecodeInstructionsConfig
import pyarrow as pa
import polars as pl


def execute(
    data: Dict[str, pa.Table], config: SvmDecodeInstructionsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)
    print(data.keys())
    table_names = data.keys() if config.tables is None else config.tables

    blocks_df = pl.from_arrow(data["blocks"])

    for table_name in table_names:
        if table_name == "blocks":
            continue
        table = data[table_name]
        table_df = pl.from_arrow(table)
        joined_df = table_df.join(
            blocks_df,
            left_on=config.join_left_on,
            right_on=config.join_blocks_on,
            how="left",
        )
        data[table_name] = joined_df.to_arrow()

    return data
