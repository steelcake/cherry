from typing import Dict
from copy import deepcopy

import pyarrow as pa
from cherry_core import cast, cast_schema
from ..config import CastConfig


def execute(data: Dict[str, pa.Table], config: CastConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.table_name]
    batches = input_table.to_batches()
    out_batches = []

    for batch in batches:
        out_batches.append(
            cast(config.mappings, batch, allow_cast_fail=config.allow_cast_fail)
        )

    data[config.table_name] = pa.Table.from_batches(
        out_batches, schema=cast_schema(config.mappings, input_table.schema)
    )

    return data
