from typing import Dict
from copy import deepcopy

import pyarrow as pa
from cherry_core import cast_by_type, cast_schema_by_type
from ..config import CastByTypeConfig


def execute(data: Dict[str, pa.Table], config: CastByTypeConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    for table_name, table_data in data.items():
        batches = table_data.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(
                cast_by_type(
                    batch, config.from_type, config.to_type, config.allow_cast_fail
                )
            )

        new_schema = cast_schema_by_type(
            table_data.schema, config.from_type, config.to_type
        )
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
