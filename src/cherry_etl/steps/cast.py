from typing import Dict
from copy import deepcopy

import pyarrow as pa
from cherry_core import cast, cast_schema
from ..config import CastConfig


def execute(data: Dict[str, pa.Table], config: CastConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    mappings = list(config.mappings.items())

    for table_name, table_data in data.items():
        if table_name != config.table_name:
            continue
        batches = table_data.to_batches()
        out_batches = []
        for batch in batches:
            out_batches.append(cast(mappings, batch, config.allow_cast_fail))

        new_schema = cast_schema(
            mappings,
            table_data.schema,
        )
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
