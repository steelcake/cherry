from typing import Dict
from copy import deepcopy

import pyarrow as pa
from ..config import CastConfig


def execute(data: Dict[str, pa.Table], config: CastConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.table_name]

    arrays = []

    for name in input_table.column_names:
        if name in config.mappings:
            arrays.append(
                input_table.column(name).cast(
                    target_type=config.mappings[name],
                    safe=config.safe,
                    options=config.options,
                )
            )
        else:
            arrays.append(input_table.column(name))

    data[config.table_name] = pa.Table.from_arrays(
        arrays, names=input_table.column_names
    )

    return data
