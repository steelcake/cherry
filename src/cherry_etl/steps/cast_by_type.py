from typing import Dict
from copy import deepcopy

import pyarrow as pa
from ..config import CastByTypeConfig


def execute(data: Dict[str, pa.Table], config: CastByTypeConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    for table_name, table_data in data.items():
        arrays = []

        for col in table_data.columns:
            if config.from_type.equals(col.type):
                arrays.append(
                    col.cast(
                        target_type=config.to_type,
                        safe=config.safe,
                        options=config.options,
                    )
                )
            else:
                arrays.append(col)

        data[table_name] = pa.Table.from_arrays(arrays, names=table_data.column_names)

    return data
