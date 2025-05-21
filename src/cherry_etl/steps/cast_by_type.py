from typing import Dict

import pyarrow as pa
from ..config import CastByTypeConfig
from ..utils import cast_table_by_type


def execute(data: Dict[str, pa.Table], config: CastByTypeConfig) -> Dict[str, pa.Table]:
    for table_name, table_data in data.items():
        data[table_name] = cast_table_by_type(table_data, config)

    return data
