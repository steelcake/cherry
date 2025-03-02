from typing import Dict
from copy import deepcopy

from pyarrow import RecordBatch
from cherry_core import cast
from ..config import CastConfig


def execute(data: Dict[str, RecordBatch], config: CastConfig) -> Dict[str, RecordBatch]:
    data = deepcopy(data)

    data[config.table_name] = cast(
        config.mappings, data[config.table_name], allow_cast_fail=config.allow_cast_fail
    )

    return data
