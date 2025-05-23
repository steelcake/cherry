from typing import Dict

from ..config import PolarsStepConfig
import pyarrow as pa
import polars as pl


def execute(data: Dict[str, pa.Table], config: PolarsStepConfig) -> Dict[str, pa.Table]:
    pl_data = {}

    for name, table in data.items():
        pl_data[name] = pl.from_arrow(table)

    out = config.runner(pl_data, config.context)

    pa_data = {}

    for name, table in out.items():
        pa_data[name] = table.to_arrow()

    return pa_data
