from typing import Dict

from ..config import DataFusionStepConfig
import pyarrow as pa
import datafusion


def execute(
    data: Dict[str, pa.Table], config: DataFusionStepConfig
) -> Dict[str, pa.Table]:
    df_data = {}

    ctx = datafusion.SessionContext()

    for name, table in data.items():
        df_data[name] = ctx.create_dataframe(
            name=name,
            partitions=[table.to_batches()],
            schema=table.schema,
        )

    out = config.runner(ctx, df_data, config.context)

    pa_data = {}

    for name, table in out.items():
        pa_data[name] = table.to_arrow_table()

    return pa_data
