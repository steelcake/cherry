from typing import Dict
from copy import deepcopy

from pyarrow import RecordBatch
from cherry_core import evm_decode_events
from ..config import EvmDecodeEventsConfig


def execute(
    data: Dict[str, RecordBatch], config: EvmDecodeEventsConfig
) -> Dict[str, RecordBatch]:
    data = deepcopy(data)

    input_table = data[config.input_table]

    output_table = evm_decode_events(
        config.event_signature, input_table, config.allow_decode_fail
    )

    if config.hstack:
        for i, col in enumerate(input_table.columns):
            output_table = output_table.append_column(input_table.field(i), col)

    data[config.output_table] = output_table

    return data
