from typing import Dict
from copy import deepcopy

from cherry_core import evm_decode_events, evm_event_signature_to_arrow_schema
from ..config import EvmDecodeEventsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: EvmDecodeEventsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.input_table]
    input_batches = input_table.to_batches()

    output_batches = []

    for batch in input_batches:
        output_batches.append(
            evm_decode_events(config.event_signature, batch, config.allow_decode_fail)
        )

    output_table = pa.Table.from_batches(
        output_batches,
        schema=evm_event_signature_to_arrow_schema(config.event_signature),
    )

    if config.hstack:
        for i, col in enumerate(input_table.columns):
            output_table = output_table.append_column(input_table.field(i), col)

    data[config.output_table] = output_table

    return data
