from typing import Dict
from copy import deepcopy

from pyarrow import RecordBatch
from cherry_core import evm_decode_events
from ..config import EvmDecodeEventsConfig


def execute(
    data: Dict[str, RecordBatch], config: EvmDecodeEventsConfig
) -> Dict[str, RecordBatch]:
    data = deepcopy(data)

    data[config.output_table] = evm_decode_events(
        config.event_signature, data[config.input_table], config.allow_decode_fail
    )

    return data
