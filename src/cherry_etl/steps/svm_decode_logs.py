from typing import Dict
from copy import deepcopy

from cherry_core import svm_decode_logs, instruction_signature_to_arrow_schema
from cherry_core.svm_decode import InstructionSignature
from ..config import SvmDecodeLogsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: SvmDecodeLogsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.input_table]
    input_batches = input_table.to_batches()

    output_batches = []

    for batch in input_batches:
        output_batches.append(
            svm_decode_logs(config.log_signature, batch, config.allow_decode_fail)
        )

    output_table = pa.Table.from_batches(
        output_batches,
        schema=instruction_signature_to_arrow_schema(
            # Using a hack here because ...to_arrow_schema is only implemented for InstructionSignature and not for LogSignature, but the params are the same
            InstructionSignature(
                discriminator="",  # field is not used for schema generation
                params=config.log_signature.params,
                accounts_names=[],  # log signatures don't have accounts separated from params
            ),
        ),
    )

    if config.hstack:
        for i, col in enumerate(input_table.columns):
            output_table = output_table.append_column(input_table.field(i), col)

    data[config.output_table] = output_table

    return data
