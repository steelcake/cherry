import asyncio
import logging
from .config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    CustomStepConfig,
    EvmDecodeEventsConfig,
    EvmValidateBlockDataConfig,
    HexEncodeConfig,
    Pipeline,
    Step,
    StepKind,
    U256ToBinaryConfig,
    SvmDecodeInstructionsConfig,
    SvmDecodeLogsConfig,
    JoinBlockDataConfig,
    JoinSvmTransactionDataConfig,
    JoinEvmTransactionDataConfig,
    GlaciersEventsConfig,
)
from typing import Dict, List, Optional
from cherry_core.ingest import start_stream
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def
from . import utils
from copy import deepcopy

logger = logging.getLogger(__name__)


async def process_steps(
    data: Dict[str, pa.Table],
    steps: List[Step],
) -> Dict[str, pa.Table]:
    logger.debug(
        f"Processing pipeline steps: {[[step.kind, step.name] for step in steps]}"
    )

    data = deepcopy(data)

    for step in steps:
        logger.debug(f"running step kind: {step.kind} name: {step.name}")

        if step.kind == StepKind.EVM_VALIDATE_BLOCK_DATA:
            assert isinstance(step.config, EvmValidateBlockDataConfig)
            data = step_def.evm_validate_block_data.execute(data, step.config)
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            assert isinstance(step.config, EvmDecodeEventsConfig)
            data = step_def.evm_decode_events.execute(data, step.config)
        elif step.kind == StepKind.GLACIERS_EVENTS:
            assert isinstance(step.config, GlaciersEventsConfig)
            data = step_def.glaciers_events.execute(data, step.config)
        elif step.kind == StepKind.SVM_DECODE_INSTRUCTIONS:
            assert isinstance(step.config, SvmDecodeInstructionsConfig)
            data = step_def.svm_decode_instructions.execute(data, step.config)
        elif step.kind == StepKind.SVM_DECODE_LOGS:
            assert isinstance(step.config, SvmDecodeLogsConfig)
            data = step_def.svm_decode_logs.execute(data, step.config)
        elif step.kind == StepKind.CAST:
            assert isinstance(step.config, CastConfig)
            data = step_def.cast.execute(data, step.config)
        elif step.kind == StepKind.HEX_ENCODE:
            assert isinstance(step.config, HexEncodeConfig)
            data = step_def.hex_encode.execute(data, step.config)
        elif step.kind == StepKind.U256_TO_BINARY:
            assert isinstance(step.config, U256ToBinaryConfig)
            data = step_def.u256_to_binary.execute(data, step.config)
        elif step.kind == StepKind.BASE58_ENCODE:
            assert isinstance(step.config, Base58EncodeConfig)
            data = await asyncio.to_thread(
                step_def.base58_encode.execute, data, step.config
            )
        elif step.kind == StepKind.CAST_BY_TYPE:
            assert isinstance(step.config, CastByTypeConfig)
            data = step_def.cast_by_type.execute(data, step.config)
        elif step.kind == StepKind.CUSTOM:
            assert isinstance(step.config, CustomStepConfig)
            pl_data = utils.pyarrow_data_to_pl(data)
            pl_data = await asyncio.to_thread(
                step.config.runner, pl_data, step.config.context
            )
            data = utils.pl_data_to_pyarrow(pl_data)
        elif step.kind == StepKind.JOIN_BLOCK_DATA:
            assert isinstance(step.config, JoinBlockDataConfig)
            data = step_def.join_block_data.execute(data, step.config)
        elif step.kind == StepKind.JOIN_SVM_TRANSACTION_DATA:
            assert isinstance(step.config, JoinSvmTransactionDataConfig)
            data = step_def.join_svm_transaction_data.execute(data, step.config)
        elif step.kind == StepKind.JOIN_EVM_TRANSACTION_DATA:
            assert isinstance(step.config, JoinEvmTransactionDataConfig)
            data = step_def.join_evm_transaction_data.execute(data, step.config)
        else:
            raise Exception(f"Unknown step kind: {step.kind}")

    return data


async def run_pipeline(pipeline: Pipeline, pipeline_name: Optional[str] = None):
    logger.info(f"Running pipeline: {pipeline_name}")

    stream = start_stream(pipeline.provider, pipeline.query)

    writer = create_writer(pipeline.writer)

    while True:
        logger.info("Waiting for data")
        data = await stream.next()
        if data is None:
            break

        tables = {}

        for table_name, table_batch in data.items():
            tables[table_name] = pa.Table.from_batches([table_batch])

        processed = await process_steps(tables, pipeline.steps)

        await writer.push_data(processed)
