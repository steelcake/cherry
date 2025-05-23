import asyncio
import logging

from .config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    PolarsStepConfig,
    DataFusionStepConfig,
    EvmDecodeEventsConfig,
    EvmValidateBlockDataConfig,
    HexEncodeConfig,
    Pipeline,
    SetChainIdConfig,
    Step,
    StepKind,
    U256ToBinaryConfig,
    SvmDecodeInstructionsConfig,
    SvmDecodeLogsConfig,
    GlaciersEventsConfig,
)
from typing import Dict, List, Optional
from cherry_core.ingest import start_stream
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def
from copy import deepcopy

logger = logging.getLogger(__name__)


def process_steps(
    data: Dict[str, pa.Table],
    steps: List[Step],
) -> Dict[str, pa.Table]:
    logger.debug("Processing pipeline steps")

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
            data = step_def.base58_encode.execute(data, step.config)
        elif step.kind == StepKind.CAST_BY_TYPE:
            assert isinstance(step.config, CastByTypeConfig)
            data = step_def.cast_by_type.execute(data, step.config)
        elif step.kind == StepKind.DATAFUSION:
            assert isinstance(step.config, DataFusionStepConfig)
            data = step_def.datafusion_step.execute(data, step.config)
        elif step.kind == StepKind.POLARS:
            assert isinstance(step.config, PolarsStepConfig)
            data = step_def.polars_step.execute(data, step.config)
        elif step.kind == StepKind.SET_CHAIN_ID:
            assert isinstance(step.config, SetChainIdConfig)
            data = step_def.set_chain_id.execute(data, step.config)
        else:
            raise Exception(f"Unknown step kind: {step.kind}")

    return data


def merge_data(data: list[Dict[str, pa.Table]]) -> Dict[str, pa.Table]:
    keys = list(data[0].keys())
    keys.sort()

    for i in range(1, len(data)):
        other_keys = list(data[i].keys())
        other_keys.sort()

        assert keys == other_keys

    out = {}

    for key in keys:
        tables = []
        for d in data:
            tables.append(d[key])

        out[key] = pa.concat_tables(tables)

    return out


async def run_pipeline(pipeline: Pipeline, pipeline_name: Optional[str] = None):
    logger.info(f"Running pipeline: {pipeline_name}")
    logger.debug(f"Pipeline config: {pipeline}")

    stream = start_stream(pipeline.provider, pipeline.query)

    writer = create_writer(pipeline.writer)

    while True:
        data = await stream.next()
        if data is None:
            break

        logger.debug("Received data from ingest")

        tables = {}

        for table_name, table_batch in data.items():
            tables[table_name] = pa.Table.from_batches([table_batch])

        processed = await asyncio.to_thread(process_steps, tables, pipeline.steps)

        logger.debug("Pushing data to writer")

        await writer.push_data(processed)


__all__ = ["run_pipeline"]
