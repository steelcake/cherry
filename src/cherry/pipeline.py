import asyncio
from collections.abc import Awaitable
import logging
from .config import (
    CastConfig,
    EvmDecodeEventsConfig,
    EvmValidateBlockDataConfig,
    HexEncodeConfig,
    Pipeline,
    Step,
    StepKind,
    Config,
)
from typing import Dict, List, Callable
from cherry_core.ingest import start_stream
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def

logger = logging.getLogger(__name__)


class Context:
    def __init__(self):
        self.steps = {}

    def add_step(
        self,
        kind: str,
        step: Callable[
            [Dict[str, pa.RecordBatch], Step], Awaitable[Dict[str, pa.RecordBatch]]
        ],
    ):
        self.steps[kind] = step


async def run_pipelines(config: Config, context: Context):
    tasks = {
        name: asyncio.create_task(run_pipeline(pipeline, context, name))
        for name, pipeline in config.pipelines.items()
    }

    for name, task in tasks.items():
        logger.debug(f"running task with name: {name}")
        await task


async def process_steps(
    record_batches: Dict[str, pa.RecordBatch],
    steps: List[Step],
    context: Context,
) -> Dict[str, pa.RecordBatch]:
    logger.debug(f"Processing pipeline steps: {[step.kind for step in steps]}")
    logger.debug(f"Available custom step handlers: {list(context.steps.keys())}")

    res = record_batches

    for step in steps:
        logger.debug(f"running step kind: {step.kind} name: {step.name}")

        if step.kind == StepKind.EVM_VALIDATE_BLOCK_DATA:
            assert isinstance(step.config, EvmValidateBlockDataConfig)
            res = step_def.evm_validate_block_data.execute(res, step.config)
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            assert isinstance(step.config, EvmDecodeEventsConfig)
            res = step_def.evm_decode_events.execute(res, step.config)
        elif step.kind == StepKind.CAST:
            assert isinstance(step.config, CastConfig)
            res = step_def.cast.execute(res, step.config)
        elif step.kind == StepKind.HEX_ENCODE:
            assert isinstance(step.config, HexEncodeConfig)
            res = step_def.hex_encode.execute(res, step.config)
        elif step.kind in context.steps:
            logger.info(f"Executing custom step: {step.kind} {res}")
            res = await context.steps[step.kind](res, step)
        else:
            logger.warning(f"Unknown step kind: {step.kind}")

        logger.debug(f"Step {step.kind} complete. Current tables: {list(res.keys())}")

    return res


async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    logger.info(f"Running pipeline: {pipeline_name}")

    stream = start_stream(pipeline.provider.config)

    writer = create_writer(pipeline.writer)

    while True:
        batch = await stream.next()
        if batch is None:
            break

        logger.debug(
            f"Raw data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in batch.items()]}"
        )

        processed = await process_steps(batch, pipeline.steps, context)

        logger.debug(
            f"Processed data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in processed.items()]}"
        )

        await writer.push_data(processed)
