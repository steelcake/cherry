import asyncio
import logging
from ..config.parser import parse_config, Pipeline, Provider, Step, StepKind, Config
from typing import Dict, Union, List
import copy
from cherry_core.ingest import (
    start_stream,
    StreamConfig,
    ProviderConfig as CoreProviderConfig,
    EvmQuery,
)
from cherry_core import evm_validate_block_data, evm_decode_events
import dacite
import pyarrow as pa
from ..writers.writer import create_writer

logger = logging.getLogger(__name__)

class Context:
    def __init__(self):
        self.steps = {}
        self.from_block = {}
    
    def add_step(self, kind: str, step: callable):
        self.steps[kind] = step

async def run_pipelines(config: Union[str, Config], context: Context):
    if isinstance(config, str):
        config = parse_config(config)

    tasks = {
        name: asyncio.create_task(
            run_pipeline(pipeline, context, name)
        )
        for name, pipeline in config.pipelines.items()
    }
    
    for name, task in tasks.items():
        try:
            await task
        except Exception as e:
            logger.error(f"Failed to run pipeline {name}: {str(e)}")
            raise Exception(f"Error running pipeline {name}: {e}")
        
def provider_to_stream_config(provider_config: CoreProviderConfig) -> StreamConfig:

    logger.info(f"Provider config: {provider_config}")
    
    core_provider_config = CoreProviderConfig(
        url=provider_config.url,
        kind=provider_config.kind,
        max_num_retries=provider_config.max_num_retries,
        retry_backoff_ms=provider_config.retry_backoff_ms,
        retry_base_ms=provider_config.retry_base_ms,
        retry_ceiling_ms=provider_config.retry_ceiling_ms,
        http_req_timeout_millis=provider_config.http_req_timeout_millis
    )

    return StreamConfig(
        format=provider_config.format,
        query=provider_config.query,
        provider=core_provider_config
    )

async def process_steps(
    record_batches: Dict[str, pa.RecordBatch],
    steps: List[Step],
    context: Context
) -> Dict[str, pa.RecordBatch]:
    """
    Process a series of data transformation steps on record batches.
    
    Args:
        record_batches: Dictionary mapping table names to PyArrow RecordBatches
        steps: List of Step objects defining the transformations
        context: Context object containing custom step implementations
        
    Returns:
        Processed record batches after applying all transformation steps
    """
    # Log step processing information
    logger.debug(f"Processing pipeline steps: {[step.kind for step in steps]}")
    logger.debug(f"Available custom step handlers: {list(context.steps.keys())}")
    
    # Initialize result with input batches
    res = record_batches
    
    # Process each step sequentially
    for step in steps:
        # Create deep copy to avoid modifying original data
        res = copy.deepcopy(res)
        
        # Handle EVM block validation
        if step.kind == StepKind.EVM_VALIDATE_BLOCK:
            logger.debug("Validating EVM block data...")
            evm_validate_block_data(
                blocks=res["blocks"],
                transactions=res["transactions"], 
                logs=res["logs"],
                traces=res["traces"]
            )
            
        # Handle EVM event decoding
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            logger.debug(f"Decoding EVM events with config: {step.config}")
            res[step.config['output_table']] = evm_decode_events(
                step.config['event_signature'],
                res[step.config['input_table']],
                step.config['allow_decode_fail']
            )
            
        # Handle custom step processing
        elif step.kind in context.steps:
            logger.debug(f"Executing custom step: {step.kind}")
            res = context.steps[step.kind](res, step)
        else:
            logger.warning(f"Unknown step kind: {step.kind}")
            
        # Log intermediate results
        logger.debug(f"Step {step.kind} complete. Current tables: {list(res.keys())}")
            
    return res

async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    """Run pipeline"""
    logger.info(f"Running pipeline: {pipeline_name}")

    # 2. Set up stream
    stream_config = provider_to_stream_config(pipeline.provider.config)
   
    logger.info(f"Stream config: {stream_config}")
    
    stream = start_stream(stream_config)
    
    # 3. Stream processing
    writer = create_writer(pipeline.writer)
    while True:
        batch = await stream.next()
        if batch is None:
            break
            
        logger.info(f"Raw data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in batch.items()]}")
        processed = await process_steps(batch, pipeline.steps, context)
        logger.info(f"Processed data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in processed.items()]}")
        await writer.push_data(processed)

