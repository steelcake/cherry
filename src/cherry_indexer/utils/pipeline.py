import asyncio
import logging
from ..config.parser import parse_config, Pipeline, Provider, Step, StepKind, StepPhase, Config
from typing import Dict, Union
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
        query=dacite.from_dict(data_class=EvmQuery, data=provider_config.query),
        provider=core_provider_config
    )

async def process_steps(data: Dict[str, pa.RecordBatch], 
                       pipeline: Pipeline, 
                       context: Context,
                       phase: StepPhase) -> Dict[str, pa.RecordBatch]:
    """Process steps based on their phase"""
    logger.debug(f"Processing {phase.value} steps: {[step.kind for step in pipeline.steps if step.phase == phase]}")
    
    res = copy.deepcopy(data)
    
    # Only process steps matching the current phase
    matching_steps = [step for step in pipeline.steps if step.phase == phase]
    
    for step in matching_steps:
        logger.info(f"Processing step: {step.kind} with phase: {step.phase}")
            
        if phase == StepPhase.PRE_STREAM:
            if context.steps.get(step.kind):
                context.steps[step.kind](pipeline)
                
        elif phase == StepPhase.STREAM:
            if step.kind == StepKind.EVM_VALIDATE_BLOCK:
                res = evm_validate_block_data(
                    blocks=res["blocks"], 
                    transactions=res["transactions"], 
                    logs=res["logs"], 
                    traces=res["traces"]
                )
            elif step.kind == StepKind.EVM_DECODE_EVENTS:
                res[step.config['output_table']] = evm_decode_events(
                                                    step.config['event_signature'],
                                                    res[step.config['input_table']], 
                                                    step.config['allow_decode_fail']
                                                    )
            elif context.steps.get(step.kind):
                res = context.steps[step.kind](res, step)
                
        elif phase == StepPhase.POST_STREAM:
            if context.steps.get(step.kind):
                context.steps[step.kind](res, pipeline, step)
            
    return res

async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    """Run a pipeline with different processing phases"""
    logger.info(f"Running pipeline: {pipeline_name}")

     # 1. Pre-stream processing
    await process_steps({}, pipeline, context, StepPhase.PRE_STREAM)

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
        processed = await process_steps(batch, pipeline, context, StepPhase.STREAM)
        logger.info(f"Processed data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in processed.items()]}")
        await writer.push_data(processed)
        
    # 4. Post-stream processing
    await process_steps({}, pipeline, context, StepPhase.POST_STREAM)

