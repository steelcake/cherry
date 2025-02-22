import asyncio, logging
import pyarrow as pa
from typing import Dict
from cherry_indexer.config.parser import (
    Config, Provider, Writer, Pipeline, Step,
    ProviderConfig, WriterConfig, WriterKind, 
    ProviderKind, Format, StepPhase
)
from cherry_indexer.utils.logging_setup import setup_logging
from cherry_indexer.utils.pipeline import run_pipelines, Context
from cherry_core.ingest import EvmQuery, LogRequest
import numpy as np

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def get_block_number_stats(data: Dict[str, pa.RecordBatch], step: Step) -> Dict[str, pa.RecordBatch]:
    """Custom processing step for block number statistics"""
    try:
        input_table = step.config.get("input_table", "logs")
        output_table = step.config.get("output_table", "block_number_stats")
        
        if input_table not in data:
            logger.warning(f"Input table {input_table} not found in data")
            return data
            
        # Get block numbers
        block_number_array = data[input_table].column("block_number").to_numpy()
        
        if len(block_number_array) == 0:
            logger.warning("No block numbers found in data")
            # Create empty stats with default values
            stats_batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([0]),  # min_block
                    pa.array([0]),  # max_block
                    pa.array([0]),  # num_blocks
                    pa.array([0.0])  # avg_block
                ],
                ["min_block", "max_block", "num_blocks", "avg_block"]
            )
        else:
            # Calculate statistics
            stats_batch = pa.RecordBatch.from_arrays(
                [
                    pa.array([int(block_number_array.min())]),
                    pa.array([int(block_number_array.max())]),
                    pa.array([len(block_number_array)]),
                    pa.array([float(block_number_array.mean())])
                ],
                ["min_block", "max_block", "num_blocks", "avg_block"]
            )
        
        # Add stats to output
        data[output_table] = stats_batch
        return data
        
    except Exception as e:
        logger.error(f"Error processing logs: {e}")
        raise
   
   

async def main():
    """Example of using custom processing steps"""
    try:
        # Create empty config
        config = Config(
            project_name="my_project",
            description="My description",
            providers={},
            pipelines={},
            writers={},
        )

        # Add provider
        provider = Provider(
            name="my_provider",
            config=ProviderConfig(
                kind=ProviderKind.SQD,
                format=Format.EVM,
                url="https://portal.sqd.dev/datasets/ethereum-mainnet",
                query=EvmQuery(from_block=0, 
                               logs=
                               [
                    LogRequest(
                        address=["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"], 
                        event_signatures= ["Transfer(address,address,uint256)"])
                ])
            )
        )
        config.providers["my_provider"] = provider

        # Add writer
        writer = Writer(
            name="my_writer",
            kind=WriterKind.ICEBERG_S3,
            config=WriterConfig(
                endpoint="http://localhost:9000",
                s3_path="s3://blockchain-data/iceberg-s3",
                anchor_table="blocks",
                use_boto3=True
            )
        )
        config.writers["my_writer"] = writer

        # Add pipeline
        pipeline = Pipeline(
            name="my_pipeline",
            provider=provider,
            writer=writer,
            steps=[
                Step(
                    name="get_block_number_stats",
                    kind="get_block_number_stats",
                    phase=StepPhase.STREAM,
                    config={
                        "input_table": "logs",
                        "output_table": "block_number_stats",
                    }
                )
            ]
        )
        config.pipelines["my_pipeline"] = pipeline

        # Create context
        context = Context()

        # Add custom processing step using the enum value
        context.add_step('get_block_number_stats', get_block_number_stats)

        # Run pipelines with custom context
        await run_pipelines(config=config, context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
