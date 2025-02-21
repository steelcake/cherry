import asyncio, logging
import pyarrow as pa
from typing import Dict
from cherry_indexer.config.parser import Step
from cherry_indexer.utils.logging_setup import setup_logging
from cherry_indexer.utils.pipeline import run_pipelines, Context
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
            
        # Get block numbers, filtering out null values
        block_number_column = data[input_table].column("block_number")
        block_number_array = block_number_column.filter(block_number_column.is_valid()).to_numpy()
        
        if len(block_number_array) == 0:
            logger.warning("No valid block numbers found in data")
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
        # Create context
        context = Context()

        # Add custom processing step using the enum value
        context.add_step('get_block_number_stats', get_block_number_stats)

        # Run pipelines with custom context
        await run_pipelines(config="./config.yaml", context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())