import asyncio, logging, sys
from pathlib import Path
from typing import Dict, Any
# Add project root to Python path
root_dir = str(Path(__file__).parent.parent.parent)
sys.path.insert(0, root_dir)

import pyarrow as pa
from typing import Dict
from src.utils.logging_setup import setup_logging
from src.utils.pipeline import run_pipelines, Context

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def get_block_number_stats(data: Dict[str, pa.RecordBatch], step_config: Dict[str, Any]) -> Dict[str, pa.RecordBatch]:
    """Custom processing step for transfer events"""
    try:
        # Get the logs
        logs = data.get(step_config['input_table'])

        # Get block number column as numpy array
        block_number_array = logs.column('block_number').to_numpy()

        # Create a new RecordBatch for block number stats
        stats_schema = pa.schema([
            ('min_block', pa.int64()),
            ('max_block', pa.int64()),
            ('mean_block', pa.float64())
        ])
        
        stats_data = [
            pa.array([int(block_number_array.min())]),
            pa.array([int(block_number_array.max())]),
            pa.array([float(block_number_array.mean())])
        ]
        
        data[step_config['output_table']] = pa.RecordBatch.from_arrays(stats_data, schema=stats_schema)
        
        return data

    except Exception as e:
        logger.error(f"Error processing logs: {e}", exc_info=True)
        raise

async def main():
    """Example of using custom processing steps"""
    try:
        # Create context
        context = Context()

        # Add custom processing step using the enum value
        context.add_step('get_block_number_stats', get_block_number_stats)

        # Run pipelines with custom context
        await run_pipelines(path="./examples/get_block_number_stats/config.yaml", context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())