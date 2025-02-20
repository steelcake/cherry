import asyncio, logging, sys
from pathlib import Path
from typing import Dict, Any
import pyarrow as pa
from typing import Dict
from cherry_indexer.utils.logging_setup import setup_logging
from cherry_indexer.utils.pipeline import run_pipelines, Context
from cherry_indexer.config.parser import Pipeline
import awswrangler as wr

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def get_block_number_stats(data: Dict[str, pa.RecordBatch], step_config: Dict[str, Any]) -> Dict[str, pa.RecordBatch]:
    return data

async def block_number_fn(pipeline_name: str, pipeline: Pipeline) -> int:
    table = pipeline.writer.config.anchor_table
    database = pipeline.writer.config.database
    table_exists = wr.catalog.does_table_exist(database=database, table=table)
    print("inside athena fetch")
    if table_exists:
        query = f"SELECT COALESCE(MAX(block_number), 0) as max_block FROM {database}.{table}"
        last_data_inserted = wr.athena.read_sql_query(
            query, database=database, ctas_approach=False
        )
        return last_data_inserted.iloc[0]["max_block"]

    else:
        print("table does not exist")
        return 0

async def main():
    """Example of using custom processing steps"""
    try:
        # Create context
        context = Context()

        # Add custom processing step using the enum value
        context.add_step('get_block_number_stats', get_block_number_stats)

        # Run pipelines with custom context
        await run_pipelines(path="./config.yaml", context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
