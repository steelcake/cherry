import asyncio, logging
from cherry_indexer.utils.logging_setup import setup_logging
from cherry_indexer.utils.pipeline import run_pipelines, Context
from cherry_indexer.config.parser import Pipeline
import duckdb

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

# Pre-stream steps
def get_last_block_number_fn(pipeline: Pipeline) -> int:
    """Get last block number before starting stream"""
    try:
        table = pipeline.writer.config.anchor_table
        s3_path = pipeline.writer.config.s3_path
        endpoint = pipeline.writer.config.endpoint
        
        # Connect to DuckDB
        con = duckdb.connect()

        logger.info(f"Getting last block number for {table} from {s3_path}")
        
        try:
            # Set up S3 connection in DuckDB
            con.execute("""
                INSTALL httpfs;
                LOAD httpfs;
            """)
            
            # Fix endpoint URL format
            endpoint_without_protocol = endpoint.replace('http://', '').replace('https://', '')
            
            con.execute(f"""
                SET s3_endpoint='{endpoint_without_protocol}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
                SET s3_access_key_id='minioadmin';
                SET s3_secret_access_key='minioadmin';
            """)
            
            # Query the parquet files in MinIO
            try:
                query = f"""
                    SELECT COALESCE(MAX(number), 0) as max_block 
                    FROM read_parquet('{s3_path}/{table}/*.parquet')
                """
                result = con.execute(query).fetchone()
                last_block = result[0] if result else 0
                logger.info(f"Last block number: {last_block}")
            except Exception as e:
                logger.warning(f"Could not read existing blocks, starting from 0: {e}")
                last_block = 0
            
            # Update pipeline config
            pipeline.provider.config.query['from_block'] = last_block
            
            # Return empty record batch dictionary to maintain pipeline flow
            return pipeline.provider.config.query['from_block']
            
        except Exception as e:
            logger.error(f"Error querying MinIO/DuckDB: {e}")
            pipeline.provider.config.query['from_block'] = 0
            return {}
        finally:
            con.close()
            
    except Exception as e:
        logger.error(f"Error in get_last_block_number: {e}")
        return {}

async def main():
    """Example of using custom processing steps"""
    try:
        # Create context
        context = Context()

        # Add custom processing step using the enum value
        context.add_step('get_last_block_number', get_last_block_number_fn)

        # Run pipelines with custom context
        await run_pipelines(path="./config.yaml", context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
