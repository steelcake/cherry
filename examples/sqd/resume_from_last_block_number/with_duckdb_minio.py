import asyncio, logging
from cherry_indexer.utils.logging_setup import setup_logging
from cherry_indexer.utils.pipeline import run_pipelines, Context
from cherry_indexer.config.parser import (
    Config, Provider, Writer, Pipeline, Step,
    ProviderConfig, WriterConfig, WriterKind, 
    ProviderKind, Format
)
import duckdb

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

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
                query={
                    "from_block": 0,
                    "logs": [{
                        "address": ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
                        "event_signatures": ["Transfer(address,address,uint256)"]
                    }]
                }
            )
        )
        config.providers["my_provider"] = provider

        # Add writer
        writer = Writer(
            name="my_writer",
            kind=WriterKind.AWS_WRANGLER_S3,
            config=WriterConfig(
                endpoint="http://localhost:9000",
                s3_path="s3://blockchain-data/aws-wrangler-s3",
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
            steps=[]
        )
        config.pipelines["my_pipeline"] = pipeline

        config.pipelines["my_pipeline"].provider.config.query['from_block'] = get_last_block_number_fn(config.pipelines["my_pipeline"])

        context = Context()

        # Run pipelines with custom context
        await run_pipelines(config=config, context=context)

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
