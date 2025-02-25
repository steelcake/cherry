from cherry_indexer import config as cc
from cherry_core import ingest
from pyiceberg.catalog.sql import SqlCatalog
import logging, os, asyncio, pyarrow as pa
from cherry_indexer.pipeline import run_pipelines, Context
from typing import Dict

logging.basicConfig(
    level=os.environ.get('LOGLEVEL', 'DEBUG').upper()
)

def prune_fields(data: Dict[str, pa.RecordBatch], step: cc.Step) -> Dict[str, pa.RecordBatch]:
    x = data['blocks'].column('number')
    blocks = pa.RecordBatch.from_arrays([
            x
    ], names=["blocks"])
    return {
        'blocks': blocks 
    }

async def main():
    provider = cc.Provider(
        name="my_provider",
        config=ingest.ProviderConfig(
            kind=ingest.ProviderKind.SQD,
            url="https://portal.sqd.dev/datasets/ethereum-mainnet",
            query=ingest.Query(
                kind=ingest.QueryKind.EVM,
                params=ingest.evm.Query(
                    from_block=0,
                    logs=[
                        ingest.evm.LogRequest(
                            address=["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
                            event_signatures=["Transfer(address,address,uint256)"],
                        )
                    ],
                    fields=ingest.evm.Fields(
                        block=ingest.evm.BlockFields(number=True, timestamp=True),
                        log=ingest.evm.LogFields(
                            address=True,
                            topic0=True,
                            topic1=True,
                            topic2=True,
                            topic3=True,
                            data=True,
                        ),
                    ),
                ),
            ),
        ),
    )

    catalog = SqlCatalog(
        name="cherry",
        uri=f"postgresql+psycopg2://postgres:postgres@localhost/iceberg",
        warehouse="s3://blockchain-data",
        **{"s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        }
                
    )

    writer = cc.Writer(
        kind=cc.WriterKind.ICEBERG,
        config=cc.IcebergWriterConfig(
            namespace="my_namespace",
            database="my_database",
            catalog=catalog,
            write_location="s3://blockchain-data/"
        )
    )

    # Create empty config
    config = cc.Config(
        project_name="my_project",
        description="My description",
        pipelines={
            "my_pipeline": cc.Pipeline(provider=provider, writer=writer, steps=[cc.Step(name="my_prune", kind="prune_fields")])
        },
    )

    context = Context()

    context.add_step("prune_fields", prune_fields)

    await run_pipelines(config, context)


if __name__ == "__main__":
    asyncio.run(main())
