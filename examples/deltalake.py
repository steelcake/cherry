from cherry import config as cc, run_pipelines, Context
from cherry_core import ingest
import logging
import os
import asyncio

logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG").upper())


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

    writer = cc.Writer(
        kind=cc.WriterKind.DELTALAKE,
        config=cc.DeltaLakeWriterConfig(
            base_path="./data"
        ),
    )

    config = cc.Config(
        project_name="my_project",
        description="My description",
        pipelines={
            "my_pipeline": cc.Pipeline(
                provider=provider,
                writer=writer,
                steps=[],
            )
        },
    )

    context = Context()

    await run_pipelines(config, context)

if __name__ == "__main__":
    asyncio.run(main())

