# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/eth/logs.py --provider hypersync --from_block 20000000 --to_block 20000100

# After run, you can see the result in the database:
# duckdb data/logs.db
# SELECT * FROM logs LIMIT 3;
# SELECT * FROM decoded_logs LIMIT 3;


import argparse
import asyncio
import logging
import os
from typing import Optional
from pathlib import Path

import duckdb
from cherry_core import ingest
from dotenv import load_dotenv

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.logs")

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

PROVIDER_URLS = {
    ingest.ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ingest.ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}


async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    logger.info(f"starting to ingest from block {from_block}")

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=provider_url,
    )

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    event_full_signature = "PairCreated(address indexed token0, address indexed token1, address pair,uint256)"
    address = ["0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"]
    topic0 = ["0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"]

    # Create the pipeline using the all_contracts dataset
    pipeline = datasets.evm.make_log_pipeline(
        provider=provider,
        writer=writer,
        event_full_signature=event_full_signature,
        from_block=from_block,
        to_block=to_block,
        address=address,
        topic0=topic0,
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="logs", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    connection = duckdb.connect("data/logs.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql("SELECT * FROM decoded_logs LIMIT 3")
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Logs")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )
    parser.add_argument(
        "--from_block",
        required=True,
        help="Specify the block to start from",
    )
    parser.add_argument(
        "--to_block",
        required=False,
        help="Specify the block to stop at, inclusive",
    )

    args = parser.parse_args()

    url = PROVIDER_URLS[args.provider]

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, url, from_block, to_block))
