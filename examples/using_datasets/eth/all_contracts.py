# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/eth/all_contracts.py --provider hypersync --from_block 20000000 --to_block 20000100

# After run, you can see the result in the database:
# duckdb data/all_contracts.db
# SELECT * FROM contracts LIMIT 3;

import argparse
import asyncio
import logging
import os
from typing import Optional

import duckdb
from cherry_core import ingest
from dotenv import load_dotenv
from pathlib import Path

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.all_contracts")

# Create directories
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

    # Create the pipeline using the all_contracts dataset
    pipeline = datasets.evm.make_all_contracts_pipeline(
        provider, writer, from_block, to_block
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="all_contracts", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    connection = duckdb.connect("data/all_contracts.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql("SELECT * FROM contracts")
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="All contracts ever created")
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
