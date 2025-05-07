# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/eth/address_appearances.py --provider sqd --from_block 20000000 --to_block 20000010

# After run, you can see the result in the database:
# duckdb data/address_appearances.db
# SELECT * FROM address_appearances LIMIT 3;


from cherry_etl import config as cc
from cherry_etl import run_pipeline
from cherry_etl.datasets.evm import make_address_appearances_pipeline
from cherry_core import ingest
import logging
import os
import asyncio
from dotenv import load_dotenv
from typing import Optional
from pathlib import Path
import argparse
import duckdb

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


# create and run the pipeline to sync the data
async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Ensure to_block is not None, use from_block + 10 as default if it is
    actual_to_block = to_block if to_block is not None else from_block + 10

    logger.info(
        f"starting to ingest from block {from_block} to block {actual_to_block}"
    )

    # The provider we want to use is selected like this, only need to change these two
    #  parameters to switch to another provider and the pipeline work exactly the same
    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=provider_url,
    )

    # configure a very simple duckdb writer
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # create the pipeline using dataset
    pipeline = make_address_appearances_pipeline(
        provider, writer, from_block, actual_to_block
    )

    # finally run the pipeline
    await run_pipeline(pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # create an in-memory duckdb database
    connection = duckdb.connect("data/address_appearances.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # read result to show
    data = connection.sql(
        "SELECT address, COUNT(*) as appearances FROM address_appearances GROUP BY address ORDER BY appearances DESC LIMIT 20"
    )
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Address appearances tracker")
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

    url = None
    if args.provider == ingest.ProviderKind.HYPERSYNC:
        url = "https://eth.hypersync.xyz"
    elif args.provider == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/ethereum-mainnet"

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, url, from_block, to_block))
