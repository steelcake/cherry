# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/using_datasets/eth/erc20_transfers.py --provider hypersync --from_block 20000000 --to_block 20000020

# After run, you can see the result in the database:
# duckdb data/erc20_transfers.db
# SELECT * FROM erc20_transfers LIMIT 3;
# SELECT * FROM token_metadata LIMIT 3;


import argparse
import asyncio
import logging
import os
from typing import Optional
from pathlib import Path

import duckdb
from cherry_core import ingest
from cherry_core import get_token_metadata_as_table, prefix_hex_encode
from dotenv import load_dotenv

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.erc20_transfers")

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

RPC_URL = "https://ethereum-rpc.publicnode.com"

PROVIDER_URLS = {
    ingest.ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ingest.ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}


async def sync_transfer_data(
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
    pipeline = datasets.evm.make_erc20_transfers_pipeline(
        provider, writer, from_block, to_block
    )

    # Run the pipeline
    await run_pipeline(pipeline_name="erc20_transfers", pipeline=pipeline)


def get_token_metadata(connection: duckdb.DuckDBPyConnection):
    connection.sql(
        "CREATE TABLE IF NOT EXISTS token_metadata (address BLOB, decimals INTEGER, symbol VARCHAR, name VARCHAR, total_supply BLOB);"
    )

    missing_metadata = (
        connection.sql("""
        SELECT DISTINCT erc20
        FROM erc20_transfers
        LEFT JOIN token_metadata ON erc20_transfers.erc20 = token_metadata.address
        WHERE token_metadata.address IS NULL
    """)
        .to_arrow_table()
        .to_batches()
    )

    # Translating address from bytes to hex strings
    missing_metadata = [prefix_hex_encode(batch) for batch in missing_metadata]

    if len(missing_metadata) > 0:
        # converting from Recordbatches to list.
        addresses = []
        for batch in missing_metadata:
            column_array = batch.column("erc20").to_numpy(zero_copy_only=False)
            addresses.extend(column_array.tolist())

        # get_token_metadata_as_table use multicall has a lenght limit of 100_000 bytes. We need to break it into chunks.
        chunk_size = 100
        for i in range(0, len(addresses), chunk_size):
            addresses_chunk = addresses[i : i + chunk_size]

            _new_token_metadata = get_token_metadata_as_table(
                "https://ethereum-rpc.publicnode.com",
                addresses_chunk,
            )

            connection.execute(
                "INSERT INTO token_metadata SELECT * FROM _new_token_metadata"
            )


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    connection = duckdb.connect("data/erc20_transfers.db")

    # sync the data into duckdb
    await sync_transfer_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql("SELECT * FROM erc20_transfers LIMIT 3")
    logger.info(f"\n{data}")

    get_token_metadata(connection.cursor())

    # Optional: read result to show
    data = connection.sql("SELECT * FROM token_metadata LIMIT 3")
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ERC20 transfers")
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
