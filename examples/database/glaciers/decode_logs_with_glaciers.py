import argparse
import asyncio
import logging
import os
import requests

from typing import Optional

import duckdb
from cherry_core import ingest

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.logs")


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
    if to_block is not None:
        logger.info(f"starting to ingest from block {from_block} to block {to_block}")
    else:
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

    # Create the pipeline using the logs dataset
    pipeline = datasets.evm.logs(provider, writer, from_block, to_block)

    # Run the pipeline
    await run_pipeline(pipeline_name="logs", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    url = "https://github.com/yulesa/glaciers/raw/refs/heads/master/ABIs/ethereum__events__abis.parquet"

    if not os.path.exists("examples/database/glaciers/ethereum__events__abis.parquet"):
        response = requests.get(url)
        with open(
            "examples/database/glaciers/ethereum__events__abis.parquet", "wb"
        ) as file:
            file.write(response.content)

    connection = duckdb.connect("examples/database/glaciers/glaciers_decoded_logs.db")

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql(
        "SELECT name, FIRST(event_values), FIRST(event_keys), FIRST(event_json), FIRST(transaction_hash), COUNT(*) AS evt_count FROM decoded_logs GROUP BY name ORDER BY evt_count DESC"
    )
    logger.info(f"\n{data}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Blocks tracker")
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
