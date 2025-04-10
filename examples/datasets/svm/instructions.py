import argparse
import asyncio
import logging
import os
from typing import Optional

import duckdb
from cherry_core import ingest

from cherry_etl import config as cc
from cherry_etl import datasets
from cherry_etl.pipeline import run_pipeline


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.svm.instructions")

async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    if to_block is None:
        logger.info(f"starting to ingest from block {from_block}")
    else:
        logger.info(f"starting to ingest from block {from_block} to {to_block}")


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

    # Create the pipeline using the blocks dataset
    pipeline = datasets.svm.instructions(provider, writer, from_block, to_block)

    # Run the pipeline
    await run_pipeline(pipeline_name="instructions", pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Connect to a persistent database file
    connection = duckdb.connect()

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # Optional: read result to show
    data = connection.sql("SELECT * FROM instructions LIMIT 20")
    data.write_parquet("out.parquet")
    logger.info(f"\n{data}")
    
    # Close the connection properly
    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instructions tracker")
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

    url = "https://portal.sqd.dev/datasets/solana-beta"

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main("sqd", url, from_block, to_block))