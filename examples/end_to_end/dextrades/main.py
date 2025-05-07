"""DEX trades data processing example using DuckDB for storage"""

import argparse
import asyncio
import os
from pathlib import Path
from typing import Optional

import polars as pl
from cherry_core import ingest
from cherry_etl import config as cc
import duckdb
from dotenv import load_dotenv
from pipeline import create_pipeline
from protocols import UniswapV2

from cherry_etl import run_pipeline

# Configuration
TABLE_NAME = "dex_trades"
SCRIPT_DIR = Path(__file__).parent
DATA_PATH = str(Path.cwd() / "data")
DEFAULT_FROM_BLOCK = 18000000
DEFAULT_TO_BLOCK = DEFAULT_FROM_BLOCK + 100


load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

pl.Config.set_tbl_cols(-1)

# Create directories
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# Provider URLs from environment variables
HYPERSYNC_PROVIDER_URL = os.getenv("HYPERSYNC_PROVIDER_URL")
SQD_PROVIDER_URL = os.getenv("SQD_PROVIDER_URL")
RPC_URL = os.getenv("RPC_URL")


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Ensure provider_url is not None
    if provider_url is None:
        raise ValueError("Provider URL cannot be None")

    connection = duckdb.connect("data/dex_trades.db")
    try:
        result = connection.sql("SELECT MAX(block_number) FROM dex_trades").fetchone()
        last_block_number = result[0] if result is not None else None
        print(f"Last run block number: {last_block_number}")
    except duckdb.Error:
        last_block_number = None
    start_block = (
        last_block_number
        if (last_block_number is not None and last_block_number > from_block)
        else from_block
    )
    end_block = to_block if to_block is not None else from_block + 100

    if start_block >= end_block:
        print("No new blocks to process")
        return

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # Setup the protocol handler
    dex = UniswapV2()

    # Create and run the pipeline
    pipeline = create_pipeline(
        dex=dex,
        provider_kind=provider_kind,
        provider_url=provider_url,
        from_block=start_block,
        to_block=end_block,
        writer=writer,
        table_name=TABLE_NAME,
    )

    await run_pipeline(pipeline_name=TABLE_NAME, pipeline=pipeline)

    data = connection.sql("SELECT * FROM dex_trades LIMIT 3")
    print(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dex trades")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        default="hypersync",
        help="Specify the provider ('sqd' or 'hypersync', default: hypersync)",
    )
    parser.add_argument(
        "--from_block",
        type=int,
        default=DEFAULT_FROM_BLOCK,
        help=f"Specify the block to start from (default: {DEFAULT_FROM_BLOCK})",
    )
    parser.add_argument(
        "--to_block",
        type=int,
        default=DEFAULT_TO_BLOCK,
        help=f"Specify the block to stop at, inclusive (default: {DEFAULT_TO_BLOCK})",
    )

    args = parser.parse_args()

    # Initialize provider_kind at the top level to avoid the unbound error
    provider_kind = ingest.ProviderKind.HYPERSYNC  # Default value
    url = None

    if args.provider == "hypersync":
        provider_kind = ingest.ProviderKind.HYPERSYNC
        url = HYPERSYNC_PROVIDER_URL
    elif args.provider == "sqd":
        provider_kind = ingest.ProviderKind.SQD
        url = SQD_PROVIDER_URL

    asyncio.run(main(provider_kind, url, args.from_block, args.to_block))
