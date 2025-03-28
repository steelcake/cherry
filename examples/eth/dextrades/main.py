"""DEX trades data processing example using Delta Lake for storage"""

import argparse
import asyncio
import os
import shutil
from pathlib import Path
from typing import Optional

import cherry_core
import polars as pl
from cherry_core import ingest
from deltalake import DeltaTable
from dotenv import load_dotenv
from pipeline import create_pipeline
from protocols import UniswapV2

from cherry_etl import run_pipeline

# Configuration
TABLE_NAME = "dex_trades"
SCRIPT_DIR = Path(__file__).parent
DATA_PATH = str(Path.cwd() / "data" / "deltalake")
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


async def sync_data(
    delta_path: str,
    provider_kind: ingest.ProviderKind,
    provider_url: str,
    from_block: int,
    to_block: int,
):
    # Setup the protocol handler
    dex = UniswapV2()

    # Define query fields
    request_fields = ingest.evm.Fields(
        block=ingest.evm.BlockFields(number=True, timestamp=True),
        transaction=ingest.evm.TransactionFields(
            block_number=True, transaction_index=True, hash=True, from_=True, to=True
        ),
        log=ingest.evm.LogFields(
            block_number=True,
            transaction_index=True,
            log_index=True,
            address=True,
            data=True,
            topic0=True,
            topic1=True,
            topic2=True,
            topic3=True,
        ),
    )

    # Create provider config and query
    provider = ingest.ProviderConfig(kind=provider_kind, url=provider_url)
    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                ingest.evm.LogRequest(
                    topic0=[cherry_core.evm_signature_to_topic0(dex.event_signature)],
                    include_blocks=True,
                    include_transactions=True,
                )
            ],
            fields=request_fields,
        ),
    )

    # Create and run the pipeline
    pipeline = create_pipeline(
        dex=dex,
        delta_path=delta_path,
        provider=provider,
        query=query,
        table_name=TABLE_NAME,
    )
    await run_pipeline(pipeline_name=TABLE_NAME, pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Ensure provider_url is not None
    if provider_url is None:
        raise ValueError("Provider URL cannot be None")

    # Ensure to_block is not None, use from_block + 100 as default if it is
    actual_to_block = to_block if to_block is not None else from_block + 100

    # Clean up existing table before running
    table_path = Path(f"{DATA_PATH}/{TABLE_NAME}")
    if table_path.exists():
        print(f"Removing existing table at {table_path}")
        shutil.rmtree(table_path)

    # Process data
    await sync_data(DATA_PATH, provider_kind, provider_url, from_block, actual_to_block)

    # Display results
    table = DeltaTable(f"{DATA_PATH}/{TABLE_NAME}").to_pyarrow_table()
    # Cast to DataFrame explicitly since we know it's a table
    df = pl.DataFrame(pl.from_arrow(table))
    print(table.schema)

    print(
        df.select(
            "blockchain",
            "project",
            "version",
            "block_date",
            "block_time",
            "token_bought_symbol",
            "token_sold_symbol",
            "token_bought_amount",
            "token_sold_amount",
            "amount_usd",
        ).tail(5)
    )


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
