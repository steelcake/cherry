"""DEX trades data processing example using Delta Lake for storage"""

import asyncio
import logging
import os
import shutil
from pathlib import Path

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
SCRIPT_DIR = Path.cwd()
DELTA_PATH = str(SCRIPT_DIR / "data" / "deltalake")
FROM_BLOCK = 18000000
TO_BLOCK = FROM_BLOCK + 100

# Setup
load_dotenv()

pl.Config.set_tbl_cols(-1)

# Create directories
Path(DELTA_PATH).mkdir(parents=True, exist_ok=True)

# Provider URLs
HYPERSYNC_PROVIDER_URL = os.environ.get("INDEXER_URL", "https://eth.hypersync.xyz")
SQD_PROVIDER_URL = os.environ.get(
    "INDEXER_URL", "https://portal.sqd.dev/datasets/ethereum-mainnet"
)


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


async def main():
    # Use Hypersync provider
    provider_kind = ingest.ProviderKind.HYPERSYNC
    provider_url = HYPERSYNC_PROVIDER_URL

    # Clean up existing table before running
    table_path = Path(f"{DELTA_PATH}/{TABLE_NAME}")
    if table_path.exists():
        print(f"Removing existing table at {table_path}")
        shutil.rmtree(table_path)

    # Process data
    await sync_data(DELTA_PATH, provider_kind, provider_url, FROM_BLOCK, TO_BLOCK)

    # Display results
    table = DeltaTable(f"{DELTA_PATH}/{TABLE_NAME}").to_pyarrow_table()
    df = pl.from_arrow(table)
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
    asyncio.run(main())
