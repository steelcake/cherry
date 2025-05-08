# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/end_to_end/uni_v3_swaps.py --provider hypersync --from_block 20000000

# After run, you can see the result in the database:
# duckdb data/uni_v3_swaps.db
# SELECT * FROM uni_v3_swaps_decoded_instructions LIMIT 3;
# SELECT * FROM uni_v3_swaps LIMIT 3;
################################################################################
# Import dependencies

import argparse
import asyncio
import pyarrow as pa
import polars as pl
from pathlib import Path
from typing import Optional, Dict, Any

import duckdb

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_core.ingest import (
    ProviderConfig,
    ProviderKind,
    QueryKind,
    Query as IngestQuery,
)
from cherry_core import evm_signature_to_topic0
from cherry_core.ingest.evm import (
    Query,
    Fields,
    TransactionFields,
    LogFields,
    TransactionRequest,
)


# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)


PROVIDER_URLS = {
    ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}


################################################################################
# Main function

def filter_uni_v3_swaps(data: Dict[str, pl.DataFrame], context: Any) -> Dict[str, pl.DataFrame]:
    data["logs"] = data["logs"].filter(
        # Filter in vitalik's logs, the topic0 is the event signature of the Swap event
        # Topic0 is ingested as a binary, so we need to encode it to a hex string before comparing.
        # At this point the transformation step to hex encode the data is not applied yet.
        pl.col("topic0").bin.encode("hex").str.to_lowercase() == context["topic0"].strip("0x")
    )
    return data


async def main(
    provider_kind: ProviderKind,
    provider_url: str,
    from_block: int,
    to_block: Optional[int],
):

    # Defining a Provider
    provider = ProviderConfig(
        kind=provider_kind,
        url=provider_url,
        stop_on_head=True,
    )

    event_signature = "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
    topic0 = evm_signature_to_topic0(event_signature)
    # Querying
    query = IngestQuery(
        kind=QueryKind.EVM,
        params=Query(
            from_block=from_block,  # Required: Starting block number
            to_block=to_block,  # Optional: Ending block number
            include_all_blocks=False,  # Optional: Weather to include blocks with no matches in the tables request
            fields=Fields(  # Required: Which fields (columns) to return on each table
                transaction=TransactionFields(  # Required: Transaction fields
                    block_number=True,
                    transaction_index=True,
                    from_=True,
                    to=True,
                ),
                log=LogFields(  # Required: Log fields
                    block_number=True,
                    block_hash=True,
                    transaction_index=True,
                    log_index=True,
                    transaction_hash=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                )
            ),
            transactions=[ # Optional: List of specific filters for instructions
                TransactionRequest(
                    from_=["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"],
                    include_logs=True,
                )
            ],
        ),
    )

    # Transformation Steps
    steps = [
        # Filter in vitalik's logs, the topic0 is the event signature of the Swap event
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=filter_uni_v3_swaps,
                context={
                    "topic0": topic0,
                },
            ),
        ),
        # Decode the events
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=event_signature,
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        # Cast numeric fields to proper types
        cc.Step(
            kind=cc.StepKind.CAST,
            config=cc.CastConfig(
                table_name="decoded_logs",
                mappings={
                    "amount0": pa.float64(),
                    "amount1": pa.float64(),
                    "sqrtPriceX96": pa.float64(),
                    "liquidity": pa.float64(),
                    "tick": pa.int64(),
                    "timestamp": pa.float64(),
                }
            ),
        ),
        # Join the transaction data
        cc.Step(
            kind=cc.StepKind.JOIN_EVM_TRANSACTION_DATA,
            config=cc.JoinEvmTransactionDataConfig(),
        ),
        # Hex encode the data
        cc.Step(
            kind=cc.StepKind.HEX_ENCODE,
            config=cc.HexEncodeConfig(),
        ),
    ]

    # Write to Database
    connection = duckdb.connect("data/uni_v3_swaps.db")
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=steps,
    )
    await run_pipeline(pipeline_name="uni_v3_swaps", pipeline=pipeline)
    data = connection.sql("SELECT * FROM decoded_logs LIMIT 3")
    print(f"Decoded Instructions:\n{data}")

    connection.close()


################################################################################
# CLI Argument Parser for starting and ending block
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instructions tracker")
    parser.add_argument(
        "--provider",
        choices=["hypersync", "sqd"],
        required=True,
        help="Specify the provider ('hypersync' or 'sqd')",
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
