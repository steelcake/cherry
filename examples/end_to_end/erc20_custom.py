# This example shows a simple custom pipeline that ingests and decodes erc20 transfers into duckdb
# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# uv run examples/end_to_end/erc20_custom.py --provider hypersync

# After run, you can see the result in the database:
# duckdb data/transfers.db
# SELECT * FROM transfers LIMIT 3;

import pyarrow as pa
from cherry_etl import config as cc
from cherry_etl import run_pipeline
from cherry_core import ingest, evm_signature_to_topic0
import logging
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
import traceback
from typing import Dict, Optional, Any
import argparse
import polars
import duckdb

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

# Create directories
DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

db_path = "./data/transfers"


# Read the last block number we have written so far to the database
def get_start_block(con: duckdb.DuckDBPyConnection) -> int:
    try:
        res = con.sql("SELECT MAX(block_number) from transfers").fetchone()
        if res is not None:
            return int(res[0])
        else:
            return 0
    except Exception:
        logger.warning(f"failed to get start block from db: {traceback.format_exc()}")
        return 0


# Custom processing step
def join_data(data: Dict[str, polars.DataFrame], _: Any) -> Dict[str, polars.DataFrame]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    bn = blocks.get_column("number")
    logger.info(f"processing data from: {bn.min()} to: {bn.max()}")

    blocks = blocks.select(
        polars.col("number").alias("block_number"),
        polars.col("timestamp").alias("block_timestamp"),
    )
    out = transfers.join(blocks, on="block_number")

    return {"transfers": out}


async def print_last_processed_transfers(db: duckdb.DuckDBPyConnection):
    while True:
        await asyncio.sleep(5)

        data = db.sql(
            'SELECT block_number, transaction_hash, "from", "to", amount FROM transfers ORDER BY block_number, log_index DESC LIMIT 10'
        )
        logger.info("printing last 10 transfers:")
        logger.info(f"\n{data}")


async def main(provider_kind: ingest.ProviderKind, url: Optional[str]):
    # Start duckdb
    connection = duckdb.connect(database=db_path).cursor()

    from_block = get_start_block(connection.cursor())
    logger.info(f"starting to ingest from block {from_block}")

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=url,
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            # Select the logs we are interested in
            logs=[
                ingest.evm.LogRequest(
                    # Don't pass address filter to get all erc20 transfers
                    # address=[
                    #     "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
                    #     "0xB8c77482e45F1F44dE1745F52C74426C631bDD52",  # BNB
                    #     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                    #     "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # stETH
                    #     "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",  # Wrapped BTC
                    #     "0x582d872A1B094FC48F5DE31D3B73F2D9bE47def1",  # Wrapped TON coin
                    # ],
                    topic0=[
                        evm_signature_to_topic0("Transfer(address,address,uint256)")
                    ],
                    # include the blocks related to our logs
                    include_blocks=True,
                )
            ],
            # select the fields we want
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(number=True, timestamp=True),
                log=ingest.evm.LogFields(
                    block_number=True,
                    transaction_hash=True,
                    log_index=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        writer=writer,
        query=query,
        steps=[
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature="Transfer(address indexed from, address indexed to, uint256 amount)",
                    output_table="transfers",
                    # Write null if decoding fails instead of erroring out.
                    #
                    # This is needed if we are trying to decode all logs that match our topic0 without
                    # filtering for contract address, because other events like NFT transfers also match our topic0
                    allow_decode_fail=True,
                ),
            ),
            # Cast all Decimal256 columns to Decimal128, we have to do this because polars doesn't support decimal256
            cc.Step(
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                    # Write null if the value doesn't fit in decimal128,
                    allow_cast_fail=True,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=join_data,
                ),
            ),
            # cast transfers.block_timestamp to int64
            cc.Step(
                kind=cc.StepKind.CAST,
                config=cc.CastConfig(
                    table_name="transfers",
                    mappings={"block_timestamp": pa.int64()},
                ),
            ),
            # hex encode all binary fields to it is easy to view them in db
            # this is not good for performance but nice for easily seeing the values when doing queries to the database
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    asyncio.create_task(print_last_processed_transfers(connection.cursor()))

    await run_pipeline(pipeline=pipeline)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="example")

    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

    url = None

    if args.provider == ingest.ProviderKind.HYPERSYNC:
        url = "https://eth.hypersync.xyz"
    elif args.provider == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/ethereum-mainnet"

    asyncio.run(main(args.provider, url))
