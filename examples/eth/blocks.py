import argparse
import asyncio
import logging
import os
from typing import Any, Dict, Optional

import duckdb
import polars as pl
from cherry_core import ingest
from dotenv import load_dotenv

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.blocks")

if not os.path.exists("data"):
    os.makedirs("data")

TABLE_NAME = "blocks"
DB_PATH = "./data/blocks"

PROVIDER_URLS = {
    ingest.ProviderKind.HYPERSYNC: "https://eth.hypersync.xyz",
    ingest.ProviderKind.SQD: "https://portal.sqd.dev/datasets/ethereum-mainnet",
}


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    out = data["blocks"]

    return {"blocks": out}


async def sync_data(
    connection: duckdb.DuckDBPyConnection,
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    logger.info(f"starting to ingest from block {from_block}")

    if to_block is not None and from_block > to_block:
        raise Exception("block range is invalid")

    provider = ingest.ProviderConfig(
        kind=provider_kind,
        url=provider_url,
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=True,
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(
                    number=True,
                    hash=True,
                    parent_hash=True,
                    nonce=True,
                    logs_bloom=True,
                    transactions_root=True,
                    state_root=True,
                    receipts_root=True,
                    miner=True,
                    # difficulty=True,
                    # total_difficulty=True,
                    # extra_data=True,
                    # size=True,
                    # gas_limit=True,
                    # gas_used=True,
                    # timestamp=True,
                    uncles=True,
                    # base_fee_per_gas=True,
                    # # blob_gas_used=True,
                    # # excess_blob_gas=True,
                    # # parent_beacon_block_root=True,
                    withdrawals_root=True,
                    # withdrawals=True,
                ),
            ),
        ),
    )

    # configure a very simple duckdb writer
    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(
            connection=connection.cursor(),
        ),
    )

    # main pipeline configuration object, this object will tell cherry how to run the etl pipeline
    pipeline = cc.Pipeline(
        # data provider to be used
        provider=provider,
        query=query,
        # writer to be used, only need to change this parameter to write to some other output.
        writer=writer,
        steps=[
            # run our custom step to process traces into address appearances
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
            # prefix hex encode all binary fields so it is easy to view later
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    # cherry will make sure each part of the pipeline is parallelized as well as possible
    await run_pipeline(pipeline_name=TABLE_NAME, pipeline=pipeline)


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    connection = duckdb.connect(database=DB_PATH)

    # sync the data into duckdb
    await sync_data(
        connection.cursor(), provider_kind, provider_url, from_block, to_block
    )

    # read result to show
    data = connection.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 20")
    logger.info(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Blocks tracker")
    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )
    parser.add_argument(
        "--from-block",
        required=True,
        help="Specify the block to start from",
    )
    parser.add_argument(
        "--to-block",
        required=False,
        help="Specify the block to stop at, inclusive",
    )

    args = parser.parse_args()

    url = PROVIDER_URLS[args.provider]

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(args.provider, url, from_block, to_block))
