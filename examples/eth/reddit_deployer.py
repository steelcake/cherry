import argparse
import asyncio
import logging
import os
from typing import Any, Dict, Optional

import duckdb
import polars as pl
import pyarrow as pa
from cherry_core import ingest
from dotenv import load_dotenv

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger("examples.eth.reddit_deployer")


PROVIDER_URLS = {
    ingest.ProviderKind.HYPERSYNC: "https://polygon.hypersync.xyz",
    ingest.ProviderKind.SQD: "https://portal.sqd.dev/datasets/polygon-mainnet",
}


TABLE_NAME = "reddit_deployer_contracts"
# Reddit deployer contract
CONTRACT_ADDRESS = "0x36FB3886CF3Fc4E44d8b99D9a8520425239618C2"


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    traces = data["traces"]

    deployed_contracts = traces.filter(pl.col("address").is_not_null())

    return {"deployed_contracts": deployed_contracts}


async def main(
    provider_kind: ingest.ProviderKind,
    provider_url: Optional[str],
    from_block: int,
    to_block: Optional[int],
):
    # Start duckdb
    connection = duckdb.connect().cursor()

    logger.info(f"starting to ingest from block {from_block}")

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
            transactions=[
                ingest.evm.TransactionRequest(
                    from_=[CONTRACT_ADDRESS], include_traces=True
                )
            ],
            fields=ingest.evm.Fields(
                trace=ingest.evm.TraceFields(
                    address=True,
                    block_number=True,
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
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline, pipeline_name="reddit deployer")

    data = connection.sql("SELECT * FROM deployed_contracts")
    logger.info(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit contract deployer")
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

    provider = "sqd"
    url = PROVIDER_URLS[provider]

    from_block = int(args.from_block)
    to_block = int(args.to_block) if args.to_block is not None else None

    asyncio.run(main(provider, url, from_block, to_block))
