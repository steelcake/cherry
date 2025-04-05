import logging
from typing import Any, Dict, Optional

import asyncio
import argparse
import polars as pl
import pyarrow as pa
from cherry_core import ingest
from dotenv import load_dotenv
import duckdb

from cherry_etl import config as cc

load_dotenv()


logger = logging.getLogger(__name__)


TABLE_NAME = "reddit_deployer_contracts"
# Reddit deployer contract
CONTRACT_ADDRESS = "0x36FB3886CF3Fc4E44d8b99D9a8520425239618C2"


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    out = data["traces"]

    return {"reddit_deployer_contracts": out}


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
            to_block=to_block,
            include_all_blocks=True,
            transactions=[ingest.evm.TransactionRequest(from_=CONTRACT_ADDRESS)],
            traces=[ingest.evm.TraceRequest(type_=["create"])],
            fields=ingest.evm.Fields(
                trace=ingest.evm.TraceFields(
                    address=True, block_number=True, transaction_hash=True
                )
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

    await run_pipeline(pipeline=pipeline)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit deployer contract")

    parser.add_argument(
        "--provider",
        choices=["sqd", "hypersync"],
        required=True,
        help="Specify the provider ('sqd' or 'hypersync')",
    )

    args = parser.parse_args()

    url = None

    if args.provider == ingest.ProviderKind.HYPERSYNC:
        url = "https://polygon.hypersync.xyz"
    elif args.provider == ingest.ProviderKind.SQD:
        url = "https://portal.sqd.dev/datasets/polygon-mainnet"

    asyncio.run(main(args.provider, url))
